package local

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// KeyFile is the name of the key file in the state directory.
const KeyFile = "secret-record.key"

// keySize is the size of the key file's key, and of the data keys it
// wraps: AES-256's.
const keySize = 32

// wrapVersion is the first byte of a wrapped key: how it was wrapped, so
// that a later way, such as one under a key that replaced this one, can be
// told from this.
const wrapVersion = 1

// Keys wraps the data keys of runs' secret records under a key in a file
// in the state directory, which only the user can read, with AES-256-GCM
// and a random nonce, through the standard library's FIPS 140-3 module.
// The run's ID is the associated data, so a wrapped key can't be moved to
// another run.
//
// Wrap creates the key file the first time it is needed, and Unwrap never
// does: a key file that has gone can't unwrap what the one before it
// wrapped, and a new one wouldn't either. A key file that isn't the user's
// own, or that others can read, a wrapped key that won't open, and one
// wrapped some other way are problems, which won't go away by trying
// again; a data key that isn't 32 bytes is the caller's mistake.
type Keys struct {
	dir string

	mu   sync.Mutex
	aead cipher.AEAD
}

var _ coordinator.Keys = (*Keys)(nil)

// NewKeys returns the Keys of the key file in dir, the state directory,
// which must be readable only by its user, as Dirs.EnsureState makes it.
func NewKeys(dir string) *Keys {
	return &Keys{dir: dir}
}

// Wrap encrypts key, the data key of the secret record of the run runID.
func (k *Keys) Wrap(ctx context.Context, runID string, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(key) != keySize {
		return nil, fmt.Errorf("local: a data key is %d bytes, not %d", keySize, len(key))
	}
	aead, err := k.cipher(true)
	if err != nil {
		return nil, err
	}
	// The nonce is random, made by Seal, and first in what it gives.
	return aead.Seal([]byte{wrapVersion}, nil, key, associated(runID)), nil //nolint:gosec // NewGCMWithRandomNonce takes no nonce
}

// Unwrap decrypts a data key that Wrap wrapped for runID.
func (k *Keys) Unwrap(ctx context.Context, runID string, wrapped []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(wrapped) == 0 || wrapped[0] != wrapVersion {
		return nil, problems.KeyWrappingUnknown.Problem(problems.Params{"run": runID})
	}
	aead, err := k.cipher(false)
	if err != nil {
		return nil, err
	}
	key, err := aead.Open(nil, nil, wrapped[1:], associated(runID))
	if err != nil {
		return nil, problems.KeyUnwrapFailed.Problem(problems.Params{"run": runID, "path": filepath.Join(k.dir, KeyFile)})
	}
	return key, nil
}

// associated is the associated data of the data key of the run runID's
// secret record: the version of how it is wrapped, and the run's ID.
func associated(runID string) []byte {
	return append([]byte{wrapVersion}, runID...)
}

// cipher returns the AEAD of the key file's key, reading the key file the
// first time, and creating it then if create is true.
func (k *Keys) cipher(create bool) (cipher.AEAD, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.aead != nil {
		return k.aead, nil
	}
	// The key file is opened in the state directory, through a root that
	// keeps every path in it there.
	root, err := os.OpenRoot(k.dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	key, err := readKeyFile(root)
	if errors.Is(err, fs.ErrNotExist) && create {
		key, err = createKeyFile(root)
	}
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, err
	}
	k.aead = aead
	return aead, nil
}

// readKeyFile reads the key in the key file in root, which must be a
// regular file, not a symbolic link, that belongs to the user and that
// only they can read (safepath.CheckFileInfo). A key file that isn't there
// is fs.ErrNotExist.
func readKeyFile(root *os.Root) ([]byte, error) {
	path := filepath.Join(root.Name(), KeyFile)
	// A root follows a link that stays in it, which only the user can put
	// in the state directory; a key file is never read through one.
	if info, err := root.Lstat(KeyFile); err == nil && info.Mode()&fs.ModeSymlink != 0 {
		return nil, problems.FileNotRegular.Problem(problems.Params{"path": path})
	}
	f, err := root.OpenFile(KeyFile, os.O_RDONLY|openFlags, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err != nil {
		return nil, unreadable(path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, unreadable(path, err)
	}
	if err := safepath.CheckFileInfo(path, info, safepath.Private); err != nil {
		return nil, err
	}
	key, err := io.ReadAll(io.LimitReader(f, keySize+1))
	if err != nil {
		return nil, unreadable(path, err)
	}
	if len(key) != keySize {
		return nil, problems.KeyFileInvalid.Problem(problems.Params{"path": path, "size": len(key)})
	}
	return key, nil
}

// createKeyFile creates the key file in root with a new random key,
// readable only by the user, and returns its key. It creates it whole or
// not at all: it writes the key to a file of its own, and links that to
// the key file's name, which fails if another process has made the key
// file meanwhile, in which case it reads that one's key.
func createKeyFile(root *os.Root) (key []byte, err error) {
	key = make([]byte, keySize)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	tmp := ".key-" + hex.EncodeToString(nonce[:])
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rmErr := root.Remove(tmp); rmErr != nil && err == nil {
			err = rmErr
		}
	}()
	if _, err := f.Write(key); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := root.Link(tmp, KeyFile); errors.Is(err, fs.ErrExist) {
		return readKeyFile(root)
	} else if err != nil {
		return nil, err
	}
	if err := syncDir(root, "."); err != nil {
		return nil, err
	}
	return key, nil
}

// syncDir makes the entries of the directory dir in root durable.
func syncDir(root *os.Root, dir string) error {
	d, err := root.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
