package local

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/parallelworks/meridian/problems"
)

// CoordinatorKeyFile is the name of the file in the state directory that
// holds the coordinator's TLS key.
const CoordinatorKeyFile = "coordinator.key"

// maxCoordinatorKey is the most bytes of the coordinator's key file
// CoordinatorKey reads: a P-256 key in PKCS #8 PEM takes about 250.
const maxCoordinatorKey = 4 << 10

// pemPrivateKey is the type of a PEM block that holds a PKCS #8 private
// key, as RFC 7468 names it.
const pemPrivateKey = "PRIVATE KEY"

// CoordinatorKey returns the coordinator's TLS key
// (coordinator.Options.TLSKey): the ECDSA key on P-256 in
// CoordinatorKeyFile in dir, the state directory, which must be readable
// only by its user, as Dirs.EnsureState makes it. Every process of the
// state directory reads the same key, so a worker placed by one reaches
// any that takes its member over.
//
// Where the file isn't there, it makes it as Keys makes its key file: a
// new key, in PKCS #8 PEM, readable only by the user, whole or not at
// all, unless another process makes it first, whose key it reads then.
// The key is made from crypto/rand, not derived from the key file's as
// Keys.Derive derives keys: FIPS 186-5 makes an ECDSA key from an
// approved random bit generator's output, which ecdsa.GenerateKey always
// uses. A file that isn't such a key is CoordinatorKeyInvalid; one that
// isn't the user's own, or that others can read, is a problem of its own,
// as the key file's is.
func CoordinatorKey(dir string) (*ecdsa.PrivateKey, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	key, err := readCoordinatorKey(root)
	if !errors.Is(err, fs.ErrNotExist) {
		return key, err
	}
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	data := pem.EncodeToMemory(&pem.Block{Type: pemPrivateKey, Bytes: der})
	created, err := createPrivate(root, CoordinatorKeyFile, data)
	clear(der)
	clear(data)
	if err != nil {
		return nil, err
	}
	if !created {
		return readCoordinatorKey(root)
	}
	return key, nil
}

// readCoordinatorKey reads the key in the coordinator's key file in root,
// which must be a regular file, not a symbolic link, that belongs to the
// user and that only they can read (readPrivate). A key file that isn't
// there is fs.ErrNotExist.
func readCoordinatorKey(root *os.Root) (*ecdsa.PrivateKey, error) {
	path := filepath.Join(root.Name(), CoordinatorKeyFile)
	data, err := readPrivate(root, CoordinatorKeyFile, maxCoordinatorKey+1)
	if err != nil {
		return nil, err
	}
	defer clear(data)
	if len(data) > maxCoordinatorKey {
		return nil, problems.FileOverLimit.Problem(problems.Params{"path": path, "limit": maxCoordinatorKey})
	}
	invalid := problems.CoordinatorKeyInvalid.Problem(problems.Params{"path": path})
	// One block and nothing else, which pem.Decode doesn't check: it
	// skips what comes before a block.
	block, rest := pem.Decode(data)
	if block == nil || block.Type != pemPrivateKey || len(block.Headers) != 0 ||
		!bytes.HasPrefix(data, []byte("-----BEGIN ")) || len(bytes.TrimSpace(rest)) != 0 {
		return nil, invalid
	}
	defer clear(block.Bytes)
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, invalid
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, invalid
	}
	return key, nil
}
