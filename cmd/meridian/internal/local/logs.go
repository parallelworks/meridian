package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/workerapi"
)

// LogsDir is the name of the directory of logs in the state directory.
const LogsDir = "logs"

// Logs keeps logs as files in the state directory: one directory for each
// store, named after its ID, holding one for each run, holding one for
// each attempt of each job instance, holding a file for each of the
// attempt's logs. The store's ID keeps a run's logs apart from those of a
// run of the same number that an earlier store, since removed, gave. A
// log that has ended has a file beside it, its end marker, which is
// written once the log's bytes are on disk, so a log whose marker is there
// has every byte it will have.
//
// Every append is on disk before Append returns, so a worker that heard
// that a part was stored never needs to send it again. Logs serializes the
// appends of one process; a run's logs are written by the one coordinator
// that holds it. Every file it opens is through a root at the logs
// directory, which keeps every path in it there.
type Logs struct {
	root  *os.Root
	seed  maphash.Seed
	locks [64]sync.Mutex
}

var _ coordinator.Logs = (*Logs)(nil)

// NewLogs returns the Logs of the store storeID in dir, the state
// directory, which must be readable only by its user, as Dirs.EnsureState
// makes it. Close closes it.
func NewLogs(dir, storeID string) (*Logs, error) {
	if storeID == "" {
		return nil, errors.New("local: logs without a store's ID")
	}
	logs := filepath.Join(dir, LogsDir, escapeName(storeID))
	if err := os.MkdirAll(logs, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(logs)
	if err != nil {
		return nil, err
	}
	return &Logs{root: root, seed: maphash.MakeSeed()}, nil
}

// Close closes the logs directory. Logs can't be used after it.
func (l *Logs) Close() error { return l.root.Close() }

// endSuffix is what a log's end marker's name adds to the log's.
const endSuffix = ".end"

// path returns the path of the log at key in the logs directory. Each
// name in it is escaped, so that no run ID or job key can name another
// directory.
func (l *Logs) path(key coordinator.LogKey) (string, error) {
	if key.Run == "" || key.Job == "" || key.Attempt < 1 || key.Step < 0 || key.Try < 0 {
		return "", fmt.Errorf("%+v doesn't name a log", key)
	}
	var name string
	switch key.Part {
	case workerapi.LogSetup:
		name = "setup.log"
	case workerapi.LogStep, workerapi.LogCleanup:
		name = fmt.Sprintf("%s-%d-%d.log", key.Part, key.Step, key.Try)
	default:
		return "", fmt.Errorf("%q isn't a log's part", key.Part)
	}
	return filepath.Join(escapeName(key.Run), escapeName(key.Job), strconv.Itoa(key.Attempt), name), nil
}

// maxName is the longest name escapeName gives, well within the 255 bytes
// most filesystems allow a name.
const maxName = 128

// escapeName escapes s for a file's name: lower-case letters, digits, _
// and - are themselves, and every other byte is % and its hex in upper
// case, so that a name is never empty, ., .. or a path, and two names
// differ even on a filesystem that ignores case. A name that would be
// longer than maxName is cut short, and ends with ~ and the start of the
// SHA-256 digest of s, which tells it from others cut the same.
func escapeName(s string) string {
	var b strings.Builder
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	name := b.String()
	if len(name) <= maxName {
		return name
	}
	sum := sha256.Sum256([]byte(s))
	suffix := "~" + hex.EncodeToString(sum[:8])
	cut := maxName - len(suffix)
	// Cut before an escape rather than within one.
	if i := strings.LastIndexByte(name[:cut], '%'); i >= cut-2 {
		cut = i
	}
	return name[:cut] + suffix
}

// lock returns the lock that serializes the appends of the log at path:
// one of a few, shared by the logs whose paths hash alike.
func (l *Logs) lock(path string) *sync.Mutex {
	return &l.locks[maphash.String(l.seed, path)%uint64(len(l.locks))]
}

// Append stores data as the part of the log at key that starts at offset.
// It skips the bytes the log has already, and returns the log's length and
// whether it has ended. An offset past the log's end, or bytes past the
// end of a log that has ended, return ErrLogGap.
func (l *Logs) Append(ctx context.Context, key coordinator.LogKey, offset int64, data []byte, end bool) (length int64, ended bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if offset < 0 {
		return 0, false, fmt.Errorf("a log's offset is %d, less than 0", offset)
	}
	path, err := l.path(key)
	if err != nil {
		return 0, false, err
	}
	m := l.lock(path)
	m.Lock()
	defer m.Unlock()

	if err := l.root.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return 0, false, err
	}
	ended, err = l.exists(path + endSuffix)
	if err != nil {
		return 0, false, err
	}
	f, err := l.root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	length = info.Size()
	upTo := offset + int64(len(data))
	switch {
	case offset > length:
		return length, ended, coordinator.ErrLogGap
	case ended && upTo > length:
		return length, ended, coordinator.ErrLogGap
	case ended:
		return length, ended, nil
	}
	if upTo > length {
		n, err := f.Write(data[length-offset:])
		length += int64(n)
		if err != nil {
			return length, false, err
		}
		if err := f.Sync(); err != nil {
			return length, false, err
		}
	}
	if end {
		if err := l.markEnd(path + endSuffix); err != nil {
			return length, false, err
		}
		ended = true
	}
	return length, ended, nil
}

// markEnd writes the end marker at path, and makes it durable.
func (l *Logs) markEnd(path string) error {
	f, err := l.root.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(l.root, filepath.Dir(path))
}

// exists says whether there is a file at path.
func (l *Logs) exists(path string) (bool, error) {
	_, err := l.root.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// Read returns at most limit bytes of the log at key, from offset, and
// whether the log has ended. A log that hasn't begun has no bytes and
// hasn't ended.
func (l *Logs) Read(ctx context.Context, key coordinator.LogKey, offset int64, limit int) (data []byte, ended bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if offset < 0 || limit < 0 {
		return nil, false, fmt.Errorf("can't read %d bytes of a log from %d", limit, offset)
	}
	path, err := l.path(key)
	if err != nil {
		return nil, false, err
	}
	// The marker is checked before the bytes are read, and written after
	// they are on disk, so a log that has ended has them all.
	ended, err = l.exists(path + endSuffix)
	if err != nil {
		return nil, false, err
	}
	f, err := l.root.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ended, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if offset >= info.Size() {
		return nil, ended, nil
	}
	data = make([]byte, min(int64(limit), info.Size()-offset))
	n, err := f.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	return data[:n], ended, nil
}
