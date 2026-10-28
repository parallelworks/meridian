package coordtest

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// A StoreTemplate is a store made, and migrated, once a test binary, in a
// directory of its own, which each test's new store is a copy of (Copy).
// Opening a copy finds its schema this release's, where a new store would
// be migrated: under the race detector that takes most of a second, which
// would be most of what many tests cost. The copies share the template's
// store ID.
type StoreTemplate struct {
	// Make makes a store at path, migrated, and closes it: store.Open's
	// work, which this package can't do itself, as the store package's
	// own tests import it.
	Make func(ctx context.Context, path string) error

	once sync.Once
	dir  string
	err  error
}

// Copy copies the template to path, a new file that only its user can
// read, as the store makes one, making the template first if no test has.
func (st *StoreTemplate) Copy(t testing.TB, path string) {
	t.Helper()
	st.once.Do(func() { st.dir, st.err = st.make(t.Context()) })
	if st.err != nil {
		t.Fatal(st.err)
	}
	in, err := os.Open(filepath.Join(st.dir, "meridian.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // a new file at the path the test chose
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// make makes the template in a new directory, and returns the directory.
func (st *StoreTemplate) make(ctx context.Context) (string, error) {
	dir, err := os.MkdirTemp("", "meridian-store-")
	if err != nil {
		return "", err
	}
	if err := st.Make(ctx, filepath.Join(dir, "meridian.db")); err != nil {
		return dir, err
	}
	// Closing the store checkpoints its write-ahead log into its file, so
	// the file holds all of it.
	if info, err := os.Stat(filepath.Join(dir, "meridian.db-wal")); err == nil && info.Size() > 0 {
		return dir, errors.New("coordtest: the store template's write-ahead log holds what its file doesn't")
	}
	return dir, nil
}

// Remove removes the template, if a test made it: a package's TestMain
// calls it once the package's tests have run.
func (st *StoreTemplate) Remove() {
	if st.dir != "" {
		_ = os.RemoveAll(st.dir)
	}
}
