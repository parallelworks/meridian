package safepath

import (
	"os"
	"path/filepath"
)

// WriteFile writes the file path whole or not at all, readable only by the
// user: to a new file beside it, synced, then renamed over it, and its
// directory synced, so that once it returns the file is there, as it
// wrote it, after a crash too; and a crash before then leaves the file as
// it was, or as it wrote it, never empty, nor part of what it wrote. The
// new file it leaves, if it fails, it removes.
func WriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".new-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	return syncDir(dir)
}

// syncDir syncs the directory dir, so that what was renamed in it lasts.
func syncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // the directory of a file the caller writes
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
