package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// A privateDir is the directory a worker keeps its own files in as it
// runs: its spooled logs, and its steps' scripts and $OUTPUTS files. Only
// its user can use it. The worker holds it open, and makes every file in
// it through that, so a path that comes to name something else, as when
// a step removes the directory and another user makes one of the same
// name in a shared /tmp, is never used: once the directory it holds is
// gone, the worker makes a new one, with a new name.
type privateDir struct {
	// parent is where the worker makes it, "" for os.TempDir().
	parent string

	mu   sync.Mutex
	path string
	root *os.Root
	// made are the directories the worker made, to remove when it is done.
	made []madeDir
}

type madeDir struct {
	path string
	info os.FileInfo
}

// makePrivateDir makes a private directory in parent.
func makePrivateDir(parent string) (*privateDir, error) {
	d := &privateDir{parent: parent}
	if err := d.fresh(); err != nil {
		return nil, fmt.Errorf("worker: making its directory: %w", err)
	}
	return d, nil
}

// fresh makes a new directory, checks that it is private, and holds it
// open. Its caller holds d.mu, or has d to itself.
func (d *privateDir) fresh() error {
	path, err := os.MkdirTemp(d.parent, "meridian-worker-")
	if err != nil {
		return err
	}
	info, err := checkPrivate(path)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	if held, err := root.Stat("."); err != nil || !os.SameFile(info, held) {
		root.Close()
		return fmt.Errorf("worker: its directory %s changed as it was made", path)
	}
	if d.root != nil {
		d.root.Close()
	}
	d.path, d.root = path, root
	d.made = append(d.made, madeDir{path, info})
	return nil
}

// checkPrivate checks that path names a directory, not a link to one,
// that belongs to the worker's user, and that no one else can use.
func checkPrivate(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || !ownedByUser(info) || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("worker: %s isn't a directory of this user's that only it can use", path)
	}
	return info, nil
}

// current returns the directory's path, and the directory, held open: the
// one the worker holds, while its path still names it, or else a new one.
func (d *privateDir) current() (string, *os.Root, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.intact() {
		if err := d.fresh(); err != nil {
			return "", nil, err
		}
	}
	return d.path, d.root, nil
}

// intact reports whether the directory's path still names the directory
// the worker holds, and it is still private. Its caller holds d.mu.
func (d *privateDir) intact() bool {
	info, err := checkPrivate(d.path)
	if err != nil {
		return false
	}
	held, err := d.root.Stat(".")
	return err == nil && os.SameFile(info, held)
}

// create makes a new file in the directory, named name, for reading and
// writing, and returns it and its path.
func (d *privateDir) create(name string, flags int) (*os.File, string, error) {
	path, root, err := d.current()
	if err != nil {
		return nil, "", err
	}
	f, err := root.OpenFile(name, flags|os.O_CREATE|openNoFollow, 0o600)
	if err != nil {
		return nil, "", err
	}
	return f, filepath.Join(path, name), nil
}

// remove removes a file of the directory's, if it is there.
func (d *privateDir) remove(name string) {
	_, root, err := d.current()
	if err == nil {
		_ = root.Remove(name)
	}
}

// removeAll removes every directory the worker made that is still the
// one it made.
func (d *privateDir) removeAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.root != nil {
		d.root.Close()
	}
	for _, m := range d.made {
		if info, err := os.Lstat(m.path); err == nil && os.SameFile(info, m.info) {
			_ = os.RemoveAll(m.path)
		}
	}
}
