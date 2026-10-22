package worker

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/problems"
)

// A privateDir is the directory a worker keeps its own files in as it
// runs: its spooled logs, and its steps' scripts and $OUTPUTS files. Only
// its user can use it, and it is in a directory no other user can change,
// where it can, so that no other user can take its name. The worker holds
// it open, and makes every file in it through that, so a path that comes
// to name something else, as when a step removes the directory and another
// user makes one of the same name in a shared /tmp, is never used: once
// the directory it holds is gone, the worker makes a new one, with a new
// name.
type privateDir struct {
	// parent is where the worker makes it, and ownParent is set when it
	// made parent too, to remove once it is empty.
	parent    string
	ownParent bool

	mu   sync.Mutex
	path string
	root *os.Root
	// gen counts the directories the worker has made, so that a step can
	// tell that the one it was given went as it ran.
	gen int
	// made are the directories the worker made, held open, to remove when
	// it is done.
	made []madeDir
}

type madeDir struct {
	path string
	info os.FileInfo
	root *os.Root
}

// A madeFile is a file the worker made in its directory, and the
// directory it made it in, held open, to remove it from.
type madeFile struct {
	root *os.Root
	name string
}

// remove removes the file from the directory it was made in, if it is
// still there.
func (f madeFile) remove() {
	_ = f.root.Remove(f.name)
}

// privateParent returns where a worker makes its directory, and whether it
// makes that too: Dir, when the config names one; otherwise
// XDG_RUNTIME_DIR, when environ sets it to a directory of the user's that
// no one else can use; otherwise .meridian in the run's directory, made
// private, when no other user can change the run's directory, where format
// 1 keeps $OUTPUTS too; and otherwise the system's temporary directory,
// with a warning, since another user could take the name of a directory
// there that a step removes as it runs.
func privateParent(dir string, environ []string, runDir string, log *slog.Logger) (string, bool) {
	if dir != "" {
		return dir, false
	}
	if xdg := lookupEnv(environ, "XDG_RUNTIME_DIR"); xdg != "" {
		if _, err := checkPrivate(xdg); err == nil {
			return xdg, false
		}
	}
	if runDir != "" {
		info, err := os.Stat(runDir)
		if os.IsNotExist(err) || err == nil && info.IsDir() && ownedByUser(info) && info.Mode().Perm()&0o022 == 0 {
			parent := filepath.Join(runDir, ".meridian")
			if err := os.MkdirAll(parent, 0o700); err == nil {
				if _, err := checkPrivate(parent); err == nil {
					return parent, true
				}
			}
		}
	}
	log.Warn("keeping the worker's files in the system's temporary directory, where another user could take the name of its directory once a step removes it: "+
		"set XDG_RUNTIME_DIR, or make the run's directory one only you can change", "dir", os.TempDir())
	return "", false
}

// lookupEnv returns the value environ gives name, the last if it gives it
// more than once.
func lookupEnv(environ []string, name string) string {
	value := ""
	for _, kv := range environ {
		if n, v, ok := strings.Cut(kv, "="); ok && n == name {
			value = v
		}
	}
	return value
}

// makePrivateDir makes a private directory in parent, "" for os.TempDir(),
// which it made if ownParent is set.
func makePrivateDir(parent string, ownParent bool) (*privateDir, error) {
	d := &privateDir{parent: parent, ownParent: ownParent}
	if err := d.fresh(); err != nil {
		where := parent
		if where == "" {
			where = os.TempDir()
		}
		return nil, problems.DirectoryUnavailable.Problem(problems.Params{"path": where, "reason": err.Error()})
	}
	return d, nil
}

// fresh makes a new directory, whose name starts with a dot, so that a
// step's rm -rf "$TMPDIR"/* passes it by, checks that it is private, and
// holds it open. Its caller holds d.mu, or has d to itself.
func (d *privateDir) fresh() error {
	path, err := os.MkdirTemp(d.parent, ".meridian-worker-")
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
	d.path, d.root = path, root
	d.gen++
	d.made = append(d.made, madeDir{path, info, root})
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

// current returns the directory's path, the directory, held open, and its
// generation: the one the worker holds, while its path still names it, or
// else a new one.
func (d *privateDir) current() (string, *os.Root, int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.intact() {
		if err := d.fresh(); err != nil {
			return "", nil, 0, err
		}
	}
	return d.path, d.root, d.gen, nil
}

// still reports whether the directory of generation gen is still the
// worker's, at its path.
func (d *privateDir) still(gen int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gen == gen && d.intact()
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

// create makes a new file in the directory, named name, opened with flags,
// and returns it, how to remove it, and its path.
func (d *privateDir) create(name string, flags int) (*os.File, madeFile, string, error) {
	path, root, _, err := d.current()
	if err != nil {
		return nil, madeFile{}, "", err
	}
	f, err := root.OpenFile(name, flags|os.O_CREATE|openNoFollow, 0o600)
	if err != nil {
		return nil, madeFile{}, "", err
	}
	return f, madeFile{root, name}, filepath.Join(path, name), nil
}

// removeAll removes every directory the worker made that is still the
// one it made, and its parent, if it made that and it is empty.
func (d *privateDir) removeAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.made {
		m.root.Close()
		if info, err := os.Lstat(m.path); err == nil && os.SameFile(info, m.info) {
			_ = os.RemoveAll(m.path)
		}
	}
	if d.ownParent {
		_ = os.Remove(d.parent)
	}
}
