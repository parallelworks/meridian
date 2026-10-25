package worker

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// A privateDir is the directory a worker keeps its own files in as it
// runs: its spooled logs, and its steps' scripts and $OUTPUTS files. Only
// its user can use it, and it is outside the run's directory, which a step
// can remove, as format 1 keeps its own files, in a directory no other
// user can change, where it can, so that no other user can take its name.
// The worker holds it open, and makes every file in it through that, so a
// path that comes to name something else, as when a step removes the
// directory and another user makes one of the same name in a shared /tmp,
// is never used: once the directory it holds is gone, the worker makes a
// new one, with a new name, in the same parent if it can, or else in the
// next it would have used.
type privateDir struct {
	// parents are where the worker can make it, in the order it tries
	// them (privateParents), and at is the one it makes it in now.
	parents []parentDir
	at      int
	log     *slog.Logger

	mu   sync.Mutex
	path string
	root *os.Root
	// gen counts the directories the worker has made, so that a step can
	// tell that the one it was given went as it ran.
	gen int
	// made are the directories the worker made, held open, to remove when
	// it is done.
	made []madeDir
	// exhausted is why the worker couldn't make a directory in any of its
	// parents, once it has tried the last.
	exhausted error
}

// A parentDir is a directory a worker can make its directory in.
type parentDir struct {
	path string
	// create is set for one the worker makes, and makes again if it goes;
	// memory for one in memory, as XDG_RUNTIME_DIR is, where it spools
	// less (memorySpoolLimit); and shared for the system's temporary
	// directory, which it uses as it is, with a warning.
	create, memory, shared bool
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

// privateParents returns where a worker can make its directory, in the
// order it tries them: dir, when the config names one; meridian/worker in
// the user's cache directory, on disk, which it makes; XDG_RUNTIME_DIR,
// which is in memory; and the system's temporary directory, where another
// user could take the name of a directory that a step removes as it runs.
// Each but the last must be a directory of the user's that no other user
// can change, on a path no other user can change (safepath.Check), so that
// none can make a directory in it, as the worker's would be named. None is in
// the run's directory, which a step can remove, as format 1's cleanup:
// rm -r ${PW_JOB_DIR} does, and which can't be removed while the worker
// holds a file in it open on NFS, where an open file that is removed stays
// as a .nfs file.
func privateParents(dir string, environ []string) []parentDir {
	var out []parentDir
	if dir != "" {
		out = append(out, parentDir{path: dir})
	}
	if cache := userCacheDir(environ); cache != "" {
		out = append(out, parentDir{path: filepath.Join(cache, "meridian", "worker"), create: true})
	}
	if xdg := lookupEnv(environ, "XDG_RUNTIME_DIR"); filepath.IsAbs(xdg) {
		out = append(out, parentDir{path: xdg, memory: true})
	}
	return append(out, parentDir{path: os.TempDir(), shared: true})
}

// userCacheDir returns the user's cache directory, as os.UserCacheDir
// finds it, from environ: on macOS ~/Library/Caches, and on the other Unix
// systems $XDG_CACHE_HOME, when it is an absolute path, as the XDG
// specification ignores a relative one, or ~/.cache; or "" without an
// absolute home, and where the worker runs no steps.
func userCacheDir(environ []string) string {
	home := lookupEnv(environ, "HOME")
	switch runtime.GOOS {
	case "windows", "plan9", "js", "wasip1":
		return ""
	case "darwin", "ios":
		if filepath.IsAbs(home) {
			return filepath.Join(home, "Library", "Caches")
		}
		return ""
	}
	if d := lookupEnv(environ, "XDG_CACHE_HOME"); filepath.IsAbs(d) {
		return d
	}
	if filepath.IsAbs(home) {
		return filepath.Join(home, ".cache")
	}
	return ""
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

// ready makes p, if the worker makes it, only its user's to use, and
// checks it: that it is a directory of the user's that no other user can
// change, on a path no other user can change (safepath.EnsureDir and
// CheckDir). The system's temporary directory is used as it is.
func (p parentDir) ready() error {
	switch {
	case p.shared:
		return nil
	case p.create:
		return safepath.EnsureDir(p.path, safepath.Unchangeable)
	}
	return safepath.CheckDir(p.path, safepath.Unchangeable)
}

// makePrivateDir makes a private directory in the first of parents it can
// (privateParents), which are never none, and logs why it couldn't in
// those before it, and that it uses the system's temporary directory, if
// it does. If it can't in any, it returns why for each, joined, each a
// problem: the first, for the parent it would have used first, is the one
// the meridian command shows, and its log has each.
func makePrivateDir(parents []parentDir, log *slog.Logger) (*privateDir, error) {
	d := &privateDir{parents: parents, log: log}
	if err := d.fresh(); err != nil {
		return nil, err
	}
	return d, nil
}

// inMemory reports whether the directory is in memory, as XDG_RUNTIME_DIR
// is.
func (d *privateDir) inMemory() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.at < len(d.parents) && d.parents[d.at].memory
}

// fresh makes a new directory, in the parent the worker uses now, which it
// makes again if it made it, and checks again, or else in the next it can,
// as it would have at first. When it can't in any, it returns why for each,
// joined, each a problem, and the same once it has none left to try. Its
// caller holds d.mu, or has d to itself.
func (d *privateDir) fresh() error {
	var errs []error
	for ; d.at < len(d.parents); d.at++ {
		p := d.parents[d.at]
		err := p.ready()
		if err == nil {
			err = d.makeIn(p.path)
		}
		if err == nil {
			if p.shared {
				d.log.Warn("keeping the worker's files in the system's temporary directory, where another user could take the name of its directory once a step removes it: "+
					"set XDG_RUNTIME_DIR, or give the user a cache directory only they can change", "dir", p.path)
			}
			return nil
		}
		d.log.Warn("can't keep the worker's files in a directory", "dir", p.path, "error", err)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		d.exhausted = errors.Join(errs...)
	}
	return d.exhausted
}

// makeIn makes a new directory in parent, whose name starts with a dot, so
// that a step's rm -rf "$TMPDIR"/* passes it by, checks that it is
// private, and holds it open. Its caller holds d.mu, or has d to itself.
func (d *privateDir) makeIn(parent string) error {
	path, err := os.MkdirTemp(parent, ".meridian-worker-")
	if err != nil {
		return dirUnavailable(parent, err)
	}
	info, err := checkPrivate(path)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		_ = os.Remove(path)
		return dirUnavailable(path, err)
	}
	held, err := root.Stat(".")
	if err != nil {
		root.Close()
		return dirUnavailable(path, err)
	}
	if !os.SameFile(info, held) {
		// Something else is at its path already: the worker leaves it.
		root.Close()
		return problems.DirectoryReplaced.Problem(problems.Params{"path": path})
	}
	d.path, d.root = path, root
	d.gen++
	d.made = append(d.made, madeDir{path, info, root})
	return nil
}

// checkPrivate checks that path names a directory, not a link to one,
// that belongs to the worker's user, and that no one else can use
// (safepath.CheckDirInfo).
func checkPrivate(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, dirUnavailable(path, err)
	}
	if err := safepath.CheckDirInfo(path, info, safepath.Private); err != nil {
		return nil, err
	}
	return info, nil
}

// dirUnavailable is the problem of a directory at path that the worker
// couldn't make, open or read, for the reason err, the operating system's,
// gives.
func dirUnavailable(path string, err error) error {
	return problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
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
// one it made.
func (d *privateDir) removeAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.made {
		m.root.Close()
		if info, err := os.Lstat(m.path); err == nil && os.SameFile(info, m.info) {
			_ = os.RemoveAll(m.path)
		}
	}
}
