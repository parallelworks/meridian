package safepath

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/parallelworks/meridian/problems"
)

// OpenExecutable opens open, the file at path or one that names the same
// file, as the file a process runs does (/proc/self/exe), once it has
// checked that no other user can change the way to path (Check); and
// checks that what it opened is a regular file of the user's or root's
// that no other user can change. It returns the file, open, and what fstat
// says of it. It is the check of a file run with what the user's other
// processes, or other users, mustn't have, as a worker's binary is, or a
// command a worker's token passes through, as systemd-run, or one that
// reaches a host with the user's keys, as ssh: another user who could
// change it could have it read what it is given. What is wrong is a
// problem, of Check's kinds or of the kind problems.FileNotRegular,
// OwnerUnknown, FileNotOwned (for a file neither the user's nor root's) or
// FileChangeable; any other error is what the operating system said.
func OpenExecutable(path, open string) (*os.File, fs.FileInfo, error) {
	if err := Check(path, false); err != nil {
		return nil, nil, err
	}
	f, err := os.Open(open) //nolint:gosec // a file to run, whose way Check checked
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	owner, known := Owner(info)
	switch {
	case !info.Mode().IsRegular():
		err = problems.FileNotRegular.Problem(problems.Params{"path": path})
	case !known:
		err = ownerUnknown(path)
	case owner != os.Getuid() && owner != 0:
		err = problems.FileNotOwned.Problem(problems.Params{"path": path})
	case info.Mode().Perm()&0o022 != 0:
		err = problems.FileChangeable.Problem(problems.Params{"path": path, "mode": mode(info.Mode().Perm())})
	default:
		return f, info, nil
	}
	f.Close()
	return nil, nil, err
}

// CheckExecutable checks the file at path as OpenExecutable does. path is
// absolute, and names no symbolic link, so that what runs it runs the file
// checked: one whose target is in a directory another user can change
// could be replaced after the check.
func CheckExecutable(path string) error {
	f, _, err := OpenExecutable(path, path)
	if f != nil {
		f.Close()
	}
	return err
}

// LookExecutable returns the path of the command name, as exec.LookPath
// finds it, on PATH where it has no /, absolute and without symbolic
// links, once CheckExecutable has checked it: a command run with what the
// user's other processes, or other users, mustn't have, as meridian host
// runs systemd-run, or the coordinator runs ssh. A directory another user
// can change can come first on a PATH, as a shared bin/ that a cluster's
// startup files add. One CheckExecutable refuses is its problem, with the
// path it checked; one that isn't there an error that is
// exec.ErrNotFound, or fs.ErrNotExist for a name with a /.
func LookExecutable(name string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil && !errors.Is(err, exec.ErrDot) {
		return "", err
	}
	// One found in a directory the PATH names relative to the working
	// directory is checked as any other, at its absolute path.
	if path, err = filepath.Abs(path); err == nil {
		path, err = filepath.EvalSymlinks(path)
	}
	if err != nil {
		return "", err
	}
	return path, CheckExecutable(path)
}
