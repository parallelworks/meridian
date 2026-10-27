package local

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// RunsDir returns the directory, under the system's temporary directory
// tmp, that runs' directories are made in by default: the user's own,
// named after their user ID, which EnsureRunsDir makes.
func RunsDir(tmp string) string {
	return filepath.Join(tmp, "meridian-"+strconv.Itoa(os.Getuid()))
}

// EnsureRunsDir makes dir, the directory runs' directories are made in, if
// it isn't there, readable only by its user, and checks that it, and every
// directory on the way to it, is safe, as EnsureState checks the state
// directory: another user who could change it could replace a run's
// directory, in which its steps run, with one of their own.
func EnsureRunsDir(dir string) error {
	return safepath.EnsureDir(dir, safepath.Private)
}

// RunDirName is the name of the directory of the run runID of the store
// storeID: its number, then the store's ID, which keeps it apart from the
// directory of a run of the same number that an earlier store gave.
func RunDirName(storeID, runID string) string {
	return escapeName(runID) + "-" + escapeName(storeID)
}

// CheckRunDir checks that a run's directory can be made at path, before
// the run is stored: that no other user can change the way to it, and
// that nothing is there yet, which is problems.RunDirExists.
func CheckRunDir(path string) error {
	if err := safepath.Check(path, false); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return problems.RunDirExists.Problem(problems.Params{"path": path})
	} else if !errors.Is(err, fs.ErrNotExist) {
		return problems.RunDirNotMade.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return nil
}

// MakeRunDir makes the run's directory at path, once the run is stored:
// new, so that a run never shares its directory with another, readable
// only by its user. Something already at path, a symbolic link among
// them, is problems.RunDirExists, and a directory it can't make
// problems.RunDirNotMade. It checks the way to path again, as the run's
// steps will run in it.
func MakeRunDir(path string) error {
	if err := safepath.Check(path, false); err != nil {
		return err
	}
	// Mkdir doesn't follow a symbolic link at path, and fails if anything
	// is there.
	err := os.Mkdir(path, 0o700)
	if errors.Is(err, fs.ErrExist) {
		return problems.RunDirExists.Problem(problems.Params{"path": path})
	}
	if err != nil {
		return problems.RunDirNotMade.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return safepath.CheckDir(path, safepath.Private)
}

// CheckMadeRunDir checks the run's directory at path, which MakeRunDir
// made, again, as before a run is taken up again: that it is still a
// directory of the user's that other users can't use, on a way no other
// user can change. What is wrong is a problem, as one of safepath.CheckDir's.
func CheckMadeRunDir(path string) error {
	return safepath.CheckDir(path, safepath.Private)
}
