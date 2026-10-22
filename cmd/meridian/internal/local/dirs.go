// Package local is what the meridian command runs a workflow with on one
// machine: its directories, the user's config, and the local
// implementations of the interfaces through which the compiler and the
// coordinator reach what differs between a laptop and a platform
// (docs/design.md, Interfaces).
//
//   - Variables reads organization and user variables from a variables file
//     in the config directory, and a value from the environment variable or
//     file it names, only when it is asked for one.
//   - Groups gives the execution groups of the groups file in the config
//     directory, and a built-in local group that needs no config.
//   - Inputs provides the input types the user's config knows: an
//     execution-groups input chooses one of its groups.
//   - Sources reads the workflows a uses: names through the git command.
//   - Keys wraps runs' data keys under a key file in the state directory.
//   - Logs keeps logs as files in the state directory.
//   - Progress shows a run's events as they happen.
//
// There is no local Sessions: a workflow with sessions doesn't run here.
//
// Every directory and file here that holds the user's config or state
// belongs to its user, and so does every directory on the way to it, as
// OpenSSH requires of a user's keys: the package refuses one that others
// can change, or read where it holds a secret.
//
// What is wrong is a *problem.Problem of one of the problems package's
// kinds, which says why and what to do; any other error is one the
// operating system or git gave, which may not happen again, or a caller's
// mistake.
package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"github.com/parallelworks/meridian/problems"
)

// The environment variables that choose Meridian's directories in place of
// the platform's.
const (
	EnvConfigDir = "MERIDIAN_CONFIG_DIR"
	EnvStateDir  = "MERIDIAN_STATE_DIR"
	EnvCacheDir  = "MERIDIAN_CACHE_DIR"
)

// Dirs are Meridian's directories on this machine.
type Dirs struct {
	// Config holds the user's execution groups and variables. Nothing in
	// it is ever read from the repository a workflow comes from.
	Config string
	// State holds the database, the key that wraps runs' secret records,
	// and logs. It is readable only by its user.
	State string
	// Cache holds copies of the repositories that workflows' uses: name.
	Cache string
}

// Locate finds Meridian's directories from the environment, which getenv
// reads: MERIDIAN_CONFIG_DIR, MERIDIAN_STATE_DIR and MERIDIAN_CACHE_DIR
// when they are set, and otherwise the platform's. On Linux and other
// Unix systems those are the XDG base directories' meridian
// subdirectories: $XDG_CONFIG_HOME/meridian (~/.config/meridian),
// $XDG_STATE_HOME/meridian (~/.local/state/meridian) and
// $XDG_CACHE_HOME/meridian (~/.cache/meridian). On macOS the config and
// state are both ~/Library/Application Support/Meridian, as Apple's
// guidelines place an application's support files, and the cache is
// ~/Library/Caches/Meridian. Locate creates nothing.
func Locate(getenv func(string) string) (Dirs, error) {
	home := getenv("HOME")
	var d Dirs
	var err error
	if d.Config, err = dir(getenv, EnvConfigDir, "XDG_CONFIG_HOME", home, ".config", "Library/Application Support/Meridian"); err != nil {
		return Dirs{}, err
	}
	if d.State, err = dir(getenv, EnvStateDir, "XDG_STATE_HOME", home, ".local/state", "Library/Application Support/Meridian"); err != nil {
		return Dirs{}, err
	}
	if d.Cache, err = dir(getenv, EnvCacheDir, "XDG_CACHE_HOME", home, ".cache", "Library/Caches/Meridian"); err != nil {
		return Dirs{}, err
	}
	return d, nil
}

// dir is one of Meridian's directories: env's when it is set, and
// otherwise the platform's, which is xdg's meridian subdirectory or
// xdgDefault's under home, or darwin's under home on macOS.
func dir(getenv func(string) string, env, xdg, home, xdgDefault, darwin string) (string, error) {
	if d := getenv(env); d != "" {
		if !filepath.IsAbs(d) {
			return "", problems.DirectoryVariableRelative.Problem(problems.Params{"variable": env, "value": d})
		}
		return filepath.Clean(d), nil
	}
	if runtime.GOOS != "darwin" {
		// The XDG specification says a relative path is invalid, and is
		// ignored.
		if d := getenv(xdg); filepath.IsAbs(d) {
			return filepath.Join(d, "meridian"), nil
		}
	}
	if !filepath.IsAbs(home) {
		return "", problems.HomeUnknown.Problem(problems.Params{"variable": env})
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, darwin), nil
	}
	return filepath.Join(home, xdgDefault, "meridian"), nil
}

// EnsureState creates the state directory if it isn't there, readable
// only by its user, and checks that it, and every directory on the way to
// it, is safe. A state directory that another user can read or change is
// refused, with the command that fixes it, rather than tightened: it holds
// the key that wraps runs' secrets, and what was in it may have been read.
func (d Dirs) EnsureState() error {
	return ensurePrivateDir(d.State)
}

// EnsureCache creates the cache directory if it isn't there, readable only
// by its user, and checks it as EnsureState checks the state directory: it
// can hold copies of private repositories.
func (d Dirs) EnsureCache() error {
	return ensurePrivateDir(d.Cache)
}

// CheckConfig checks the config directory, if it is there: that it, and
// every directory on the way to it, belongs to the user or to root, and
// that no other user can change it. LoadConfig checks the files in it.
func (d Dirs) CheckConfig() error {
	if _, err := os.Stat(d.Config); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return checkSafePath(d.Config, true)
}

// DatabaseFile is the name of the database in the state directory.
const DatabaseFile = "meridian.db"

// Database returns the path of the database, in the state directory, which
// it creates if it isn't there. SQLite can't lock reliably on a network
// filesystem, which is where home directories on clusters often are, so
// Database refuses a state directory on one, and says how to choose a
// local one.
func (d Dirs) Database() (string, error) {
	if err := d.EnsureState(); err != nil {
		return "", err
	}
	fsType, network, err := detectNetworkFS(d.State)
	if err != nil {
		return "", problems.FilesystemUnknown.Problem(problems.Params{"path": d.State, "reason": err.Error()})
	}
	if network {
		return "", problems.NetworkFilesystem.Problem(problems.Params{"path": d.State, "type": fsType})
	}
	return filepath.Join(d.State, DatabaseFile), nil
}

// detectNetworkFS says whether a path is on a network filesystem, and its
// type. A test can replace it.
var detectNetworkFS = networkFS

// ensurePrivateDir creates the directory path if it isn't there, readable
// only by its user, and checks that it is, and that every directory on the
// way to it is safe, before it creates anything and after.
func ensurePrivateDir(path string) error {
	if err := checkSafePath(path, false); err != nil {
		return err
	}
	// The XDG specification asks for 0700 for the directories it creates,
	// the parents included.
	if err := os.MkdirAll(path, 0o700); err != nil {
		return problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	if err := checkSafePath(path, false); err != nil {
		return err
	}
	return checkPrivateDir(path)
}

// checkPrivateDir checks that path is a directory that belongs to the user
// and that no other user can read or change. It follows a symbolic link,
// so that a state directory can be a link to one on a local disk.
func checkPrivateDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	switch {
	case !info.IsDir():
		return problems.NotADirectory.Problem(problems.Params{"path": path})
	case !ownedByUser(info):
		return problems.DirectoryNotOwned.Problem(problems.Params{"path": path})
	case info.Mode().Perm()&0o077 != 0:
		return problems.DirectoryNotPrivate.Problem(problems.Params{"path": path, "mode": fmt.Sprintf("%04o", info.Mode().Perm())})
	}
	return nil
}

// checkPrivateInfo checks that the file info describes, at path, is a
// regular file that belongs to the user and that no other user can read
// or change.
func checkPrivateInfo(path string, info fs.FileInfo) error {
	if err := checkOwnFile(path, info); err != nil {
		return err
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return problems.FileNotPrivate.Problem(problems.Params{"path": path, "mode": fmt.Sprintf("%04o", perm)})
	}
	return nil
}

// checkUnchangeable checks that the file info describes, at path, is a
// regular file that belongs to the user and that no other user can
// change: one who could would decide where the user's jobs run, or what
// they read.
func checkUnchangeable(path string, info fs.FileInfo) error {
	if err := checkOwnFile(path, info); err != nil {
		return err
	}
	if perm := info.Mode().Perm(); perm&0o022 != 0 {
		return problems.FileChangeable.Problem(problems.Params{"path": path, "mode": fmt.Sprintf("%04o", perm)})
	}
	return nil
}

// checkOwnFile checks that the file info describes, at path, is a regular
// file that belongs to the user.
func checkOwnFile(path string, info fs.FileInfo) error {
	if !info.Mode().IsRegular() {
		return problems.FileNotRegular.Problem(problems.Params{"path": path})
	}
	if !ownedByUser(info) {
		return problems.FileNotOwned.Problem(problems.Params{"path": path})
	}
	return nil
}

// ownedByUser says whether the file info describes belongs to the user,
// which only Unix systems say: elsewhere it is true.
func ownedByUser(info fs.FileInfo) bool {
	uid, ok := fileOwner(info)
	return !ok || uid == os.Getuid()
}
