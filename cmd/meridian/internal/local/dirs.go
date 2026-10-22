// Package local is what the meridian command runs a workflow with on one
// machine: its directories, the user's config, and the local
// implementations of the interfaces through which the compiler and the
// coordinator reach what differs between a laptop and a platform
// (docs/design.md, Interfaces).
//
//   - Variables reads organization and user variables from a variables file
//     in the config directory, and a secret's value from the environment
//     variable or file it names, only when it is asked for one.
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
// Every directory and file here that holds the user's config or state is
// readable only by its user, and the package refuses one that others can
// change.
package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
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
			return "", fmt.Errorf("%s is %q, which isn't an absolute path", env, d)
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
		return "", fmt.Errorf("can't find Meridian's directories: HOME isn't set to an absolute path, and %s isn't set", env)
	}
	if runtime.GOOS == "darwin" {
		return filepath.Join(home, darwin), nil
	}
	return filepath.Join(home, xdgDefault, "meridian"), nil
}

// EnsureState creates the state directory if it isn't there, readable
// only by its user, and checks that it is. A state directory that another
// user can read or change is refused, with the command that fixes it,
// rather than used: it holds the key that wraps runs' secrets.
func (d Dirs) EnsureState() error {
	return ensurePrivateDir(d.State, "state directory")
}

// EnsureCache creates the cache directory if it isn't there, readable only
// by its user, and checks that it is: it can hold copies of private
// repositories.
func (d Dirs) EnsureCache() error {
	return ensurePrivateDir(d.Cache, "cache directory")
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
		return "", fmt.Errorf("can't tell what filesystem the state directory %s is on: %w", d.State, err)
	}
	if network {
		return "", fmt.Errorf("the state directory %s is on a network filesystem (%s), where SQLite can't lock its database reliably; "+
			"set %s to a directory on a local disk, such as %s=/var/tmp/$USER/meridian", d.State, fsType, EnvStateDir, EnvStateDir)
	}
	return filepath.Join(d.State, DatabaseFile), nil
}

// detectNetworkFS says whether a path is on a network filesystem, and its
// type. A test can replace it.
var detectNetworkFS = networkFS

// ensurePrivateDir creates the directory path if it isn't there, readable
// only by its user, and checks that it is.
func ensurePrivateDir(path, what string) error {
	// The XDG specification asks for 0700 for the directories it creates,
	// the parents included.
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("can't create the %s: %w", what, err)
	}
	return checkPrivateDir(path, what)
}

// checkPrivateDir checks that path is a directory that belongs to the user
// and that no other user can read or change. It follows a symbolic link,
// so that a state directory can be a link to one on a local disk.
func checkPrivateDir(path, what string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("the %s %s isn't a directory", what, path)
	}
	if err := checkOwner(path, info); err != nil {
		return fmt.Errorf("the %s %s %w", what, path, err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("the %s %s can be read or changed by other users (its mode is %04o); run chmod 700 %s", what, path, perm, path)
	}
	return nil
}

// checkPrivateInfo checks that the file info describes, at path, is a
// regular file that belongs to the user and that no other user can read
// or change.
func checkPrivateInfo(path, what string, info fs.FileInfo) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("the %s %s isn't a regular file", what, path)
	}
	if err := checkOwner(path, info); err != nil {
		return fmt.Errorf("the %s %s %w", what, path, err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("the %s %s can be read or changed by other users (its mode is %04o); run chmod 600 %s", what, path, perm, path)
	}
	return nil
}

// checkUnchangeable checks that the file info describes, at path, is a
// regular file that belongs to the user and that no other user can
// change: one who could would decide where the user's jobs run, or what
// they read.
func checkUnchangeable(path, what string, info fs.FileInfo) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("the %s %s isn't a regular file", what, path)
	}
	if err := checkOwner(path, info); err != nil {
		return fmt.Errorf("the %s %s %w", what, path, err)
	}
	if perm := info.Mode().Perm(); perm&0o022 != 0 {
		return fmt.Errorf("the %s %s can be changed by other users (its mode is %04o); run chmod go-w %s", what, path, perm, path)
	}
	return nil
}

// errNotOwner is what checkOwner returns for a file another user owns.
var errNotOwner = errors.New("belongs to another user")
