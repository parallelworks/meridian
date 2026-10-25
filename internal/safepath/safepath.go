// Package safepath checks the directories on the way to a path that holds
// a user's files, as OpenSSH's safe_path checks those on the way to a
// user's keys (Check), and the directories and files at the end of it:
// that each is the user's, and that other users can read or change it no
// more than it allows (CheckDir, EnsureDir and ReadFile). The
// meridian command checks its config, state and cache directories, and the
// files in them, with it, and the worker its bootstrap token's file and
// the directories it keeps its own files in.
//
// What can't be told is refused: where the system doesn't say who owns a
// file, every check fails, as problems.OwnerUnknown, and where a user
// namespace hides an owner, as problems.OwnerUnmapped.
package safepath

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// A checker checks the directories on the way to a path, as OpenSSH's
// safe_path checks those on the way to a user's keys: each must belong to
// the user or to root, and no other user may change it, unless it has the
// sticky bit, as /tmp does, which stops them removing what isn't theirs,
// and what the path takes from it next is the user's or root's. A user who
// could change one could replace what is in it, after it was checked,
// with something of their own. It reads the filesystem through lstat and
// readlink, and asks unmapped whether an owner is one a user namespace
// hides, which a test can replace.
//
// In a user namespace that doesn't map root, as unshare --user,
// Apptainer, bubblewrap and some CI sandboxes make, / and much else show
// as owned by the kernel's overflow ID. Meridian can't tell who owns such
// a directory, so it refuses it, as problems.OwnerUnmapped, rather than
// assume no one else can change it.
type checker struct {
	uid      int
	lstat    func(string) (fs.FileInfo, error)
	readlink func(string) (string, error)
	unmapped func(uid int) bool
}

// maxLinks is how many symbolic links a path may go through, as Linux's
// limit is.
const maxLinks = 40

// Check checks the directories on the way to path, and with leaf, path
// itself, as a directory that holds the user's files. A part of the path
// that isn't there yet is one Meridian will create, and the directory it
// is created in must be safe. What is wrong is a *problem.Problem: of the
// kind problems.PathUnsafe, OwnerUnmapped, OwnerUnknown, PathLinksTooMany
// or DirectoryUnavailable.
func Check(path string, leaf bool) error {
	return checker{uid: os.Getuid(), lstat: os.Lstat, readlink: os.Readlink, unmapped: Unmapped}.check(path, leaf)
}

// check walks path from the root, following symbolic links as the kernel
// does, and checks each directory against what it takes from it next.
func (pc checker) check(path string, leaf bool) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	path = abs
	unavailable := func(at string, err error) error {
		return problems.DirectoryUnavailable.Problem(problems.Params{"path": at, "reason": err.Error()})
	}
	dir := string(filepath.Separator)
	dirInfo, err := pc.lstat(dir)
	if err != nil {
		return unavailable(dir, err)
	}
	rest := split(path)
	links := 0
	for len(rest) > 0 {
		name := rest[0]
		rest = rest[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			dir = filepath.Dir(dir)
			if dirInfo, err = pc.lstat(dir); err != nil {
				return unavailable(dir, err)
			}
			continue
		}
		next := filepath.Join(dir, name)
		info, err := pc.lstat(next)
		if errors.Is(err, fs.ErrNotExist) {
			// What isn't there yet Meridian creates, as the user's, in dir.
			return pc.holds(path, dir, dirInfo, next, nil)
		}
		if err != nil {
			return unavailable(next, err)
		}
		if err := pc.holds(path, dir, dirInfo, next, info); err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			if links++; links > maxLinks {
				return problems.PathLinksTooMany.Problem(problems.Params{"path": path, "max": maxLinks})
			}
			target, err := pc.readlink(next)
			if err != nil {
				return unavailable(next, err)
			}
			if filepath.IsAbs(target) {
				dir = string(filepath.Separator)
				if dirInfo, err = pc.lstat(dir); err != nil {
					return unavailable(dir, err)
				}
			}
			rest = append(split(target), rest...)
			continue
		}
		dir, dirInfo = next, info
	}
	if leaf {
		return pc.holds(path, dir, dirInfo, "", nil)
	}
	return nil
}

// split splits a path into its names.
func split(path string) []string {
	return strings.Split(filepath.ToSlash(path), "/")
}

// holds checks that dir, which dirInfo describes, keeps other users from
// changing entry, which info describes, or which Meridian will create as
// the user's when info is nil. path is what is being checked, for the
// problem.
func (pc checker) holds(path, dir string, dirInfo fs.FileInfo, entry string, info fs.FileInfo) error {
	// unsafe says that d, whose owner is owner, is unsafe, or that its owner
	// is one the user namespace hides.
	unsafe := func(d string, owner int) error {
		if owner != 0 && owner != pc.uid && pc.unmapped(owner) {
			return problems.OwnerUnmapped.Problem(problems.Params{"path": path, "dir": d})
		}
		return problems.PathUnsafe.Problem(problems.Params{"path": path, "dir": d})
	}
	owner, ok := Owner(dirInfo)
	if !ok {
		return ownerUnknown(dir)
	}
	if owner != 0 && owner != pc.uid {
		return unsafe(dir, owner)
	}
	entryOwner := pc.uid
	if info != nil {
		if entryOwner, ok = Owner(info); !ok {
			return ownerUnknown(entry)
		}
	}
	entryOurs := entryOwner == 0 || entryOwner == pc.uid
	if dirInfo.Mode().Perm()&0o022 != 0 {
		switch {
		case dirInfo.Mode()&fs.ModeSticky == 0:
			return unsafe(dir, owner)
		case !entryOurs:
			return unsafe(entry, entryOwner)
		}
	}
	// A symbolic link's owner decides where it points.
	if info != nil && info.Mode()&fs.ModeSymlink != 0 && !entryOurs {
		return unsafe(entry, entryOwner)
	}
	return nil
}
