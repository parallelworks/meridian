package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// A pathChecker checks the directories on the way to a path, as OpenSSH's
// safe_path checks those on the way to a user's keys: each must belong to
// the user or to root, and no other user may change it, unless it has the
// sticky bit, as /tmp does, which stops them removing what isn't theirs,
// and what the path takes from it next is the user's or root's. A user who
// could change one could replace what is in it, after it was checked,
// with something of their own. It reads the filesystem through lstat and
// readlink, which a test can replace.
type pathChecker struct {
	uid      int
	lstat    func(string) (fs.FileInfo, error)
	readlink func(string) (string, error)
}

// maxLinks is how many symbolic links a path may go through, as Linux's
// limit is.
const maxLinks = 40

// checkSafePath checks the directories on the way to path, an absolute
// path, and with leaf, path itself, as a directory that holds the user's
// files. A part of the path that isn't there yet is one Meridian will
// create, and the directory it is created in must be safe.
func checkSafePath(path string, leaf bool) error {
	return pathChecker{uid: os.Getuid(), lstat: os.Lstat, readlink: os.Readlink}.check(path, leaf)
}

// check walks path from the root, following symbolic links as the kernel
// does, and checks each directory against what it takes from it next.
func (pc pathChecker) check(path string, leaf bool) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("local: %s isn't an absolute path", path)
	}
	dir := string(filepath.Separator)
	dirInfo, err := pc.lstat(dir)
	if err != nil {
		return err
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
				return err
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
			return err
		}
		if err := pc.holds(path, dir, dirInfo, next, info); err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			if links++; links > maxLinks {
				return fmt.Errorf("local: %s goes through more than %d symbolic links", path, maxLinks)
			}
			target, err := pc.readlink(next)
			if err != nil {
				return err
			}
			if filepath.IsAbs(target) {
				dir = string(filepath.Separator)
				if dirInfo, err = pc.lstat(dir); err != nil {
					return err
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
func (pc pathChecker) holds(path, dir string, dirInfo fs.FileInfo, entry string, info fs.FileInfo) error {
	unsafe := func(d string) error {
		return problems.PathUnsafe.Problem(problems.Params{"path": path, "dir": d})
	}
	owner, ok := fileOwner(dirInfo)
	if !ok {
		return nil // nothing here says who owns a file
	}
	if owner != 0 && owner != pc.uid {
		return unsafe(dir)
	}
	entryOwner := pc.uid
	if info != nil {
		entryOwner, _ = fileOwner(info)
	}
	entryOurs := entryOwner == 0 || entryOwner == pc.uid
	if dirInfo.Mode().Perm()&0o022 != 0 {
		switch {
		case dirInfo.Mode()&fs.ModeSticky == 0:
			return unsafe(dir)
		case !entryOurs:
			return unsafe(entry)
		}
	}
	// A symbolic link's owner decides where it points.
	if info != nil && info.Mode()&fs.ModeSymlink != 0 && !entryOurs {
		return unsafe(entry)
	}
	return nil
}
