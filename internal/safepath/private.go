package safepath

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/parallelworks/meridian/problems"
)

// Access is what other users may do with a directory or a file of the
// user's: the permission bits they must not have.
type Access fs.FileMode

const (
	// Private is a directory or a file that other users may neither read
	// nor change, as one that holds a secret, or what protects one.
	Private Access = 0o077
	// Unchangeable is one that other users may read but not change, as a
	// config file, or a directory others could otherwise make a file in.
	Unchangeable Access = 0o022
)

// ownedByUser reports whether info, of the file or directory at path,
// belongs to the user. Where the system doesn't say who owns a file, it is
// problems.OwnerUnknown: what can't be told is refused.
func ownedByUser(path string, info fs.FileInfo) (bool, error) {
	owner, ok := Owner(info)
	if !ok {
		return false, ownerUnknown(path)
	}
	return owner == os.Getuid(), nil
}

// ownerUnknown is the problem of a file or directory at path whose owner
// the system doesn't say.
func ownerUnknown(path string) error {
	return problems.OwnerUnknown.Problem(problems.Params{"path": path})
}

// mode is permissions as a problem's mode param shows them, as 0755.
func mode(perm fs.FileMode) string {
	return fmt.Sprintf("%04o", perm)
}

// CheckDirInfo checks that info describes, at path, a directory that
// belongs to the user, and that other users can use no more than access
// allows. What is wrong is a problem of the kind problems.NotADirectory,
// DirectoryNotOwned, DirectoryNotPrivate or OwnerUnknown.
func CheckDirInfo(path string, info fs.FileInfo, access Access) error {
	if !info.IsDir() {
		return problems.NotADirectory.Problem(problems.Params{"path": path})
	}
	if ours, err := ownedByUser(path, info); err != nil {
		return err
	} else if !ours {
		return problems.DirectoryNotOwned.Problem(problems.Params{"path": path})
	}
	if perm := info.Mode().Perm(); perm&fs.FileMode(access) != 0 {
		return problems.DirectoryNotPrivate.Problem(problems.Params{"path": path, "mode": mode(perm)})
	}
	return nil
}

// CheckFileInfo checks that info describes, at path, a regular file that
// belongs to the user, and that other users can use no more than access
// allows. What is wrong is a problem of the kind problems.FileNotRegular,
// FileNotOwned, FileNotPrivate for a Private file, FileChangeable for an
// Unchangeable one, or OwnerUnknown.
func CheckFileInfo(path string, info fs.FileInfo, access Access) error {
	if !info.Mode().IsRegular() {
		return problems.FileNotRegular.Problem(problems.Params{"path": path})
	}
	if ours, err := ownedByUser(path, info); err != nil {
		return err
	} else if !ours {
		return problems.FileNotOwned.Problem(problems.Params{"path": path})
	}
	if perm := info.Mode().Perm(); perm&fs.FileMode(access) != 0 {
		k := problems.FileChangeable
		if access == Private {
			k = problems.FileNotPrivate
		}
		return k.Problem(problems.Params{"path": path, "mode": mode(perm)})
	}
	return nil
}

// checked checks that no other user can change the way to path (Check),
// and returns the path it checked: absolute, and clean, as Check reads it,
// so that what is opened through it is what was checked, where the kernel
// would take a .. after a symbolic link to the link's target's parent.
func checked(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return abs, Check(abs, false)
}

// CheckDir checks that no other user can change the way to path (Check),
// and that path is a directory of the user's that other users can use no
// more than access allows (CheckDirInfo). It follows a symbolic link at
// path, whose way Check checks too, so that a directory can be a link to
// one elsewhere, such as on a local disk. A problem names path as Check
// reads it, absolute and clean.
func CheckDir(path string, access Access) error {
	path, err := checked(path)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return CheckDirInfo(path, info, access)
}

// EnsureDir makes the directory path, and those on the way to it that
// aren't there, as the user's, readable only by them, once it has checked
// that no other user can change the way to it (Check), and then checks it
// as CheckDir does: before it makes anything, and after.
func EnsureDir(path string, access Access) error {
	path, err := checked(path)
	if err != nil {
		return err
	}
	// The XDG specification asks for 0700 for the directories it makes,
	// the parents included.
	if err := os.MkdirAll(path, 0o700); err != nil {
		return problems.DirectoryUnavailable.Problem(problems.Params{"path": path, "reason": err.Error()})
	}
	return CheckDir(path, access)
}

// ReadFile reads the file at path whole, once it has checked that no
// other user can change the way to it (Check), if what it opened is a
// regular file of the user's that other users can use no more than access
// allows (CheckFileInfo), and holds at most limit bytes, without cutting it
// short: a larger one is problems.FileOverLimit. It doesn't follow a
// symbolic link at path, which is problems.FileNotRegular, though a
// directory on the way to it can be one, nor wait for a FIFO's writer.
// What is wrong is a problem, which names path as Check reads it,
// absolute and clean; any other error is what the operating system said,
// as fs.ErrNotExist for a file that isn't there.
func ReadFile(path string, access Access, limit int) ([]byte, error) {
	path, err := checked(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|openFlags, 0) //nolint:gosec // a path whose way Check checked, to a file CheckFileInfo checks
	if err != nil {
		// Systems say a link where O_NOFOLLOW stopped in different ways:
		// ELOOP, EMLINK on FreeBSD, EFTYPE on NetBSD.
		if info, lerr := os.Lstat(path); lerr == nil && info.Mode()&fs.ModeSymlink != 0 {
			return nil, problems.FileNotRegular.Problem(problems.Params{"path": path})
		}
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := CheckFileInfo(path, info, access); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, problems.FileOverLimit.Problem(problems.Params{"path": path, "limit": limit})
	}
	return data, nil
}
