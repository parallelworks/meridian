//go:build unix

package worker

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// ownedByUser reports whether a file belongs to the worker's user.
func ownedByUser(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}

// peerAllowed reports whether the worker sends a token or a credential to
// a coordinator's socket served by the user uid: its own user, or root.
func peerAllowed(uid int) bool {
	return uid == os.Getuid() || uid == 0
}

// checkSocketDir checks, where the system doesn't say who serves a Unix
// socket, that no other user could have put one at its path: its
// directory must be the worker's user's, and writable by no one else.
func checkSocketDir(path string) error {
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return problems.FileUnreadable.Problem(problems.Params{"path": dir, "reason": err.Error()})
	}
	if !ownedByUser(info) || info.Mode().Perm()&0o022 != 0 {
		return problems.PathUnsafe.Problem(problems.Params{"path": path, "dir": dir})
	}
	return nil
}

// The flags the worker opens a file a step could have replaced with: not
// through a symbolic link, and without waiting for a FIFO's writer.
const (
	openNoFollow = syscall.O_NOFOLLOW
	openNonBlock = syscall.O_NONBLOCK
)
