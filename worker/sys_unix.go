//go:build unix

package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
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
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return err
	}
	if !ownedByUser(info) || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("worker: the coordinator's socket %s must be in a directory of this user's that no one else can write to", path)
	}
	return nil
}

// The flags the worker opens a file a step could have replaced with: not
// through a symbolic link, and without waiting for a FIFO's writer.
const (
	openNoFollow = syscall.O_NOFOLLOW
	openNonBlock = syscall.O_NONBLOCK
)
