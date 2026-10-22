//go:build unix

package worker

import (
	"os"
	"syscall"
)

// ownedByUser reports whether a file belongs to the worker's user.
func ownedByUser(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Getuid()
}

// The flags the worker opens a file a step could have replaced with: not
// through a symbolic link, and without waiting for a FIFO's writer.
const (
	openNoFollow = syscall.O_NOFOLLOW
	openNonBlock = syscall.O_NONBLOCK
)
