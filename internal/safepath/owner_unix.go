//go:build unix

package safepath

import (
	"io/fs"
	"syscall"
)

// Owner returns the user ID of the owner of the file info describes.
func Owner(info fs.FileInfo) (int, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}

// openFlags are the flags OpenFile opens a file with besides O_RDONLY: not
// through a symbolic link, and without waiting for a FIFO's writer, so that
// the file's info says what it is first. Reading a regular file ignores
// O_NONBLOCK.
const openFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK
