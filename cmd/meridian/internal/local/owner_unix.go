//go:build unix

package local

import (
	"io/fs"
	"syscall"
)

// fileOwner returns the user ID of the owner of the file info describes.
func fileOwner(info fs.FileInfo) (int, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}

// openFlags are the flags a config file, a key file or a variable's file
// is opened with besides O_RDONLY: O_NONBLOCK, so that opening a FIFO
// doesn't wait for a writer, and its file info says what it is first.
// Reading a regular file ignores it.
const openFlags = syscall.O_NONBLOCK
