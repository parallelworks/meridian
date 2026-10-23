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
