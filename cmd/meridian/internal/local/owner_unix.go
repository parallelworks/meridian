//go:build unix

package local

import (
	"io/fs"
	"os"
	"syscall"
)

// checkOwner checks that the file info describes belongs to the user.
func checkOwner(_ string, info fs.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if int(st.Uid) != os.Getuid() {
		return errNotOwner
	}
	return nil
}
