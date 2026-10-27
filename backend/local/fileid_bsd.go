//go:build darwin || freebsd

package local

import (
	"io/fs"
	"syscall"
)

// identity returns the identity of the file info describes, as fstat gave
// it: its device and inode, its size, and when its data and its status
// last changed.
func identity(info fs.FileInfo) (fileIdentity, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, false
	}
	return fileIdentity{
		dev: uint64(st.Dev), ino: uint64(st.Ino), size: st.Size, //nolint:gosec,unconvert // the types differ by system
		mtime: st.Mtimespec.Nano(), ctime: st.Ctimespec.Nano(),
	}, true
}
