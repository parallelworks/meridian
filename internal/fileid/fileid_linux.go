package fileid

import (
	"io/fs"
	"syscall"
)

// Of returns the ID of the file info describes, as fstat gave it: its
// device and inode, its size, and when its data and its status last
// changed.
func Of(info fs.FileInfo) (ID, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ID{}, false
	}
	return ID{
		dev: uint64(st.Dev), ino: uint64(st.Ino), size: st.Size, //nolint:unconvert // the types differ by system and architecture
		mtime: st.Mtim.Nano(), ctime: st.Ctim.Nano(),
	}, true
}
