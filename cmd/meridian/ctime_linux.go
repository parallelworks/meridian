package main

import (
	"io/fs"
	"syscall"
)

// changeTime returns when the status of the file info describes last
// changed, in nanoseconds, as fstat says it.
func changeTime(info fs.FileInfo) (int64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Ctim.Nano(), true
}
