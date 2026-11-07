//go:build !linux

package main

import "io/fs"

// changeTime is unknown but on Linux, where a scheduler's login node runs:
// elsewhere a binary's copy is hashed at each check.
func changeTime(fs.FileInfo) (int64, bool) { return 0, false }
