//go:build !linux

package fileid

import "io/fs"

// Of is unknown but on Linux, where the file a placement starts, and
// hashes, is the one the process runs, which nothing can write while it
// runs, and a scheduler's login node runs: elsewhere a file is hashed each
// time, since a file system whose times are as coarse as two seconds, as
// FAT's, can keep its times through a change.
func Of(fs.FileInfo) (ID, bool) { return ID{}, false }
