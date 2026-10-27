//go:build !linux

package local

import "io/fs"

// identity is unknown but on Linux, where the file a placement starts, and
// hashes, is the one the process runs, which nothing can write while it
// runs: elsewhere the file at the path is hashed at each placement, since
// a file system whose times are as coarse as two seconds, as FAT's, can
// keep its times through a change.
func identity(fs.FileInfo) (fileIdentity, bool) { return fileIdentity{}, false }
