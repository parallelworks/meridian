//go:build !unix

package local

import "io/fs"

// fileOwner returns the user ID of the owner of the file info describes,
// which only Unix systems say.
func fileOwner(fs.FileInfo) (int, bool) { return 0, false }

// openFlags are the flags a file is opened with besides O_RDONLY.
const openFlags = 0
