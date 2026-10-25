//go:build !unix

package safepath

import "io/fs"

// Owner returns the user ID of the owner of the file info describes, which
// only Unix systems say.
func Owner(fs.FileInfo) (int, bool) { return 0, false }

// openFlags are the flags OpenFile opens a file with besides O_RDONLY.
// It opens none here: Check refuses every path, since nothing says who
// owns it.
const openFlags = 0
