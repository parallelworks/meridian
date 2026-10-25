//go:build !unix

package safepath

import "io/fs"

// Owner returns the user ID of the owner of the file info describes, which
// only Unix systems say.
func Owner(fs.FileInfo) (int, bool) { return 0, false }
