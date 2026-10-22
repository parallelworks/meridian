//go:build !unix

package local

import "io/fs"

// checkOwner checks that the file info describes belongs to the user,
// which only Unix systems say.
func checkOwner(string, fs.FileInfo) error { return nil }
