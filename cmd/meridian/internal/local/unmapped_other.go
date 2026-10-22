//go:build !linux

package local

// unmappedOwner says whether uid, a file's owner, is one a user namespace
// hides, which only Linux has.
func unmappedOwner(int) bool { return false }
