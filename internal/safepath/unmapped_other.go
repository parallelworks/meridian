//go:build !linux

package safepath

// Unmapped says whether uid, a file's owner, is one a user namespace
// hides, which only Linux has.
func Unmapped(int) bool { return false }
