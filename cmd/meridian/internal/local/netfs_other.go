//go:build !linux && !darwin && !freebsd && !dragonfly && !openbsd

package local

// networkFS says whether path is on a network filesystem, which only Linux,
// macOS and some BSDs are asked: elsewhere it says no.
func networkFS(string) (fsType string, network bool, err error) { return "", false, nil }
