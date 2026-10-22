//go:build !linux && !darwin

package local

// networkFS says whether path is on a network filesystem, which only Linux
// and macOS are asked: elsewhere it says no.
func networkFS(string) (fsType string, network bool, err error) { return "", false, nil }
