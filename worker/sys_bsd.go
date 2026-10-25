//go:build unix && !linux

package worker

import "net"

// checkPeer checks, where the system doesn't say who serves a Unix
// socket, that no other user could have put one at its path
// (checkSocketDir).
func checkPeer(_ *net.UnixConn, path string) error {
	return checkSocketDir(path)
}

// harden does nothing where the system has no prctl.
func harden() error { return nil }
