//go:build unix && !linux

package worker

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// checkPeer checks, where the system doesn't say who serves a Unix
// socket, that no other user could have put one at its path: its
// directory must be the worker's user's, and writable by no one else.
func checkPeer(_ *net.UnixConn, path string) error {
	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return err
	}
	if !ownedByUser(info) || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("worker: the coordinator's socket %s must be in a directory of this user's that no one else can write to", path)
	}
	return nil
}

// harden does nothing where the system has no prctl.
func harden() error { return nil }
