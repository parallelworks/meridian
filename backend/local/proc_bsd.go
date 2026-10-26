//go:build darwin || freebsd

package local

import (
	"errors"
	"syscall"
)

// runningExecutable returns the path that starts the file this process
// runs: exe, which no other user can change.
func runningExecutable(exe string) (string, error) { return exe, nil }

// signalWorker sends sig to r's worker, once it has checked that the
// process of its ID is r's.
func signalWorker(r *record, sig syscall.Signal) error {
	if alive, err := r.alive(); err != nil {
		return err
	} else if !alive {
		return errNoProcess
	}
	err := syscall.Kill(r.PID, sig)
	if errors.Is(err, syscall.ESRCH) {
		return errNoProcess
	}
	return err
}
