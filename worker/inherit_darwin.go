//go:build darwin

package worker

import "os"

// closeOnExec marks every descriptor from 3 close-on-exec, one by one, as
// /dev/fd lists them.
func closeOnExec() {
	if !closeOnExecListed("/dev/fd") {
		closeOnExecUpTo()
	}
}

// runningExecutable returns the path of the file the process runs.
func runningExecutable() (string, error) { return os.Executable() }
