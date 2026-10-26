//go:build !linux && !darwin && !freebsd

package worker

import "os"

// closeOnExec does nothing where the worker doesn't run, which it refuses
// before it starts anything.
func closeOnExec() {}

// runningExecutable returns the path of the file the process runs.
func runningExecutable() (string, error) { return os.Executable() }
