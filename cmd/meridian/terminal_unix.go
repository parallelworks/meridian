//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// isTerminal reports whether r is a terminal: a file whose terminal
// settings can be read.
func isTerminal(r any) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	_, err := unix.IoctlGetTermios(int(f.Fd()), getTermios) //nolint:gosec // a file descriptor fits an int
	return err == nil
}
