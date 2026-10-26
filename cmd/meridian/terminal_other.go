//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package main

// isTerminal reports whether r is a terminal, which meridian can't tell on
// this system, so it says no: an interrupt then leaves a run running,
// rather than ask what to do.
func isTerminal(any) bool { return false }
