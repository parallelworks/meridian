//go:build !linux && !darwin && !freebsd

package harden

// Process returns ErrUnsupported: the system gives a process no way to
// keep the user's other processes from tracing it.
func Process() error { return ErrUnsupported }
