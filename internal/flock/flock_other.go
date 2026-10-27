//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package flock

import "context"

// Lock returns ErrUnsupported: the system has no file locks that package
// syscall offers.
func Lock(context.Context, string) (func(), error) { return nil, ErrUnsupported }

// TryLock returns ErrUnsupported, as Lock does.
func TryLock(string) (func(), error) { return nil, ErrUnsupported }
