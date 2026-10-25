//go:build unix

package worker

import "syscall"

// The flags the worker opens a file a step could have replaced with: not
// through a symbolic link, and without waiting for a FIFO's writer.
const (
	openNoFollow = syscall.O_NOFOLLOW
	openNonBlock = syscall.O_NONBLOCK
)
