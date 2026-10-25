//go:build unix

package local

import "syscall"

// openFlags are the flags the key file is opened with besides O_RDONLY:
// O_NONBLOCK, so that opening a FIFO doesn't wait for a writer, and its
// file info says what it is first. Reading a regular file ignores it.
const openFlags = syscall.O_NONBLOCK
