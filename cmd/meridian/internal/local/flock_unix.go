//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package local

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// retryContention says whether Sources tries a git command again when
// another process held a lock against it. Here lockFile keeps other
// processes out, so it doesn't.
const retryContention = false

// lockFile takes an exclusive lock on the file at path, creating it, which
// keeps out other processes that lock it, until the function it returns is
// called. It waits for a process that holds it, until ctx ends.
func lockFile(ctx context.Context, path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600) //nolint:gosec // a lock file in Meridian's cache directory
	if err != nil {
		return nil, err
	}
	fd := int(f.Fd()) //nolint:gosec // a file descriptor fits an int
	for wait := 10 * time.Millisecond; ; wait = min(2*wait, 500*time.Millisecond) {
		err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// Closing the file releases the lock.
			return func() { f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
