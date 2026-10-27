//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flock

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// Lock takes an exclusive lock on the file at path, creating it, readable
// and writable by its user alone, if it isn't there, and returns the
// function that releases it. It waits for a process that holds the lock,
// until ctx ends. It doesn't follow a symbolic link at path.
func Lock(ctx context.Context, path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600) //nolint:gosec // a lock file in one of Meridian's own directories
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

// TryLock takes an exclusive lock on the file at path, as Lock does, but
// doesn't wait: a lock another holds is ErrHeld. The function it returns
// removes the file, and then releases the lock, so that a lock of its own
// for each of many things leaves no file behind once released. A lock
// taken on a file that its holder removed meanwhile, which no one else
// would find, is let go, and taken on the file at path instead.
func TryLock(path string) (release func(), err error) {
	for {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600) //nolint:gosec // a lock file in one of Meridian's own directories
		if err != nil {
			return nil, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) //nolint:gosec // a file descriptor fits an int
		switch {
		case errors.Is(err, syscall.EWOULDBLOCK):
			f.Close()
			return nil, ErrHeld
		case errors.Is(err, syscall.EINTR):
			f.Close()
			continue
		case err != nil:
			f.Close()
			return nil, err
		}
		held, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		there, err := os.Lstat(path)
		if err == nil && os.SameFile(held, there) {
			return func() {
				_ = os.Remove(path)
				f.Close()
			}, nil
		}
		f.Close()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
}
