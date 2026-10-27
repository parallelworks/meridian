// Package flock takes exclusive locks on files, which keep out other
// processes that lock the same file until the process that holds one
// releases it or ends, as flock(2) does. Each lock is of the file's open
// file description, so two locks on one file in the same process exclude
// each other too.
package flock

import "errors"

// ErrUnsupported is what Lock and TryLock return on a system without the
// file locks that package syscall offers.
var ErrUnsupported = errors.New("flock: this system has no file locks")

// ErrHeld is what TryLock returns for a file whose lock another holds.
var ErrHeld = errors.New("flock: another holds the lock")
