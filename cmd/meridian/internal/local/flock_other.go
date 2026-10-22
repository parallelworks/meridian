//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package local

import "context"

// retryContention says whether Sources tries a git command again when
// another process held a lock against it. Here there is no file lock to
// keep other processes out, so it does.
const retryContention = true

// lockFile locks nothing: the system has no file locks that package
// syscall offers.
func lockFile(context.Context, string) (func(), error) { return func() {}, nil }
