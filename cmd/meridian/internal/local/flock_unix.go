//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package local

import (
	"context"

	"github.com/parallelworks/meridian/internal/flock"
)

// retryContention says whether Sources tries a git command again when
// another process held a lock against it. Here lockFile keeps other
// processes out, so it doesn't.
const retryContention = false

// lockFile takes an exclusive lock on the file at path, creating it, which
// keeps out other processes that lock it, until the function it returns is
// called. It waits for a process that holds it, until ctx ends.
func lockFile(ctx context.Context, path string) (func(), error) {
	return flock.Lock(ctx, path)
}
