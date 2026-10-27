//go:build unix && !linux && !darwin && !freebsd

package worker

import (
	"runtime"

	"github.com/parallelworks/meridian/problems"
)

// A worker runs on a Unix system only where it can keep the user's other
// processes from reading its memory, where its credential is, and the
// system says which user serves a Unix socket: Linux, macOS and FreeBSD.
// Elsewhere it refuses to run, before it claims the attempt: NetBSD and
// OpenBSD say who serves a socket, but give a process no way to keep the
// user's others from tracing it, and OpenBSD's ktrace records what a
// process it traces writes, the worker's requests among them, whatever
// kern.global_ptrace says.

func systemUnsupported() error {
	return problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
}
