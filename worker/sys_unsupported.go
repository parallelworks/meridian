//go:build unix && !linux && !darwin && !freebsd && !openbsd

package worker

import (
	"net"
	"runtime"

	"github.com/parallelworks/meridian/problems"
)

// A worker runs on a Unix system only where it can keep the user's other
// processes from reading its memory, where its credential is, and the
// system says which user serves a Unix socket: Linux, macOS, FreeBSD and
// OpenBSD. Elsewhere, as on NetBSD, which says who serves a socket but
// gives a process no way to keep the user's others from attaching to it,
// it refuses to run, before it claims the attempt.

func checkPeer(*net.UnixConn, string) error { return systemUnsupported() }

func harden() error { return systemUnsupported() }

func systemUnsupported() error {
	return problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
}
