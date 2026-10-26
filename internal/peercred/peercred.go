// Package peercred asks the system which user is at the other end of a
// Unix socket, so that Meridian sends a token or a credential, or takes a
// request, only from its own user or root: the worker checks who serves
// the coordinator's socket, and the coordinator who connects to it.
//
// The system says who the process ran as when it connected or listened:
// SO_PEERCRED on Linux, and LOCAL_PEERCRED on macOS and FreeBSD. NetBSD
// and OpenBSD can say too, but Meridian's worker doesn't run there, since
// they give a process no way to keep the user's other processes from
// tracing it; so UID refuses them, as it does every other system, with
// ErrUnsupported, and the caller refuses the socket.
package peercred

import (
	"errors"
	"os"
)

// ErrUnsupported is UID's error on a system where Meridian doesn't ask who
// is at the other end of a socket. It wraps errors.ErrUnsupported.
var ErrUnsupported = errUnsupported{}

type errUnsupported struct{}

func (errUnsupported) Error() string        { return "peercred: not supported on this system" }
func (errUnsupported) Is(target error) bool { return target == errors.ErrUnsupported }

// Trusted reports whether uid is one Meridian trusts at the other end of a
// socket: the process's own user, or root.
func Trusted(uid int) bool {
	return uid == os.Getuid() || uid == 0
}
