//go:build darwin || freebsd

package worker

import (
	"net"

	"golang.org/x/sys/unix"
)

// xucredVersion is XUCRED_VERSION, from <sys/ucred.h>: the version of the
// struct xucred that LOCAL_PEERCRED gives, whose layout the worker reads.
const xucredVersion = 0

// checkPeer checks that the process at the other end of a Unix socket
// runs as the worker's user, or as root, before the worker sends it a
// token or a credential: LOCAL_PEERCRED says who it ran as when it
// listened. A struct xucred of another version is EINVAL, as getpeereid(3)
// says of one.
func checkPeer(conn *net.UnixConn, path string) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return peerUnknown(path, err)
	}
	var cred *unix.Xucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return peerUnknown(path, err)
	}
	switch {
	case credErr != nil:
		return peerUnknown(path, credErr)
	case cred.Version != xucredVersion:
		return peerUnknown(path, unix.EINVAL)
	}
	return checkPeerUID(path, int(cred.Uid))
}
