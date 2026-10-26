//go:build darwin || freebsd

package peercred

import (
	"net"

	"golang.org/x/sys/unix"
)

// xucredVersion is XUCRED_VERSION, from <sys/ucred.h>: the version of the
// struct xucred that LOCAL_PEERCRED gives, whose layout UID reads.
const xucredVersion = 0

// UID returns the user of the process at the other end of conn, as
// LOCAL_PEERCRED says it was when it listened or connected. A struct
// xucred of another version is EINVAL, as getpeereid(3) says of one.
func UID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *unix.Xucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return 0, err
	}
	switch {
	case credErr != nil:
		return 0, credErr
	case cred.Version != xucredVersion:
		return 0, unix.EINVAL
	}
	return int(cred.Uid), nil
}
