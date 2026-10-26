//go:build !linux && !darwin && !freebsd

package peercred

import "net"

// UID returns ErrUnsupported: Meridian asks who is at the other end of a
// socket only on Linux, macOS and FreeBSD.
func UID(*net.UnixConn) (int, error) { return 0, ErrUnsupported }
