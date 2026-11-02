//go:build linux

package sockowner

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// diagSeq numbers each request, which goes on a socket of its own.
const diagSeq = 1

// rootAccepts is set where the owner Owner says is the user of the process
// that accepted the connection, as on Linux, so that root's is a process
// of root's (Trusted).
const rootAccepts = true

// lookup asks NETLINK_SOCK_DIAG for the TCP socket whose end is e, alone,
// and returns its owner, as diagReply reads it. The kernel answers as it
// is asked, before the request's send returns, so the receive waits a
// second only where something is wrong.
func lookup(e end) (uint32, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, unix.NETLINK_SOCK_DIAG)
	if err != nil {
		return 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG: %w", err)
	}
	defer unix.Close(fd)
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1}); err != nil {
		return 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG: %w", err)
	}
	req := diagRequest(e, diagSeq)
	if err := retried(func() error { return unix.Sendto(fd, req, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}) }); err != nil {
		return 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG: %w", err)
	}
	// One socket's message is a few hundred bytes. MSG_TRUNC has the
	// receive say how long the message was, should it be longer.
	buf := make([]byte, 8192)
	var (
		n    int
		from unix.Sockaddr
	)
	if err := retried(func() (err error) {
		n, from, err = unix.Recvfrom(fd, buf, unix.MSG_TRUNC)
		return err
	}); err != nil {
		return 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG: %w", err)
	}
	switch nl, ok := from.(*unix.SockaddrNetlink); {
	case !ok || nl.Pid != 0:
		return 0, errors.New("sockowner: NETLINK_SOCK_DIAG's answer came from another process than the kernel")
	case n > len(buf):
		return 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG answered with %d bytes, more than a socket's", n)
	}
	return diagReply(buf[:n], e, diagSeq)
}

// retried calls f again for as long as a signal interrupts it.
func retried(f func() error) error {
	for {
		if err := f(); !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}
