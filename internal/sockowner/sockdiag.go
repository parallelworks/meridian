package sockowner

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"syscall"
)

// What NETLINK_SOCK_DIAG is asked and answers with, from Linux's
// <linux/netlink.h>, <linux/sock_diag.h>, <linux/inet_diag.h> and
// <netinet/tcp.h>: defined here, rather than taken from
// golang.org/x/sys/unix, so that the requests are made, and the answers
// read, and tested, on every system.
const (
	nlmsgHdrLen      = 16         // sizeof(struct nlmsghdr)
	nlmsgError       = 2          // NLMSG_ERROR
	nlmFRequest      = 1          // NLM_F_REQUEST
	sockDiagByFamily = 20         // SOCK_DIAG_BY_FAMILY
	afInet           = 2          // AF_INET
	afInet6          = 10         // AF_INET6, on Linux
	ipprotoTCP       = 6          // IPPROTO_TCP
	diagReqLen       = 56         // sizeof(struct inet_diag_req_v2)
	diagMsgLen       = 72         // sizeof(struct inet_diag_msg)
	diagNoCookie     = ^uint32(0) // INET_DIAG_NOCOOKIE
	tcpEstablished   = 1          // TCP_ESTABLISHED
	tcpListen        = 10         // TCP_LISTEN
	enoent           = 2          // ENOENT
	// tcpSynRecv is TCP_SYN_RECV, the state the kernel shows a request
	// socket in, whose handshake it hasn't finished, and whose owner it
	// gives as 0, whoever listens.
	tcpSynRecv = 3
)

// diagRequest returns a SOCK_DIAG_BY_FAMILY request, numbered seq, for the
// TCP socket whose end is e: a struct nlmsghdr and a struct
// inet_diag_req_v2 with e's four addresses, without NLM_F_DUMP, so that
// the kernel looks the socket up alone, as it would a packet's, and
// answers with it, or with an error. The kernel reads the header, the
// family, the protocol and the states in its own byte order, and the
// addresses and ports in network order.
func diagRequest(e end, seq uint32) []byte {
	b := make([]byte, nlmsgHdrLen+diagReqLen)
	ne := binary.NativeEndian
	ne.PutUint32(b[0:], nlmsgHdrLen+diagReqLen)
	ne.PutUint16(b[4:], sockDiagByFamily)
	ne.PutUint16(b[6:], nlmFRequest)
	ne.PutUint32(b[8:], seq)
	// nlmsg_pid, at 12, is 0, the kernel's.
	r := b[nlmsgHdrLen:]
	r[0] = afInet6
	if e.local.Addr().Is4() {
		r[0] = afInet
	}
	r[1] = ipprotoTCP
	// idiag_ext and pad are 0: no extensions are asked for.
	ne.PutUint32(r[4:], ^uint32(0)) // idiag_states: any
	// struct inet_diag_sockid.
	id := r[8:]
	binary.BigEndian.PutUint16(id[0:], e.local.Port())
	binary.BigEndian.PutUint16(id[2:], e.remote.Port())
	putDiagAddr(id[4:20], e.local.Addr())
	putDiagAddr(id[20:36], e.remote.Addr())
	// idiag_if, at 36, is 0: any interface.
	ne.PutUint32(id[40:], diagNoCookie)
	ne.PutUint32(id[44:], diagNoCookie)
	return b
}

// putDiagAddr writes a to b, an idiag_src or idiag_dst: an IPv4 address in
// its first 4 bytes, and an IPv6 one in all 16.
func putDiagAddr(b []byte, a netip.Addr) {
	if a.Is4() {
		a4 := a.As4()
		copy(b, a4[:])
		return
	}
	a16 := a.As16()
	copy(b, a16[:])
}

// diagSeq numbers each request, which goes on a socket of its own.
const diagSeq = 1

// diagOwner returns the owner of the socket whose end is e, as the kernel
// answers the requests that ask sends it. Once an answer says that a
// process has accepted the socket, it asks again, and returns the owner
// that the second answer gives, of the same socket, by its inode; of any
// other answer, it returns what diagReply says.
//
// The kernel reads a socket's owner, and then its inode, apart, as it
// answers (inet_sk_diag_fill), and a process can accept the socket
// between the two. The answer then has the inode of the accepting
// process's socket, but the owner from before the accept: root's
// (sock_i_uid, before Linux 6.17) or the listener's (sk_uid, since),
// whoever accepted it. The second answer's owner is read after the first
// answer's inode, so after the accept. Each socket is asked about twice,
// rather than only one the first answer gives as root's, because the
// listener's owner can be this user's, as for a listener this user made
// and passed to another user's program.
func diagOwner(e end, ask func(req []byte) ([]byte, error)) (uint32, error) {
	answer := func() (uid, inode uint32, err error) {
		b, err := ask(diagRequest(e, diagSeq))
		if err != nil {
			return 0, 0, err
		}
		return diagReply(b, e, diagSeq)
	}
	_, accepted, err := answer()
	if err != nil {
		return 0, err
	}
	uid, inode, err := answer()
	switch {
	case err != nil:
		return 0, err
	case inode != accepted:
		return 0, fmt.Errorf("sockowner: the socket at the connection's other end was of inode %d, and then of %d", accepted, inode)
	}
	return uid, nil
}

// diagReply returns the owner of the socket whose end is e, and its
// inode, from the kernel's answer, b, to the request numbered seq that
// asked for it: a struct nlmsghdr and a struct inet_diag_msg, whose
// idiag_uid is the owner, for a socket of e's four addresses that a
// process has accepted (TCP_ESTABLISHED, of an inode), though it may be
// the owner from before the accept (diagOwner); ErrNotYet for a request
// socket, in the middle of its handshake, or one no process has accepted
// yet; errUnlisted where the kernel has no socket of e's, which it
// answers with ENOENT, or with the listener at e's local address, as it
// looks a packet's socket up, as for a connection answered with a SYN
// cookie, whose socket comes with the handshake's last packet; and an
// error for a socket in any other state, as a closed one, whose owner the
// kernel gives as 0, and for any other socket, or another error the
// kernel answered with.
func diagReply(b []byte, e end, seq uint32) (uid, inode uint32, err error) {
	ne := binary.NativeEndian
	if len(b) < nlmsgHdrLen {
		return 0, 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG answered with %d bytes, too few for a message", len(b))
	}
	n, typ, got := ne.Uint32(b[0:]), ne.Uint16(b[4:]), ne.Uint32(b[8:])
	switch {
	case n < nlmsgHdrLen || uint64(n) > uint64(len(b)):
		return 0, 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG's answer of %d bytes says it has %d", len(b), n)
	case got != seq:
		return 0, 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG answered request %d, not %d", got, seq)
	}
	body := b[nlmsgHdrLen:n]
	switch typ {
	case nlmsgError:
		if len(body) < 4 {
			return 0, 0, errors.New("sockowner: NETLINK_SOCK_DIAG answered with an error too short to read")
		}
		// struct nlmsgerr's error: an errno, negated, or 0 for none.
		switch code := ne.Uint32(body); {
		case -code == enoent:
			return 0, 0, fmt.Errorf("%w: NETLINK_SOCK_DIAG: %w", errUnlisted, syscall.Errno(-code))
		case code != 0:
			return 0, 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG: %w", syscall.Errno(-code))
		}
		return 0, 0, errors.New("sockowner: NETLINK_SOCK_DIAG answered with no socket")
	case sockDiagByFamily:
	default:
		return 0, 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG answered with a message of type %d", typ)
	}
	if len(body) < diagMsgLen {
		return 0, 0, fmt.Errorf("sockowner: NETLINK_SOCK_DIAG answered with %d bytes of a socket, too few", len(body))
	}
	if body[1] == tcpListen && e.isDiagListener(body[0], body[4:52]) {
		return 0, 0, fmt.Errorf("%w: NETLINK_SOCK_DIAG answered with its listener", errUnlisted)
	}
	if !e.isDiagID(body[0], body[4:52]) {
		return 0, 0, errors.New("sockowner: NETLINK_SOCK_DIAG answered with another socket than the connection's other end")
	}
	// A socket no process has accepted has no inode, idiag_inode, and the
	// kernel gives its owner as root, whoever listens, or, since Linux
	// 6.17, as whoever made the listener.
	switch state, ino := body[1], ne.Uint32(body[68:]); {
	case state == tcpEstablished && ino != 0:
		return ne.Uint32(body[64:]), ino, nil
	case state == tcpEstablished, state == tcpSynRecv:
		return 0, 0, ErrNotYet
	default:
		return 0, 0, fmt.Errorf("sockowner: the socket at the connection's other end is in TCP state %d, whose owner Linux doesn't say", state)
	}
}

// isDiagListener reports whether a socket of family, whose struct
// inet_diag_sockid is id, is a listener at e's local port, as the kernel
// answers for a connection it has no socket of: of no remote port, and of
// e's local address, or none, any address of its family.
func (e end) isDiagListener(family byte, id []byte) bool {
	be := binary.BigEndian
	if be.Uint16(id[0:]) != e.local.Port() || be.Uint16(id[2:]) != 0 {
		return false
	}
	src := id[4:20]
	unspecified := make([]byte, 16)
	switch {
	case family == afInet && e.local.Addr().Is4():
		l := e.local.Addr().As4()
		return string(src[:4]) == string(l[:]) || string(src) == string(unspecified)
	case family == afInet6:
		l := e.local.Addr().As16()
		return string(src) == string(l[:]) || string(src) == string(unspecified)
	}
	return false
}

// isDiagID reports whether a socket of family, whose struct
// inet_diag_sockid is id, is e: an IPv6 one whose addresses are e's
// IPv4 ones mapped, as a socket that listens on both an IPv6 address
// and IPv4 accepts an IPv4 connection with, among them.
func (e end) isDiagID(family byte, id []byte) bool {
	be := binary.BigEndian
	if be.Uint16(id[0:]) != e.local.Port() || be.Uint16(id[2:]) != e.remote.Port() {
		return false
	}
	src, dst := id[4:20], id[20:36]
	switch {
	case family == afInet && e.local.Addr().Is4():
		l, r := e.local.Addr().As4(), e.remote.Addr().As4()
		return string(src[:4]) == string(l[:]) && string(dst[:4]) == string(r[:])
	case family == afInet6:
		l, r := e.local.Addr().As16(), e.remote.Addr().As16()
		return string(src) == string(l[:]) && string(dst) == string(r[:])
	}
	return false
}
