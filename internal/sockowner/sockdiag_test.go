package sockowner

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"slices"
	"syscall"
	"testing"
)

// Linux's netlink header and its inet_diag structs, from
// <linux/netlink.h> and <linux/inet_diag.h>.
var (
	nlmsghdr = layOut(0, 0, u32("nlmsg_len"), u16("nlmsg_type"), u16("nlmsg_flags"), u32("nlmsg_seq"), u32("nlmsg_pid"))
	diagID   = layOut(0, 0, u16("idiag_sport"), u16("idiag_dport"), array("idiag_src", 4, u32("")),
		array("idiag_dst", 4, u32("")), u32("idiag_if"), array("idiag_cookie", 2, u32("")))
	diagReqV2 = layOut(0, 0, u8("sdiag_family"), u8("sdiag_protocol"), u8("idiag_ext"), u8("pad"),
		u32("idiag_states"), sub("id", diagID))
	diagMsg = layOut(0, 0, u8("idiag_family"), u8("idiag_state"), u8("idiag_timer"), u8("idiag_retrans"),
		sub("id", diagID), u32("idiag_expires"), u32("idiag_rqueue"), u32("idiag_wqueue"), u32("idiag_uid"),
		u32("idiag_inode"))
)

// diagAddr returns a's address as an idiag_src or idiag_dst: an IPv4
// address in its first 4 bytes, and an IPv6 one in all 16.
func diagAddr(a []byte) []byte { return append(slices.Clone(a), make([]byte, 16-len(a))...) }

// TestDiagRequest checks the request for a connection's other end that
// NETLINK_SOCK_DIAG is sent: a struct nlmsghdr and inet_diag_req_v2 of
// the socket's four addresses, its own first.
func TestDiagRequest(t *testing.T) {
	if nlmsgHdrLen != nlmsghdr.size || diagReqLen != diagReqV2.size || diagMsgLen != diagMsg.size {
		t.Fatalf("the lengths %d, %d, %d are %d, %d, %d", nlmsgHdrLen, diagReqLen, diagMsgLen, nlmsghdr.size, diagReqV2.size, diagMsg.size)
	}
	l4, r4 := end4.local.Addr().As4(), end4.remote.Addr().As4()
	l6, r6 := end6.local.Addr().As16(), end6.remote.Addr().As16()
	for _, tc := range []struct {
		e        end
		family   uint8
		src, dst []byte
	}{{end4, afInet, l4[:], r4[:]}, {end6, afInet6, l6[:], r6[:]}} {
		hdr := newRecord(nlmsghdr).u32("nlmsg_len", uint32(nlmsghdr.size+diagReqV2.size)).
			u16("nlmsg_type", sockDiagByFamily).u16("nlmsg_flags", nlmFRequest).u32("nlmsg_seq", 7)
		req := newRecord(diagReqV2).u8("sdiag_family", tc.family).u8("sdiag_protocol", ipprotoTCP).
			u32("idiag_states", ^uint32(0)).
			u16be("id.idiag_sport", tc.e.local.Port()).u16be("id.idiag_dport", tc.e.remote.Port()).
			bytes("id.idiag_src", diagAddr(tc.src)).bytes("id.idiag_dst", diagAddr(tc.dst)).
			bytes("id.idiag_cookie", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
		if got, want := diagRequest(tc.e, 7), slices.Concat(hdr.b, req.b); !slices.Equal(got, want) {
			t.Errorf("for %v, the request is\n%x\nwant\n%x", tc.e, got, want)
		}
	}
}

// diagAnswer returns the kernel's answer to request seq with the socket of
// family, in state, whose struct inet_diag_sockid is the end e's, and
// whose owner is uid.
func diagAnswer(seq uint32, family, state uint8, e end, uid uint32) []byte {
	var src, dst []byte
	if family == afInet {
		l, r := e.local.Addr().As4(), e.remote.Addr().As4()
		src, dst = l[:], r[:]
	} else {
		l, r := e.local.Addr().As16(), e.remote.Addr().As16()
		src, dst = l[:], r[:]
	}
	msg := newRecord(diagMsg).u8("idiag_family", family).u8("idiag_state", state).
		u16be("id.idiag_sport", e.local.Port()).u16be("id.idiag_dport", e.remote.Port()).
		bytes("id.idiag_src", diagAddr(src)).bytes("id.idiag_dst", diagAddr(dst)).
		u32("idiag_uid", uid).u32("idiag_inode", 4242)
	// The kernel puts attributes after the message, as INET_DIAG_SHUTDOWN.
	attrs := []byte{5, 0, 8, 0, 0, 0, 0, 0}
	hdr := newRecord(nlmsghdr).u32("nlmsg_len", uint32(nlmsghdr.size+diagMsg.size+len(attrs))).
		u16("nlmsg_type", sockDiagByFamily).u32("nlmsg_seq", seq).u32("nlmsg_pid", 4000)
	return slices.Concat(hdr.b, msg.b, attrs)
}

// diagErrorAnswer returns the kernel's answer to request seq that it
// can't: an NLMSG_ERROR of errno.
func diagErrorAnswer(seq uint32, errno syscall.Errno) []byte {
	body := make([]byte, 4+nlmsghdr.size+diagReqV2.size)
	binary.NativeEndian.PutUint32(body, uint32(-int32(errno)))
	hdr := newRecord(nlmsghdr).u32("nlmsg_len", uint32(nlmsghdr.size+len(body))).u16("nlmsg_type", nlmsgError).u32("nlmsg_seq", seq)
	return slices.Concat(hdr.b, body)
}

// TestDiagReply checks what the kernel's answer says of the owner of a
// connection's other end: its idiag_uid, for the socket of the
// connection's four addresses that a process has accepted, IPv4, IPv6, or
// IPv4 accepted by a socket of IPv6; none yet for a request socket, whose
// handshake hasn't finished, nor for a socket no process has accepted,
// whose owners the kernel gives as 0, whoever listens; and an error for a
// socket in any other state, as one closed, in
// TIME_WAIT, whose owner it gives as 0 too, for any other socket, an
// answer to another request, an error the kernel answers with, and an
// answer cut short.
func TestDiagReply(t *testing.T) {
	unaccepted := diagAnswer(1, afInet, tcpEstablished, end4, 0)
	binary.NativeEndian.PutUint32(unaccepted[nlmsghdr.size+diagMsg.off("idiag_inode"):], 0)
	for _, tc := range []struct {
		name   string
		answer []byte
		e      end
		uid    uint32
		err    error
	}{
		{"IPv4", diagAnswer(1, afInet, tcpEstablished, end4, 501), end4, 501, nil},
		{"IPv6", diagAnswer(1, afInet6, tcpEstablished, end6, 501), end6, 501, nil},
		{"IPv4 accepted by IPv6", diagAnswer(1, afInet6, tcpEstablished, end4, 501), end4, 501, nil},
		{"root's", diagAnswer(1, afInet, tcpEstablished, end4, 0), end4, 0, nil},
		{"a request socket", diagAnswer(1, afInet, tcpSynRecv, end4, 0), end4, 0, ErrNotYet},
		{"a socket no process has accepted", unaccepted, end4, 0, ErrNotYet},
		// The kernel has no socket of the connection's, as one answered
		// with a SYN cookie hasn't until its handshake's last packet.
		{"its listener", diagAnswer(1, afInet, tcpListen, end{local: end4.local, remote: netip.AddrPortFrom(netip.IPv4Unspecified(), 0)}, 501), end4, 0, errUnlisted},
		{"its listener, at any address", diagAnswer(1, afInet, tcpListen, end{local: netip.AddrPortFrom(netip.IPv4Unspecified(), end4.local.Port()), remote: netip.AddrPortFrom(netip.IPv4Unspecified(), 0)}, 501), end4, 0, errUnlisted},
		{"its listener, of IPv6 and IPv4", diagAnswer(1, afInet6, tcpListen, end{local: netip.AddrPortFrom(netip.IPv6Unspecified(), end4.local.Port()), remote: netip.AddrPortFrom(netip.IPv6Unspecified(), 0)}, 501), end4, 0, errUnlisted},
		{"no socket", diagErrorAnswer(1, syscall.ENOENT), end4, 0, errUnlisted},
	} {
		if uid, err := diagReply(tc.answer, tc.e, 1); uid != tc.uid || !errors.Is(err, tc.err) || (tc.err == nil) != (err == nil) {
			t.Errorf("%s: user %d, %v; want %d, %v", tc.name, uid, err, tc.uid, tc.err)
		}
	}
	refused := map[string][]byte{
		"in TIME_WAIT":               diagAnswer(1, afInet, 6, end4, 0),
		"in FIN_WAIT2":               diagAnswer(1, afInet, 5, end4, 0),
		"another port's listener":    diagAnswer(1, afInet, tcpListen, end{local: netip.AddrPortFrom(end4.local.Addr(), end4.local.Port()+1), remote: netip.AddrPortFrom(netip.IPv4Unspecified(), 0)}, 501),
		"another address's listener": diagAnswer(1, afInet, tcpListen, end{local: netip.AddrPortFrom(end4.local.Addr().Next(), end4.local.Port()), remote: netip.AddrPortFrom(netip.IPv4Unspecified(), 0)}, 501),
		"a listener, connected":      diagAnswer(1, afInet, tcpListen, end4, 501),
		"permission denied":          diagErrorAnswer(1, syscall.EACCES),
		"of IPv6 for IPv4":           diagAnswer(1, afInet6, tcpEstablished, end6, 501),
		"another request's":          diagAnswer(2, afInet, tcpEstablished, end4, 501),
		"an acknowledgement":         diagErrorAnswer(1, 0),
		"of another type":            newRecord(nlmsghdr).u32("nlmsg_len", uint32(nlmsghdr.size)).u16("nlmsg_type", 3).u32("nlmsg_seq", 1).b,
		"longer than its message":    diagAnswer(1, afInet, tcpEstablished, end4, 501)[:nlmsgHdrLen+diagMsgLen-1],
	}
	for _, o := range others(end4) {
		refused["of "+o.local.String()+" from "+o.remote.String()] = diagAnswer(1, afInet, tcpEstablished, o, 501)
	}
	for name, answer := range refused {
		if uid, err := diagReply(answer, end4, 1); err == nil || errors.Is(err, ErrNotYet) || errors.Is(err, errUnlisted) || errors.Is(err, ErrUnsupported) {
			t.Errorf("%s: user %d, %v; want an error", name, uid, err)
		}
	}
	if _, err := diagReply(diagErrorAnswer(1, syscall.ENOENT), end4, 1); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("an NLMSG_ERROR of ENOENT is %v", err)
	}
	answer := diagAnswer(1, afInet, tcpEstablished, end4, 501)
	for n := range nlmsgHdrLen + diagMsgLen {
		if uid, err := diagReply(answer[:n], end4, 1); err == nil {
			t.Errorf("cut short at %d bytes: user %d", n, uid)
		}
	}
}
