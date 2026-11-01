package sockowner

import (
	"encoding/binary"
	"fmt"
)

// darwinList is the sysctl that lists macOS's TCP sockets, as netstat
// reads them.
const darwinList = "net.inet.tcp.pcblist_n"

// The records of darwinList, as XNU's <netinet/in_pcb.h>,
// <sys/socketvar.h> and <netinet/tcp_var.h> lay them out, under #pragma
// pack(4), in the kernel's byte order, but for ports and addresses, which
// are in network order (bsd/netinet/in_pcblist.c, get_pcblist_n). The
// list starts with a struct xinpgen, and, unless it has no socket, ends
// with another. Between, each socket has a struct xinpcb_n, xsocket_n,
// two xsockbuf_n, its receive and send buffers', xsockstat_n and
// xtcpcb_n, in that order, each at a multiple of 8 bytes from the one
// before, and each starting with its length and its kind, XSO_*. A
// socket the kernel no longer has, or a flow of a TCP stack outside it,
// has a struct xsocket_n of zeros but for its length, its kind and its
// protocol, whose so_uid, 0, is no owner's; and a buffer's record of such
// a socket can be of kind 0.
const (
	darwinGenLen = 24 // sizeof(struct xinpgen)
	darwinGenGen = 8  // struct xinpgen's xig_gen

	// struct xinpcb_n's ports, its vflag and its addresses: its foreign
	// and local address unions, each as struct in6_addr, or as struct
	// in_addr_4in6, whose IPv4 address is in its last 4 bytes.
	darwinInpFport = 16
	darwinInpLport = 18
	darwinInpVflag = 44
	darwinInpFaddr = 48
	darwinInpLaddr = 64

	// struct xsocket_n's handle of its socket, xso_so, 0 for none; the
	// socket's protocol and family; and its owner.
	darwinSoHandle   = 8
	darwinSoProtocol = 36
	darwinSoFamily   = 40
	darwinSoUID      = 64

	darwinInpIPv4 = 0x1 // INP_IPV4
	darwinInpIPv6 = 0x2 // INP_IPV6
	darwinAFInet  = 2   // AF_INET
	darwinAFInet6 = 30  // AF_INET6, on macOS
)

// darwinRecords are the records of each socket in darwinList, in order:
// each one's kind and its length.
var darwinRecords = [...]struct {
	kind uint32
	len  int
}{
	{0x010, 104}, // XSO_INPCB, struct xinpcb_n
	{0x001, 104}, // XSO_SOCKET, struct xsocket_n
	{0x002, 32},  // XSO_RCVBUF, struct xsockbuf_n
	{0x004, 32},  // XSO_SNDBUF, struct xsockbuf_n
	{0x008, 136}, // XSO_STATS, struct xsockstat_n
	{0x020, 204}, // XSO_TCPCB, struct xtcpcb_n
}

// darwinOwner returns the owner of the socket whose end is e, as list,
// what darwinList gives, says: the so_uid of the one socket whose local
// and foreign addresses and ports are e's; errUnlisted where there is
// none, as for a connection its listener answered with a SYN cookie,
// whose socket XNU makes only as the handshake's last packet comes,
// rather than with its first, or where there are several, of a list that
// changed as it was read; and an error for a list with a record whose
// kind or length isn't the one Owner knows of the record in its place, or
// that ends in the middle of one.
func darwinOwner(list []byte, e end) (uint32, error) {
	ne, be := binary.NativeEndian, binary.BigEndian
	bad := func(what string, offset int) error { return errUnknownRecord{darwinList, what, offset} }
	if len(list) < darwinGenLen || ne.Uint32(list) != darwinGenLen {
		return 0, bad("a header of another length", 0)
	}
	var candidates []uint32
	sockets, ended, changed := 0, false, false
	for off := darwinGenLen; off < len(list); sockets++ {
		if len(list)-off >= 4 && ne.Uint32(list[off:]) == darwinGenLen {
			if off+darwinGenLen != len(list) {
				return 0, bad("a struct xinpgen before the list's end", off)
			}
			ended = true
			changed = ne.Uint64(list[off+darwinGenGen:]) != ne.Uint64(list[darwinGenGen:])
			break
		}
		start := off
		var records [len(darwinRecords)][]byte
		for i, want := range darwinRecords {
			if len(list)-off < 8 {
				return 0, bad("the list's end in the middle of a socket", off)
			}
			n, kind := int(ne.Uint32(list[off:])), ne.Uint32(list[off+4:])
			switch {
			case n != want.len || kind != want.kind && (kind != 0 || i != 2 && i != 3):
				return 0, bad(fmt.Sprintf("a record of %d bytes and kind %#x, in place of %d bytes and kind %#x", n, kind, want.len, want.kind), off)
			case off+roundUp8(n) > len(list):
				return 0, bad("the list's end in the middle of a record", off)
			}
			records[i] = list[off : off+n]
			off += roundUp8(n)
		}
		inp, so := records[0], records[1]
		if ne.Uint64(so[darwinSoHandle:]) == 0 {
			// No socket: no owner.
			continue
		}
		if ne.Uint32(records[2][4:]) == 0 || ne.Uint32(records[3][4:]) == 0 {
			return 0, bad("a socket's buffer of kind 0", start)
		}
		family := ne.Uint32(so[darwinSoFamily:])
		if ne.Uint32(so[darwinSoProtocol:]) != ipprotoTCP || family != darwinAFInet && family != darwinAFInet6 {
			continue
		}
		lport, fport := be.Uint16(inp[darwinInpLport:]), be.Uint16(inp[darwinInpFport:])
		laddr, faddr := inp[darwinInpLaddr:darwinInpLaddr+16], inp[darwinInpFaddr:darwinInpFaddr+16]
		vflag := inp[darwinInpVflag]
		if vflag&darwinInpIPv4 != 0 && e.matches(false, lport, fport, laddr, faddr) ||
			vflag&darwinInpIPv6 != 0 && e.matches(true, lport, fport, laddr, faddr) {
			candidates = append(candidates, ne.Uint32(so[darwinSoUID:]))
		}
	}
	if sockets > 0 && !ended {
		return 0, bad("the list's end without a struct xinpgen", len(list))
	}
	return ownerOf(darwinList, candidates, false, changed)
}
