package sockowner

import (
	"encoding/binary"
	"fmt"
)

// freebsdList is the sysctl that lists FreeBSD's TCP sockets, as netstat
// reads them.
const freebsdList = "net.inet.tcp.pcblist"

// The records of freebsdList, as FreeBSD 13's and 14's
// <netinet/in_pcb.h>, <netinet/tcp_var.h> and <sys/socketvar.h> lay them
// out, in the kernel's byte order, but for ports and addresses, which are
// in network order (sys/netinet/tcp_subr.c, tcp_pcblist). The list starts
// with a struct xinpgen, and ends with another; between, each socket is a
// struct xtcpcb, which holds a struct xinpcb, which holds a struct
// xsocket, each starting with its length, as a ksize_t, the same on every
// architecture. A connection whose handshake the kernel hasn't finished,
// in its syncache, has a struct xsocket without a socket, xso_so 0,
// whose so_uid, 0, is no owner's; one the kernel no longer has a socket
// of, as in TIME_WAIT, a struct xsocket of zeros. What Owner reads comes
// before the first field whose place differs on 32-bit x86, where a
// uint64_t is aligned on 4 bytes, rather than 8.
const (
	freebsdGenLen    = 64  // sizeof(struct xinpgen)
	freebsdTCPCBLen  = 744 // sizeof(struct xtcpcb)
	freebsdInpcbLen  = 400 // sizeof(struct xinpcb)
	freebsdSocketLen = 240 // sizeof(struct xsocket)

	// Where in a struct xtcpcb its struct xinpcb's length is, and its
	// struct xsocket's length, its handle of its socket, xso_so, the
	// socket's protocol and its owner.
	freebsdInpLen     = 8
	freebsdSoLen      = 16
	freebsdSoHandle   = 24
	freebsdSoProtocol = 112
	freebsdSoUID      = 136
	freebsdIncFlags   = 256 // struct in_conninfo's inc_flags
	freebsdIncFport   = 260
	freebsdIncLport   = 262
	freebsdIncFaddr   = 264 // union in_dependaddr, 16 bytes
	freebsdIncLaddr   = 280
	freebsdIncIsIPv6  = 0x01 // INC_ISIPV6
	freebsdKsizeLen   = 8    // sizeof(ksize_t)
	freebsdGenGen     = 16   // struct xinpgen's xig_gen
)

// freebsdOwner returns the owner of the socket whose end is e, as list,
// what freebsdList gives, says: the so_uid of the one socket whose local
// and foreign addresses and ports are e's; where there is none, ErrNotYet
// for a connection in the syncache, which the kernel makes a socket of as
// its handshake's last packet comes, a moment after the end that
// connected has finished it, and errUnlisted for no connection at all, as
// in the moment between, or for one answered with a SYN cookie, or
// several, of a list that changed as it was read; and an error for a list
// with a record whose length isn't one Owner knows, or that ends in the
// middle of one.
func freebsdOwner(list []byte, e end) (uint32, error) {
	ne, be := binary.NativeEndian, binary.BigEndian
	bad := func(what string, offset int) error { return errUnknownRecord{freebsdList, what, offset} }
	if len(list) < freebsdGenLen || ne.Uint64(list) != freebsdGenLen {
		return 0, bad("a header of another length", 0)
	}
	var candidates []uint32
	ended, pending, changed := false, false, false
	for off := freebsdGenLen; off < len(list); off += freebsdTCPCBLen {
		if len(list)-off < freebsdKsizeLen {
			return 0, bad("the list's end in the middle of a record", off)
		}
		switch n := ne.Uint64(list[off:]); {
		case n == freebsdGenLen && off+freebsdGenLen == len(list):
			ended = true
			changed = ne.Uint64(list[off+freebsdGenGen:]) != ne.Uint64(list[freebsdGenGen:])
		case n == freebsdGenLen:
			return 0, bad("a struct xinpgen before the list's end", off)
		case n != freebsdTCPCBLen:
			return 0, bad(fmt.Sprintf("a struct xtcpcb of %d bytes", n), off)
		case off+freebsdTCPCBLen > len(list):
			return 0, bad("the list's end in the middle of a record", off)
		}
		if ended {
			break
		}
		xt := list[off : off+freebsdTCPCBLen]
		if n := ne.Uint64(xt[freebsdInpLen:]); n != freebsdInpcbLen {
			return 0, bad(fmt.Sprintf("a struct xinpcb of %d bytes", n), off)
		}
		switch n := ne.Uint64(xt[freebsdSoLen:]); {
		case n == 0:
			// No socket, as one gone: no owner.
			continue
		case n != freebsdSocketLen:
			return 0, bad(fmt.Sprintf("a struct xsocket of %d bytes", n), off)
		}
		if ne.Uint32(xt[freebsdSoProtocol:]) != ipprotoTCP {
			continue
		}
		v6 := xt[freebsdIncFlags]&freebsdIncIsIPv6 != 0
		lport, fport := be.Uint16(xt[freebsdIncLport:]), be.Uint16(xt[freebsdIncFport:])
		laddr, faddr := xt[freebsdIncLaddr:freebsdIncLaddr+16], xt[freebsdIncFaddr:freebsdIncFaddr+16]
		switch {
		case !e.matches(v6, lport, fport, laddr, faddr):
		case ne.Uint64(xt[freebsdSoHandle:]) == 0:
			// In the syncache, of no socket yet.
			pending = true
		default:
			candidates = append(candidates, ne.Uint32(xt[freebsdSoUID:]))
		}
	}
	if !ended {
		return 0, bad("the list's end without a struct xinpgen", len(list))
	}
	return ownerOf(freebsdList, candidates, pending, changed)
}
