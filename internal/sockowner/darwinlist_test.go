package sockowner

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"slices"
	"testing"
)

// The ends the parsers' tests look for: the socket that a service at
// 127.0.0.1:8080, or [::1]:8080, accepted from port 50000 of the same
// address.
var (
	end4 = end{local: netip.MustParseAddrPort("127.0.0.1:8080"), remote: netip.MustParseAddrPort("127.0.0.1:50000")}
	end6 = end{local: netip.MustParseAddrPort("[::1]:8080"), remote: netip.MustParseAddrPort("[::1]:50000")}
)

// String is e as a test says it: its local address, and its remote one.
func (e end) String() string { return e.local.String() + " from " + e.remote.String() }

// others are sockets near e's that aren't it, each with an owner of its
// own: each differs from e in one of its ports or addresses; one is the
// socket at the connection's other end, ours, whose local and remote ends
// are e's, swapped; and one the service's listener, with no remote end.
func others(e end) []end {
	next := func(a netip.Addr) netip.Addr { return a.Next() }
	unspecified := netip.IPv6Unspecified()
	if e.local.Addr().Is4() {
		unspecified = netip.IPv4Unspecified()
	}
	return []end{
		{local: netip.AddrPortFrom(e.local.Addr(), e.local.Port()+1), remote: e.remote},
		{local: e.local, remote: netip.AddrPortFrom(e.remote.Addr(), e.remote.Port()+1)},
		{local: netip.AddrPortFrom(next(e.local.Addr()), e.local.Port()), remote: e.remote},
		{local: e.local, remote: netip.AddrPortFrom(next(e.remote.Addr()), e.remote.Port())},
		{local: e.remote, remote: e.local},
		{local: e.local, remote: netip.AddrPortFrom(unspecified, 0)},
	}
}

// in4in6 returns a's address as a 16-byte union of struct in6_addr and
// struct in_addr_4in6: an IPv4 address in its last 4 bytes.
func in4in6(a netip.Addr) []byte {
	b := make([]byte, 16)
	if a.Is4() {
		a4 := a.As4()
		copy(b[12:], a4[:])
		return b
	}
	a16 := a.As16()
	copy(b, a16[:])
	return b
}

// darwinSocket returns the records of the TCP socket whose end is e,
// whose owner is uid, as get_pcblist_n writes them: of the family its
// address is, but for a v4-mapped one of an IPv6 socket, as one that
// listens on both IPv6 and IPv4 accepts an IPv4 connection with, where
// mapped is set.
func darwinSocket(e end, uid uint32, mapped bool) []record {
	vflag, family := uint8(darwinInpIPv6), uint32(darwinAFInet6)
	if e.local.Addr().Is4() {
		vflag = darwinInpIPv4
		if !mapped {
			family = darwinAFInet
		}
	}
	return []record{
		newRecord(xnuXinpcbN).u32("xi_len", uint32(xnuXinpcbN.size)).u32("xi_kind", 0x010).
			u16be("inp_fport", e.remote.Port()).u16be("inp_lport", e.local.Port()).u8("inp_vflag", vflag).
			bytes("inp_dependfaddr", in4in6(e.remote.Addr())).bytes("inp_dependladdr", in4in6(e.local.Addr())),
		newRecord(xnuXsocketN).u32("xso_len", uint32(xnuXsocketN.size)).u32("xso_kind", 0x001).
			u64("xso_so", 0x9f3c_51e0_77d2_0a48).u32("xso_protocol", ipprotoTCP).u32("xso_family", family).u32("so_uid", uid),
		newRecord(xnuXsockbufN).u32("xsb_len", uint32(xnuXsockbufN.size)).u32("xsb_kind", 0x002),
		newRecord(xnuXsockbufN).u32("xsb_len", uint32(xnuXsockbufN.size)).u32("xsb_kind", 0x004),
		newRecord(xnuXsockstatN).u32("xst_len", uint32(xnuXsockstatN.size)).u32("xst_kind", 0x008),
		newRecord(xnuXtcpcbN).u32("xt_len", uint32(xnuXtcpcbN.size)).u32("xt_kind", 0x020),
	}
}

// darwinListOf returns what darwinList gives of sockets: a struct xinpgen,
// each socket's records, each at a multiple of 8 bytes, and another
// struct xinpgen.
func darwinListOf(sockets ...[]record) []byte {
	gen := newRecord(xnuXinpgen).u32("xig_len", uint32(xnuXinpgen.size)).u32("xig_count", uint32(len(sockets))).b
	list := slices.Clone(gen)
	for _, s := range sockets {
		for _, r := range s {
			list = append(list, r.b...)
			list = append(list, make([]byte, roundUp8(len(r.b))-len(r.b))...)
		}
	}
	return append(list, gen...)
}

// TestDarwinOwner checks what the owner of a connection's other end is in
// macOS's list of TCP sockets: the socket's whose four addresses are the
// connection's, exactly, and no other's; IPv4 or IPv6, or IPv4 accepted
// by a socket of IPv6; and none of a socket the list says it has no
// kernel's socket of, whose so_uid, 0, is no owner's, nor of another
// protocol's, which, as a socket the list doesn't have at all, is no
// socket to wait for, since macOS lists a connection's socket from its
// first packet.
func TestDarwinOwner(t *testing.T) {
	near := func(e end) [][]record {
		var sockets [][]record
		for i, o := range others(e) {
			sockets = append(sockets, darwinSocket(o, uint32(1000+i), false))
		}
		return sockets
	}
	for _, e := range []end{end4, end6} {
		sockets := near(e)
		list := darwinListOf(slices.Insert(slices.Clone(sockets), 3, darwinSocket(e, 501, false))...)
		if uid, err := darwinOwner(list, e); uid != 501 || err != nil {
			t.Errorf("%v is user %d's, %v; want 501's", e, uid, err)
		}
		if uid, err := darwinOwner(darwinListOf(sockets...), e); !errors.Is(err, errUnlisted) {
			t.Errorf("of sockets near %v alone, it is user %d's, %v", e, uid, err)
		}
	}
	if uid, err := darwinOwner(darwinListOf(darwinSocket(end4, 502, true)), end4); uid != 502 || err != nil {
		t.Errorf("an IPv4 connection to a socket of IPv6 is user %d's, %v; want 502's", uid, err)
	}
	none := darwinSocket(end4, 0, false)
	none[1].u64("xso_so", 0).u32("xso_family", 0)
	noneKinds := darwinSocket(end4, 0, false)
	noneKinds[1].u64("xso_so", 0)
	noneKinds[2].u32("xsb_kind", 0)
	noneKinds[3].u32("xsb_kind", 0)
	udp := darwinSocket(end4, 503, false)
	udp[1].u32("xso_protocol", 17)
	unix := darwinSocket(end4, 504, false)
	unix[1].u32("xso_family", 1)
	for name, s := range map[string][]record{"no socket": none, "no socket, nor buffers' kinds": noneKinds, "UDP": udp, "AF_UNIX": unix} {
		if uid, err := darwinOwner(darwinListOf(s), end4); !errors.Is(err, errUnlisted) {
			t.Errorf("%s: user %d, %v; want none", name, uid, err)
		}
	}
	if uid, err := darwinOwner(darwinListOf(), end4); !errors.Is(err, errUnlisted) {
		t.Errorf("an empty list: user %d, %v; want none", uid, err)
	}
	header := newRecord(xnuXinpgen).u32("xig_len", uint32(xnuXinpgen.size)).b
	if uid, err := darwinOwner(header, end4); !errors.Is(err, errUnlisted) {
		t.Errorf("a list of no socket, without its last struct xinpgen: user %d, %v; want none", uid, err)
	}
}

// TestDarwinOwnerRefuses checks that a list Owner can't read all of, or
// that has two sockets at the connection's other end, says no owner, but
// an error: one with a record whose length or kind isn't the one in its
// place, a struct xinpgen in the middle, a socket's buffer of no kind,
// and every list cut short, at every byte.
func TestDarwinOwnerRefuses(t *testing.T) {
	refused := func(name string, list []byte) {
		t.Helper()
		uid, err := darwinOwner(list, end4)
		if err == nil || errors.Is(err, ErrNotYet) || errors.Is(err, errUnlisted) {
			t.Errorf("%s: user %d, %v; want an error", name, uid, err)
		}
	}
	for i := range darwinRecords {
		longer := darwinSocket(end4, 501, false)
		longer[i].u32(xnuLenField[i], uint32(longer[i].s.size+4))
		longer[i].b = append(longer[i].b, 0, 0, 0, 0)
		refused("a longer record "+xnuLenField[i], darwinListOf(longer))
		says := darwinSocket(end4, 501, false)
		says[i].u32(xnuLenField[i], uint32(says[i].s.size+8))
		refused("a record that says it is longer "+xnuLenField[i], darwinListOf(says))
		other := darwinSocket(end4, 501, false)
		other[i].u32(xnuKindField[i], 0x040) // XSO_KCREG
		refused("a record of another kind in place of "+xnuLenField[i], darwinListOf(other))
	}
	swapped := darwinSocket(end4, 501, false)
	swapped[2], swapped[3] = swapped[3], swapped[2]
	refused("buffers swapped", darwinListOf(swapped))
	kindless := darwinSocket(end4, 501, false)
	kindless[3].u32("xsb_kind", 0)
	refused("a socket's buffer of no kind", darwinListOf(kindless))
	refused("two sockets", darwinListOf(darwinSocket(end4, 501, false), darwinSocket(end4, 501, false)))
	// Of a list that changed as the kernel wrote it, as its first and last
	// struct xinpgen's generations say, it asks again.
	changed := darwinListOf(darwinSocket(end4, 501, false), darwinSocket(end4, 501, false))
	binary.NativeEndian.PutUint64(changed[len(changed)-darwinGenLen+xnuXinpgen.off("xig_gen"):], 7)
	if uid, err := darwinOwner(changed, end4); !errors.Is(err, errUnlisted) {
		t.Errorf("two sockets of a list that changed: user %d, %v; want to ask again", uid, err)
	}
	if uid, err := darwinOwner(changed[:len(changed)-darwinGenLen], end4); err == nil || errors.Is(err, errUnlisted) {
		t.Errorf("two sockets of a list without its last struct xinpgen: user %d, %v; want an error", uid, err)
	}
	list := darwinListOf(darwinSocket(end6, 7, false), darwinSocket(end4, 501, false))
	gen := list[len(list)-darwinGenLen:]
	refused("a struct xinpgen in the middle", slices.Concat(list[:darwinGenLen], gen, list[darwinGenLen:]))
	longHeader := slices.Clone(list)
	binary.NativeEndian.PutUint32(longHeader, darwinGenLen+4)
	refused("a header of another length", longHeader)
	for n := range len(list) {
		// Of a list cut short, only its header alone has no socket, and
		// so, as an empty list, no owner; any other is an error.
		if uid, err := darwinOwner(list[:n], end4); err == nil || n != darwinGenLen && errors.Is(err, errUnlisted) {
			t.Errorf("cut short at %d of %d bytes: user %d, %v; want an error", n, len(list), uid, err)
		}
	}
}

// The fields of each of a socket's records that hold its length and its
// kind.
var (
	xnuLenField  = [...]string{"xi_len", "xso_len", "xsb_len", "xsb_len", "xst_len", "xt_len"}
	xnuKindField = [...]string{"xi_kind", "xso_kind", "xsb_kind", "xsb_kind", "xst_kind", "xt_kind"}
)
