package sockowner

import (
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

// fbsd is FreeBSD 14's structs, for 64 bits, which the fixtures are laid
// out as: FreeBSD 13's, and 32-bit x86's, put what the parser reads in the
// same places (TestLayouts).
var fbsd = freebsdOf(lp64, 14)

// freebsdSocket returns the struct xtcpcb of the TCP socket whose end is
// e, whose owner is uid, as tcp_pcblist writes it.
func freebsdSocket(e end, uid uint32) record {
	r := newRecord(fbsd.xtcpcb).u64("xt_len", uint64(fbsd.xtcpcb.size)).
		u64("xt_inp.xi_len", uint64(fbsd.xinpcb.size)).
		u64("xt_inp.xi_socket.xso_len", uint64(fbsd.xsocket.size)).
		u64("xt_inp.xi_socket.xso_so", 0xfffff800_1c2e_4a00).
		u32("xt_inp.xi_socket.xso_protocol", ipprotoTCP).
		u32("xt_inp.xi_socket.so_uid", uid).
		u16be("xt_inp.inp_inc.inc_ie.ie_fport", e.remote.Port()).
		u16be("xt_inp.inp_inc.inc_ie.ie_lport", e.local.Port()).
		bytes("xt_inp.inp_inc.inc_ie.ie_dependfaddr", in4in6(e.remote.Addr())).
		bytes("xt_inp.inp_inc.inc_ie.ie_dependladdr", in4in6(e.local.Addr()))
	if !e.local.Addr().Is4() {
		r.u8("xt_inp.inp_inc.inc_flags", freebsdIncIsIPv6)
	}
	return r
}

// freebsdSyncache returns the struct xtcpcb of the connection whose end is
// e, whose handshake the kernel hasn't finished, as syncache_pcblist
// writes it: of a struct xsocket that holds no socket.
func freebsdSyncache(e end) record {
	r := freebsdSocket(e, 0)
	return r.u64("xt_inp.xi_socket.xso_so", 0)
}

// freebsdListOf returns what freebsdList gives of sockets: a struct
// xinpgen, each socket's struct xtcpcb, and another struct xinpgen.
func freebsdListOf(sockets ...record) []byte {
	gen := newRecord(fbsd.xinpgen).u64("xig_len", uint64(fbsd.xinpgen.size)).u32("xig_count", uint32(len(sockets))).b
	list := slices.Clone(gen)
	for _, s := range sockets {
		list = append(list, s.b...)
	}
	return append(list, gen...)
}

// TestFreeBSDOwner checks what the owner of a connection's other end is in
// FreeBSD's list of TCP sockets: the socket's whose four addresses are the
// connection's, exactly, and no other's, IPv4 or IPv6; and none of a
// connection in the syncache, which is one to wait for, as the kernel
// makes its socket as the handshake's last packet comes, nor of one the
// kernel no longer has a socket of, nor of another protocol's, nor of a
// socket the list doesn't have at all, as one of another user's that it
// hides, which aren't.
func TestFreeBSDOwner(t *testing.T) {
	for _, e := range []end{end4, end6} {
		var sockets []record
		for i, o := range others(e) {
			sockets = append(sockets, freebsdSocket(o, uint32(1000+i)))
		}
		list := freebsdListOf(slices.Insert(slices.Clone(sockets), 2, freebsdSocket(e, 501))...)
		if uid, err := freebsdOwner(list, e); uid != 501 || err != nil {
			t.Errorf("%v is user %d's, %v; want 501's", e, uid, err)
		}
		if uid, err := freebsdOwner(freebsdListOf(sockets...), e); !errors.Is(err, errUnlisted) {
			t.Errorf("of sockets near %v alone, it is user %d's, %v", e, uid, err)
		}
	}
	gone := newRecord(fbsd.xtcpcb).u64("xt_len", uint64(fbsd.xtcpcb.size)).u64("xt_inp.xi_len", uint64(fbsd.xinpcb.size))
	copy(gone.b[freebsdIncFlags:], freebsdSocket(end4, 0).b[freebsdIncFlags:freebsdIncLaddr+16])
	udp := freebsdSocket(end4, 503).u32("xt_inp.xi_socket.xso_protocol", 17)
	for name, s := range map[string]record{"without a socket": gone, "UDP": udp} {
		if uid, err := freebsdOwner(freebsdListOf(s), end4); !errors.Is(err, errUnlisted) {
			t.Errorf("%s: user %d, %v; want none", name, uid, err)
		}
	}
	if uid, err := freebsdOwner(freebsdListOf(freebsdSyncache(end4)), end4); !errors.Is(err, ErrNotYet) {
		t.Errorf("in the syncache: user %d, %v; want none yet", uid, err)
	}
	if uid, err := freebsdOwner(freebsdListOf(freebsdSyncache(end{local: end4.local, remote: end6.remote})), end4); !errors.Is(err, errUnlisted) {
		t.Errorf("another connection in the syncache: user %d, %v; want none", uid, err)
	}
	if uid, err := freebsdOwner(freebsdListOf(), end4); !errors.Is(err, errUnlisted) {
		t.Errorf("an empty list: user %d, %v; want none", uid, err)
	}
	// A connection that the syncache has, beside its socket, as the
	// kernel lists them as it accepts it, is the socket's.
	if uid, err := freebsdOwner(freebsdListOf(freebsdSyncache(end4), freebsdSocket(end4, 501)), end4); uid != 501 || err != nil {
		t.Errorf("of a connection in the syncache and a socket, user %d, %v; want 501", uid, err)
	}
}

// TestFreeBSDOwnerRefuses checks that a list Owner can't read all of, or
// that has two sockets at the connection's other end, says no owner, but
// an error: one with a struct xtcpcb, xinpcb or xsocket of a length Owner
// doesn't know, a struct xinpgen in the middle, or none at its end, and
// every list cut short, at every byte.
func TestFreeBSDOwnerRefuses(t *testing.T) {
	refused := func(name string, list []byte) {
		t.Helper()
		uid, err := freebsdOwner(list, end4)
		if err == nil || errors.Is(err, ErrNotYet) || errors.Is(err, errUnlisted) {
			t.Errorf("%s: user %d, %v; want an error", name, uid, err)
		}
	}
	longer := freebsdSocket(end4, 501).u64("xt_len", uint64(fbsd.xtcpcb.size+8))
	longer.b = append(longer.b, make([]byte, 8)...)
	refused("a longer struct xtcpcb", freebsdListOf(longer))
	refused("a struct xtcpcb that says it is longer", freebsdListOf(freebsdSocket(end4, 501).u64("xt_len", uint64(fbsd.xtcpcb.size+8))))
	refused("a longer struct xinpcb", freebsdListOf(freebsdSocket(end4, 501).u64("xt_inp.xi_len", uint64(fbsd.xinpcb.size+8))))
	refused("a longer struct xsocket", freebsdListOf(freebsdSocket(end4, 501).u64("xt_inp.xi_socket.xso_len", uint64(fbsd.xsocket.size+8))))
	refused("two sockets", freebsdListOf(freebsdSocket(end4, 501), freebsdSocket(end4, 501)))
	// Of a list that changed as the kernel wrote it, as its first and last
	// struct xinpgen's generations say, it asks again.
	changed := freebsdListOf(freebsdSocket(end4, 501), freebsdSocket(end4, 501))
	binary.NativeEndian.PutUint64(changed[len(changed)-freebsdGenLen+fbsd.xinpgen.off("xig_gen"):], 7)
	if uid, err := freebsdOwner(changed, end4); !errors.Is(err, errUnlisted) {
		t.Errorf("two sockets of a list that changed: user %d, %v; want to ask again", uid, err)
	}
	list := freebsdListOf(freebsdSocket(end6, 7), freebsdSocket(end4, 501))
	gen := list[len(list)-freebsdGenLen:]
	refused("a struct xinpgen in the middle", slices.Concat(list[:freebsdGenLen], gen, list[freebsdGenLen:]))
	refused("no struct xinpgen at its end", list[:len(list)-freebsdGenLen])
	longHeader := slices.Clone(list)
	binary.NativeEndian.PutUint64(longHeader, freebsdGenLen+8)
	refused("a header of another length", longHeader)
	for n := range len(list) {
		refused("cut short", list[:n])
	}
}
