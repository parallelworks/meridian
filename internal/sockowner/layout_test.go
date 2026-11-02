package sockowner

import (
	"encoding/binary"
	"strconv"
	"strings"
	"testing"
)

// The tests lay out the structs that the lists of macOS's and FreeBSD's
// TCP sockets hold from their headers' declarations, field by field, as a
// C compiler does, rather than from the offsets the parsers read, so that
// the fixtures they make of them, and the parsers' offsets, are checked
// against the headers, not against themselves.

// A cField is a field of a C struct, as its header declares it: its name,
// and its type's size and alignment; or, for a struct or an array of
// them, sub, the struct, and n, how many.
type cField struct {
	name        string
	size, align int
	sub         *cStruct
}

// A cStruct is a C struct as a compiler lays it out: each field at the
// first offset after the one before that its alignment allows, which
// pack, unless it is 0, caps, as #pragma pack does; and its size a
// multiple of its alignment, its widest field's, capped too, or aligned,
// where its header gives it __aligned.
type cStruct struct {
	size, align int
	fields      map[string]cField
	offsets     map[string]int
}

func layOut(pack, aligned int, fields ...cField) *cStruct {
	s := &cStruct{align: 1, fields: map[string]cField{}, offsets: map[string]int{}}
	for _, f := range fields {
		a := f.align
		if pack != 0 {
			a = min(a, pack)
		}
		s.align = max(s.align, a)
		s.size = (s.size + a - 1) / a * a
		s.fields[f.name], s.offsets[f.name] = f, s.size
		s.size += f.size
	}
	s.align = max(s.align, aligned)
	s.size = (s.size + s.align - 1) / s.align * s.align
	return s
}

// off returns the offset of the field at path, its name, or a struct's
// name and its field's path, as xt_inp.xi_socket.so_uid.
func (s *cStruct) off(path string) int {
	name, rest, nested := strings.Cut(path, ".")
	f, ok := s.fields[name]
	if !ok {
		panic("no field " + name) //nolint:forbidigo // a test's own typo
	}
	if !nested {
		return s.offsets[name]
	}
	return s.offsets[name] + f.sub.off(rest)
}

// An abi is what a C compiler aligns a uint64_t on, inside a struct: 8
// bytes, but on 32-bit x86, 4.
type abi struct{ u64align int }

var (
	lp64 = abi{u64align: 8}
	i386 = abi{u64align: 4}
)

func u8(name string) cField  { return cField{name: name, size: 1, align: 1} }
func u16(name string) cField { return cField{name: name, size: 2, align: 2} }
func u32(name string) cField { return cField{name: name, size: 4, align: 4} }
func (a abi) u64(name string) cField {
	return cField{name: name, size: 8, align: a.u64align}
}

// array is n of f, under name.
func array(name string, n int, f cField) cField {
	return cField{name: name, size: n * f.size, align: f.align, sub: f.sub}
}

// sub is the struct s, as the field name.
func sub(name string, s *cStruct) cField {
	return cField{name: name, size: s.size, align: s.align, sub: s}
}

// addr16 is a union of struct in6_addr, whose widest member is a
// uint32_t's array, and struct in_addr_4in6, on both systems.
func addr16(name string) cField { return cField{name: name, size: 16, align: 4} }

// XNU's, under #pragma pack(4), for 64-bit macOS, from bsd/netinet/in_pcb.h,
// bsd/sys/socketvar.h and bsd/netinet/tcp_var.h.
var (
	xnuXinpgen = layOut(4, 0, u32("xig_len"), u32("xig_count"), lp64.u64("xig_gen"), lp64.u64("xig_sogen"))
	xnuXinpcbN = layOut(4, 0,
		u32("xi_len"), u32("xi_kind"), lp64.u64("xi_inpp"), u16("inp_fport"), u16("inp_lport"),
		lp64.u64("inp_ppcb"), lp64.u64("inp_gencnt"), u32("inp_flags"), u32("inp_flow"),
		u8("inp_vflag"), u8("inp_ip_ttl"), u8("inp_ip_p"),
		addr16("inp_dependfaddr"), addr16("inp_dependladdr"),
		sub("inp_depend4", layOut(4, 0, u8("inp4_ip_tos"))),
		sub("inp_depend6", layOut(4, 0, u8("inp6_hlim"), u32("inp6_cksum"), u16("inp6_ifindex"), u16("inp6_hops"))),
		u32("inp_flowhash"), u32("inp_flags2"))
	xnuXsocketN = layOut(4, 0,
		u32("xso_len"), u32("xso_kind"), lp64.u64("xso_so"), u16("so_type"), u32("so_options"),
		u16("so_linger"), u16("so_state"), lp64.u64("so_pcb"), u32("xso_protocol"), u32("xso_family"),
		u16("so_qlen"), u16("so_incqlen"), u16("so_qlimit"), u16("so_timeo"), u16("so_error"),
		u32("so_pgid"), u32("so_oobmark"), u32("so_uid"), u32("so_last_pid"), u32("so_e_pid"),
		lp64.u64("so_gencnt"), u32("so_flags"), u32("so_flags1"), u32("so_usecount"), u32("so_retaincnt"),
		u32("xso_filter_flags"))
	xnuXsockbufN = layOut(4, 0,
		u32("xsb_len"), u32("xsb_kind"), u32("sb_cc"), u32("sb_hiwat"), u32("sb_mbcnt"), u32("sb_mbmax"),
		u32("sb_lowat"), u16("sb_flags"), u16("sb_timeo"))
	xnuXsockstatN = layOut(4, 0, u32("xst_len"), u32("xst_kind"),
		array("xst_tc_stats", 4, sub("data_stats", layOut(4, 0,
			lp64.u64("rxpackets"), lp64.u64("rxbytes"), lp64.u64("txpackets"), lp64.u64("txbytes")))))
	xnuXtcpcbN = layOut(4, 0,
		u32("xt_len"), u32("xt_kind"), lp64.u64("t_segq"), u32("t_dupacks"),
		array("t_timer", 4, u32("")), // TCPT_NTIMERS_EXT
		u32("t_state"), u32("t_flags"), u32("t_force"),
		u32("snd_una"), u32("snd_max"), u32("snd_nxt"), u32("snd_up"),
		u32("snd_wl1"), u32("snd_wl2"), u32("iss"), u32("irs"),
		u32("rcv_nxt"), u32("rcv_adv"), u32("rcv_wnd"), u32("rcv_up"),
		u32("snd_wnd"), u32("snd_cwnd"), u32("snd_ssthresh"), u32("t_maxopd"),
		u32("t_rcvtime"), u32("t_starttime"), u32("t_rtttime"), u32("t_rtseq"),
		u32("t_rxtcur"), u32("t_maxseg"), u32("t_srtt"), u32("t_rttvar"),
		u32("t_rxtshift"), u32("t_rttmin"), u32("t_rttupdated"), u32("max_sndwnd"),
		u32("t_softerror"), u8("t_oobflags"), u8("t_iobc"), u8("snd_scale"), u8("rcv_scale"),
		u8("request_r_scale"), u8("requested_s_scale"), u32("ts_recent"),
		u32("ts_recent_age"), u32("last_ack_sent"), u32("cc_send"), u32("cc_recv"), u32("snd_recover"),
		u32("snd_cwnd_prev"), u32("snd_ssthresh_prev"))
)

// A freebsd is FreeBSD's structs, for an ABI, from FreeBSD 13's or 14's
// sys/netinet/in_pcb.h, sys/netinet/tcp_var.h and sys/sys/socketvar.h.
type freebsd struct {
	xinpgen, xsocket, xinpcb, xtcpcb *cStruct
}

// freebsdOf lays FreeBSD's structs out for a, as release 13 or 14
// declares them: they differ in which spares hold fields, not in where.
func freebsdOf(a abi, release int) freebsd {
	var sockSpare, bufSpare, tcpTail []cField
	switch release {
	case 13:
		sockSpare = []cField{array("so_spare64", 8, a.u64(""))}
		bufSpare = []cField{u32("sb_mcnt"), u32("sb_ccnt")}
		tcpTail = []cField{u16("xt_encaps_port"), u16("spare16"), array("spare32", 25, u32(""))}
	case 14:
		sockSpare = []cField{a.u64("so_splice_so"), array("so_spare64", 7, a.u64(""))}
		bufSpare = []cField{u32("sb_spare0"), u32("sb_spare1")}
		tcpTail = []cField{
			u32("t_dsack_bytes"), u32("t_dsack_tlp_bytes"), u32("t_dsack_pack"),
			u16("xt_encaps_port"), u16("spare16"), array("spare32", 22, u32("")),
		}
	}
	xsockbuf := layOut(0, 0, append(append([]cField{u32("sb_cc"), u32("sb_hiwat"), u32("sb_mbcnt")}, bufSpare...),
		u32("sb_mbmax"), u32("sb_lowat"), u32("sb_timeo"), u16("sb_flags"))...)
	var f freebsd
	f.xinpgen = layOut(0, 8, a.u64("xig_len"), u32("xig_count"), u32("_xig_spare32"),
		a.u64("xig_gen"), a.u64("xig_sogen"), array("_xig_spare64", 4, a.u64("")))
	f.xsocket = layOut(0, 0, append(append([]cField{a.u64("xso_len"), a.u64("xso_so"), a.u64("so_pcb"), a.u64("so_oobmark")}, sockSpare...),
		u32("xso_protocol"), u32("xso_family"), u32("so_qlen"), u32("so_incqlen"), u32("so_qlimit"),
		u32("so_pgid"), u32("so_uid"), array("so_spare32", 8, u32("")),
		u16("so_type"), u16("so_options"), u16("so_linger"), u16("so_state"), u16("so_timeo"), u16("so_error"),
		sub("so_rcv", xsockbuf), sub("so_snd", xsockbuf))...)
	inEndpoints := layOut(0, 0, u16("ie_fport"), u16("ie_lport"), addr16("ie_dependfaddr"), addr16("ie_dependladdr"), u32("ie6_zoneid"))
	inConninfo := layOut(0, 0, u8("inc_flags"), u8("inc_len"), u16("inc_fibnum"), sub("inc_ie", inEndpoints))
	f.xinpcb = layOut(0, 8,
		a.u64("xi_len"), sub("xi_socket", f.xsocket), sub("inp_inc", inConninfo), a.u64("inp_gencnt"),
		a.u64("inp_ppcb"), array("inp_spare64", 4, a.u64("")), u32("inp_flow"), u32("inp_flowid"),
		u32("inp_flowtype"), u32("inp_flags"), u32("inp_flags2"), u32("inp_rss_listen_bucket"),
		u32("in6p_cksum"), array("inp_spare32", 4, u32("")), u16("in6p_hops"), u8("inp_ip_tos"),
		u8("pad8"), u8("inp_vflag"), u8("inp_ip_ttl"), u8("inp_ip_p"), u8("inp_ip_minttl"),
		array("inp_spare8", 4, u8("")))
	f.xtcpcb = layOut(0, 8, append([]cField{
		a.u64("xt_len"), sub("xt_inp", f.xinpcb),
		array("xt_stack", 32, u8("")), // TCP_FUNCTION_NAME_LEN_MAX
		array("xt_logid", 64, u8("")), // TCP_LOG_ID_LEN
		array("xt_cc", 16, u8("")),    // TCP_CA_NAME_MAX
		array("spare64", 6, a.u64("")),
		u32("t_state"), u32("t_flags"), u32("t_sndzerowin"), u32("t_sndrexmitpack"), u32("t_rcvoopack"),
		u32("t_rcvtime"), u32("tt_rexmt"), u32("tt_persist"), u32("tt_keep"), u32("tt_2msl"),
		u32("tt_delack"), u32("t_logstate"), u32("t_snd_cwnd"), u32("t_snd_ssthresh"), u32("t_maxseg"),
		u32("t_rcv_wnd"), u32("t_snd_wnd"), u32("xt_ecn"),
	}, tcpTail...)...)
	return f
}

// TestLayouts checks the lengths and offsets the parsers read against the
// structs, as their headers declare them: macOS's for 64 bits, and
// FreeBSD's, of 13 and 14, for 64 bits and for 32-bit x86, where the
// lengths are the same, and so are the offsets the parser reads, though
// not every other.
func TestLayouts(t *testing.T) {
	check := func(what string, got, want int) {
		t.Helper()
		if got != want {
			t.Errorf("%s is %d, want %d", what, got, want)
		}
	}
	check("darwinGenLen", darwinGenLen, xnuXinpgen.size)
	check("darwinGenGen", darwinGenGen, xnuXinpgen.off("xig_gen"))
	for i, s := range []*cStruct{xnuXinpcbN, xnuXsocketN, xnuXsockbufN, xnuXsockbufN, xnuXsockstatN, xnuXtcpcbN} {
		check("darwinRecords["+strconv.Itoa(i)+"].len", darwinRecords[i].len, s.size)
	}
	check("darwinInpFport", darwinInpFport, xnuXinpcbN.off("inp_fport"))
	check("darwinInpLport", darwinInpLport, xnuXinpcbN.off("inp_lport"))
	check("darwinInpVflag", darwinInpVflag, xnuXinpcbN.off("inp_vflag"))
	check("darwinInpFaddr", darwinInpFaddr, xnuXinpcbN.off("inp_dependfaddr"))
	check("darwinInpLaddr", darwinInpLaddr, xnuXinpcbN.off("inp_dependladdr"))
	check("darwinSoHandle", darwinSoHandle, xnuXsocketN.off("xso_so"))
	check("darwinSoProtocol", darwinSoProtocol, xnuXsocketN.off("xso_protocol"))
	check("darwinSoFamily", darwinSoFamily, xnuXsocketN.off("xso_family"))
	check("darwinSoUID", darwinSoUID, xnuXsocketN.off("so_uid"))

	for _, release := range []int{13, 14} {
		for _, a := range []abi{lp64, i386} {
			f := freebsdOf(a, release)
			check("freebsdGenLen", freebsdGenLen, f.xinpgen.size)
			check("freebsdGenGen", freebsdGenGen, f.xinpgen.off("xig_gen"))
			check("freebsdTCPCBLen", freebsdTCPCBLen, f.xtcpcb.size)
			check("freebsdInpcbLen", freebsdInpcbLen, f.xinpcb.size)
			check("freebsdSocketLen", freebsdSocketLen, f.xsocket.size)
			check("freebsdInpLen", freebsdInpLen, f.xtcpcb.off("xt_inp.xi_len"))
			check("freebsdSoLen", freebsdSoLen, f.xtcpcb.off("xt_inp.xi_socket.xso_len"))
			check("freebsdSoHandle", freebsdSoHandle, f.xtcpcb.off("xt_inp.xi_socket.xso_so"))
			check("freebsdSoProtocol", freebsdSoProtocol, f.xtcpcb.off("xt_inp.xi_socket.xso_protocol"))
			check("freebsdSoUID", freebsdSoUID, f.xtcpcb.off("xt_inp.xi_socket.so_uid"))
			check("freebsdIncFlags", freebsdIncFlags, f.xtcpcb.off("xt_inp.inp_inc.inc_flags"))
			check("freebsdIncFport", freebsdIncFport, f.xtcpcb.off("xt_inp.inp_inc.inc_ie.ie_fport"))
			check("freebsdIncLport", freebsdIncLport, f.xtcpcb.off("xt_inp.inp_inc.inc_ie.ie_lport"))
			check("freebsdIncFaddr", freebsdIncFaddr, f.xtcpcb.off("xt_inp.inp_inc.inc_ie.ie_dependfaddr"))
			check("freebsdIncLaddr", freebsdIncLaddr, f.xtcpcb.off("xt_inp.inp_inc.inc_ie.ie_dependladdr"))
		}
	}
	// What the parser doesn't read is where 32-bit x86 differs.
	if a, b := freebsdOf(lp64, 14).xinpcb.off("inp_vflag"), freebsdOf(i386, 14).xinpcb.off("inp_vflag"); a == b {
		t.Errorf("inp_vflag is at %d on both", a)
	}
}

// A record is a struct's bytes, as a fixture fills them.
type record struct {
	s *cStruct
	b []byte
}

func newRecord(s *cStruct) record { return record{s, make([]byte, s.size)} }

func (r record) u8(path string, v uint8) record {
	r.b[r.s.off(path)] = v
	return r
}

func (r record) u16(path string, v uint16) record {
	binary.NativeEndian.PutUint16(r.b[r.s.off(path):], v)
	return r
}

func (r record) u16be(path string, v uint16) record {
	binary.BigEndian.PutUint16(r.b[r.s.off(path):], v)
	return r
}

func (r record) u32(path string, v uint32) record {
	binary.NativeEndian.PutUint32(r.b[r.s.off(path):], v)
	return r
}

func (r record) u64(path string, v uint64) record {
	binary.NativeEndian.PutUint64(r.b[r.s.off(path):], v)
	return r
}

func (r record) bytes(path string, v []byte) record {
	copy(r.b[r.s.off(path):], v)
	return r
}
