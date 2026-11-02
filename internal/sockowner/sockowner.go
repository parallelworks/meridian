// Package sockowner asks the system which user owns the socket at the
// other end of a TCP connection to this machine's loopback address, so
// that Meridian passes a session's requests, with the browser's cookies
// and passwords, only to a service of its own user's, and never to another
// user's program that took the service's port first (docs/design.md,
// Serving a session).
//
// It asks about the connection's own socket at the other end, rather than
// the listener, so that no program can listen in the service's place
// between a check and the connection, as one could between a look at the
// port's listener and a dial. What the system says of that socket differs:
//
//   - Linux says who owns it only once a process has accepted it: the user
//     of the process that did, which Owner waits for. It says so through
//     NETLINK_SOCK_DIAG, asked for that socket alone, as ss asks.
//   - macOS and FreeBSD give each connection a listener accepts the
//     credential of whoever made the listener, from the start, whoever
//     accepts it. They say so in their lists of TCP sockets, the sysctls
//     net.inet.tcp.pcblist_n and net.inet.tcp.pcblist, as netstat reads
//     them, of which Owner checks each record's kind and length, and
//     refuses any it doesn't know.
//
// So whom an owner may be trusted as differs too (Trusted). Meridian
// coordinates nowhere else, so Owner returns ErrUnsupported on every other
// system, and its caller refuses the connection.
package sockowner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"time"
)

// ErrUnsupported is Owner's error on a system where Meridian doesn't ask
// who owns a connection's other end. It wraps errors.ErrUnsupported.
var ErrUnsupported = errUnsupported{}

type errUnsupported struct{}

func (errUnsupported) Error() string        { return "sockowner: not supported on this system" }
func (errUnsupported) Is(target error) bool { return target == errors.ErrUnsupported }

// ErrNotYet is Owner's error where, for as long as it waits, the system
// says no owner of the socket at the connection's other end yet: its
// handshake isn't finished, as for a listener whose queue of connections
// to accept is full; or, on Linux, no process has accepted it, whose
// kernel gives the owner of such a socket as root, whoever listens. A
// service that doesn't accept a connection in time doesn't answer it.
var ErrNotYet = errors.New("sockowner: the system says no owner of the connection's other end yet")

// errUnlisted is a lookup's error where the system lists no socket at the
// connection's other end, nor its handshake: one gone, one it hides from
// this user, as FreeBSD hides other users' sockets where
// security.bsd.see_other_uids is 0, or one in another network namespace;
// or one not made yet, as for a connection the listener answered with a
// SYN cookie, whose socket comes with the handshake's last packet, which
// on this machine's loopback address comes about as the end that
// connected has finished it, and as for one FreeBSD's syncache is handing
// to its socket, which the list shows for neither the moment between. So
// Owner asks again, for grace, before it gives up on one.
var errUnlisted = errors.New("sockowner: the system lists no socket at the connection's other end")

// What Owner waits for an owner of the connection's other end: settle at
// most for one the system says it has no owner of yet (ErrNotYet), and
// grace for one it doesn't list (errUnlisted), asking again after
// firstWait, and at most every lastWait.
var (
	settle    = 5 * time.Second
	grace     = time.Second
	firstWait = time.Millisecond
	lastWait  = 50 * time.Millisecond
)

// Owner returns the user who owns the socket at the other end of conn, a
// connection to a port of this machine's loopback address: on Linux, the
// user of the process that accepted it, once one has, which it waits for,
// until ctx is done, as it does for the system to finish the connection's
// handshake (ErrNotYet); on macOS and FreeBSD, the user who made the
// listener that accepted it. A socket the system doesn't say the owner
// of, as a closed one, or one it still doesn't list after a moment, as
// one in another network namespace, is an error, as is any record of the
// system's that Owner doesn't know.
func Owner(ctx context.Context, conn *net.TCPConn) (uint32, error) {
	return ownerBy(ctx, conn, lookup)
}

// ownerBy is Owner, which asks the system with lookup, as a test replaces
// it.
func ownerBy(ctx context.Context, conn *net.TCPConn, lookup func(end) (uint32, error)) (uint32, error) {
	e, err := otherEnd(conn)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	for wait := firstWait; ; wait = min(2*wait, lastWait) {
		uid, err := lookup(e)
		var limit time.Duration
		switch {
		case errors.Is(err, ErrNotYet):
			limit = settle
		case errors.Is(err, errUnlisted):
			limit = grace
		default:
			return uid, err
		}
		if time.Since(start) > limit {
			return 0, fmt.Errorf("%w, after %v", err, limit)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return 0, fmt.Errorf("%w: %w", err, context.Cause(ctx))
		case <-t.C:
		}
	}
}

// Trusted reports whether a connection whose other end Owner says is
// uid's may be trusted as the process's own user's: one of the process's
// own user's; or, on Linux, root's too, where uid is the user of the
// process that accepted the connection, and a process of root's could
// take any connection anyway. On macOS and FreeBSD, uid is the user who
// made the listener, whose credential the system gives each connection
// it accepts, not the user of the process that accepts on it; and a
// listener root made may have been passed to any user's program: so
// root's is another user's there.
func Trusted(uid uint32) bool {
	return int(uid) == os.Getuid() || rootAccepts && uid == 0
}

// An end is a TCP socket's end of its connection, as the system knows
// it: its own address, local, and its peer's, remote, each IPv4 or IPv6
// alike.
type end struct {
	local, remote netip.AddrPort
}

// otherEnd returns the end of conn's other socket, whose local address is
// conn's remote one, which is this machine's loopback address.
func otherEnd(conn *net.TCPConn) (end, error) {
	la, lok := conn.LocalAddr().(*net.TCPAddr)
	ra, rok := conn.RemoteAddr().(*net.TCPAddr)
	if !lok || !rok {
		return end{}, errors.New("sockowner: the connection has no TCP addresses")
	}
	unmapped := func(a *net.TCPAddr) netip.AddrPort {
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	e := end{local: unmapped(ra), remote: unmapped(la)}
	switch {
	case !e.local.Addr().IsLoopback():
		return end{}, fmt.Errorf("sockowner: %v isn't this machine's loopback address", e.local)
	case e.local.Addr().Is4() != e.remote.Addr().Is4():
		return end{}, fmt.Errorf("sockowner: the connection from %v to %v mixes IPv4 and IPv6", e.remote, e.local)
	}
	return e, nil
}

// matches reports whether a record's ports, and its 16-byte local and
// foreign addresses, as BSD keeps them, are those of e: an IPv4 address,
// for a record whose socket says it is, in the last 4 bytes, and an IPv6
// one in all 16.
func (e end) matches(v6 bool, lport, fport uint16, laddr, faddr []byte) bool {
	if lport != e.local.Port() || fport != e.remote.Port() || v6 == e.local.Addr().Is4() {
		return false
	}
	if v6 {
		l, r := e.local.Addr().As16(), e.remote.Addr().As16()
		return string(laddr) == string(l[:]) && string(faddr) == string(r[:])
	}
	l, r := e.local.Addr().As4(), e.remote.Addr().As4()
	return string(laddr[12:16]) == string(l[:]) && string(faddr[12:16]) == string(r[:])
}

// errUnknownRecord is a record's error where its kind or its length
// isn't one Owner knows, or the list ends in the middle of it, so that
// Owner can't read what it says, and refuses the connection rather than
// guess.
type errUnknownRecord struct {
	list, what string
	offset     int
}

func (e errUnknownRecord) Error() string {
	return fmt.Sprintf("sockowner: %s at byte %d of %s, which Owner doesn't read", e.what, e.offset, e.list)
}

// ownerOf returns the owner of the one socket of candidates, the users of
// the records that match a connection's end that a socket holds: for
// none, ErrNotYet where the list has the connection's handshake, pending,
// and errUnlisted where it doesn't; and for more than one, which no
// single connection has, errUnlisted, to ask again, where the list
// changed as the kernel wrote it, changed, as its first and last struct
// xinpgen's generations say, and otherwise an error.
func ownerOf(list string, candidates []uint32, pending, changed bool) (uint32, error) {
	switch {
	case len(candidates) == 0 && pending:
		return 0, ErrNotYet
	case len(candidates) == 0:
		return 0, errUnlisted
	case len(candidates) == 1:
		return candidates[0], nil
	case changed:
		return 0, fmt.Errorf("%w: %s changed as it was read, and has %d sockets at it", errUnlisted, list, len(candidates))
	}
	return 0, fmt.Errorf("sockowner: %s has %d sockets at the connection's other end", list, len(candidates))
}

// roundUp8 returns n rounded up to a multiple of 8.
func roundUp8(n int) int { return (n + 7) &^ 7 }
