package sshconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"golang.org/x/crypto/ssh"
)

// MaxTCPConns is how many connections to addresses (DialTCP) a Conn
// carries at once: what its host is asked to open for one user, as a
// browser's for a session, is bounded, however many it asks for.
const MaxTCPConns = 64

// ErrTCPForwardingRefused is the error of a host that refused to connect
// to an address for the connection (DialTCP), as sshd refuses where
// AllowTcpForwarding, DisableForwarding or PermitOpen forbids it, or as a
// server that forwards no TCP connections at all does.
var ErrTCPForwardingRefused = errors.New("sshconn: the host refused to forward a TCP connection")

// ErrTCPConnectFailed is the error of a host that answered that it
// couldn't connect to an address for the connection (DialTCP), as where
// nothing listens there, or the host can't resolve its name: the host
// answered, so the connection is up.
var ErrTCPConnectFailed = errors.New("sshconn: the host couldn't connect to the address")

// DialTCP asks the host to connect to host at port, and to carry what that
// connection carries over this one (direct-tcpip), as ssh -L and ssh -W
// ask it. The host resolves host, so a name only it knows, as a cluster's
// node's, will do. It doesn't check host or port, which its caller does.
//
// It returns the connection, which closes once ctx is done, or this
// connection closes, as well as with its Close: ctx bounds its life,
// where a net.Dialer's bounds only the dial. A host that refuses is
// ErrTCPForwardingRefused, and one that couldn't connect
// ErrTCPConnectFailed, each with what the host said, escaped.
//
// It waits, while MaxTCPConns of the connection's are open, for one to
// close, and for the host's answer, each only until ctx is done, or the
// connection closes (await): then it returns ctx's error, or why the
// connection closed. A connection the host opens after is closed, and
// only then gives its place back.
func (c *Conn) DialTCP(ctx context.Context, host string, port int) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case c.tcp <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.err
	}
	release := sync.OnceFunc(func() { <-c.tcp })
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := await(ctx, c, func() (net.Conn, error) { return c.client.Dial("tcp", addr) }, func(conn net.Conn) {
		if conn != nil {
			_ = conn.Close()
		}
		release()
	})
	if err == nil {
		return newTCPConn(ctx, c, conn, release), nil
	}
	if errors.Is(err, errAbandoned) {
		// Nothing opens on a connection that has closed, so its place
		// is given back at once.
		if c.Err() != nil {
			release()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, c.Err()
	}
	release()
	if oe, ok := errors.AsType[*ssh.OpenChannelError](err); ok {
		why := ErrTCPConnectFailed
		if oe.Reason == ssh.Prohibited || oe.Reason == ssh.UnknownChannelType {
			why = ErrTCPForwardingRefused
		}
		return nil, fmt.Errorf("%w: %s: %s", why, oe.Reason, escape(oe.Message))
	}
	return nil, err
}

// A tcpConn is a connection a host made for a Conn (DialTCP), which
// closes once the context it was dialed with is done, or the Conn closes,
// as well as with its Close, and then gives its place on the Conn back.
type tcpConn struct {
	net.Conn
	once    sync.Once
	release func()
	// closed is closed once Close is called, and err is what closing
	// the connection gave.
	closed chan struct{}
	err    error
}

// newTCPConn returns conn, a connection the host made for c, which closes
// once ctx is done, or c closes, and then calls release: a goroutine waits
// for either until conn is closed.
func newTCPConn(ctx context.Context, c *Conn, conn net.Conn, release func()) *tcpConn {
	t := &tcpConn{Conn: conn, release: release, closed: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
		case <-c.done:
		case <-t.closed:
			return
		}
		_ = t.Close()
	}()
	return t
}

// Close closes the connection, once, and gives its place on its Conn
// back.
func (t *tcpConn) Close() error {
	t.once.Do(func() {
		close(t.closed)
		t.err = t.Conn.Close()
		t.release()
	})
	return t.err
}
