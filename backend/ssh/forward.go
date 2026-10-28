package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"strings"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/backend/ssh/internal/hostbin"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/problems"
)

var _ backend.Forwarder = (*Driver)(nil)

// MemberKey returns m's key, which is its name (Name): ssh- and the keyed
// hash of its host and logins.
func (d *Driver) MemberKey(m *backend.Member) (string, error) { return d.Name(m) }

// Endpoint returns where m's workers reach the coordinator on the host:
// the socket, in the sockets directory hello says, named by the keyed hash
// of m's host and logins (Name), at which Forward listens.
func (d *Driver) Endpoint(ctx context.Context, m *backend.Member) (backend.Endpoint, error) {
	t, err := targetOf(m)
	if err != nil {
		return backend.Endpoint{}, err
	}
	_, h, err := d.learned(ctx, t)
	if err != nil {
		return backend.Endpoint{}, err
	}
	p, err := d.socket(m, h)
	if err != nil {
		return backend.Endpoint{}, err
	}
	return backend.Endpoint{Address: p}, nil
}

// Connect returns m's endpoint (Endpoint), and nothing to close: the
// coordinator listens there through Forward, as a backend.Forwarder's
// does, and calls no Connect of it.
func (d *Driver) Connect(ctx context.Context, m *backend.Member, _ backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	e, err := d.Endpoint(ctx, m)
	return e, nil, err
}

// Forward asks sshd on m's host to listen at m's endpoint, and to forward
// each connection to it over the pool's connection to the host
// (streamlocal-forward@openssh.com), once it has removed a socket there,
// which a holder before this one left: OpenSSH's StreamLocalBindUnlink is
// no by default, and then sshd won't listen where a socket is. It removes
// only a socket, never anything else. The listener's connections are the
// workers', and it ends with the connection. A host whose sshd won't
// listen there, as one that doesn't forward Unix sockets
// (AllowStreamLocalForwarding, DisableForwarding), is
// problems.SSHForwardingRefused. A connection the pool kept can have
// closed before it noticed, as one a host dropped as the listener before
// this one ended: what fails on it for that is tried once more, over the
// connection made again, as the driver's other calls are (closed).
func (d *Driver) Forward(ctx context.Context, m *backend.Member) (net.Listener, error) {
	t, err := targetOf(m)
	if err != nil {
		return nil, err
	}
	for try := 1; ; try++ {
		l, conn, err := d.forward(ctx, m, t)
		if err == nil || try > 1 || conn == nil || ctx.Err() != nil || asProblem(err) != nil || !closed(conn, err) {
			return l, err
		}
	}
}

// forward is Forward, once, of m, whose target is t, over the pool's
// connection to it, which it returns too, once it has it.
func (d *Driver) forward(ctx context.Context, m *backend.Member, t sshconn.Target) (net.Listener, *sshconn.Conn, error) {
	conn, h, err := d.learned(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	p, err := d.socket(m, h)
	if err != nil {
		return nil, conn, err
	}
	if err := unlinkSocket(ctx, conn, p); err != nil {
		return nil, conn, err
	}
	l, err := conn.ListenUnix(p)
	switch {
	case err == nil:
		return l, conn, nil
	case errors.Is(err, sshconn.ErrForwardingRefused):
		return nil, conn, problems.SSHForwardingRefused.Problem(problems.Params{"host": t.Host, "path": p})
	}
	return nil, conn, err
}

// socket returns the path of m's socket on the host h: its sockets
// directory's, named by the keyed hash in m's name, which fits
// hostproto.MaxSocketName.
func (d *Driver) socket(m *backend.Member, h *host) (string, error) {
	name, err := d.Name(m)
	if err != nil {
		return "", err
	}
	file := strings.TrimPrefix(name, "ssh-") + ".sock"
	if len(file) > hostproto.MaxSocketName || !path.IsAbs(h.hello.Paths.Sockets) {
		return "", fmt.Errorf("ssh: no socket of %d bytes in the sockets directory %q", len(file), h.hello.Paths.Sockets)
	}
	return path.Join(h.hello.Paths.Sockets, file), nil
}

// learned returns the pool's connection to t's host, and what the driver
// learned of the host, as run does: a connection that turns out to have
// closed is made again, once.
func (d *Driver) learned(ctx context.Context, t sshconn.Target) (*sshconn.Conn, *host, error) {
	for try := 1; ; try++ {
		conn, err := d.connect(ctx, t)
		if err != nil {
			return nil, nil, err
		}
		h, err := d.host(ctx, conn, t)
		if err == nil || try > 1 || ctx.Err() != nil || !closed(conn, sentOrNot(err)) {
			return conn, h, sentOrNot(err)
		}
	}
}

// unlinkSocket removes the socket at p on the host conn reaches, if a
// socket is there, with a script (sshconn.Conn.Script) that runs the
// system's rm.
func unlinkSocket(ctx context.Context, conn *sshconn.Conn, p string) error {
	sc, err := conn.Script(ctx, "PATH=/usr/bin:/bin; export PATH; p="+sshconn.Quote(p)+"; if [ -S \"$p\" ]; then rm -f \"$p\"; fi")
	if err != nil {
		return err
	}
	defer sc.Close()
	_ = sc.CloseWrite()
	if err := sc.Wait(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if said := strings.TrimSpace(sc.Stderr()); said != "" {
			return fmt.Errorf("ssh: remove a socket left at %s: %w: %s", p, err, problems.EscapeControl(said))
		}
		return fmt.Errorf("ssh: remove a socket left at %s: %w", p, err)
	}
	return nil
}

// Options are what New makes a Driver of.
type Options struct {
	// SSH, ConfigFile, Home, User and Getenv say how to connect to hosts
	// as the user would: the ssh client that reads their config, as
	// ssh -G, or "ssh" on PATH, for ""; the config it reads in place of
	// theirs and the system's, as ssh -F, or "" for those; their home
	// directory and name; and how to read the environment, for
	// SSH_AUTH_SOCK and SHELL. Each is the process's for "" and nil.
	SSH, ConfigFile, Home, User string
	Getenv                      func(string) string
	// Version is this release's, of which every host's binary must be, and
	// Binaries the directory of binaries for hosts of other systems than
	// the coordinator's, which the user fetched:
	// Binaries/<version>/<os>-<arch>/meridian.
	Version, Binaries string
	// Store and Key are the Driver's.
	Store string
	Key   []byte
}

// New returns a Driver that connects to hosts as o says, through a pool
// of connections of its own, which Close closes.
func New(o Options) *Driver {
	dialer := &sshconn.Dialer{SSH: o.SSH, ConfigFile: o.ConfigFile, Home: o.Home, User: o.User, Getenv: o.Getenv}
	return &Driver{
		Pool:     &sshconn.Pool{Dialer: dialer},
		Binaries: &hostbin.Binaries{Version: o.Version, Dir: o.Binaries},
		Store:    o.Store, Key: o.Key,
	}
}

// Close closes the Driver's connections to hosts, which ends the listeners
// forwarded over them.
func (d *Driver) Close() error { return d.Pool.Close() }
