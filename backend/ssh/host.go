package ssh

import (
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/parallelworks/meridian/backend/hostproto"
	"github.com/parallelworks/meridian/backend/ssh/internal/hostbin"
	"github.com/parallelworks/meridian/backend/ssh/internal/sshconn"
	"github.com/parallelworks/meridian/problems"
)

// A host is what the driver learned of the host a connection reaches,
// once: the binary it put there, what meridian host says of the host, and
// the host's tag (Driver.tag). ready is closed once it has, and err says
// why it couldn't.
type host struct {
	ready  chan struct{}
	binary hostbin.Binary
	hello  *hostproto.Hello
	tag    string
	err    error
}

// host returns what the driver knows of the host conn reaches, t's, which
// it learns once for the connection, and forgets with it: it puts the
// binary for the host's system there (hostbin.Install), and asks meridian
// host, run from it, what the host is (hostproto.OpHello). What failed is
// learned again the next time.
func (d *Driver) host(ctx context.Context, conn *sshconn.Conn, t sshconn.Target) (*host, error) {
	for {
		d.mu.Lock()
		if d.hosts == nil {
			d.hosts = map[*sshconn.Conn]*host{}
		}
		h, ok := d.hosts[conn]
		if !ok {
			h = &host{ready: make(chan struct{})}
			d.hosts[conn] = h
		}
		d.mu.Unlock()
		if !ok {
			h.err = d.learn(ctx, conn, t, h)
			if h.err != nil {
				d.drop(conn, h)
				close(h.ready)
				return nil, h.err
			}
			close(h.ready)
			go func() {
				<-conn.Done()
				d.drop(conn, h)
			}()
			return h, nil
		}
		select {
		case <-h.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if h.err == nil {
			return h, nil
		}
		// What another call learned failed: learn it again.
	}
}

// drop forgets h, what the driver learned of the host conn reaches, if it
// is still what it knows.
func (d *Driver) drop(conn *sshconn.Conn, h *host) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.hosts[conn] == h {
		delete(d.hosts, conn)
	}
}

// learn puts the binary on the host conn reaches, and asks meridian host
// what the host is: a host whose meridian host runs another binary than
// the one put there isn't driven, which may be a race with another that
// put a binary there, to be tried again; nor one of another release than
// the binary's (problems.SSHHostReleaseMismatch), which trying again won't
// change.
func (d *Driver) learn(ctx context.Context, conn *sshconn.Conn, t sshconn.Target, h *host) error {
	bin, _, err := hostbin.Install(ctx, conn, t.Host, d.Binaries)
	if err != nil {
		return err
	}
	m, err := call(ctx, conn, bin.Path, t.Host, hostproto.OpHello, &hostproto.Request{})
	if err != nil {
		return err
	}
	if m.Hello == nil {
		return errors.New("ssh: meridian host's hello said nothing of the host")
	}
	if d.hello != nil {
		d.hello(m.Hello)
	}
	switch {
	case m.Hello.Binary.Digest != bin.Digest:
		return fmt.Errorf("ssh: meridian host runs a binary of the digest %s, not the one put there, %s", m.Hello.Binary.Digest, bin.Digest)
	case d.Binaries.Version != "" && m.Hello.Version != d.Binaries.Version:
		return problems.SSHHostReleaseMismatch.Problem(problems.Params{"host": t.Host, "release": m.Hello.Version, "expected": d.Binaries.Version})
	}
	if h.tag, err = d.tag(m.Hello); err != nil {
		return err
	}
	h.binary, h.hello = bin, m.Hello
	return nil
}

// call runs meridian host op on the host h, which conn reaches, t's, and
// returns what it said (call). One that didn't answer in the protocol, as
// one whose binary has gone, makes the driver learn the host again.
func (d *Driver) call(ctx context.Context, conn *sshconn.Conn, h *host, t sshconn.Target, op string, req *hostproto.Request) (*hostproto.Message, error) {
	m, err := call(ctx, conn, h.binary.Path, t.Host, op, req)
	if m == nil && err != nil && ctx.Err() == nil {
		d.drop(conn, h)
	}
	return m, err
}

// call runs meridian host op, the binary at path on the host, which conn
// reaches, and which host names, as a script (sshconn.Conn.Script), and
// returns what it said, as hostproto says: once it says it is ready, in
// the protocol's version, the request is written, and only then, and its
// answer read. A problem it says is returned as the *problem.Problem it
// is, with the message, and an error it says as an error.
//
// The script runs meridian host, at path quoted for sh, without GODEBUG,
// which could turn off what meridian requires of its cryptography, as
// meridian host starts a worker without it. What a login script prints
// first is skipped: the protocol's messages are JSON objects, a line each.
//
// meridian host that ends without a message, saying why on its standard
// error, as meridian says a problem before it runs a command, as one with
// a binary that isn't in FIPS 140-3 mode does, is
// problems.SSHHostHelperFailed; one that ends without saying why, as one
// whose binary isn't there, is an error.
//
// An error before the request was written is an *unsentError: nothing was
// asked of the host.
func call(ctx context.Context, conn *sshconn.Conn, path, host, op string, req *hostproto.Request) (_ *hostproto.Message, err error) {
	sent := false
	defer func() {
		if err != nil && !sent {
			err = &unsentError{err}
		}
	}()
	sc, err := conn.Script(ctx, "unset GODEBUG; exec "+sshconn.Quote(path)+" host "+op)
	if err != nil {
		return nil, err
	}
	defer sc.Close()
	failed := func(err error) (*hostproto.Message, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// What it said is whole once it has ended.
		sc.Settle()
		said := sc.Stderr()
		if line := helperLine(said); line != "" {
			return nil, problems.SSHHostHelperFailed.Problem(problems.Params{"host": host, "op": op, "reason": line})
		}
		if said = strings.TrimSpace(said); said != "" {
			return nil, fmt.Errorf("ssh: meridian host %s: %w: %s", op, err, problems.EscapeControl(said))
		}
		return nil, fmt.Errorf("ssh: meridian host %s: %w", op, err)
	}
	ready, err := readMessage(sc)
	if err != nil {
		return failed(err)
	}
	switch {
	case ready.Problem != nil:
		return ready, ready.Problem
	case ready.Protocol != hostproto.Version:
		return failed(fmt.Errorf("it speaks the protocol of version %d, not %d", ready.Protocol, hostproto.Version))
	case !ready.Ready:
		return failed(errors.New("it didn't say it was ready"))
	}
	data, err := jsonv2.Marshal(req)
	if err != nil {
		return nil, err
	}
	sent = true
	if _, err := sc.Write(append(data, '\n')); err != nil {
		return failed(err)
	}
	_ = sc.CloseWrite()
	m, err := readMessage(sc)
	if err != nil {
		return failed(err)
	}
	// It exits 2 for a problem or an error, which m says.
	sc.Settle()
	switch {
	case m.Problem != nil:
		return m, m.Problem
	case m.Error != "":
		return m, fmt.Errorf("ssh: meridian host %s: %s", op, problems.EscapeControl(m.Error))
	}
	return m, nil
}

// An unsentError is an error of call's before it wrote its request, or of
// learning the host before it: nothing was asked of the host.
type unsentError struct{ err error }

func (e *unsentError) Error() string { return e.err.Error() }
func (e *unsentError) Unwrap() error { return e.err }

// unsent reports whether err is an *unsentError itself, as call and
// runOn return one, not one it wraps.
func unsent(err error) bool {
	_, ok := err.(*unsentError) //nolint:errorlint // the error's own layer, not what it wraps
	return ok
}

// sentOrNot returns err without what says it came before a request was
// written.
func sentOrNot(err error) error {
	for {
		u, ok := err.(*unsentError) //nolint:errorlint // the error's own layers, not what they wrap
		if !ok {
			return err
		}
		err = u.err
	}
}

// helperLine returns the last line of what meridian host wrote on its
// standard error that meridian wrote, as it writes a problem before it
// runs a command (meridian: and the problem), with its control characters
// escaped, or "" if there is none.
func helperLine(said string) string {
	var last string
	for line := range strings.Lines(said) {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "meridian: ") {
			last = line
		}
	}
	return problems.EscapeControl(last)
}

// readMessage reads what meridian host says until it is a message of its
// protocol's: a JSON object whose protocol isn't 0 (sshconn.Next).
func readMessage(sc *sshconn.Script) (*hostproto.Message, error) {
	m, err := sshconn.Next(sc, func(m *hostproto.Message) bool { return m.Protocol != 0 })
	if errors.Is(err, io.EOF) {
		return nil, errors.New("it ended without a message")
	}
	return m, err
}
