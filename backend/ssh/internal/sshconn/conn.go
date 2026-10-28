package sshconn

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"
	"golang.org/x/crypto/ssh"
)

// What a connection does by default: how often it asks its host whether
// it is alive, and how long the host has to answer before the connection
// is closed, which finds a connection that stopped carrying anything,
// without being closed, long before TCP would; and how many sessions it
// opens at once, fewer than sshd's default MaxSessions, 10.
const (
	DefaultKeepAlive        = 15 * time.Second
	DefaultKeepAliveTimeout = 10 * time.Second
	DefaultSessions         = 8
)

// How long a session that sshd refused to open waits before it asks
// again: from refusedFirst, doubling, up to refusedMost.
const (
	refusedFirst = 100 * time.Millisecond
	refusedMost  = 5 * time.Second
)

// ErrClosed is the error of a connection closed by its user.
var ErrClosed = errors.New("sshconn: the connection is closed")

// ErrNotAlive is the error of a connection whose host didn't answer a
// keepalive in time.
var ErrNotAlive = errors.New("sshconn: the host stopped answering keepalives")

// A Conn is a connection to a host.
type Conn struct {
	client  *ssh.Client
	closers []func()
	notes   []*problem.Problem
	// sessions has a value for each session open.
	sessions chan struct{}
	// done is closed once the connection is, and err says why.
	done     chan struct{}
	doneOnce sync.Once
	err      error
}

func newConn(client *ssh.Client, closers []func(), notes []*problem.Problem, interval, timeout time.Duration, sessions int) *Conn {
	c := &Conn{client: client, closers: closers, notes: notes, sessions: make(chan struct{}, sessions), done: make(chan struct{})}
	go func() {
		err := client.Wait()
		if err == nil {
			err = errors.New("sshconn: the host closed the connection")
		}
		c.end(err)
	}()
	go c.keepAlive(interval, timeout)
	return c
}

// end closes the connection, for err, once.
func (c *Conn) end(err error) {
	c.doneOnce.Do(func() {
		c.err = err
		close(c.done)
		_ = c.client.Close()
		closeAll(c.closers)
	})
}

// keepAlive asks the host whether it is alive every interval, and closes
// the connection when it doesn't answer within timeout. Any answer will
// do: OpenSSH refuses the request, and so answers it.
func (c *Conn) keepAlive(interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
		}
		answered := make(chan error, 1)
		go func() {
			_, _, err := c.client.SendRequest("keepalive@openssh.com", true, nil)
			answered <- err
		}()
		wait := time.NewTimer(timeout)
		select {
		case err := <-answered:
			wait.Stop()
			if err != nil {
				c.end(err)
				return
			}
		case <-wait.C:
			c.end(ErrNotAlive)
			return
		case <-c.done:
			wait.Stop()
			return
		}
	}
}

// Notes are what is worth knowing about how the connection was made, such
// as a config there was no ssh client to read (problems.SSHConfigNotRead).
func (c *Conn) Notes() []*problem.Problem { return c.notes }

// Done is closed once the connection is.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err says why the connection closed, or is nil while it is open.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}

// Close closes the connection, and what it was made through.
func (c *Conn) Close() error {
	c.end(ErrClosed)
	return nil
}

// A Session is a session on a connection, which takes one of the
// connection's places until it is closed.
type Session struct {
	*ssh.Session
	release func()
}

// Close closes the session, and gives its place back.
func (s *Session) Close() error {
	err := s.Session.Close()
	s.release()
	return err
}

// NewSession opens a session, once fewer than the connection's limit are
// open. One sshd refuses, as for its MaxSessions, is asked for again, with
// a backoff, until ctx is done: a refusal says nothing of whether the
// connection is alive. It returns once ctx is done, or the connection
// closes, whether or not the host has answered: x/crypto/ssh waits for its
// answer with no bound, and for ever where the connection closed as it
// asked.
func (c *Conn) NewSession(ctx context.Context) (*Session, error) {
	select {
	case c.sessions <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.err
	}
	release := sync.OnceFunc(func() { <-c.sessions })
	wait := refusedFirst
	for {
		s, err := c.open(ctx)
		if err == nil {
			return &Session{Session: s, release: release}, nil
		}
		if errors.Is(err, errOpenAbandoned) {
			release()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, c.Err()
		}
		if _, refused := errors.AsType[*ssh.OpenChannelError](err); !refused {
			release()
			return nil, err
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			release()
			return nil, errors.Join(ctx.Err(), err)
		case <-c.done:
			timer.Stop()
			release()
			return nil, c.err
		}
		wait = min(wait*2, refusedMost)
	}
}

// errOpenAbandoned is open's error for a session it stopped waiting for.
var errOpenAbandoned = errors.New("sshconn: stopped waiting for the host to open a session")

// open asks the host for a session, as x/crypto/ssh's Client.NewSession
// does, but waits for it only until ctx is done, or the connection closes:
// then it is errOpenAbandoned, and a session the host opens after is
// closed.
func (c *Conn) open(ctx context.Context) (*ssh.Session, error) {
	type opened struct {
		s   *ssh.Session
		err error
	}
	ch := make(chan opened, 1)
	go func() {
		s, err := c.client.NewSession()
		ch <- opened{s, err}
	}()
	select {
	case o := <-ch:
		return o.s, o.err
	case <-ctx.Done():
	case <-c.done:
	}
	go func() {
		if o := <-ch; o.s != nil {
			_ = o.s.Close()
		}
	}()
	return nil, errOpenAbandoned
}

// ListenUnix asks the host to listen on a Unix socket at path, and to
// forward each connection to it over this connection
// (streamlocal-forward@openssh.com).
func (c *Conn) ListenUnix(path string) (net.Listener, error) {
	return c.client.ListenUnix(path)
}

// A Pool keeps one connection to each target, which its users share.
type Pool struct {
	Dialer *Dialer

	mu      sync.Mutex
	entries map[string]*entry
	closed  bool
}

// An entry is a pool's connection to a target, or how it last failed to
// make one: after how many failures in a row, and when it may try again.
type entry struct {
	conn     *Conn
	dialing  chan struct{}
	failures int
	retryAt  time.Time
}

// How long a pool waits to dial a target again after it failed: from
// redialFirst, doubling with each failure in a row, up to redialMost, less
// up to a quarter at random, so that the connections a host's outage cut
// don't all come back at once.
const (
	redialFirst = time.Second
	redialMost  = 30 * time.Second
)

// Get returns the pool's connection to t, and dials one if it has none
// that is open, once any backoff from failing to has passed. A dial ctx
// ended isn't counted as a failure.
func (p *Pool) Get(ctx context.Context, t Target) (*Conn, error) {
	key := t.key()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrClosed
		}
		if p.entries == nil {
			p.entries = map[string]*entry{}
		}
		e := p.entries[key]
		if e == nil {
			e = &entry{}
			p.entries[key] = e
		}
		if e.conn != nil && e.conn.Err() == nil {
			p.mu.Unlock()
			return e.conn, nil
		}
		wait := time.Until(e.retryAt)
		if dialing := e.dialing; dialing != nil || wait > 0 {
			p.mu.Unlock()
			if err := waitFor(ctx, dialing, wait); err != nil {
				return nil, err
			}
			continue
		}
		e.dialing = make(chan struct{})
		p.mu.Unlock()

		conn, err := p.Dialer.Dial(ctx, t)

		p.mu.Lock()
		close(e.dialing)
		e.dialing = nil
		switch {
		case p.closed:
			// Closed while it dialed: what it dialed is closed too.
			if conn != nil {
				_ = conn.Close()
			}
			conn, err = nil, ErrClosed
		case err == nil:
			e.conn, e.failures, e.retryAt = conn, 0, time.Time{}
		case ctx.Err() == nil:
			e.failures++
			e.retryAt = time.Now().Add(redialAfter(e.failures))
		}
		p.mu.Unlock()
		return conn, err
	}
}

// waitFor waits until dialing is closed, if it isn't nil, or else for d,
// or until ctx is done.
func waitFor(ctx context.Context, dialing chan struct{}, d time.Duration) error {
	if dialing != nil {
		select {
		case <-dialing:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// redialAfter is how long to wait before dialing again after failures
// failures in a row.
func redialAfter(failures int) time.Duration {
	d := redialMost
	if failures < 6 {
		d = min(redialFirst<<(failures-1), redialMost)
	}
	return d - rand.N(d/4+1) //nolint:gosec // jitter, not a secret
}

// Close closes every connection of the pool's, and those it is dialing
// once they are made. It gets none after.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, e := range p.entries {
		if e.conn != nil {
			_ = e.conn.Close()
		}
	}
	return nil
}
