package sshconn

import (
	"context"
	"errors"
	"fmt"
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

// ErrForwardingRefused is the error of a host that answered no when asked
// to listen on a Unix socket for the connection, as sshd does where
// AllowStreamLocalForwarding or DisableForwarding forbids it, or where it
// can't listen at the path.
var ErrForwardingRefused = errors.New("sshconn: the host refused to forward a Unix socket")

// A Conn is a connection to a host.
type Conn struct {
	client  *ssh.Client
	closers []func()
	notes   []*problem.Problem
	// timeout is how long the host has to answer a keepalive.
	timeout time.Duration
	// sessions has a value for each session open.
	sessions chan struct{}
	// done is closed once the connection is, and err says why.
	done     chan struct{}
	doneOnce sync.Once
	err      error
}

func newConn(client *ssh.Client, closers []func(), notes []*problem.Problem, interval, timeout time.Duration, sessions int) *Conn {
	c := &Conn{
		client: client, closers: closers, notes: notes, timeout: timeout,
		sessions: make(chan struct{}, sessions), done: make(chan struct{}),
	}
	go func() {
		err := client.Wait()
		if err == nil {
			err = errors.New("sshconn: the host closed the connection")
		}
		c.end(err)
	}()
	go c.keepAlive(interval)
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
// the connection when it doesn't answer in time (ping).
func (c *Conn) keepAlive(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
		}
		if err := c.ping(); err != nil {
			c.end(err)
			return
		}
	}
}

// ping asks the host whether it is alive, and returns nil once it answers
// within the connection's timeout. Any answer will do: OpenSSH refuses the
// request, and so answers it. Otherwise it returns the request's error,
// ErrNotAlive when the host doesn't answer in time, or why the connection
// closed meanwhile.
func (c *Conn) ping() error {
	answered := make(chan error, 1)
	go func() {
		_, _, err := c.client.SendRequest("keepalive@openssh.com", true, nil)
		answered <- err
	}()
	wait := time.NewTimer(c.timeout)
	defer wait.Stop()
	select {
	case err := <-answered:
		return err
	case <-wait.C:
		return ErrNotAlive
	case <-c.done:
		return c.err
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
		s, err := c.open(ctx, release)
		if err == nil {
			return &Session{Session: s, release: release}, nil
		}
		if errors.Is(err, errOpenAbandoned) {
			// The place is given back once the open the host may still
			// answer has ended (open).
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
// then it is errOpenAbandoned, and the call that asks closes a session the
// host opens after, and only then calls release, which gives the
// session's place on the connection back, so that the connection never
// has more open than its limit. Where the connection has closed, the
// place is given back at once: nothing opens on it again, and the call
// can wait for ever, as x/crypto/ssh's can on a connection that closed as
// it asked.
func (c *Conn) open(ctx context.Context, release func()) (*ssh.Session, error) {
	type opened struct {
		s   *ssh.Session
		err error
	}
	var (
		mu        sync.Mutex
		abandoned bool
	)
	ch := make(chan opened, 1)
	go func() {
		s, err := c.client.NewSession()
		mu.Lock()
		defer mu.Unlock()
		if !abandoned {
			ch <- opened{s, err}
			return
		}
		if s != nil {
			_ = s.Close()
		}
		release()
	}()
	select {
	case o := <-ch:
		return o.s, o.err
	case <-ctx.Done():
	case <-c.done:
	}
	mu.Lock()
	defer mu.Unlock()
	select {
	case o := <-ch:
		// Answered as it was given up on.
		return o.s, o.err
	default:
	}
	abandoned = true
	if c.Err() != nil {
		release()
	}
	return nil, errOpenAbandoned
}

// ListenUnix asks the host to listen on a Unix socket at path, and to
// forward each connection to it over this connection
// (streamlocal-forward@openssh.com). A host that answers no is
// ErrForwardingRefused. x/crypto fails the request in words of its own
// when the host answers no, and with the transport's error when the
// connection is lost, so which it was is told by the connection, not the
// words: a host that answers a keepalive after is up, and refused.
func (c *Conn) ListenUnix(path string) (net.Listener, error) {
	l, err := c.client.ListenUnix(path)
	if err == nil || c.ping() != nil {
		return l, err
	}
	return nil, fmt.Errorf("%w: %w", ErrForwardingRefused, err)
}

// A Pool keeps one connection to each target, which its users share. It
// never makes its users wait on a target that isn't answering: while it
// dials a target, or waits to dial one again after failing to reach it,
// Get answers at once that it can't yet (DialError).
type Pool struct {
	Dialer *Dialer
	// DialTimeout bounds a dial in all, over the bounds the Dialer gives
	// each of its parts and the whole (Dialer.Dial), or 0 for none of the
	// pool's own. A dial that reaches it has failed.
	DialTimeout time.Duration

	mu      sync.Mutex
	entries map[string]*entry
	closed  bool
	// dials are the dials going on, which Close waits for.
	dials sync.WaitGroup
	// dialed, which a test sets, is called once a dial has ended, and
	// what it made, or how it failed, is recorded.
	dialed func()
}

// An entry is a pool's connection to a target, or how it last failed to
// make one: after how many failures in a row, and how many in all, with
// what error, and when it may try again; and, while it dials one, what
// closes once it is done, and what ends the dial.
type entry struct {
	conn     *Conn
	dialing  chan struct{}
	cancel   context.CancelFunc
	failures int
	failed   int
	lastErr  error
	retryAt  time.Time
}

// ErrDialing is a DialError's error while another user of the pool is
// connecting to the target, before any dial of it has failed.
var ErrDialing = errors.New("sshconn: the host is being connected to")

// A DialError is a pool's answer for a target it has no connection to:
// the error of its last dial of it, or ErrDialing before one has failed;
// how many of its dials in a row have failed, Failures, and how many in
// all, Failed, which only grows, so that what failed since a caller last
// asked is the difference. Waiting is set where Get didn't dial, as
// another user is dialing the target, or the pool is waiting to dial it
// again after failing to reach it, until RetryAt: what the caller asks of
// the host is to be asked again later.
type DialError struct {
	Err      error
	Failures int
	Failed   int
	Waiting  bool
	RetryAt  time.Time
}

func (e *DialError) Error() string { return e.Err.Error() }

func (e *DialError) Unwrap() error { return e.Err }

// How long a pool waits to dial a target again after it failed: from
// redialFirst, doubling with each failure in a row, up to redialMost, less
// up to a quarter at random, so that the connections a host's outage cut
// don't all come back at once.
const (
	redialFirst = time.Second
	redialMost  = 30 * time.Second
)

// Get returns the pool's connection to t. Where it has none that is open,
// it dials t, and waits for the dial until ctx is done: the dial goes on,
// bounded by the Dialer's own timeouts, and by DialTimeout in all, and
// what it makes is the pool's, for the next Get. While another Get's dial
// of t is going on, or before the backoff from failing to reach t has
// passed, it dials nothing, and answers at once, a *DialError that is
// Waiting. A dial that fails, or reaches DialTimeout, is a *DialError,
// which counts it. Only the pool's Close ends a dial early.
func (p *Pool) Get(ctx context.Context, t Target) (*Conn, error) {
	key := t.key()
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
	if e.dialing != nil || time.Now().Before(e.retryAt) {
		err := e.lastErr
		if err == nil {
			err = ErrDialing
		}
		de := &DialError{Err: err, Failures: e.failures, Failed: e.failed, Waiting: true}
		if e.dialing == nil {
			de.RetryAt = e.retryAt
		}
		p.mu.Unlock()
		return nil, de
	}
	done := make(chan struct{})
	dctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if p.DialTimeout > 0 {
		dctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), p.DialTimeout)
	}
	e.dialing, e.cancel = done, cancel
	p.dials.Add(1)
	p.mu.Unlock()
	go p.dial(dctx, e, t)
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.closed:
		return nil, ErrClosed
	case e.conn != nil && e.lastErr == nil:
		return e.conn, nil
	}
	return nil, &DialError{Err: e.lastErr, Failures: e.failures, Failed: e.failed}
}

// Failed returns how many of the pool's dials of t have failed, in all.
func (p *Pool) Failed(t Target) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.entries[t.key()]; e != nil {
		return e.failed
	}
	return 0
}

// dial dials t for the entry e, with ctx, which only DialTimeout and the
// pool's Close end, records what it made, or how it failed, and closes
// the entry's dialing once it has.
func (p *Pool) dial(ctx context.Context, e *entry, t Target) {
	defer p.dials.Done()
	if p.dialed != nil {
		defer p.dialed()
	}
	conn, err := p.Dialer.Dial(ctx, t)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("sshconn: %s: %w", t.Host, errDialTimeout)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e.cancel()
	defer close(e.dialing)
	e.dialing, e.cancel = nil, nil
	switch {
	case p.closed:
		// Closed while it dialed: what it dialed is closed too.
		if conn != nil {
			_ = conn.Close()
		}
		e.lastErr = ErrClosed
	case err == nil:
		e.conn, e.failures, e.lastErr, e.retryAt = conn, 0, nil, time.Time{}
	default:
		e.failures++
		e.failed++
		e.lastErr = err
		e.retryAt = time.Now().Add(redialAfter(e.failures))
	}
}

// errDialTimeout is the error of a dial that reached its pool's
// DialTimeout.
var errDialTimeout = errors.New("connecting took longer than the pool waits for a connection in all")

// redialAfter is how long to wait before dialing again after failures
// failures in a row.
func redialAfter(failures int) time.Duration {
	d := redialMost
	if failures < 6 {
		d = min(redialFirst<<(failures-1), redialMost)
	}
	return d - rand.N(d/4+1) //nolint:gosec // jitter, not a secret
}

// Close closes every connection of the pool's, ends the dials going on,
// and waits for them, closing what they made. It gets none after.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	for _, e := range p.entries {
		if e.conn != nil {
			_ = e.conn.Close()
		}
		if e.cancel != nil {
			e.cancel()
		}
	}
	p.mu.Unlock()
	p.dials.Wait()
	return nil
}
