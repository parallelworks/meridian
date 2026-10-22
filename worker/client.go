package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/workerapi"
)

// The errors that end an attempt without the worker: it stops the job's
// steps and exits, since nothing more it sends is stored.
var (
	// errUnauthorized is a 401 after the claim: the attempt has ended,
	// or, for a job that fences itself, its credential lapsed with its
	// lease.
	errUnauthorized = errors.New("worker: the coordinator no longer accepts the attempt's credential")
	// errFenced is a Fenced problem: another attempt of the job has
	// superseded this one.
	errFenced = errors.New("worker: another attempt of the job has superseded this one")
	// errPlanTooNew is a PlanTooNew problem: this worker doesn't read the
	// run's plan version. The attempt failed before its first step, and is
	// placed again.
	errPlanTooNew = errors.New("worker: the run's plan is newer than this worker reads")
)

// An outOfOrderError is an OutOfOrder problem: the coordinator expects
// events from another sequence number, or a log's bytes from another
// offset.
type outOfOrderError struct {
	expected int64
}

func (e *outOfOrderError) Error() string {
	return fmt.Sprintf("worker: the coordinator expects %d next", e.expected)
}

// A temporaryError is a request that failed without an answer, or with a
// status that asks to try again: a 5xx or a 429. Every request is safe to
// repeat.
type temporaryError struct {
	err error
}

func (e *temporaryError) Error() string { return e.err.Error() }
func (e *temporaryError) Unwrap() error { return e.err }

// A client makes the worker API's requests to one coordinator.
type client struct {
	base      string
	http      *http.Client
	transport *http.Transport
	// credential is the attempt's, the bearer token of every request
	// after the claim. It is set once the claim succeeds, before anything
	// else uses the client.
	credential string
	timing     timing
	log        *slog.Logger
	// peer checks who serves a Unix socket (checkPeer).
	peer func(*net.UnixConn, string) error
}

// newClient returns a client for the coordinator at ep: over a Unix socket,
// or over TLS 1.3 to a coordinator whose public key has ep's hash.
func newClient(ep endpoint, tm timing, log *slog.Logger) *client {
	c := &client{timing: tm, log: log, peer: checkPeer}
	t := &http.Transport{
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
	}
	base := "https://" + ep.address
	if ep.socket() {
		base = "http://meridian"
		t.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			conn, err := d.DialContext(ctx, "unix", ep.address)
			if err != nil {
				return nil, err
			}
			// The token and the credential go only to a coordinator of
			// this user's, or root's.
			if err := c.peer(conn.(*net.UnixConn), ep.address); err != nil {
				conn.Close()
				return nil, &peerError{err}
			}
			return conn, nil
		}
	} else {
		// HTTP/1.1, which a Transport with a TLS config of its own
		// speaks, rather than HTTP/2, which would send every request down
		// one connection, to one replica: after a replica's 503, the
		// worker drops its connections, and the next may reach another.
		t.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS13,
			// The coordinator is known by its key's hash, not by a
			// certificate authority: VerifyConnection checks the key, and
			// TLS checks that the coordinator holds it.
			InsecureSkipVerify: true, //nolint:gosec // VerifyConnection pins the coordinator's public key
			VerifyConnection:   pinned(ep.keyHash),
		}
	}
	// A redirect is refused: the worker talks only to the coordinator it
	// was given.
	refuse := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.base, c.http, c.transport = base, &http.Client{Transport: t, CheckRedirect: refuse}, t
	return c
}

// pinned returns a check that a TLS connection's peer holds the public key
// whose SHA-256 digest, of its DER SubjectPublicKeyInfo, is hash.
func pinned(hash []byte) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if cs.Version != tls.VersionTLS13 {
			return errors.New("worker: the coordinator's connection isn't TLS 1.3")
		}
		if len(cs.PeerCertificates) == 0 {
			return errors.New("worker: the coordinator sent no certificate")
		}
		got := sha256.Sum256(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
		if subtle.ConstantTimeCompare(got[:], hash) != 1 {
			return errKeyMismatch
		}
		return nil
	}
}

// errKeyMismatch is a TLS connection to a server whose key isn't the
// coordinator's. The worker never talks to it, and doesn't try again.
var errKeyMismatch = fmt.Errorf("worker: the server's TLS key isn't the one %s names", workerapi.EnvKeySHA256)

// A peerError is a Unix socket served by another user. The worker never
// talks to it, and doesn't try again.
type peerError struct{ err error }

func (e *peerError) Error() string { return e.err.Error() }
func (e *peerError) Unwrap() error { return e.err }

func (c *client) close() { c.transport.CloseIdleConnections() }

// A call is one request of the worker API.
type call struct {
	method string
	path   string
	query  url.Values
	// body is JSON, or a log's bytes when octets is set.
	body   []byte
	octets bool
	// bearer is the request's bearer token: the attempt's credential when
	// it is "".
	bearer string
	// timeout bounds each try of the request.
	timeout time.Duration
	// once is set for a request that isn't tried again when it fails.
	once bool
	// sent, if it isn't nil, is set to when the try the coordinator
	// answered was sent.
	sent *time.Time
}

// do makes a call, and decodes its answer into out, if out isn't nil. It
// tries again, with backoff, when the request fails without an answer or
// with a status that asks it to, until ctx is done, and otherwise returns
// errUnauthorized, errFenced, errPlanTooNew, an *outOfOrderError, or an
// error that says what the coordinator refused.
func (c *client) do(ctx context.Context, cl call, out any) error {
	for try := 0; ; try++ {
		err := c.send(ctx, cl, out)
		var temp *temporaryError
		if err == nil || cl.once || !errors.As(err, &temp) {
			return err
		}
		c.log.Debug("worker API request failed; trying again", "path", cl.path, "error", err)
		if err := sleep(ctx, c.timing.backoff(try)); err != nil {
			return err
		}
	}
}

// maxAnswer is the most bytes of an answer the client reads: a claim, with
// the job's part of the run, is the largest.
const maxAnswer = 256 << 20

func (c *client) send(ctx context.Context, cl call, out any) error {
	tctx, cancel := context.WithTimeout(ctx, cl.timeout)
	defer cancel()
	target := c.base + cl.path
	if len(cl.query) > 0 {
		target += "?" + cl.query.Encode()
	}
	req, err := http.NewRequestWithContext(tctx, cl.method, target, bytes.NewReader(cl.body))
	if err != nil {
		return err
	}
	bearer := cl.bearer
	if bearer == "" {
		bearer = c.credential
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Accept", "application/json, "+problem.MediaType)
	if cl.body != nil {
		if cl.octets {
			req.Header.Set("Content-Type", "application/octet-stream")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	if cl.sent != nil {
		*cl.sent = time.Now()
	}
	resp, err := c.http.Do(req)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, errKeyMismatch):
			return errKeyMismatch
		}
		if peer, ok := errors.AsType[*peerError](err); ok {
			return peer
		}
		return &temporaryError{err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &temporaryError{err}
	}
	switch status := resp.StatusCode; {
	case status >= 200 && status < 300:
		if out == nil {
			return nil
		}
		if err := workerapi.Unmarshal(data, out); err != nil {
			return fmt.Errorf("worker: reading the coordinator's answer to %s: %w", cl.path, err)
		}
		return nil
	case status == http.StatusServiceUnavailable:
		// A replica that doesn't serve this run, or none that can now: a
		// new connection may reach another, so none is kept, this one
		// included.
		resp.Body.Close()
		c.transport.CloseIdleConnections()
		return &temporaryError{fmt.Errorf("worker: %s: %s", cl.path, resp.Status)}
	case status == http.StatusTooManyRequests || status >= 500:
		return &temporaryError{fmt.Errorf("worker: %s: %s", cl.path, resp.Status)}
	case status == http.StatusUnauthorized:
		return errUnauthorized
	}
	var p problem.Problem
	if jsonv2.Unmarshal(data, &p) != nil {
		p = problem.Problem{Status: resp.StatusCode}
	}
	switch p.Code {
	case workerapi.Fenced:
		return errFenced
	case workerapi.PlanTooNew:
		return errPlanTooNew
	case workerapi.OutOfOrder:
		if expected, ok := intParam(p.Params[workerapi.ParamExpected]); ok {
			return &outOfOrderError{expected}
		}
	}
	return fmt.Errorf("worker: the coordinator refused %s: %s (%s): %s", cl.path, resp.Status, p.Key(), p.Detail)
}

// intParam returns a problem's param that is a whole number, as JSON gives
// it.
func intParam(v any) (int64, bool) {
	switch v := v.(type) {
	case float64:
		if v == float64(int64(v)) {
			return int64(v), true
		}
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	return 0, false
}

// The time each request may take: long enough for a claim's large answer,
// and for a control poll the coordinator holds.
const (
	requestTimeout = time.Minute
	controlTimeout = workerapi.ControlWaitSeconds*time.Second + requestTimeout
)

// claim claims the attempt token is for, as req, and returns the claim and
// when the worker sent the request the coordinator answered, from which
// the attempt's lease counts.
func (c *client) claim(ctx context.Context, token string, req workerapi.ClaimRequest) (*workerapi.Claim, time.Time, error) {
	var sent time.Time
	body, err := workerapi.Marshal(&req)
	if err != nil {
		return nil, sent, err
	}
	claim := new(workerapi.Claim)
	err = c.do(ctx, call{method: http.MethodPost, path: workerapi.PathClaim, body: body, bearer: token, timeout: requestTimeout, sent: &sent}, claim)
	if errors.Is(err, errUnauthorized) {
		err = errors.New("worker: the coordinator refused the bootstrap token: the attempt was claimed, or its placement withdrawn")
	}
	return claim, sent, err
}

// heartbeat renews the attempt's lease, once.
func (c *client) heartbeat(ctx context.Context, timeout time.Duration) error {
	return c.do(ctx, call{method: http.MethodPost, path: workerapi.PathHeartbeat, timeout: timeout, once: true}, nil)
}

// control waits for what the coordinator asks of the worker after version.
func (c *client) control(ctx context.Context, version int64) (*workerapi.Control, error) {
	ctl := new(workerapi.Control)
	q := url.Values{workerapi.QueryVersion: {strconv.FormatInt(version, 10)}}
	err := c.do(ctx, call{method: http.MethodGet, path: workerapi.PathControl, query: q, timeout: controlTimeout}, ctl)
	return ctl, err
}

// events reports events.
func (c *client) events(ctx context.Context, events []workerapi.Event) (workerapi.EventsAck, error) {
	var ack workerapi.EventsAck
	body, err := workerapi.Marshal(&workerapi.Events{Events: events})
	if err != nil {
		return ack, err
	}
	err = c.do(ctx, call{method: http.MethodPost, path: workerapi.PathEvents, body: body, timeout: requestTimeout}, &ack)
	return ack, err
}

// upload uploads data, a log's bytes from offset, ending the log if end is
// set.
func (c *client) upload(ctx context.Context, key logKey, offset int64, data []byte, end bool) (workerapi.LogAck, error) {
	var ack workerapi.LogAck
	q := url.Values{
		workerapi.QueryPart:   {string(key.part)},
		workerapi.QueryOffset: {strconv.FormatInt(offset, 10)},
	}
	if key.part != workerapi.LogSetup {
		q.Set(workerapi.QueryStep, strconv.Itoa(key.step))
		q.Set(workerapi.QueryTry, strconv.Itoa(key.try))
	}
	if end {
		q.Set(workerapi.QueryEnd, "true")
	}
	err := c.do(ctx, call{method: http.MethodPost, path: workerapi.PathLogs, query: q, body: data, octets: true, timeout: requestTimeout}, &ack)
	return ack, err
}

// complete reports the attempt's end.
func (c *client) complete(ctx context.Context, end *workerapi.Complete) error {
	body, err := workerapi.Marshal(end)
	if err != nil {
		return err
	}
	return c.do(ctx, call{method: http.MethodPost, path: workerapi.PathComplete, body: body, timeout: requestTimeout}, nil)
}

// timing holds how long the worker waits for what it waits for, which
// tests shorten.
type timing struct {
	// grace is how long a step has after SIGTERM before SIGKILL, unless
	// the coordinator's cancellation gives another.
	grace time.Duration
	// afterKill is how long the worker waits, after SIGKILL, for a step's
	// output to close, before it stops reading it.
	afterKill time.Duration
	// backoffMin and backoffMax bound the wait before a request is tried
	// again.
	backoffMin, backoffMax time.Duration
	// flush bounds how long a worker stopped from outside spends sending
	// what it has.
	flush time.Duration
}

var defaultTiming = timing{
	grace:      30 * time.Second,
	afterKill:  5 * time.Second,
	backoffMin: 250 * time.Millisecond,
	backoffMax: 30 * time.Second,
	flush:      10 * time.Second,
}

// backoff returns how long to wait before trying a request again after
// tries failures: doubling from backoffMin up to backoffMax, less up to a
// quarter at random, so that workers cut off together don't all come back
// at once.
func (t timing) backoff(tries int) time.Duration {
	d := t.backoffMax
	if tries < 30 {
		d = min(t.backoffMin<<tries, t.backoffMax)
	}
	return d - rand.N(d/4+1) //nolint:gosec // jitter, not a secret
}

// sleep waits for d, or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
