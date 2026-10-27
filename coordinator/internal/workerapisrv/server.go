// Package workerapisrv serves the worker API (package workerapi) for the
// coordinator, from its store: the claims of attempts, their heartbeats,
// control polls, step events, logs and ends (docs/design.md, The worker
// API).
//
// The server keeps nothing between requests but the control polls it
// holds: each request reads the store, acts, and stores what it did, so any
// replica serves any attempt. It checks every write against the attempt's
// fence and credential in the transaction that stores it, so a superseded
// attempt's reports are refused (workerapi.Fenced), and nothing it sends is
// stored.
//
// It takes connections on a Unix socket only, and asks the system who each
// is from before it reads a request (Peers): a connection it doesn't trust
// is closed unread. It holds at most a number of connections at once, and
// past it closes the one that has waited longest for a request, once it
// has waited a second, gives
// each request's headers and body a time to arrive in, at a least
// rate for its size, refuses a body larger than the API allows, 413, and
// reads every body strictly (workerapi.Unmarshal), 400. So a process of the
// user's, as every step is, can't hold the server's connections or
// handlers by sending slowly.
//
// What it needs of the rest of the coordinator comes through narrow
// interfaces: the store; where logs go (runlog.Logs); the run's secret
// record, opened and sealed again (Sealer); the job an attempt claims, as
// Ready gives it again (Readier, which the coordinator gives); and
// whom to wake once an attempt changes (Options.Wake).
package workerapisrv

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/runlog"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/peercred"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// The largest bodies the server reads, beside a log upload's, which is
// workerapi.MaxLogUpload: a claim is small; a worker sends at most 64
// events at once, with about 4 MiB of their outputs, but for one larger
// step's; and an attempt's end holds every output of its job's steps, each
// up to 1 MiB. A larger body is refused, 413.
const (
	MaxClaimBody    = 64 << 10
	MaxEventsBody   = 16 << 20
	MaxCompleteBody = 64 << 20
)

// What the server keeps of an attempt's reports at most: its events, by
// their number and the bytes they take, and the bytes of its logs, all of
// them together, of its setup, of each step's try to MaxLogTry, and of
// each step's cleanup. Events past them are refused, 413, as no worker of
// a job the plan bounds sends them. The log that reaches MaxLogBytes keeps
// its bytes to there, then a line that says the rest wasn't kept
// (problems.LogTruncated), and each of the attempt's logs after it keeps
// only that line; a try past MaxLogTry, which no workflow is likely to
// retry to, keeps no log; and the worker is told the rest is stored, so
// that it goes on. So the logs an attempt has are bounded too, by its job's
// steps.
const (
	MaxEvents      = 1 << 20
	MaxEventsBytes = 1 << 30
	MaxLogBytes    = 1 << 30
	MaxLogTry      = 1000
)

// The bounds on a request's time: its headers arrive within headerTime,
// and its body within bodyTime and as long as minRate takes for the body's
// limit; its answer is written within writeTime, past how long a control
// poll is held. maxHeaderBytes bounds its headers.
const (
	headerTime     = 10 * time.Second
	bodyTime       = 10 * time.Second
	minRate        = 1 << 20 // bytes a second
	writeTime      = time.Minute
	maxHeaderBytes = 16 << 10
)

// DefaultMaxConnections is how many connections a server holds at once
// unless Options.MaxConnections says another: far more than the workers of
// a machine's jobs hold, each a few.
const DefaultMaxConnections = 1024

// idleTime is how long a connection is kept between requests. A worker
// polls for control again as soon as a poll is answered, so its
// connections are seldom idle for long, and it connects again in place of
// one that was closed.
const idleTime = 30 * time.Second

// shutdownTime is how long Serve waits, once its context is done, for the
// requests it is handling to end. A handler that outlives it, as one
// waiting for the database's lock, goes on after Serve returns, and can
// meet the logs or the store that Serve's caller closes then. That is
// safe: what a request stores, it stores in one transaction, which then
// fails whole, and a log's part is answered only once it is stored; the
// worker, whose request fails, sends it again, to whichever coordinator
// serves the API next.
const shutdownTime = 5 * time.Second

// Sealer opens a run's secret record, and seals and opens its jobs' parts
// of it: *seal.Sealer.
type Sealer interface {
	Open(ctx context.Context, runID string, gen uint64, sealed seal.Sealed) (*plan.Record, error)
	SealJob(ctx context.Context, runID, job string, gen uint64, wrapped []byte, rec plan.JobRecord) ([]byte, error)
	OpenJobs(ctx context.Context, runID string, wrapped []byte, parts map[string]seal.JobPart) (map[string]plan.JobRecord, error)
}

var _ Sealer = (*seal.Sealer)(nil)

// Options say what a Server serves from.
type Options struct {
	// Store is the coordinator's store.
	Store *store.Store
	// Logs stores the logs workers upload: the coordinator's Logs.
	Logs runlog.Logs
	// Sealer opens runs' secret records, and seals each job's part of its
	// run's, as an attempt's end stores its private outputs.
	Sealer Sealer
	// Readier gives the job an attempt claims, as Ready gives it again.
	Readier Readier
	// Peers says whom the server takes requests from, and for which runs.
	// Nil means LocalPeers: the process's own user, or root, for every run.
	Peers Peers
	// MaxConnections is how many connections the server holds at once, or
	// DefaultMaxConnections for 0. Another closes the one that has waited
	// longest for a request, once that one has waited a second, and until
	// one has, as while every one is handling a request, waits.
	MaxConnections int
	// Wake, if it isn't nil, is called once the server has stored a change
	// to an attempt that its job's supervisor acts on: its claim, its
	// first step's start, and its end, before its first step or after.
	Wake func(ctx context.Context, run, job string)
	// Log hears of what the server does that no answer says, such as a
	// connection it refused, or an error of its own. Nil means none.
	Log *slog.Logger
}

// A Server serves the worker API. Its methods are safe for concurrent use.
type Server struct {
	store    *store.Store
	logs     runlog.Logs
	sealer   Sealer
	readier  Readier
	peers    Peers
	maxConns int
	wake     func(ctx context.Context, run, job string)
	log      *slog.Logger
	polls    *polls
	// placed are the first claims that wait for their placements to be
	// recorded, by attempt (awaitPlaced).
	placed struct {
		sync.Mutex
		waits map[string]*placedWaits
	}

	// What tests change: the newest plan version the server reads, how
	// long it holds a control poll, how long it holds a first claim for
	// its placement (awaitPlaced), and how many bytes of a log it keeps.
	reads          int
	hold           time.Duration
	placedWait     time.Duration
	placedRecheck  time.Duration
	maxLogBytes    int64
	maxEvents      int64
	maxEventsBytes int64
	headerTime     time.Duration
	bodyTime       time.Duration
	// beforeEnd, if it is set, is called as an end is stored, between its
	// checks and its transaction.
	beforeEnd func()
}

// New returns a Server that serves from opts.
func New(opts Options) (*Server, error) {
	switch {
	case opts.Store == nil:
		return nil, errors.New("workerapisrv: no store")
	case opts.Logs == nil:
		return nil, errors.New("workerapisrv: no Logs")
	case opts.Sealer == nil:
		return nil, errors.New("workerapisrv: no Sealer")
	case opts.Readier == nil:
		return nil, errors.New("workerapisrv: no Readier")
	}
	s := &Server{
		store: opts.Store, logs: opts.Logs, sealer: opts.Sealer, readier: opts.Readier, peers: opts.Peers,
		maxConns: opts.MaxConnections, wake: opts.Wake, log: opts.Log,
		reads: plan.Version, hold: workerapi.ControlWaitSeconds * time.Second, placedWait: placedWait, placedRecheck: placedRecheck,
		maxLogBytes: MaxLogBytes, maxEvents: MaxEvents, maxEventsBytes: MaxEventsBytes, headerTime: headerTime, bodyTime: bodyTime,
	}
	if s.peers == nil {
		s.peers = LocalPeers{}
	}
	if s.maxConns <= 0 {
		s.maxConns = DefaultMaxConnections
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	s.polls = newPolls(s.store, s.log)
	return s, nil
}

// Notify wakes the control polls the server holds for the attempts ids, or
// for every attempt for none, to read the store again. Whoever opened the
// store calls it once a transaction of this process's that changed
// controls has committed, its own and the server's alike
// (store.Options.OnControlsChanged). The server reads the controls of the
// attempts it holds polls for again every second besides, for one another
// process stored.
func (s *Server) Notify(ids ...string) { s.polls.wake(ids) }

// Placed answers the first claims of the attempts ids that s holds until
// their placements are recorded, once they have been: the coordinator that
// recorded them calls it, once the transaction that recorded them has
// committed (store.Options.OnPlacementsRecorded, awaitPlaced).
func (s *Server) Placed(ids ...string) {
	s.placed.Lock()
	defer s.placed.Unlock()
	for _, id := range ids {
		if w := s.placed.waits[id]; w != nil {
			close(w.ch)
			delete(s.placed.waits, id)
		}
	}
}

// Serve serves the worker API on l, a Unix socket's listener, until ctx is
// done, and then closes l, once the requests it is handling have ended,
// for a short while. It takes a connection only from a peer that Peers
// accepts, as the system says who it is (peercred), and closes any other,
// and every connection that isn't a Unix socket's, before it reads a
// request from it.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	conns := newConnSet(s.maxConns, evictAfter)
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: s.headerTime,
		ReadTimeout:       s.headerTime + s.bodyTime,
		IdleTimeout:       idleTime,
		MaxHeaderBytes:    maxHeaderBytes,
		ConnState:         conns.setState,
		// The requests' context ends with ctx, which ends the control
		// polls held, so that a shutdown waits for none of them.
		BaseContext: func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if pc, ok := c.(*peerConn); ok {
				return context.WithValue(ctx, peerKey{}, pc.uid)
			}
			return ctx
		},
	}
	shut := make(chan error, 1)
	stop := context.AfterFunc(ctx, func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTime)
		defer cancel()
		err := srv.Shutdown(sctx)
		if err != nil {
			err = errors.Join(err, srv.Close())
		}
		shut <- err
	})
	err := srv.Serve(&peerListener{Listener: l, s: s, conns: conns})
	if stop() {
		// Serve ended on its own, before ctx: nothing shuts it down.
		return err
	}
	if serr := <-shut; serr != nil {
		s.log.Warn("workerapisrv: requests outlived the shutdown", "error", serr)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// peerKey is the context key of the user a request's connection is from.
type peerKey struct{}

// peerOf returns the user the request's connection is from, as Serve's
// listener asked the system, and false for a request that didn't come
// through it, which the server refuses.
func peerOf(r *http.Request) (int, bool) {
	uid, ok := r.Context().Value(peerKey{}).(int)
	return uid, ok
}

// A peerListener takes only connections from the peers its server
// accepts, and holds at most as many at once as conns does.
type peerListener struct {
	net.Listener
	s     *Server
	conns *connSet
}

// A peerConn is a connection a peerListener took, and the user it is from.
// Closing it takes it out of its set.
type peerConn struct {
	net.Conn
	uid int
	set *connSet
}

func (c *peerConn) Close() error {
	err := c.Conn.Close()
	c.set.remove(c)
	return err
}

func (l *peerListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		uc, ok := c.(*net.UnixConn)
		if !ok {
			l.s.log.Warn("workerapisrv: refused a connection that isn't a Unix socket's", "remote", c.RemoteAddr().String())
			c.Close()
			continue
		}
		uid, err := peercred.UID(uc)
		if err != nil {
			l.s.log.Warn("workerapisrv: refused a connection whose user the system doesn't say", "error", err)
			c.Close()
			continue
		}
		if !l.s.peers.Accept(uid) {
			l.s.log.Warn("workerapisrv: refused a connection from another user", "uid", uid)
			c.Close()
			continue
		}
		pc := &peerConn{Conn: c, uid: uid, set: l.conns}
		if !l.conns.add(pc) {
			c.Close()
			return nil, net.ErrClosed
		}
		return pc, nil
	}
}

// Close closes the listener, and ends an Accept that waits for room.
func (l *peerListener) Close() error {
	l.conns.close()
	return l.Listener.Close()
}

// A connSet holds the connections a server's listener took, at most max
// at once, with whether each is waiting for a request, as the HTTP server
// says (http.Server.ConnState): idle between requests, or new, before its
// first. Past max, a new connection closes the one that has waited longest,
// once it has waited grace, rather than wait for one to close, so that
// connections that send nothing can keep a worker out for grace at most.
// Until one has waited that long, as while every one is handling a request,
// the new one waits. grace is time enough for a connection that has just
// connected to send its request, and for one just answered to send its
// next, so that the set never closes one of a worker's before it could.
type connSet struct {
	max    int
	grace  time.Duration
	mu     sync.Mutex
	room   *sync.Cond
	held   map[*peerConn]connState
	closed bool
}

// evictAfter is how long a connection waits for a request before a new
// one may take its place, past the server's limit (connSet).
const evictAfter = time.Second

// connState is whether a connection is waiting for a request, and since
// when.
type connState struct {
	waiting bool
	since   time.Time
}

func newConnSet(maxConns int, grace time.Duration) *connSet {
	cs := &connSet{max: maxConns, grace: grace, held: map[*peerConn]connState{}}
	cs.room = sync.NewCond(&cs.mu)
	return cs
}

// add adds c, once there is room for it, and reports whether it did: not
// once the set is closed.
func (cs *connSet) add(c *peerConn) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for !cs.closed {
		if len(cs.held) < cs.max {
			cs.held[c] = connState{waiting: true, since: time.Now()}
			return true
		}
		oldest, wait := cs.longestWaiting()
		switch {
		case oldest != nil:
			delete(cs.held, oldest)
			// Closed outside the lock, which its Close takes.
			cs.mu.Unlock()
			oldest.Close()
			cs.mu.Lock()
		case wait > 0:
			// Woken once the one that has waited longest has waited grace,
			// unless a connection is closed, or is answered, first.
			t := time.AfterFunc(wait, cs.wake)
			cs.room.Wait()
			t.Stop()
		default:
			cs.room.Wait()
		}
	}
	return false
}

// longestWaiting returns the connection that has waited longest for a
// request, once it has waited grace; or else how long until it has, or 0
// while none waits.
func (cs *connSet) longestWaiting() (*peerConn, time.Duration) {
	var oldest *peerConn
	for held, st := range cs.held {
		if st.waiting && (oldest == nil || st.since.Before(cs.held[oldest].since)) {
			oldest = held
		}
	}
	if oldest == nil {
		return nil, 0
	}
	if waited := time.Since(cs.held[oldest].since); waited < cs.grace {
		return nil, cs.grace - waited
	}
	return oldest, 0
}

// wake wakes an add that waits, once it waits: add holds the lock until it
// does.
func (cs *connSet) wake() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.room.Broadcast()
}

// remove takes c out of the set, which makes room.
func (cs *connSet) remove(c *peerConn) {
	cs.mu.Lock()
	delete(cs.held, c)
	cs.mu.Unlock()
	cs.room.Broadcast()
}

// setState records the state the HTTP server says c is in.
func (cs *connSet) setState(c net.Conn, state http.ConnState) {
	pc, ok := c.(*peerConn)
	if !ok {
		return
	}
	switch state {
	case http.StateActive:
		cs.mu.Lock()
		if _, ok := cs.held[pc]; ok {
			cs.held[pc] = connState{}
		}
		cs.mu.Unlock()
	case http.StateIdle:
		cs.mu.Lock()
		if _, ok := cs.held[pc]; ok {
			cs.held[pc] = connState{waiting: true, since: time.Now()}
		}
		cs.mu.Unlock()
		cs.room.Broadcast()
	case http.StateHijacked, http.StateClosed:
		cs.remove(pc)
	}
}

// close ends an add that waits for room, and every add after.
func (cs *connSet) close() {
	cs.mu.Lock()
	cs.closed = true
	cs.mu.Unlock()
	cs.room.Broadcast()
}

// Peers says whom a server takes requests from. A coordinator that runs as
// its user, as meridian's does, takes them from the user and root
// (LocalPeers). One that a service account or root runs for several users,
// as a platform's can, maps a connection's user to the user it acts for,
// and takes from each only what is of that user's runs.
type Peers interface {
	// Accept reports whether to take requests on a connection from the
	// user whose ID is uid, as the system says.
	Accept(uid int) bool
	// Allows reports whether a request on a connection from uid may claim,
	// or report on, an attempt of run. One it doesn't is refused, 401, as
	// one with a credential that is no attempt's is.
	Allows(ctx context.Context, uid int, run *store.Run) bool
}

// LocalPeers are the peers of a coordinator that runs as its user: the
// user, or root (peercred.Trusted), for every run of its store, which only
// the user can read.
type LocalPeers struct{}

// Accept reports whether uid is the process's user, or root.
func (LocalPeers) Accept(uid int) bool { return peercred.Trusted(uid) }

// Allows allows every run of the store: they are all the user's.
func (LocalPeers) Allows(context.Context, int, *store.Run) bool { return true }

// methods are the method of each of the API's paths.
var methods = map[string]string{
	workerapi.PathClaim:     http.MethodPost,
	workerapi.PathHeartbeat: http.MethodPost,
	workerapi.PathControl:   http.MethodGet,
	workerapi.PathEvents:    http.MethodPost,
	workerapi.PathLogs:      http.MethodPost,
	workerapi.PathComplete:  http.MethodPost,
}

// ServeHTTP serves one request of the worker API, which came on a
// connection Serve took.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The answer is written in time, or not at all: a worker that reads it
	// slowly holds nothing past it.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(s.hold + writeTime))
	method, ok := methods[r.URL.Path]
	switch {
	case !ok:
		writeStatus(w, http.StatusNotFound)
		return
	case r.Method != method:
		w.Header().Set("Allow", method)
		writeStatus(w, http.StatusMethodNotAllowed)
		return
	}
	uid, ok := peerOf(r)
	if !ok {
		// Not through Serve, whose listener asks who a connection is
		// from: it is no peer's the server trusts.
		writeStatus(w, http.StatusUnauthorized)
		return
	}
	if r.URL.Path == workerapi.PathClaim {
		s.serveClaim(w, r, uid)
		return
	}
	a, run, ok := s.attempt(w, r, uid)
	if !ok {
		return
	}
	switch r.URL.Path {
	case workerapi.PathHeartbeat:
		s.serveHeartbeat(w, r)
	case workerapi.PathControl:
		s.serveControl(w, r, a)
	case workerapi.PathEvents:
		s.serveEvents(w, r, a)
	case workerapi.PathLogs:
		s.serveLogs(w, r, a, run)
	case workerapi.PathComplete:
		s.serveComplete(w, r, a, run)
	}
}

// bearer returns the request's bearer token, or "".
func bearer(r *http.Request) string {
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return token
}

// attempt returns the attempt whose credential the request bears, and its
// run, once it has answered a request it refuses: 401 for a credential
// that is no attempt's, of a run the peer may not report on, or that has
// lapsed with a lease, and 503 for a run whose plan is of a version the
// server doesn't read. Whether the attempt still holds its job, its write
// checks in its own transaction. Nothing of the request's body is read
// before.
func (s *Server) attempt(w http.ResponseWriter, r *http.Request, uid int) (*store.Attempt, *store.Run, bool) {
	ctx := r.Context()
	credential := bearer(r)
	if credential == "" {
		writeStatus(w, http.StatusUnauthorized)
		return nil, nil, false
	}
	var (
		a      *store.Attempt
		run    *store.Run
		lapsed bool
	)
	err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if a, err = tx.AttemptByCredential(credential); err != nil {
			return err
		}
		if run, err = tx.Run(a.Run); err != nil {
			return err
		}
		lapsed, err = tx.Lapsed(a)
		return err
	})
	switch {
	case err != nil:
		s.writeError(w, r, err)
		return nil, nil, false
	case run.PlanVersion > s.reads:
		writeStatus(w, http.StatusServiceUnavailable)
		return nil, nil, false
	case !s.peers.Allows(ctx, uid, run), lapsed:
		writeStatus(w, http.StatusUnauthorized)
		return nil, nil, false
	}
	return a, run, true
}

// readBody reads the request's body, at most limit bytes, in the time a
// body of limit bytes takes at minRate, once it has answered one larger,
// 413, or one that didn't arrive in time.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(s.bodyTime + time.Duration(limit/minRate)*time.Second))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeStatus(w, http.StatusRequestEntityTooLarge)
		} else {
			writeStatus(w, http.StatusBadRequest)
		}
		return nil, false
	}
	return body, true
}

// writeError answers with what err says of the request: 401 for a token or
// a credential that is no attempt's, or another credential, or one that
// has lapsed; Fenced for an attempt that no longer holds its job;
// OutOfOrder for events that skip one; 503 for a run of a plan version
// this release doesn't read, which another replica may; and 500 for the
// rest, which the server logs, and which the worker tries again.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	if gap, ok := errors.AsType[*store.GapError](err); ok {
		problem.Write(w, outOfOrder(gap.Expected))
		return
	}
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrCredential), errors.Is(err, store.ErrLapsed):
		writeStatus(w, http.StatusUnauthorized)
	case errors.Is(err, store.ErrFenced):
		problem.Write(w, fenced())
	case errors.Is(err, plan.ErrVersion):
		writeStatus(w, http.StatusServiceUnavailable)
	default:
		s.log.ErrorContext(r.Context(), "workerapisrv: a request failed", "path", r.URL.Path, "error", err)
		writeStatus(w, http.StatusInternalServerError)
	}
}

// writeStatus answers with an about:blank problem: one its status says all
// of. A refusal closes the connection after it, so that a body the server
// didn't read isn't read to its end, however slowly it comes.
func writeStatus(w http.ResponseWriter, status int) {
	if status >= 400 {
		w.Header().Set("Connection", "close")
	}
	problem.Write(w, problem.Status(status, ""))
}

// writeJSON answers 200 with v, as the API encodes a body.
func writeJSON(w http.ResponseWriter, v any) {
	if err := workerapi.WriteBody(w, v); err != nil {
		writeStatus(w, http.StatusInternalServerError)
	}
}

// wakeJob calls Options.Wake, if there is one.
func (s *Server) wakeJob(ctx context.Context, run, job string) {
	if s.wake != nil {
		s.wake(context.WithoutCancel(ctx), run, job)
	}
}

// badRequest answers 400, and logs why, which the answer doesn't say. why
// is the server's own text: never what decoding a body said, which can
// quote a credential or a value a worker reported.
func (s *Server) badRequest(w http.ResponseWriter, r *http.Request, why string) {
	s.log.DebugContext(r.Context(), "workerapisrv: refused a request it can't read", "path", r.URL.Path, "why", why)
	writeStatus(w, http.StatusBadRequest)
}

// fenced is the problem of a request from an attempt that no longer holds
// its job.
func fenced() *problem.Problem { return problems.Fenced.Problem(nil) }

// outOfOrder is the problem of a report that skips what the server expects
// next, expected.
func outOfOrder(expected int64) *problem.Problem {
	return problems.OutOfOrder.Problem(problems.Params{workerapi.ParamExpected: expected})
}
