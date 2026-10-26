// Package workerapisrv serves the worker API (package workerapi) for the
// coordinator, from its store: the claims of attempts, their heartbeats,
// control polls, step events, logs and ends (docs/design.md, The worker
// API).
//
// The server keeps nothing between requests but a broadcast that wakes the
// control polls it holds: each request reads the store, acts, and stores
// what it did, so any replica serves any attempt. It checks every write
// against the attempt's fence and credential in the transaction that
// stores it, so a superseded attempt's reports are refused (workerapi.Fenced),
// and nothing it sends is stored.
//
// It takes connections on a Unix socket only, and asks the system who each
// is from before it reads a request (Peers): a connection it doesn't trust
// is closed unread. It refuses a body larger than the API allows, 413, and
// reads every body strictly (workerapi.Unmarshal), 400.
//
// What it needs of the rest of the coordinator comes through narrow
// interfaces: the store; where logs go (Logs); the run's secret record,
// opened and sealed again (Sealer); the job an attempt claims, as Ready
// gives it again (Readier, which PlanReadier is from the plan); and whom
// to wake once an attempt changes (Options.Wake).
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

// recheck is how often a held control poll reads the store again, for a
// control another process stored, whose Notify doesn't reach this one.
const recheck = time.Second

// A LogKey names one log of an attempt, as coordinator.LogKey does.
type LogKey struct {
	Run string
	// Job is the job instance's key, and Attempt the attempt's number,
	// from 1.
	Job     string
	Attempt int
	// Part is which of the attempt's logs it is, and Step and Try the
	// step's index and its try, for a step's and a cleanup's.
	Part workerapi.LogPart
	Step int
	Try  int
}

// ErrLogGap is what Logs.Append returns for an offset past a log's end, or
// for bytes past the end of a log that has ended (coordinator.ErrLogGap).
var ErrLogGap = errors.New("workerapisrv: log write past its end")

// Logs stores the logs workers upload: the coordinator's Logs
// (coordinator.Logs), whose rules these are, with ErrLogGap for its gap.
// The package names it itself, since the coordinator, which serves the API
// through it, imports this one.
type Logs interface {
	// Append stores data as the part of the log at key that starts at
	// offset, skipping what the log has, and returns the log's length after
	// it, and whether it has ended; end ends it. An offset past the log's
	// end, or bytes past the end of one that has ended, return the length
	// and ErrLogGap.
	Append(ctx context.Context, key LogKey, offset int64, data []byte, end bool) (length int64, ended bool, err error)
}

// Sealer opens a run's secret record, and seals it again with a part added:
// *seal.Sealer.
type Sealer interface {
	Open(ctx context.Context, runID string, gen uint64, sealed seal.Sealed) (*plan.Record, error)
	Reseal(ctx context.Context, runID string, gen uint64, sealed seal.Sealed, rec *plan.Record) (seal.Sealed, uint64, error)
}

var _ Sealer = (*seal.Sealer)(nil)

// Options say what a Server serves from.
type Options struct {
	// Store is the coordinator's store.
	Store *store.Store
	// Logs stores the logs workers upload.
	Logs Logs
	// Sealer opens runs' secret records, and seals them again, as an
	// attempt's end adds its private outputs.
	Sealer Sealer
	// Readier gives the job an attempt claims, as Ready gives it again.
	Readier Readier
	// Peers says whom the server takes requests from, and for which runs.
	// Nil means LocalPeers: the process's own user, or root, for every run.
	Peers Peers
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
	store   *store.Store
	logs    Logs
	sealer  Sealer
	readier Readier
	peers   Peers
	wake    func(ctx context.Context, run, job string)
	log     *slog.Logger

	// reads is the newest plan version the server reads, and hold how long
	// it holds a control poll: plan.Version and
	// workerapi.ControlWaitSeconds, but in tests.
	reads int
	hold  time.Duration

	mu sync.Mutex
	// changed is closed, and replaced, by Notify.
	changed chan struct{}
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
		store: opts.Store, logs: opts.Logs, sealer: opts.Sealer, readier: opts.Readier, peers: opts.Peers, wake: opts.Wake, log: opts.Log,
		reads: plan.Version, hold: workerapi.ControlWaitSeconds * time.Second, changed: make(chan struct{}),
	}
	if s.peers == nil {
		s.peers = LocalPeers{}
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	return s, nil
}

// Notify wakes the control polls the server holds, to read the store again:
// call it once a control stored in this process has committed. A poll
// reads the store again every second besides, for a control another
// process stored.
func (s *Server) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.changed)
	s.changed = make(chan struct{})
}

// changes returns a channel that Notify closes.
func (s *Server) changes() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// Serve serves the worker API on l, a Unix socket's listener, until ctx is
// done, and then closes l. It takes a connection only from a peer that
// Peers accepts, as the system says who it is (peercred), and closes any
// other, and every connection that isn't a Unix socket's, before it reads
// a request from it.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if pc, ok := c.(*peerConn); ok {
				return context.WithValue(ctx, peerKey{}, pc.uid)
			}
			return ctx
		},
	}
	stop := context.AfterFunc(ctx, func() { srv.Close() })
	defer stop()
	err := srv.Serve(&peerListener{Listener: l, s: s})
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
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
// accepts.
type peerListener struct {
	net.Listener
	s *Server
}

// A peerConn is a connection a peerListener took, and the user it is from.
type peerConn struct {
	net.Conn
	uid int
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
		return &peerConn{Conn: c, uid: uid}, nil
	}
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
// checks in its own transaction.
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

// readBody reads the request's body, at most limit bytes, once it has
// answered one larger, 413.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
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
// OutOfOrder for events that skip one; and 500 for the rest, which the
// server logs, and which the worker tries again.
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
// of.
func writeStatus(w http.ResponseWriter, status int) {
	problem.Write(w, problem.Status(status, ""))
}

// writeJSON answers 200 with v, as the API encodes a body.
func writeJSON(w http.ResponseWriter, v any) {
	data, err := workerapi.Marshal(v)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
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
