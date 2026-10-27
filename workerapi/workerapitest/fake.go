// Package workerapitest helps test both sides of the worker API: Fake, a
// coordinator that serves one attempt in memory, which the worker's tests
// run against, and Conformance, a suite that plays a worker against a
// coordinator, the Fake among them, and checks that it serves the API as
// package workerapi says.
package workerapitest

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A Fault is what a Fake does to a request instead of answering it, for a
// test of how a worker copes.
type Fault int

const (
	// Answer answers the request.
	Answer Fault = iota
	// Drop closes the connection without reading the request.
	Drop
	// DropAnswer handles the request, and closes the connection without
	// answering it: the worker can't know that it was stored.
	DropAnswer
	// Unavailable answers 503.
	Unavailable
	// Fence answers that the attempt was superseded.
	Fence
	// Unauthorized answers that the attempt's credential isn't accepted.
	Unauthorized
	// Forget forgets the log the request uploads to, or the events it
	// stored, or that the attempt's end follows, as a coordinator that lost
	// what it had stored would, then handles the request.
	Forget
	// Redirect answers with a redirect to the same path.
	Redirect
	// Hang holds the request until the worker gives up on it, as a
	// connection that is half open would.
	Hang
	// Refuse answers with a problem of the coordinator's own (Refusal).
	Refuse
	// RefuseBlank answers with an about:blank problem whose detail holds
	// an escape sequence.
	RefuseBlank
	// BadRequest answers 400 with no problem.
	BadRequest
	// RefuseHostile answers with a problem whose code isn't one.
	RefuseHostile
	// RefuseNested answers with a problem of a known kind whose param holds
	// an escape sequence in a list.
	RefuseNested
	// RefuseLarge answers with a problem of LargeRefusal bytes.
	RefuseLarge
	// WrongClaim answers a claim with one whose run's ID is a number.
	WrongClaim
	// Garble answers 200 with a body that isn't JSON.
	Garble
	// AckAhead answers events with an acknowledgement of events past
	// those the worker sent.
	AckAhead
)

// Refusal is the code of the problem a Fake refuses a request with for
// Refuse, which isn't a kind's.
const Refusal problem.Code = "request_refused"

// LargeRefusal is about how large a Fake's problem is for RefuseLarge: far
// more than a worker reads of a refusal.
const LargeRefusal = 1 << 20

// ControlHold is how long a Fake holds a control poll for something newer
// before it answers with what it has: far less than a coordinator may
// (workerapi.ControlWaitSeconds), so that a test runs quickly.
const ControlHold = 200 * time.Millisecond

// A Fake serves the worker API for one attempt, in memory, as a
// coordinator does, strictly: it refuses events out of order, logs past
// their ends or of steps the job doesn't have, a different Complete or one
// that says what only the coordinator says, and every write once the
// attempt is superseded or has ended, with the API's problems; and it
// takes any request again. Set its exported fields before it serves.
type Fake struct {
	// Token is the attempt's bootstrap token.
	Token string
	// Claim is what the Fake answers a claim with. Its PlanVersion is the
	// run's. Its APIVersion, unless a test sets it, is what the Fake
	// serves the attempt at, as its first claim decides it: the lower of
	// the worker's and Speaks. Its Control, unless a test sets it, is the
	// Fake's control as of each claim, in an attempt of version 2 or later.
	Claim workerapi.Claim
	// Reads is the newest plan version the Fake reads, as a replica's
	// release does, or plan.Version for 0. It answers 503 for a run of a
	// newer one, and refuses a worker that reads only an older one than the
	// run's with plan_too_new.
	Reads int
	// Speaks is the newest version of the API the Fake speaks, as a
	// coordinator's release does, or workerapi.Version for 0; and Needs
	// the oldest the attempt needs, or 1 for 0. It refuses a worker whose
	// version is older than Needs with worker_api_too_old.
	Speaks, Needs int
	// Fault, if set, says what to do to each request.
	Fault func(r *http.Request) Fault
	// OnEvent, if set, is called with each event stored, before the worker
	// has the acknowledgement.
	OnEvent func(e workerapi.Event)
	// ControlAtOnce makes the Fake answer a control poll at once, rather
	// than hold it until there is something newer.
	ControlAtOnce bool
	// AnyVersion makes the Fake answer a claim whatever the versions of the
	// run's plan and the worker, of the plan and of the API, as a
	// coordinator with that bug would, for a test of what a worker makes of
	// a job it can't read.
	AnyVersion bool
	// Lenient makes the Fake answer a request that no worker sends, as one
	// it can't read, with its status alone. Otherwise such a request fails
	// the test, since a worker sent it. Conformance sends them on purpose.
	Lenient bool
	// Act, if set, performs the built-in actions the worker asks for, and
	// returns each one's answer; without it, every action succeeds. The
	// Fake calls it once for a step's try, with its lock held, and answers
	// a repeat of the request as it answered the first. An answer's Reason
	// stands for a cancel of the step's job the action made.
	Act func(req workerapi.ActionRequest) workerapi.ActionAnswer

	t          testing.TB
	mu         sync.Mutex
	credential [32]byte
	claimed    bool
	superseded bool
	lapsed     bool
	withdrawn  bool
	events     []workerapi.Event
	logs       map[LogKey]*Log
	end        *workerapi.Complete
	endBody    []byte
	control    workerapi.Control
	heartbeats int
	requests   map[string]int
	actions    []Action
	// changed is closed, and replaced, on each change.
	changed chan struct{}
}

// An Action is a built-in action a Fake performed: the request that asked
// for it, as the worker sent it the first time, and its answer.
type Action struct {
	Request workerapi.ActionRequest
	Answer  workerapi.ActionAnswer
	// body is the request as the API writes it, which a repeat matches.
	body []byte
}

// A LogKey names one of an attempt's logs: its part, and a step's index
// and try, for a step's or a cleanup's.
type LogKey struct {
	Part      workerapi.LogPart
	Step, Try int
}

// A Log is what a coordinator stored of a log: its bytes, and whether it
// has ended.
type Log struct {
	Data  []byte
	Ended bool
}

// Stored is what a coordinator stored of an attempt: its events, its logs
// and its end, nil until it has one.
type Stored struct {
	Events   []workerapi.Event
	Logs     map[LogKey]Log
	Complete *workerapi.Complete
}

// NewFake returns a Fake that serves an attempt of job, as attempt1 of
// run1, with a run directory in t's.
func NewFake(t testing.TB, job *plan.ReadyJob) *Fake {
	t.Helper()
	data, err := plan.EncodeReadyJob(job)
	if err != nil {
		t.Fatal(err)
	}
	return &Fake{
		Token: "bootstrap-token",
		Claim: workerapi.Claim{
			RunID: "run1", AttemptID: "attempt1", PlanVersion: plan.Version, Job: data, Attempt: 1,
			Group: "local", Member: "laptop", LeaseSeconds: 60, HeartbeatSeconds: 1,
			Run:    plan.RunValues{Format: 1},
			RunDir: filepath.Join(t.TempDir(), "run"),
		},
		t:        t,
		logs:     map[LogKey]*Log{},
		requests: map[string]int{},
		changed:  make(chan struct{}),
	}
}

// Serve serves f on a Unix socket until the test ends, and returns the
// socket's path.
func (f *Fake) Serve() string {
	f.t.Helper()
	// A socket's path is short: t.TempDir's can be too long.
	dir, err := os.MkdirTemp("", "mw")
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "c.sock")
	l, err := new(net.ListenConfig).Listen(f.t.Context(), "unix", socket)
	if err != nil {
		f.t.Fatal(err)
	}
	srv := &httptest.Server{Listener: l, Config: &http.Server{Handler: f, ReadHeaderTimeout: 10 * time.Second}}
	srv.Start()
	f.t.Cleanup(srv.Close)
	return socket
}

// SetControl asks something more of the worker: c, with the next version.
func (f *Fake) SetControl(c workerapi.Control) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c.Version = f.control.Version + 1
	f.control = c
	f.notify()
}

// Supersede supersedes the attempt with another of its job: every write of
// its from then on is refused as fenced.
func (f *Fake) Supersede() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.superseded = true
	f.notify()
}

// Lapse lets the attempt's lease lapse. For a job that fences itself, its
// credential lapses with it, and is refused from then on, 401; another
// job's lasts until the attempt ends, so that a worker cut off from the
// coordinator delivers what it kept.
func (f *Fake) Lapse() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lapsed = true
	f.notify()
}

// Withdraw withdraws the attempt's placement before it is claimed: its
// bootstrap token is refused from then on, 401.
func (f *Fake) Withdraw() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.withdrawn = true
	f.notify()
}

// Stored returns a copy of what f stored.
func (f *Fake) Stored() Stored {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := Stored{Events: slices.Clone(f.events), Logs: map[LogKey]Log{}}
	for k, l := range f.logs {
		s.Logs[k] = Log{Data: slices.Clone(l.Data), Ended: l.Ended}
	}
	if f.end != nil {
		end := *f.end
		s.Complete = &end
	}
	return s
}

// Events returns the events stored, in order.
func (f *Fake) Events() []workerapi.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

// Log returns a log, as text, and whether it has ended.
func (f *Fake) Log(key LogKey) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.logs[key]
	if l == nil {
		return "", false
	}
	return string(l.Data), l.Ended
}

// End returns the attempt's end, or nil before the worker completes it.
func (f *Fake) End() *workerapi.Complete {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.end
}

// Claimed reports whether the attempt has been claimed.
func (f *Fake) Claimed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimed
}

// Requests returns how many requests f has handled, by path, those of
// unknown paths too.
func (f *Fake) Requests() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.requests)
}

// Actions returns the actions f performed, in order: each once, however
// often the worker asked.
func (f *Fake) Actions() []Action {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.actions)
}

// Heartbeats returns how many heartbeats f has answered.
func (f *Fake) Heartbeats() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heartbeats
}

// notify wakes whoever waits for a change. Its caller holds f.mu.
func (f *Fake) notify() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// unexpected fails the test for a request no worker sends, unless f is
// Lenient.
func (f *Fake) unexpected(format string, args ...any) {
	if !f.Lenient {
		f.t.Errorf(format, args...)
	}
}

// ServeHTTP serves the worker API for f's attempt.
func (f *Fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flt := Answer
	if f.Fault != nil {
		flt = f.Fault(r)
	}
	switch flt {
	case Drop:
		hangUp(w)
		return
	case Unavailable:
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	case Fence:
		problem.Write(w, problems.Fenced.Problem(nil))
		return
	case Unauthorized:
		w.WriteHeader(http.StatusUnauthorized)
		return
	case Refuse:
		writeProblem(w, http.StatusUnprocessableEntity, Refusal, map[string]any{"path": r.URL.Path})
		return
	case RefuseBlank:
		problem.Write(w, &problem.Problem{Type: problem.Blank, Status: http.StatusNotFound, Detail: "no such\x1b[2J attempt"})
		return
	case RefuseHostile:
		writeProblem(w, http.StatusUnprocessableEntity, "refused\x1b]0;owned\x07\nmeridian: error: all is well (ok", nil)
		return
	case RefuseNested:
		writeProblem(w, http.StatusUnprocessableEntity, problems.InputRequired.Code(), map[string]any{"input": []any{"\x1b[2J"}})
		return
	case RefuseLarge:
		writeProblem(w, http.StatusUnprocessableEntity, Refusal, map[string]any{"path": strings.Repeat("x", LargeRefusal)})
		return
	case WrongClaim:
		writeJSON(w, workerapi.Version, map[string]any{"runID": 1})
		return
	case BadRequest:
		w.WriteHeader(http.StatusBadRequest)
		return
	case Garble:
		_, _ = w.Write([]byte("not json"))
		return
	case AckAhead:
		writeJSON(w, workerapi.Version, workerapi.EventsAck{Seq: 1 << 20})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	switch flt {
	case Hang:
		<-r.Context().Done()
		return
	case Redirect:
		http.Redirect(w, r, r.URL.Path, http.StatusTemporaryRedirect) //nolint:gosec // the fault redirects to the request's own path, which a worker must not follow
		return
	case Forget:
		f.forget(r)
	}
	rec := httptest.NewRecorder()
	stored := f.handle(rec, r, body)
	if f.OnEvent != nil {
		for _, e := range stored {
			f.OnEvent(e)
		}
	}
	if flt == DropAnswer {
		hangUp(w)
		return
	}
	maps.Copy(w.Header(), rec.Header())
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

// forget forgets what r would add to: the log it uploads to, or the
// events, for events and an end.
func (f *Fake) forget(r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	step, _ := strconv.Atoi(q.Get(workerapi.QueryStep))
	try, _ := strconv.Atoi(q.Get(workerapi.QueryTry))
	delete(f.logs, LogKey{workerapi.LogPart(q.Get(workerapi.QueryPart)), step, try})
	if r.URL.Path == workerapi.PathEvents || r.URL.Path == workerapi.PathComplete {
		f.events = nil
	}
}

// A route is one of the API's paths: its method, and the version of the
// API that added it.
type route struct {
	method string
	since  int
}

// routes are the API's paths.
var routes = map[string]route{
	workerapi.PathClaim:     {http.MethodPost, 1},
	workerapi.PathHeartbeat: {http.MethodPost, 1},
	workerapi.PathControl:   {http.MethodGet, 1},
	workerapi.PathEvents:    {http.MethodPost, 1},
	workerapi.PathLogs:      {http.MethodPost, 1},
	workerapi.PathComplete:  {http.MethodPost, 1},
	workerapi.PathAction:    {http.MethodPost, workerapi.Version3},
}

// handle handles a request, and returns the events it stored.
func (f *Fake) handle(w http.ResponseWriter, r *http.Request, body []byte) []workerapi.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.notify()
	f.requests[r.URL.Path]++
	rt, ok := routes[r.URL.Path]
	switch {
	case !ok:
		writeStatus(w, http.StatusNotFound)
		return nil
	case r.Method != rt.method:
		w.Header().Set("Allow", rt.method)
		writeStatus(w, http.StatusMethodNotAllowed)
		return nil
	case f.Claim.PlanVersion > f.reads() && !f.AnyVersion:
		writeStatus(w, http.StatusServiceUnavailable)
		return nil
	}
	bearer, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if r.URL.Path == workerapi.PathClaim {
		f.handleClaim(w, bearer, body)
		return nil
	}
	if !f.claimed || f.lapsed && f.Claim.SelfFencing || sha256.Sum256([]byte(bearer)) != f.credential {
		writeStatus(w, http.StatusUnauthorized)
		return nil
	}
	if f.Claim.APIVersion < rt.since {
		f.unexpected("a request of %s, of version %d of the API, in an attempt of version %d", r.URL.Path, rt.since, f.Claim.APIVersion)
		writeStatus(w, http.StatusNotFound)
		return nil
	}
	// A superseded attempt's writes are refused, and so is every write
	// once the attempt has ended, but its end again.
	if r.URL.Path != workerapi.PathControl && (f.superseded || f.end != nil && r.URL.Path != workerapi.PathComplete) {
		problem.Write(w, problems.Fenced.Problem(nil))
		return nil
	}
	switch r.URL.Path {
	case workerapi.PathHeartbeat:
		f.heartbeats++
		w.WriteHeader(http.StatusNoContent)
	case workerapi.PathControl:
		f.handleControl(w, r)
	case workerapi.PathEvents:
		return f.handleEvents(w, body)
	case workerapi.PathLogs:
		f.handleLogs(w, r, body)
	case workerapi.PathComplete:
		f.handleComplete(w, body)
	case workerapi.PathAction:
		f.handleAction(w, body)
	}
	return nil
}

// steps returns how many steps the attempt's job has, and false if the
// Fake can't read its job, as for a test of a job no worker reads.
func (f *Fake) steps() (int, bool) {
	job, err := plan.DecodeReadyJob(f.Claim.PlanVersion, f.Claim.Job)
	if err != nil {
		return 0, false
	}
	return len(job.Steps), true
}

// reads returns the newest plan version f reads.
func (f *Fake) reads() int {
	if f.Reads == 0 {
		return plan.Version
	}
	return f.Reads
}

func (f *Fake) handleClaim(w http.ResponseWriter, bearer string, body []byte) {
	var req workerapi.ClaimRequest
	if err := workerapi.Unmarshal(body, &req); err != nil {
		f.unexpected("a claim the coordinator can't read: %v", err)
		writeStatus(w, http.StatusBadRequest)
		return
	}
	cred := sha256.Sum256([]byte(req.Credential))
	switch {
	case bearer != f.Token, f.claimed && cred != f.credential, f.withdrawn && !f.claimed:
		writeStatus(w, http.StatusUnauthorized)
		return
	case f.claimed && (f.superseded || f.end != nil):
		// The same claim again, of an attempt that has ended without its
		// worker, or with it.
		problem.Write(w, problems.Fenced.Problem(nil))
		return
	case len(req.Credential) != 64:
		f.unexpected("the credential is %q, not 32 bytes as hex", req.Credential)
	case req.PlanVersion != plan.Version:
		f.unexpected("the worker reads plan version %d, not %d", req.PlanVersion, plan.Version)
	case req.APIVersion != workerapi.Version:
		f.unexpected("the worker speaks version %d of the API, not %d", req.APIVersion, workerapi.Version)
	}
	if !f.claimed && !f.AnyVersion {
		switch {
		case req.PlanVersion < f.Claim.PlanVersion:
			problem.Write(w, problems.PlanTooNew.Problem(nil))
			return
		case req.Speaks() < f.needs():
			problem.Write(w, problems.WorkerAPITooOld.Problem(problems.Params{workerapi.ParamVersion: req.Speaks(), workerapi.ParamNeeded: f.needs()}))
			return
		}
	}
	if f.Claim.APIVersion == 0 {
		f.Claim.APIVersion = min(req.Speaks(), f.speaks())
	}
	f.claimed, f.credential = true, cred
	// An attempt of version 2 or later is given its control as it is now,
	// unless a test set the claim's.
	c := f.Claim
	if c.APIVersion >= workerapi.Version2 && c.Control == nil {
		control := f.control
		c.Control = &control
	}
	// What a test sets of the claim is written as it is, for the worker to
	// refuse what its version doesn't have.
	writeJSON(w, workerapi.Version, &c)
}

// speaks returns the newest version of the API f speaks.
func (f *Fake) speaks() int {
	if f.Speaks == 0 {
		return workerapi.Version
	}
	return f.Speaks
}

// needs returns the oldest version of the API f's attempt needs.
func (f *Fake) needs() int {
	if f.Needs == 0 {
		return 1
	}
	return f.Needs
}

func (f *Fake) handleControl(w http.ResponseWriter, r *http.Request) {
	seen, err := strconv.ParseInt(r.URL.Query().Get(workerapi.QueryVersion), 10, 64)
	if err != nil {
		f.unexpected("a control poll without a version: %s", r.URL.RawQuery)
		writeStatus(w, http.StatusBadRequest)
		return
	}
	// Hold the poll, briefly, until there is something newer.
	timeout := time.After(ControlHold)
	for f.control.Version <= seen && !f.ControlAtOnce {
		changed := f.changed
		f.mu.Unlock()
		done := false
		select {
		case <-changed:
		case <-timeout:
			done = true
		case <-r.Context().Done():
			done = true
		}
		f.mu.Lock()
		if done {
			break
		}
	}
	writeJSON(w, f.Claim.APIVersion, &f.control)
}

func (f *Fake) handleEvents(w http.ResponseWriter, body []byte) []workerapi.Event {
	var events workerapi.Events
	if err := workerapi.UnmarshalAt(f.Claim.APIVersion, body, &events); err != nil {
		f.unexpected("events the coordinator can't read: %v", err)
		writeStatus(w, http.StatusBadRequest)
		return nil
	}
	var stored []workerapi.Event
	for _, e := range events.Events {
		n := int64(len(f.events))
		if e.Seq <= n {
			continue
		}
		if e.Seq != n+1 {
			problem.Write(w, problems.OutOfOrder.Problem(problems.Params{workerapi.ParamExpected: n + 1}))
			return stored
		}
		f.events = append(f.events, e)
		stored = append(stored, e)
	}
	writeJSON(w, f.Claim.APIVersion, &workerapi.EventsAck{Seq: int64(len(f.events))})
	return stored
}

// parts are the logs an attempt has.
var parts = []workerapi.LogPart{workerapi.LogSetup, workerapi.LogStep, workerapi.LogCleanup}

func (f *Fake) handleLogs(w http.ResponseWriter, r *http.Request, body []byte) {
	q := r.URL.Query()
	key := LogKey{Part: workerapi.LogPart(q.Get(workerapi.QueryPart))}
	offset, err := strconv.ParseInt(q.Get(workerapi.QueryOffset), 10, 64)
	ok := err == nil && offset >= 0 && slices.Contains(parts, key.Part)
	if key.Part != workerapi.LogSetup {
		step, errStep := strconv.Atoi(q.Get(workerapi.QueryStep))
		try, errTry := strconv.Atoi(q.Get(workerapi.QueryTry))
		key.Step, key.Try = step, try
		ok = ok && errStep == nil && errTry == nil && step >= 0 && try >= 0
	}
	if steps, known := f.steps(); ok && known && key.Part != workerapi.LogSetup && key.Step >= steps {
		f.unexpected("a log of step %d, of a job of %d steps", key.Step, steps)
		writeStatus(w, http.StatusBadRequest)
		return
	}
	switch {
	case !ok:
		f.unexpected("a log upload the coordinator can't read: %s", r.URL.RawQuery)
		writeStatus(w, http.StatusBadRequest)
		return
	case len(body) > workerapi.MaxLogUpload:
		f.unexpected("a log upload of %d bytes, more than %d", len(body), workerapi.MaxLogUpload)
		writeStatus(w, http.StatusRequestEntityTooLarge)
		return
	}
	l := f.logs[key]
	if l == nil {
		l = &Log{}
		f.logs[key] = l
	}
	length := int64(len(l.Data))
	switch {
	case offset > length, l.Ended && offset+int64(len(body)) > length:
		problem.Write(w, problems.OutOfOrder.Problem(problems.Params{workerapi.ParamExpected: length}))
		return
	case offset+int64(len(body)) > length:
		l.Data = append(l.Data, body[length-offset:]...)
	}
	if q.Get(workerapi.QueryEnd) == "true" {
		l.Ended = true
	}
	writeJSON(w, f.Claim.APIVersion, &workerapi.LogAck{Length: int64(len(l.Data)), Ended: l.Ended})
}

func (f *Fake) handleComplete(w http.ResponseWriter, body []byte) {
	end := new(workerapi.Complete)
	if err := workerapi.UnmarshalAt(f.Claim.APIVersion, body, end); err != nil {
		f.unexpected("a Complete the coordinator can't read: %v", err)
		writeStatus(w, http.StatusBadRequest)
		return
	}
	// The end as the API writes it, which a repeat must match.
	canonical, err := workerapi.Marshal(end)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	result := end.Result
	switch {
	case result.NeedsFailed || result.Cancelled,
		end.Problem != nil && (result.Outcome != plan.OutcomeFailure || end.Reason != plan.ReasonSetupFailed):
		f.unexpected("a Complete that says what only the coordinator says, or a problem other than a setup's: %s", body)
		writeStatus(w, http.StatusBadRequest)
		return
	case end.Seq > int64(len(f.events)):
		problem.Write(w, problems.OutOfOrder.Problem(problems.Params{workerapi.ParamExpected: len(f.events) + 1}))
		return
	case end.Seq < int64(len(f.events)):
		f.unexpected("a Complete after event %d, of the %d stored", end.Seq, len(f.events))
		writeStatus(w, http.StatusBadRequest)
		return
	case f.end != nil && !bytes.Equal(f.endBody, canonical):
		problem.Write(w, problems.Fenced.Problem(nil))
		return
	}
	for key, l := range f.logs {
		if !l.Ended {
			f.unexpected("the attempt completed before its log %v ended", key)
		}
	}
	f.end, f.endBody = end, canonical
	w.WriteHeader(http.StatusNoContent)
}

// handleAction performs the action a step's try asks for, once, and
// answers a repeat as it answered the first: a request of a step that
// uses no built-in action is one it can't read, and another request for
// the same try is fenced. A step that doesn't outlive its job's
// cancellation, of a job its control asks to cancel, for any reason but
// the run's timeout, is answered cancelled, for that reason, without Act;
// so is a step whose early-cancel: is any-job-failed, once a job of the
// run failed, as the control says, with no reason; and another's answer
// carries the cancel's reason too, unless Act gave one.
func (f *Fake) handleAction(w http.ResponseWriter, body []byte) {
	var req workerapi.ActionRequest
	if err := workerapi.UnmarshalAt(f.Claim.APIVersion, body, &req); err != nil {
		f.unexpected("an action's request the coordinator can't read: %v", err)
		writeStatus(w, http.StatusBadRequest)
		return
	}
	job, err := plan.DecodeReadyJob(f.Claim.PlanVersion, f.Claim.Job)
	if err != nil || req.Step < 0 || req.Try < 0 || req.Step >= len(job.Steps) || !strings.HasPrefix(job.Steps[req.Step].Uses, "parallelworks/") {
		f.unexpected("a request for the action of step %d's try %d, which uses none", req.Step, req.Try)
		writeStatus(w, http.StatusBadRequest)
		return
	}
	canonical, err := workerapi.Marshal(&req)
	if err != nil {
		writeStatus(w, http.StatusInternalServerError)
		return
	}
	for _, a := range f.actions {
		if a.Request.Step != req.Step || a.Request.Try != req.Try {
			continue
		}
		if !bytes.Equal(a.body, canonical) {
			problem.Write(w, problems.Fenced.Problem(nil))
			return
		}
		writeJSON(w, f.Claim.APIVersion, &a.Answer)
		return
	}
	answer := workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}
	var cancel plan.Reason
	if c := f.control; c.Cancel && c.Reason != plan.ReasonTimeout {
		cancel = c.Reason
	}
	switch {
	case cancel != "" && !req.OutlivesCancel:
		answer = workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled, Reason: cancel}
	case f.control.AnyJobFailed && job.Steps[req.Step].EarlyCancel == plan.EarlyCancelAnyJobFailed:
		answer = workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled}
	case f.Act != nil:
		answer = f.Act(req)
	}
	if answer.Reason == "" {
		answer.Reason = cancel
	}
	f.actions = append(f.actions, Action{Request: req, Answer: answer, body: canonical})
	writeJSON(w, f.Claim.APIVersion, &answer)
}

// hangUp closes the request's connection without answering it.
func hangUp(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err == nil {
		conn.Close()
	}
}

// writeProblem answers with a problem of code, which needn't be a kind's.
func writeProblem(w http.ResponseWriter, status int, code problem.Code, params map[string]any) {
	problem.Write(w, &problem.Problem{Type: "/problems/worker/" + string(code), Title: string(code), Status: status, Code: code, Params: params})
}

// writeStatus answers with an about:blank problem: one its status says
// all of.
func writeStatus(w http.ResponseWriter, status int) {
	problem.Write(w, problem.Status(status, ""))
}

// writeJSON answers 200 with v, as the API encodes a body of an attempt
// of version of the API.
func writeJSON(w http.ResponseWriter, version int, v any) {
	if err := workerapi.WriteBody(w, version, v); err != nil {
		writeStatus(w, http.StatusInternalServerError)
	}
}

// String writes a log's key as the worker's log does.
func (k LogKey) String() string {
	if k.Part == workerapi.LogSetup {
		return string(k.Part)
	}
	return fmt.Sprintf("%s %d.%d", k.Part, k.Step, k.Try)
}
