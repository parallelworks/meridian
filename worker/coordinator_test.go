package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
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

// A fault is what the fake coordinator does to a request instead of
// answering it.
type fault int

const (
	// answer answers the request.
	answer fault = iota
	// drop closes the connection without reading the request.
	drop
	// dropAnswer handles the request, and closes the connection without
	// answering it: the worker can't know that it was stored.
	dropAnswer
	// unavailable answers 503.
	unavailable
	// fence answers that the attempt was superseded, and unauthorized
	// that its credential isn't accepted.
	fence
	unauthorized
	// forget forgets the log the request uploads to, or the events it
	// stored, as a coordinator that lost what it had stored would, then
	// handles the request.
	forget
	// redirect answers with a redirect to the same path.
	redirect
	// hang holds the request until the worker gives up on it, as a
	// connection that is half open would.
	hang
)

// A fakeCoordinator serves the worker API for one attempt, in memory, as
// the coordinator does, strictly: it refuses events out of order, logs
// past their ends and a different Complete, and accepts any request again.
type fakeCoordinator struct {
	t     *testing.T
	token string
	// claim is what it answers a claim with; job is the job in it.
	claim workerapi.Claim
	// fault, if set, says what to do to each request.
	fault func(r *http.Request) fault
	// onEvent, if set, is called with each event stored, with mu held.
	onEvent func(e workerapi.Event)

	// controlAtOnce makes it answer a control poll at once, rather than
	// hold it until there is something newer.
	controlAtOnce bool

	mu         sync.Mutex
	credential [32]byte
	claimed    bool
	planTooNew bool
	events     []workerapi.Event
	logs       map[logKey]*fakeLog
	end        *workerapi.Complete
	control    workerapi.Control
	heartbeats int
	requests   map[string]int
	// changed is closed, and replaced, on each change.
	changed chan struct{}
}

type fakeLog struct {
	data  []byte
	ended bool
}

func newFakeCoordinator(t *testing.T, job *plan.ReadyJob) *fakeCoordinator {
	t.Helper()
	data, err := plan.EncodeReadyJob(job)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeCoordinator{
		t:     t,
		token: "bootstrap-token",
		claim: workerapi.Claim{
			RunID: "run1", AttemptID: "attempt1", PlanVersion: plan.Version, Job: data, Attempt: 1,
			Group: "local", Member: "laptop", LeaseSeconds: 60, HeartbeatSeconds: 1,
			Run:    plan.RunValues{Format: 1},
			RunDir: filepath.Join(t.TempDir(), "run"),
		},
		logs:     map[logKey]*fakeLog{},
		requests: map[string]int{},
		changed:  make(chan struct{}),
	}
}

// notify wakes whoever waits for a change. Its caller holds f.mu.
func (f *fakeCoordinator) notify() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// setControl asks something more of the worker.
func (f *fakeCoordinator) setControl(c workerapi.Control) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setControlLocked(c)
}

func (f *fakeCoordinator) setControlLocked(c workerapi.Control) {
	c.Version = f.control.Version + 1
	f.control = c
	f.notify()
}

func (f *fakeCoordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flt := answer
	if f.fault != nil {
		flt = f.fault(r)
	}
	switch flt {
	case drop:
		hangUp(w)
		return
	case unavailable:
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	case fence:
		writeProblem(w, http.StatusConflict, workerapi.Fenced, nil)
		return
	case unauthorized:
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	if flt == hang {
		<-r.Context().Done()
		return
	}
	if flt == redirect {
		http.Redirect(w, r, r.URL.Path, http.StatusTemporaryRedirect)
		return
	}
	if flt == forget {
		f.mu.Lock()
		q := r.URL.Query()
		step, _ := strconv.Atoi(q.Get(workerapi.QueryStep))
		try, _ := strconv.Atoi(q.Get(workerapi.QueryTry))
		delete(f.logs, logKey{workerapi.LogPart(q.Get(workerapi.QueryPart)), step, try})
		if r.URL.Path == workerapi.PathEvents {
			f.events = nil
		}
		f.mu.Unlock()
	}
	rec := httptest.NewRecorder()
	f.handle(rec, r, body)
	if flt == dropAnswer {
		hangUp(w)
		return
	}
	maps.Copy(w.Header(), rec.Header())
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

// hangUp closes the request's connection without answering it.
func hangUp(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err == nil {
		conn.Close()
	}
}

func writeProblem(w http.ResponseWriter, status int, code problem.Code, params map[string]any) {
	p := problem.Problem{Type: "/problems/worker/" + string(code), Title: string(code), Status: status, Code: code, Params: params}
	w.Header().Set("Content-Type", problem.MediaType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

func (f *fakeCoordinator) handle(w http.ResponseWriter, r *http.Request, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.notify()
	f.requests[r.URL.Path]++
	bearer, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if r.URL.Path == workerapi.PathClaim {
		f.handleClaim(w, bearer, body)
		return
	}
	if !f.claimed || sha256.Sum256([]byte(bearer)) != f.credential {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case workerapi.PathHeartbeat:
		f.heartbeats++
		w.WriteHeader(http.StatusNoContent)
	case workerapi.PathControl:
		f.handleControl(w, r)
	case workerapi.PathEvents:
		f.handleEvents(w, body)
	case workerapi.PathLogs:
		f.handleLogs(w, r, body)
	case workerapi.PathComplete:
		f.handleComplete(w, body)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeCoordinator) handleClaim(w http.ResponseWriter, bearer string, body []byte) {
	var req workerapi.ClaimRequest
	if err := workerapi.Unmarshal(body, &req); err != nil {
		f.t.Errorf("a claim the coordinator can't read: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	cred := sha256.Sum256([]byte(req.Credential))
	switch {
	case bearer != f.token, f.claimed && cred != f.credential:
		w.WriteHeader(http.StatusUnauthorized)
		return
	case len(req.Credential) != 64:
		f.t.Errorf("the credential is %q, not 32 bytes as hex", req.Credential)
	case req.PlanVersion != plan.Version:
		f.t.Errorf("the worker reads plan version %d, not %d", req.PlanVersion, plan.Version)
	}
	if f.planTooNew {
		writeProblem(w, http.StatusConflict, workerapi.PlanTooNew, nil)
		return
	}
	f.claimed, f.credential = true, cred
	writeJSON(w, &f.claim)
}

func (f *fakeCoordinator) handleControl(w http.ResponseWriter, r *http.Request) {
	seen, _ := strconv.ParseInt(r.URL.Query().Get(workerapi.QueryVersion), 10, 64)
	// Hold the poll, briefly, until there is something newer.
	timeout := time.After(200 * time.Millisecond)
	for f.control.Version <= seen && !f.controlAtOnce {
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
	writeJSON(w, &f.control)
}

func (f *fakeCoordinator) handleEvents(w http.ResponseWriter, body []byte) {
	var events workerapi.Events
	if err := workerapi.Unmarshal(body, &events); err != nil {
		f.t.Errorf("events the coordinator can't read: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, e := range events.Events {
		stored := int64(len(f.events))
		if e.Seq <= stored {
			continue
		}
		if e.Seq != stored+1 {
			writeProblem(w, http.StatusConflict, workerapi.OutOfOrder, map[string]any{workerapi.ParamExpected: stored + 1})
			return
		}
		f.events = append(f.events, e)
		if f.onEvent != nil {
			f.onEvent(e)
		}
	}
	writeJSON(w, &workerapi.EventsAck{Seq: int64(len(f.events))})
}

func (f *fakeCoordinator) handleLogs(w http.ResponseWriter, r *http.Request, body []byte) {
	q := r.URL.Query()
	key := logKey{part: workerapi.LogPart(q.Get(workerapi.QueryPart))}
	key.step, _ = strconv.Atoi(q.Get(workerapi.QueryStep))
	key.try, _ = strconv.Atoi(q.Get(workerapi.QueryTry))
	offset, err := strconv.ParseInt(q.Get(workerapi.QueryOffset), 10, 64)
	if err != nil || len(body) > workerapi.MaxLogUpload {
		f.t.Errorf("a log upload the coordinator can't read: %s, %d bytes", r.URL.RawQuery, len(body))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	l := f.logs[key]
	if l == nil {
		l = &fakeLog{}
		f.logs[key] = l
	}
	length := int64(len(l.data))
	switch {
	case offset > length, l.ended && offset+int64(len(body)) > length:
		writeProblem(w, http.StatusConflict, workerapi.OutOfOrder, map[string]any{workerapi.ParamExpected: length})
		return
	case offset+int64(len(body)) > length:
		l.data = append(l.data, body[length-offset:]...)
	}
	if q.Get(workerapi.QueryEnd) == "true" {
		l.ended = true
	}
	writeJSON(w, &workerapi.LogAck{Length: int64(len(l.data)), Ended: l.ended})
}

func (f *fakeCoordinator) handleComplete(w http.ResponseWriter, body []byte) {
	end := new(workerapi.Complete)
	if err := workerapi.Unmarshal(body, end); err != nil {
		f.t.Errorf("a Complete the coordinator can't read: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch {
	case end.Seq > int64(len(f.events)):
		writeProblem(w, http.StatusConflict, workerapi.OutOfOrder, map[string]any{workerapi.ParamExpected: len(f.events) + 1})
		return
	case f.end != nil && !bytes.Equal(mustMarshal(f.t, f.end), body):
		w.WriteHeader(http.StatusConflict)
		return
	}
	for key, l := range f.logs {
		if !l.ended {
			f.t.Errorf("the attempt completed before its log %v ended", key)
		}
	}
	f.end = end
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	data, err := workerapi.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := workerapi.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// serve serves f on a Unix socket, and returns its path.
func (f *fakeCoordinator) serve() string {
	f.t.Helper()
	// A socket's path is short: t.TempDir's can be too long.
	dir, err := os.MkdirTemp("", "mw")
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		f.t.Fatal(err)
	}
	srv := &httptest.Server{Listener: l, Config: &http.Server{Handler: f, ReadHeaderTimeout: 10 * time.Second}}
	srv.Start()
	f.t.Cleanup(srv.Close)
	return socket
}

// claimedClient returns a client that has claimed f's attempt, served on a
// Unix socket, as a worker's has once it runs it.
func claimedClient(t *testing.T, f *fakeCoordinator) *client {
	t.Helper()
	c := newClient(endpoint{address: f.serve(), token: f.token}, testTiming, testLogger(t))
	t.Cleanup(c.close)
	credential := newCredential()
	if _, _, err := c.claim(t.Context(), f.token, workerapi.ClaimRequest{Credential: credential, PlanVersion: plan.Version}); err != nil {
		t.Fatal(err)
	}
	c.credential = credential
	return c
}

// sendingAttempt returns an attempt of job, as a worker that has claimed it
// from f has it, the context it sends in, and start, which starts sending
// its events, so that a test can queue some first. The worker stopping, as
// for events it couldn't send, fails the test.
func sendingAttempt(t *testing.T, f *fakeCoordinator, job *plan.ReadyJob) (*attempt, context.Context, func()) {
	t.Helper()
	c := claimedClient(t, f)
	a := &attempt{client: c, log: testLogger(t), events: newEventQueue(), spool: newSpool(nil, 0), job: job, mask: plan.NewMasker()}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	stop := func(err error) {
		t.Errorf("the worker stopped: %v", err)
		cancel()
	}
	return a, ctx, func() { wg.Go(func() { a.events.send(ctx, c, stop, a.reportNotEncoded) }) }
}

// testTiming is the worker's timing, shortened for tests.
var testTiming = timing{
	grace:      2 * time.Second,
	afterKill:  time.Second,
	backoffMin: 5 * time.Millisecond,
	backoffMax: 50 * time.Millisecond,
	flush:      time.Second,
}

// runWorker runs a worker against f, served on a Unix socket unless
// cfg.Environ says where it is, with its directory in a test's own unless
// cfg.Environ gives it a home, and returns what Run returns.
func (f *fakeCoordinator) runWorker(ctx context.Context, cfg Config) error {
	f.t.Helper()
	if !slices.ContainsFunc(cfg.Environ, func(kv string) bool { return strings.HasPrefix(kv, workerapi.EnvCoordinator+"=") }) {
		cfg.Environ = append(cfg.Environ, workerapi.EnvCoordinator+"="+f.serve())
	}
	cfg.Environ = append(cfg.Environ,
		workerapi.EnvBootstrapToken+"="+f.token,
		"PATH="+os.Getenv("PATH"),
		"FROM_WORKER=inherited",
	)
	if cfg.Worker.Digest == "" {
		cfg.Worker = workerapi.Worker{Version: "v0.0.0-test", Digest: "sha256:" + strings.Repeat("0", 64)}
	}
	if cfg.Dir == "" && lookupEnv(cfg.Environ, "HOME") == "" {
		// A test that gives the worker a home lets it choose its own
		// directory, in that home's cache.
		cfg.Dir = f.t.TempDir()
	}
	if cfg.Log == nil {
		cfg.Log = testLogger(f.t)
	}
	return run(ctx, cfg, testTiming, func() error { return nil })
}

// testLogger returns a logger that writes to a test's log.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter writes to a test's log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

// stepEvents returns the events stored, as strings that are easy to
// compare: "started 0.1", "finished 0.1 failure 3", "cleanup started 0".
func (f *fakeCoordinator) stepEvents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k, e := range f.events {
		if e.Seq != int64(k+1) {
			f.t.Errorf("event %d has the sequence number %d", k+1, e.Seq)
		}
		s := ""
		if e.Cleanup {
			s = "cleanup "
		}
		if e.Finished {
			s += "finished " + strconv.Itoa(e.Step) + "." + strconv.Itoa(e.Try) + " " + string(e.Outcome) + " " + strconv.Itoa(e.ExitCode)
		} else {
			s += "started " + strconv.Itoa(e.Step) + "." + strconv.Itoa(e.Try)
		}
		out = append(out, s)
	}
	return out
}

// event returns the finished event of a step's try.
func (f *fakeCoordinator) finishedEvent(step, try int, cleanup bool) workerapi.Event {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.events {
		if e.Finished && e.Step == step && e.Try == try && e.Cleanup == cleanup {
			return e
		}
	}
	f.t.Fatalf("no finished event for step %d, try %d", step, try)
	return workerapi.Event{}
}

// log returns a log, and whether it has ended.
func (f *fakeCoordinator) log(key logKey) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.logs[key]
	if l == nil {
		return "", false
	}
	return string(l.data), l.ended
}

// codes returns the codes of diagnostics.
func codes(ds problems.Diagnostics) []string {
	var out []string
	for _, d := range ds {
		out = append(out, string(d.Code))
	}
	return out
}

// keyHash returns the hash a worker checks a TLS server's key against.
func keyHash(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	sum := sha256.Sum256(srv.Certificate().RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(sum[:])
}
