package workerapitest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// A Target is a coordinator that Conformance checks. It makes attempts
// for the suite to claim, changes them as the coordinator would on its
// own, and says what it stored of each. Conformance calls it from one
// goroutine at a time.
type Target interface {
	// NewAttempt makes an attempt as a, ready to claim, and returns the
	// path of the Unix socket the target serves it on, and its bootstrap
	// token.
	NewAttempt(t *testing.T, a Attempt) (socket, token string)
	// Supersede places a newer attempt of the attempt's job, as when its
	// lease lapsed without it.
	Supersede(t *testing.T, attemptID string)
	// Control asks something more of the attempt's worker: c, at a newer
	// version, which the target gives it.
	Control(t *testing.T, attemptID string, c workerapi.Control)
	// Lapse lets the attempt's lease lapse, for a job that fences itself,
	// whose credential lapses with it.
	Lapse(t *testing.T, attemptID string)
	// Stored returns what the target stored of the attempt.
	Stored(t *testing.T, attemptID string) Stored
}

// An Attempt is an attempt for a Target to make: of Job, as Ready gave it,
// in a run whose values the job reads are Run, and whose plan is of
// PlanVersion, or plan.Version for 0, with its lease's terms.
type Attempt struct {
	Job                            *plan.ReadyJob
	Run                            plan.RunValues
	PlanVersion                    int
	SelfFencing                    bool
	LeaseSeconds, HeartbeatSeconds int64
}

// marker is in the job and the run of every attempt the suite makes, so
// that it can tell an answer that holds any of them.
const marker = "conformance-marker-7f3a"

// newAttempt returns the attempt the suite claims, of a job of three
// steps.
func newAttempt() Attempt {
	steps := make([]plan.Step, 3)
	for i := range steps {
		steps[i] = plan.Step{Path: "/jobs/build/steps/" + strconv.Itoa(i), Run: plan.Literal("echo " + marker)}
	}
	return Attempt{
		Job:          &plan.ReadyJob{Key: "build", Name: "build", Steps: steps, Path: "/jobs/build"},
		Run:          plan.RunValues{Format: 1, Inputs: plan.DataOf(map[string]any{"note": marker})},
		LeaseSeconds: 60, HeartbeatSeconds: 15,
	}
}

// Conformance checks that target serves the worker API as package
// workerapi says, playing a worker over HTTP on the target's Unix socket:
// the claim and its credential, heartbeats, control polls, events, logs
// and the attempt's end, each with what a coordinator refuses, and
// strict decoding, fencing, unknown paths and wrong methods.
func Conformance(t *testing.T, target Target) {
	t.Run("claim", func(t *testing.T) { checkClaim(t, target) })
	t.Run("plan versions", func(t *testing.T) { checkPlanVersions(t, target) })
	t.Run("authorization", func(t *testing.T) { checkAuthorization(t, target) })
	t.Run("heartbeat", func(t *testing.T) { checkHeartbeat(t, target) })
	t.Run("self-fencing lease", func(t *testing.T) { checkLapse(t, target) })
	t.Run("control", func(t *testing.T) { checkControl(t, target) })
	t.Run("events", func(t *testing.T) { checkEvents(t, target) })
	t.Run("logs", func(t *testing.T) { checkLogs(t, target) })
	t.Run("complete", func(t *testing.T) { checkComplete(t, target) })
	t.Run("strict decoding", func(t *testing.T) { checkStrict(t, target) })
	t.Run("superseded", func(t *testing.T) { checkSuperseded(t, target) })
	t.Run("paths and methods", func(t *testing.T) { checkPaths(t, target) })
}

// A worker is the suite's worker, for one attempt.
type worker struct {
	t       *testing.T
	target  Target
	attempt Attempt
	http    *http.Client
	token   string
	// credential is the one the worker claims with, and claim what it was
	// given, once it has claimed.
	credential string
	claim      workerapi.Claim
}

func newWorker(t *testing.T, target Target, a Attempt) *worker {
	t.Helper()
	socket, token := target.NewAttempt(t, a)
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}
	t.Cleanup(tr.CloseIdleConnections)
	return &worker{
		t: t, target: target, attempt: a, token: token, credential: newCredential(t),
		http: &http.Client{
			Transport: tr,
			Timeout:   (workerapi.ControlWaitSeconds + 15) * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// newCredential returns a credential as a worker makes one: 32 bytes from
// crypto/rand, as hex.
func newCredential(t *testing.T) string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// An answer is the target's answer to a request.
type answer struct {
	status int
	header http.Header
	body   []byte
	took   time.Duration
}

// send sends a request, with bearer as its bearer token unless it is "",
// and returns the answer.
func (w *worker) send(method, path string, query url.Values, body []byte, bearer string) answer {
	w.t.Helper()
	u := "http://coordinator" + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(w.t.Context(), method, u, bytes.NewReader(body))
	if err != nil {
		w.t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if path == workerapi.PathLogs {
		req.Header.Set("Content-Type", "application/octet-stream")
	} else if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	resp, err := w.http.Do(req)
	if err != nil {
		w.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		w.t.Fatalf("%s %s: %v", method, path, err)
	}
	return answer{status: resp.StatusCode, header: resp.Header, body: data, took: time.Since(start)}
}

// post sends body, a body Marshal encodes, to path with the worker's
// credential.
func (w *worker) post(path string, body any) answer {
	w.t.Helper()
	return w.send(http.MethodPost, path, nil, marshal(w.t, body), w.credential)
}

// marshal encodes a body as the API does.
func marshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := workerapi.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// claimRequest returns the worker's claim, as one that reads planVersion.
func (w *worker) claimRequest(planVersion int) []byte {
	return marshal(w.t, workerapi.ClaimRequest{
		Credential: w.credential, PlanVersion: planVersion,
		Worker: workerapi.Worker{Version: "v0.0.0-conformance", Digest: "sha256:" + strings.Repeat("0", 64)},
	})
}

// claimed claims the attempt, checks the claim, and returns the worker.
func (w *worker) claimed() *worker {
	w.t.Helper()
	a := w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version), w.token)
	if a.status != http.StatusOK {
		w.t.Fatalf("the claim was answered %d: %s", a.status, a.body)
	}
	if err := workerapi.Unmarshal(a.body, &w.claim); err != nil {
		w.t.Fatalf("the claim isn't one: %v: %s", err, a.body)
	}
	c := w.claim
	job, err := plan.DecodeReadyJob(c.PlanVersion, c.Job)
	switch {
	case err != nil:
		w.t.Fatalf("the claim's job doesn't decode: %v", err)
	case c.AttemptID == "" || c.RunID == "":
		w.t.Errorf("the claim names no run or no attempt: %+v", c)
	case job.Key != w.attempt.Job.Key || len(job.Steps) != len(w.attempt.Job.Steps):
		w.t.Errorf("the claim's job is %s, of %d steps, not the attempt's", job.Key, len(job.Steps))
	case c.PlanVersion != planVersion(w.attempt):
		w.t.Errorf("the claim's plan version is %d, not the run's %d", c.PlanVersion, planVersion(w.attempt))
	case c.SelfFencing != w.attempt.SelfFencing || c.LeaseSeconds != w.attempt.LeaseSeconds || c.HeartbeatSeconds != w.attempt.HeartbeatSeconds:
		w.t.Errorf("the claim's lease is %v, %d s, every %d s; the attempt's is %v, %d s, every %d s",
			c.SelfFencing, c.LeaseSeconds, c.HeartbeatSeconds, w.attempt.SelfFencing, w.attempt.LeaseSeconds, w.attempt.HeartbeatSeconds)
	}
	return w
}

func planVersion(a Attempt) int {
	if a.PlanVersion == 0 {
		return plan.Version
	}
	return a.PlanVersion
}

// stored returns what the target stored of the worker's attempt.
func (w *worker) stored() Stored {
	w.t.Helper()
	return w.target.Stored(w.t, w.claim.AttemptID)
}

// wantStatus checks an answer's status.
func wantStatus(t *testing.T, what string, a answer, status int) {
	t.Helper()
	if a.status != status {
		t.Errorf("%s was answered %d, not %d: %s", what, a.status, status, a.body)
	}
}

// wantProblem checks that an answer is a 409 problem of code, and, for
// out_of_order, that it expects expected.
func wantProblem(t *testing.T, what string, a answer, code problem.Code, expected int64) {
	t.Helper()
	if a.status != http.StatusConflict {
		t.Errorf("%s was answered %d, not 409 %s: %s", what, a.status, code, a.body)
		return
	}
	if ct := a.header.Get("Content-Type"); !strings.HasPrefix(ct, problem.MediaType) {
		t.Errorf("%s was answered as %q, not a problem", what, ct)
	}
	var p problem.Problem
	if err := jsonv2.Unmarshal(a.body, &p); err != nil || p.Code != code {
		t.Errorf("%s was answered %s, not a problem of %s: %v", what, a.body, code, err)
		return
	}
	if code == workerapi.OutOfOrder {
		if got, ok := p.Params[workerapi.ParamExpected].(float64); !ok || int64(got) != expected {
			t.Errorf("%s expects %v, not %d", what, p.Params[workerapi.ParamExpected], expected)
		}
	}
}

// event returns the attempt's event seq: a step's start, then its end, in
// turn, for its three steps.
func event(seq int64) workerapi.Event {
	e := workerapi.Event{Seq: seq, Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Step: int((seq - 1) / 2)}
	if seq%2 == 0 {
		e.Finished, e.Outcome = true, plan.OutcomeSuccess
	}
	return e
}

// events returns the events seq from first to last.
func events(first, last int64) workerapi.Events {
	var out workerapi.Events
	for seq := first; seq <= last; seq++ {
		out.Events = append(out.Events, event(seq))
	}
	return out
}

// sendEvents sends the events seq from first to last, and checks that
// they are acknowledged up to ack.
func (w *worker) sendEvents(first, last, ack int64) {
	w.t.Helper()
	a := w.post(workerapi.PathEvents, events(first, last))
	if a.status != http.StatusOK {
		w.t.Fatalf("events %d to %d were answered %d: %s", first, last, a.status, a.body)
	}
	var got workerapi.EventsAck
	if err := workerapi.Unmarshal(a.body, &got); err != nil {
		w.t.Fatalf("the acknowledgement isn't one: %v: %s", err, a.body)
	}
	if got.Seq != ack {
		w.t.Errorf("events %d to %d were acknowledged to %d, not %d", first, last, got.Seq, ack)
	}
}

// upload uploads data to a log from offset, and returns the answer.
func (w *worker) upload(key LogKey, offset int64, data []byte, end bool) answer {
	w.t.Helper()
	q := url.Values{workerapi.QueryPart: {string(key.Part)}, workerapi.QueryOffset: {strconv.FormatInt(offset, 10)}}
	if key.Part != workerapi.LogSetup {
		q.Set(workerapi.QueryStep, strconv.Itoa(key.Step))
		q.Set(workerapi.QueryTry, strconv.Itoa(key.Try))
	}
	if end {
		q.Set(workerapi.QueryEnd, "true")
	}
	return w.send(http.MethodPost, workerapi.PathLogs, q, data, w.credential)
}

// uploaded uploads data, and checks that the log's length is then length,
// and whether it has ended.
func (w *worker) uploaded(key LogKey, offset int64, data string, end bool, length int64, ended bool) {
	w.t.Helper()
	a := w.upload(key, offset, []byte(data), end)
	if a.status != http.StatusOK {
		w.t.Fatalf("%q at %d of the %s log was answered %d: %s", data, offset, key, a.status, a.body)
	}
	var got workerapi.LogAck
	if err := workerapi.Unmarshal(a.body, &got); err != nil {
		w.t.Fatalf("the acknowledgement isn't one: %v: %s", err, a.body)
	}
	if got.Length != length || got.Ended != ended {
		w.t.Errorf("%q at %d of the %s log: the log is %d bytes, ended %v; want %d, %v", data, offset, key, got.Length, got.Ended, length, ended)
	}
}

// poll polls for control, having seen version.
func (w *worker) poll(version int64) (workerapi.Control, time.Duration) {
	w.t.Helper()
	a := w.send(http.MethodGet, workerapi.PathControl, url.Values{workerapi.QueryVersion: {strconv.FormatInt(version, 10)}}, nil, w.credential)
	if a.status != http.StatusOK {
		w.t.Fatalf("a control poll was answered %d: %s", a.status, a.body)
	}
	var c workerapi.Control
	if err := workerapi.Unmarshal(a.body, &c); err != nil {
		w.t.Fatalf("the control isn't one: %v: %s", err, a.body)
	}
	return c, a.took
}

// A claim is answered only for the attempt's token, the same way each time
// for the same credential, and never for another credential.
func checkClaim(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	wantStatus(t, "a claim with another token", w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version), "not-the-token"), http.StatusUnauthorized)
	wantStatus(t, "a claim without a token", w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version), ""), http.StatusUnauthorized)
	w.claimed()
	first := marshal(t, w.claim)
	again := w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version), w.token)
	if again.status != http.StatusOK {
		t.Fatalf("the claim repeated was answered %d: %s", again.status, again.body)
	}
	var c workerapi.Claim
	if err := workerapi.Unmarshal(again.body, &c); err != nil || !bytes.Equal(marshal(t, c), first) {
		t.Errorf("the claim repeated was answered otherwise:\n%s\nnot\n%s", again.body, first)
	}
	other := *w
	other.credential = newCredential(t)
	wantStatus(t, "the claim with another credential", other.send(http.MethodPost, workerapi.PathClaim, nil, other.claimRequest(plan.Version), w.token), http.StatusUnauthorized)
}

// A worker that reads only an older plan version than the run's is
// refused with plan_too_new, which holds nothing of the run; and a run
// whose version the coordinator doesn't read is answered 503.
func checkPlanVersions(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	a := w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version-1), w.token)
	wantProblem(t, "a claim of a worker that reads an older version", a, workerapi.PlanTooNew, 0)
	if bytes.Contains(a.body, []byte(marker)) || bytes.Contains(a.body, []byte(`"build"`)) {
		t.Errorf("plan_too_new holds the run's values or its job: %s", a.body)
	}
	newer := newAttempt()
	newer.PlanVersion = plan.Version + 1
	w = newWorker(t, target, newer)
	wantStatus(t, "a claim of a run of a newer version", w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version+1), w.token), http.StatusServiceUnavailable)
}

// paths are the requests a worker makes after its claim, each as it
// makes it.
func (w *worker) requests() []struct {
	name, method, path string
	query              url.Values
	body               []byte
} {
	return []struct {
		name, method, path string
		query              url.Values
		body               []byte
	}{
		{"a heartbeat", http.MethodPost, workerapi.PathHeartbeat, nil, nil},
		{"a control poll", http.MethodGet, workerapi.PathControl, url.Values{workerapi.QueryVersion: {"0"}}, nil},
		{"events", http.MethodPost, workerapi.PathEvents, nil, marshal(w.t, events(1, 1))},
		{"a log upload", http.MethodPost, workerapi.PathLogs, url.Values{workerapi.QueryPart: {string(workerapi.LogSetup)}, workerapi.QueryOffset: {"0"}}, []byte("x")},
		{"the attempt's end", http.MethodPost, workerapi.PathComplete, nil, marshal(w.t, workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}})},
	}
}

// Every request but the claim needs the attempt's credential: not the
// bootstrap token, not another credential, and not before the claim.
func checkAuthorization(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	for _, r := range w.requests() {
		wantStatus(t, r.name+" before the claim", w.send(r.method, r.path, r.query, r.body, w.credential), http.StatusUnauthorized)
	}
	w.claimed()
	for _, r := range w.requests() {
		wantStatus(t, r.name+" without a credential", w.send(r.method, r.path, r.query, r.body, ""), http.StatusUnauthorized)
		wantStatus(t, r.name+" with the bootstrap token", w.send(r.method, r.path, r.query, r.body, w.token), http.StatusUnauthorized)
		wantStatus(t, r.name+" with another credential", w.send(r.method, r.path, r.query, r.body, newCredential(t)), http.StatusUnauthorized)
	}
	if s := w.stored(); len(s.Events) > 0 || len(s.Logs) > 0 || s.Complete != nil {
		t.Errorf("requests without the credential stored %+v", s)
	}
}

// A heartbeat is answered 204, until another attempt supersedes this one:
// then it is fenced.
func checkHeartbeat(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	wantStatus(t, "a heartbeat", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), http.StatusNoContent)
	wantStatus(t, "a heartbeat again", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), http.StatusNoContent)
	target.Supersede(t, w.claim.AttemptID)
	wantProblem(t, "a heartbeat once superseded", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), workerapi.Fenced, 0)
}

// For a job that fences itself, the credential lapses with the lease.
func checkLapse(t *testing.T, target Target) {
	a := newAttempt()
	a.SelfFencing, a.LeaseSeconds, a.HeartbeatSeconds = true, 60, 10
	w := newWorker(t, target, a).claimed()
	wantStatus(t, "a heartbeat", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), http.StatusNoContent)
	target.Lapse(t, w.claim.AttemptID)
	wantStatus(t, "a heartbeat once the lease lapsed", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), http.StatusUnauthorized)
}

// A control poll is answered at once when the coordinator has a newer
// version than the one the worker saw, and otherwise held, for at most
// ControlWaitSeconds, until it has; Cancel and AnyJobFailed reach the
// worker; and the version only grows.
func checkControl(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	limit := (workerapi.ControlWaitSeconds + 5) * time.Second
	first, took := w.poll(-1)
	if took > 5*time.Second {
		t.Errorf("a poll for any version took %v", took)
	}
	versions := []int64{first.Version}

	// Asked to cancel, while no poll waits.
	cancel := workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 7}
	target.Control(t, w.claim.AttemptID, cancel)
	c, took := w.poll(first.Version)
	if took > 5*time.Second {
		t.Errorf("a poll for a newer version, which the coordinator had, took %v", took)
	}
	versions = append(versions, c.Version)
	if c.Version <= first.Version || !c.Cancel || c.Reason != plan.ReasonCancelled || c.GraceSeconds != 7 {
		t.Errorf("after a cancel the control is %+v, after %+v", c, first)
	}

	// Asked more while a poll waits: the poll, or one after it, has it.
	seen := c.Version
	go func() {
		time.Sleep(300 * time.Millisecond)
		target.Control(t, w.claim.AttemptID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 7, AnyJobFailed: true})
	}()
	deadline := time.Now().Add(limit)
	for {
		c, took = w.poll(seen)
		versions = append(versions, c.Version)
		if took > limit {
			t.Errorf("a poll was held %v, longer than %d s", took, workerapi.ControlWaitSeconds)
		}
		if c.Version > seen {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the control never reached a poll")
		}
	}
	if !c.AnyJobFailed || !c.Cancel {
		t.Errorf("the control is %+v, without what the coordinator asked", c)
	}
	if !slices.IsSorted(versions) {
		t.Errorf("the control's version went back: %v", versions)
	}
}

// Events are stored in order, each once: a repeat is acknowledged as
// before, a gap is out of order, and the acknowledgement is never ahead of
// what was sent.
func checkEvents(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	w.sendEvents(1, 1, 1)
	w.sendEvents(1, 1, 1)
	w.sendEvents(2, 3, 3)
	w.sendEvents(3, 4, 4)
	wantProblem(t, "event 6 after 4", w.post(workerapi.PathEvents, events(6, 6)), workerapi.OutOfOrder, 5)
	wantProblem(t, "events 5 and 7", w.post(workerapi.PathEvents, workerapi.Events{Events: []workerapi.Event{event(5), event(7)}}), workerapi.OutOfOrder, 6)
	w.sendEvents(5, 6, 6)
	stored := w.stored().Events
	if want := events(1, 6).Events; !bytes.Equal(marshal(t, stored), marshal(t, want)) {
		t.Errorf("the events stored are\n%s\nnot\n%s", marshal(t, stored), marshal(t, want))
	}
}

// A log upload appends what the log doesn't have yet, and an overlap is
// stored once; one past the log's end, or after its end, is out of order;
// one too large is 413, and one of a log that isn't one 400.
func checkLogs(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	setup := LogKey{Part: workerapi.LogSetup}
	w.uploaded(setup, 0, "abc", false, 3, false)
	w.uploaded(setup, 1, "bcde", false, 5, false)
	wantProblem(t, "an upload past the log's end", w.upload(setup, 7, []byte("x"), false), workerapi.OutOfOrder, 5)
	w.uploaded(setup, 5, "f", true, 6, true)
	w.uploaded(setup, 5, "f", true, 6, true)
	wantProblem(t, "bytes after the log's end", w.upload(setup, 6, []byte("g"), false), workerapi.OutOfOrder, 6)
	step := LogKey{Part: workerapi.LogStep, Step: 1, Try: 2}
	w.uploaded(step, 0, "hello\n", true, 6, true)
	wantStatus(t, "an upload too large", w.upload(LogKey{Part: workerapi.LogStep}, 0, make([]byte, workerapi.MaxLogUpload+1), false), http.StatusRequestEntityTooLarge)
	wantStatus(t, "an upload to a log that isn't one", w.upload(LogKey{Part: "journal"}, 0, []byte("x"), false), http.StatusBadRequest)
	logs := w.stored().Logs
	if l := logs[setup]; string(l.Data) != "abcdef" || !l.Ended {
		t.Errorf("the setup log is %q, ended %v", l.Data, l.Ended)
	}
	if l := logs[step]; string(l.Data) != "hello\n" || !l.Ended {
		t.Errorf("the step's log is %q, ended %v", l.Data, l.Ended)
	}
	if l, ok := logs[LogKey{Part: workerapi.LogStep}]; ok && len(l.Data) > 0 {
		t.Errorf("an upload too large stored %d bytes", len(l.Data))
	}
}

// The attempt's end is stored once its events are: the same end again is
// answered as before, and another is fenced, as is every write after it.
func checkComplete(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	w.sendEvents(1, 2, 2)
	wantProblem(t, "an end after events not stored", w.post(workerapi.PathComplete, workerapi.Complete{Seq: 3, Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}), workerapi.OutOfOrder, 3)
	end := workerapi.Complete{Seq: 2, Result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"answer": "42"}}}
	wantStatus(t, "the attempt's end", w.post(workerapi.PathComplete, end), http.StatusNoContent)
	wantStatus(t, "the attempt's end again", w.post(workerapi.PathComplete, end), http.StatusNoContent)
	other := end
	other.Result.Outcome = plan.OutcomeFailure
	wantProblem(t, "another end", w.post(workerapi.PathComplete, other), workerapi.Fenced, 0)
	wantProblem(t, "events after the end", w.post(workerapi.PathEvents, events(3, 3)), workerapi.Fenced, 0)
	wantProblem(t, "a log after the end", w.upload(LogKey{Part: workerapi.LogSetup}, 0, []byte("late"), true), workerapi.Fenced, 0)
	wantProblem(t, "a heartbeat after the end", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), workerapi.Fenced, 0)
	s := w.stored()
	if s.Complete == nil || !bytes.Equal(marshal(t, s.Complete), marshal(t, end)) {
		t.Errorf("the end stored is %+v, not %+v", s.Complete, end)
	}
	if len(s.Events) != 2 {
		t.Errorf("%d events are stored, not 2", len(s.Events))
	}
}

// A body with a member its type doesn't have is refused, 400.
func checkStrict(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	extra := func(body []byte) []byte {
		return append(bytes.TrimSuffix(body, []byte("}")), []byte(`,"unknown":true}`)...)
	}
	wantStatus(t, "a claim with an unknown member", w.send(http.MethodPost, workerapi.PathClaim, nil, extra(w.claimRequest(plan.Version)), w.token), http.StatusBadRequest)
	w.claimed()
	wantStatus(t, "events with an unknown member", w.send(http.MethodPost, workerapi.PathEvents, nil, extra(marshal(t, events(1, 1))), w.credential), http.StatusBadRequest)
	event := extra(marshal(t, event(1)))
	wantStatus(t, "an event with an unknown member", w.send(http.MethodPost, workerapi.PathEvents, nil, []byte(`{"events":[`+string(event)+`]}`), w.credential), http.StatusBadRequest)
	end := marshal(t, workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}})
	wantStatus(t, "an end with an unknown member", w.send(http.MethodPost, workerapi.PathComplete, nil, extra(end), w.credential), http.StatusBadRequest)
	wantStatus(t, "an end that isn't JSON", w.send(http.MethodPost, workerapi.PathComplete, nil, []byte("{"), w.credential), http.StatusBadRequest)
	if s := w.stored(); len(s.Events) > 0 || s.Complete != nil {
		t.Errorf("bodies refused stored %+v", s)
	}
}

// Once another attempt supersedes this one, every write of its is fenced.
func checkSuperseded(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	w.sendEvents(1, 1, 1)
	target.Supersede(t, w.claim.AttemptID)
	wantProblem(t, "events once superseded", w.post(workerapi.PathEvents, events(2, 2)), workerapi.Fenced, 0)
	wantProblem(t, "a log once superseded", w.upload(LogKey{Part: workerapi.LogSetup}, 0, []byte("x"), false), workerapi.Fenced, 0)
	wantProblem(t, "the end once superseded", w.post(workerapi.PathComplete, workerapi.Complete{Seq: 1, Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}), workerapi.Fenced, 0)
	if s := w.stored(); len(s.Events) != 1 || len(s.Logs) > 0 && len(s.Logs[LogKey{Part: workerapi.LogSetup}].Data) > 0 || s.Complete != nil {
		t.Errorf("a superseded attempt's writes were stored: %+v", s)
	}
}

// A path the API doesn't have is 404, and one of its paths with another
// method is 405.
func checkPaths(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	wantStatus(t, "a path the API doesn't have", w.send(http.MethodPost, "/v1/unknown", nil, nil, w.credential), http.StatusNotFound)
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, workerapi.PathClaim},
		{http.MethodGet, workerapi.PathHeartbeat},
		{http.MethodPost, workerapi.PathControl},
		{http.MethodGet, workerapi.PathEvents},
		{http.MethodPut, workerapi.PathLogs},
		{http.MethodDelete, workerapi.PathComplete},
	} {
		wantStatus(t, fmt.Sprintf("%s %s", r.method, r.path), w.send(r.method, r.path, nil, nil, w.credential), http.StatusMethodNotAllowed)
	}
}

// A FakeTarget is a Target whose attempts are each a Fake, Lenient, served
// on a socket of its own: Conformance checks the Fake through it. Its zero
// value is ready to use.
type FakeTarget struct {
	mu    sync.Mutex
	fakes map[string]*Fake
}

var _ Target = (*FakeTarget)(nil)

// NewAttempt makes a Fake for a.
func (ft *FakeTarget) NewAttempt(t *testing.T, a Attempt) (string, string) {
	t.Helper()
	f := NewFake(t, a.Job)
	f.Lenient = true
	ft.mu.Lock()
	if ft.fakes == nil {
		ft.fakes = map[string]*Fake{}
	}
	id := "attempt" + strconv.Itoa(len(ft.fakes)+1)
	ft.fakes[id] = f
	ft.mu.Unlock()
	f.Token = "token-" + id
	f.Claim.AttemptID = id
	f.Claim.Run = a.Run
	f.Claim.PlanVersion = planVersion(a)
	f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = a.SelfFencing, a.LeaseSeconds, a.HeartbeatSeconds
	return f.Serve(), f.Token
}

func (ft *FakeTarget) fake(t *testing.T, attemptID string) *Fake {
	t.Helper()
	ft.mu.Lock()
	defer ft.mu.Unlock()
	f := ft.fakes[attemptID]
	if f == nil {
		t.Fatalf("no attempt %s", attemptID)
	}
	return f
}

// Supersede supersedes the attempt's Fake.
func (ft *FakeTarget) Supersede(t *testing.T, attemptID string) { ft.fake(t, attemptID).Supersede() }

// Control sets the control of the attempt's Fake.
func (ft *FakeTarget) Control(t *testing.T, attemptID string, c workerapi.Control) {
	ft.fake(t, attemptID).SetControl(c)
}

// Lapse lets the lease of the attempt's Fake lapse.
func (ft *FakeTarget) Lapse(t *testing.T, attemptID string) { ft.fake(t, attemptID).Lapse() }

// Stored returns what the attempt's Fake stored.
func (ft *FakeTarget) Stored(t *testing.T, attemptID string) Stored {
	return ft.fake(t, attemptID).Stored()
}
