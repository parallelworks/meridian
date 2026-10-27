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
	// path of the Unix socket the target serves it on, its bootstrap token
	// and its ID.
	NewAttempt(t *testing.T, a Attempt) (socket, token, attemptID string)
	// Withdraw withdraws the attempt's placement before it is claimed, as
	// when its run is cancelled.
	Withdraw(t *testing.T, attemptID string)
	// Supersede places a newer attempt of the attempt's job, as when its
	// lease lapsed without it.
	Supersede(t *testing.T, attemptID string)
	// Lapse lets the attempt's lease lapse, as when its worker stops
	// heartbeating, but not yet anything the coordinator does after.
	Lapse(t *testing.T, attemptID string)
	// Control asks something more of the attempt's worker: c, at a newer
	// version, which the target gives it.
	Control(t *testing.T, attemptID string, c workerapi.Control)
	// ControlHold is how long the target holds a control poll for a
	// version it has no newer one than, before it answers with that one:
	// workerapi.ControlWaitSeconds at most, and less for a test.
	ControlHold() time.Duration
	// Stored returns what the target stored of the attempt.
	Stored(t *testing.T, attemptID string) Stored
}

// An Attempt is an attempt for a Target to make: of Job, as Ready gave it,
// in a run whose values the job reads are Run, and the results of the
// jobs it needs that it reads Needs, whose plan is of PlanVersion, or
// plan.Version for 0, with its lease's terms.
type Attempt struct {
	Job                            *plan.ReadyJob
	Run                            plan.RunValues
	Needs                          map[string]plan.JobResult
	PlanVersion                    int
	SelfFencing                    bool
	LeaseSeconds, HeartbeatSeconds int64
}

// marker is in the job and the run of every attempt the suite makes, so
// that it can tell an answer that holds any of them.
const marker = "conformance-marker-7f3a"

// newAttempt returns the attempt the suite claims: of a job of three
// steps, whose first reads all the run's values and the needed job's
// results that the attempt gives it, so that a coordinator gives a claim
// exactly those (plan.Job.Reads and ReadsNeeds).
func newAttempt() Attempt {
	reads, err := plan.ValueOf("echo ${{ inputs.note }} ${{ needs.prep.outputs.x }}")
	if err != nil {
		panic(err) //nolint:forbidigo // the suite's own expression, which parses
	}
	steps := []plan.Step{{Run: reads}, {Run: plan.Literal("echo " + marker)}, {Run: plan.Literal("true")}}
	for i := range steps {
		steps[i].Path = "/jobs/build/steps/" + strconv.Itoa(i)
	}
	return Attempt{
		Job:          &plan.ReadyJob{Key: "build", Name: "build", Steps: steps, Path: "/jobs/build"},
		Run:          plan.RunValues{Format: 1, Inputs: plan.DataOf(map[string]any{"note": marker})},
		Needs:        map[string]plan.JobResult{"prep": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": marker}}},
		LeaseSeconds: 60, HeartbeatSeconds: 15,
	}
}

// selfFencing returns an attempt of a job that fences itself.
func selfFencing() Attempt {
	a := newAttempt()
	a.SelfFencing, a.LeaseSeconds, a.HeartbeatSeconds = true, 60, 10
	return a
}

// Conformance checks that target serves the worker API as package
// workerapi says, playing a worker over HTTP on the target's Unix socket:
// the claim and its credential, heartbeats, leases, control polls, events,
// logs, actions and the attempt's end, each with what a coordinator
// refuses, and strict decoding, fencing, error bodies, unknown paths and
// wrong methods.
func Conformance(t *testing.T, target Target) {
	t.Run("claim", func(t *testing.T) { checkClaim(t, target) })
	t.Run("claim again", func(t *testing.T) { checkClaimAgain(t, target) })
	t.Run("plan versions", func(t *testing.T) { checkPlanVersions(t, target) })
	t.Run("API versions", func(t *testing.T) { checkAPIVersions(t, target) })
	t.Run("authorization", func(t *testing.T) { checkAuthorization(t, target) })
	t.Run("heartbeat", func(t *testing.T) { checkHeartbeat(t, target) })
	t.Run("lease", func(t *testing.T) { checkLapse(t, target) })
	t.Run("self-fencing lease", func(t *testing.T) { checkSelfFencingLapse(t, target) })
	t.Run("control", func(t *testing.T) { checkControl(t, target) })
	t.Run("control before the claim", func(t *testing.T) { checkControlBeforeClaim(t, target) })
	t.Run("events", func(t *testing.T) { checkEvents(t, target) })
	t.Run("logs", func(t *testing.T) { checkLogs(t, target) })
	t.Run("complete", func(t *testing.T) { checkComplete(t, target) })
	t.Run("strict decoding", func(t *testing.T) { checkStrict(t, target) })
	t.Run("superseded", func(t *testing.T) { checkSuperseded(t, target) })
	t.Run("paths and methods", func(t *testing.T) { checkPaths(t, target) })
	t.Run("actions", func(t *testing.T) { checkActions(t, target) })
}

// A worker is the suite's worker, for one attempt.
type worker struct {
	t         *testing.T
	target    Target
	attempt   Attempt
	attemptID string
	http      *http.Client
	token     string
	// credential is the one the worker claims with, and claim what it was
	// given, once it has claimed.
	credential string
	claim      workerapi.Claim
	// apiVersion is the newest version of the API the worker speaks:
	// workerapi.Version, unless a check sets an earlier one.
	apiVersion int
}

func newWorker(t *testing.T, target Target, a Attempt) *worker {
	t.Helper()
	socket, token, id := target.NewAttempt(t, a)
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}
	t.Cleanup(tr.CloseIdleConnections)
	return &worker{
		t: t, target: target, attempt: a, attemptID: id, token: token, credential: newCredential(t), apiVersion: workerapi.Version,
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
	method, path string
	status       int
	header       http.Header
	body         []byte
	took         time.Duration
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
	return answer{method: method, path: path, status: resp.StatusCode, header: resp.Header, body: data, took: time.Since(start)}
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

// claimRequest returns the worker's claim, as one that reads planVersion,
// and speaks its version of the API.
func (w *worker) claimRequest(planVersion int) []byte {
	return w.claimRequestAt(w.apiVersion, planVersion)
}

// claimRequestAt returns the worker's claim, as one that speaks apiVersion
// of the API and reads planVersion.
func (w *worker) claimRequestAt(apiVersion, planVersion int) []byte {
	return marshal(w.t, workerapi.ClaimRequest{
		Credential: w.credential, APIVersion: apiVersion, PlanVersion: planVersion,
		Worker: workerapi.Worker{Version: "v0.0.0-conformance", Digest: "sha256:" + strings.Repeat("0", 64)},
	})
}

// claimAgain sends the worker's claim, with its token, and returns the
// answer.
func (w *worker) claimAgain() answer {
	w.t.Helper()
	return w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version), w.token)
}

// claimed claims the attempt, checks the claim, and returns the worker.
func (w *worker) claimed() *worker {
	w.t.Helper()
	a := w.claimAgain()
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
	case c.AttemptID != w.attemptID || c.RunID == "":
		w.t.Errorf("the claim is of run %q and attempt %q, not %s", c.RunID, c.AttemptID, w.attemptID)
	case job.Key != w.attempt.Job.Key || len(job.Steps) != len(w.attempt.Job.Steps):
		w.t.Errorf("the claim's job is %s, of %d steps, not the attempt's", job.Key, len(job.Steps))
	case c.PlanVersion != planVersion(w.attempt):
		w.t.Errorf("the claim's plan version is %d, not the run's %d", c.PlanVersion, planVersion(w.attempt))
	case c.APIVersion != w.apiVersion:
		w.t.Errorf("a worker of version %d of the API was served at %d", w.apiVersion, c.APIVersion)
	case (c.Control != nil) != (c.APIVersion >= workerapi.Version2):
		w.t.Errorf("a claim of version %d of the API holds the control %+v", c.APIVersion, c.Control)
	case c.SelfFencing != w.attempt.SelfFencing || c.LeaseSeconds != w.attempt.LeaseSeconds || c.HeartbeatSeconds != w.attempt.HeartbeatSeconds:
		w.t.Errorf("the claim's lease is %v, %d s, every %d s; the attempt's is %v, %d s, every %d s",
			c.SelfFencing, c.LeaseSeconds, c.HeartbeatSeconds, w.attempt.SelfFencing, w.attempt.LeaseSeconds, w.attempt.HeartbeatSeconds)
	case c.SelfFencing && c.HeartbeatSeconds*5 > c.LeaseSeconds:
		w.t.Errorf("a job that fences itself heartbeats every %d s, more than a fifth of its lease of %d s", c.HeartbeatSeconds, c.LeaseSeconds)
	case !bytes.Equal(marshal(w.t, c.Run), marshal(w.t, w.attempt.Run)):
		w.t.Errorf("the claim's run values are\n%s\nnot what the job reads\n%s", marshal(w.t, c.Run), marshal(w.t, w.attempt.Run))
	case !bytes.Equal(marshal(w.t, c.Needs), marshal(w.t, w.attempt.Needs)):
		w.t.Errorf("the claim's needs are\n%s\nnot what the job reads\n%s", marshal(w.t, c.Needs), marshal(w.t, w.attempt.Needs))
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
	return w.target.Stored(w.t, w.attemptID)
}

// wantStatus checks an answer's status, and that an error is a problem,
// as wantError says.
func wantStatus(t *testing.T, what string, a answer, status int) {
	t.Helper()
	if a.status != status {
		t.Errorf("%s was answered %d, not %d: %s", what, a.status, status, a.body)
		return
	}
	if status >= 400 {
		_ = wantError(t, what, a)
	}
}

// wantError checks that an error answer is an RFC 9457 problem of its
// status, whose code, for an about:blank one, is the status's, as
// unauthenticated for 401; and that a 405 says which method the path
// takes.
func wantError(t *testing.T, what string, a answer) *problem.Problem {
	t.Helper()
	if ct := a.header.Get("Content-Type"); !strings.HasPrefix(ct, problem.MediaType) {
		t.Errorf("%s was answered as %q, not a problem: %s", what, ct, a.body)
		return nil
	}
	p := new(problem.Problem)
	switch err := jsonv2.Unmarshal(a.body, p); {
	case err != nil:
		t.Errorf("%s was answered %s, not a problem: %v", what, a.body, err)
		return nil
	case p.Type == "" || p.Status != a.status:
		t.Errorf("%s was answered %d with a problem of type %q, status %d", what, a.status, p.Type, p.Status)
	case p.Type == problem.Blank && p.Key() != problem.CodeForStatus(a.status):
		t.Errorf("%s was answered with the code %s, not %s", what, p.Key(), problem.CodeForStatus(a.status))
	case a.status == http.StatusUnauthorized && p.Key() != problem.Unauthenticated:
		t.Errorf("%s was answered 401 with the code %s, not unauthenticated", what, p.Key())
	}
	if a.status == http.StatusMethodNotAllowed {
		if allow := a.header.Get("Allow"); !slices.Contains(strings.Split(strings.ReplaceAll(allow, " ", ""), ","), routes[a.path].method) {
			t.Errorf("%s was answered 405, allowing %q, not %s", what, allow, routes[a.path].method)
		}
	}
	return p
}

// wantProblem checks that an answer is a 409 problem of code, and, for
// out_of_order, that it expects one of expected.
func wantProblem(t *testing.T, what string, a answer, code problem.Code, expected ...int64) int64 {
	t.Helper()
	if a.status != http.StatusConflict {
		t.Errorf("%s was answered %d, not 409 %s: %s", what, a.status, code, a.body)
		return 0
	}
	p := wantError(t, what, a)
	if p == nil {
		return 0
	}
	if p.Code != code {
		t.Errorf("%s was answered %s, not a problem of %s", what, a.body, code)
		return 0
	}
	if code != workerapi.OutOfOrder {
		return 0
	}
	got, ok := p.Params[workerapi.ParamExpected].(float64)
	if !ok || !slices.Contains(expected, int64(got)) {
		t.Errorf("%s expects %v, not one of %v", what, p.Params[workerapi.ParamExpected], expected)
	}
	return int64(got)
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

// logQuery returns the query of an upload to a log from offset.
func logQuery(key LogKey, offset int64, end bool) url.Values {
	q := url.Values{workerapi.QueryPart: {string(key.Part)}, workerapi.QueryOffset: {strconv.FormatInt(offset, 10)}}
	if key.Part != workerapi.LogSetup {
		q.Set(workerapi.QueryStep, strconv.Itoa(key.Step))
		q.Set(workerapi.QueryTry, strconv.Itoa(key.Try))
	}
	if end {
		q.Set(workerapi.QueryEnd, "true")
	}
	return q
}

// upload uploads data to a log from offset, and returns the answer.
func (w *worker) upload(key LogKey, offset int64, data []byte, end bool) answer {
	w.t.Helper()
	return w.send(http.MethodPost, workerapi.PathLogs, logQuery(key, offset, end), data, w.credential)
}

// uploaded uploads data, and checks that the log's length is then length,
// and whether it has ended.
func (w *worker) uploaded(key LogKey, offset int64, data string, end bool, length int64, ended bool) {
	w.t.Helper()
	a := w.upload(key, offset, []byte(data), end)
	if a.status != http.StatusOK {
		w.t.Fatalf("%d bytes at %d of the %s log were answered %d: %.200s", len(data), offset, key, a.status, a.body)
	}
	var got workerapi.LogAck
	if err := workerapi.Unmarshal(a.body, &got); err != nil {
		w.t.Fatalf("the acknowledgement isn't one: %v: %s", err, a.body)
	}
	if got.Length != length || got.Ended != ended {
		w.t.Errorf("%d bytes at %d of the %s log: the log is %d bytes, ended %v; want %d, %v", len(data), offset, key, got.Length, got.Ended, length, ended)
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
	again := w.claimAgain()
	if again.status != http.StatusOK {
		t.Fatalf("the claim repeated was answered %d: %s", again.status, again.body)
	}
	var c workerapi.Claim
	if err := workerapi.Unmarshal(again.body, &c); err != nil || !bytes.Equal(marshal(t, c), first) {
		t.Errorf("the claim repeated was answered otherwise:\n%s\nnot\n%s", again.body, first)
	}
	other := *w
	other.credential = newCredential(t)
	wantStatus(t, "the claim with another credential", other.claimAgain(), http.StatusUnauthorized)
}

// A claim repeated once the attempt is superseded, or has ended, is
// fenced, as its other requests are; and a token whose placement was
// withdrawn before it was claimed claims nothing.
func checkClaimAgain(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	target.Supersede(t, w.attemptID)
	wantProblem(t, "the claim repeated once superseded", w.claimAgain(), workerapi.Fenced)

	w = newWorker(t, target, newAttempt()).claimed()
	wantStatus(t, "the attempt's end", w.post(workerapi.PathComplete, workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}), http.StatusNoContent)
	wantProblem(t, "the claim repeated once the attempt ended", w.claimAgain(), workerapi.Fenced)

	w = newWorker(t, target, newAttempt())
	target.Withdraw(t, w.attemptID)
	wantStatus(t, "a claim once the placement was withdrawn", w.claimAgain(), http.StatusUnauthorized)
}

// A worker that reads only an older plan version than the run's is
// refused with plan_too_new, which holds nothing of the run; and a run
// whose version the coordinator doesn't read is answered 503.
func checkPlanVersions(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	a := w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version-1), w.token)
	wantProblem(t, "a claim of a worker that reads an older version", a, workerapi.PlanTooNew)
	if bytes.Contains(a.body, []byte(marker)) || bytes.Contains(a.body, []byte(`"build"`)) {
		t.Errorf("plan_too_new holds the run's values or its job: %s", a.body)
	}
	newer := newAttempt()
	newer.PlanVersion = plan.Version + 1
	w = newWorker(t, target, newer)
	wantStatus(t, "a claim of a run of a newer version", w.send(http.MethodPost, workerapi.PathClaim, nil, w.claimRequest(plan.Version+1), w.token), http.StatusServiceUnavailable)
}

// A worker of a later version of the API than the coordinator's is served
// at the coordinator's, and one of a release from before the API had a
// version, whose claim says none, at version 1; and so is each one's claim
// repeated. A worker of version 1 is served at 1, with no control in its
// claim, and its events with lastTry, which version 1 doesn't have, are
// refused; a worker of version 2's are stored with it.
func checkAPIVersions(t *testing.T, target Target) {
	for _, tc := range []struct {
		name          string
		speaks, serve int
	}{
		{"a worker of a later version", workerapi.Version + 1, workerapi.Version},
		{"a worker without a version", 0, 1},
	} {
		w := newWorker(t, target, newAttempt())
		claim := w.claimRequestAt(tc.speaks, plan.Version)
		for _, again := range []string{"", ", repeated"} {
			what := "the claim of " + tc.name + again
			a := w.send(http.MethodPost, workerapi.PathClaim, nil, claim, w.token)
			if a.status != http.StatusOK {
				t.Fatalf("%s was answered %d: %s", what, a.status, a.body)
			}
			var c workerapi.Claim
			if err := workerapi.Unmarshal(a.body, &c); err != nil {
				t.Fatalf("%s was answered with a claim that isn't one: %v: %s", what, err, a.body)
			}
			if c.APIVersion != tc.serve {
				t.Errorf("%s was served at version %d, not %d", what, c.APIVersion, tc.serve)
			}
		}
	}

	for _, version := range []int{1, workerapi.Version2} {
		w := newWorker(t, target, newAttempt())
		w.apiVersion = version
		w.claimed()
		w.sendEvents(1, 1, 1)
		failed := workerapi.Events{Events: []workerapi.Event{{
			Seq: 2, Time: event(2).Time, Finished: true, LastTry: true, Outcome: plan.OutcomeFailure, ExitCode: 1,
		}}}
		a := w.post(workerapi.PathEvents, failed)
		stored := w.stored().Events
		if version < workerapi.Version2 {
			wantStatus(t, "events of version 1 with lastTry", a, http.StatusBadRequest)
			if len(stored) != 1 {
				t.Errorf("events of version 1 with lastTry stored %d events", len(stored))
			}
			continue
		}
		if a.status != http.StatusOK {
			t.Errorf("events of version %d with lastTry were answered %d: %s", version, a.status, a.body)
		} else if len(stored) != 2 || !stored[1].LastTry {
			t.Errorf("events of version %d with lastTry are stored as %+v", version, stored)
		}
	}
}

// A request is what a worker sends after its claim.
type request struct {
	name, method, path string
	query              url.Values
	body               []byte
}

// requests returns the requests a worker makes after its claim, each as it
// makes it: those that write, then a control poll, unless reads is unset.
func (w *worker) requests(reads bool) []request {
	out := []request{
		{"a heartbeat", http.MethodPost, workerapi.PathHeartbeat, nil, nil},
		{"events", http.MethodPost, workerapi.PathEvents, nil, marshal(w.t, events(1, 1))},
		{"a log upload", http.MethodPost, workerapi.PathLogs, logQuery(LogKey{Part: workerapi.LogSetup}, 0, false), []byte("x")},
		{"the attempt's end", http.MethodPost, workerapi.PathComplete, nil, marshal(w.t, workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}})},
	}
	if reads {
		out = append(out, request{"a control poll", http.MethodGet, workerapi.PathControl, url.Values{workerapi.QueryVersion: {"-1"}}, nil})
	}
	return out
}

// Every request but the claim needs the attempt's credential: not the
// bootstrap token, not another credential, and not before the claim.
func checkAuthorization(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	for _, r := range w.requests(true) {
		wantStatus(t, r.name+" before the claim", w.send(r.method, r.path, r.query, r.body, w.credential), http.StatusUnauthorized)
	}
	w.claimed()
	for _, r := range w.requests(true) {
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
	target.Supersede(t, w.attemptID)
	wantProblem(t, "a heartbeat once superseded", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), workerapi.Fenced)
}

// For a job that doesn't fence itself, the credential lasts until the
// attempt ends, though its lease lapses: a worker cut off from the
// coordinator still delivers what it kept.
func checkLapse(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	target.Lapse(t, w.attemptID)
	w.sendEvents(1, 2, 2)
	w.uploaded(LogKey{Part: workerapi.LogSetup}, 0, "kept\n", true, 5, true)
	wantStatus(t, "the attempt's end once the lease lapsed", w.post(workerapi.PathComplete, workerapi.Complete{Seq: 2, Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}), http.StatusNoContent)
	if s := w.stored(); len(s.Events) != 2 || s.Complete == nil {
		t.Errorf("what was sent once the lease lapsed wasn't stored: %+v", s)
	}
}

// For a job that fences itself, the credential lapses with the lease:
// every request is refused, 401.
func checkSelfFencingLapse(t *testing.T, target Target) {
	w := newWorker(t, target, selfFencing()).claimed()
	wantStatus(t, "a heartbeat", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), http.StatusNoContent)
	target.Lapse(t, w.attemptID)
	for _, r := range w.requests(true) {
		wantStatus(t, r.name+" once the lease lapsed", w.send(r.method, r.path, r.query, r.body, w.credential), http.StatusUnauthorized)
	}
	if s := w.stored(); len(s.Events) > 0 || len(s.Logs) > 0 || s.Complete != nil {
		t.Errorf("requests once the lease lapsed stored %+v", s)
	}
}

// A control poll is answered at once when the coordinator has a newer
// version than the one the worker saw, and otherwise held, for the
// target's hold, until it has; Cancel and AnyJobFailed reach the worker;
// and the version only grows.
func checkControl(t *testing.T, target Target) {
	hold := target.ControlHold()
	if hold <= 0 || hold > workerapi.ControlWaitSeconds*time.Second {
		t.Fatalf("the target holds a control poll %v, not up to %d s", hold, workerapi.ControlWaitSeconds)
	}
	w := newWorker(t, target, newAttempt()).claimed()
	first, took := w.poll(-1)
	if took > hold/2 {
		t.Errorf("a poll for any version took %v", took)
	}
	versions := []int64{first.Version}

	// A poll for the version the coordinator has is held.
	same, took := w.poll(first.Version)
	versions = append(versions, same.Version)
	if took < hold/2 || took > hold+5*time.Second {
		t.Errorf("a poll for the coordinator's version was held %v, not about %v", took, hold)
	}

	// Asked to cancel, while no poll waits.
	cancel := workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 7}
	target.Control(t, w.attemptID, cancel)
	c, took := w.poll(same.Version)
	if took > hold/2 {
		t.Errorf("a poll for a newer version, which the coordinator had, took %v", took)
	}
	versions = append(versions, c.Version)
	if c.Version <= same.Version || !c.Cancel || c.Reason != plan.ReasonCancelled || c.GraceSeconds != 7 {
		t.Errorf("after a cancel the control is %+v, after %+v", c, same)
	}

	// Asked more while a poll waits: the poll, or one after it, has it.
	seen := c.Version
	go func() {
		<-time.After(hold / 4)
		target.Control(t, w.attemptID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 7, AnyJobFailed: true})
	}()
	deadline := time.Now().Add(hold + 10*time.Second)
	for {
		c, took = w.poll(seen)
		versions = append(versions, c.Version)
		if took > hold+5*time.Second {
			t.Errorf("a poll was held %v, longer than %v", took, hold)
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

// What the coordinator asked of an attempt before its claim reaches the
// worker before the job's first step (workerapi.Control): in the claim,
// for a worker of version 2 or later; and for one of version 1, in the
// answer to its first poll, for any version, at once.
func checkControlBeforeClaim(t *testing.T, target Target) {
	hold := target.ControlHold()
	for _, version := range []int{1, workerapi.Version2} {
		w := newWorker(t, target, newAttempt())
		w.apiVersion = version
		target.Control(t, w.attemptID, workerapi.Control{Cancel: true, Reason: plan.ReasonTimeout})
		w.claimed()
		var c workerapi.Control
		if version >= workerapi.Version2 {
			// The claim holds it.
			if w.claim.Control == nil {
				continue
			}
			c = *w.claim.Control
		} else {
			var took time.Duration
			if c, took = w.poll(-1); took > hold/2 {
				t.Errorf("the first poll took %v", took)
			}
		}
		if c.Version <= 0 || !c.Cancel || c.Reason != plan.ReasonTimeout {
			t.Errorf("version %d: the first control is %+v, without what the coordinator asked before the claim", version, c)
		}
	}
}

// Events are stored in order, each once: a repeat is acknowledged as
// before, a gap is out of order, and the acknowledgement is never ahead of
// what was sent. Of a batch with a gap, the coordinator stores the events
// before the gap, or none, and says which by what it expects.
func checkEvents(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	w.sendEvents(1, 1, 1)
	w.sendEvents(1, 1, 1)
	w.sendEvents(2, 3, 3)
	w.sendEvents(3, 4, 4)
	wantProblem(t, "event 6 after 4", w.post(workerapi.PathEvents, events(6, 6)), workerapi.OutOfOrder, 5)
	expected := wantProblem(t, "events 5 and 7", w.post(workerapi.PathEvents, workerapi.Events{Events: []workerapi.Event{event(5), event(7)}}), workerapi.OutOfOrder, 5, 6)
	if n := int64(len(w.stored().Events)); n != expected-1 {
		t.Errorf("of events 5 and 7, the coordinator stored up to %d, and expects %d", n, expected)
	}
	w.sendEvents(5, 6, 6)
	stored := w.stored().Events
	if want := events(1, 6).Events; !bytes.Equal(marshal(t, stored), marshal(t, want)) {
		t.Errorf("the events stored are\n%s\nnot\n%s", marshal(t, stored), marshal(t, want))
	}
}

// A log upload appends what the log doesn't have yet, and an overlap is
// stored once; one past the log's end, or after its end, is out of order;
// one of MaxLogUpload bytes is taken, and one larger is 413; and one of a
// log that isn't one, or of a step the job doesn't have, is 400.
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
	full := LogKey{Part: workerapi.LogStep, Step: 2}
	w.uploaded(full, 0, strings.Repeat("a", workerapi.MaxLogUpload), false, workerapi.MaxLogUpload, false)
	wantStatus(t, "an upload too large", w.upload(LogKey{Part: workerapi.LogStep}, 0, make([]byte, workerapi.MaxLogUpload+1), false), http.StatusRequestEntityTooLarge)
	wantStatus(t, "an upload to a log that isn't one", w.upload(LogKey{Part: "journal"}, 0, []byte("x"), false), http.StatusBadRequest)
	wantStatus(t, "a step the job doesn't have", w.upload(LogKey{Part: workerapi.LogStep, Step: 3}, 0, []byte("x"), false), http.StatusBadRequest)
	wantStatus(t, "a cleanup of a step the job doesn't have", w.upload(LogKey{Part: workerapi.LogCleanup, Step: 3}, 0, []byte("x"), false), http.StatusBadRequest)
	logs := w.stored().Logs
	if l := logs[setup]; string(l.Data) != "abcdef" || !l.Ended {
		t.Errorf("the setup log is %q, ended %v", l.Data, l.Ended)
	}
	if l := logs[step]; string(l.Data) != "hello\n" || !l.Ended {
		t.Errorf("the step's log is %q, ended %v", l.Data, l.Ended)
	}
	if l := logs[full]; len(l.Data) != workerapi.MaxLogUpload {
		t.Errorf("an upload of MaxLogUpload bytes stored %d", len(l.Data))
	}
	for _, key := range []LogKey{{Part: workerapi.LogStep}, {Part: workerapi.LogStep, Step: 3}, {Part: workerapi.LogCleanup, Step: 3}} {
		if l, ok := logs[key]; ok && len(l.Data) > 0 {
			t.Errorf("an upload refused stored %d bytes of the %s log", len(l.Data), key)
		}
	}
}

// The attempt's end is stored once its events are: the same end again is
// answered as before, and another is fenced, as is every write after it.
// An end that says what only the coordinator says, a problem but a
// setup's, or a last event behind those stored, is 400.
func checkComplete(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	w.sendEvents(1, 2, 2)
	success := plan.JobResult{Outcome: plan.OutcomeSuccess}
	wantProblem(t, "an end after events not stored", w.post(workerapi.PathComplete, workerapi.Complete{Seq: 3, Result: success}), workerapi.OutOfOrder, 3)
	for what, end := range map[string]workerapi.Complete{
		"an end whose needs failed":           {Seq: 2, Result: plan.JobResult{Outcome: plan.OutcomeSkipped, NeedsFailed: true}},
		"an end cancelled by the coordinator": {Seq: 2, Result: plan.JobResult{Outcome: plan.OutcomeCancelled, Cancelled: true}},
		"a problem with a success": {Seq: 2, Result: success, Problem: &problem.Problem{
			Type: "/problems/workflow/container_not_supported", Status: 422, Code: "container_not_supported",
		}},
		"an end after event 1 of 2 stored": {Seq: 1, Result: success},
	} {
		wantStatus(t, what, w.post(workerapi.PathComplete, end), http.StatusBadRequest)
	}
	end := workerapi.Complete{Seq: 2, Result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"answer": "42"}}}
	wantStatus(t, "the attempt's end", w.post(workerapi.PathComplete, end), http.StatusNoContent)
	wantStatus(t, "the attempt's end again", w.post(workerapi.PathComplete, end), http.StatusNoContent)
	other := end
	other.Result.Outcome = plan.OutcomeFailure
	wantProblem(t, "another end", w.post(workerapi.PathComplete, other), workerapi.Fenced)
	wantProblem(t, "events after the end", w.post(workerapi.PathEvents, events(3, 3)), workerapi.Fenced)
	wantProblem(t, "a log after the end", w.upload(LogKey{Part: workerapi.LogSetup}, 0, []byte("late"), true), workerapi.Fenced)
	wantProblem(t, "a heartbeat after the end", w.send(http.MethodPost, workerapi.PathHeartbeat, nil, nil, w.credential), workerapi.Fenced)
	s := w.stored()
	if s.Complete == nil || !bytes.Equal(marshal(t, s.Complete), marshal(t, end)) {
		t.Errorf("the end stored is %+v, not %+v", s.Complete, end)
	}
	if len(s.Events) != 2 {
		t.Errorf("%d events are stored, not 2", len(s.Events))
	}
}

// changed returns body with old replaced by new, once, and fails the test
// if body doesn't hold old.
func changed(t *testing.T, body []byte, old, new string) []byte {
	t.Helper()
	if !bytes.Contains(body, []byte(old)) {
		t.Fatalf("%s doesn't hold %s", body, old)
	}
	return bytes.Replace(body, []byte(old), []byte(new), 1)
}

// A body is refused, 400, with a member its type doesn't have, a member
// given twice, one that the API always writes missing, null where its type
// has none, a value a plan type refuses, such as an outcome that isn't
// one, or JSON that isn't.
func checkStrict(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt())
	claim := w.claimRequest(plan.Version)
	for what, body := range map[string][]byte{
		"a claim with an unknown member":         changed(t, claim, `{`, `{"unknown":true,`),
		"a claim with its credential twice":      changed(t, claim, `{`, `{"credential":"`+w.credential+`",`),
		"a claim without its plan version":       changed(t, claim, `,"planVersion":`+strconv.Itoa(plan.Version), ``),
		"a claim whose plan version is null":     changed(t, claim, `"planVersion":`+strconv.Itoa(plan.Version), `"planVersion":null`),
		"a claim whose plan version is text":     changed(t, claim, `"planVersion":`+strconv.Itoa(plan.Version), `"planVersion":"1"`),
		"a claim whose API version is null":      changed(t, claim, `"apiVersion":`+strconv.Itoa(workerapi.Version), `"apiVersion":null`),
		"a claim whose API version is text":      changed(t, claim, `"apiVersion":`+strconv.Itoa(workerapi.Version), `"apiVersion":"1"`),
		"a claim that isn't JSON":                []byte("{"),
		"a claim of two JSON values":             append(slices.Clone(claim), claim...),
		"a claim whose worker's version is null": changed(t, claim, `"version":"v0.0.0-conformance"`, `"version":null`),
	} {
		wantStatus(t, what, w.send(http.MethodPost, workerapi.PathClaim, nil, body, w.token), http.StatusBadRequest)
	}
	w.claimed()
	evs := marshal(t, events(1, 1))
	end := marshal(t, workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}})
	for what, r := range map[string]struct {
		path string
		body []byte
	}{
		"events with an unknown member":          {workerapi.PathEvents, changed(t, evs, `{"events"`, `{"unknown":true,"events"`)},
		"an event with an unknown member":        {workerapi.PathEvents, changed(t, evs, `"seq":1`, `"seq":1,"unknown":true`)},
		"an event with its seq twice":            {workerapi.PathEvents, changed(t, evs, `"seq":1`, `"seq":1,"seq":1`)},
		"events without their list":              {workerapi.PathEvents, []byte(`{}`)},
		"events whose list is null":              {workerapi.PathEvents, []byte(`{"events":null}`)},
		"an event whose outcome isn't one":       {workerapi.PathEvents, marshal(t, workerapi.Events{Events: []workerapi.Event{{Seq: 1, Time: event(1).Time, Finished: true, Outcome: "maybe"}}})},
		"an end with an unknown member":          {workerapi.PathComplete, changed(t, end, `{`, `{"unknown":true,`)},
		"an end with its seq twice":              {workerapi.PathComplete, changed(t, end, `"seq":0`, `"seq":0,"seq":0`)},
		"an end without its seq":                 {workerapi.PathComplete, changed(t, end, `"seq":0,`, ``)},
		"an end whose seq is null":               {workerapi.PathComplete, changed(t, end, `"seq":0`, `"seq":null`)},
		"an end whose result is null":            {workerapi.PathComplete, changed(t, end, `"result":{"outcome":"success"}`, `"result":null`)},
		"an end whose outcome isn't one":         {workerapi.PathComplete, changed(t, end, `"outcome":"success"`, `"outcome":"maybe"`)},
		"an end that isn't JSON":                 {workerapi.PathComplete, []byte("{")},
		"an end whose result's outputs are text": {workerapi.PathComplete, changed(t, end, `"outcome":"success"`, `"outcome":"success","outputs":"x"`)},
	} {
		wantStatus(t, what, w.send(http.MethodPost, r.path, nil, r.body, w.credential), http.StatusBadRequest)
	}
	if s := w.stored(); len(s.Events) > 0 || s.Complete != nil {
		t.Errorf("bodies refused stored %+v", s)
	}
}

// Once another attempt supersedes this one, every write of its is fenced.
func checkSuperseded(t *testing.T, target Target) {
	w := newWorker(t, target, newAttempt()).claimed()
	w.sendEvents(1, 1, 1)
	target.Supersede(t, w.attemptID)
	for _, r := range w.requests(false) {
		if r.path == workerapi.PathComplete {
			r.body = marshal(t, workerapi.Complete{Seq: 1, Result: plan.JobResult{Outcome: plan.OutcomeSuccess}})
		}
		if r.path == workerapi.PathEvents {
			r.body = marshal(t, events(2, 2))
		}
		wantProblem(t, r.name+" once superseded", w.send(r.method, r.path, r.query, r.body, w.credential), workerapi.Fenced)
	}
	if s := w.stored(); len(s.Events) != 1 || len(s.Logs) > 0 && len(s.Logs[LogKey{Part: workerapi.LogSetup}].Data) > 0 || s.Complete != nil {
		t.Errorf("a superseded attempt's writes were stored: %+v", s)
	}
}

// A path the API doesn't have is 404, and one of its paths with another
// method is 405, which says the method it takes.
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
		{http.MethodGet, workerapi.PathAction},
	} {
		wantStatus(t, fmt.Sprintf("%s %s", r.method, r.path), w.send(r.method, r.path, nil, nil, w.credential), http.StatusMethodNotAllowed)
	}
}

// actionStep is the index of the step of actionAttempt's job that uses a
// built-in action the coordinator performs, which has with: actionWith.
const actionStep = 1

// actionAttempt returns an attempt of a job whose step actionStep uses a
// built-in action the coordinator performs, cancel-jobs, of a job its run
// doesn't have, which the coordinator answers as it does, whatever that
// is: none is cancelled.
func actionAttempt() Attempt {
	a := newAttempt()
	job := *a.Job
	job.Steps = slices.Clone(job.Steps)
	job.Steps[actionStep] = plan.Step{
		Uses: "parallelworks/cancel-jobs",
		With: plan.Literal(map[string]any{"jobs": []any{marker}}),
		Path: job.Steps[actionStep].Path,
	}
	a.Job = &job
	return a
}

// actionWith is the with: of actionAttempt's action, as Step evaluates it.
func actionWith() plan.Data {
	return plan.DataOf(map[string]any{"jobs": []any{marker}})
}

// A step's try asks for its action at version 3 of the API, and is
// answered with how it ended, once: a repeat, however its JSON is written,
// is given the same answer, and another request for the same try is
// fenced, as is any request once the attempt is superseded. Once the
// attempt's control asks to cancel its job, a step whose if: doesn't outlive
// that is answered cancelled, for the control's reason; and so, with no
// reason, is a step whose early-cancel: is any-job-failed, once a job of
// the run has failed. A request of
// a step that uses no action the coordinator performs, of a try that is
// none, or one that isn't an ActionRequest, is 400; and the path is one
// an attempt of version 2 doesn't have, 404.
func checkActions(t *testing.T, target Target) {
	old := newWorker(t, target, actionAttempt())
	old.apiVersion = workerapi.Version2
	old.claimed()
	wantStatus(t, "an action of an attempt of version 2", old.post(workerapi.PathAction, workerapi.ActionRequest{Step: actionStep, With: actionWith()}), http.StatusNotFound)

	w := newWorker(t, target, actionAttempt()).claimed()
	req := workerapi.ActionRequest{Step: actionStep, With: actionWith()}
	first := w.post(workerapi.PathAction, req)
	if first.status != http.StatusOK {
		t.Fatalf("an action was answered %d: %s", first.status, first.body)
	}
	var answer workerapi.ActionAnswer
	if err := workerapi.UnmarshalAt(workerapi.Version3, first.body, &answer); err != nil {
		t.Fatalf("an action was answered with %s, which isn't an ActionAnswer: %v", first.body, err)
	}
	if o := answer.Outcome; o != plan.OutcomeSuccess && o != plan.OutcomeFailure && o != plan.OutcomeCancelled {
		t.Errorf("an action ended %q", o)
	}
	// The same request, its members in another order, as JSON allows.
	again := w.send(http.MethodPost, workerapi.PathAction, nil, []byte(`{"with":{"jobs":["`+marker+`"]},"step":1}`), w.credential)
	if again.status != http.StatusOK || !bytes.Equal(again.body, first.body) {
		t.Errorf("an action asked for again was answered %d: %s, not as it was first: %s", again.status, again.body, first.body)
	}
	other := req
	other.With = plan.DataOf(map[string]any{"jobs": []any{"another"}})
	wantProblem(t, "another request for an action's try", w.post(workerapi.PathAction, other), workerapi.Fenced)
	next := req
	next.Try = 1
	if a := w.post(workerapi.PathAction, next); a.status != http.StatusOK {
		t.Errorf("an action's next try was answered %d: %s", a.status, a.body)
	}

	// Once the attempt's control asks to cancel its job, a step whose if:
	// doesn't outlive that, as the run's plan has it, is answered
	// cancelled, for the control's reason, and one whose if: does has its
	// action performed; each answer says the cancel.
	target.Control(t, w.attemptID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
	outliving := actionAttempt()
	outliving.Job.Steps[actionStep].If = plan.Literal("always")
	ow := newWorker(t, target, outliving).claimed()
	target.Control(t, ow.attemptID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
	next.Try++
	for _, tc := range []struct {
		w        *worker
		req      workerapi.ActionRequest
		outlives bool
	}{{w, next, false}, {ow, req, true}} {
		a := tc.w.post(workerapi.PathAction, tc.req)
		var answer workerapi.ActionAnswer
		if a.status != http.StatusOK || workerapi.UnmarshalAt(workerapi.Version3, a.body, &answer) != nil {
			t.Errorf("an action of a cancelled job, of a step that outlives that %v, was answered %d: %s", tc.outlives, a.status, a.body)
			continue
		}
		if cancelled := answer.Outcome == plan.OutcomeCancelled; cancelled == tc.outlives || answer.Reason != plan.ReasonCancelled {
			t.Errorf("an action of a cancelled job, of a step that outlives that %v, was answered %s", tc.outlives, a.body)
		}
	}

	// A step whose early-cancel: is any-job-failed is answered cancelled,
	// with no reason, once a job of the run has failed, though it outlives
	// its job's cancellation.
	early := actionAttempt()
	early.Job.Steps[actionStep].EarlyCancel = plan.EarlyCancelAnyJobFailed
	early.Job.Steps[actionStep].If = plan.Literal("always")
	ew := newWorker(t, target, early).claimed()
	target.Control(t, ew.attemptID, workerapi.Control{AnyJobFailed: true})
	if a := ew.post(workerapi.PathAction, req); a.status != http.StatusOK || string(a.body) != `{"outcome":"cancelled"}` {
		t.Errorf("an action of a step that early-cancels, once a job failed, was answered %d: %s", a.status, a.body)
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"an action of a step that runs a command", marshal(t, workerapi.ActionRequest{Step: 0})},
		{"an action of a step the job doesn't have", marshal(t, workerapi.ActionRequest{Step: len(w.attempt.Job.Steps)})},
		{"an action of a step before the first", []byte(`{"step":-1}`)},
		{"an action of a try before the first", []byte(`{"step":1,"try":-1}`)},
		{"an action's request with a member it doesn't have", []byte(`{"step":1,"uses":"parallelworks/checkout"}`)},
		{"an action's request that says its step outlives a cancel, which the plan says", []byte(`{"step":1,"outlivesCancel":true}`)},
		{"an action's request whose with: is null", []byte(`{"step":1,"with":null}`)},
		{"an action's request that isn't JSON", []byte(`{"step":`)},
	} {
		wantStatus(t, tc.name, w.send(http.MethodPost, workerapi.PathAction, nil, tc.body, w.credential), http.StatusBadRequest)
	}

	target.Supersede(t, w.attemptID)
	wantProblem(t, "an action asked for again once superseded", w.post(workerapi.PathAction, req), workerapi.Fenced)
	next.Try = 2
	wantProblem(t, "an action once superseded", w.post(workerapi.PathAction, next), workerapi.Fenced)
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
func (ft *FakeTarget) NewAttempt(t *testing.T, a Attempt) (string, string, string) {
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
	f.Claim.Run, f.Claim.Needs = a.Run, a.Needs
	f.Claim.PlanVersion = planVersion(a)
	f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = a.SelfFencing, a.LeaseSeconds, a.HeartbeatSeconds
	return f.Serve(), f.Token, id
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

// Withdraw withdraws the placement of the attempt's Fake.
func (ft *FakeTarget) Withdraw(t *testing.T, attemptID string) { ft.fake(t, attemptID).Withdraw() }

// Supersede supersedes the attempt's Fake.
func (ft *FakeTarget) Supersede(t *testing.T, attemptID string) { ft.fake(t, attemptID).Supersede() }

// Lapse lets the lease of the attempt's Fake lapse.
func (ft *FakeTarget) Lapse(t *testing.T, attemptID string) { ft.fake(t, attemptID).Lapse() }

// Control sets the control of the attempt's Fake.
func (ft *FakeTarget) Control(t *testing.T, attemptID string, c workerapi.Control) {
	ft.fake(t, attemptID).SetControl(c)
}

// ControlHold is a Fake's hold: ControlHold.
func (*FakeTarget) ControlHold() time.Duration { return ControlHold }

// Stored returns what the attempt's Fake stored.
func (ft *FakeTarget) Stored(t *testing.T, attemptID string) Stored {
	return ft.fake(t, attemptID).Stored()
}
