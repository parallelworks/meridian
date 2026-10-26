package workerapisrv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	jsonv2 "encoding/json/v2"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// envChild names what a process the tests start does, and envPath and
// envAttempt the store and the attempt it does it to.
const (
	envChild   = "MERIDIAN_WORKERAPISRV_CHILD"
	envPath    = "MERIDIAN_WORKERAPISRV_STORE"
	envAttempt = "MERIDIAN_WORKERAPISRV_ATTEMPT"
)

// runChild does what role names, in a process of a test's own, and returns
// its exit status.
func runChild(role string) int {
	ctx := context.Background() //nolint:forbidigo // a process of a test's own, which has no test to take a context from
	s, err := store.Open(ctx, os.Getenv(envPath), nil)
	if err == nil {
		switch role {
		case "control":
			err = s.Tx(ctx, func(tx *store.Tx) error {
				_, err := tx.SetControl(os.Getenv(envAttempt), workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
				return err
			})
		default:
			err = fmt.Errorf("no role %q", role)
		}
		err = errors.Join(err, s.Close())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// claimed claims a new attempt of a run of the rig's, of a job the readier
// gives as job, and returns the attempt, its run and the credential.
func (r *rig) claimed(c *client, p placed) (*store.Attempt, string, string) {
	r.t.Helper()
	run := r.newRun(nil)
	job := &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true"), Path: "/jobs/build/steps/0"}}}
	r.readier.set(run, &workerapisrv.Ready{Job: job, Run: plan.RunValues{Format: 1}})
	a, token := r.newAttempt(run, "build", p)
	credential := newCredential(r.t)
	status, body, err := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(r.t, credential, conformanceDigest), token)
	if err != nil || status != http.StatusOK {
		r.t.Fatalf("the claim was answered %d, %v: %s", status, err, body)
	}
	return a, run, credential
}

// wantProblem fails the test unless an answer is a problem of code, and
// returns it.
func wantProblem(t *testing.T, what string, status int, body []byte, code problem.Code) *problem.Problem {
	t.Helper()
	p := new(problem.Problem)
	if err := jsonv2.Unmarshal(body, p); err != nil || p.Code != code {
		t.Fatalf("%s was answered %d: %s; want a problem of %s", what, status, body, code)
	}
	return p
}

// A worker whose control poll waits is answered once another process
// stores a newer control, which no Notify of this one's says: the poll
// reads the store again each second.
func TestControlAcrossProcesses(t *testing.T) {
	r := newRig(t, rigOptions{hold: 20 * time.Second})
	c := newClient(t, r.socket)
	a, _, credential := r.claimed(c, placed{})
	type answer struct {
		status int
		body   []byte
		err    error
		took   time.Duration
	}
	answered := make(chan answer, 1)
	go func() {
		start := time.Now()
		status, body, err := c.send(http.MethodGet, workerapi.PathControl+"?"+workerapi.QueryVersion+"=0", nil, credential)
		answered <- answer{status, body, err, time.Since(start)}
	}()
	// The poll is held, until the other process stores the control.
	select {
	case got := <-answered:
		t.Fatalf("a poll for the version the coordinator has was answered at once: %d %s", got.status, got.body)
	case <-time.After(1500 * time.Millisecond):
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=^$")
	cmd.Env = append(os.Environ(), envChild+"=control", envPath+"="+r.path, envAttempt+"="+a.ID)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the other process: %v: %s", err, out)
	}
	stored := time.Now()
	got := <-answered
	if got.err != nil || got.status != http.StatusOK {
		t.Fatalf("the poll was answered %d, %v: %s", got.status, got.err, got.body)
	}
	var control workerapi.Control
	if err := workerapi.Unmarshal(got.body, &control); err != nil || !control.Cancel || control.Version != 1 {
		t.Fatalf("the poll was answered %s, %v", got.body, err)
	}
	if late := time.Since(stored); late > 5*time.Second {
		t.Errorf("the poll was answered %v after the other process stored the control", late)
	}
}

// refusingPeers refuses every connection, as from another user, or allows
// none of the runs.
type refusingPeers struct{ connections bool }

func (p refusingPeers) Accept(uid int) bool { return !p.connections && uid == os.Getuid() }

func (p refusingPeers) Allows(context.Context, int, *store.Run) bool { return false }

// A connection from a user the server doesn't take requests from is closed
// before a request is read; the local server takes only its own user's
// and root's; and a peer that may not report on a run's attempts is
// refused, 401, as one with no credential is.
func TestPeers(t *testing.T) {
	if !(workerapisrv.LocalPeers{}).Accept(os.Getuid()) || !(workerapisrv.LocalPeers{}).Accept(0) {
		t.Error("the local server doesn't take its own user's or root's requests")
	}
	if other := os.Getuid() + 1; other != 0 && (workerapisrv.LocalPeers{}).Accept(other) {
		t.Errorf("the local server takes user %d's requests", other)
	}

	r := newRig(t, rigOptions{peers: refusingPeers{connections: true}})
	a, token := r.newAttempt(r.newRun(nil), "build", placed{})
	status, body, err := newClient(t, r.socket).send(http.MethodPost, workerapi.PathClaim, claimRequest(t, newCredential(t), conformanceDigest), token)
	if err == nil {
		t.Errorf("a connection of another user was answered %d: %s", status, body)
	}
	if got := r.attempt(a.ID); !got.ClaimedAt.IsZero() {
		t.Error("a connection of another user claimed the attempt")
	}

	r = newRig(t, rigOptions{peers: refusingPeers{}})
	a, token = r.newAttempt(r.newRun(nil), "build", placed{})
	status, body, err = newClient(t, r.socket).send(http.MethodPost, workerapi.PathClaim, claimRequest(t, newCredential(t), conformanceDigest), token)
	if err != nil || status != http.StatusUnauthorized {
		t.Errorf("a claim of a run the peer may not report on was answered %d, %v: %s", status, err, body)
	}
	if got := r.attempt(a.ID); !got.ClaimedAt.IsZero() {
		t.Error("a peer that may not report on the run claimed its attempt")
	}
}

// A body larger than the API allows is refused, 413, and nothing of it is
// stored.
func TestBodyLimits(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	_, token := r.newAttempt(r.newRun(nil), "build", placed{})
	claim := claimRequest(t, newCredential(t), conformanceDigest)
	padded := append(bytes.Repeat([]byte(" "), workerapisrv.MaxClaimBody), claim...)
	if status, body, err := c.send(http.MethodPost, workerapi.PathClaim, padded, token); err != nil || status != http.StatusRequestEntityTooLarge {
		t.Errorf("a claim of %d bytes was answered %d, %v: %s", len(padded), status, err, body)
	}

	a, _, credential := r.claimed(c, placed{})
	for path, limit := range map[string]int{workerapi.PathEvents: workerapisrv.MaxEventsBody, workerapi.PathComplete: workerapisrv.MaxCompleteBody} {
		body := append([]byte(`{"events":[],"x":"`), bytes.Repeat([]byte("a"), limit)...)
		if status, answer, err := c.send(http.MethodPost, path, body, credential); err != nil || status != http.StatusRequestEntityTooLarge {
			t.Errorf("%s of %d bytes was answered %d, %v: %.200s", path, len(body), status, err, answer)
		}
	}
	if got := r.attempt(a.ID); got.State == store.AttemptEnded {
		t.Error("an end too large was stored")
	}
}

// A worker that isn't the binary the attempt's placement started is
// refused before anything of the run is sent, and the attempt ends, failed
// before its first step, with the problem, which its job's supervisor is
// woken to place again.
func TestDigestMismatch(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	run := r.newRun(nil)
	r.readier.set(run, &workerapisrv.Ready{Job: &plan.ReadyJob{Key: "build", Name: "build", Path: "/jobs/build", Steps: []plan.Step{{Run: plan.Literal("true")}}}})
	placedDigest := "sha256:" + strings.Repeat("a", 64)
	a, token := r.newAttempt(run, "build", placed{digest: placedDigest})
	status, body, err := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, newCredential(t), conformanceDigest), token)
	if err != nil || status != http.StatusConflict {
		t.Fatalf("a claim of another binary was answered %d, %v: %s", status, err, body)
	}
	p := wantProblem(t, "a claim of another binary", status, body, workerapi.WorkerDigestMismatch)
	if p.Params[workerapi.ParamPlaced] != placedDigest || p.Params[workerapi.ParamClaimed] != conformanceDigest {
		t.Errorf("the problem's params are %v", p.Params)
	}
	if bytes.Contains(body, []byte(`"build"`)) || bytes.Contains(body, []byte("/runs/")) {
		t.Errorf("the refusal holds something of the run: %s", body)
	}
	got := r.attempt(a.ID)
	if got.State != store.AttemptEnded || got.End.Kind != store.EndSetupFailed || got.End.Reason != plan.ReasonSetupFailed ||
		got.End.Problem == nil || got.End.Problem.Code != problems.WorkerDigestMismatch.Code() || !got.ClaimedAt.IsZero() {
		t.Errorf("the attempt is %+v, ended %+v", got, got.End)
	}
	if woken := r.wokenJobs(); !slices.Contains(woken, run+"/build") {
		t.Errorf("the job's supervisor wasn't woken: %v", woken)
	}
	// The token claims nothing once the attempt has ended.
	if status, body, _ := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, newCredential(t), placedDigest), token); status != http.StatusUnauthorized {
		t.Errorf("a claim once the attempt ended was answered %d: %s", status, body)
	}

	// The binary the placement started claims it.
	a, token = r.newAttempt(run, "build", placed{digest: placedDigest})
	if status, body, _ := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, newCredential(t), placedDigest), token); status != http.StatusOK {
		t.Errorf("a claim of the binary placed was answered %d: %s", status, body)
	}
	if got := r.attempt(a.ID); got.Worker.Digest != placedDigest || got.ClaimedAt.IsZero() {
		t.Errorf("the attempt is %+v", got)
	}
}

// Once a newer attempt of its job supersedes an attempt, every write of the
// attempt's is fenced, and stores nothing; the newer attempt's claim and
// reports are stored.
func TestSuperseded(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, run, credential := r.claimed(c, placed{})
	b, token := r.newAttempt(run, "build", placed{})
	events, err := workerapi.Marshal(workerapi.Events{Events: []workerapi.Event{{Seq: 1, Time: time.Unix(1, 0).UTC()}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []struct {
		path string
		body []byte
	}{
		{workerapi.PathHeartbeat, nil},
		{workerapi.PathEvents, events},
		{workerapi.PathComplete, []byte(`{"seq":0,"result":{"outcome":"success"}}`)},
	} {
		status, body, err := c.send(http.MethodPost, req.path, req.body, credential)
		if err != nil || status != http.StatusConflict {
			t.Fatalf("%s once superseded was answered %d, %v: %s", req.path, status, err, body)
		}
		_ = wantProblem(t, req.path+" once superseded", status, body, workerapi.Fenced)
	}
	if got := r.attempt(a.ID); got.End.Kind != store.EndSuperseded || got.StepStartedAt.IsZero() == false {
		t.Errorf("the superseded attempt is %+v", got)
	}
	newer := newCredential(t)
	if status, body, _ := c.send(http.MethodPost, workerapi.PathClaim, claimRequest(t, newer, conformanceDigest), token); status != http.StatusOK {
		t.Fatalf("the newer attempt's claim was answered %d: %s", status, body)
	}
	if status, body, _ := c.send(http.MethodPost, workerapi.PathEvents, events, newer); status != http.StatusOK {
		t.Errorf("the newer attempt's events were answered %d: %s", status, body)
	}
	if got := r.attempt(b.ID); got.State != store.AttemptRunning {
		t.Errorf("the newer attempt is %s, not running", got.State)
	}
}

// An end with private outputs stores them only in the run's secret record,
// sealed again with the next generation, and the rest with the attempt.
func TestCompletePrivate(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	a, run, credential := r.claimed(c, placed{})
	end := workerapi.Complete{Result: plan.JobResult{
		Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"plain": "1", "token": "s3cr3t-v4lue"}, Secrets: []string{"token"},
	}}
	body, err := workerapi.Marshal(end)
	if err != nil {
		t.Fatal(err)
	}
	if status, answer, _ := c.send(http.MethodPost, workerapi.PathComplete, body, credential); status != http.StatusNoContent {
		t.Fatalf("the end was answered %d: %s", status, answer)
	}
	got := r.attempt(a.ID)
	if bytes.Contains(got.End.Report, []byte("s3cr3t-v4lue")) || !bytes.Contains(got.End.Report, []byte(`"plain":"1"`)) {
		t.Errorf("the attempt's report is %s", got.End.Report)
	}
	var stored *store.Record
	r.inTx(func(tx *store.Tx) error {
		stored, err = tx.Record(run)
		return err
	})
	if stored.Generation != 2 {
		t.Errorf("the record's generation is %d, not 2", stored.Generation)
	}
	rec, err := r.sealer.Open(t.Context(), run, stored.Generation, seal.Sealed{Key: stored.WrappedKey, Record: stored.Sealed})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Jobs["build"].Outputs["token"] != "s3cr3t-v4lue" || rec.Jobs["build"].Outputs["plain"] != "" {
		t.Errorf("the record holds %+v of the job", rec.Jobs["build"])
	}
	// The same end again is answered as before, and changes nothing.
	if status, answer, _ := c.send(http.MethodPost, workerapi.PathComplete, body, credential); status != http.StatusNoContent {
		t.Errorf("the end again was answered %d: %s", status, answer)
	}
	r.inTx(func(tx *store.Tx) error {
		stored, err = tx.Record(run)
		return err
	})
	if stored.Generation != 2 {
		t.Errorf("the end again sealed the record again, to generation %d", stored.Generation)
	}
}

// The server wakes a job's supervisor once its attempt is claimed, once a
// step of it starts, and once it ends.
func TestWake(t *testing.T) {
	r := newRig(t, rigOptions{})
	c := newClient(t, r.socket)
	_, run, credential := r.claimed(c, placed{})
	want := []string{run + "/build"}
	if got := r.wokenJobs(); !slices.Equal(got, want) {
		t.Errorf("after the claim, woken %v, want %v", got, want)
	}
	events, _ := workerapi.Marshal(workerapi.Events{Events: []workerapi.Event{{Seq: 1, Time: time.Unix(1, 0).UTC()}}})
	if status, body, _ := c.send(http.MethodPost, workerapi.PathEvents, events, credential); status != http.StatusOK {
		t.Fatalf("events were answered %d: %s", status, body)
	}
	if status, body, _ := c.send(http.MethodPost, workerapi.PathComplete, []byte(`{"seq":1,"result":{"outcome":"success"}}`), credential); status != http.StatusNoContent {
		t.Fatalf("the end was answered %d: %s", status, body)
	}
	want = append(want, run+"/build", run+"/build")
	if got := r.wokenJobs(); !slices.Equal(got, want) {
		t.Errorf("woken %v, want %v", got, want)
	}
}

// A request that didn't come through Serve, whose listener says who a
// connection is from, is refused.
func TestNotThroughServe(t *testing.T) {
	r := newRig(t, rigOptions{})
	rec := &recorder{header: http.Header{}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, workerapi.PathHeartbeat, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+strconv.Itoa(1))
	r.server.ServeHTTP(rec, req)
	if rec.status != http.StatusUnauthorized {
		t.Errorf("a request not through Serve was answered %d", rec.status)
	}
}

// A recorder records an answer's status.
type recorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}

func (r *recorder) WriteHeader(status int) { r.status = status }
