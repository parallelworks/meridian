package coordinator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/runlog/runlogtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// runWorkflow runs workflow from its start to its end with meridian's
// workers, as TestRunsConformance does, and returns the rig and the run's
// ID.
func runWorkflow(t *testing.T, workflow string) (*workerRig, string) {
	t.Helper()
	r := newWorkerRig(t, nil)
	id := r.create(t, []byte(workflow), nil, nil)
	serve(t, r.c)
	waitEnded(t, r.c, id)
	return r, id
}

// jobOutcomes returns how each of the run's jobs ended, by key, and how
// many attempts each had.
func jobOutcomes(t *testing.T, c *Coordinator, id string) (map[string]plan.Outcome, map[string]int) {
	t.Helper()
	var jobs []*store.Job
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		jobs, err = tx.Jobs(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	outcomes, attempts := map[string]plan.Outcome{}, map[string]int{}
	for _, j := range jobs {
		outcomes[j.Key], attempts[j.Key] = j.Outcome, j.Attempts
	}
	return outcomes, attempts
}

// stepLog returns the log of the step's first try, or its cleanup's, of
// the latest attempt of the run's job key.
func stepLog(t *testing.T, r *workerRig, id, key string, step int, cleanup bool) string {
	t.Helper()
	j := storedJob(t, r.c, id, key)
	part := workerapi.LogStep
	if cleanup {
		part = workerapi.LogCleanup
	}
	return string(r.logs.Of(id, key, j.Attempts)[workerapitest.LogKey{Part: part, Step: step}].Data)
}

// TestCancelJobsWaiting checks a cancel of a job that waits on a job it
// needs, which is still running, a bug fix to format 1: the job ends
// cancelled at once and never starts, nor is placed; the job that needs it
// is skipped because its needs failed, and the run fails. The job it
// waits on finishes only once the cancel is answered.
func TestCancelJobsWaiting(t *testing.T) {
	t.Parallel()
	r, id := runWorkflow(t, `
jobs:
  first:
    steps:
      - run: until [ -f cancelled ]; do sleep 0.02; done
  canceller:
    steps:
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [waiting]
      - run: touch cancelled
  waiting:
    needs: [first]
    steps:
      - run: touch ran
  after:
    needs: [waiting]
    steps:
      - run: echo after
`)
	outcomes, attempts := jobOutcomes(t, r.c, id)
	want := map[string]plan.Outcome{
		"first": plan.OutcomeSuccess, "canceller": plan.OutcomeSuccess,
		"waiting": plan.OutcomeCancelled, "after": plan.OutcomeSkipped,
	}
	if !mapsEqual(outcomes, want) || attempts["waiting"] != 0 {
		t.Errorf("the jobs ended %v, with %v attempts", outcomes, attempts)
	}
	if run := storedRun(t, r.c, id); run.Outcome != plan.OutcomeFailure {
		t.Errorf("the run ended %s", run.Outcome)
	}
	j := storedJob(t, r.c, id, "waiting")
	if j.Reason != plan.ReasonCancelled || j.CancelledBy != "/jobs/canceller/steps/0" {
		t.Errorf("the cancelled job is %+v", j)
	}
	if after := storedJob(t, r.c, id, "after"); after.Result == nil || !after.Result.NeedsFailed {
		t.Errorf("the job that needs it ended %+v", after.Result)
	}
	if _, err := os.Stat(filepath.Join(r.runs, id, "ran")); err == nil {
		t.Error("the cancelled job ran")
	}
}

// TestCancelJobsRunning checks a cancel of a job that runs: its step is
// stopped, it ends cancelled, and its steps that outlive a cancel, and its
// cleanups, run; the run, in which no job failed, succeeds, as in format
// 1.
func TestCancelJobsRunning(t *testing.T) {
	t.Parallel()
	r, id := runWorkflow(t, `
jobs:
  target:
    steps:
      - run: touch started; sleep 60
        cleanup: echo cleaned
      - run: echo skipped
      - if: ${{ always() }}
        run: echo always
  canceller:
    steps:
      - run: until [ -f started ]; do sleep 0.02; done
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [target]
`)
	outcomes, _ := jobOutcomes(t, r.c, id)
	if want := (map[string]plan.Outcome{"target": plan.OutcomeCancelled, "canceller": plan.OutcomeSuccess}); !mapsEqual(outcomes, want) {
		t.Errorf("the jobs ended %v", outcomes)
	}
	if run := storedRun(t, r.c, id); run.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended %s", run.Outcome)
	}
	if got := stepLog(t, r, id, "target", 2, false); got != "always\n" {
		t.Errorf("the always() step's log is %q", got)
	}
	if got := stepLog(t, r, id, "target", 0, true); got != "cleaned\n" {
		t.Errorf("the cleanup's log is %q", got)
	}
	if e := finishedStep(t, r.c, id, "target", 0, 0); e.Outcome != plan.OutcomeCancelled {
		t.Errorf("the stopped step ended %s", e.Outcome)
	}
}

// TestCancelJobsItself checks a step that cancels its own job: it
// succeeds, and the steps after it run as after a cancel, before any of
// them, however soon they come: the first that comes to run whose if:
// passes ends cancelled without starting, and from then on cancelled()
// is true; the job ends cancelled, and the run succeeds.
func TestCancelJobsItself(t *testing.T) {
	t.Parallel()
	r, id := runWorkflow(t, `
jobs:
  main:
    steps:
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [main]
      - run: echo success
      - if: ${{ failure() }}
        run: echo failure
      - if: ${{ cancelled() }}
        run: echo cancelled
      - if: ${{ !success() }}
        run: echo not success
      - if: ${{ always() }}
        run: echo always
      - if: false
        run: echo never
`)
	if run := storedRun(t, r.c, id); run.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended %s", run.Outcome)
	}
	want := []plan.Outcome{
		plan.OutcomeSuccess, plan.OutcomeCancelled, plan.OutcomeSkipped,
		plan.OutcomeSuccess, plan.OutcomeSuccess, plan.OutcomeSuccess, plan.OutcomeSkipped,
	}
	for step, outcome := range want {
		if e := finishedStep(t, r.c, id, "main", step, 0); e.Outcome != outcome {
			t.Errorf("step %d ended %s, want %s", step, e.Outcome, outcome)
		}
	}
	if j := storedJob(t, r.c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled {
		t.Errorf("the job ended %s (%s)", j.Outcome, j.Reason)
	}
}

// TestCancelJobsRun checks a step without jobs:, or with an empty one,
// which cancels its run, as a person does: it succeeds, the run ends
// cancelled, and its other job, running, ends cancelled too.
func TestCancelJobsRun(t *testing.T) {
	t.Parallel()
	for _, with := range []string{"", "\n        with:\n          jobs: []"} {
		r, id := runWorkflow(t, `
jobs:
  other:
    steps:
      - run: touch started; sleep 60
  canceller:
    steps:
      - run: until [ -f started ]; do sleep 0.02; done
      - uses: parallelworks/cancel-jobs`+with+`
`)
		run := storedRun(t, r.c, id)
		if run.Outcome != plan.OutcomeCancelled || run.StopReason != plan.ReasonCancelled {
			t.Errorf("with %q, the run ended %s, stopped for %s", with, run.Outcome, run.StopReason)
		}
		outcomes, _ := jobOutcomes(t, r.c, id)
		if outcomes["other"] != plan.OutcomeCancelled {
			t.Errorf("with %q, the jobs ended %v", with, outcomes)
		}
		if e := finishedStep(t, r.c, id, "canceller", 1, 0); e.Outcome != plan.OutcomeSuccess {
			t.Errorf("with %q, the step ended %s", with, e.Outcome)
		}
	}
}

// TestCancelJobsNames checks what a cancel-jobs step does with names that
// it can't cancel: a name that is no job of its run, with a suggestion,
// and a matrix job's, warn, and cancel nothing; a job that has ended is
// left as it is; and a name that isn't text, which Step refuses on the
// worker, and a cancel of another run's jobs, fail the step, each with a
// problem at its key, and cancel nothing.
func TestCancelJobsNames(t *testing.T) {
	t.Parallel()
	r, id := runWorkflow(t, `
jobs:
  done:
    steps:
      - run: echo done
  matrix:
    strategy:
      matrix:
        n: [1]
    steps:
      - run: echo ${{ matrix.n }}
  main:
    needs: [done, matrix]
    steps:
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [donne, matrix, done]
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [done, "${{ fromJSON('1') }}"]
      - if: ${{ always() }}
        uses: parallelworks/cancel-jobs
        with:
          run: 7
          jobs: [done]
`)
	outcomes, _ := jobOutcomes(t, r.c, id)
	if outcomes["done"] != plan.OutcomeSuccess || outcomes["matrix"] != plan.OutcomeSuccess || outcomes["main"] != plan.OutcomeFailure {
		t.Errorf("the jobs ended %v", outcomes)
	}
	e := finishedStep(t, r.c, id, "main", 0, 0)
	if e.Outcome != plan.OutcomeSuccess || len(e.Diagnostics) != 2 {
		t.Fatalf("the step of unknown names ended %s, with %+v", e.Outcome, e.Diagnostics)
	}
	for i, want := range []struct {
		pointer   string
		job       string
		suggested string
	}{
		{"#/jobs/main/steps/0/with/jobs/0", "donne", "true"},
		{"#/jobs/main/steps/0/with/jobs/1", "matrix", "false"},
	} {
		d := e.Diagnostics[i]
		if d.Code != problems.CancelJobsJobUnknown.Code() || d.Pointer != want.pointer || d.Params["job"] != want.job || d.Params["suggested"] != want.suggested {
			t.Errorf("warning %d is %+v at %q, %#v", i, d, d.Pointer, d.Params)
		}
	}
	if got := stepLog(t, r, id, "main", 0, false); !strings.Contains(got, `"donne"`) || !strings.Contains(got, `"done"?`) {
		t.Errorf("the step's log is %q", got)
	}
	for step, want := range map[int]struct{ code, pointer string }{
		// Step finds it on the worker, before it asks.
		1: {string(problems.WrongType.Code()), "#/jobs/main/steps/1/with/jobs/1"},
		2: {string(problems.CancelJobsOtherRun.Code()), "#/jobs/main/steps/2/with/run"},
	} {
		e := finishedStep(t, r.c, id, "main", step, 0)
		if e.Outcome != plan.OutcomeFailure || len(e.Diagnostics) != 1 || string(e.Diagnostics[0].Code) != want.code || e.Diagnostics[0].Pointer != want.pointer {
			t.Errorf("step %d ended %s, with %+v at %q", step, e.Outcome, e.Diagnostics, e.Diagnostics[0].Pointer)
		}
	}
}

// mapsEqual reports whether two maps hold the same.
func mapsEqual[K, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// An actionRig is a coordinator whose run's attempts the test makes and
// claims itself, in its store, and whose worker API serves them on a
// socket, so that a test can send their steps' requests for actions as it
// pleases, without workers.
type actionRig struct {
	c    *Coordinator
	http *http.Client
}

// newActionRig returns an actionRig, its worker API served until the test
// ends.
func newActionRig(t *testing.T) *actionRig {
	t.Helper()
	return newActionRigWith(t, Options{})
}

// newActionRigWith returns an actionRig whose coordinator is opened with
// opts, as newActionRig does.
func newActionRigWith(t *testing.T, opts Options) *actionRig {
	t.Helper()
	c := open(t, opts)
	srv, err := workerapisrv.New(workerapisrv.Options{
		Store: c.store, Logs: &runlogtest.Logs{}, Sealer: c.sealer, Readier: claimReadier{c}, Actions: actions{c}, Children: childAPI{c},
		Wake: c.wakeWorkerJob,
	})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "meridian-act-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s")
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return new(net.Dialer).DialContext(ctx, "unix", socket)
	}}
	t.Cleanup(func() {
		tr.CloseIdleConnections()
		stop()
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	return &actionRig{c: c, http: &http.Client{Transport: tr, Timeout: time.Minute}}
}

// start stores and starts a run of workflow, and returns its ID.
func (r *actionRig) start(t *testing.T, workflow string) string {
	t.Helper()
	id, _ := create(t, r.c, workflow, nil, nil)
	if _, err := r.c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	return id
}

// attempt makes an attempt of the run's job key, placed, and claimed
// unless placed is set, as a worker of this release claims it, and
// returns its ID, its credential and its bootstrap token.
func (r *actionRig) attempt(t *testing.T, id, key string, placed bool) (string, string, string) {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	credential := hex.EncodeToString(b)
	var attempt, token string
	if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
		a, tok, err := tx.NewAttempt(r.c.newAttempt(id, key, false))
		if err != nil {
			return err
		}
		attempt, token = a.ID, tok
		if err := tx.RecordPlacement(a.ID, store.Placement{Backend: testBackend, ID: "test-" + a.ID, WorkerDigest: testDigest}); err != nil || placed {
			return err
		}
		_, err = tx.Claim(tok, credential, workerapi.Worker{Version: "test", Digest: testDigest}, workerapi.Version)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return attempt, credential, token
}

// act sends the request of the step's first try for its action, with with,
// with the credential, and returns the answer.
func (r *actionRig) act(t *testing.T, credential string, step int, with map[string]any) workerapi.ActionAnswer {
	t.Helper()
	return r.ask(t, credential, workerapi.ActionRequest{Step: step, With: plan.DataOf(with)})
}

// ask sends req with the credential, and returns the answer.
func (r *actionRig) ask(t *testing.T, credential string, req workerapi.ActionRequest) workerapi.ActionAnswer {
	t.Helper()
	body, err := workerapi.Marshal(&req)
	if err != nil {
		t.Fatal(err)
	}
	hr, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://coordinator"+workerapi.PathAction, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	hr.Header.Set("Authorization", "Bearer "+credential)
	hr.Header.Set("Content-Type", "application/json")
	resp, err := r.http.Do(hr)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var answer workerapi.ActionAnswer
	data := new(bytes.Buffer)
	if _, err := data.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the action was answered %d: %s", resp.StatusCode, data)
	}
	if err := workerapi.Unmarshal(data.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	return answer
}

// control returns what the attempt id's control asks.
func (r *actionRig) control(t *testing.T, id string) workerapi.Control {
	t.Helper()
	var c workerapi.Control
	if err := r.c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		c, err = tx.Control(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return c
}

// mutualWorkflow is a run whose two jobs each cancel the other.
const mutualWorkflow = `
jobs:
  a:
    steps:
      - run: echo a
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [b]
  b:
    steps:
      - run: echo b
      - uses: parallelworks/cancel-jobs
        with:
          jobs: [a]
`

// TestCancelJobsEachOther checks two jobs that cancel each other at once,
// many times over: exactly one is cancelled each time. Its step succeeds,
// and the other's is answered cancelled, and cancels nothing.
func TestCancelJobsEachOther(t *testing.T) {
	t.Parallel()
	r := newActionRig(t)
	for range 50 {
		id := r.start(t, mutualWorkflow)
		attempts := map[string]string{}
		credentials := map[string]string{}
		for _, key := range []string{"a", "b"} {
			attempts[key], credentials[key], _ = r.attempt(t, id, key, false)
		}
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			answers = map[string]workerapi.ActionAnswer{}
		)
		for key, other := range map[string]string{"a": "b", "b": "a"} {
			wg.Go(func() {
				answer := r.act(t, credentials[key], 1, map[string]any{"jobs": []any{other}})
				mu.Lock()
				answers[key] = answer
				mu.Unlock()
			})
		}
		wg.Wait()
		var cancelled []string
		for _, key := range []string{"a", "b"} {
			if !storedJob(t, r.c, id, key).CancelledAt.IsZero() {
				cancelled = append(cancelled, key)
			}
		}
		if len(cancelled) != 1 {
			t.Fatalf("run %s: the jobs cancelled are %v, with the answers %+v", id, cancelled, answers)
		}
		loser := cancelled[0]
		winner := map[string]string{"a": "b", "b": "a"}[loser]
		if answers[winner].Outcome != plan.OutcomeSuccess || answers[winner].Reason != "" ||
			answers[loser].Outcome != plan.OutcomeCancelled || answers[loser].Reason != plan.ReasonCancelled {
			t.Fatalf("run %s: %s was cancelled, with the answers %+v", id, loser, answers)
		}
		if ctl := r.control(t, attempts[loser]); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
			t.Fatalf("the cancelled job's attempt is told %+v", ctl)
		}
		if ctl := r.control(t, attempts[winner]); ctl.Cancel {
			t.Fatalf("the other job's attempt is told %+v", ctl)
		}
	}
}

// TestCancelJobsAttempts checks a cancel of a job whose attempt is placed
// and not yet claimed: its control asks to cancel the job, which its
// claim gives the worker, as its first control; a cancel of a job whose
// worker was then lost ends it cancelled at its supervisor's next look,
// and places it no more; and an attempt made after the cancel, as by a
// supervisor that looked before it, is told from the start.
func TestCancelJobsAttempts(t *testing.T) {
	t.Parallel()
	r := newActionRig(t)
	id := r.start(t, mutualWorkflow)
	_, credential, _ := r.attempt(t, id, "a", false)
	placed, _, _ := r.attempt(t, id, "b", true)
	if answer := r.act(t, credential, 1, map[string]any{"jobs": []any{"b"}}); answer.Outcome != plan.OutcomeSuccess || answer.Reason != "" {
		t.Fatalf("the cancel was answered %+v", answer)
	}
	if ctl := r.control(t, placed); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
		t.Errorf("the placed attempt is told %+v", ctl)
	}
	// Lost before its claim: placed no more, but ended, cancelled.
	if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(placed, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.c.supervise(t.Context(), id, "b", func(context.Context, *store.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if j := storedJob(t, r.c, id, "b"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled || j.Attempts != 1 {
		t.Errorf("the job is %s (%s), with %d attempts", j.Outcome, j.Reason, j.Attempts)
	}

	// Another run, whose job b is cancelled before its first attempt is
	// made, but by a supervisor that looked before.
	id = r.start(t, mutualWorkflow)
	_, credential, _ = r.attempt(t, id, "a", false)
	if answer := r.act(t, credential, 1, map[string]any{"jobs": []any{"b"}}); answer.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the cancel was answered %+v", answer)
	}
	if j := storedJob(t, r.c, id, "b"); j.Outcome != plan.OutcomeCancelled || j.Attempts != 0 {
		t.Errorf("a job cancelled before its first attempt is %s, with %d attempts", j.Outcome, j.Attempts)
	}
}

// TestCancelJobsOutlived checks that a step whose if: outlives its job's
// cancellation, as the run's plan has it, still cancels, though its own
// job is cancelled already, and is told so.
func TestCancelJobsOutlived(t *testing.T) {
	t.Parallel()
	r := newActionRig(t)
	id := r.start(t, strings.Replace(mutualWorkflow, "      - uses: parallelworks/cancel-jobs\n        with:\n          jobs: [b]", "      - uses: parallelworks/cancel-jobs\n        if: ${{ always() }}\n        with:\n          jobs: [b]", 1))
	a, credentialA, _ := r.attempt(t, id, "a", false)
	_, credentialB, _ := r.attempt(t, id, "b", false)
	if answer := r.act(t, credentialB, 1, map[string]any{"jobs": []any{"a"}}); answer.Outcome != plan.OutcomeSuccess {
		t.Fatalf("b's cancel was answered %+v", answer)
	}
	answer := r.ask(t, credentialA, workerapi.ActionRequest{Step: 1, With: plan.DataOf(map[string]any{"jobs": []any{"b"}})})
	if answer.Outcome != plan.OutcomeSuccess || answer.Reason != plan.ReasonCancelled {
		t.Fatalf("a's cancel, which outlives a's own, was answered %+v", answer)
	}
	for _, key := range []string{"a", "b"} {
		if storedJob(t, r.c, id, key).CancelledAt.IsZero() {
			t.Errorf("job %s isn't cancelled", key)
		}
	}
	if ctl := r.control(t, a); !ctl.Cancel {
		t.Errorf("a's attempt is told %+v", ctl)
	}
}

// TestCancelJobsOwnRun checks the forms of with: by which format 1 names
// the run whose jobs a cancel-jobs step cancels: a negative run, the run's
// own workflow and number, and its own number alone name its own run, as
// no slug, workflow or run does; a slug, which a Meridian run has none of,
// another workflow, or another number, name another run, and fail the step
// (cancel_jobs_other_run), as does a workflow without a run
// (cancel_jobs_run_missing), and a run that isn't a whole number, or a
// slug or a workflow that isn't a string (wrong_type), each cancelling
// nothing; null and "" are as if not given.
func TestCancelJobsOwnRun(t *testing.T) {
	t.Parallel()
	r := newActionRig(t)
	const named = "name: flows\n" + mutualWorkflow
	for _, tc := range []struct {
		name string
		with func(own int64) map[string]any
		// own is set for a form that names the step's own run, whose job b
		// it cancels; code and key for one that fails the step.
		own  bool
		code string
		key  string
	}{
		{"a negative run", func(int64) map[string]any { return map[string]any{"run": int64(-1)} }, true, "", ""},
		{"its own number", func(own int64) map[string]any { return map[string]any{"run": own} }, true, "", ""},
		{"its own workflow and number", func(own int64) map[string]any { return map[string]any{"workflow": "flows", "run": own} }, true, "", ""},
		{"null and empty", func(int64) map[string]any { return map[string]any{"slug": "", "workflow": nil, "run": nil} }, true, "", ""},
		{"null and empty, the other way", func(int64) map[string]any { return map[string]any{"slug": nil, "workflow": ""} }, true, "", ""},
		{"another number", func(own int64) map[string]any { return map[string]any{"run": own + 1} }, false, string(problems.CancelJobsOtherRun.Code()), "run"},
		{"run 0", func(int64) map[string]any { return map[string]any{"run": int64(0)} }, false, string(problems.CancelJobsOtherRun.Code()), "run"},
		{"another workflow", func(own int64) map[string]any { return map[string]any{"workflow": "other", "run": own} }, false, string(problems.CancelJobsOtherRun.Code()), "workflow"},
		{"another number of its workflow", func(own int64) map[string]any { return map[string]any{"workflow": "flows", "run": own + 1} }, false, string(problems.CancelJobsOtherRun.Code()), "workflow"},
		{"a slug", func(own int64) map[string]any { return map[string]any{"slug": "s", "run": int64(-1)} }, false, string(problems.CancelJobsOtherRun.Code()), "slug"},
		{"a workflow without a run", func(int64) map[string]any { return map[string]any{"workflow": "flows"} }, false, string(problems.CancelJobsRunMissing.Code()), "workflow"},
		{"a workflow with a negative run", func(int64) map[string]any { return map[string]any{"workflow": "flows", "run": int64(-1)} }, false, string(problems.CancelJobsRunMissing.Code()), "workflow"},
		{"a run that isn't whole", func(int64) map[string]any { return map[string]any{"run": 1.5} }, false, wireCode(t, wrongType("/", 1.5, "integer")), "run"},
		{"a run of false", func(int64) map[string]any { return map[string]any{"run": false} }, false, wireCode(t, wrongType("/", false, "integer")), "run"},
		{"a slug of false", func(int64) map[string]any { return map[string]any{"slug": false} }, false, wireCode(t, wrongType("/", false, "string")), "slug"},
		{"a workflow that is a number", func(own int64) map[string]any { return map[string]any{"workflow": int64(7), "run": own} }, false, wireCode(t, wrongType("/", int64(7), "string")), "workflow"},
	} {
		id := r.start(t, named)
		_, credential, _ := r.attempt(t, id, "a", false)
		own, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		with := tc.with(own)
		with["jobs"] = []any{"b"}
		answer := r.act(t, credential, 1, with)
		cancelled := !storedJob(t, r.c, id, "b").CancelledAt.IsZero()
		if tc.own {
			if answer.Outcome != plan.OutcomeSuccess || len(answer.Diagnostics) != 0 || !cancelled {
				t.Errorf("%s: answered %+v, and b cancelled %v", tc.name, answer, cancelled)
			}
			continue
		}
		if answer.Outcome != plan.OutcomeFailure || len(answer.Diagnostics) != 1 || cancelled ||
			string(answer.Diagnostics[0].Code) != tc.code || answer.Diagnostics[0].Pointer != "#/jobs/a/steps/1/with/"+tc.key {
			t.Errorf("%s: answered %+v, and b cancelled %v", tc.name, answer, cancelled)
		}
	}

	// Its own workflow and number, without jobs:, cancel its own run.
	id := r.start(t, named)
	_, credential, _ := r.attempt(t, id, "a", false)
	own, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if answer := r.act(t, credential, 1, map[string]any{"workflow": "flows", "run": own}); answer.Outcome != plan.OutcomeSuccess {
		t.Errorf("a cancel of its own run, by its workflow and number, was answered %+v", answer)
	}
	if run := storedRun(t, r.c, id); run.StopReason != plan.ReasonCancelled {
		t.Errorf("the run is stopped for %q", run.StopReason)
	}
}

// wireCode returns the code d has once the worker API has carried it,
// which, for a kind of one of foundation's shared rules, is the rule's.
func wireCode(t *testing.T, d problems.Diagnostic) string {
	t.Helper()
	data, err := workerapi.Marshal(&d)
	if err != nil {
		t.Fatal(err)
	}
	var back problems.Diagnostic
	if err := workerapi.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	return string(back.Code)
}
