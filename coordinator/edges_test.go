package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// nopDone is a supervisor's done that completes no hopper job, as a test's
// looks have none.
func nopDone(context.Context, *store.Tx) error { return nil }

// TestJobTimeoutWithdrawn checks the job timeout's backstop to its end: a
// worker that, past its job's timeout, neither stops the job, once told to,
// nor renews its lease, though its backend says it runs, has its placement
// withdrawn once the lease lapses, and the job fails, with the reason
// timeout and job_timeout_enforced, as the worker would have failed it.
func TestJobTimeoutWithdrawn(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	c.timing.WorkerStop, c.timing.Heartbeat, c.timing.FenceMargin = 0, time.Second, 0
	id, a := startJob(t, c, timedJob, store.AttemptRunning)
	d.says(backend.Status{State: backend.Running}, nil)
	// Past the job's timeout, of a second, and a heartbeat period, well
	// within the lease.
	clock.Advance(3 * time.Second)
	if wait := look(t, c, id); wait <= 0 {
		t.Fatalf("within the lease, the supervisor waits %v", wait)
	}
	if ctl := controlOf(t, c, a.ID); !ctl.Cancel || ctl.Reason != plan.ReasonTimeout {
		t.Errorf("the worker past its job's timeout is told %+v", ctl)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != "" {
		t.Fatalf("within the lease, the job ended %s", j.Outcome)
	}
	clock.Advance(c.timing.Lease)
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("once the lease lapsed, the supervisor waits %v", wait)
	}
	j := storedJob(t, c, id, "main")
	if j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonTimeout || !slices.Equal(jobCodes(j), []string{"job_timeout_enforced"}) {
		t.Errorf("the job is %+v", j)
	}
	if got := latestAttempt(t, c, id, "main"); got.End.Kind != store.EndWithdrawn {
		t.Errorf("the attempt ended %+v", got.End)
	}
	d.testDriver.mu.Lock()
	withdrawn := len(d.cancelled)
	d.testDriver.mu.Unlock()
	if withdrawn != 1 {
		t.Errorf("the placement was withdrawn %d times", withdrawn)
	}
}

// privateTimeout is a run whose timeout: a variable gives, which run state
// doesn't hold.
const privateTimeout = `
timeout: ${{ org.T }}
jobs:
  main:
    steps:
      - run: echo main
`

// TestRunTimeoutPrivate checks a run's timeout: made from a variable: its
// watcher reads it from the run's secret record, waits for it a minute at
// most at a time, so that hopper's schedule doesn't show it, and stops the
// run once it has passed, as for any timeout.
func TestRunTimeoutPrivate(t *testing.T) {
	t.Parallel()
	t.Run("waits", func(t *testing.T) {
		parallel(t)
		vars := conformancetest.Variables{"org.T": {Value: "10m"}}
		c := open(t, Options{Variables: vars})
		id, _ := create(t, c, privateTimeout, nil, vars)
		if _, err := c.startRun(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
		if r := storedRun(t, c, id); !r.StartState.TimeoutPrivate || r.StartState.TimeoutSeconds != 0 {
			t.Errorf("run state holds the timeout %d, private %v", r.StartState.TimeoutSeconds, r.StartState.TimeoutPrivate)
		}
		if wait, err := c.watchTimeout(t.Context(), id); err != nil || wait <= 0 || wait > time.Minute {
			t.Errorf("the watcher of a private timeout of 10 minutes waits %v, %v", wait, err)
		}
		// A timeout that isn't private is waited for whole.
		public, _ := create(t, c, "timeout: 10m\njobs:\n  main:\n    steps:\n      - run: echo main\n", nil, nil)
		if _, err := c.startRun(t.Context(), public, nil); err != nil {
			t.Fatal(err)
		}
		if wait, err := c.watchTimeout(t.Context(), public); err != nil || wait <= time.Minute {
			t.Errorf("the watcher of a timeout of 10 minutes waits %v, %v", wait, err)
		}
	})
	t.Run("stops", func(t *testing.T) {
		parallel(t)
		vars := conformancetest.Variables{"org.T": {Value: "1s"}}
		c, _ := openStopWith(t, Options{Variables: vars}, map[string]string{"main": obey})
		id, _ := create(t, c, privateTimeout, nil, vars)
		serve(t, c)
		r := waitEnded(t, c, id)
		if r.Outcome != plan.OutcomeFailure || r.Reason != plan.ReasonTimeout || r.StopReason != plan.ReasonTimeout {
			t.Errorf("the run ended %s (%s), stopped for %s", r.Outcome, r.Reason, r.StopReason)
		}
	})
}

// TestPlanNewerRunning checks a run that a newer release started, as in a
// rolling upgrade, whose plan this release doesn't read: its job's
// supervisor leaves it as it is, its attempt running however long ago its
// lease lapsed, and says why, so that a replica of the newer release sees
// it through.
func TestPlanNewerRunning(t *testing.T) {
	parallel(t)
	log := &logRecords{}
	clock := newFakeClock()
	c, d := openLeaseWith(t, Options{now: clock.Now, Log: slog.New(log)}, []bool{true}, []bool{true})
	id, a := startJob(t, c, oneJob, store.AttemptRunning)
	d.says(backend.Status{State: backend.Dead}, nil)
	exec(t, c, "DROP TRIGGER meridian_runs_plan_fixed")
	exec(t, c, fmt.Sprintf("UPDATE meridian_runs SET plan_version = %d WHERE id = %s", plan.Version+1, id))
	// A release that read the plan would have lost the worker by now.
	clock.Advance(2 * c.timing.Lease)
	if wait := look(t, c, id); wait != c.retry.newer {
		t.Errorf("the supervisor of a run of a newer plan waits %v, not %v", wait, c.retry.newer)
	}
	if !log.has(string(problems.RunPlanNewer.Code())) {
		t.Error("the supervisor didn't say why it left the run")
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != "" {
		t.Errorf("the job of a run of a newer plan ended %s", j.Outcome)
	}
	if got := latestAttempt(t, c, id, "main"); got.ID != a.ID || got.State != store.AttemptRunning || d.checked() != 0 {
		t.Errorf("the attempt is %s, %s, and the backend was asked %d times", got.ID, got.State, d.checked())
	}
}

// TestFollowSetupFailure checks that Follow reports the problem an attempt
// failed before its first step with, with the attempt's end, as meridian
// run shows it.
func TestFollowSetupFailure(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	failSetup(c, setupProblems[1])
	r := runToEnd(t, c, oneJob, nil)
	var f followed
	if _, err := c.Follow(t.Context(), r.ID, f.options(nil)); err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(f.events(), func(e Event) bool { return e.Kind == AttemptEnded && e.Reason == plan.ReasonSetupFailed })
	if i < 0 {
		t.Fatalf("no attempt ended failing its setup: %+v", f.events())
	}
	e := f.events()[i]
	want := setupProblems[1]
	if len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != want.Code || e.Diagnostics[0].Params["path"] != want.Params["path"] {
		t.Errorf("the attempt's end has %+v, want the problem %s", e.Diagnostics, want.Code)
	}
}

// TestEndJobRaces checks a supervisor's end of its job as another changes
// the job before it: a job another ended is left as that one ended it
// (errSettled), and one with an attempt that hasn't ended is looked at
// again (errAgain); neither stores the end.
func TestEndJobRaces(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	id, attempt := claimedAttempt(t, c)
	success := store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	if err := c.endJobWith(t.Context(), id, "main", success, nil, nopDone); !errors.Is(err, errAgain) {
		t.Errorf("an end of a job whose attempt runs = %v, want errAgain", err)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != "" {
		t.Errorf("the job whose attempt runs ended %s", j.Outcome)
	}
	// Another supervisor ends it, failed.
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		if err := tx.EndAttempt(attempt, store.AttemptEnd{Kind: store.EndWithdrawn}); err != nil {
			return err
		}
		return tx.EndJob(id, "main", store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}})
	}); err != nil {
		t.Fatal(err)
	}
	done := false
	if err := c.endJobWith(t.Context(), id, "main", success, nil, func(context.Context, *store.Tx) error { done = true; return nil }); !errors.Is(err, errSettled) {
		t.Errorf("an end of a job another ended = %v, want errSettled", err)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeFailure || done {
		t.Errorf("the job another ended is %s, and the second end completed its hopper job: %v", j.Outcome, done)
	}
}

// TestExpandedMeanwhile checks a matrix job's supervisor that expands a
// matrix another expanded since it looked, as two of the same hopper job
// at once can, when one's lease lapsed: the second stores nothing, and
// looks again.
func TestExpandedMeanwhile(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	byJob(c, map[string]testEnd{
		"first":   {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"parts": `["p", "q"]`}}},
		"after#0": {result: plan.JobResult{Outcome: plan.OutcomeSuccess}, wait: release},
		"after#1": {result: plan.JobResult{Outcome: plan.OutcomeSuccess}, wait: release},
	})
	id, _ := create(t, c, `
jobs:
  first:
    steps:
      - id: parts
        run: echo parts='["p", "q"]' >> $OUTPUTS
    outputs:
      parts: ${{ steps.parts.outputs.parts }}
  after:
    needs: [first]
    strategy:
      matrix:
        part: ${{ fromJSON(needs.first.outputs.parts) }}
    steps:
      - run: echo ${{ matrix.part }}
`, nil, nil)
	serve(t, c)
	waitFor(t, func() bool { return len(instancesOf(t, c, id, "after")) == 2 })
	// The job as a supervisor that looked before the other expanded it
	// read it.
	stale := *storedJob(t, c, id, "after")
	stale.Expansion = nil
	if _, err := c.expand(t.Context(), storedRun(t, c, id), &stale, nopDone); !errors.Is(err, errAgain) {
		t.Errorf("expanding a matrix expanded meanwhile = %v, want errAgain", err)
	}
	if n := len(instancesOf(t, c, id, "after")); n != 2 {
		t.Errorf("the matrix job has %d instances", n)
	}
}

// TestAnyChosenMeanwhile checks the marker of a need on any instance that
// chooses as another chose since it looked: the first choice is kept, and
// the second neither stores its own nor completes its hopper job.
func TestAnyChosenMeanwhile(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	id, _ := create(t, c, anyWorkflow, nil, nil)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	// The matrix job's supervisor inserts its instances, and the second's
	// places it; its worker succeeds, and its supervisor ends it.
	if _, err := c.supervise(t.Context(), id, "build", nopDone); err != nil {
		t.Fatal(err)
	}
	if _, err := c.supervise(t.Context(), id, "build#1", nopDone); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		a := latestAttempt(t, c, id, "build#1")
		return a != nil && a.State == store.AttemptEnded
	})
	if _, err := c.supervise(t.Context(), id, "build#1", nopDone); err != nil {
		t.Fatal(err)
	}
	// Another marker holds the store's write lock as this one looks, and
	// chooses the first instance before this one's choice is stored.
	held, choose := make(chan struct{}), make(chan struct{})
	other := make(chan error, 1)
	go func() {
		other <- c.store.Tx(context.WithoutCancel(t.Context()), func(tx *store.Tx) error {
			if _, err := tx.Now(); err != nil {
				return err
			}
			close(held)
			<-choose
			return tx.ChooseAny(id, "build", "build#0", nil)
		})
	}()
	<-held
	done := false
	chose := make(chan error, 1)
	go func() {
		_, err := c.chooseAny(context.WithoutCancel(t.Context()), id, "build", func(context.Context, *store.Tx) error { done = true; return nil })
		chose <- err
	}()
	// Long enough for this marker to have read the store, as it does
	// before it takes the lock; if it hadn't, it reads the other's choice.
	<-time.After(200 * time.Millisecond)
	close(choose)
	if err := <-other; err != nil {
		t.Fatal(err)
	}
	if err := <-chose; !errors.Is(err, errSettled) {
		t.Errorf("a choice made as another was = %v, want errSettled", err)
	}
	var a *store.Any
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		a, err = tx.Any(id, "build")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if a.Instance != "build#0" || done {
		t.Errorf("the choice is %q, and the second marker completed its hopper job: %v", a.Instance, done)
	}
}
