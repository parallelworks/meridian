package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// The ways a stopDriver's worker does its attempt, by its job's key.
const (
	// obey claims the attempt, starts a step, and once told to stop,
	// ends the attempt as a worker does: cancelled, or failed with the
	// reason timeout.
	obey = "obey"
	// hold is obey, but ends the attempt only once the test releases it.
	hold = "hold"
	// outlive claims the attempt, starts a step, and ends it with success
	// once the test releases it, whatever it is told, as a job whose if:
	// is cancel-aware runs on.
	outlive = "outlive"
	// late claims the attempt only once the run has been stopped, and
	// then obeys.
	late = "late"
	// never never claims the attempt.
	never = "never"
	// silent claims the attempt, starts a step, and then says nothing.
	silent = "silent"
)

// A stopDriver is a testDriver whose workers do as the test says, by job,
// and which records the placements it was asked to cancel.
type stopDriver struct {
	*testDriver

	mu        sync.Mutex
	ways      map[string]string
	cancelled []string
	release   chan struct{}
}

func (d *stopDriver) Cancel(_ context.Context, _ *backend.Member, p backend.Placement) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cancelled = append(d.cancelled, p.Attempt)
	return nil
}

// withdrawn returns the attempts whose placements d was asked to cancel.
func (d *stopDriver) withdrawn() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.cancelled...)
}

var placeName = regexp.MustCompile(`^meridian-\d+-(.+)$`)

func (d *stopDriver) worker(req backend.PlaceRequest) testEnd {
	key := placeName.FindStringSubmatch(req.Name)[1]
	d.mu.Lock()
	way := d.ways[key]
	d.mu.Unlock()
	if way == "" {
		return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	}
	d.workers.Go(func() { d.work(req, way) })
	return testEnd{never: true}
}

// work is the worker of the attempt req is for, which does as way says.
func (d *stopDriver) work(req backend.PlaceRequest, way string) {
	ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
	defer cancel()
	var a *store.Attempt
	until := func(ok func() bool) bool {
		for !ok() {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(5 * time.Millisecond):
			}
		}
		return true
	}
	if !until(func() bool {
		err := d.c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			a, err = tx.Attempt(req.Attempt)
			return err
		})
		return err == nil && a.Placed()
	}) || way == never {
		return
	}
	if way == late && !until(func() bool {
		var r *store.Run
		err := d.c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			r, err = tx.Run(a.Run)
			return err
		})
		return err == nil && !r.StoppedAt.IsZero()
	}) {
		return
	}
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	if err := d.c.store.Tx(ctx, func(tx *store.Tx) error {
		claimed, err := tx.Claim(req.BootstrapToken, hex.EncodeToString(b), workerapi.Worker{Version: "test", Digest: testDigest}, workerapi.Version)
		if err != nil {
			return err
		}
		return tx.StepStarted(claimed.ID)
	}); err != nil || way == silent {
		return
	}
	var ctl workerapi.Control
	if way != outlive && !until(func() bool {
		err := d.c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			ctl, err = tx.Control(a.ID)
			return err
		})
		return err == nil && ctl.Cancel
	}) {
		return
	}
	if way == hold || way == outlive {
		select {
		case <-ctx.Done():
			return
		case <-d.release:
		}
	}
	report := workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	switch {
	case way == outlive:
	case ctl.Reason == plan.ReasonTimeout:
		report = workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonTimeout}
	default:
		report = workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeCancelled}, Reason: ctl.Reason}
	}
	body, err := workerapi.Marshal(&report)
	if err != nil {
		return
	}
	if err := d.c.store.Tx(ctx, func(tx *store.Tx) error {
		return tx.EndAttempt(a.ID, store.AttemptEnd{
			Kind: store.EndCompleted, Outcome: report.Result.Outcome, Reason: report.Reason, Report: body,
		})
	}); err != nil {
		return
	}
	d.c.wakeJob(ctx, a.Run, a.Job)
}

// openStop opens a coordinator with a stopDriver whose workers do as ways
// says, by job.
func openStop(t *testing.T, ways map[string]string) (*Coordinator, *stopDriver) {
	t.Helper()
	return openStopWith(t, Options{}, ways)
}

// openStopWith is openStop, with opts.
func openStopWith(t *testing.T, opts Options, ways map[string]string) (*Coordinator, *stopDriver) {
	t.Helper()
	d := &stopDriver{testDriver: newTestDriver(), ways: ways, release: make(chan struct{})}
	opts.Drivers = map[string]backend.Driver{testBackend: d}
	c := open(t, opts)
	// Its workers run with the test's context, as its testDriver's do,
	// which open sets only for a testDriver of its own.
	d.c, d.ctx = c, t.Context()
	d.testDriver.worker = d.worker
	t.Cleanup(d.workers.Wait)
	return c, d
}

// latestIs waits for the latest attempt of the run id's job key to be as
// want, and returns it.
func latestIs(t *testing.T, c *Coordinator, id, key string, want func(*store.Attempt) bool) *store.Attempt {
	t.Helper()
	var a *store.Attempt
	waitFor(t, func() bool {
		err := c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			a, err = tx.LatestAttempt(id, key)
			return err
		})
		return err == nil && want(a)
	})
	return a
}

func running(a *store.Attempt) bool { return a.State == store.AttemptRunning }

// controlOf returns what the attempt id's control asks.
func controlOf(t *testing.T, c *Coordinator, id string) workerapi.Control {
	t.Helper()
	var ctl workerapi.Control
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		ctl, err = tx.Control(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return ctl
}

// TestCancelRunning checks a cancel during a step: the stop record first,
// which tells the attempt's worker to stop, for the reason cancelled; the
// job ends cancelled, as its worker ended it, and so does the run.
func TestCancelRunning(t *testing.T) {
	parallel(t)
	c, _ := openStop(t, map[string]string{"main": obey})
	id, _ := create(t, c, oneJob, nil, nil)
	serve(t, c)
	a := latestIs(t, c, id, "main", running)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	r := waitEnded(t, c, id)
	if r.Outcome != plan.OutcomeCancelled || r.Reason != plan.ReasonCancelled || r.StopReason != plan.ReasonCancelled || r.StoppedAt.IsZero() {
		t.Errorf("the run ended %s (%s), stopped for %s", r.Outcome, r.Reason, r.StopReason)
	}
	if ctl := controlOf(t, c, a.ID); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
		t.Errorf("the attempt's control is %+v", ctl)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled {
		t.Errorf("the job is %+v", j)
	}
	// Cancelling it again: it has ended.
	if err := c.Cancel(t.Context(), id); !errors.Is(err, ErrRunEnded) {
		t.Errorf("cancelling an ended run = %v", err)
	}
	if err := c.Cancel(t.Context(), "9"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("cancelling a run the store hasn't = %v", err)
	}
}

// alwaysJob runs on through a cancel: its if: is cancel-aware.
const alwaysJob = `
jobs:
  main:
    if: always()
    steps:
      - run: echo main
`

// TestCancelOutlives checks that a job whose if: is cancel-aware runs on
// through a cancel: its worker isn't told to stop, and the job succeeds,
// while the run is cancelled.
func TestCancelOutlives(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"main": outlive})
	id, _ := create(t, c, alwaysJob, nil, nil)
	serve(t, c)
	a := latestIs(t, c, id, "main", running)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	// Its supervisor has looked at it since the stop. The wake's retry
	// tells the queue's producer as its transaction commits, so the
	// supervisor runs again within milliseconds; ten polls' time leaves a
	// loaded runner room.
	c.wakeJob(t.Context(), id, "main")
	time.Sleep(10 * testPollInterval) //nolint:forbidigo // nothing says the supervisor has looked
	if ctl := controlOf(t, c, a.ID); ctl.Cancel {
		t.Errorf("a cancel-aware job's attempt was told to stop: %+v", ctl)
	}
	close(d.release)
	r := waitEnded(t, c, id)
	if r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeSuccess {
		t.Errorf("the cancel-aware job is %+v", j)
	}
}

// TestCancelKeepsPlacement checks that after a cancel, a placement no
// worker has claimed isn't withdrawn, however often its supervisor looks:
// its control asks its worker to cancel, which it reads as it claims the
// attempt, as a worker queued by a scheduler would.
func TestCancelKeepsPlacement(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"main": never})
	id, _ := create(t, c, oneJob, nil, nil)
	serve(t, c)
	a := latestIs(t, c, id, "main", func(a *store.Attempt) bool { return a.Placed() })
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	// Looked at again and again, as by its supervisor, the job waits.
	for range 3 {
		if wait, err := c.supervise(t.Context(), id, "main", nil); err != nil || wait <= 0 {
			t.Fatalf("supervise = %v, %v", wait, err)
		}
	}
	if got := latestAttempt(t, c, id, "main"); got.ID != a.ID || got.State != store.AttemptPlacing {
		t.Errorf("the attempt is %+v", got)
	}
	if ctl := controlOf(t, c, a.ID); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
		t.Errorf("the placement's worker is told %+v", ctl)
	}
	if w := d.withdrawn(); len(w) != 0 {
		t.Errorf("the backend cancelled %v", w)
	}
}

// timedOut is a run with a timeout: of a second.
const timedOut = `
timeout: 1s
jobs:
  main:
    steps:
      - run: echo main
`

// TestRunTimeout checks a run past its timeout: its watcher stores the
// stop record, with the reason timeout, which tells the running worker so;
// the job fails with the reason timeout, as its worker ends it, and the
// run with it.
func TestRunTimeout(t *testing.T) {
	parallel(t)
	c, _ := openStop(t, map[string]string{"main": obey})
	id, _ := create(t, c, timedOut, nil, nil)
	serve(t, c)
	a := latestIs(t, c, id, "main", running)
	r := waitEnded(t, c, id)
	if r.Outcome != plan.OutcomeFailure || r.Reason != plan.ReasonTimeout || r.StopReason != plan.ReasonTimeout {
		t.Errorf("the run ended %s (%s), stopped for %s", r.Outcome, r.Reason, r.StopReason)
	}
	if ctl := controlOf(t, c, a.ID); !ctl.Cancel || ctl.Reason != plan.ReasonTimeout {
		t.Errorf("the attempt's control is %+v", ctl)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonTimeout {
		t.Errorf("the job is %+v", j)
	}
}

// TestRunTimeoutUnclaimed checks that after the run's timeout, a placement
// no worker has claimed isn't withdrawn: its worker claims it and is told
// at once that the run's timeout has passed, and the job fails, as format
// 1's does when it starts under the passed deadline.
func TestRunTimeoutUnclaimed(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"main": late})
	id, _ := create(t, c, timedOut, nil, nil)
	serve(t, c)
	r := waitEnded(t, c, id)
	if r.Outcome != plan.OutcomeFailure || r.Reason != plan.ReasonTimeout {
		t.Errorf("the run ended %s (%s)", r.Outcome, r.Reason)
	}
	if w := d.withdrawn(); len(w) != 0 {
		t.Errorf("the backend cancelled %v", w)
	}
	a := latestAttempt(t, c, id, "main")
	if a.End.Kind != store.EndCompleted || a.End.Reason != plan.ReasonTimeout {
		t.Errorf("the attempt is %+v", a)
	}
}

// needsWorkflow has a job that runs, and three that need it: one with no
// if:, which a cancel ends at once, skipped because its needs failed; one
// whose if: calls failure(), which waits for its need, and then ends
// cancelled; and one whose if: is always(), which waits, and then runs.
const needsWorkflow = `
jobs:
  a:
    steps:
      - run: echo a
  b:
    needs: [a]
    steps:
      - run: echo b
  c:
    needs: [a]
    if: failure()
    steps:
      - run: echo c
  d:
    needs: [a]
    if: always()
    steps:
      - run: echo d
`

// TestCancelNotReadied checks the three paths of the jobs not yet readied
// when a run is cancelled.
func TestCancelNotReadied(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"a": hold})
	id, _ := create(t, c, needsWorkflow, nil, nil)
	serve(t, c)
	latestIs(t, c, id, "a", running)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	// b ends at once, while a still runs.
	b := storedJob(t, c, id, "b")
	if b.Outcome != plan.OutcomeSkipped || b.Result == nil || !b.Result.NeedsFailed {
		t.Errorf("b, with no if:, is %+v", b)
	}
	if j := storedJob(t, c, id, "a"); j.Outcome != "" {
		t.Fatalf("a has ended: %+v", j)
	}
	for _, key := range []string{"c", "d"} {
		if j := storedJob(t, c, id, key); j.Outcome != "" {
			t.Errorf("%s ended before the job it needs: %+v", key, j)
		}
	}
	close(d.release)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	for key, want := range map[string]plan.Outcome{"a": plan.OutcomeCancelled, "c": plan.OutcomeCancelled, "d": plan.OutcomeSuccess} {
		if j := storedJob(t, c, id, key); j.Outcome != want {
			t.Errorf("%s is %+v, want %s", key, j, want)
		}
	}
}

// TestCancelCreated checks that a run cancelled before it started ends
// cancelled, and its Start leaves it so.
func TestCancelCreated(t *testing.T) {
	parallel(t)
	c, _ := openStop(t, nil)
	id, _ := create(t, c, oneJob, nil, nil)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	r := waitEnded(t, c, id)
	if r.Outcome != plan.OutcomeCancelled || r.StopReason != plan.ReasonCancelled || r.StartState != nil {
		t.Errorf("the run is %s, stopped for %s, started %t", r.Outcome, r.StopReason, r.StartState != nil)
	}
}

// timedJob is a job whose timeout: is a second.
const timedJob = `
jobs:
  main:
    timeout: 1s
    steps:
      - run: echo main
`

// TestJobTimeoutBackstop checks that the coordinator tells the worker of a
// job past its timeout: to stop it, with the reason timeout, once the
// worker's time to stop, a heartbeat period and the margin have passed
// too, as a backstop for a worker that doesn't stop it itself.
func TestJobTimeoutBackstop(t *testing.T) {
	parallel(t)
	c, _ := openStop(t, map[string]string{"main": silent})
	c.timing.WorkerStop, c.timing.Heartbeat, c.timing.FenceMargin = 0, time.Second, 0
	id, _ := create(t, c, timedJob, nil, nil)
	serve(t, c)
	a := latestIs(t, c, id, "main", running)
	if ctl := controlOf(t, c, a.ID); ctl.Cancel {
		t.Fatalf("a job was told to stop as it started: %+v", ctl)
	}
	waitFor(t, func() bool {
		c.wakeJob(t.Context(), id, "main")
		return controlOf(t, c, a.ID).Cancel
	})
	if ctl := controlOf(t, c, a.ID); ctl.Reason != plan.ReasonTimeout {
		t.Errorf("the job past its timeout was told %+v", ctl)
	}
	if r := storedRun(t, c, id); !r.StoppedAt.IsZero() {
		t.Errorf("a job's timeout stopped the run: %+v", r)
	}
}

// TestStopBetweenAttempts checks that a job between attempts as its run
// stops, waiting to be placed again after a failure before its first
// step, isn't placed again: it ends cancelled after a cancel, and failed,
// with the reason timeout, after the run's timeout.
func TestStopBetweenAttempts(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name, workflow string
		outcome        plan.Outcome
		reason         plan.Reason
	}{
		{"cancel", oneJob, plan.OutcomeCancelled, plan.ReasonCancelled},
		{"timeout", timedOut, plan.OutcomeFailure, plan.ReasonTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := open(t, Options{})
			c.supervision.setup = []time.Duration{time.Minute, time.Minute, time.Minute}
			failSetup(c, setupProblems[0])
			id, _ := create(t, c, tc.workflow, nil, nil)
			serve(t, c)
			latestIs(t, c, id, "main", func(a *store.Attempt) bool { return a.End.Reason == plan.ReasonSetupFailed })
			if tc.reason == plan.ReasonCancelled {
				if err := c.Cancel(t.Context(), id); err != nil {
					t.Fatal(err)
				}
			}
			r := waitEnded(t, c, id)
			if r.Outcome != tc.outcome || r.StopReason != tc.reason {
				t.Errorf("the run ended %s, stopped for %s", r.Outcome, r.StopReason)
			}
			if j := storedJob(t, c, id, "main"); j.Outcome != tc.outcome || j.Reason != tc.reason || j.Attempts != 1 {
				t.Errorf("the job is %+v", j)
			}
			if placed := placedJobs(c); len(placed) != 1 {
				t.Errorf("the job was placed %d times", len(placed))
			}
		})
	}
}

// TestStopAfterLost checks that a job whose worker was lost, waiting its
// retry:'s interval to run again as its run is cancelled, isn't placed
// again: it ends cancelled at once, after its one attempt.
func TestStopAfterLost(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, _ := openLease(t, clock, []bool{true, true}, []bool{true, true})
	id, first := startJob(t, c, retryJob, store.AttemptRunning)
	clock.Advance(c.timing.Lease + c.timing.FenceMargin + time.Second)
	if wait := look(t, c, id); wait != time.Minute {
		t.Fatalf("once lost, the supervisor waits %v for the retry:'s interval", wait)
	}
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("after the cancel, the supervisor waits %v", wait)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled || j.Attempts != 1 {
		t.Errorf("the job is %+v", j)
	}
	if a := latestAttempt(t, c, id, "main"); a.ID != first.ID {
		t.Errorf("the job was placed again: %+v", a)
	}
}

// TestStopUnclaimedLost checks that after a cancel, a placement no worker
// has claimed, which isn't withdrawn, is watched as any is: once its backend
// says its worker has ended, the job ends cancelled, and isn't placed
// again.
func TestStopUnclaimedLost(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, nil, nil)
	d.set(func(d *testDriver) {
		d.worker = func(backend.PlaceRequest) testEnd { return testEnd{never: true} }
	})
	id, first := startJob(t, c, oneJob, store.AttemptPlacing)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if wait := look(t, c, id); wait <= 0 || d.checked() == 0 {
		t.Fatalf("with the backend unable to decide, the supervisor waits %v, having asked it %d times", wait, d.checked())
	}
	d.says(backend.Status{State: backend.Dead}, nil)
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("once the backend says the worker has ended, the supervisor waits %v", wait)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled || j.Attempts != 1 {
		t.Errorf("the job is %+v", j)
	}
	if a := latestAttempt(t, c, id, "main"); a.ID != first.ID {
		t.Errorf("the job was placed again: %+v", a)
	}
}

// TestStopLapseGrace checks that after a cancel, the placement of a claimed
// attempt whose lease has lapsed is withdrawn only once the grace in which
// no lapsed lease counts has passed, and without asking its backend: the
// job ends cancelled.
func TestStopLapseGrace(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, _ := startJob(t, c, oneJob, store.AttemptRunning)
	d.says(backend.Status{State: backend.Dead}, nil)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	clock.Advance(c.timing.Lease - 10*time.Second)
	h, err := c.store.HoldEndpoint(t.Context(), "socket", "a test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Release(context.WithoutCancel(t.Context())) })
	// The lease lapsed 10 seconds ago, within the grace.
	clock.Advance(20 * time.Second)
	if wait := look(t, c, id); wait <= 0 {
		t.Fatalf("within the grace, the supervisor waits %v", wait)
	}
	if a := latestAttempt(t, c, id, "main"); a.State != store.AttemptRunning {
		t.Fatalf("within the grace, the attempt is %s", a.State)
	}
	clock.Advance(c.timing.Grace)
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("after the grace, the supervisor waits %v", wait)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled {
		t.Errorf("the job is %+v", j)
	}
	// The testDriver's lock, not the leaseDriver's, holds its cancels.
	d.testDriver.mu.Lock()
	withdrawn := len(d.cancelled)
	d.testDriver.mu.Unlock()
	if withdrawn != 1 || d.checked() != 0 {
		t.Errorf("the placement was withdrawn %d times, and the backend asked %d times", withdrawn, d.checked())
	}
}

// TestStopWhilePlacing checks that a run stopped after its job's supervisor
// looked, and before it made the job's attempt, has no attempt made: the
// supervisor looks again, and ends the job, which doesn't outlive the stop.
func TestStopWhilePlacing(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	id, _ := create(t, c, oneJob, nil, nil)
	var once sync.Once
	driverOf(c).set(func(d *testDriver) {
		d.probe = func(*backend.Member) (backend.Fit, error) {
			// The look began before the stop, and the attempt would be
			// made after it.
			once.Do(func() {
				if err := c.Cancel(context.WithoutCancel(t.Context()), id); err != nil {
					t.Error(err)
				}
			})
			return backend.Fit{Availability: backend.StartsNow}, nil
		}
	})
	serve(t, c)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Attempts != 0 {
		t.Errorf("the job is %+v", j)
	}
	if placed := placedJobs(c); len(placed) != 0 {
		t.Errorf("the job was placed: %v", placed)
	}
}

// TestCancelIfAlways checks that a job readied before a cancel, whose if:
// was read once its needs had ended, and evaluated to the word always,
// runs on through the cancel: its placement isn't withdrawn, and its
// worker runs it.
func TestCancelIfAlways(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	release := make(chan struct{})
	byJob(c, map[string]testEnd{
		"a": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"mode": "always"}}},
		"b": {result: plan.JobResult{Outcome: plan.OutcomeSuccess}, wait: release},
	})
	id, _ := create(t, c, `
jobs:
  a:
    steps:
      - run: echo mode=always >> $OUTPUTS
  b:
    needs: [a]
    if: ${{ needs.a.outputs.mode }}
    steps:
      - run: echo b
`, nil, nil)
	serve(t, c)
	latestIs(t, c, id, "b", func(a *store.Attempt) bool { return a.Placement.ID != "" })
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	c.wakeJob(t.Context(), id, "b")
	close(release)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	if j := storedJob(t, c, id, "b"); j.Outcome != plan.OutcomeSuccess || j.Attempts != 1 {
		t.Errorf("b, whose if: is always once a has ended, is %+v", j)
	}
	if a := latestAttempt(t, c, id, "b"); a.End.Kind != store.EndCompleted {
		t.Errorf("b's attempt is %+v", a)
	}
}

// TestStopTwice checks that the first stop of a run is its record: a
// cancel after the run's timeout, or the run's timeout after a cancel,
// changes nothing, neither the run's outcome nor what its worker is told.
func TestStopTwice(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name          string
		first, second plan.Reason
		outcome       plan.Outcome
	}{
		{"cancel first", plan.ReasonCancelled, plan.ReasonTimeout, plan.OutcomeCancelled},
		{"timeout first", plan.ReasonTimeout, plan.ReasonCancelled, plan.OutcomeFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, d := openStop(t, map[string]string{"main": hold})
			id, _ := create(t, c, oneJob, nil, nil)
			serve(t, c)
			a := latestIs(t, c, id, "main", running)
			for _, reason := range []plan.Reason{tc.first, tc.second} {
				if err := c.stop(t.Context(), id, reason); err != nil {
					t.Fatal(err)
				}
			}
			if ctl := controlOf(t, c, a.ID); !ctl.Cancel || ctl.Reason != tc.first {
				t.Errorf("the attempt's control is %+v", ctl)
			}
			close(d.release)
			r := waitEnded(t, c, id)
			if r.StopReason != tc.first || r.Outcome != tc.outcome {
				t.Errorf("the run ended %s, stopped for %s", r.Outcome, r.StopReason)
			}
		})
	}
}

// TestCancelPlacing checks a cancel as the job's placement is made, once
// its attempt is: the attempt is told, its placement is kept once it is
// recorded, and its worker, which claims it once the run is stopped, ends
// the job cancelled.
func TestCancelPlacing(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"main": late})
	id, _ := create(t, c, oneJob, nil, nil)
	var once sync.Once
	d.set(func(td *testDriver) {
		td.place = func(backend.PlaceRequest) error {
			once.Do(func() {
				if err := c.Cancel(context.WithoutCancel(t.Context()), id); err != nil {
					t.Error(err)
				}
			})
			return nil
		}
	})
	serve(t, c)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	a := latestAttempt(t, c, id, "main")
	if a.End.Kind != store.EndCompleted || a.Number != 1 {
		t.Errorf("the attempt is %+v", a)
	}
	if w := d.withdrawn(); len(w) != 0 {
		t.Errorf("the backend cancelled %v", w)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled {
		t.Errorf("the job is %+v", j)
	}
}

// TestCancelClaiming checks a cancel as a worker claims its attempt:
// whether the claim or the withdrawal comes first, the job ends
// cancelled, and so does the run.
func TestCancelClaiming(t *testing.T) {
	parallel(t)
	c, _ := openStop(t, map[string]string{"main": late})
	id, _ := create(t, c, oneJob, nil, nil)
	serve(t, c)
	latestIs(t, c, id, "main", func(a *store.Attempt) bool { return a.Placement.ID != "" })
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled {
		t.Errorf("the job is %+v", j)
	}
}

// TestCancelFollowed checks that following a run that is cancelled reports
// its stop, with the reason, before its end.
func TestCancelFollowed(t *testing.T) {
	parallel(t)
	c, _ := openStop(t, nil)
	id, _ := create(t, c, oneJob, nil, nil)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	var events []Event
	status, err := c.Follow(t.Context(), id, FollowOptions{Event: func(e Event) error {
		events = append(events, e)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != RunStopping || events[0].Reason != plan.ReasonCancelled || events[1].Kind != RunFinished ||
		status.Outcome != plan.OutcomeCancelled {
		t.Errorf("following the run gave %+v, and %+v", events, status)
	}
}

// TestCancelMatrix checks a cancel of a run whose matrix job runs its
// instances one at a time: the instance running is told to stop, and the
// one waiting for its turn ends at once, cancelled, never placed, as
// format 1 ends one; and the matrix job ends cancelled once they have.
func TestCancelMatrix(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"main#0": hold})
	id, _ := create(t, c, `
jobs:
  main:
    strategy:
      max-parallel: 1
      matrix:
        n: [0, 1]
    steps:
      - run: echo ${{ matrix.n }}
`, nil, nil)
	serve(t, c)
	held := latestIs(t, c, id, "main#0", running)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return storedJob(t, c, id, "main#1").Outcome != "" })
	if j := storedJob(t, c, id, "main#1"); j.Outcome != plan.OutcomeCancelled || j.Reason != plan.ReasonCancelled || j.Attempts != 0 {
		t.Errorf("the instance waiting for its turn is %+v", j)
	}
	if ctl := controlOf(t, c, held.ID); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
		t.Errorf("the running instance's worker is told %+v", ctl)
	}
	close(d.release)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	for _, key := range []string{"main", "main#0"} {
		if j := storedJob(t, c, id, key); j.Outcome != plan.OutcomeCancelled {
			t.Errorf("%s is %+v", key, j)
		}
	}
}

// TestCancelInstanceAlways checks that an instance whose if: is the word
// always with its own matrix values outlives a cancel, while its sibling,
// whose if: isn't, is told to stop: what outlives a stop is decided with
// each instance's values.
func TestCancelInstanceAlways(t *testing.T) {
	parallel(t)
	c, d := openStop(t, map[string]string{"main#0": hold, "main#1": outlive})
	id, _ := create(t, c, `
jobs:
  main:
    if: ${{ matrix.n == 1 && 'always' || 'true' }}
    strategy:
      matrix:
        n: [0, 1]
    steps:
      - run: echo ${{ matrix.n }}
`, nil, nil)
	serve(t, c)
	stopped := latestIs(t, c, id, "main#0", running)
	outlived := latestIs(t, c, id, "main#1", running)
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if ctl := controlOf(t, c, stopped.ID); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
		t.Errorf("the instance that doesn't outlive the cancel is told %+v", ctl)
	}
	if ctl := controlOf(t, c, outlived.ID); ctl.Cancel {
		t.Errorf("the instance that outlives the cancel is told %+v", ctl)
	}
	close(d.release)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeCancelled {
		t.Errorf("the run ended %s", r.Outcome)
	}
	for key, want := range map[string]plan.Outcome{"main#0": plan.OutcomeCancelled, "main#1": plan.OutcomeSuccess} {
		if j := storedJob(t, c, id, key); j.Outcome != want {
			t.Errorf("%s is %+v, want %s", key, j, want)
		}
	}
}
