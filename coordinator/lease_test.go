package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A fakeClock is the store's clock in a test, which moves only as the test
// moves it.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Now().UTC().Truncate(time.Second)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func (f *fakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = t
}

// A leaseDriver is a testDriver whose Check says what the test says, and
// whose workers, as the test says, claim their attempts and then say
// nothing more, as workers cut off from the coordinator do.
type leaseDriver struct {
	*testDriver

	mu     sync.Mutex
	status backend.Status
	err    error
	checks int
	// asked are the placements Check was asked about.
	asked []backend.Placement
	// silent says, for each attempt by its number, whether its worker
	// claims it and goes silent, and started whether it then starts a
	// step; an attempt beyond them completes with success, or with failure
	// for a job fails names.
	silent, started []bool
	fails           map[string]bool
	placed          int
	// credentials are those of the silent workers, by attempt.
	credentials map[string]string
}

func (d *leaseDriver) Check(_ context.Context, _ *backend.Member, p backend.Placement) (backend.Status, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.checks++
	d.asked = append(d.asked, p)
	return d.status, d.err
}

// checked returns how many times Check was called.
func (d *leaseDriver) checked() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.checks
}

// says sets what Check says.
func (d *leaseDriver) says(st backend.Status, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.status, d.err = st, err
}

// worker is what the worker of an attempt d places does.
func (d *leaseDriver) worker(req backend.PlaceRequest) testEnd {
	d.mu.Lock()
	n := d.placed
	d.placed++
	silent := n < len(d.silent) && d.silent[n]
	started := n < len(d.started) && d.started[n]
	fails := d.fails[jobOf(req)]
	d.mu.Unlock()
	switch {
	case !silent && fails:
		return testEnd{result: plan.JobResult{Outcome: plan.OutcomeFailure}}
	case !silent:
		return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	}
	d.workers.Go(func() { d.claim(req, started) })
	return testEnd{never: true}
}

// claim claims the attempt req is for, once its placement is recorded, and
// starts a step of it if started is set; then its worker says nothing more.
func (d *leaseDriver) claim(req backend.PlaceRequest, started bool) {
	ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
	defer cancel()
	for {
		var a *store.Attempt
		if err := d.c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			a, err = tx.Attempt(req.Attempt)
			return err
		}); err != nil {
			return
		}
		if a.Placed() {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	credential := hex.EncodeToString(b)
	err := d.c.store.Tx(ctx, func(tx *store.Tx) error {
		a, err := tx.Claim(req.BootstrapToken, credential, workerapi.Worker{Version: "test"}, workerapi.Version)
		if err != nil || !started {
			return err
		}
		return tx.StepStarted(a.ID)
	})
	if err != nil {
		return
	}
	d.mu.Lock()
	d.credentials[req.Attempt] = credential
	d.mu.Unlock()
}

// openLease opens a coordinator whose store's clock is clock, with a
// leaseDriver whose workers do as silent and started say.
func openLease(t *testing.T, clock *fakeClock, silent, started []bool) (*Coordinator, *leaseDriver) {
	t.Helper()
	return openLeaseWith(t, Options{now: clock.Now}, silent, started)
}

// openLeaseWith opens a coordinator with opts, and a leaseDriver whose
// workers do as silent and started say.
func openLeaseWith(t *testing.T, opts Options, silent, started []bool) (*Coordinator, *leaseDriver) {
	t.Helper()
	d := &leaseDriver{testDriver: newTestDriver(), silent: silent, started: started, credentials: map[string]string{}}
	// Its workers, silent or not, run with the test's context, as open
	// gives a testDriver's.
	d.ctx = context.WithoutCancel(t.Context())
	opts.Drivers = map[string]backend.Driver{testBackend: d, "ssh": d}
	c := open(t, opts)
	d.c = c
	d.testDriver.worker = d.worker
	t.Cleanup(d.workers.Wait)
	return c, d
}

// startJob creates a run of workflow, has c serve it until the latest
// attempt of its job main is as want, its placement recorded, and stops
// serving it: from then on, the test's looks (look) are its supervisor's
// only ones.
func startJob(t *testing.T, c *Coordinator, workflow string, want store.AttemptState) (string, *store.Attempt) {
	t.Helper()
	return startJobWith(t, c, workflow, nil, want)
}

// startJobWith is startJob, for a workflow that reads vars.
func startJobWith(t *testing.T, c *Coordinator, workflow string, vars conformancetest.Variables, want store.AttemptState) (string, *store.Attempt) {
	t.Helper()
	id, _ := create(t, c, workflow, nil, vars)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.Serve(ctx) }()
	var a *store.Attempt
	waitFor(t, func() bool {
		err := c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			a, err = tx.LatestAttempt(id, "main")
			return err
		})
		return err == nil && a.State == want && a.Placed()
	})
	cancel()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	return id, a
}

// look has the supervisor of the run id's job main look at it once, as
// hopper would run it, and returns how long it would wait then; 0 once it
// has ended the job.
func look(t *testing.T, c *Coordinator, id string) time.Duration {
	t.Helper()
	wait, err := c.supervise(t.Context(), id, "main", func(context.Context, *store.Tx) error { return nil })
	if err != nil && !errors.Is(err, errSettled) {
		t.Fatal(err)
	}
	return wait
}

// jobCodes returns the codes of the job's diagnostics.
func jobCodes(j *store.Job) []string {
	var codes []string
	for _, d := range j.Diagnostics {
		codes = append(codes, string(d.Code))
	}
	return codes
}

// TestWorkerLost checks that a worker whose lease lapsed is lost only once
// its backend says it has ended, and that a job whose retry: doesn't list
// worker-lost then fails, with the reason worker-lost.
func TestWorkerLost(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, a := startJob(t, c, oneJob, store.AttemptRunning)
	if a.SelfFencing {
		t.Fatal("a job without retry: fences itself")
	}
	d.says(backend.Status{State: backend.Dead}, nil)
	// Before the lease lapses, nothing asks the backend.
	clock.Advance(c.timing.Lease - time.Second)
	if wait := look(t, c, id); wait <= 0 || d.checked() != 0 {
		t.Fatalf("before the lease lapsed, the supervisor waits %v, and asked the backend %d times", wait, d.checked())
	}
	clock.Advance(2 * time.Second)
	if wait := look(t, c, id); wait != 0 || d.checked() != 1 {
		t.Fatalf("once the lease lapsed, the supervisor waits %v, having asked the backend %d times", wait, d.checked())
	}
	j := storedJob(t, c, id, "main")
	if j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonWorkerLost || !slices.Equal(jobCodes(j), []string{"worker_lost"}) {
		t.Errorf("the job is %+v", j)
	}
	if a := latestAttempt(t, c, id, "main"); a.End.Kind != store.EndLost || a.End.Reason != plan.ReasonWorkerLost {
		t.Errorf("the attempt ended %+v", a.End)
	}
}

// TestCheckUnknown checks that a worker whose backend can't decide whether
// it runs, or says it runs, is never lost: it is asked again every
// heartbeat period, however long ago the lease lapsed.
func TestCheckUnknown(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, _ := startJob(t, c, oneJob, store.AttemptRunning)
	clock.Advance(c.timing.Lease + time.Second)
	for i, st := range []backend.Status{{State: backend.Unknown}, {State: backend.Running}, {State: backend.Queued}, {State: backend.Unknown}} {
		var err error
		if i == 3 {
			err = errors.New("the connection to the member is down")
		}
		d.says(st, err)
		if wait := look(t, c, id); wait != c.timing.Heartbeat {
			t.Errorf("with the backend saying %+v, %v, the supervisor waits %v", st, err, wait)
		}
		clock.Advance(time.Hour)
	}
	if n := d.checked(); n != 4 {
		t.Errorf("the backend was asked %d times", n)
	}
	if a := latestAttempt(t, c, id, "main"); a.State != store.AttemptRunning {
		t.Errorf("the attempt is %s", a.State)
	}
}

// TestCheckProblem checks that a worker whose backend's check gives a
// problem, a cause that won't pass by itself, counts as running for
// CheckGrace from the first such check since the backend last decided,
// and is then lost, with the problem: a check that decides meanwhile
// starts the grace again, and one that can't decide for a cause that may
// pass, as a connection that is down, neither ends it nor starts it again.
func TestCheckProblem(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, _ := startJob(t, c, oneJob, store.AttemptRunning)
	clock.Advance(c.timing.Lease + time.Second)
	elsewhere := problems.SSHPlacementElsewhere.Problem(problems.Params{"host": "farm"})
	running := func(what string) {
		t.Helper()
		if wait := look(t, c, id); wait != c.timing.Heartbeat {
			t.Fatalf("%s, the supervisor waits %v", what, wait)
		}
	}
	d.says(backend.Status{State: backend.Unknown}, elsewhere)
	running("at the first check that gives a problem")
	clock.Advance(c.timing.CheckGrace - time.Second)
	running("within the grace")
	d.says(backend.Status{State: backend.Running}, nil)
	running("once the backend decides")
	d.says(backend.Status{State: backend.Unknown}, elsewhere)
	running("at the first check that gives a problem since it decided")
	clock.Advance(c.timing.CheckGrace - time.Second)
	running("within the grace it started again")
	d.says(backend.Status{State: backend.Unknown}, errors.New("the connection to the member is down"))
	clock.Advance(time.Hour)
	running("over a connection that is down")
	d.says(backend.Status{State: backend.Unknown}, elsewhere)
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("past the grace, the supervisor waits %v", wait)
	}
	a := latestAttempt(t, c, id, "main")
	if a.End.Kind != store.EndLost || a.End.Reason != plan.ReasonWorkerLost || a.End.Problem == nil || a.End.Problem.Code != problems.SSHPlacementElsewhere.Code() {
		t.Errorf("the attempt ended %+v", a.End)
	}
}

// retryJob runs again once its worker is lost after a step starts.
const retryJob = `
jobs:
  main:
    retry:
      reasons: [worker-lost]
      max-retries: 1
      interval: 1m
    steps:
      - run: echo main
`

// TestSelfFenceRetry checks a job whose retry: lists worker-lost: its
// attempt fences itself, and isn't lost before its lease and the fence
// margin have passed since its last renewal, whatever its backend says,
// which isn't asked; then it waits its retry:'s interval, and its next
// attempt runs, counting the lost one, whose writes are refused.
func TestSelfFenceRetry(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, first := startJob(t, c, retryJob, store.AttemptRunning)
	if !first.SelfFencing {
		t.Fatal("a job whose retry: lists worker-lost doesn't fence itself")
	}
	d.says(backend.Status{State: backend.Dead}, nil)
	clock.Advance(c.timing.Lease + c.timing.FenceMargin - time.Second)
	if wait := look(t, c, id); wait <= 0 {
		t.Fatalf("the supervisor waits %v", wait)
	}
	if a := latestAttempt(t, c, id, "main"); a.ID != first.ID || a.State != store.AttemptRunning {
		t.Fatalf("before the lease and the margin had passed, the attempt is %+v", a)
	}
	clock.Advance(2 * time.Second)
	// Lost, it waits the retry:'s interval.
	if wait := look(t, c, id); wait != time.Minute {
		t.Errorf("once lost, the supervisor waits %v for the retry:'s interval", wait)
	}
	if d.checked() != 0 {
		t.Errorf("the backend of an attempt that fences itself was asked %d times", d.checked())
	}
	lost := latestAttempt(t, c, id, "main")
	if lost.ID != first.ID || lost.End.Kind != store.EndLost || lost.End.Reason != plan.ReasonWorkerLost {
		t.Fatalf("the first attempt is %+v", lost)
	}
	// Its worker's writes are refused.
	d.mu.Lock()
	credential := d.credentials[first.ID]
	d.mu.Unlock()
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { _, err := tx.Renew(credential); return err }); !errors.Is(err, store.ErrFenced) {
		t.Errorf("the lost attempt's heartbeat = %v", err)
	}
	clock.Advance(time.Minute)
	look(t, c, id)
	second := latestAttempt(t, c, id, "main")
	if second.ID == first.ID || second.Number != 2 || !second.SelfFencing {
		t.Fatalf("the job's next attempt is %+v", second)
	}
	// Its claim says one attempt before it started a step, as
	// PW_WORKFLOW_JOB_CURRENT_RETRY does.
	var retry int
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		retry, err = tx.StartedBefore(second)
		return err
	}); err != nil || retry != 1 {
		t.Errorf("the next attempt's claim gives the retry %d, %v", retry, err)
	}
	waitFor(t, func() bool { return latestAttempt(t, c, id, "main").State == store.AttemptEnded })
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("once the next attempt ended, the supervisor waits %v", wait)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeSuccess || j.Attempts != 2 || j.AttemptsStarted != 2 {
		t.Errorf("the job is %+v", j)
	}
}

// TestRetriesSpent checks that a job whose worker is lost once more than
// its retry: allows fails, with the reason worker-lost.
func TestRetriesSpent(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, _ := openLease(t, clock, []bool{true, true}, []bool{true, true})
	id, _ := startJob(t, c, retryJob, store.AttemptRunning)
	clock.Advance(c.timing.Lease + c.timing.FenceMargin + time.Second)
	look(t, c, id)
	clock.Advance(time.Minute)
	look(t, c, id)
	waitFor(t, func() bool { return latestAttempt(t, c, id, "main").State == store.AttemptRunning })
	clock.Advance(c.timing.Lease + c.timing.FenceMargin + time.Second)
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("the supervisor waits %v", wait)
	}
	j := storedJob(t, c, id, "main")
	if j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonWorkerLost || !slices.Equal(jobCodes(j), []string{"worker_lost_again"}) {
		t.Errorf("the job is %+v", j)
	}
	if got := j.Diagnostics[0].Params["retries"]; got != 1 && got != int64(1) && got != float64(1) {
		t.Errorf("the job's diagnostic says %v retries", got)
	}
}

// TestRestartGrace checks that once a coordinator takes the endpoint
// workers reach it through, or its host wakes, no lapsed lease counts until
// the grace has passed: a worker cut off for less than that isn't lost.
func TestRestartGrace(t *testing.T) {
	parallel(t)
	for _, how := range []string{"endpoint", "wake"} {
		t.Run(how, func(t *testing.T) {
			clock := newFakeClock()
			c, d := openLease(t, clock, []bool{true}, []bool{true})
			id, _ := startJob(t, c, oneJob, store.AttemptRunning)
			d.says(backend.Status{State: backend.Dead}, nil)
			clock.Advance(50 * time.Second)
			switch how {
			case "endpoint":
				h, err := c.store.HoldEndpoint(t.Context(), "socket", "a test")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = h.Release(context.WithoutCancel(t.Context())) })
			case "wake":
				// Another coordinator of the store, whose host woke.
				other := open(t, Options{Database: c.opts.Database, Keys: c.opts.Keys, now: clock.Now})
				other.woke(t.Context())
			}
			// The lease lapsed 10 seconds ago, within the grace.
			clock.Advance(20 * time.Second)
			if wait := look(t, c, id); wait <= 0 || d.checked() != 0 {
				t.Fatalf("within the grace, the supervisor waits %v, having asked the backend %d times", wait, d.checked())
			}
			if a := latestAttempt(t, c, id, "main"); a.State != store.AttemptRunning {
				t.Fatalf("within the grace, the attempt is %s", a.State)
			}
			clock.Advance(c.timing.Grace)
			if wait := look(t, c, id); wait != 0 || d.checked() != 1 {
				t.Fatalf("after the grace, the supervisor waits %v, having asked the backend %d times", wait, d.checked())
			}
		})
	}
}

// TestClockReset checks that a lease renewed while the store's clock read
// a year ahead, once the host's clock is right again, lapses a lease
// period after the reset, with the grace, not a year after: the store
// takes the reset, and brings the renewal back to it.
func TestClockReset(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	right := clock.Now()
	clock.Advance(365 * 24 * time.Hour)
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, _ := startJob(t, c, oneJob, store.AttemptRunning)
	d.says(backend.Status{State: backend.Dead}, nil)
	supervisor := storedJob(t, c, id, "main").SupervisorID
	scheduled := func() time.Time {
		t.Helper()
		row, err := c.inserter.JobGet(t.Context(), supervisor)
		if err != nil {
			t.Fatal(err)
		}
		return row.ScheduledAt
	}
	if at := scheduled(); !at.After(time.Now()) {
		t.Fatalf("the supervisor that waits is scheduled at %v", at)
	}
	clock.Set(right)
	look(t, c, id)
	// The reset woke the supervisor, which hopper had scheduled later.
	if at := scheduled(); at.After(time.Now()) {
		t.Errorf("after the reset, the supervisor is scheduled at %v", at)
	}
	clock.Advance(30 * time.Second)
	if wait := look(t, c, id); wait <= 0 || d.checked() != 0 {
		t.Fatalf("within the reset's grace, the supervisor waits %v, having asked the backend %d times", wait, d.checked())
	}
	clock.Advance(c.timing.Grace)
	if wait := look(t, c, id); wait != 0 || d.checked() != 1 {
		t.Fatalf("once the grace and the lease had passed since the reset, the supervisor waits %v, having asked the backend %d times", wait, d.checked())
	}
	if j := storedJob(t, c, id, "main"); j.Reason != plan.ReasonWorkerLost {
		t.Errorf("the job is %+v", j)
	}
}

// TestMemberGone checks that an attempt whose member its group no longer
// has, whose backend can't be asked, is lost once the grace has passed
// since its lease lapsed: a failure before its first step, which places
// the job again on a member the group has.
func TestMemberGone(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	group := &backend.Group{Name: "local", Members: []backend.Member{{Name: "m1", Backend: testBackend}, {Name: "m2", Backend: testBackend}}}
	c, d := openLeaseWith(t, Options{now: clock.Now, Groups: groupsOf{"local": group}}, []bool{true}, []bool{false})
	id, first := startJob(t, c, oneJob, store.AttemptClaimed)
	group.Members = group.Members[1:]
	c.mu.Lock()
	clear(c.members)
	c.mu.Unlock()
	clock.Advance(c.timing.Lease + time.Second)
	if wait := look(t, c, id); wait <= 0 || d.checked() != 0 {
		t.Fatalf("before the grace had passed, the supervisor waits %v, having asked a backend %d times", wait, d.checked())
	}
	clock.Advance(c.timing.Grace)
	look(t, c, id)
	if lost := setupFailures(t, c, id, "main"); len(lost) != 1 || lost[0].ID != first.ID || lost[0].End.Problem == nil || lost[0].End.Problem.Code != problems.MemberGone.Code() {
		t.Fatalf("the setup failures are %+v", lost)
	}
	// Placed again once the wait after a setup failure has passed.
	clock.Advance(time.Second)
	look(t, c, id)
	waitFor(t, func() bool { return latestAttempt(t, c, id, "main").State == store.AttemptEnded })
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("the supervisor waits %v", wait)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeSuccess {
		t.Errorf("the job placed again is %+v", j)
	}
	if _, members := d.requests(); len(members) != 2 || members[1] != "m2" {
		t.Errorf("the job was placed on %v", members)
	}
}

// TestLostBeforeStep checks that a worker that ended before the job's
// first step, claimed or not, fails the attempt before its first step,
// which places the job again; and that one its scheduler ended at its time
// limit fails the job with the reason timeout.
func TestLostBeforeStep(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name    string
		claimed bool
		end     backend.End
	}{{"claimed", true, ""}, {"never claimed", false, ""}, {"timed out", true, backend.EndTimedOut}} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			c, d := openLease(t, clock, []bool{true}, []bool{false})
			want := store.AttemptClaimed
			if !tc.claimed {
				// Its first worker never claims it; the next completes it.
				first := true
				d.set(func(d *testDriver) {
					d.worker = func(backend.PlaceRequest) testEnd {
						end := testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}, never: first}
						first = false
						return end
					}
				})
				want = store.AttemptPlacing
			}
			id, first := startJob(t, c, oneJob, want)
			d.says(backend.Status{State: backend.Dead, End: tc.end}, nil)
			clock.Advance(c.timing.Lease + time.Second)
			wait := look(t, c, id)
			a := latestAttempt(t, c, id, "main")
			if tc.end == backend.EndTimedOut {
				j := storedJob(t, c, id, "main")
				if wait != 0 || j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonTimeout || !slices.Equal(jobCodes(j), []string{"worker_killed"}) {
					t.Errorf("the job whose worker timed out is %+v", j)
				}
				return
			}
			if a.ID != first.ID || a.End.Kind != store.EndSetupFailed || a.End.Problem == nil || a.End.Problem.Code != problems.WorkerEndedEarly.Code() {
				t.Fatalf("the attempt is %+v", a)
			}
			if wait <= 0 {
				t.Fatalf("after a setup failure, the supervisor waits %v", wait)
			}
			clock.Advance(time.Second)
			look(t, c, id)
			waitFor(t, func() bool {
				a := latestAttempt(t, c, id, "main")
				return a.ID != first.ID && a.State == store.AttemptEnded
			})
			if wait := look(t, c, id); wait != 0 {
				t.Fatalf("the supervisor waits %v", wait)
			}
			if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeSuccess || j.Attempts != 2 {
				t.Errorf("the job placed again is %+v", j)
			}
		})
	}
}

// TestRenewedMeanwhile checks that an attempt whose worker renewed its
// lease, or claimed it, after the supervisor looked at it isn't lost on
// what it saw.
func TestRenewedMeanwhile(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true}, []bool{true})
	id, a := startJob(t, c, oneJob, store.AttemptRunning)
	d.mu.Lock()
	credential := d.credentials[a.ID]
	d.mu.Unlock()
	clock.Advance(c.timing.Lease + time.Second)
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { _, err := tx.Renew(credential); return err }); err != nil {
		t.Fatal(err)
	}
	if err := c.lose(t.Context(), a, backend.Status{State: backend.Dead}, nil); !errors.Is(err, errAgain) {
		t.Fatal(err)
	}
	if now := latestAttempt(t, c, id, "main"); now.State != store.AttemptRunning {
		t.Errorf("an attempt renewed after the supervisor looked was lost: %+v", now)
	}
}

func TestSlept(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		wall, mono time.Duration
		want       bool
	}{
		{time.Second, time.Second, false},
		{11 * time.Second, time.Second, false},
		{12 * time.Second, time.Second, true},
		{time.Hour, time.Second, true},
		// A wall clock set back is no sleep.
		{-time.Hour, time.Second, false},
	} {
		if got := slept(tc.wall, tc.mono, 10*time.Second); got != tc.want {
			t.Errorf("slept(%v on the wall clock, %v on the monotonic clock) = %t", tc.wall, tc.mono, got)
		}
	}
}

// retriedBeside is retryJob, beside a job whose step's early-cancel: is
// any-job-failed.
const retriedBeside = `
jobs:
  main:
    retry:
      reasons: [worker-lost]
      max-retries: 1
      interval: 1m
    steps:
      - run: echo main
  beside:
    steps:
      - run: sleep 60
        early-cancel: any-job-failed
`

// TestLostNotMarked checks that an attempt lost of a job whose retry:
// runs it again doesn't mark the run as one a job of which failed: the
// attempt of the job beside it isn't told, and its step whose
// early-cancel: is any-job-failed runs on. The job's next attempt, its
// last, which fails, marks it, as the job fails.
func TestLostNotMarked(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLease(t, clock, []bool{true, true}, []bool{true, true})
	d.fails = map[string]bool{"main": true}
	id, _ := create(t, c, retriedBeside, nil, nil)
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.Serve(ctx) }()
	var beside, main *store.Attempt
	waitFor(t, func() bool {
		err := c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			if beside, err = tx.LatestAttempt(id, "beside"); err != nil {
				return err
			}
			main, err = tx.LatestAttempt(id, "main")
			return err
		})
		return err == nil && beside.State == store.AttemptRunning && main.State == store.AttemptRunning
	})
	cancel()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	marked := func() bool {
		t.Helper()
		return !storedRun(t, c, id).AnyJobFailedAt.IsZero()
	}

	clock.Advance(c.timing.Lease + c.timing.FenceMargin + time.Second)
	if wait := look(t, c, id); wait != time.Minute {
		t.Fatalf("once main's attempt was lost, its supervisor waits %v", wait)
	}
	if lost := latestAttempt(t, c, id, "main"); lost.End.Kind != store.EndLost {
		t.Fatalf("main's attempt is %+v", lost)
	}
	if marked() || toldFailed(t, c, beside.ID) {
		t.Fatal("an attempt lost of a job its retry: runs again marked the run")
	}
	// What the store asks of an end, as the worker API stores it.
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		if !tx.RetriesFor(main.ID, plan.ReasonWorkerLost) || tx.RetriesFor(main.ID, plan.ReasonTimeout) || tx.RetriesFor(beside.ID, plan.ReasonWorkerLost) {
			t.Error("the store's RetriesFor doesn't go by the jobs' retry:")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Minute)
	look(t, c, id)
	waitFor(t, func() bool {
		a := latestAttempt(t, c, id, "main")
		return a.Number == 2 && a.State == store.AttemptEnded
	})
	look(t, c, id)
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeFailure {
		t.Fatalf("main is %+v", j)
	}
	if !marked() || !toldFailed(t, c, beside.ID) {
		t.Error("main's last attempt, which failed, didn't mark the run")
	}
}

// toldFailed reports whether the attempt id's control tells its worker
// that a job of its run failed.
func toldFailed(t *testing.T, c *Coordinator, id string) bool {
	t.Helper()
	var ctl workerapi.Control
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		ctl, err = tx.Control(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return ctl.AnyJobFailed
}

// TestPrivateLost checks that the backend of a private attempt, whose
// place the store holds only sealed, is asked about the placement by the
// backend's ID for it that the sealed place holds, once its placement is
// recorded, and that the attempt is lost as any other is.
func TestPrivateLost(t *testing.T) {
	parallel(t)
	clock := newFakeClock()
	c, d := openLeaseWith(t, Options{now: clock.Now, Variables: hostVars}, []bool{true}, []bool{true})
	id, a := startJobWith(t, c, privateTarget, hostVars, store.AttemptRunning)
	if !a.Private || a.Placement.ID != "" {
		t.Fatalf("the private attempt is placed as %+v", a.Placement)
	}
	d.says(backend.Status{State: backend.Dead}, nil)
	clock.Advance(c.timing.Lease + time.Second)
	if wait := look(t, c, id); wait != 0 {
		t.Fatalf("the supervisor waits %v", wait)
	}
	d.mu.Lock()
	asked := slices.Clone(d.asked)
	d.mu.Unlock()
	if len(asked) != 1 || asked[0].Attempt != a.ID || asked[0].ID != "test-"+a.ID || asked[0].WorkerDigest != a.Placement.WorkerDigest {
		t.Errorf("the backend was asked about %+v", asked)
	}
	if j := storedJob(t, c, id, "main"); j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonWorkerLost {
		t.Errorf("the job is %+v", j)
	}
}

// TestRetriesAgain checks when a job's retry: runs it again: for a
// reason it lists, fewer times than its max-retries, counting the
// attempts that started a step but the first.
func TestRetriesAgain(t *testing.T) {
	parallel(t)
	r := &plan.ReadyJobRetry{Reasons: []plan.Reason{plan.ReasonWorkerLost}, MaxRetries: 2}
	for _, tc := range []struct {
		r       *plan.ReadyJobRetry
		reason  plan.Reason
		started int
		again   bool
		retried int
	}{
		{r, plan.ReasonWorkerLost, 1, true, 0},
		{r, plan.ReasonWorkerLost, 2, true, 1},
		{r, plan.ReasonWorkerLost, 3, false, 2},
		{r, plan.ReasonTimeout, 1, false, 0},
		{nil, plan.ReasonWorkerLost, 1, false, 0},
		{r, plan.ReasonWorkerLost, 0, true, 0},
	} {
		if again, retried := retriesAgain(tc.r, tc.reason, tc.started); again != tc.again || retried != tc.retried {
			t.Errorf("retriesAgain(%+v, %s, %d) = %t, %d; want %t, %d", tc.r, tc.reason, tc.started, again, retried, tc.again, tc.retried)
		}
	}
}

func TestRetryWait(t *testing.T) {
	parallel(t)
	r := &plan.ReadyJobRetry{IntervalSeconds: 60}
	for n, want := range map[int]time.Duration{1: time.Minute, 2: 2 * time.Minute, 3: 4 * time.Minute, 7: time.Hour, 20: time.Hour} {
		if got := retryWait(r, n); got != want {
			t.Errorf("retry %d waits %v, want %v", n, got, want)
		}
	}
	if got := retryWait(&plan.ReadyJobRetry{IntervalSeconds: 7200}, 3); got != 2*time.Hour {
		t.Errorf("an interval longer than an hour doubles to %v", got)
	}
}
