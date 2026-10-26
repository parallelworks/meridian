package coordinator

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"
	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// oneJob is a workflow of one job, which reads nothing.
const oneJob = `
jobs:
  main:
    steps:
      - run: echo main
`

// runToEnd creates a run of workflow, with vars, has c serve it, and
// returns it once it has ended.
func runToEnd(t *testing.T, c *Coordinator, workflow string, vars conformancetest.Variables) *store.Run {
	t.Helper()
	id, _ := create(t, c, workflow, nil, vars)
	serve(t, c)
	return waitEnded(t, c, id)
}

// latestAttempt returns the latest attempt of the run id's job key.
func latestAttempt(t *testing.T, c *Coordinator, id, key string) *store.Attempt {
	t.Helper()
	var a *store.Attempt
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		a, err = tx.LatestAttempt(id, key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return a
}

// chosenPlace returns where the attempt id was to be placed.
func chosenPlace(t *testing.T, c *Coordinator, id string) store.Place {
	t.Helper()
	var p store.Place
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		p, _, err = tx.ChosenPlace(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return p
}

// groupsOf is a compile.Groups of the groups it holds, by name.
type groupsOf map[string]*backend.Group

func (g groupsOf) Group(_ context.Context, name string) (*backend.Group, error) {
	if group, ok := g[name]; ok {
		return group, nil
	}
	return nil, compile.ErrNotFound
}

func (groupsOf) Environment(context.Context, plan.Environment) (*backend.Group, error) {
	return nil, compile.ErrNotFound
}

func (groupsOf) ComputeTarget(context.Context, plan.ComputeTarget) (*backend.Group, error) {
	return nil, compile.ErrNotFound
}

// TestJobPlaced checks a job's life: the supervisor readies it, places an
// attempt on its group's member, with its bootstrap token and where its
// worker reaches the coordinator, records where, and ends the job as the
// attempt ended, once its worker wakes it.
func TestJobPlaced(t *testing.T) {
	c := open(t, Options{})
	began := time.Now()
	r := runToEnd(t, c, oneJob, nil)
	if r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended with %s", r.Outcome)
	}
	// The supervisor waits on the attempt for longer, unless its worker
	// wakes it.
	if took := time.Since(began); took > fastSupervision.wait/2 {
		t.Errorf("the run took %v: its supervisor wasn't woken", took)
	}
	if j := storedJob(t, c, r.ID, "main"); j.Outcome != plan.OutcomeSuccess || j.ReadyAt.IsZero() || j.Attempts != 1 {
		t.Errorf("the job is %+v", j)
	}
	a := latestAttempt(t, c, r.ID, "main")
	want := store.Placement{Group: "local", Member: "m1", Backend: testBackend, ID: "test-" + a.ID, WorkerDigest: testDigest}
	if a.Placement != want || a.End.Kind != store.EndCompleted || a.SelfFencing {
		t.Errorf("the attempt is %+v", a)
	}
	if p := chosenPlace(t, c, a.ID); p.Group != "local" || p.Member != "m1" || p.Backend != testBackend || p.Private != nil {
		t.Errorf("the attempt's place is %+v", p)
	}
	reqs, members := driverOf(c).requests()
	if len(reqs) != 1 || !slices.Equal(members, []string{"m1"}) {
		t.Fatalf("the driver placed %+v on %v", reqs, members)
	}
	req := reqs[0]
	if req.Attempt != a.ID || req.Name != "meridian-"+r.ID+"-main" || req.RunDir != "/runs/"+r.ID ||
		req.Coordinator.Address != "/meridian-test.sock" || len(req.BootstrapToken) < 32 {
		t.Errorf("the driver placed %+v", req)
	}
}

// TestJobsInOrder checks that a job is placed once the job it needs has
// ended, and that a member's connection is made once.
func TestJobsInOrder(t *testing.T) {
	c := open(t, Options{})
	r := runToEnd(t, c, `
jobs:
  b:
    needs: [a]
    steps:
      - run: echo b
  a:
    steps:
      - run: echo a
`, nil)
	if r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended with %s", r.Outcome)
	}
	a, b := latestAttempt(t, c, r.ID, "a"), latestAttempt(t, c, r.ID, "b")
	if b.CreatedAt.Before(a.EndedAt) {
		t.Errorf("b was placed at %v, before a ended at %v", b.CreatedAt, a.EndedAt)
	}
	d := driverOf(c)
	d.mu.Lock()
	connects := d.connects
	d.mu.Unlock()
	if connects != 1 {
		t.Errorf("the member's connection was made %d times", connects)
	}
}

// TestPrivateNeeds checks that a job reads the private outputs of the job
// it needs, which the worker API stores apart from run state, as the
// needed job's part of the run's secret record.
func TestPrivateNeeds(t *testing.T) {
	c := open(t, Options{})
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(req backend.PlaceRequest) testEnd {
			if !strings.HasSuffix(req.Name, "-a") {
				return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
			}
			return testEnd{result: plan.JobResult{
				Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"token": "s3cret", "shown": "yes"}, Private: []string{"token"},
			}}
		}
	})
	r := runToEnd(t, c, `
jobs:
  b:
    needs: [a]
    if: ${{ needs.a.outputs.token == 's3cret' && needs.a.outputs.shown == 'yes' }}
    steps:
      - run: echo b
  a:
    steps:
      - run: echo a
`, nil)
	a, b := storedJob(t, c, r.ID, "a"), storedJob(t, c, r.ID, "b")
	if r.Outcome != plan.OutcomeSuccess || b.Outcome != plan.OutcomeSuccess || b.Attempts != 1 {
		t.Fatalf("the run ended with %s, and b is %+v", r.Outcome, b)
	}
	if _, ok := a.Result.Outputs["token"]; ok || a.Result.Outputs["shown"] != "yes" {
		t.Errorf("run state holds a's outputs %v", a.Result.Outputs)
	}
}

// TestJobFails checks that a job whose attempt failed fails, and its run.
func TestJobFails(t *testing.T) {
	c := open(t, Options{})
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(backend.PlaceRequest) testEnd {
			return testEnd{result: plan.JobResult{Outcome: plan.OutcomeFailure}}
		}
	})
	r := runToEnd(t, c, oneJob, nil)
	if j := storedJob(t, c, r.ID, "main"); r.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeFailure {
		t.Errorf("the run ended with %s, and its job with %s", r.Outcome, j.Outcome)
	}
}

// TestJobSkipped checks that a job whose if: is false ends skipped,
// without an attempt.
func TestJobSkipped(t *testing.T) {
	c := open(t, Options{})
	r := runToEnd(t, c, `
jobs:
  main:
    if: ${{ false }}
    steps:
      - run: echo never
`, nil)
	if j := storedJob(t, c, r.ID, "main"); r.Outcome != plan.OutcomeSuccess || j.Outcome != plan.OutcomeSkipped || j.Attempts != 0 {
		t.Errorf("the run ended with %s, and its job is %+v", r.Outcome, j)
	}
	if reqs, _ := driverOf(c).requests(); len(reqs) != 0 {
		t.Errorf("a skipped job was placed: %+v", reqs)
	}
}

// TestPlacementImpossible checks that a job no member can run fails, with
// placement_impossible and each member's reason.
func TestPlacementImpossible(t *testing.T) {
	g := groupsOf{"local": {Name: "local", Members: []backend.Member{{Name: "m1", Backend: "slurm"}, {Name: "m2", Backend: testBackend}}}}
	c := open(t, Options{Groups: g})
	tooSmall := problems.ResourcesExceedMember.Problem(problems.Params{"member": "m2", "resource": "cpu", "asks": 4000, "offers": 2000})
	driverOf(c).set(func(d *testDriver) {
		d.probe = func(*backend.Member) (backend.Fit, error) { return backend.Fit{Problem: tooSmall}, nil }
	})
	r := runToEnd(t, c, oneJob, nil)
	j := storedJob(t, c, r.ID, "main")
	if r.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeFailure || len(j.Diagnostics) != 3 {
		t.Fatalf("the run ended with %s, and its job with %s, %v", r.Outcome, j.Outcome, j.Diagnostics)
	}
	for i, k := range []*problems.Kind{problems.PlacementImpossible, problems.BackendNotBuilt, problems.ResourcesExceedMember} {
		if j.Diagnostics[i].Code != k.Code() {
			t.Errorf("the job's diagnostic %d is %v, not %s", i, j.Diagnostics[i], k.Code())
		}
	}
	if j.Diagnostics[0].Params["job"] != "main" || j.Diagnostics[1].Params["member"] != "m1" {
		t.Errorf("the job's diagnostics are %v", j.Diagnostics)
	}
}

// setupProblems are problems a worker can fail with before its first step,
// each of a kind of its own.
var setupProblems = []*problem.Problem{
	problems.IsolationNotBuilt.Problem(problems.Params{"member": "m1", "isolation": "podman", "has_isolation": true}),
	problems.RunDirUnusable.Problem(problems.Params{"path": "/runs/1", "reason": "denied"}),
	problems.BackendNotBuilt.Problem(problems.Params{"member": "m1", "backend": "slurm"}),
	problems.PlanTooNew.Problem(nil),
}

// failSetup has the driver's workers fail before their first step with the
// problems fails gives, in turn, and then succeed.
func failSetup(c *Coordinator, fails ...*problem.Problem) {
	driverOf(c).set(func(d *testDriver) {
		n := 0
		d.worker = func(backend.PlaceRequest) testEnd {
			d.mu.Lock() // the driver's lock isn't held as worker is called
			defer d.mu.Unlock()
			if n < len(fails) {
				n++
				return testEnd{problem: fails[n-1]}
			}
			return testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
		}
	})
}

// TestSetupFailures checks that a job whose attempts fail before a step
// started is placed again, after a while, until the fourth such failure,
// or the second for the same reason, which fails it.
func TestSetupFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fails    []*problem.Problem
		outcome  plan.Outcome
		attempts int
	}{
		{"placed again", setupProblems[:3], plan.OutcomeSuccess, 4},
		{"same reason twice", []*problem.Problem{setupProblems[0], setupProblems[1], setupProblems[0]}, plan.OutcomeFailure, 3},
		{"four times", setupProblems, plan.OutcomeFailure, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := open(t, Options{})
			failSetup(c, tc.fails...)
			r := runToEnd(t, c, oneJob, nil)
			j := storedJob(t, c, r.ID, "main")
			if r.Outcome != tc.outcome || j.Outcome != tc.outcome || j.Attempts != tc.attempts {
				t.Fatalf("the run ended with %s, and its job with %s after %d attempts, %v", r.Outcome, j.Outcome, j.Attempts, j.Diagnostics)
			}
			if tc.outcome == plan.OutcomeSuccess {
				return
			}
			last := tc.fails[len(tc.fails)-1]
			if j.Reason != plan.ReasonSetupFailed || len(j.Diagnostics) != 2 ||
				j.Diagnostics[0].Code != problems.SetupFailed.Code() || j.Diagnostics[0].Params["tries"] != float64(tc.attempts) ||
				j.Diagnostics[1].Code != last.Code {
				t.Errorf("the job ended for %s, with %v", j.Reason, j.Diagnostics)
			}
		})
	}
}

// TestPlaceProblem checks that a problem from the driver's Place fails the
// attempt before its first step, and the job is placed again.
func TestPlaceProblem(t *testing.T) {
	c := open(t, Options{})
	driverOf(c).set(func(d *testDriver) {
		n := 0
		d.place = func(backend.PlaceRequest) error {
			d.mu.Lock()
			defer d.mu.Unlock()
			if n++; n == 1 {
				return setupProblems[1]
			}
			return nil
		}
	})
	r := runToEnd(t, c, oneJob, nil)
	if j := storedJob(t, c, r.ID, "main"); r.Outcome != plan.OutcomeSuccess || j.Attempts != 2 {
		t.Fatalf("the run ended with %s, and its job is %+v", r.Outcome, j)
	}
	if failures := setupFailures(t, c, r.ID, "main"); len(failures) != 1 || failures[0].End.Problem.Code != setupProblems[1].Code {
		t.Errorf("the job's setup failures are %v", failures)
	}
}

// setupFailures returns the attempts of the run id's job key that failed
// before a step started.
func setupFailures(t *testing.T, c *Coordinator, id, key string) []*store.Attempt {
	t.Helper()
	var failures []*store.Attempt
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		failures, err = tx.SetupFailures(id, key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return failures
}

// TestPlacedElsewhere checks that a job whose attempt failed before a step
// started is placed again on a member it hasn't failed on, while there is
// one, and then on the first again.
func TestPlacedElsewhere(t *testing.T) {
	c := open(t, Options{})
	failSetup(c, setupProblems[:3]...)
	created, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(oneJob), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}})
	if err != nil {
		t.Fatal(err)
	}
	id, err := c.CreateRun(t.Context(), created, RunOptions{DefaultGroup: "pair", RunDir: runOptions.RunDir})
	if err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeSuccess {
		t.Fatalf("the run ended with %s", r.Outcome)
	}
	if _, members := driverOf(c).requests(); !slices.Equal(members, []string{"m1", "m2", "m1", "m1"}) {
		t.Errorf("the job was placed on %v", members)
	}
}

// TestPlacementUnanswered checks that a job whose members' backends
// couldn't answer is placed later, a few times, after which that counts as
// a failure before its first step, with placement_unanswered, of an
// attempt made for the record; so that the second time fails the job, as
// the same reason twice does; and that the log says why.
func TestPlacementUnanswered(t *testing.T) {
	log := &logRecords{}
	c := open(t, Options{Log: slog.New(log)})
	driverOf(c).set(func(d *testDriver) {
		d.probe = func(*backend.Member) (backend.Fit, error) { return backend.Fit{}, errors.New("the scheduler is down") }
	})
	r := runToEnd(t, c, oneJob, nil)
	j := storedJob(t, c, r.ID, "main")
	if r.Outcome != plan.OutcomeFailure || j.Attempts != 2 || len(j.Diagnostics) != 2 ||
		j.Diagnostics[0].Code != problems.SetupFailed.Code() || j.Diagnostics[1].Code != problems.PlacementUnanswered.Code() ||
		j.Diagnostics[1].Params["tries"] != float64(fastSupervision.tries) {
		t.Errorf("the run ended with %s, and its job is %+v", r.Outcome, j)
	}
	if reqs, _ := driverOf(c).requests(); len(reqs) != 0 {
		t.Errorf("a job no member could say it runs was placed: %+v", reqs)
	}
	if !log.has("the scheduler is down") {
		t.Errorf("the log doesn't say why: %v", log.records)
	}
}

// TestPlaceUnanswered checks that an attempt whose Place couldn't answer is
// placed again, on the same member, after the driver's Find found it
// wasn't, with a new bootstrap token, so that it is never placed twice.
func TestPlaceUnanswered(t *testing.T) {
	c := open(t, Options{})
	finds := 0
	driverOf(c).set(func(d *testDriver) {
		n := 0
		d.place = func(backend.PlaceRequest) error {
			d.mu.Lock()
			defer d.mu.Unlock()
			if n++; n == 1 {
				return errors.New("the connection dropped")
			}
			return nil
		}
		d.find = func(string) error {
			d.mu.Lock()
			defer d.mu.Unlock()
			finds++
			return nil
		}
	})
	r := runToEnd(t, c, oneJob, nil)
	if j := storedJob(t, c, r.ID, "main"); r.Outcome != plan.OutcomeSuccess || j.Attempts != 1 {
		t.Fatalf("the run ended with %s, and its job is %+v", r.Outcome, j)
	}
	reqs, _ := driverOf(c).requests()
	if len(reqs) != 2 || reqs[0].Attempt != reqs[1].Attempt || reqs[0].BootstrapToken == reqs[1].BootstrapToken || finds != 1 {
		t.Errorf("the driver found %d times, and placed %+v", finds, reqs)
	}
}

// TestResumePlace checks that an attempt a supervisor chose a place for,
// but ended before it recorded its placement, is found where it was
// placed, and its placement recorded, without placing it again.
func TestResumePlace(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	d := driverOf(c)
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	var (
		a     *store.Attempt
		token string
	)
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		var err error
		if a, token, err = tx.NewAttempt(store.NewAttempt{Run: id, Job: "main", LeaseSeconds: 60, HeartbeatSeconds: 10}); err != nil {
			return err
		}
		return tx.ChoosePlace(a.ID, store.Place{Group: "local", Member: "m1", Backend: testBackend})
	}); err != nil {
		t.Fatal(err)
	}
	// The placement the supervisor didn't record, whose worker runs.
	p := backend.Placement{Attempt: a.ID, ID: "test-" + a.ID, WorkerDigest: testDigest}
	d.set(func(d *testDriver) { d.placements[a.ID] = p })
	ctx := context.WithoutCancel(t.Context())
	d.workers.Go(func() {
		d.work(ctx, backend.PlaceRequest{Attempt: a.ID, BootstrapToken: token}, p, testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}})
	})
	serve(t, c)
	r := waitEnded(t, c, id)
	if j := storedJob(t, c, id, "main"); r.Outcome != plan.OutcomeSuccess || j.Attempts != 1 {
		t.Fatalf("the run ended with %s, and its job is %+v", r.Outcome, j)
	}
	if got := latestAttempt(t, c, id, "main"); got.Placement.ID != p.ID || got.Placement.Member != "m1" {
		t.Errorf("the attempt's placement is %+v", got.Placement)
	}
	if reqs, _ := d.requests(); len(reqs) != 0 {
		t.Errorf("an attempt that was placed was placed again: %+v", reqs)
	}
}

// privateTarget is a workflow whose job runs where a variable says.
const privateTarget = `
jobs:
  main:
    ssh:
      remoteHost: ${{ vars.HOST }}
    steps:
      - run: hostname
`

// hostVars has the variable privateTarget reads.
var hostVars = conformancetest.Variables{"vars.HOST": {Value: "secret-host.example.org"}}

// TestPrivatePlacement checks that where a job whose target is private is
// placed is kept only sealed: its group, member and backend, and the
// problems its members give.
func TestPrivatePlacement(t *testing.T) {
	t.Run("placed", func(t *testing.T) {
		d := newTestDriver()
		c := open(t, Options{Variables: hostVars, Drivers: map[string]backend.Driver{testBackend: d, "ssh": d}})
		r := runToEnd(t, c, privateTarget, hostVars)
		if r.Outcome != plan.OutcomeSuccess {
			t.Fatalf("the run ended with %s", r.Outcome)
		}
		a := latestAttempt(t, c, r.ID, "main")
		if a.Placement != (store.Placement{WorkerDigest: testDigest}) {
			t.Errorf("a private placement is %+v in the clear", a.Placement)
		}
		if place := chosenPlace(t, c, a.ID); place.Group != "" || place.Member != "" || place.Private == nil {
			t.Fatalf("a private place is %+v", place)
		}
		if _, members := d.requests(); !slices.Equal(members, []string{"ssh"}) {
			t.Errorf("the job was placed on %v", members)
		}
	})
	t.Run("sealed", func(t *testing.T) {
		// While the run runs, its place opens as the attempt's alone: its
		// member, as it is chosen, and then the backend's ID for the
		// placement too, which the store holds only sealed.
		d := newTestDriver()
		d.worker = func(backend.PlaceRequest) testEnd { return testEnd{never: true} }
		c := open(t, Options{Variables: hostVars, Drivers: map[string]backend.Driver{"ssh": d}})
		id, _ := create(t, c, privateTarget, nil, hostVars)
		if _, err := c.startRun(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.supervise(t.Context(), id, "main", nil); err != nil {
			t.Fatal(err)
		}
		a := latestAttempt(t, c, id, "main")
		if !a.Private || !a.Placed() || a.Placement != (store.Placement{WorkerDigest: a.Placement.WorkerDigest}) {
			t.Errorf("the private attempt's placement is %+v in the clear", a.Placement)
		}
		var wrapped []byte
		if err := c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			wrapped, err = tx.WrappedKey(id)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		place := chosenPlace(t, c, a.ID)
		open := func(p store.Place) (sealedPlace, error) {
			sp, err := c.openPlaces(t.Context(), id, wrapped, map[string]store.Place{a.ID: p})
			return sp[a.ID], err
		}
		want := sealedPlace{Group: "ssh", Member: "ssh", Backend: "ssh", ID: "test-" + a.ID}
		if sp, err := open(place); err != nil || place.Generation != 2 || sp != want {
			t.Errorf("the sealed place of generation %d is %+v, %v; want %+v", place.Generation, sp, err, want)
		}
		for _, gen := range []uint64{1, 3} {
			_, err := open(store.Place{Private: place.Private, Generation: gen})
			kindtest.Want(t, err, problems.RecordUnreadable)
		}
		_, err := c.sealer.OpenParts(t.Context(), id, wrapped, []seal.SealedPart{{
			Purpose: seal.PurposeJobRecord, ID: a.ID, Generation: place.Generation, Sealed: place.Private,
		}})
		kindtest.Want(t, err, problems.RecordUnreadable)
	})
	t.Run("masked", func(t *testing.T) {
		log := &logRecords{}
		d := newTestDriver()
		d.probe = func(m *backend.Member) (backend.Fit, error) {
			return backend.Fit{Problem: problems.BackendNotBuilt.Problem(problems.Params{"member": m.Name, "backend": "secret-host.example.org"})}, nil
		}
		c := open(t, Options{Variables: hostVars, Drivers: map[string]backend.Driver{"ssh": d}, Log: slog.New(log)})
		r := runToEnd(t, c, privateTarget, hostVars)
		j := storedJob(t, c, r.ID, "main")
		if len(j.Diagnostics) != 2 || j.Diagnostics[1].Code != problems.BackendNotBuilt.Code() || j.Diagnostics[1].Params["backend"] != problems.Masked {
			t.Fatalf("the job's diagnostics are %v", j.Diagnostics)
		}
		if strings.Contains(j.Diagnostics[1].Detail, "secret-host") {
			t.Errorf("the job's diagnostic shows its member: %v", j.Diagnostics[1])
		}
	})
	t.Run("unanswered", func(t *testing.T) {
		log := &logRecords{}
		d := newTestDriver()
		d.probe = func(*backend.Member) (backend.Fit, error) {
			return backend.Fit{}, errors.New("can't reach secret-host.example.org")
		}
		c := open(t, Options{Variables: hostVars, Drivers: map[string]backend.Driver{"ssh": d}, Log: slog.New(log)})
		r := runToEnd(t, c, privateTarget, hostVars)
		if j := storedJob(t, c, r.ID, "main"); j.Outcome != plan.OutcomeFailure {
			t.Errorf("the job ended with %s", j.Outcome)
		}
		if log.has("secret-host") {
			t.Errorf("the log names a private member: %v", log.records)
		}
	})
}

// TestStaleSupervisor checks that a supervisor whose look went stale, as
// one hopper rescued from a replica still running it, changes nothing
// another supervisor of the job did meanwhile: it neither supersedes the
// attempt the other made, whose worker runs a step, nor counts a placement
// failure of it; and it neither ends an attempt the other placed, nor
// gives it a new token.
func TestStaleSupervisor(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	c.supervision.tries = 1
	started := func() (*store.Run, string) {
		id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
		if _, err := c.startRun(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
		return storedRun(t, c, id), id
	}
	place := store.Place{Group: "local", Member: "m1", Backend: testBackend}
	placement := store.Placement{Group: "local", Member: "m1", Backend: testBackend, ID: "test-other", WorkerDigest: testDigest}

	// The other supervisor's attempt runs a step.
	run, id := started()
	seen := storedJob(t, c, id, "main")
	var running *store.Attempt
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		a, token, err := tx.NewAttempt(c.newAttempt(id, "main", false))
		if err != nil {
			return err
		}
		running = a
		if err := tx.ChoosePlace(a.ID, place); err != nil {
			return err
		}
		if err := tx.RecordPlacement(a.ID, placement); err != nil {
			return err
		}
		if _, err := tx.Claim(token, hex.EncodeToString(make([]byte, 32)), workerapi.Worker{Version: "test", Digest: testDigest}); err != nil {
			return err
		}
		return tx.StepStarted(a.ID)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.placeJob(t.Context(), run, seen, nil); !errors.Is(err, errAgain) {
		t.Errorf("placeJob of the job as it was = %v, want errAgain", err)
	}
	if _, err := c.unanswered(t.Context(), run, seen, false, errors.New("no answer")); !errors.Is(err, errAgain) {
		t.Errorf("unanswered of the job as it was = %v, want errAgain", err)
	}
	if a := latestAttempt(t, c, id, "main"); a.ID != running.ID || a.State != store.AttemptRunning || !a.Holds {
		t.Errorf("the running attempt is %+v", a)
	}
	if reqs, _ := driverOf(c).requests(); len(reqs) != 0 {
		t.Errorf("the stale supervisor placed %+v", reqs)
	}

	// The other supervisor placed the attempt both were placing.
	run, id = started()
	var placing *store.Attempt
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		a, _, err := tx.NewAttempt(c.newAttempt(id, "main", false))
		if err != nil {
			return err
		}
		placing = a
		return tx.ChoosePlace(a.ID, place)
	}); err != nil {
		t.Fatal(err)
	}
	seen = storedJob(t, c, id, "main")
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error { return tx.RecordPlacement(placing.ID, placement) }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.resumePlace(t.Context(), run, seen, placing); !errors.Is(err, errAgain) {
		t.Errorf("resumePlace of an attempt placed meanwhile = %v, want errAgain", err)
	}
	if err := c.attemptSetupFailed(t.Context(), placing.ID, setupProblems[0]); !errors.Is(err, errAgain) {
		t.Errorf("attemptSetupFailed of an attempt placed meanwhile = %v, want errAgain", err)
	}
	if a := latestAttempt(t, c, id, "main"); a.ID != placing.ID || a.State != store.AttemptPlacing || a.Placement.ID != placement.ID {
		t.Errorf("the placed attempt is %+v", a)
	}
}

// TestFinisherWithdraws checks that the finisher fails a job that never
// ended, and withdraws its attempt, whose worker's reports are refused.
func TestFinisherWithdraws(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	driverOf(c).set(func(d *testDriver) {
		d.worker = func(backend.PlaceRequest) testEnd { return testEnd{never: true} }
	})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	if wait, err := c.supervise(t.Context(), id, "main", nil); err != nil || wait != fastSupervision.wait {
		t.Fatalf("supervise = %v, %v", wait, err)
	}
	noop := func(context.Context, *store.Tx) error { return nil }
	if _, err := c.finishRun(t.Context(), id, noop); err != nil {
		t.Fatal(err)
	}
	a := latestAttempt(t, c, id, "main")
	j := storedJob(t, c, id, "main")
	if a.End.Kind != store.EndWithdrawn || j.Outcome != plan.OutcomeFailure || !hasCode(j.Diagnostics, problems.CoordinatorJobFailed) {
		t.Errorf("the attempt ended %s, and the job %s, with %v", a.End.Kind, j.Outcome, j.Diagnostics)
	}
}

// TestWakeEnded checks that waking the supervisor of a job that has ended
// doesn't run it again.
func TestWakeEnded(t *testing.T) {
	c := open(t, Options{})
	r := runToEnd(t, c, oneJob, nil)
	j := storedJob(t, c, r.ID, "main")
	c.wakeJob(t.Context(), r.ID, "main")
	row, err := c.inserter.JobGet(t.Context(), j.SupervisorID)
	if err != nil || row.State != hopper.JobStateCompleted {
		t.Errorf("woken once its job ended, the supervisor is %+v, %v", row, err)
	}
	if got := storedJob(t, c, r.ID, "main"); got.Wake != j.Wake+1 {
		t.Errorf("the job's wake is %d, then %d", j.Wake, got.Wake)
	}
}

// TestNotRunYet checks that CreateRun refuses a matrix job, and a need on
// any instance of one, which the coordinator doesn't run yet.
func TestNotRunYet(t *testing.T) {
	c := open(t, Options{})
	for _, tc := range []struct {
		workflow string
		kind     *problems.Kind
		params   problems.Params
	}{
		{`
jobs:
  main:
    strategy:
      matrix:
        n: [1, 2]
    steps:
      - run: echo ${{ matrix.n }}
  after:
    needs: [main:any]
    steps:
      - run: echo after
`, problems.MatrixNotBuilt, problems.Params{"job": "main"}},
		{`
jobs:
  main:
    steps:
      - run: echo main
  after:
    needs: [main:any]
    steps:
      - run: echo after
`, problems.AnyNotBuilt, problems.Params{"job": "after", "needed": "main"}},
	} {
		created, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(tc.workflow), nil, compile.Options{Source: plan.Source{Location: "workflow.yaml"}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CreateRun(t.Context(), created, runOptions)
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != tc.kind.Code() || !maps.Equal(p.Params, tc.params) {
			t.Errorf("CreateRun = %v, want %s with %v", err, tc.kind.Code(), tc.params)
		}
	}
}

// TestSupervisorStops checks that a supervisor whose coordinator stops as
// it waits on a backend gives hopper its attempt back.
func TestSupervisorStops(t *testing.T) {
	c := open(t, Options{})
	c.stopTimeout = 100 * time.Millisecond
	placing := make(chan struct{}, 1)
	driverOf(c).set(func(d *testDriver) {
		d.place = func(backend.PlaceRequest) error {
			select {
			case placing <- struct{}{}:
			default:
			}
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo // a backend slow to answer
			return errors.New("too slow")
		}
	})
	id, _ := create(t, c, oneJob, nil, nil)
	stop := serving(t, c)
	select {
	case <-placing:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never placed")
	}
	stop()
	if j := storedJob(t, c, id, "main"); j.Outcome != "" {
		t.Errorf("once the coordinator stopped, the job is %+v", j)
	}
}
