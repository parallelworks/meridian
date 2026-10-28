package coordinator

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// treeVars are the variables the runs of treeRoot's tree read.
var treeVars = conformancetest.Variables{"vars.CHILD_VAR": {Value: "v"}}

// treeWith is the with: of treeRoot's steps that call the child.
var treeWith = map[string]any{"$yaml": "w.yaml", "region": "east"}

// TestChildStoppedWithItsAttempt checks that the children of an attempt's
// steps stop, cancelled, in the transaction that ends the attempt, however
// it ends: lost, withdrawn, or superseded by a newer attempt of its job;
// one that hasn't started ends at once.
func TestChildStoppedWithItsAttempt(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		name    string
		started bool
		end     func(tx *store.Tx, r *actionRig, root, attempt string) error
	}{
		{"lost", true, func(tx *store.Tx, _ *actionRig, _, attempt string) error {
			return tx.EndAttempt(attempt, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
		}},
		{"withdrawn", true, func(tx *store.Tx, _ *actionRig, _, attempt string) error {
			return tx.EndAttempt(attempt, store.AttemptEnd{Kind: store.EndWithdrawn})
		}},
		{"superseded", true, func(tx *store.Tx, r *actionRig, root, _ string) error {
			_, _, err := tx.NewAttempt(r.c.newAttempt(root, "build", false))
			return err
		}},
		{"lost, its child not started", false, func(tx *store.Tx, _ *actionRig, _, attempt string) error {
			return tx.EndAttempt(attempt, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newActionRigWith(t, Options{Variables: treeVars})
			root, _ := createTree(t, r.c, treeVars)
			if _, err := r.c.startRun(t.Context(), root, nil); err != nil {
				t.Fatal(err)
			}
			attempt, _, _ := r.attempt(t, root, "build", false)
			child, _, err := askChild(t, r.c, attempt, workerapi.ChildRequest{With: plan.DataOf(treeWith)})
			if err != nil {
				t.Fatal(err)
			}
			if tc.started {
				if _, err := r.c.startRun(t.Context(), child, nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
				if err := tc.end(tx, r, root, attempt); err != nil {
					return err
				}
				// Before the transaction commits.
				cr, err := tx.Run(child)
				if err == nil && (cr.StoppedAt.IsZero() || cr.StopReason != plan.ReasonCancelled) {
					t.Errorf("as the attempt ends, its step's child is stopped at %v, for %q", cr.StoppedAt, cr.StopReason)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			cr := storedRun(t, r.c, child)
			switch {
			case tc.started && cr.State != store.RunRunning:
				t.Errorf("the child that ran is %s", cr.State)
			case !tc.started && (cr.State != store.RunEnded || cr.Outcome != plan.OutcomeCancelled || cr.Reason != plan.ReasonCancelled):
				t.Errorf("the child that hadn't started is %s, %s, %s", cr.State, cr.Outcome, cr.Reason)
			}
		})
	}
}

// TestChildStoppedWithItsRun checks that a run that stops stops, in the
// same transaction, the children of the steps its stop stops, for the
// stop's reason, a cancel or the run's timeout, and not those of a step
// that outlives it; each child's jobs are then told to stop. Its tree ends
// before it: it waits for its children to end.
func TestChildStoppedWithItsRun(t *testing.T) {
	parallel(t)
	for _, reason := range []plan.Reason{plan.ReasonCancelled, plan.ReasonTimeout} {
		t.Run(string(reason), func(t *testing.T) {
			parallel(t)
			c := open(t, Options{Variables: treeVars})
			buildGo, mainGo := make(chan struct{}), make(chan struct{})
			byJob(c, map[string]testEnd{
				"build": {wait: buildGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}},
				"main":  {wait: mainGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}},
			})
			root, _ := createTree(t, c, treeVars)
			serve(t, c)
			waitFor(t, func() bool {
				return c.store.View(t.Context(), func(tx *store.Tx) error {
					_, err := tx.LatestAttempt(root, "build")
					return err
				}) == nil
			})
			a := latestAttempt(t, c, root, "build")
			stopped, _, err := askChild(t, c, a.ID, workerapi.ChildRequest{Step: 0, With: plan.DataOf(treeWith)})
			if err != nil {
				t.Fatal(err)
			}
			// Step 2's if: is always(), which outlives the stop.
			outlives, _, err := askChild(t, c, a.ID, workerapi.ChildRequest{Step: 2, With: plan.DataOf(treeWith)})
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{stopped, outlives} {
				waitFor(t, func() bool {
					return c.store.View(t.Context(), func(tx *store.Tx) error {
						_, err := tx.LatestAttempt(id, "main")
						return err
					}) == nil
				})
			}
			if err := c.stop(t.Context(), root, reason); err != nil {
				t.Fatal(err)
			}
			if r := storedRun(t, c, stopped); r.StopReason != reason {
				t.Errorf("the child of a step the stop stops is stopped for %q", r.StopReason)
			}
			if r := storedRun(t, c, outlives); !r.StoppedAt.IsZero() {
				t.Errorf("the child of a step that outlives the stop is stopped for %q", r.StopReason)
			}
			// Its job's supervisor tells its attempt to stop.
			waitFor(t, func() bool {
				var ctl workerapi.Control
				err := c.store.View(t.Context(), func(tx *store.Tx) error {
					a, err := tx.LatestAttempt(stopped, "main")
					if err == nil {
						ctl, err = tx.Control(a.ID)
					}
					return err
				})
				return err == nil && ctl.Cancel && ctl.Reason == reason
			})
			close(mainGo)
			for _, id := range []string{stopped, outlives} {
				waitFor(t, func() bool { return storedRun(t, c, id).State == store.RunEnded })
			}
			if r := storedRun(t, c, root); r.State == store.RunEnded {
				t.Error("the root ended before its job did")
			}
			close(buildGo)
			waitFor(t, func() bool { return storedRun(t, c, root).State == store.RunEnded })
			rootEnd := storedRun(t, c, root).EndedAt
			for _, id := range []string{stopped, outlives} {
				if end := storedRun(t, c, id).EndedAt; end.After(rootEnd) {
					t.Errorf("the child %s ended at %v, after its root, at %v", id, end, rootEnd)
				}
			}
		})
	}
}

// TestChildStoppedWithItsLostAttempt checks that the child of a step whose
// attempt is lost as it waits for the child, as a worker that died, stops,
// cancelled, and its root waits for it to end before it ends, failed, its
// job having failed for the worker lost.
func TestChildStoppedWithItsLostAttempt(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: treeVars})
	started, mainGo := make(chan struct{}), make(chan struct{})
	byJob(c, map[string]testEnd{
		"build": {running: true, started: started},
		"main":  {wait: mainGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}},
	})
	root, _ := createTree(t, c, treeVars)
	serve(t, c)
	<-started
	a := latestAttempt(t, c, root, "build")
	child, _, err := askChild(t, c, a.ID, workerapi.ChildRequest{With: plan.DataOf(treeWith)})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			_, err := tx.LatestAttempt(child, "main")
			return err
		}) == nil
	})
	// As its supervisor loses it, once its backend says its worker has
	// ended, and looks again.
	if err := c.lose(t.Context(), a, backend.Status{}, nil); err != nil && !errors.Is(err, errAgain) {
		t.Fatal(err)
	}
	var again func()
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		var err error
		again, err = c.wakeWorkerJob(t.Context(), tx, root, "build")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if again != nil {
		again()
	}
	if r := storedRun(t, c, child); r.StopReason != plan.ReasonCancelled {
		t.Errorf("the child of a lost attempt's step is stopped for %q", r.StopReason)
	}
	waitFor(t, func() bool { return storedJob(t, c, root, "build").Outcome != "" })
	if j := storedJob(t, c, root, "build"); j.Outcome != plan.OutcomeFailure || j.Reason != plan.ReasonWorkerLost {
		t.Errorf("the lost attempt's job ended %s, %s", j.Outcome, j.Reason)
	}
	if r := storedRun(t, c, root); r.State == store.RunEnded {
		t.Error("the root ended before its child")
	}
	close(mainGo)
	waitFor(t, func() bool { return storedRun(t, c, root).State == store.RunEnded })
	cr, rr := storedRun(t, c, child), storedRun(t, c, root)
	if cr.State != store.RunEnded || cr.Outcome != plan.OutcomeCancelled || cr.EndedAt.After(rr.EndedAt) || rr.Outcome != plan.OutcomeFailure {
		t.Errorf("the child ended %s, %s at %v, and the root %s at %v", cr.State, cr.Outcome, cr.EndedAt, rr.Outcome, rr.EndedAt)
	}
}

// TestChildStopRewakes checks that a transaction that stops a child, as
// the end of the attempt whose step called it does, has the supervisors it
// woke while hopper ran them woken again once it commits: one placing its
// job's attempt, which a backend takes a while to answer.
func TestChildStopRewakes(t *testing.T) {
	parallel(t)
	c := open(t, Options{Variables: treeVars})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	placing := make(chan struct{}, 1)
	driverOf(c).set(func(d *testDriver) {
		d.place = func(req backend.PlaceRequest) error {
			if jobOf(req) == "main" {
				select {
				case placing <- struct{}{}:
				default:
				}
				<-release
			}
			return nil
		}
	})
	byJob(c, map[string]testEnd{"build": {running: true}})
	var (
		mu      sync.Mutex
		rewoken []string
	)
	c.rewake = func(ws []*waking) {
		mu.Lock()
		defer mu.Unlock()
		for _, w := range ws {
			if w != nil {
				rewoken = append(rewoken, w.run+"/"+w.key)
			}
		}
	}
	root, _ := createTree(t, c, treeVars)
	serve(t, c)
	waitFor(t, func() bool {
		a, err := latestAttemptOf(t, c, root, "build")
		return err == nil && a.State == store.AttemptRunning
	})
	a := latestAttempt(t, c, root, "build")
	child, _, err := askChild(t, c, a.ID, workerapi.ChildRequest{With: plan.DataOf(treeWith)})
	if err != nil {
		t.Fatal(err)
	}
	// The child's job's supervisor is placing its attempt, which hopper
	// runs.
	<-placing
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(rewoken, child+"/main") {
		t.Errorf("the supervisors woken again are %v, want %s/main", rewoken, child)
	}
}

// TestChildUnreadable checks that an attempt ends though a child of its
// steps can't be read, as one the store holds damaged: it isn't stopped,
// and is left as it is.
func TestChildUnreadable(t *testing.T) {
	parallel(t)
	r := newActionRigWith(t, Options{Variables: treeVars})
	root, _ := createTree(t, r.c, treeVars)
	if _, err := r.c.startRun(t.Context(), root, nil); err != nil {
		t.Fatal(err)
	}
	attempt, _, _ := r.attempt(t, root, "build", false)
	child, _, err := askChild(t, r.c, attempt, workerapi.ChildRequest{With: plan.DataOf(treeWith)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
		_, err := tx.SQL().ExecContext(t.Context(), "UPDATE meridian_runs SET diagnostics = ? WHERE id = ?", []byte("{"), child)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(attempt, store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Errorf("an attempt whose step's child can't be read didn't end: %v", err)
	}
	if a := latestAttempt(t, r.c, root, "build"); a.State != store.AttemptEnded {
		t.Errorf("the attempt is %s", a.State)
	}
}

// latestAttemptOf returns the latest attempt of the run id's job key.
func latestAttemptOf(t *testing.T, c *Coordinator, id, key string) (*store.Attempt, error) {
	t.Helper()
	var a *store.Attempt
	err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		a, err = tx.LatestAttempt(id, key)
		return err
	})
	return a, err
}

// cancellerRoot calls cancellerChild, whose step 1 cancels jobs as a test
// asks, from build's step 0; and has a job of its own, other.
const cancellerRoot = `name: flows
jobs:
  build:
    steps:
      - uses: github/t/canceller@main
        with: {$yaml: w.yaml}
  other:
    steps:
      - run: echo other
`

// cancellerSources are the workflows cancellerRoot calls: a child whose
// job main cancels jobs, and which has a job of its own, x.
var cancellerSources = childSources{"github/t/canceller@main": `name: inner
jobs:
  main:
    steps:
      - run: echo main
      - uses: parallelworks/cancel-jobs
        with: {jobs: [x]}
  x:
    steps:
      - run: echo x
`}

// TestCancelJobsInChild checks what a cancel-jobs step of a child run
// cancels, as format 1's of a subworkflow does: the child's run is named
// as its root is, so a workflow and run that name the root cancel the
// root's jobs, or the root; a run alone that names it, or nothing, the
// child's own; and a name of the child's own, which format 1 doesn't
// know, another run's, which the step fails for.
func TestCancelJobsInChild(t *testing.T) {
	parallel(t)
	r := newActionRigWith(t, Options{})
	for _, tc := range []struct {
		name string
		with func(root, child int64) map[string]any
		// cancelled is the job cancelled, as root/other or child/x, by the
		// step by, with the child's ID for {child}; and code and key a
		// step's failure.
		cancelled, by string
		code, key     string
	}{
		{"the root's workflow and number", func(root, _ int64) map[string]any {
			return map[string]any{"workflow": "flows", "run": root, "jobs": []any{"other"}}
		}, "root/other", "{child}:/jobs/main/steps/1", "", ""},
		{"the root's number", func(root, _ int64) map[string]any {
			return map[string]any{"run": root, "jobs": []any{"x"}}
		}, "child/x", "/jobs/main/steps/1", "", ""},
		{"nothing", func(int64, int64) map[string]any { return map[string]any{"jobs": []any{"x"}} }, "child/x", "/jobs/main/steps/1", "", ""},
		{"the child's number", func(_, child int64) map[string]any {
			return map[string]any{"run": child, "jobs": []any{"x"}}
		}, "", "", string(problems.CancelJobsOtherRun.Code()), "run"},
		{"the child's workflow and number", func(_, child int64) map[string]any {
			return map[string]any{"workflow": "inner", "run": child, "jobs": []any{"x"}}
		}, "", "", string(problems.CancelJobsOtherRun.Code()), "workflow"},
	} {
		root, child, credential := startCanceller(t, r)
		rootN, _ := strconv.ParseInt(root, 10, 64)
		childN, _ := strconv.ParseInt(child, 10, 64)
		answer := r.act(t, credential, 1, tc.with(rootN, childN))
		jobs := map[string]*store.Job{"root/other": storedJob(t, r.c, root, "other"), "child/x": storedJob(t, r.c, child, "x")}
		for name, j := range jobs {
			if was := !j.CancelledAt.IsZero(); was != (name == tc.cancelled) {
				t.Errorf("%s: %s cancelled %v", tc.name, name, was)
			}
			if by := strings.ReplaceAll(tc.by, "{child}", child); name == tc.cancelled && j.CancelledBy != by {
				t.Errorf("%s: %s cancelled by %q, want %q", tc.name, name, j.CancelledBy, by)
			}
		}
		switch {
		case tc.code == "" && (answer.Outcome != plan.OutcomeSuccess || len(answer.Diagnostics) > 0):
			t.Errorf("%s: answered %+v", tc.name, answer)
		case tc.code != "" && (answer.Outcome != plan.OutcomeFailure || len(answer.Diagnostics) != 1 ||
			string(answer.Diagnostics[0].Code) != tc.code || answer.Diagnostics[0].Pointer != "#/jobs/main/steps/1/with/"+tc.key):
			t.Errorf("%s: answered %+v", tc.name, answer)
		}
	}

	// The root's workflow and number without jobs: cancel the root; the
	// child's number alone, the child.
	root, _, credential := startCanceller(t, r)
	rootN, _ := strconv.ParseInt(root, 10, 64)
	if answer := r.act(t, credential, 1, map[string]any{"workflow": "flows", "run": rootN}); answer.Outcome != plan.OutcomeSuccess {
		t.Errorf("a cancel of the root was answered %+v", answer)
	}
	if rr := storedRun(t, r.c, root); rr.StopReason != plan.ReasonCancelled {
		t.Errorf("the root is stopped for %q", rr.StopReason)
	}
	root, child, credential := startCanceller(t, r)
	if answer := r.act(t, credential, 1, map[string]any{}); answer.Outcome != plan.OutcomeSuccess {
		t.Errorf("a cancel of the child's own run was answered %+v", answer)
	}
	if cr, rr := storedRun(t, r.c, child), storedRun(t, r.c, root); cr.StopReason != plan.ReasonCancelled || !rr.StoppedAt.IsZero() {
		t.Errorf("the child is stopped for %q, and the root for %q", cr.StopReason, rr.StopReason)
	}
}

// startCanceller starts a run of cancellerRoot, and the child its build's
// step calls, and an attempt of the child's job main, claimed; and returns
// the root's ID, the child's, and the credential of the child's attempt.
func startCanceller(t *testing.T, r *actionRig) (string, string, string) {
	t.Helper()
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(cancellerRoot), nil,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: cancellerSources})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	root, err := r.c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.c.startRun(t.Context(), root, nil); err != nil {
		t.Fatal(err)
	}
	attempt, _, _ := r.attempt(t, root, "build", false)
	child, _, err := askChild(t, r.c, attempt, workerapi.ChildRequest{With: plan.DataOf(map[string]any{"$yaml": "w.yaml"})})
	if err != nil || child == "" {
		t.Fatal(child, err)
	}
	if _, err := r.c.startRun(t.Context(), child, nil); err != nil {
		t.Fatal(err)
	}
	_, credential, _ := r.attempt(t, child, "main", false)
	return root, child, credential
}
