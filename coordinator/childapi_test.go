package coordinator

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// endNotifier hears of the runs that ended (controlNotifier.Ended).
type endNotifier struct {
	mu    sync.Mutex
	ended []string
}

func (n *endNotifier) Notify(...string) {}
func (n *endNotifier) Placed(...string) {}
func (n *endNotifier) Ended(ids ...string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ended = append(n.ended, ids...)
}

func (n *endNotifier) heard(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Contains(n.ended, id)
}

// TestChildAPI checks what the worker API hears of a step's child: its
// run, the same for the same try; a child whose Create failed, failure,
// with child_not_created at the step's uses: and the child's own problems;
// one the coordinator refused, failure, with its problem there; cancelled,
// without one made, for a step of a job its control cancels, or of a run
// that was stopped, unless its if: outlives that, whatever the worker
// says, and for a step whose early-cancel: is any-job-failed, once a job
// failed; ErrNotChild for a step that calls no workflow, or a try past its
// retry:; Fenced for an attempt superseded; and a child's stop.
func TestChildAPI(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.CHILD_VAR": {Value: "v"}}
	c := open(t, Options{Variables: vars})
	notifier := &endNotifier{}
	c.controls = notifier
	hold, mainGo := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(hold) })
	byJob(c, map[string]testEnd{"build": {wait: hold}, "main": {wait: mainGo, result: plan.JobResult{Outcome: plan.OutcomeSuccess}}})
	root, _ := createTree(t, c, vars)
	serve(t, c)
	var attempt *store.Attempt
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			var err error
			attempt, err = tx.LatestAttempt(root, "build")
			return err
		}) == nil && storedRun(t, c, root).StartState != nil
	})
	api := childAPI{c}
	run := storedRun(t, c, root)
	with := plan.DataOf(map[string]any{"$yaml": "w.yaml", "region": "east"})
	ask := func(req workerapi.ChildRequest) (*workerapi.Child, error) {
		t.Helper()
		a := latestAttempt(t, c, root, "build")
		return api.Child(t.Context(), storedRun(t, c, root), a, &req)
	}

	child, err := ask(workerapi.ChildRequest{Step: 0, With: with})
	if err != nil || child.Run == "" || child.Ended {
		t.Fatalf("the step's child is %+v, %v", child, err)
	}
	if again, err := ask(workerapi.ChildRequest{Step: 0, With: with}); err != nil || again.Run != child.Run {
		t.Errorf("the step's child asked again is %+v, %v", again, err)
	}
	failed, err := ask(workerapi.ChildRequest{Step: 0, Try: 1, With: plan.DataOf(map[string]any{"$yaml": "w.yaml"})})
	if err != nil || failed.Run != "" || !failed.Ended || failed.Outcome != plan.OutcomeFailure || len(failed.Diagnostics) < 2 ||
		failed.Diagnostics[0].Code != problems.ChildNotCreated.Code() || failed.Diagnostics[0].Path() != "/jobs/build/steps/0/uses" ||
		failed.Diagnostics[1].Code != problems.InputRequired.Code() {
		t.Errorf("a child whose Create failed: %+v, %v", failed, err)
	}
	for name, req := range map[string]workerapi.ChildRequest{
		"a step that calls no workflow": {Step: 1},
		"a step the job hasn't":         {Step: 4},
		"a try past the step's retry:":  {Step: 0, Try: 21, With: with},
	} {
		if got, err := ask(req); !errors.Is(err, workerapisrv.ErrNotChild) {
			t.Errorf("%s: %+v, %v", name, got, err)
		}
	}
	outside, err := ask(workerapi.ChildRequest{Step: 0, Try: 2, With: with, WorkingDirectory: "/elsewhere"})
	if err != nil || !outside.Ended || outside.Outcome != plan.OutcomeFailure || len(outside.Diagnostics) != 1 ||
		outside.Diagnostics[0].Code != problems.ChildDirOutside.Code() || outside.Diagnostics[0].Path() != "/jobs/build/steps/0/uses" {
		t.Errorf("a child outside its root's directory: %+v, %v", outside, err)
	}

	// The child's stop is a cancel of its run, and its end is heard of.
	if err := api.StopChild(t.Context(), child.Run, plan.ReasonTimeout); err != nil {
		t.Fatal(err)
	}
	if r := storedRun(t, c, child.Run); r.StopReason != plan.ReasonTimeout {
		t.Errorf("the stopped child's stop is %q", r.StopReason)
	}
	close(mainGo)
	waitFor(t, func() bool { return notifier.heard(child.Run) })
	if err := api.StopChild(t.Context(), child.Run, plan.ReasonCancelled); err != nil {
		t.Errorf("stopping a child that has ended: %v", err)
	}

	// Once a job of the run has failed, a step whose early-cancel: is
	// any-job-failed gets no child, and another does: a change of the
	// attempt's control between the ask's Create and the transaction that
	// would store the child is read again there, and no run is stored.
	runs := func() int {
		n := 0
		_ = c.store.View(t.Context(), func(tx *store.Tx) error {
			return tx.SQL().QueryRowContext(t.Context(), "SELECT count(*) FROM meridian_runs").Scan(&n)
		})
		return n
	}
	setControl := func(change func(ctl *workerapi.Control)) {
		t.Helper()
		if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
			_, err := tx.UpdateControl(attempt.ID, func(ctl *workerapi.Control) bool {
				change(ctl)
				return true
			})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	before := runs()
	c.beforeChildStore = func() { setControl(func(ctl *workerapi.Control) { ctl.AnyJobFailed = true }) }
	got, err := ask(workerapi.ChildRequest{Step: 3, With: with})
	c.beforeChildStore = nil
	if err != nil || got.Run != "" || !got.Ended || got.Outcome != plan.OutcomeCancelled || got.Reason != "" || runs() != before {
		t.Errorf("a step that early-cancels, as a job failed: %+v, %v, with %d runs, %d before", got, err, runs(), before)
	}
	if got, err := ask(workerapi.ChildRequest{Step: 2, Try: 1, With: with}); err != nil || got.Run == "" {
		t.Errorf("a step that doesn't early-cancel, once a job failed: %+v, %v", got, err)
	}

	// A job whose control cancels it gets no child for a step that
	// doesn't outlive that, as the plan says, whether the cancel came
	// before the ask or as the child was stored.
	before = runs()
	c.beforeChildStore = func() {
		setControl(func(ctl *workerapi.Control) { ctl.Cancel, ctl.Reason = true, plan.ReasonFailFast })
	}
	got, err = ask(workerapi.ChildRequest{Step: 0, Try: 3, With: with})
	c.beforeChildStore = nil
	if err != nil || got.Run != "" || !got.Ended || got.Outcome != plan.OutcomeCancelled || got.Reason != plan.ReasonFailFast || runs() != before {
		t.Errorf("a step of a job cancelled as its child was stored: %+v, %v, with %d runs, %d before", got, err, runs(), before)
	}
	if got, err := ask(workerapi.ChildRequest{Step: 0, Try: 4, With: with}); err != nil || got.Run != "" || !got.Ended || got.Outcome != plan.OutcomeCancelled || got.Reason != plan.ReasonFailFast {
		t.Errorf("a step of a job being cancelled: %+v, %v", got, err)
	}
	// A try that has its child is answered with it all the same, as a
	// repeat of the ask that made it is.
	if again, err := ask(workerapi.ChildRequest{Step: 0, With: with}); err != nil || again.Run != child.Run {
		t.Errorf("a try's child asked again once its job is cancelled: %+v, %v", again, err)
	}
	if got, err := ask(workerapi.ChildRequest{Step: 2, With: with}); err != nil || got.Run == "" {
		t.Errorf("a step that outlives its job's cancel: %+v, %v", got, err)
	}
	// Nor does a step of a run that was stopped, with its stop's reason.
	if err := c.Cancel(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	setControl(func(ctl *workerapi.Control) { ctl.Cancel, ctl.Reason = true, plan.ReasonTimeout })
	if got, err := ask(workerapi.ChildRequest{Step: 0, Try: 5, With: with}); err != nil || got.Run != "" || got.Outcome != plan.OutcomeCancelled || got.Reason != plan.ReasonCancelled {
		t.Errorf("a step of a stopped run: %+v, %v", got, err)
	}
	// An attempt superseded is fenced.
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		_, _, err := tx.NewAttempt(store.NewAttempt{Run: root, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.Child(t.Context(), run, attempt, &workerapi.ChildRequest{Step: 2, Try: 2, With: with}); !errors.Is(err, store.ErrFenced) {
		t.Errorf("a superseded attempt's step: %v", err)
	}
}

// TestChildClaims checks the version of the worker API an attempt needs: 3
// for a job with a step whose uses: names a workflow, or of a child run,
// and 1 for another; and what a child's attempt's claim has of its tree:
// its root's ID and directory, the call's name, and the strings the step's
// worker masked.
func TestChildClaims(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.CHILD_VAR": {Value: "v"}}
	c := open(t, Options{Variables: vars})
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	byJob(c, map[string]testEnd{"build": {wait: hold}, "main": {wait: hold}})
	root, _ := createTree(t, c, vars)
	plain, _ := create(t, c, oneJob, nil, nil)
	serve(t, c)
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			_, err := tx.LatestAttempt(root, "build")
			return err
		}) == nil && storedRun(t, c, root).StartState != nil
	})
	a := latestAttempt(t, c, root, "build")
	id, _, err := c.CreateChild(t.Context(), ChildRequest{Attempt: a.ID, With: map[string]any{"$yaml": "w.yaml", "region": "east"}, Masked: []string{"s3cr", "t0k3n-0123"}})
	if err != nil || id == "" {
		t.Fatal(id, err)
	}
	waitFor(t, func() bool {
		r := storedRun(t, c, id)
		return r.StartState != nil && c.store.View(t.Context(), func(tx *store.Tx) error {
			_, err := tx.LatestAttempt(id, "main")
			return err
		}) == nil
	})
	readier := claimReadier{c}
	for _, tc := range []struct {
		run, job string
		want     int
	}{{root, "build", workerapi.Version3}, {id, "main", workerapi.Version3}} {
		r := storedRun(t, c, tc.run)
		if got, err := readier.Needs(t.Context(), r, latestAttempt(t, c, tc.run, tc.job)); err != nil || got != tc.want {
			t.Errorf("run %s's job %s needs version %d, %v, want %d", tc.run, tc.job, got, err, tc.want)
		}
	}
	waitFor(t, func() bool {
		return c.store.View(t.Context(), func(tx *store.Tx) error {
			_, err := tx.LatestAttempt(plain, "main")
			return err
		}) == nil
	})
	if got, err := readier.Needs(t.Context(), storedRun(t, c, plain), latestAttempt(t, c, plain, "main")); err != nil || got != 1 {
		t.Errorf("a job that calls no workflow needs version %d, %v", got, err)
	}
	ready, err := readier.ReadyAttempt(t.Context(), storedRun(t, c, id), latestAttempt(t, c, id, "main"))
	if err != nil {
		t.Fatal(err)
	}
	want := workerapi.Tree{JobID: root, RootDir: "/runs/" + root, WorkflowName: "github.t.child", Masked: []string{"s3cr", "t0k3n-0123"}}
	if ready.Tree == nil || ready.Tree.JobID != want.JobID || ready.Tree.RootDir != want.RootDir || ready.Tree.WorkflowName != want.WorkflowName || !slices.Equal(ready.Tree.Masked, want.Masked) {
		t.Errorf("the child's claim has of its tree %+v, want %+v", ready.Tree, want)
	}
	if ready, err := readier.ReadyAttempt(t.Context(), storedRun(t, c, root), a); err != nil || ready.Tree != nil {
		t.Errorf("the root's claim has of its tree %+v, %v", ready.Tree, err)
	}
}
