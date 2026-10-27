package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// TestStatus checks a run's status through its life: created, then ended
// with its outcome, and a run the store doesn't have.
func TestStatus(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	if len(c.StoreID()) == 0 {
		t.Error("the store has no ID")
	}
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	s, err := c.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if s.ID != id || s.State != RunCreated || s.RunDir != "/runs/"+id || s.Outcome != "" {
		t.Errorf("a run created is %+v", s)
	}
	serve(t, c)
	waitEnded(t, c, id)
	if s, err = c.Status(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if s.State != RunEnded || s.Outcome != plan.OutcomeSuccess || len(s.Diagnostics) != 0 {
		t.Errorf("a run ended is %+v", s)
	}
	for _, missing := range []string{"2", "x", ""} {
		if _, err := c.Status(t.Context(), missing); !errors.Is(err, ErrRunNotFound) {
			t.Errorf("Status(%q) = %v, want ErrRunNotFound", missing, err)
		}
	}
}

// TestFailRun checks that a run that hasn't started can be ended failed,
// with a problem, as one whose directory couldn't be made is, and that no
// other can.
func TestFailRun(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	p := problems.RunDirExists.Problem(problems.Params{"path": "/runs/" + id})
	if err := c.FailRun(t.Context(), id, p); err != nil {
		t.Fatal(err)
	}
	s, err := c.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if s.State != RunEnded || s.Outcome != plan.OutcomeFailure || len(s.Diagnostics) != 1 || s.Diagnostics[0].Code != p.Code {
		t.Errorf("a run failed is %+v", s)
	}
	if rec := storedRecord(t, c, id); rec != nil {
		t.Error("a run failed keeps its secret record")
	}
	if err := c.FailRun(t.Context(), id, p); !errors.Is(err, ErrRunStarted) {
		t.Errorf("FailRun of a run that has ended = %v", err)
	}
	if err := c.FailRun(t.Context(), "9", p); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("FailRun of a run the store hasn't = %v", err)
	}
	// Its Start and finisher, worked, leave it as it is.
	serve(t, c)
	waitEnded(t, c, id)
	if s, err = c.Status(t.Context(), id); err != nil || s.Outcome != plan.OutcomeFailure || len(s.Diagnostics) != 1 {
		t.Errorf("a run failed, then served, is %+v, %v", s, err)
	}
}

// TestHoldEndpoint checks that one coordinator of a store at a time holds
// an endpoint: another waits for it, and takes it once the first releases
// it.
func TestHoldEndpoint(t *testing.T) {
	db := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	first := open(t, Options{Database: db})
	second := open(t, Options{Database: db})
	h, err := first.HoldEndpoint(t.Context(), "socket", "first")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := second.HoldEndpoint(ctx, "socket", "second"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a second hold of an endpoint held: %v", err)
	}
	took := make(chan error, 1)
	go func() {
		h2, err := second.HoldEndpoint(t.Context(), "socket", "second")
		if err == nil {
			err = h2.Release(t.Context())
		}
		took <- err
	}()
	if err := h.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-took:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the endpoint wasn't taken once its holder released it")
	}
}

// TestRunsAndJobs checks the store's runs, the newest first, each with what
// a list shows, and a run's jobs in each state: waiting, running where its
// attempt was placed, and ended.
func TestRunsAndJobs(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	first, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	second, _ := create(t, c, oneJob, nil, nil)
	runs, err := c.Runs(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].ID != second || runs[1].ID != first {
		t.Fatalf("the runs are %+v", runs)
	}
	for _, r := range runs {
		if r.State != RunCreated || r.Location != "workflow.yaml" || r.PlanVersion != plan.Version || r.CreatedAt.IsZero() || r.RunDir != "/runs/"+r.ID {
			t.Errorf("a run listed is %+v", r)
		}
	}
	if runs, err := c.Runs(t.Context(), 1); err != nil || len(runs) != 1 || runs[0].ID != second {
		t.Errorf("the last run is %+v, %v", runs, err)
	}
	jobs, err := c.Jobs(t.Context(), second)
	if err != nil || len(jobs) != 1 || jobs[0].Key != "main" || jobs[0].State != JobWaiting || jobs[0].Attempts != 0 {
		t.Errorf("a run's jobs before it starts are %+v, %v", jobs, err)
	}
	running, _ := claimedAttempt(t, c)
	jobs, err = c.Jobs(t.Context(), running)
	if err != nil || len(jobs) != 1 || jobs[0].State != JobRuns || jobs[0].Attempts != 1 || jobs[0].Member != "local/m1" {
		t.Errorf("a run's job once its attempt is claimed is %+v, %v", jobs, err)
	}
	serve(t, c)
	waitEnded(t, c, first)
	s, err := c.Status(t.Context(), first)
	if err != nil || s.StartedAt.IsZero() || s.EndedAt.IsZero() {
		t.Errorf("a run ended is %+v, %v", s, err)
	}
	jobs, err = c.Jobs(t.Context(), first)
	if err != nil || len(jobs) != 1 || jobs[0].State != JobEnded || jobs[0].Outcome != plan.OutcomeSuccess || jobs[0].Attempts != 1 {
		t.Errorf("a run's job once it ended is %+v, %v", jobs, err)
	}
	if _, err := c.Jobs(t.Context(), "99"); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("Jobs of a run the store hasn't = %v", err)
	}
}

// TestResume checks that a coordinator takes up a run another created,
// which hasn't started, by working its start queue, once the one that
// created it no longer holds its Start, so that it starts and ends there;
// that a check that refuses it leaves it as it is; and that a run of a
// newer plan, or one the store hasn't, is refused.
func TestResume(t *testing.T) {
	db := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	keys := coordtest.NewKeys(t)
	queue := func(id string) string { return "meridian.start." + id }
	creator := open(t, Options{Database: db, Keys: keys, Variables: regionVars, StartQueue: queue})
	id, _ := create(t, creator, regionWorkflow, regionSubmitted, regionVars)
	refused := errors.New("refused")
	resumer := open(t, Options{Database: db, Keys: keys, Variables: regionVars, StartQueue: queue})
	if _, err := resumer.Resume(t.Context(), id, func(s *RunStatus) error {
		if s.ID != id || s.State != RunCreated {
			t.Errorf("Resume checked %+v", s)
		}
		return refused
	}); !errors.Is(err, refused) {
		t.Errorf("Resume with a check that refuses = %v", err)
	}
	worked := func() int {
		resumer.mu.Lock()
		defer resumer.mu.Unlock()
		return len(resumer.queues)
	}
	if worked() != 0 {
		t.Error("a run a check refused had its start queue worked")
	}
	// The coordinator that created the run holds its Start while it
	// lives: Start runs there, not here.
	if s, err := resumer.Resume(t.Context(), id, nil); err != nil || s.State != RunCreated || worked() != 0 {
		t.Errorf("Resume of a run whose creator lives = %+v, %v, with %d start queues worked", s, err, worked())
	}
	// It stops working the queue, as when it ends.
	creator.retireQueue(t.Context(), queue(id))
	s, err := resumer.Resume(t.Context(), id, nil)
	if err != nil || s.ID != id || s.State != RunCreated || worked() != 1 {
		t.Fatalf("Resume = %+v, %v", s, err)
	}
	serve(t, resumer)
	if r := waitEnded(t, resumer, id); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run resumed ended with %s", r.Outcome)
	}
	if _, err := os.Lstat(db + ".start-" + id + ".lock"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the hold on the run's Start is there once it has started: %v", err)
	}
	if s, err := resumer.Resume(t.Context(), id, nil); err != nil || s.State != RunEnded {
		t.Errorf("Resume of a run that has ended = %+v, %v", s, err)
	}

	// A run a newer Meridian created, whose plan this one can't read.
	newer, _ := create(t, creator, oneJob, nil, nil)
	exec(t, creator, "DROP TRIGGER meridian_runs_plan_fixed")
	exec(t, creator, fmt.Sprintf("UPDATE meridian_runs SET plan_version = %d WHERE id = %s", plan.Version+1, newer))
	_, err = resumer.Resume(t.Context(), newer, func(*RunStatus) error {
		t.Error("a run of a newer plan was checked")
		return nil
	})
	kindtest.Want(t, err, problems.RunPlanNewer)
	if _, err := resumer.Resume(t.Context(), "99", nil); !errors.Is(err, ErrRunNotFound) {
		t.Errorf("Resume of a run the store hasn't = %v", err)
	}
}

// TestResumeLetsStaleHoldGo checks that Resume of a run that has started
// removes the file of a hold on its Start that a coordinator that ended
// without letting it go left, as one killed does.
func TestResumeLetsStaleHoldGo(t *testing.T) {
	db := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	queue := func(id string) string { return "meridian.start." + id }
	c := open(t, Options{Database: db, Variables: regionVars, StartQueue: queue})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	serve(t, c)
	waitEnded(t, c, id)
	stale := db + ".start-" + id + ".lock"
	if err := os.WriteFile(stale, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if s, err := c.Resume(t.Context(), id, nil); err != nil || s.State != RunEnded {
		t.Fatalf("Resume = %+v, %v", s, err)
	}
	if _, err := os.Lstat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the stale hold's file is there: %v", err)
	}
}

// TestReadOnly checks a coordinator opened only to read the store, without
// Keys: it reports runs, and refuses every call that would change one, or
// serve.
func TestReadOnly(t *testing.T) {
	db := filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	c := open(t, Options{Database: db})
	ended := runToEnd(t, c, oneJob, nil)
	// A run whose Start is in a queue of its own, which its creator, which
	// doesn't serve, works alone.
	creator := open(t, Options{Database: db, Keys: c.opts.Keys, StartQueue: func(id string) string { return "meridian.start." + id }})
	created, _ := create(t, creator, oneJob, nil, nil)
	r, err := Open(t.Context(), Options{Database: db, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if s, err := r.Status(t.Context(), ended.ID); err != nil || s.Outcome != plan.OutcomeSuccess {
		t.Errorf("Status read-only = %+v, %v", s, err)
	}
	if runs, err := r.Runs(t.Context(), 5); err != nil || len(runs) != 2 {
		t.Errorf("Runs read-only = %+v, %v", runs, err)
	}
	if jobs, err := r.Jobs(t.Context(), ended.ID); err != nil || len(jobs) != 1 || jobs[0].State != JobEnded {
		t.Errorf("Jobs read-only = %+v, %v", jobs, err)
	}
	if s, err := r.Follow(t.Context(), ended.ID, FollowOptions{Event: func(Event) error { return nil }}); err != nil || s.State != RunEnded {
		t.Errorf("Follow read-only = %+v, %v", s, err)
	}
	for name, err := range map[string]error{
		"Serve":        r.Serve(t.Context()),
		"ServeWorkers": r.ServeWorkers(t.Context(), nil, nil),
		"Resume":       func() error { _, err := r.Resume(t.Context(), created, nil); return err }(),
		"FailRun":      r.FailRun(t.Context(), created, problems.RunDirExists.Problem(problems.Params{"path": "/r"})),
		"HoldEndpoint": func() error { _, err := r.HoldEndpoint(t.Context(), "socket", "test"); return err }(),
		"CreateRun":    func() error { _, err := r.CreateRun(t.Context(), nil, RunOptions{}); return err }(),
		"Cancel":       r.Cancel(t.Context(), created),
	} {
		if !errors.Is(err, ErrReadOnly) {
			t.Errorf("%s read-only = %v, want ErrReadOnly", name, err)
		}
	}
	if s, err := c.Status(t.Context(), created); err != nil || s.State != RunCreated {
		t.Errorf("the run created is %+v, %v", s, err)
	}
}
