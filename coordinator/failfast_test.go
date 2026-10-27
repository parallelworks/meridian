package coordinator

import (
	"slices"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestFailFastMissed checks an instance whose attempt was made as its
// matrix job was marked, as by a supervisor that looked before: once its
// worker claimed it, its control is made to ask it to cancel at the next
// look, and the worker API hears of it.
func TestFailFastMissed(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	notifier := &countNotifier{}
	c.controls = notifier
	id, _ := create(t, c, `
jobs:
  main:
    strategy:
      matrix:
        n: [0, 1]
    steps:
      - run: echo ${{ matrix.n }}
`, nil, nil)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.supervise(t.Context(), id, "main", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		a, token, err := tx.NewAttempt(c.newAttempt(id, "main#1", false))
		if err != nil {
			return err
		}
		if err := tx.RecordPlacement(a.ID, store.Placement{Backend: testBackend, ID: "test-" + a.ID, WorkerDigest: testDigest}); err != nil {
			return err
		}
		_, err = tx.Claim(token, "1111111111111111111111111111111111111111111111111111111111111111", workerapi.Worker{Version: "test", Digest: testDigest})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The mark, as it lands after the attempt was made without it.
	exec(t, c, "UPDATE meridian_jobs SET fail_fast_at = 1 WHERE key = 'main'")
	a := latestAttempt(t, c, id, "main#1")
	// It is told, and then steps as any job does, waiting on its attempt.
	if _, handled, err := c.instanceStep(t.Context(), storedRun(t, c, id), storedJob(t, c, id, "main#1"), a, nil); err != nil || handled {
		t.Fatalf("instanceStep = %v, %v", handled, err)
	}
	var ctl workerapi.Control
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		ctl, err = tx.Control(a.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !ctl.Cancel || ctl.Reason != plan.ReasonFailFast || notifier.n.Load() != 1 {
		t.Errorf("the claimed instance's worker is told %+v, and the worker API heard %d", ctl, notifier.n.Load())
	}
}

// TestFailFastAfterSteps checks that an instance that failed before its
// first step, as one whose setup failed, doesn't stop the rest, as format
// 1's doesn't.
func TestFailFastAfterSteps(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	release := make(chan struct{})
	byJob(c, map[string]testEnd{
		"main#0": {problem: setupProblems[0]},
		"main#1": {result: plan.JobResult{Outcome: plan.OutcomeSuccess}, wait: release},
	})
	id, _ := create(t, c, `
jobs:
  main:
    strategy:
      matrix:
        n: [0, 1]
    steps:
      - run: echo ${{ matrix.n }}
`, nil, nil)
	serve(t, c)
	waitFor(t, func() bool {
		instances := instancesOf(t, c, id, "main")
		return len(instances) == 2 && instances[0].Outcome != ""
	})
	close(release)
	r := waitEnded(t, c, id)
	first, second := storedJob(t, c, id, "main#0"), storedJob(t, c, id, "main#1")
	if r.Outcome != plan.OutcomeFailure || first.Outcome != plan.OutcomeFailure || first.AttemptsStarted != 0 || second.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s; the first instance with %s, and the second with %s", r.Outcome, first.Outcome, second.Outcome)
	}
	if j := storedJob(t, c, id, "main"); !j.FailFastAt.IsZero() {
		t.Error("an instance that failed before its first step stopped the rest")
	}
}

// TestAnyOutputs checks what a job that needs a matrix job on any instance
// reads of its outputs: every instance's that had ended as one was chosen,
// an output more than one set the highest index's, as format 1 merges
// them, with the chosen instance's outcome.
func TestAnyOutputs(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	firstEnded, release := make(chan struct{}), make(chan struct{})
	byJob(c, map[string]testEnd{
		"build#0": {result: plan.JobResult{Outcome: plan.OutcomeFailure, Outputs: map[string]string{"x": "0", "y": "from 0"}}},
		"build#1": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1"}}, wait: firstEnded},
		"build#2": {result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "2"}}, wait: release},
	})
	id, _ := create(t, c, `
jobs:
  build:
    strategy:
      fail-fast: false
      matrix:
        n: [0, 1, 2]
    steps:
      - id: s
        run: echo x=${{ matrix.n }} >> $OUTPUTS
    outputs:
      x: ${{ steps.s.outputs.x }}
      y: ${{ steps.s.outputs.y }}
  deploy:
    needs: [build:any]
    if: ${{ needs.build.outputs.x == '1' && needs.build.outputs.y == 'from 0' }}
    steps:
      - run: echo deploy
`, nil, nil)
	serve(t, c)
	waitFor(t, func() bool {
		instances := instancesOf(t, c, id, "build")
		return len(instances) == 3 && instances[0].Outcome != ""
	})
	close(firstEnded)
	waitFor(t, func() bool { return slices.Contains(placedJobs(c), "deploy") })
	if j := storedJob(t, c, id, "deploy"); j.AnyChoices["build"] != "build#1" {
		t.Errorf("deploy was readied with %v", j.AnyChoices)
	}
	close(release)
	r := waitEnded(t, c, id)
	if j := storedJob(t, c, r.ID, "deploy"); j.Outcome != plan.OutcomeSuccess {
		t.Errorf("deploy ended with %s", j.Outcome)
	}
}
