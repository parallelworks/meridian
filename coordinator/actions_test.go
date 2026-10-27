package coordinator

import (
	"errors"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// finishedStep returns the finished event of the step's try of the latest
// attempt of the run's job key.
func finishedStep(t *testing.T, c *Coordinator, id, key string, step, try int) workerapi.Event {
	t.Helper()
	a := latestAttempt(t, c, id, key)
	var events []workerapi.Event
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		events, err = tx.Events(a.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(events, func(e workerapi.Event) bool {
		return e.Finished && !e.Cleanup && e.Step == step && e.Try == try
	})
	if i < 0 {
		t.Fatalf("job %s has no finished event of step %d's try %d", key, step, try)
	}
	return events[i]
}

// TestActionsNotPerformedYet checks that a step that uses an action the
// coordinator performs, which it doesn't perform yet, asks the
// coordinator for it, which fails it, as the worker failed it before it
// asked: a failure that the step's if: failure() after it sees.
func TestActionsNotPerformedYet(t *testing.T) {
	t.Parallel()
	r := newWorkerRig(t, nil)
	id := r.create(t, []byte(`
jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        with:
          name: desktop
      - if: ${{ failure() }}
        run: echo failed
`), nil, nil)
	serve(t, r.c)
	run := waitEnded(t, r.c, id)
	if run.Outcome != plan.OutcomeFailure {
		t.Errorf("the run ended %s", run.Outcome)
	}
	e := finishedStep(t, r.c, id, "main", 0, 0)
	want := problems.UsesNotSupported.At("", problems.Position{}, "/jobs/main/steps/0", problems.Params{"uses": "parallelworks/update-session"})
	if e.Outcome != plan.OutcomeFailure || len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != want.Code || e.Diagnostics[0].Pointer != want.Pointer {
		t.Errorf("the step ended %s, with %+v", e.Outcome, e.Diagnostics)
	}
	j := storedJob(t, r.c, id, "main")
	logs := r.logs.Of(id, "main", j.Attempts)
	if got := string(logs[workerapitest.LogKey{Part: workerapi.LogStep, Step: 0}].Data); got != want.Message()+"\n" {
		t.Errorf("the step's log is %q", got)
	}
	if got := string(logs[workerapitest.LogKey{Part: workerapi.LogStep, Step: 1}].Data); got != "failed\n" {
		t.Errorf("the step after's log is %q", got)
	}

	// A step that runs a command, or that the job doesn't have, uses no
	// action the coordinator performs, as the worker never asks.
	a := latestAttempt(t, r.c, id, "main")
	for _, step := range []int{1, 2} {
		call := &workerapisrv.ActionCall{Run: run, Attempt: a, Request: workerapi.ActionRequest{Step: step}}
		if _, err := (actions{r.c}).Act(t.Context(), call); !errors.Is(err, workerapisrv.ErrNotAction) {
			t.Errorf("an action of step %d: %v", step, err)
		}
	}
}
