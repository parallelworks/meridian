package coordinator

import (
	"errors"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
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

// TestNotAnAction checks that a step that runs a command, or that its job
// doesn't have, uses no action the coordinator performs, as the worker
// never asks for one: Act refuses it (workerapisrv.ErrNotAction).
func TestNotAnAction(t *testing.T) {
	t.Parallel()
	r := newWorkerRig(t, nil)
	id := r.create(t, []byte(`
jobs:
  main:
    steps:
      - run: "true"
      - run: "true"
`), nil, nil)
	serve(t, r.c)
	run := waitEnded(t, r.c, id)
	if run.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended %s", run.Outcome)
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
