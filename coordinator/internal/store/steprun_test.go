package store

import (
	"bytes"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// TestStepRun checks a step run: a child run that names its caller's plan
// rather than keep one of its own, which the store reads from the caller,
// and which it refuses without a link, or with another plan than the
// caller's.
func TestStepRun(t *testing.T) {
	t.Parallel()
	s := open(t)
	root := createRun(t, s)
	var attempt *Attempt
	inTx(t, s, func(tx *Tx) error {
		if err := tx.StartRun(root, testState(), nil); err != nil {
			return err
		}
		var err error
		attempt, _, err = tx.NewAttempt(NewAttempt{Run: root, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	p := testPlan(t)
	link := Link{Run: root, Job: "build", Attempt: attempt.ID, Step: 1, Try: 0, Root: root, Depth: 1}
	insert := func(r NewRun) (string, error) {
		var id string
		err := s.Tx(t.Context(), func(tx *Tx) error {
			var err error
			if id, err = tx.NewRunID(); err != nil {
				return err
			}
			r.ID = id
			return tx.InsertRun(r)
		})
		return id, err
	}
	run := NewRun{Plan: p, StepRun: true, DefaultGroup: "local", RunDir: "/runs/1", WorkflowID: fakeJobID(10), FinisherID: fakeJobID(11), Parent: &link}
	id, err := insert(run)
	if err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := s.db.QueryRowContext(t.Context(), "SELECT plan FROM meridian_runs WHERE id = ?", mustNumber(t, id)).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 0 {
		t.Errorf("the step run keeps a plan of its own, of %d bytes", len(stored))
	}
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		callers, err := tx.Run(root)
		if err != nil {
			return err
		}
		if !r.StepRun || r.Parent == nil || *r.Parent != link || !bytes.Equal(r.Plan, callers.Plan) || r.PlanDigest != callers.PlanDigest {
			t.Errorf("the step run is %+v, with a plan of %d bytes", r, len(r.Plan))
		}
		if callers.StepRun {
			t.Error("its caller is a step run")
		}
		return nil
	})

	// Without a link, there is no caller to name the plan of.
	unlinked := run
	unlinked.Parent = nil
	if _, err := insert(unlinked); err == nil {
		t.Error("a step run without a link was stored")
	}
	// Another plan than its caller's.
	other, diags, err := compile.Create(t.Context(), "w.yaml", []byte("jobs:\n  other:\n    steps:\n      - run: echo other\n"), nil, compile.Options{Source: plan.Source{Location: "w.yaml"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	elsewhere := run
	elsewhere.Plan = other.Plan
	second := link
	second.Try = 1
	elsewhere.Parent = &second
	if _, err := insert(elsewhere); err == nil {
		t.Error("a step run of another plan than its caller's was stored")
	}
}

// mustNumber returns the number of the run id, as the store keys it.
func mustNumber(t *testing.T, id string) int64 {
	t.Helper()
	n, ok := runNumber(id)
	if !ok {
		t.Fatalf("%q isn't a run's ID", id)
	}
	return n
}
