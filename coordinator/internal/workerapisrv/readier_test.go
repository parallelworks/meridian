package workerapisrv_test

import (
	"context"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
)

// PlanReadier readies an attempt's job from the plan, with the run's
// values and its needs' results each joined from run state and the secret
// record, a need's from its job's part of the record, and gives the job
// only the part of them it reads; and says how many steps the job has.
func TestPlanReadier(t *testing.T) {
	r := newRig(t, rigOptions{})
	rec := created(t).Record
	rec.Run = &plan.RunValues{Format: 1, Vars: map[string]string{"TOKEN": "t0k3n"}, Secrets: []string{"vars.TOKEN"}}
	// What the record itself holds of a job gives way to the job's part.
	rec.Jobs = map[string]plan.JobRecord{"prep": {Outputs: map[string]string{"x": "record-x"}}}
	run := r.newRun(&rec)
	r.inTx(func(tx *store.Tx) error {
		key, err := tx.WrappedKey(run)
		if err != nil {
			return err
		}
		part, err := r.sealer.SealJob(t.Context(), run, "prep", 1, key, plan.JobRecord{Outputs: map[string]string{"x": "private-x"}})
		if err != nil {
			return err
		}
		if err := tx.PutJobRecord(run, "prep", 1, part); err != nil {
			return err
		}
		return tx.EndJob(run, "prep", store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSuccess, Private: []string{"x"}}})
	})
	a, _ := r.newAttempt(run, "build", placed{})

	var readiedWith []string
	pr := &workerapisrv.PlanReadier{
		Store: r.store, Sealer: r.sealer,
		Plan: func(run *store.Run) (*plan.Plan, error) { return plan.Decode(run.Plan) },
		RunValues: func(*store.Run) (plan.RunValues, error) {
			return plan.RunValues{Format: 1, Inputs: plan.DataOf(map[string]any{"note": "hello"})}, nil
		},
		Readied: func(_ context.Context, _ *store.Tx, _ *store.Run, job *store.Job) (*workerapisrv.Readied, error) {
			readiedWith = append(readiedWith, job.Key)
			return &workerapisrv.Readied{}, nil
		},
	}
	var stored *store.Run
	r.inTx(func(tx *store.Tx) error {
		var err error
		stored, err = tx.Run(run)
		return err
	})
	got, err := pr.ReadyAttempt(t.Context(), stored, a)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case got.Job.Key != "build" || len(got.Job.Steps) != 3:
		t.Errorf("the job readied is %s, of %d steps", got.Job.Key, len(got.Job.Steps))
	case !slices.Equal(readiedWith, []string{"build"}):
		t.Errorf("the job was readied with what was kept of %v", readiedWith)
	}
	inputs, _ := got.Run.Inputs.Get().(map[string]any)
	if inputs["note"] != "hello" || len(got.Run.Vars) != 0 || len(got.Run.Secrets) != 0 {
		t.Errorf("the run's values the job reads are %+v", got.Run)
	}
	prep := got.Needs["prep"]
	if prep.Outcome != plan.OutcomeSuccess || prep.Outputs["x"] != "private-x" || !slices.Equal(prep.Private, []string{"x"}) {
		t.Errorf("the job reads %+v of prep", prep)
	}
	again, err := pr.ReadyAttempt(t.Context(), stored, a)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := plan.EncodeReadyJob(got.Job)
	second, _ := plan.EncodeReadyJob(again.Job)
	if string(first) != string(second) {
		t.Errorf("the job readies otherwise again:\n%s\n%s", first, second)
	}
	if steps, err := pr.Steps(t.Context(), stored, a); err != nil || steps != 3 {
		t.Errorf("Steps = %d, %v; want 3", steps, err)
	}

	// A need on what hasn't ended can't be readied.
	r2 := r.newRun(nil)
	b, _ := r.newAttempt(r2, "build", placed{})
	r.inTx(func(tx *store.Tx) error {
		var err error
		stored, err = tx.Run(r2)
		return err
	})
	if _, err := pr.ReadyAttempt(t.Context(), stored, b); err == nil {
		t.Error("a job whose need hasn't ended was readied")
	}
}
