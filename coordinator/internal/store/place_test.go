package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// TestPinReady checks that what a job is first readied with is pinned:
// later calls give it again, whatever they are given.
func TestPinReady(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.PinReady(id, "test", true, map[string]string{"build": "build#2"})
		if err != nil {
			return err
		}
		if j.ReadyAt.IsZero() || !j.ReadiedStopped || !reflect.DeepEqual(j.AnyChoices, map[string]string{"build": "build#2"}) {
			t.Errorf("a job readied is %+v", j)
		}
		again, err := tx.PinReady(id, "test", false, nil)
		if err != nil {
			return err
		}
		if !again.ReadyAt.Equal(j.ReadyAt) || !again.ReadiedStopped || again.AnyChoices["build"] != "build#2" {
			t.Errorf("readied again, the job is %+v", again)
		}
		if j, err := tx.PinReady(id, "build", false, nil); err != nil || j.ReadiedStopped || j.AnyChoices != nil {
			t.Errorf("PinReady = %+v, %v", j, err)
		}
		if _, err := tx.PinReady(id, "deploy", false, nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("PinReady of no job = %v, want ErrNotFound", err)
		}
		return tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSkipped}})
	})
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.PinReady(id, "build", false, nil); !errors.Is(err, ErrState) {
			t.Errorf("PinReady of an ended job = %v, want ErrState", err)
		}
		return nil
	})
}

// TestWake checks that waking a job counts, and gives its supervisor.
func TestWake(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		for i := range 2 {
			j, err := tx.Wake(id, "build")
			if err != nil || j.SupervisorID != fakeJobID(1) || j.Wake != int64(i+1) {
				t.Errorf("Wake = %+v, %v", j, err)
			}
		}
		if _, err := tx.Wake(id, "deploy"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Wake of no job = %v, want ErrNotFound", err)
		}
		if _, err := tx.Wake("x", "build"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Wake of no run = %v, want ErrNotFound", err)
		}
		return nil
	})
}

// testPlace is a place in the clear.
var testPlace = Place{Group: "g", Member: "m", Backend: "local"}

// TestChoosePlace checks that an attempt's place is recorded before its
// backend is asked, once, in the clear or sealed, and that it starts its
// job's tries at placing again.
func TestChoosePlace(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		if tries, err := tx.PlaceTried(id, "build"); err != nil || tries != 1 {
			t.Errorf("PlaceTried = %d, %v", tries, err)
		}
		if tries, err := tx.PlaceTried(id, "build"); err != nil || tries != 2 {
			t.Errorf("PlaceTried again = %d, %v", tries, err)
		}
		a, token, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if _, chosen, err := tx.ChosenPlace(a.ID); err != nil || chosen {
			t.Errorf("before ChoosePlace, ChosenPlace = %v, %v", chosen, err)
		}
		for _, p := range []Place{
			{},
			{Group: "g"},
			{Group: "g", Member: "m", Backend: "local", Private: []byte("sealed")},
			{Member: "m", Private: []byte("sealed")},
		} {
			if err := tx.ChoosePlace(a.ID, p); err == nil {
				t.Errorf("ChoosePlace(%+v) succeeded", p)
			}
		}
		if err := tx.ChoosePlace(a.ID, testPlace); err != nil {
			return err
		}
		if err := tx.ChoosePlace(a.ID, testPlace); !errors.Is(err, ErrState) {
			t.Errorf("ChoosePlace again = %v, want ErrState", err)
		}
		if p, chosen, err := tx.ChosenPlace(a.ID); err != nil || !chosen || !reflect.DeepEqual(p, testPlace) {
			t.Errorf("ChosenPlace = %+v, %v, %v", p, chosen, err)
		}
		if j, err := tx.Job(id, "build"); err != nil || j.PlaceTries != 0 {
			t.Errorf("once a place is chosen, the job is %+v, %v", j, err)
		}
		// The placement, once recorded, is where the place said.
		if err := tx.RecordPlacement(a.ID, Placement{Group: "g", Member: "m", Backend: "local", ID: "1:2"}); err != nil {
			return err
		}
		if p, _, err := tx.ChosenPlace(a.ID); err != nil || !reflect.DeepEqual(p, testPlace) {
			t.Errorf("once placed, ChosenPlace = %+v, %v", p, err)
		}
		if _, err := tx.Claim(token, credential('a'), testWorker); err != nil {
			return err
		}
		b, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		// A private place is sealed whole.
		private := Place{Private: []byte("sealed place")}
		if err := tx.ChoosePlace(b.ID, private); err != nil {
			return err
		}
		if p, chosen, err := tx.ChosenPlace(b.ID); err != nil || !chosen || !reflect.DeepEqual(p, private) {
			t.Errorf("a private place is %+v, %v, %v", p, chosen, err)
		}
		got, err := tx.Attempt(b.ID)
		if err != nil {
			return err
		}
		if got.Placement.Group != "" || got.Placement.Member != "" || got.Placement.Backend != "" {
			t.Errorf("a private place is in the clear: %+v", got.Placement)
		}
		// A new attempt supersedes b, which no longer chooses.
		c, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.ChoosePlace(b.ID, testPlace); !errors.Is(err, ErrFenced) {
			t.Errorf("ChoosePlace of a superseded attempt = %v, want ErrFenced", err)
		}
		if err := tx.ChoosePlace(c.ID, testPlace); err != nil {
			return err
		}
		if _, _, err := tx.ChosenPlace("none"); !errors.Is(err, ErrNotFound) {
			t.Errorf("ChosenPlace of no attempt = %v, want ErrNotFound", err)
		}
		return nil
	})
	inTx(t, s, func(tx *Tx) error {
		// An attempt claimed chooses no place.
		a, err := tx.LatestAttempt(id, "build")
		if err != nil {
			return err
		}
		if err := tx.ChoosePlace(a.ID, testPlace); !errors.Is(err, ErrState) {
			t.Errorf("ChoosePlace of a claimed attempt = %v, want ErrState", err)
		}
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndWithdrawn}); err != nil {
			return err
		}
		if err := tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}}); err != nil {
			return err
		}
		if _, err := tx.PlaceTried(id, "build"); !errors.Is(err, ErrState) {
			t.Errorf("PlaceTried of an ended job = %v, want ErrState", err)
		}
		if _, err := tx.PlaceTried(id, "deploy"); !errors.Is(err, ErrNotFound) {
			t.Errorf("PlaceTried of no job = %v, want ErrNotFound", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}

// TestSetupFailures checks that a job's setup failures are given in the
// order of its attempts, with their problems, and its latest attempt.
func TestSetupFailures(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	kinds := []*problems.Kind{problems.IsolationNotBuilt, problems.RunDirUnusable}
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.LatestAttempt(id, "build"); !errors.Is(err, ErrNotFound) {
			t.Errorf("LatestAttempt of a job without one = %v, want ErrNotFound", err)
		}
		for _, k := range kinds {
			a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
			if err != nil {
				return err
			}
			p := k.Problem(problems.Params{"member": "m", "isolation": "podman", "has_isolation": true, "path": "/r", "reason": "denied"})
			if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: p}); err != nil {
				return err
			}
		}
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndWithdrawn}); err != nil {
			return err
		}
		failures, err := tx.SetupFailures(id, "build")
		if err != nil {
			return err
		}
		if len(failures) != 2 || failures[0].End.Problem.Code != kinds[0].Code() || failures[1].End.Problem.Code != kinds[1].Code() ||
			failures[0].Number != 1 || failures[1].Number != 2 {
			t.Errorf("the job's setup failures are %v", failures)
		}
		if latest, err := tx.LatestAttempt(id, "build"); err != nil || latest.Number != 3 || latest.End.Kind != EndWithdrawn {
			t.Errorf("LatestAttempt = %+v, %v", latest, err)
		}
		if failures, err := tx.SetupFailures(id, "test"); err != nil || len(failures) != 0 {
			t.Errorf("SetupFailures of a job without any = %v, %v", failures, err)
		}
		return nil
	})
}

// TestPlaceFailed checks that a job that couldn't be placed counts it as a
// failure before a step started: of its attempt that hasn't ended, or of
// one made for the record; and starts its tries at placing again.
func TestPlaceFailed(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	p := problems.PlacementUnanswered.Problem(problems.Params{"job": "build", "tries": 10})
	n := NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10}
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.PlaceTried(id, "build"); err != nil {
			return err
		}
		a, err := tx.PlaceFailed(n, p)
		if err != nil {
			return err
		}
		if a.Number != 1 || a.End.Kind != EndSetupFailed || a.End.Reason != plan.ReasonSetupFailed || a.End.Problem.Code != p.Code {
			t.Errorf("an attempt made for the record is %+v", a)
		}
		if j, err := tx.Job(id, "build"); err != nil || j.PlaceTries != 0 {
			t.Errorf("once placing failed, the job is %+v, %v", j, err)
		}
		b, _, err := tx.NewAttempt(n)
		if err != nil {
			return err
		}
		if a, err := tx.PlaceFailed(n, p); err != nil || a.ID != b.ID || a.End.Kind != EndSetupFailed {
			t.Errorf("PlaceFailed of an attempt being placed = %+v, %v", a, err)
		}
		_, token, err := tx.NewAttempt(n)
		if err != nil {
			return err
		}
		if _, err := tx.Claim(token, credential('p'), testWorker); err != nil {
			return err
		}
		if _, err := tx.PlaceFailed(n, p); !errors.Is(err, ErrState) {
			t.Errorf("PlaceFailed of a claimed attempt = %v, want ErrState", err)
		}
		return nil
	})
}

// TestJobDiagnostics checks that a job's problems are kept with how it
// ended, and read only as the store writes them.
func TestJobDiagnostics(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	d := problems.GroupUnknown.At("", problems.Position{}, "/jobs/build", problems.Params{"group": "gpu"})
	inTx(t, s, func(tx *Tx) error {
		return tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: problems.Diagnostics{d}})
	})
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job(id, "build")
		if err != nil || len(j.Diagnostics) != 1 || j.Diagnostics[0].Code != d.Code || j.Diagnostics[0].Params["group"] != "gpu" {
			t.Errorf("the job is %+v, %v", j, err)
		}
		return nil
	})
	for _, set := range []string{
		`diagnostics = CAST('[{"type":"x"}]' AS BLOB)`,
		`diagnostics = NULL, any_choices = CAST('{"build":1}' AS BLOB)`,
		`diagnostics = NULL, any_choices = CAST('not JSON' AS BLOB)`,
	} {
		inTx(t, s, func(tx *Tx) error {
			_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_jobs SET "+set+" WHERE key = 'build'")
			return err
		})
		inTx(t, s, func(tx *Tx) error {
			if _, err := tx.Job(id, "build"); !errors.Is(err, ErrMalformed) {
				t.Errorf("Job with %s = %v, want ErrMalformed", set, err)
			}
			return nil
		})
	}
}
