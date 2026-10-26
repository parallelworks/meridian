package store

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
)

func TestRun(t *testing.T) {
	s := open(t)
	p := testPlan(t)
	var before time.Time
	inTx(t, s, func(tx *Tx) error {
		var err error
		before, err = tx.Now()
		return err
	})
	id := createRun(t, s)
	if id != "1" {
		t.Errorf("the first run is %q, want 1", id)
	}
	if next := createRun(t, s); next != "2" {
		t.Errorf("the second run is %q, want 2", next)
	}
	wantPlan, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := plan.Digest(p)
	if err != nil {
		t.Fatal(err)
	}
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		switch {
		case r.ID != id || r.State != RunCreated || r.Outcome != "" || !r.StartedAt.IsZero() || !r.EndedAt.IsZero():
			t.Errorf("run = %+v", r)
		case !bytes.Equal(r.Plan, wantPlan) || r.PlanVersion != plan.Version || r.PlanDigest != wantDigest:
			t.Errorf("the run's plan is version %d, digest %s, %d bytes; want %d, %s, %d", r.PlanVersion, r.PlanDigest, len(r.Plan), plan.Version, wantDigest, len(wantPlan))
		case r.Name != p.Run.Name || r.Location != "w.yaml" || r.Release != p.Run.Release:
			t.Errorf("the run's name, location and release are %q, %q, %q", r.Name, r.Location, r.Release)
		case r.DefaultGroup != "local" || r.RunDir != "/runs/1" || r.WorkflowID != fakeJobID(0) || r.RecordGeneration != 1:
			t.Errorf("run = %+v", r)
		case r.CreatedAt.Before(before):
			t.Errorf("the run was created at %v, before %v", r.CreatedAt, before)
		}
		if _, err := plan.Decode(r.Plan); err != nil {
			t.Errorf("the stored plan doesn't decode: %v", err)
		}
		return nil
	})

	// The plan never changes.
	err = s.Tx(t.Context(), func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_runs SET plan = x'00' WHERE id = 1")
		return err
	})
	if err == nil {
		t.Error("the run's plan changed")
	}

	for _, bad := range []string{"0", "01", "+1", "x", "", "3"} {
		inTx(t, s, func(tx *Tx) error {
			if _, err := tx.Run(bad); !errors.Is(err, ErrNotFound) {
				t.Errorf("Run(%q) = %v, want ErrNotFound", bad, err)
			}
			return nil
		})
	}
	checkIntegrity(t, s)
}

// TestRunIDs checks that a run's ID is one NewRunID gave, that a
// transaction rolled back gives its number back, and that a number
// committed is never given again.
func TestRunIDs(t *testing.T) {
	s := open(t)
	p := testPlan(t)
	err := s.Tx(t.Context(), func(tx *Tx) error {
		return tx.InsertRun(NewRun{ID: "1", Plan: p, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)})
	})
	if err == nil {
		t.Error("InsertRun stored a run whose ID NewRunID didn't give")
	}
	_ = s.Tx(t.Context(), func(tx *Tx) error {
		if _, err := tx.NewRunID(); err != nil {
			return err
		}
		return errors.New("rolled back")
	})
	inTx(t, s, func(tx *Tx) error {
		id, err := tx.NewRunID()
		if id != "1" {
			t.Errorf("NewRunID after a rollback = %q (%v), want 1", id, err)
		}
		for _, r := range []NewRun{
			{ID: id, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)},
			{ID: id, Plan: p, RunDir: "/r", WorkflowID: fakeJobID(0)},
			{ID: id, Plan: p, DefaultGroup: "local", WorkflowID: fakeJobID(0)},
			{ID: id, Plan: p, DefaultGroup: "local", RunDir: "/r"},
			{ID: "01", Plan: p, DefaultGroup: "local", RunDir: "/r", WorkflowID: fakeJobID(0)},
		} {
			if err := tx.InsertRun(r); err == nil {
				t.Errorf("InsertRun(%+v) succeeded", r)
			}
		}
		return nil
	})
	// The number 1 was committed, though no run has it.
	if id := createRun(t, s); id != "2" {
		t.Errorf("the run after 1 was given is %s, want 2", id)
	}
}

func TestRunLifecycle(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error { return tx.StartRun(id) })
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		if r.State != RunRunning || r.StartedAt.IsZero() {
			t.Errorf("a started run is %s, started at %v", r.State, r.StartedAt)
		}
		if err := tx.StartRun(id); !errors.Is(err, ErrState) {
			t.Errorf("StartRun again = %v, want ErrState", err)
		}
		if err := tx.StartRun("9"); !errors.Is(err, ErrNotFound) {
			t.Errorf("StartRun of no run = %v, want ErrNotFound", err)
		}
		if err := tx.EndRun(id, plan.OutcomeSkipped, ""); err == nil {
			t.Error("EndRun skipped succeeded")
		}
		// A run with an attempt that hasn't ended doesn't end.
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		if err := tx.EndRun(id, plan.OutcomeFailure, plan.ReasonTimeout); !errors.Is(err, ErrState) {
			t.Errorf("EndRun with an attempt that hasn't ended = %v, want ErrState", err)
		}
		return tx.EndAttempt(a.ID, AttemptEnd{Kind: EndWithdrawn})
	})
	inTx(t, s, func(tx *Tx) error { return tx.EndRun(id, plan.OutcomeFailure, plan.ReasonTimeout) })
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		if r.State != RunEnded || r.Outcome != plan.OutcomeFailure || r.Reason != plan.ReasonTimeout || r.EndedAt.IsZero() {
			t.Errorf("an ended run is %+v", r)
		}
		// The record is deleted with the run's end, and the run takes no
		// other.
		if _, err := tx.Record(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Record of an ended run = %v, want ErrNotFound", err)
		}
		if err := tx.InsertRecord(id, 1, []byte("k"), []byte("s")); !errors.Is(err, ErrState) {
			t.Errorf("InsertRecord of an ended run = %v, want ErrState", err)
		}
		if err := tx.EndRun(id, plan.OutcomeSuccess, ""); !errors.Is(err, ErrState) {
			t.Errorf("EndRun again = %v, want ErrState", err)
		}
		if err := tx.InsertJob(NewJob{Run: id, Name: "build", Key: "build#0", SupervisorID: fakeJobID(9)}); !errors.Is(err, ErrState) {
			t.Errorf("InsertJob of an ended run = %v, want ErrState", err)
		}
		if _, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10}); !errors.Is(err, ErrState) {
			t.Errorf("NewAttempt of an ended run = %v, want ErrState", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}

// TestRecord checks that a run's secret record and its generation, which
// the run's row keeps, change together, or neither does.
func TestRecord(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	check := func(gen uint64, sealed string) {
		t.Helper()
		inTx(t, s, func(tx *Tx) error {
			rec, err := tx.Record(id)
			if err != nil {
				return err
			}
			r, err := tx.Run(id)
			if err != nil {
				return err
			}
			if rec.Generation != gen || r.RecordGeneration != gen || string(rec.Sealed) != sealed || string(rec.WrappedKey) != "wrapped "+id {
				t.Errorf("the record is generation %d (the run says %d), %q under %q; want %d, %q", rec.Generation, r.RecordGeneration, rec.Sealed, rec.WrappedKey, gen, sealed)
			}
			if rec.Run != id || rec.UpdatedAt.IsZero() {
				t.Errorf("record = %+v", rec)
			}
			return nil
		})
	}
	check(1, "sealed 1 1")

	inTx(t, s, func(tx *Tx) error { return tx.Reseal(id, 2, []byte("sealed 1 2")) })
	check(2, "sealed 1 2")

	// A generation that isn't the next changes neither.
	for _, gen := range []uint64{2, 4, 1, 0} {
		inTx(t, s, func(tx *Tx) error {
			if err := tx.Reseal(id, gen, []byte("sealed 1 x")); !errors.Is(err, ErrState) {
				t.Errorf("Reseal with generation %d = %v, want ErrState", gen, err)
			}
			return nil
		})
	}
	check(2, "sealed 1 2")

	// A reseal rolled back changes neither.
	_ = s.Tx(t.Context(), func(tx *Tx) error {
		if err := tx.Reseal(id, 3, []byte("sealed 1 3")); err != nil {
			t.Error(err)
		}
		return errors.New("rolled back")
	})
	check(2, "sealed 1 2")

	inTx(t, s, func(tx *Tx) error {
		if err := tx.InsertRecord(id, 1, []byte("k"), []byte("s")); !errors.Is(err, ErrState) {
			t.Errorf("InsertRecord again = %v, want ErrState", err)
		}
		if err := tx.Reseal("9", 2, []byte("s")); !errors.Is(err, ErrNotFound) {
			t.Errorf("Reseal of no run = %v, want ErrNotFound", err)
		}
		if err := tx.InsertRecord("9", 1, []byte("k"), []byte("s")); !errors.Is(err, ErrNotFound) {
			t.Errorf("InsertRecord of no run = %v, want ErrNotFound", err)
		}
		for _, r := range []struct {
			gen         uint64
			key, sealed string
		}{{2, "k", "s"}, {0, "k", "s"}, {1, "", "s"}, {1, "k", ""}} {
			if err := tx.InsertRecord("9", r.gen, []byte(r.key), []byte(r.sealed)); err == nil || errors.Is(err, ErrNotFound) {
				t.Errorf("InsertRecord(%+v) = %v, want the caller's mistake", r, err)
			}
		}
		return nil
	})

	// The data key never changes.
	err := s.Tx(t.Context(), func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_records SET wrapped_key = x'00'")
		return err
	})
	if err == nil {
		t.Error("the record's data key changed")
	}
	check(2, "sealed 1 2")
}
