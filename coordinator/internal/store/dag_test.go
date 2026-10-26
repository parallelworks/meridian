package store

import (
	"errors"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestMarkJobFailed checks that the first job of a run to fail marks the
// run, once, and has each of its attempts that hasn't ended tell its worker
// so, with what its control asked already; and that an attempt made after
// it is told so from the start.
func TestMarkJobFailed(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	n := func(job string) NewAttempt {
		return NewAttempt{Run: id, Job: job, LeaseSeconds: 60, HeartbeatSeconds: 10}
	}
	var before, placing, claimed *Attempt
	inTx(t, s, func(tx *Tx) error {
		var err error
		if before, _, err = tx.NewAttempt(n("build")); err != nil {
			return err
		}
		if err := tx.StartControl(before.ID); err != nil {
			return err
		}
		// An attempt that ended is told nothing more.
		if err := tx.EndAttempt(before.ID, AttemptEnd{Kind: EndWithdrawn}); err != nil {
			return err
		}
		if placing, _, err = tx.NewAttempt(n("build")); err != nil {
			return err
		}
		if _, err := tx.SetControl(placing.ID, workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled}); err != nil {
			return err
		}
		var token string
		if claimed, token, err = tx.NewAttempt(n("test")); err != nil {
			return err
		}
		_, err = tx.Claim(token, credential('m'), testWorker)
		return err
	})
	control := func(tx *Tx, id string) workerapi.Control {
		c, err := tx.Control(id)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	inTx(t, s, func(tx *Tx) error {
		if c := control(tx, before.ID); c.Version != 0 {
			t.Errorf("before a job failed, an attempt's control is %+v", c)
		}
		for range 2 {
			marked, err := tx.MarkJobFailed(id)
			if err != nil {
				return err
			}
			if c := control(tx, placing.ID); !c.AnyJobFailed || !c.Cancel || c.Reason != plan.ReasonCancelled || c.Version != 2 {
				t.Errorf("an attempt being placed is told %+v", c)
			}
			if c := control(tx, claimed.ID); !c.AnyJobFailed || c.Version != 1 {
				t.Errorf("a claimed attempt is told %+v", c)
			}
			if c := control(tx, before.ID); c.Version != 0 {
				t.Errorf("an attempt that ended is told %+v", c)
			}
			if r, err := tx.Run(id); err != nil || r.AnyJobFailedAt.IsZero() {
				t.Errorf("the run is %+v, %v", r, err)
			}
			if !marked {
				return nil
			}
		}
		t.Error("the run was marked twice")
		return nil
	})
	inTx(t, s, func(tx *Tx) error {
		if err := tx.EndAttempt(placing.ID, AttemptEnd{Kind: EndWithdrawn}); err != nil {
			return err
		}
		after, _, err := tx.NewAttempt(n("build"))
		if err != nil {
			return err
		}
		if err := tx.StartControl(after.ID); err != nil {
			return err
		}
		if c := control(tx, after.ID); !c.AnyJobFailed || c.Version != 1 {
			t.Errorf("an attempt made after a job failed is told %+v", c)
		}
		if _, err := tx.MarkJobFailed("99"); !errors.Is(err, ErrNotFound) {
			t.Errorf("MarkJobFailed of no run = %v, want ErrNotFound", err)
		}
		if err := tx.StartControl("none"); !errors.Is(err, ErrNotFound) {
			t.Errorf("StartControl of no attempt = %v, want ErrNotFound", err)
		}
		return nil
	})
}
