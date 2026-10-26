package store

import (
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// stepEvent is the event of a step's try, numbered seq: started, or
// finished with outcome when it isn't "".
func stepEvent(seq int64, step, try int, outcome plan.Outcome) workerapi.Event {
	return workerapi.Event{Seq: seq, Time: time.Unix(seq, 0).UTC(), Step: step, Try: try, Finished: outcome != "", Outcome: outcome}
}

// TestFailedStepMarks checks that a step that fails marks its run, in the
// transaction that stores its events: a cleanup at once; a step's try once
// it is surely the step's last, as the store's LastTry says, or else once
// the next event shows the step didn't try again; and that a try that is
// tried again doesn't.
func TestFailedStepMarks(t *testing.T) {
	type batch struct {
		events []workerapi.Event
		marked bool
	}
	cleanupFailed := workerapi.Event{Seq: 3, Time: time.Unix(3, 0).UTC(), Step: 0, Cleanup: true, Finished: true, Outcome: plan.OutcomeFailure}
	for _, tc := range []struct {
		name    string
		lastTry func(tx *Tx, attempt string, step, try int) bool
		batches []batch
	}{
		{"a cleanup fails", nil, []batch{
			{[]workerapi.Event{stepEvent(1, 0, 0, ""), stepEvent(2, 0, 0, plan.OutcomeSuccess)}, false},
			{[]workerapi.Event{cleanupFailed}, true},
		}},
		{"tried again, then its last try", nil, []batch{
			{[]workerapi.Event{stepEvent(1, 1, 0, ""), stepEvent(2, 1, 0, plan.OutcomeFailure)}, false},
			{[]workerapi.Event{stepEvent(3, 1, 1, "")}, false},
			{[]workerapi.Event{stepEvent(4, 1, 1, plan.OutcomeFailure)}, false},
			// The next event, of a batch of its own, shows it was the last.
			{[]workerapi.Event{stepEvent(5, 2, 0, plan.OutcomeSkipped)}, true},
		}},
		{"its last try, known", func(_ *Tx, _ string, _, try int) bool { return try == 1 }, []batch{
			{[]workerapi.Event{stepEvent(1, 0, 0, ""), stepEvent(2, 0, 0, plan.OutcomeFailure)}, false},
			{[]workerapi.Event{stepEvent(3, 0, 1, ""), stepEvent(4, 0, 1, plan.OutcomeFailure)}, true},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t)
			s.lastTry = tc.lastTry
			id := createRun(t, s)
			n := func(job string) NewAttempt {
				return NewAttempt{Run: id, Job: job, LeaseSeconds: 60, HeartbeatSeconds: 10}
			}
			var a, b *Attempt
			inTx(t, s, func(tx *Tx) error {
				var token string
				var err error
				if a, token, err = tx.NewAttempt(n("build")); err != nil {
					return err
				}
				if _, err := tx.Claim(token, credential('a'), testWorker); err != nil {
					return err
				}
				if b, token, err = tx.NewAttempt(n("test")); err != nil {
					return err
				}
				_, err = tx.Claim(token, credential('b'), testWorker)
				return err
			})
			for _, batch := range tc.batches {
				inTx(t, s, func(tx *Tx) error {
					if _, err := tx.AppendEvents(a.ID, batch.events); err != nil {
						return err
					}
					r, err := tx.Run(id)
					if err != nil {
						return err
					}
					if marked := !r.AnyJobFailedAt.IsZero(); marked != batch.marked || tx.ControlsChanged() != batch.marked {
						t.Errorf("after event %d, the run is marked: %v, and controls changed: %v", batch.events[0].Seq, marked, tx.ControlsChanged())
					}
					return nil
				})
			}
			inTx(t, s, func(tx *Tx) error {
				if c, err := tx.Control(b.ID); err != nil || !c.AnyJobFailed {
					t.Errorf("the other job's attempt is told %+v, %v", c, err)
				}
				return nil
			})
		})
	}
}

// TestUpdateControl checks that a control is changed with what it asked
// kept, at the next version, and isn't when nothing changes.
func TestUpdateControl(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		a, _, err := tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
		if err != nil {
			return err
		}
		cancel := func(c *workerapi.Control) bool {
			c.Cancel, c.Reason = true, plan.ReasonCancelled
			return true
		}
		if v, err := tx.UpdateControl(a.ID, cancel); err != nil || v != 1 || !tx.ControlsChanged() {
			t.Errorf("UpdateControl = %d, %v", v, err)
		}
		if err := tx.tellJobFailed(a.ID); err != nil {
			return err
		}
		if err := tx.tellJobFailed(a.ID); err != nil {
			return err
		}
		if c, err := tx.Control(a.ID); err != nil || c.Version != 2 || !c.Cancel || c.Reason != plan.ReasonCancelled || !c.AnyJobFailed {
			t.Errorf("the control is %+v, %v", c, err)
		}
		return nil
	})
}

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
