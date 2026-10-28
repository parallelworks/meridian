package store

import (
	"context"
	"errors"
	"slices"
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

// last returns e, a failed try's finished event, as a worker of version 2
// of the worker API sends it of its step's last try.
func last(e workerapi.Event) workerapi.Event {
	e.LastTry = true
	return e
}

// TestFailedStepMarks checks that a step that fails marks its run, in the
// transaction that stores its events: a cleanup at once; a step's try, of
// an attempt of version 2, at once when its worker says it was the last;
// of one of version 1, once it is surely the step's last, as the store's
// LastTry says, or else once the next event shows the step didn't try
// again; and that a try that is tried again doesn't.
func TestFailedStepMarks(t *testing.T) {
	t.Parallel()
	t.Run("its job's retry: lists timeout", func(t *testing.T) {
		// Its job's deadline may have failed it, and another attempt may
		// follow: the job marks the run as it ends.
		s := open(t)
		s.retriesFor = func(_ *Tx, _ string, reason plan.Reason) bool { return reason == plan.ReasonTimeout }
		id := createRun(t, s)
		inTx(t, s, func(tx *Tx) error {
			a, token, err := placedAttempt(tx, NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10})
			if err != nil {
				return err
			}
			if _, err := tx.Claim(token, credential('a'), testWorker, workerapi.Version); err != nil {
				return err
			}
			events := []workerapi.Event{stepEvent(1, 0, 0, ""), last(stepEvent(2, 0, 0, plan.OutcomeFailure)), stepEvent(3, 1, 0, plan.OutcomeSkipped)}
			if _, err := tx.AppendEvents(a.ID, events); err != nil {
				return err
			}
			if r, err := tx.Run(id); err != nil || !r.AnyJobFailedAt.IsZero() {
				t.Errorf("the run is %+v, %v", r, err)
			}
			return nil
		})
	})
	type batch struct {
		events []workerapi.Event
		marked bool
	}
	cleanupFailed := workerapi.Event{Seq: 3, Time: time.Unix(3, 0).UTC(), Step: 0, Cleanup: true, Finished: true, Outcome: plan.OutcomeFailure}
	// whatever is the store's LastTry where the plan doesn't say, as for a
	// max-retries: that is an expression; and lastIsSecond where it says a
	// step's second try is its last.
	var whatever func(*Tx, string, int, int) bool
	lastIsSecond := func(_ *Tx, _ string, _, try int) bool { return try == 1 }
	for _, tc := range []struct {
		name    string
		version int
		lastTry func(tx *Tx, attempt string, step, try int) bool
		batches []batch
	}{
		{"a cleanup fails", 1, whatever, []batch{
			{[]workerapi.Event{stepEvent(1, 0, 0, ""), stepEvent(2, 0, 0, plan.OutcomeSuccess)}, false},
			{[]workerapi.Event{cleanupFailed}, true},
		}},
		{"tried again, then its last try", 1, whatever, []batch{
			{[]workerapi.Event{stepEvent(1, 1, 0, ""), stepEvent(2, 1, 0, plan.OutcomeFailure)}, false},
			{[]workerapi.Event{stepEvent(3, 1, 1, "")}, false},
			{[]workerapi.Event{stepEvent(4, 1, 1, plan.OutcomeFailure)}, false},
			// The next event, of a batch of its own, shows it was the last.
			{[]workerapi.Event{stepEvent(5, 2, 0, plan.OutcomeSkipped)}, true},
		}},
		{"its last try, known", 1, lastIsSecond, []batch{
			{[]workerapi.Event{stepEvent(1, 0, 0, ""), stepEvent(2, 0, 0, plan.OutcomeFailure)}, false},
			{[]workerapi.Event{stepEvent(3, 0, 1, ""), stepEvent(4, 0, 1, plan.OutcomeFailure)}, true},
		}},
		// Version 2: the worker says which try was the last, so the run is
		// marked as its event is stored, though the plan doesn't say, and
		// no event comes after it.
		{"version 2: a cleanup fails", 2, whatever, []batch{
			{[]workerapi.Event{stepEvent(1, 0, 0, ""), stepEvent(2, 0, 0, plan.OutcomeSuccess)}, false},
			{[]workerapi.Event{cleanupFailed}, true},
		}},
		{"version 2: tried again, then its last try", 2, whatever, []batch{
			{[]workerapi.Event{stepEvent(1, 1, 0, ""), stepEvent(2, 1, 0, plan.OutcomeFailure)}, false},
			{[]workerapi.Event{stepEvent(3, 1, 1, "")}, false},
			{[]workerapi.Event{last(stepEvent(4, 1, 1, plan.OutcomeFailure))}, true},
		}},
		{"version 2: only the worker says", 2, lastIsSecond, []batch{
			// Neither the plan nor the next event is asked: a try the
			// worker doesn't say was the last, it tries again.
			{[]workerapi.Event{stepEvent(1, 0, 0, ""), stepEvent(2, 0, 0, plan.OutcomeFailure)}, false},
			{[]workerapi.Event{stepEvent(3, 0, 1, ""), stepEvent(4, 0, 1, plan.OutcomeFailure), stepEvent(5, 1, 0, plan.OutcomeSkipped)}, false},
			{[]workerapi.Event{stepEvent(6, 2, 0, ""), last(stepEvent(7, 2, 0, plan.OutcomeFailure))}, true},
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
				if a, token, err = placedAttempt(tx, n("build")); err != nil {
					return err
				}
				if _, err := tx.Claim(token, credential('a'), testWorker, tc.version); err != nil {
					return err
				}
				if b, token, err = placedAttempt(tx, n("test")); err != nil {
					return err
				}
				_, err = tx.Claim(token, credential('b'), testWorker, workerapi.Version)
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
					// Every attempt of the run that hasn't ended is told, the
					// one whose step failed among them.
					var want []string
					if batch.marked {
						want = slices.Sorted(slices.Values([]string{a.ID, b.ID}))
					}
					if marked := !r.AnyJobFailedAt.IsZero(); marked != batch.marked || !slices.Equal(tx.ControlsChanged(), want) {
						t.Errorf("after event %d, the run is marked: %v, and the controls changed are %v", batch.events[0].Seq, marked, tx.ControlsChanged())
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
	t.Parallel()
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
		if v, err := tx.UpdateControl(a.ID, cancel); err != nil || v != 1 || !slices.Equal(tx.ControlsChanged(), []string{a.ID}) {
			t.Errorf("UpdateControl = %d, %v, and the controls changed are %v", v, err, tx.ControlsChanged())
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

// TestOnPlacementsRecorded checks that the store calls
// Options.OnPlacementsRecorded once a transaction that recorded attempts'
// placements has committed, with their IDs, in the order it recorded
// them, and not for one that rolled back.
func TestOnPlacementsRecorded(t *testing.T) {
	t.Parallel()
	var heard [][]string
	s := openWith(t, newPath(t), &Options{
		OnPlacementsRecorded: func(_ context.Context, ids []string) { heard = append(heard, slices.Clone(ids)) },
	})
	id := createRun(t, s)
	var a, b *Attempt
	inTx(t, s, func(tx *Tx) error {
		var err error
		if a, _, err = tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10}); err != nil {
			return err
		}
		b, _, err = tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	rolledBack := errors.New("rolled back")
	if err := s.Tx(t.Context(), func(tx *Tx) error {
		if err := tx.RecordPlacement(a.ID, testPlacement); err != nil {
			return err
		}
		return rolledBack
	}); !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	if len(heard) != 0 {
		t.Fatalf("a transaction that rolled back was heard of: %v", heard)
	}
	inTx(t, s, func(tx *Tx) error {
		for _, x := range []*Attempt{b, a} {
			if err := tx.RecordPlacement(x.ID, testPlacement); err != nil {
				return err
			}
		}
		return nil
	})
	if len(heard) != 1 || !slices.Equal(heard[0], []string{b.ID, a.ID}) {
		t.Errorf("heard %v, want [[%s %s]]", heard, b.ID, a.ID)
	}
}

// TestOnControlsChanged checks that the store calls
// Options.OnControlsChanged once a transaction that changed attempts'
// controls commits, with their IDs, in order; and not for one that rolls
// back, nor one that changed none.
func TestOnControlsChanged(t *testing.T) {
	t.Parallel()
	var heard [][]string
	s := openWith(t, newPath(t), &Options{
		OnControlsChanged: func(_ context.Context, ids []string) { heard = append(heard, ids) },
	})
	id := createRun(t, s)
	var a, b *Attempt
	inTx(t, s, func(tx *Tx) error {
		var err error
		if a, _, err = tx.NewAttempt(NewAttempt{Run: id, Job: "build", LeaseSeconds: 60, HeartbeatSeconds: 10}); err != nil {
			return err
		}
		b, _, err = tx.NewAttempt(NewAttempt{Run: id, Job: "test", LeaseSeconds: 60, HeartbeatSeconds: 10})
		return err
	})
	if len(heard) != 0 {
		t.Fatalf("new attempts without a first control were heard of: %v", heard)
	}
	rolledBack := errors.New("rolled back")
	if err := s.Tx(t.Context(), func(tx *Tx) error {
		if err := tx.tellJobFailed(a.ID); err != nil {
			return err
		}
		return rolledBack
	}); !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	if len(heard) != 0 {
		t.Fatalf("a transaction that rolled back was heard of: %v", heard)
	}
	inTx(t, s, func(tx *Tx) error {
		for _, x := range []*Attempt{b, a} {
			if err := tx.tellJobFailed(x.ID); err != nil {
				return err
			}
		}
		return nil
	})
	want := []string{a.ID, b.ID}
	slices.Sort(want)
	if len(heard) != 1 || !slices.Equal(heard[0], want) {
		t.Errorf("heard %v, want [%v]", heard, want)
	}
}

// TestMarkJobFailed checks that the first job of a run to fail marks the
// run, once, and has each of its attempts that hasn't ended tell its worker
// so, with what its control asked already; and that an attempt made after
// it is told so from the start.
func TestMarkJobFailed(t *testing.T) {
	t.Parallel()
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
		if _, err := tx.UpdateControl(placing.ID, func(c *workerapi.Control) bool {
			c.Cancel, c.Reason = true, plan.ReasonCancelled
			return true
		}); err != nil {
			return err
		}
		var token string
		if claimed, token, err = placedAttempt(tx, n("test")); err != nil {
			return err
		}
		_, err = tx.Claim(token, credential('m'), testWorker, workerapi.Version)
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
