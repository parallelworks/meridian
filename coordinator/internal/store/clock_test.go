package store

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/plan"
)

// openClock opens the store at path with the clock now, which the test
// closes.
func openClock(t *testing.T, path string, now func() time.Time) *Store {
	t.Helper()
	return openWith(t, path, &Options{Now: now, MaxLead: time.Minute})
}

// openWith opens the store at path with opts, which the test closes.
func openWith(t *testing.T, path string, opts *Options) *Store {
	t.Helper()
	s, err := Open(t.Context(), path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// clockOf returns s's clock, as a transaction that writes reads it, or as
// a View does.
func clockOf(t *testing.T, s *Store, view bool) time.Time {
	t.Helper()
	var now time.Time
	run := s.Tx
	if view {
		run = s.View
	}
	if err := run(t.Context(), func(tx *Tx) error {
		var err error
		now, err = tx.Now()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return now
}

// graceOf returns when s's latest grace began.
func graceOf(t *testing.T, s *Store) time.Time {
	t.Helper()
	var from time.Time
	if err := s.View(t.Context(), func(tx *Tx) error {
		var err error
		from, err = tx.GraceFrom()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return from
}

// TestClockMark checks that the store's clock never goes back by a little,
// whichever process reads it: one whose host clock is behind another's, as
// one that started after the host's clock stepped back, reads the latest
// time a transaction that writes read, and so never takes a lease another
// renewed for older than it is. A View reads the mark, and leaves it.
func TestClockMark(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	base := time.Now().UTC().Truncate(time.Microsecond)
	const step = 30 * time.Second
	ahead := openClock(t, path, func() time.Time { return base.Add(step) })
	behind := openClock(t, path, func() time.Time { return base })
	if got := clockOf(t, behind, false); !got.Equal(base) {
		t.Fatalf("the store's clock is %v, want %v", got, base)
	}
	if got := clockOf(t, ahead, false); !got.Equal(base.Add(step)) {
		t.Fatalf("the store's clock ahead is %v", got)
	}
	for _, view := range []bool{true, false} {
		if got := clockOf(t, behind, view); !got.Equal(base.Add(step)) {
			t.Errorf("a store whose clock is behind reads %v (view %t), not the mark", got, view)
		}
	}
	// A View's clock isn't the mark.
	further := openClock(t, path, func() time.Time { return base.Add(2 * step) })
	clockOf(t, further, true)
	if got := clockOf(t, behind, false); !got.Equal(base.Add(step)) {
		t.Errorf("after a View with its clock further ahead, the clock is %v", got)
	}
	// Once the host's clock passes the mark, the store's moves on with it.
	if got := clockOf(t, further, false); !got.Equal(base.Add(2 * step)) {
		t.Errorf("the store's clock past the mark is %v", got)
	}
	if from := graceOf(t, behind); !from.IsZero() {
		t.Errorf("a step back within the lead started a grace at %v", from)
	}
}

// TestClockReset checks that one bad reading, a year ahead, doesn't hold
// the store's clock up for a year once the host's clock is right again:
// the store takes it that the clock was reset, takes the host's, starts a
// grace, and brings a lease renewed ahead of it back to it, so that it is
// fresh, and lapses a lease period after the reset, not a year after. The
// times of the attempt's run and job come back too, but the run's
// created_at, which never changes; and OnClockReset is called once.
func TestClockReset(t *testing.T) {
	path := filepath.Join(privateDir(t), "meridian.db")
	base := time.Now().UTC().Truncate(time.Microsecond)
	ahead := base.Add(365 * 24 * time.Hour)
	bad := openClock(t, path, func() time.Time { return ahead })
	a := claimedAttempt(t, bad, 'a')
	if !a.RenewedAt.Equal(ahead) {
		t.Fatalf("the attempt was renewed at %v", a.RenewedAt)
	}
	inTx(t, bad, func(tx *Tx) error {
		if _, err := tx.PinReady(a.Run, a.Job, false, nil); err != nil {
			return err
		}
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_runs SET state = ?, started_at = ? WHERE id = ?", RunRunning, ahead.UnixMicro(), a.Run)
		return err
	})
	resets := 0
	right := openWith(t, path, &Options{
		Now: func() time.Time { return base }, MaxLead: time.Minute,
		OnClockReset: func(context.Context) { resets++ },
	})
	// A View takes the host's clock, but records nothing.
	if got := clockOf(t, right, true); !got.Equal(base) {
		t.Errorf("a View after the reset reads %v, want %v", got, base)
	}
	if got := clockOf(t, right, false); !got.Equal(base) {
		t.Fatalf("after the reset, the store's clock is %v, want %v", got, base)
	}
	if from := graceOf(t, right); !from.Equal(base) {
		t.Errorf("the reset's grace began at %v, want %v", from, base)
	}
	var now *Attempt
	if err := right.View(t.Context(), func(tx *Tx) error {
		var err error
		now, err = tx.Attempt(a.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !now.RenewedAt.Equal(base) || !now.ClaimedAt.Equal(base) || now.CreatedAt.After(base) {
		t.Errorf("the attempt renewed ahead of the clock is %+v", now)
	}
	if err := right.View(t.Context(), func(tx *Tx) error {
		r, err := tx.Run(a.Run)
		if err != nil {
			return err
		}
		if !r.StartedAt.Equal(base) || !r.CreatedAt.Equal(ahead) {
			t.Errorf("the run started ahead of the clock started at %v, created at %v", r.StartedAt, r.CreatedAt)
		}
		j, err := tx.Job(a.Run, a.Job)
		if err != nil {
			return err
		}
		if !j.CreatedAt.Equal(base) || !j.ReadyAt.Equal(base) {
			t.Errorf("the job readied ahead of the clock was created at %v, readied at %v", j.CreatedAt, j.ReadyAt)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clockOf(t, right, false)
	if resets != 1 {
		t.Errorf("OnClockReset was called %d times, want once", resets)
	}
	// The clock moves on from the host's again.
	later := openClock(t, path, func() time.Time { return base.Add(time.Second) })
	if got := clockOf(t, later, false); !got.Equal(base.Add(time.Second)) {
		t.Errorf("after the reset, the clock moves on to %v", got)
	}
}

// TestClockHost checks that a store that reads the host's clock takes it
// again once it is right, though it read it a year ahead before in the
// same process: hopper's clock never goes back within a process, so the
// store reads the host's itself.
func TestClockHost(t *testing.T) {
	resets := 0
	s := openWith(t, filepath.Join(privateDir(t), "meridian.db"), &Options{
		MaxLead: time.Minute, OnClockReset: func(context.Context) { resets++ },
	})
	ahead := time.Now().UTC().Truncate(time.Microsecond).Add(365 * 24 * time.Hour)
	hostClock = func() time.Time { return ahead }
	t.Cleanup(func() { hostClock = time.Now })
	if got := clockOf(t, s, false); !got.Equal(ahead) {
		t.Fatalf("the store's clock is %v, want %v", got, ahead)
	}
	hostClock = time.Now
	for range 2 {
		before := time.Now().UTC().Truncate(time.Microsecond)
		if got := clockOf(t, s, false); got.Before(before) || got.After(time.Now()) {
			t.Errorf("once the host's clock is right, the store's is %v, from %v", got, before)
		}
	}
	if resets != 1 {
		t.Errorf("OnClockReset was called %d times, want once", resets)
	}
}

// TestReadiedSupervisors checks that the supervisors a reset of the clock
// wakes are those of the readied jobs that haven't ended of the runs that
// are running.
func TestReadiedSupervisors(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	readied := func() []hopper.JobID {
		t.Helper()
		var ids []hopper.JobID
		if err := s.View(t.Context(), func(tx *Tx) error {
			var err error
			ids, err = tx.ReadiedSupervisors()
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	inTx(t, s, func(tx *Tx) error {
		for _, key := range []string{"build", "test"} {
			if _, err := tx.PinReady(id, key, false, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if ids := readied(); len(ids) != 0 {
		t.Errorf("the readied supervisors of a run that isn't running are %v", ids)
	}
	inTx(t, s, func(tx *Tx) error {
		_, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_runs SET state = ? WHERE id = ?", RunRunning, id)
		return err
	})
	if ids := readied(); !slices.Equal(ids, []hopper.JobID{fakeJobID(1), fakeJobID(2)}) {
		t.Errorf("the readied supervisors are %v", ids)
	}
	inTx(t, s, func(tx *Tx) error {
		return tx.EndJob(id, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}})
	})
	if ids := readied(); !slices.Equal(ids, []hopper.JobID{fakeJobID(2)}) {
		t.Errorf("once build ended, the readied supervisors are %v", ids)
	}
}
