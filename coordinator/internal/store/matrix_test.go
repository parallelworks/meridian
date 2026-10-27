package store

import (
	"errors"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// insertInstances stores instances of the run's job build, by index.
func insertInstances(tx *Tx, run string, indexes ...int) error {
	for _, i := range indexes {
		key := "build#" + string(rune('0'+i))
		if err := tx.InsertJob(NewJob{Run: run, Name: "build", Key: key, SupervisorID: fakeJobID(10 + i)}); err != nil {
			return err
		}
	}
	return nil
}

// TestInstances checks a matrix job's instances, its expansion, and the
// instances' turns.
func TestInstances(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	x := &plan.Expansion{Instances: []plan.Instance{{Key: "build#0", Index: 0}, {Key: "build#1", Index: 1}}, MaxParallel: 1}
	inTx(t, s, func(tx *Tx) error {
		if err := insertInstances(tx, id, 1, 0); err != nil {
			return err
		}
		jobs, err := tx.Instances(id, "build")
		if err != nil {
			return err
		}
		if len(jobs) != 2 || jobs[0].Key != "build#0" || jobs[1].Key != "build#1" || jobs[1].Instance != 1 {
			t.Errorf("the instances are %v", keysOf(jobs))
		}
		if err := tx.SetExpansion(id, "build", x); err != nil {
			return err
		}
		if err := tx.SetExpansion(id, "build", x); !errors.Is(err, ErrState) {
			t.Errorf("SetExpansion again = %v, want ErrState", err)
		}
		if err := tx.SetExpansion(id, "deploy", x); !errors.Is(err, ErrNotFound) {
			t.Errorf("SetExpansion of no job = %v, want ErrNotFound", err)
		}
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		if j.Expansion == nil || len(j.Expansion.Instances) != 2 || j.Expansion.MaxParallel != 1 || j.Expansion.Instances[1].Key != "build#1" {
			t.Errorf("the job's expansion is %+v", j.Expansion)
		}
		granted, err := tx.GrantTurns(id, []string{"build#0"})
		if err != nil || !slices.Equal(granted, []string{"build#0"}) {
			t.Errorf("GrantTurns = %v, %v", granted, err)
		}
		granted, err = tx.GrantTurns(id, []string{"build#0", "build#1"})
		if err != nil || !slices.Equal(granted, []string{"build#1"}) {
			t.Errorf("GrantTurns again = %v, %v", granted, err)
		}
		if j, err := tx.Job(id, "build#1"); err != nil || j.TurnAt.IsZero() {
			t.Errorf("the instance given its turn is %+v, %v", j, err)
		}
		return nil
	})
}

// keysOf returns the keys of jobs.
func keysOf(jobs []*Job) []string {
	var keys []string
	for _, j := range jobs {
		keys = append(keys, j.Key)
	}
	return keys
}

// TestMarkFailFast checks that a matrix job whose instance failed is marked
// once, and has each attempt of its instances that hasn't ended told to
// cancel with the reason fail-fast, keeping what its control asked, and no
// other job's.
func TestMarkFailFast(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	n := func(job string) NewAttempt {
		return NewAttempt{Run: id, Job: job, LeaseSeconds: 60, HeartbeatSeconds: 10}
	}
	var running, placing, cancelled, other *Attempt
	inTx(t, s, func(tx *Tx) error {
		if err := insertInstances(tx, id, 0, 1, 2); err != nil {
			return err
		}
		// One told to cancel already, for the run's timeout, keeps why.
		var err error
		if cancelled, _, err = tx.NewAttempt(n("build#2")); err != nil {
			return err
		}
		if _, err := tx.UpdateControl(cancelled.ID, func(c *workerapi.Control) bool {
			c.Cancel, c.Reason = true, plan.ReasonTimeout
			return true
		}); err != nil {
			return err
		}
		var token string
		if running, token, err = tx.NewAttempt(n("build#0")); err != nil {
			return err
		}
		if _, err := tx.Claim(token, credential('f'), testWorker); err != nil {
			return err
		}
		if placing, _, err = tx.NewAttempt(n("build#1")); err != nil {
			return err
		}
		if err := tx.tellJobFailed(placing.ID); err != nil {
			return err
		}
		other, _, err = tx.NewAttempt(n("test"))
		return err
	})
	inTx(t, s, func(tx *Tx) error {
		for round := range 2 {
			marked, err := tx.MarkFailFast(id, "build")
			if err != nil || marked != (round == 0) {
				t.Errorf("MarkFailFast = %v, %v", marked, err)
			}
		}
		for _, a := range []*Attempt{running, placing} {
			if c, err := tx.Control(a.ID); err != nil || !c.Cancel || c.Reason != plan.ReasonFailFast {
				t.Errorf("the instance's attempt is told %+v, %v", c, err)
			}
		}
		if c, err := tx.Control(placing.ID); err != nil || !c.AnyJobFailed || c.Version != 2 {
			t.Errorf("the control kept nothing of what it asked: %+v, %v", c, err)
		}
		if c, err := tx.Control(other.ID); err != nil || c != (workerapi.Control{}) {
			t.Errorf("another job's attempt is told %+v, %v", c, err)
		}
		if c, err := tx.Control(cancelled.ID); err != nil || c.Reason != plan.ReasonTimeout || c.Version != 1 {
			t.Errorf("an attempt told to cancel already is told %+v, %v", c, err)
		}
		if j, err := tx.Job(id, "build"); err != nil || j.FailFastAt.IsZero() {
			t.Errorf("the job is %+v, %v", j, err)
		}
		// An instance's attempt made after the mark, as by a supervisor
		// that looked before it, is told so from the start, and another
		// job's isn't.
		if err := tx.EndAttempt(placing.ID, AttemptEnd{Kind: EndWithdrawn}); err != nil {
			return err
		}
		later, _, err := tx.NewAttempt(n("build#1"))
		if err != nil {
			return err
		}
		if c, err := tx.Control(later.ID); err != nil || !c.Cancel || c.Reason != plan.ReasonFailFast {
			t.Errorf("an attempt made after the mark is told %+v, %v", c, err)
		}
		if err := tx.EndAttempt(other.ID, AttemptEnd{Kind: EndWithdrawn}); err != nil {
			return err
		}
		next, _, err := tx.NewAttempt(n("test"))
		if err != nil {
			return err
		}
		if c, err := tx.Control(next.ID); err != nil || c.Cancel {
			t.Errorf("another job's attempt made after the mark is told %+v, %v", c, err)
		}
		return nil
	})
}

// TestAny checks the jobs needed on any instance, and the instance chosen,
// once.
func TestAny(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		if err := tx.InsertAny(id, "build", fakeJobID(20)); err != nil {
			return err
		}
		if err := tx.InsertAny(id, "test", fakeJobID(21)); err != nil {
			return err
		}
		a, err := tx.Any(id, "build")
		if err != nil || a.MarkerID != fakeJobID(20) || !a.ChosenAt.IsZero() {
			t.Errorf("Any = %+v, %v", a, err)
		}
		if err := tx.ChooseAny(id, "build", "build#1", []string{"build#0", "build#1"}); err != nil {
			return err
		}
		if err := tx.ChooseAny(id, "build", "build#0", nil); !errors.Is(err, ErrState) {
			t.Errorf("ChooseAny again = %v, want ErrState", err)
		}
		if a, err := tx.Any(id, "build"); err != nil || a.ChosenAt.IsZero() || a.Instance != "build#1" || !slices.Equal(a.Ended, []string{"build#0", "build#1"}) {
			t.Errorf("once chosen, Any = %+v, %v", a, err)
		}
		if err := tx.ChooseAny(id, "test", "", nil); err != nil {
			return err
		}
		if a, err := tx.Any(id, "test"); err != nil || a.ChosenAt.IsZero() || a.Instance != "" || a.Ended != nil {
			t.Errorf("chosen as itself, Any = %+v, %v", a, err)
		}
		if _, err := tx.Any(id, "deploy"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Any of a job none needs so = %v, want ErrNotFound", err)
		}
		if err := tx.ChooseAny(id, "deploy", "", nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("ChooseAny of a job none needs so = %v, want ErrNotFound", err)
		}
		return nil
	})
	checkIntegrity(t, s)
}
