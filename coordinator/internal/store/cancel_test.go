package store

import (
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestCancelJob checks that a job a step cancels is marked once, with the
// step, and that each of its attempts that hasn't ended is told to cancel
// its job, keeping what its control asked already, as one told to cancel
// for the run's timeout; that an attempt made later is told from its
// first control; that another job's attempts aren't told; and that a job
// that has ended isn't marked.
func TestCancelJob(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	n := func(job string) NewAttempt {
		return NewAttempt{Run: id, Job: job, LeaseSeconds: 60, HeartbeatSeconds: 10}
	}
	var running, other *Attempt
	inTx(t, s, func(tx *Tx) error {
		var (
			token string
			err   error
		)
		if running, token, err = placedAttempt(tx, n("build")); err != nil {
			return err
		}
		if _, err := tx.Claim(token, credential('c'), testWorker, workerapi.Version); err != nil {
			return err
		}
		if err := tx.tellJobFailed(running.ID); err != nil {
			return err
		}
		other, _, err = tx.NewAttempt(n("test"))
		return err
	})
	inTx(t, s, func(tx *Tx) error {
		for round := range 2 {
			marked, j, err := tx.CancelJob(id, "build", "/jobs/test/steps/1")
			if err != nil || marked != (round == 0) || j == nil || j.CancelledAt.IsZero() || j.CancelledBy != "/jobs/test/steps/1" {
				t.Errorf("CancelJob = %v, %+v, %v", marked, j, err)
			}
		}
		if c, err := tx.Control(running.ID); err != nil || !c.Cancel || c.Reason != plan.ReasonCancelled || !c.AnyJobFailed || c.Version != 2 {
			t.Errorf("the job's attempt is told %+v, %v", c, err)
		}
		if c, err := tx.Control(other.ID); err != nil || c != (workerapi.Control{}) {
			t.Errorf("another job's attempt is told %+v, %v", c, err)
		}
		// An attempt made after the mark, as after a lost worker, is told
		// from the start.
		if err := tx.EndAttempt(running.ID, AttemptEnd{Kind: EndLost}); err != nil {
			return err
		}
		later, _, err := tx.NewAttempt(n("build"))
		if err != nil {
			return err
		}
		if c, err := tx.Control(later.ID); err != nil || !c.Cancel || c.Reason != plan.ReasonCancelled || c.Version != 1 {
			t.Errorf("an attempt made after the mark is told %+v, %v", c, err)
		}
		return nil
	})

	// One told to cancel already, for the run's timeout, keeps why; a job
	// that has ended isn't marked.
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.UpdateControl(other.ID, func(c *workerapi.Control) bool {
			c.Cancel, c.Reason = true, plan.ReasonTimeout
			return true
		}); err != nil {
			return err
		}
		if marked, _, err := tx.CancelJob(id, "test", "/jobs/build/steps/0"); err != nil || !marked {
			t.Errorf("CancelJob = %v, %v", marked, err)
		}
		if c, err := tx.Control(other.ID); err != nil || c.Reason != plan.ReasonTimeout || c.Version != 1 {
			t.Errorf("an attempt told to cancel already is told %+v, %v", c, err)
		}
		return nil
	})
	id2 := createRun(t, s)
	inTx(t, s, func(tx *Tx) error {
		if err := tx.EndJob(id2, "build", JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSuccess}}); err != nil {
			return err
		}
		marked, j, err := tx.CancelJob(id2, "build", "/jobs/test/steps/0")
		if err != nil || marked || !j.CancelledAt.IsZero() {
			t.Errorf("a job that has ended: CancelJob = %v, %+v, %v", marked, j, err)
		}
		return nil
	})
}
