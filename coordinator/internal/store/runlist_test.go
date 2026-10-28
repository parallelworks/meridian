package store

import (
	"errors"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestRecentRuns checks the store's last runs, the newest first, each with
// what a list of them shows, and a run's summary on its own.
func TestRecentRuns(t *testing.T) {
	t.Parallel()
	s := open(t)
	p := testPlan(t)
	for range 3 {
		createRun(t, s)
	}
	inTx(t, s, func(tx *Tx) error {
		if err := tx.StartRun("2", testState(), nil); err != nil {
			return err
		}
		return tx.EndRun("2", plan.OutcomeFailure, plan.ReasonTimeout)
	})
	inTx(t, s, func(tx *Tx) error {
		runs, err := tx.RecentRuns(2, false)
		if err != nil {
			return err
		}
		if len(runs) != 2 || runs[0].ID != "3" || runs[1].ID != "2" {
			t.Fatalf("the last two runs are %+v", runs)
		}
		created, ended := runs[0], runs[1]
		if created.State != RunCreated || created.Outcome != "" || !created.StartedAt.IsZero() || !created.EndedAt.IsZero() || created.CreatedAt.IsZero() {
			t.Errorf("a run created is %+v", created)
		}
		if ended.State != RunEnded || ended.Outcome != plan.OutcomeFailure || ended.Reason != plan.ReasonTimeout || ended.StartedAt.IsZero() || ended.EndedAt.Before(ended.StartedAt) {
			t.Errorf("a run ended is %+v", ended)
		}
		if ended.Name != p.Run.Name || ended.Location != "w.yaml" || ended.PlanVersion != 1 || ended.RunDir != "/runs/2" {
			t.Errorf("a run's name, location, plan version and directory are %+v", ended)
		}
		if all, err := tx.RecentRuns(10, false); err != nil || len(all) != 3 || all[2].ID != "1" {
			t.Errorf("the last ten runs are %+v, %v", all, err)
		}
		if _, err := tx.RecentRuns(0, false); err == nil {
			t.Error("RecentRuns(0) returned no error")
		}
		r, err := tx.RunSummary("2")
		if err != nil || *r != ended {
			t.Errorf("RunSummary(2) = %+v, %v; want %+v", r, err, ended)
		}
		for _, missing := range []string{"9", "x", ""} {
			if _, err := tx.RunSummary(missing); !errors.Is(err, ErrNotFound) {
				t.Errorf("RunSummary(%q) = %v, want ErrNotFound", missing, err)
			}
		}
		return nil
	})
}
