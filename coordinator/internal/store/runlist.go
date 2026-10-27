package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/meridian/plan"
)

// A RunSummary is what RecentRuns gives of a run: what a list of runs
// shows, without its plan, what it started with, or its problems.
type RunSummary struct {
	ID string
	// Name and Location are the plan's: the run's name and its workflow's
	// location. PlanVersion is the plan's version.
	Name        string
	Location    string
	PlanVersion int
	RunDir      string
	State       RunState
	Outcome     plan.Outcome
	Reason      plan.Reason
	CreatedAt   time.Time
	StartedAt   time.Time
	EndedAt     time.Time
	// StoppedAt is when the run was stopped early, and StopReason why, as
	// its stop record has it (StopRun), or the zero time and "".
	StoppedAt  time.Time
	StopReason plan.Reason
}

// summaryColumns are what scanSummary reads of a run.
const summaryColumns = "id, name, location, plan_version, run_dir, state, outcome, reason, created_at, started_at, ended_at, stopped_at, stop_reason"

func scanSummary(row scanner) (RunSummary, error) {
	var (
		r                           RunSummary
		number, created             int64
		state                       string
		outcome, reason, stopReason sql.NullString
		started, ended, stopped     sql.NullInt64
	)
	if err := row.Scan(&number, &r.Name, &r.Location, &r.PlanVersion, &r.RunDir, &state, &outcome, &reason, &created, &started, &ended, &stopped, &stopReason); err != nil {
		return RunSummary{}, err
	}
	r.ID, r.State, r.Outcome, r.Reason = runID(number), RunState(state), plan.Outcome(outcome.String), plan.Reason(reason.String)
	r.CreatedAt, r.StartedAt, r.EndedAt = time.UnixMicro(created).UTC(), timeOf(started), timeOf(ended)
	r.StoppedAt, r.StopReason = timeOf(stopped), plan.Reason(stopReason.String)
	return r, nil
}

// RunSummary returns what a list of runs shows of the run id, which it
// reads whatever release stored it: ErrNotFound for a run the store
// hasn't.
func (tx *Tx) RunSummary(id string) (*RunSummary, error) {
	n, ok := runNumber(id)
	if !ok {
		return nil, ErrNotFound
	}
	r, err := scanSummary(tx.queryRow("SELECT "+summaryColumns+" FROM meridian_runs WHERE id = ?", n))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// RecentRuns returns the store's last limit runs, the newest first.
func (tx *Tx) RecentRuns(limit int) ([]RunSummary, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("store: %d runs", limit)
	}
	rows, err := tx.query("SELECT "+summaryColumns+" FROM meridian_runs ORDER BY id DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunSummary
	for rows.Next() {
		r, err := scanSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
