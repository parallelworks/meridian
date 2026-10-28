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
	// Parent is a child run's link to the try of the step that called it
	// (Run.Parent), or nil for a run no step called, and ParentAttempt the
	// number of the attempt that ran the step; and Descendants how many runs
	// of a root's tree there are below it, at any depth.
	Parent        *Link
	ParentAttempt int
	Descendants   int
}

// summaryColumns are what scanSummary reads of a run.
const summaryColumns = `id, name, location, plan_version, run_dir, state, outcome, reason, created_at, started_at, ended_at, stopped_at, stop_reason,
  parent_run, parent_job, parent_attempt, parent_step, parent_try, root_run, depth,
  (SELECT a.number FROM meridian_attempts a WHERE a.id = meridian_runs.parent_attempt),
  (SELECT count(*) FROM meridian_runs d WHERE d.root_run = meridian_runs.id)`

func scanSummary(row scanner) (RunSummary, error) {
	var (
		r                           RunSummary
		number, created             int64
		state                       string
		outcome, reason, stopReason sql.NullString
		started, ended, stopped     sql.NullInt64
		parent, root, step, try     sql.NullInt64
		job, attempt                sql.NullString
		depth                       int
		attemptNumber               sql.NullInt64
	)
	if err := row.Scan(&number, &r.Name, &r.Location, &r.PlanVersion, &r.RunDir, &state, &outcome, &reason, &created, &started, &ended, &stopped, &stopReason,
		&parent, &job, &attempt, &step, &try, &root, &depth, &attemptNumber, &r.Descendants); err != nil {
		return RunSummary{}, err
	}
	r.ID, r.State, r.Outcome, r.Reason = runID(number), RunState(state), plan.Outcome(outcome.String), plan.Reason(reason.String)
	r.CreatedAt, r.StartedAt, r.EndedAt = time.UnixMicro(created).UTC(), timeOf(started), timeOf(ended)
	r.StoppedAt, r.StopReason = timeOf(stopped), plan.Reason(stopReason.String)
	if parent.Valid {
		r.Parent = &Link{
			Run: runID(parent.Int64), Job: job.String, Attempt: attempt.String, Step: int(step.Int64), Try: int(try.Int64),
			Root: runID(root.Int64), Depth: depth,
		}
		r.ParentAttempt = int(attemptNumber.Int64)
	}
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

// RecentRuns returns the store's last limit runs that no step called, the
// newest first, or, with all, of every run, the runs steps called among
// them.
func (tx *Tx) RecentRuns(limit int, all bool) ([]RunSummary, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("store: %d runs", limit)
	}
	return tx.summaries("SELECT "+summaryColumns+" FROM meridian_runs WHERE ? OR parent_run IS NULL ORDER BY id DESC LIMIT ?", all, limit)
}

// Children returns the runs the steps of the run's attempts called, its
// children, in the order of their IDs: ErrNotFound for a run the store
// hasn't.
func (tx *Tx) Children(run string) ([]RunSummary, error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return nil, err
	}
	return tx.summaries("SELECT "+summaryColumns+" FROM meridian_runs WHERE parent_attempt IN (SELECT id FROM meridian_attempts WHERE run_id = ?) ORDER BY id", n)
}

// summaries returns the runs query, with args, gives.
func (tx *Tx) summaries(query string, args ...any) ([]RunSummary, error) {
	rows, err := tx.query(query, args...)
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
