package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// The kinds of a run's events (RunEvent.Kind), in the order a run's
// happen: it starts; a job is ready once its dependencies have finished,
// placing once a member is chosen for its attempt, and running once the
// attempt's worker has claimed it; its steps start and finish; the attempt
// ends, and the job; the run is stopping, once a person cancels it or it
// runs past its timeout:, if it does; and the run ends.
const (
	EventRunStarted   = "run.started"
	EventJobReady     = "job.ready"
	EventJobPlacing   = "job.placing"
	EventJobRunning   = "job.running"
	EventStepStarted  = "step.started"
	EventStepFinished = "step.finished"
	EventAttemptEnded = "attempt.ended"
	EventJobFinished  = "job.finished"
	EventRunStopping  = "run.stopping"
	EventRunFinished  = "run.finished"
)

// A RunEvent is one of a run's events, as the store keeps them: added in
// the transaction that made the change it reports, and numbered in the
// order of those transactions, so that whoever follows the run, in any
// process, sees the same stream (RunEvents). What it reports is read as it
// is now, from the row that changed.
type RunEvent struct {
	// Seq numbers the run's events, from 1, one more each time, and At is
	// when the event was added, by the store's clock.
	Seq  int64
	Kind string
	At   time.Time
	// Job is the key of the job, or the matrix instance, of a job's, an
	// attempt's or a step's event; Attempt the attempt's number, of an
	// attempt's or a step's.
	Job     string
	Attempt int
	// Group and Member are where the attempt was placed, of an attempt's
	// event, or "" where that is private, or not chosen yet.
	Group  string
	Member string
	// Step is a step's event, as the attempt's worker reported it: its
	// outputs masked, as its logs are.
	Step *workerapi.Event
	// End is how the attempt ended, of attempt.ended.
	End *AttemptEnd
	// Outcome and Reason are how the job ended, of job.finished, or the
	// run, of run.finished, and Diagnostics their problems: the job's, as
	// Ready's or placement's, or the run's. Reason is why the run stopped,
	// of run.stopping: cancelled or timeout.
	Outcome     plan.Outcome
	Reason      plan.Reason
	Diagnostics problems.Diagnostics
}

// addEvent adds an event of kind to the run numbered n, after its last:
// about its job job, or "", its attempt attempt, or "", and that attempt's
// event numbered step, or 0.
func (tx *Tx) addEvent(n int64, kind, job, attempt string, step int64) error {
	now, err := tx.now()
	if err != nil {
		return err
	}
	var seq int64
	if err := tx.queryRow("UPDATE meridian_runs SET events_seq = events_seq + 1 WHERE id = ? RETURNING events_seq", n).Scan(&seq); err != nil {
		return err
	}
	if _, err := tx.exec("INSERT INTO meridian_run_events (run_id, seq, kind, job_key, attempt_id, step_seq, at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		n, seq, kind, nullString(job), nullString(attempt), sql.NullInt64{Int64: step, Valid: step > 0}, now); err != nil {
		return err
	}
	tx.events = true
	return nil
}

// RunEvents returns the run's events after the one numbered after, at most
// limit of them, in order, each with what it reports as it is now:
// ErrNotFound for a run the store hasn't. Read in one View, a run that has
// ended has its run.finished event among them, last.
func (tx *Tx) RunEvents(run string, after int64, limit int) ([]RunEvent, error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("store: %d of a run's events", limit)
	}
	rows, err := tx.query(`SELECT e.seq, e.kind, e.job_key, e.attempt_id, e.at, s.body
FROM meridian_run_events e LEFT JOIN meridian_events s ON s.attempt_id = e.attempt_id AND s.seq = e.step_seq
WHERE e.run_id = ? AND e.seq > ? ORDER BY e.seq LIMIT ?`, n, after, limit)
	if err != nil {
		return nil, err
	}
	type row struct {
		event   RunEvent
		attempt string
	}
	var read []row
	for rows.Next() {
		var (
			r            row
			job, attempt sql.NullString
			at           int64
			body         []byte
		)
		if err := rows.Scan(&r.event.Seq, &r.event.Kind, &job, &attempt, &at, &body); err != nil {
			rows.Close()
			return nil, err
		}
		r.event.Job, r.attempt, r.event.At = job.String, attempt.String, time.UnixMicro(at).UTC()
		if body != nil {
			r.event.Step = new(workerapi.Event)
			if err := workerapi.Unmarshal(body, r.event.Step); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: run %s's event %d: %w", ErrMalformed, run, r.event.Seq, err)
			}
		}
		read = append(read, r)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]RunEvent, len(read))
	attempts := map[string]*Attempt{}
	for i, r := range read {
		e := r.event
		if r.attempt != "" {
			a, ok := attempts[r.attempt]
			if !ok {
				if a, err = tx.Attempt(r.attempt); err != nil {
					return nil, err
				}
				attempts[r.attempt] = a
			}
			e.Attempt, e.Group, e.Member = a.Number, a.Placement.Group, a.Placement.Member
			if e.Kind == EventAttemptEnded {
				end := a.End
				e.End = &end
			}
		}
		switch e.Kind {
		case EventJobFinished:
			j, err := tx.Job(run, e.Job)
			if err != nil {
				return nil, err
			}
			e.Outcome, e.Reason, e.Diagnostics = j.Outcome, j.Reason, j.Diagnostics
		case EventRunStopping:
			r, err := tx.Run(run)
			if err != nil {
				return nil, err
			}
			e.Reason = r.StopReason
		case EventRunFinished:
			r, err := tx.Run(run)
			if err != nil {
				return nil, err
			}
			e.Outcome, e.Reason, e.Diagnostics = r.Outcome, r.Reason, r.Diagnostics
		}
		out[i] = e
	}
	return out, nil
}
