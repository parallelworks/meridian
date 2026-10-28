package store

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// MarkJobFailed marks the run as one a job of which failed, as the first
// failure of the run does, in the transaction that stores it, and has each
// of the run's attempts that hasn't ended tell its worker so
// (workerapi.Control.AnyJobFailed), for its steps whose early-cancel: is
// any-job-failed. It reports whether it marked the run now: false for one
// marked already, whose attempts were told then.
func (tx *Tx) MarkJobFailed(run string) (bool, error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return false, err
	}
	now, err := tx.now()
	if err != nil {
		return false, err
	}
	res, err := tx.exec("UPDATE meridian_runs SET any_job_failed_at = ? WHERE id = ? AND any_job_failed_at IS NULL", now, n)
	if err != nil {
		return false, err
	}
	if marked, err := res.RowsAffected(); err != nil || marked == 0 {
		return false, err
	}
	rows, err := tx.query("SELECT id FROM meridian_attempts WHERE run_id = ? AND state <> ? ORDER BY id", n, AttemptEnded)
	if err != nil {
		return false, err
	}
	var live []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		live = append(live, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	for _, id := range live {
		if err := tx.tellJobFailed(id); err != nil {
			return false, err
		}
	}
	return true, nil
}

// StartControl gives the new attempt id, as its first control, what every
// attempt like it tells its worker from then on: that a job of its run
// failed (MarkJobFailed); for an instance of a matrix job whose instances
// fail-fast: stopped, to cancel its job (MarkFailFast); and for a job a
// step of its run cancelled, to cancel it (CancelJob).
// NewAttempt calls it, so that no attempt misses either, as one made as
// they are marked.
func (tx *Tx) StartControl(id string) error {
	var failed, failFast, cancelled sql.NullInt64
	err := tx.queryRow(`SELECT r.any_job_failed_at, p.fail_fast_at, j.cancelled_at FROM meridian_attempts a
JOIN meridian_runs r ON r.id = a.run_id
JOIN meridian_jobs j ON j.run_id = a.run_id AND j.key = a.job_key
LEFT JOIN meridian_jobs p ON p.run_id = j.run_id AND p.key = j.name AND j.instance IS NOT NULL
WHERE a.id = ?`, id).Scan(&failed, &failFast, &cancelled)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if !failed.Valid && !failFast.Valid && !cancelled.Valid {
		return nil
	}
	// All at one version, the attempt's first control. No step cancels a
	// matrix instance, so fail-fast and a step's cancel never meet.
	_, err = tx.UpdateControl(id, func(c *workerapi.Control) bool {
		changed := failed.Valid && askJobFailed(c)
		changed = failFast.Valid && askFailFast(c) || changed
		return cancelled.Valid && askCancelled(c) || changed
	})
	return err
}

// tellJobFailed adds to what the attempt id's control asks that a job of
// its run failed.
func (tx *Tx) tellJobFailed(id string) error {
	_, err := tx.UpdateControl(id, askJobFailed)
	return err
}

// askJobFailed adds to what c asks that a job of its attempt's run failed,
// and reports whether that changed it.
func askJobFailed(c *workerapi.Control) bool {
	if c.AnyJobFailed {
		return false
	}
	c.AnyJobFailed = true
	return true
}

// RetriesFor reports whether the retry: of the attempt id's job lists
// reason (Options.RetriesFor): an end of the attempt for that reason, as
// its worker lost, or the job past its timeout, may be followed by another
// attempt, and isn't the job's failure, which marks the run only as the
// job ends (MarkJobFailed).
func (tx *Tx) RetriesFor(id string, reason plan.Reason) bool {
	return tx.s.retriesFor != nil && tx.s.retriesFor(tx, id, reason)
}

// markFailedStep marks run, the attempt id's, as one a job of which failed
// (MarkJobFailed) once fresh, the attempt's events AppendEvents has just
// stored, show that a step of it failed, as format 1 marks it, as the step
// fails: a cleanup that finished failed, which is never tried again; or a
// step's try that finished failed, and that the step doesn't try again.
// An attempt claimed at version 2 or later of the worker API, apiVersion,
// says so of the try (workerapi.Event.LastTry). Of one of version 1, the
// store's Options.LastTry says so when the step's retry: is written out,
// or else the next event, which isn't its next try, shows it. A try that
// is tried again isn't the step's failure, nor is a step that ignores its
// errors, which reports success; a failed step of version 1 with no event
// after it is marked as its job ends. Neither is a step of a job whose
// retry: lists timeout, which its job's deadline may have failed, of an
// attempt that another may follow: its job marks the run as it ends
// (RetriesFor).
func (tx *Tx) markFailedStep(id, run string, apiVersion int, fresh []workerapi.Event) error {
	if len(fresh) == 0 || tx.RetriesFor(id, plan.ReasonTimeout) {
		return nil
	}
	for _, e := range fresh {
		if e.Finished && e.Outcome == plan.OutcomeFailure && (e.Cleanup || e.LastTry) {
			_, err := tx.MarkJobFailed(run)
			return err
		}
	}
	if apiVersion >= workerapi.Version2 {
		return nil
	}
	events := fresh
	if before := fresh[0].Seq - 1; before > 0 {
		prev, err := tx.event(id, before)
		if err != nil {
			return err
		}
		events = append([]workerapi.Event{prev}, fresh...)
	}
	for k, e := range events {
		if !e.Finished || e.Outcome != plan.OutcomeFailure {
			continue
		}
		final := tx.s.lastTry != nil && tx.s.lastTry(tx, id, e.Step, e.Try)
		if !final && k+1 < len(events) {
			next := events[k+1]
			final = next.Cleanup || next.Step != e.Step || next.Try != e.Try+1
		}
		if final {
			_, err := tx.MarkJobFailed(run)
			return err
		}
	}
	return nil
}

// event returns the attempt id's event with the sequence number seq.
func (tx *Tx) event(id string, seq int64) (workerapi.Event, error) {
	var body []byte
	err := tx.queryRow("SELECT body FROM meridian_events WHERE attempt_id = ? AND seq = ?", id, seq).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return workerapi.Event{}, ErrNotFound
	}
	if err != nil {
		return workerapi.Event{}, err
	}
	var e workerapi.Event
	if err := workerapi.Unmarshal(body, &e); err != nil {
		return workerapi.Event{}, fmt.Errorf("%w: attempt %s's event %d: %w", ErrMalformed, id, seq, err)
	}
	return e, nil
}
