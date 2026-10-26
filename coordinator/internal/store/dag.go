package store

import (
	"database/sql"
	"errors"
)

// MarkJobFailed marks the run as one a job of which failed, as the first
// of its jobs to fail does, in the transaction that ends it, and has each
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
	res, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_runs SET any_job_failed_at = ? WHERE id = ? AND any_job_failed_at IS NULL", now, n)
	if err != nil {
		return false, err
	}
	if marked, err := res.RowsAffected(); err != nil || marked == 0 {
		return false, err
	}
	rows, err := tx.tx.QueryContext(tx.ctx, "SELECT id FROM meridian_attempts WHERE run_id = ? AND state <> ? ORDER BY id", n, AttemptEnded)
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

// StartControl gives the new attempt id, of a run a job of which has
// failed, what each of the run's attempts tells its worker from then on
// (MarkJobFailed), as its first control.
func (tx *Tx) StartControl(id string) error {
	var failed sql.NullInt64
	err := tx.tx.QueryRowContext(tx.ctx, `SELECT r.any_job_failed_at FROM meridian_attempts a JOIN meridian_runs r ON r.id = a.run_id WHERE a.id = ?`, id).
		Scan(&failed)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrNotFound
	case err != nil || !failed.Valid:
		return err
	}
	return tx.tellJobFailed(id)
}

// tellJobFailed adds to what the attempt id's control asks that a job of
// its run failed.
func (tx *Tx) tellJobFailed(id string) error {
	c, err := tx.Control(id)
	if err != nil || c.AnyJobFailed {
		return err
	}
	c.AnyJobFailed = true
	_, err = tx.SetControl(id, c)
	return err
}
