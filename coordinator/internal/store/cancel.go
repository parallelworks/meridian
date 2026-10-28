package store

import (
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// CancelJob marks the run's job key as cancelled by a step of the run,
// through parallelworks/cancel-jobs: by, as /jobs/<its job's key>/steps/<its
// index>. It marks a job once, and not one that has ended. Each attempt
// of the job that hasn't ended tells its worker to cancel its job, with
// the reason cancelled, unless it tells it to cancel already, keeping what
// else its control asks (askCancelled); an attempt made later tells it
// from the start (StartControl). It reports whether it marked the job now,
// and returns the job as it is then.
func (tx *Tx) CancelJob(run, key, by string) (bool, *Job, error) {
	n, ok := runNumber(run)
	if !ok {
		return false, nil, ErrNotFound
	}
	now, err := tx.now()
	if err != nil {
		return false, nil, err
	}
	res, err := tx.exec("UPDATE meridian_jobs SET cancelled_at = ?, cancelled_by = ? WHERE run_id = ? AND key = ? AND cancelled_at IS NULL AND outcome IS NULL",
		now, by, n, key)
	if err != nil {
		return false, nil, err
	}
	marked, err := res.RowsAffected()
	if err != nil {
		return false, nil, err
	}
	j, err := tx.Job(run, key)
	if err != nil || marked == 0 {
		return false, j, err
	}
	rows, err := tx.query("SELECT id FROM meridian_attempts WHERE run_id = ? AND job_key = ? AND state <> ? ORDER BY id", n, key, AttemptEnded)
	if err != nil {
		return false, nil, err
	}
	var live []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, nil, err
		}
		live = append(live, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, nil, err
	}
	for _, id := range live {
		if _, err := tx.UpdateControl(id, askCancelled); err != nil {
			return false, nil, err
		}
	}
	return true, j, nil
}

// askCancelled has c ask its worker to cancel its job, with the reason
// cancelled and the worker's own grace, unless it asks it to cancel
// already, as for the run's stop, whose reason it keeps; and reports
// whether that changed it.
func askCancelled(c *workerapi.Control) bool {
	if c.Cancel {
		return false
	}
	c.Cancel, c.Reason, c.GraceSeconds = true, plan.ReasonCancelled, 0
	return true
}
