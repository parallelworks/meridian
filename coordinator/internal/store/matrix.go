package store

import (
	"database/sql"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// SetExpansion records x, a matrix job's instances as run state holds them
// (plan.Expansion.Split), once the job's supervisor expanded its matrix at
// Ready. A job that has ended, or whose instances are recorded already, is
// ErrState.
func (tx *Tx) SetExpansion(run, key string, x *plan.Expansion) error {
	body, err := workerapi.Marshal(x)
	if err != nil {
		return err
	}
	n, ok := runNumber(run)
	if !ok {
		return ErrNotFound
	}
	res, err := tx.exec("UPDATE meridian_jobs SET expansion = ? WHERE run_id = ? AND key = ? AND outcome IS NULL AND expansion IS NULL",
		body, n, key)
	if err != nil {
		return err
	}
	if changed, err := res.RowsAffected(); err != nil || changed == 1 {
		return err
	}
	if _, err := tx.Job(run, key); err != nil {
		return err
	}
	return fmt.Errorf("%w: run %s's job %q has ended, or has its instances", ErrState, run, key)
}

// Instances returns the instances of the run's matrix job named name, in
// the order of their indexes: none before its supervisor inserts them.
func (tx *Tx) Instances(run, name string) ([]*Job, error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return nil, err
	}
	rows, err := tx.query("SELECT "+jobColumns+" FROM meridian_jobs WHERE run_id = ? AND name = ? AND instance IS NOT NULL ORDER BY instance",
		n, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// InstancesLeft returns how many of the instances of the run's matrix job
// named name haven't ended, and how many of those haven't had their turn
// to be placed yet (GrantTurns).
func (tx *Tx) InstancesLeft(run, name string) (unended, unturned int, err error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return 0, 0, err
	}
	err = tx.queryRow(`SELECT count(*), coalesce(sum(turn_at IS NULL), 0) FROM meridian_jobs
WHERE run_id = ? AND name = ? AND instance IS NOT NULL AND outcome IS NULL`, n, name).Scan(&unended, &unturned)
	return unended, unturned, err
}

// GrantTurns gives each of the run's jobs keys that hasn't one its turn to
// be placed, as its matrix's max-parallel: allows, and returns the keys of
// those it gave one now.
func (tx *Tx) GrantTurns(run string, keys []string) ([]string, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	now, err := tx.now()
	if err != nil {
		return nil, err
	}
	var granted []string
	for _, key := range keys {
		res, err := tx.exec("UPDATE meridian_jobs SET turn_at = ? WHERE run_id = ? AND key = ? AND turn_at IS NULL", now, n, key)
		if err != nil {
			return nil, err
		}
		if changed, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if changed == 1 {
			granted = append(granted, key)
		}
	}
	return granted, nil
}

// MarkFailFast marks the run's matrix job key as one whose instance's
// failure stopped the rest, as its strategy's fail-fast: asks, once, and has
// each attempt of its instances that hasn't ended tell its worker to cancel
// its job, with the reason fail-fast, unless it told it to cancel already,
// keeping what its control asked (tellFailFast); an attempt made later
// tells it from the start (StartControl). It reports whether it marked the
// job now.
func (tx *Tx) MarkFailFast(run, key string) (bool, error) {
	n, ok := runNumber(run)
	if !ok {
		return false, ErrNotFound
	}
	now, err := tx.now()
	if err != nil {
		return false, err
	}
	res, err := tx.exec("UPDATE meridian_jobs SET fail_fast_at = ? WHERE run_id = ? AND key = ? AND fail_fast_at IS NULL", now, n, key)
	if err != nil {
		return false, err
	}
	if marked, err := res.RowsAffected(); err != nil || marked == 0 {
		return false, err
	}
	rows, err := tx.query(`SELECT a.id FROM meridian_attempts a JOIN meridian_jobs j ON j.run_id = a.run_id AND j.key = a.job_key
WHERE a.run_id = ? AND j.name = ? AND j.instance IS NOT NULL AND a.state <> ? ORDER BY a.id`, n, key, AttemptEnded)
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
		if err := tx.TellFailFast(id); err != nil {
			return false, err
		}
	}
	return true, nil
}

// TellFailFast has the attempt id's control ask its worker to cancel its
// job, with the reason fail-fast, unless it asks it to cancel already, as
// for the run's timeout, whose reason it keeps; and keeps what else it
// asked (UpdateControl).
func (tx *Tx) TellFailFast(id string) error {
	_, err := tx.UpdateControl(id, askFailFast)
	return err
}

// askFailFast has c ask its worker to cancel its job, with the reason
// fail-fast, unless it asks it to cancel already, and reports whether that
// changed it.
func askFailFast(c *workerapi.Control) bool {
	if c.Cancel {
		return false
	}
	c.Cancel, c.Reason = true, plan.ReasonFailFast
	return true
}

// An Any is a job other jobs of its run need any instance of (needs:
// <job>:any): the hopper job that waits for one, which the jobs that need
// it wait for, and the instance it chose.
type Any struct {
	// Job is the key of the job needed, and MarkerID the hopper job that
	// waits for one of its instances.
	Job      string
	MarkerID hopper.JobID
	// ChosenAt is when the marker chose, or the zero time before; Instance
	// is the key of the instance chosen, which the jobs that need it read,
	// or "" for the job itself, as for one that never expanded its matrix;
	// and Ended the keys of the instances that had ended by then, whose
	// outputs they read.
	ChosenAt time.Time
	Instance string
	Ended    []string
}

// InsertAny stores that the run's job is needed on any instance, which the
// hopper job marker, inserted in the same transaction, waits for.
func (tx *Tx) InsertAny(run, job string, marker hopper.JobID) error {
	n, err := tx.liveRun(run)
	if err != nil {
		return err
	}
	_, err = tx.exec("INSERT INTO meridian_anys (run_id, job_key, marker_id) VALUES (?, ?, ?)", n, job, marker)
	return err
}

// Any returns what the run's job, needed on any instance, has chosen:
// ErrNotFound for a job no job needs so.
func (tx *Tx) Any(run, job string) (*Any, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	a := Any{Job: job}
	var (
		chosen   sql.NullInt64
		instance sql.NullString
		ended    []byte
	)
	err := tx.queryRow("SELECT marker_id, chosen_at, instance_key, ended_keys FROM meridian_anys WHERE run_id = ? AND job_key = ?", n, job).
		Scan(&a.MarkerID, &chosen, &instance, &ended)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	a.ChosenAt, a.Instance = timeOf(chosen), instance.String
	if ended != nil {
		if err := jsonv2.Unmarshal(ended, &a.Ended, jsonv2.RejectUnknownMembers(true)); err != nil {
			return nil, fmt.Errorf("%w: the instances of run %s's job %q that had ended: %w", ErrMalformed, run, job, err)
		}
	}
	return &a, nil
}

// ChooseAny records the instance the jobs that need the run's job on any
// instance read: instance, its key, or "" for the job itself; and ended,
// the keys of its instances that had ended by then, whose outputs they
// read. One chosen already is ErrState, and a job no job needs so
// ErrNotFound.
func (tx *Tx) ChooseAny(run, job, instance string, ended []string) error {
	n, ok := runNumber(run)
	if !ok {
		return ErrNotFound
	}
	var keys []byte
	if len(ended) > 0 {
		var err error
		if keys, err = jsonv2.Marshal(ended); err != nil {
			return err
		}
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec("UPDATE meridian_anys SET chosen_at = ?, instance_key = ?, ended_keys = ? WHERE run_id = ? AND job_key = ? AND chosen_at IS NULL",
		now, nullString(instance), keys, n, job)
	if err != nil {
		return err
	}
	if changed, err := res.RowsAffected(); err != nil || changed == 1 {
		return err
	}
	if _, err := tx.Any(run, job); err != nil {
		return err
	}
	return fmt.Errorf("%w: run %s's job %q's instance was chosen", ErrState, run, job)
}
