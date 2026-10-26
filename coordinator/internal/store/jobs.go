package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// A Job is a job of a run, or an instance of a matrix job, as the store
// holds it.
type Job struct {
	Run string
	// Key is the job's name, or an instance's: the name, # and its index,
	// as in build#2. Name is the job's name in the plan, and Instance the
	// instance's index, or -1 for a job that isn't an instance.
	Key      string
	Name     string
	Instance int
	// SupervisorID is the hopper job that supervises it.
	SupervisorID hopper.JobID
	// Fence is the fence of its latest attempt, or 0 before its first:
	// the attempt with it holds the job while neither has ended. Attempts
	// counts its attempts, and AttemptsStarted those that started a step,
	// which its retry: has used.
	Fence           int64
	Attempts        int
	AttemptsStarted int
	CreatedAt       time.Time
	// Outcome is how the job ended, or "" until it has, Reason why it
	// ended other than as its steps did, and Result how it ended as run
	// state holds it (plan.JobResult.Split), or nil until it has.
	Outcome plan.Outcome
	Reason  plan.Reason
	Result  *plan.JobResult
	EndedAt time.Time
}

// A NewJob is a job, or an instance of a matrix job, to store.
type NewJob struct {
	Run string
	// Name is the job's name in the plan, and Key its key: the name, or
	// for a matrix instance the instance's, its plan.Instance.Key.
	Name string
	Key  string
	// SupervisorID is the hopper job that supervises it, inserted in the
	// same transaction.
	SupervisorID hopper.JobID
}

// InsertJob stores a job of a run that hasn't ended: ErrNotFound for a run
// the store hasn't, and ErrState for one that has ended.
func (tx *Tx) InsertJob(j NewJob) error {
	instance, ok := instanceOf(j.Name, j.Key)
	switch {
	case j.Name == "":
		return errors.New("store: a job without a name")
	case !ok:
		return fmt.Errorf("store: %q is neither the job %q's key nor an instance's", j.Key, j.Name)
	case j.SupervisorID.IsZero():
		return fmt.Errorf("store: the job %q without a supervisor", j.Key)
	}
	n, err := tx.liveRun(j.Run)
	if err != nil {
		return err
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	_, err = tx.tx.ExecContext(tx.ctx, "INSERT INTO meridian_jobs (run_id, key, name, instance, supervisor_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		n, j.Key, j.Name, sql.NullInt64{Int64: int64(instance), Valid: instance >= 0}, j.SupervisorID, now)
	return err
}

// instanceOf returns the index of the instance whose key is key, of the
// job named name, or -1 if key is the job's own; and whether key is
// either.
func instanceOf(name, key string) (int, bool) {
	if key == name {
		return -1, true
	}
	rest, ok := strings.CutPrefix(key, name+"#")
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(rest)
	if err != nil || i < 0 || strconv.Itoa(i) != rest {
		return 0, false
	}
	return i, true
}

const jobColumns = `run_id, key, name, instance, supervisor_id, fence, attempts, attempts_started, created_at,
  outcome, reason, result, ended_at`

// Job returns the run's job with the key key.
func (tx *Tx) Job(run, key string) (*Job, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	j, err := scanJob(tx.tx.QueryRowContext(tx.ctx, "SELECT "+jobColumns+" FROM meridian_jobs WHERE run_id = ? AND key = ?", n, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// Jobs returns the run's jobs, and their instances, in the order of their
// keys: ErrNotFound for a run the store hasn't.
func (tx *Tx) Jobs(run string) ([]*Job, error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return nil, err
	}
	rows, err := tx.tx.QueryContext(tx.ctx, "SELECT "+jobColumns+" FROM meridian_jobs WHERE run_id = ? ORDER BY key", n)
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

// scanner is a row to scan: an *sql.Row or *sql.Rows.
type scanner interface{ Scan(dest ...any) error }

func scanJob(row scanner) (*Job, error) {
	var (
		j               Job
		run, created    int64
		instance, ended sql.NullInt64
		outcome, reason sql.NullString
		result          []byte
	)
	if err := row.Scan(&run, &j.Key, &j.Name, &instance, &j.SupervisorID, &j.Fence, &j.Attempts, &j.AttemptsStarted, &created,
		&outcome, &reason, &result, &ended); err != nil {
		return nil, err
	}
	j.Run, j.CreatedAt, j.EndedAt = runID(run), time.UnixMicro(created).UTC(), timeOf(ended)
	j.Instance = -1
	if instance.Valid {
		j.Instance = int(instance.Int64)
	}
	j.Outcome, j.Reason = plan.Outcome(outcome.String), plan.Reason(reason.String)
	if result != nil {
		j.Result = new(plan.JobResult)
		if err := workerapi.Unmarshal(result, j.Result); err != nil {
			return nil, fmt.Errorf("%w: the result of run %s's job %q: %w", ErrMalformed, j.Run, j.Key, err)
		}
	}
	return &j, nil
}

// A JobEnd is how a job ended.
type JobEnd struct {
	// Result is how it ended, as run state holds it: the state half of
	// plan.JobResult.Split, which holds no private value, since the run's
	// secret record holds those.
	Result plan.JobResult
	// Reason is why it ended other than as its steps did, or "".
	Reason plan.Reason
}

// EndJob records how the run's job ended. A job that has ended already, or
// that has an attempt that hasn't, is ErrState.
func (tx *Tx) EndJob(run, key string, end JobEnd) error {
	switch end.Result.Outcome {
	case plan.OutcomeSuccess, plan.OutcomeFailure, plan.OutcomeCancelled, plan.OutcomeSkipped:
	default:
		return fmt.Errorf("store: %q isn't how a job ends", end.Result.Outcome)
	}
	if record, _ := end.Result.Split(); len(record.Outputs) > 0 || len(record.Steps) > 0 {
		return fmt.Errorf("store: the result of job %q holds private values, which only the run's secret record holds", key)
	}
	result, err := workerapi.Marshal(end.Result)
	if err != nil {
		return err
	}
	j, err := tx.Job(run, key)
	if err != nil {
		return err
	}
	switch live, err := tx.liveAttempt(j); {
	case err != nil:
		return err
	case j.Outcome != "":
		return fmt.Errorf("%w: run %s's job %q has ended", ErrState, run, key)
	case live:
		return fmt.Errorf("%w: run %s's job %q has an attempt that hasn't ended", ErrState, run, key)
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	n, _ := runNumber(run)
	_, err = tx.tx.ExecContext(tx.ctx, "UPDATE meridian_jobs SET outcome = ?, reason = ?, result = ?, ended_at = ? WHERE run_id = ? AND key = ?",
		end.Result.Outcome, nullString(string(end.Reason)), result, now, n, key)
	return err
}

// liveAttempt reports whether the job has an attempt that hasn't ended.
func (tx *Tx) liveAttempt(j *Job) (bool, error) {
	n, _ := runNumber(j.Run)
	var live bool
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT EXISTS (SELECT 1 FROM meridian_attempts WHERE run_id = ? AND job_key = ? AND state <> ?)",
		n, j.Key, AttemptEnded).Scan(&live)
	return live, err
}
