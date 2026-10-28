package store

import (
	"database/sql"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
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
	// ReadyAt is when the job was first readied, or the zero time before;
	// ReadiedStopped and AnyChoices are what it was readied with then
	// (PinReady).
	ReadyAt        time.Time
	ReadiedStopped bool
	AnyChoices     map[string]string
	// Wake grows each time something its supervisor waits for happens
	// (Wake), and PlaceTries counts the times in a row that a backend
	// couldn't answer as it was placed (PlaceTried).
	Wake       int64
	PlaceTries int
	// Diagnostics are the job's problems: the warnings Ready gave it as it
	// was to run (NoteJob), and then its end's, as Ready's errors or
	// placement's.
	Diagnostics problems.Diagnostics
	// Expansion is a matrix job's instances as run state holds them, once
	// its supervisor expanded its matrix at Ready (SetExpansion), or nil.
	// TurnAt is when an instance was given its turn to be placed
	// (GrantTurns), and FailFastAt when a matrix job's instance's failure
	// stopped the rest (MarkFailFast): each the zero time before.
	Expansion  *plan.Expansion
	TurnAt     time.Time
	FailFastAt time.Time
	// CancelledAt is when a step of the run cancelled the job, through
	// parallelworks/cancel-jobs (CancelJob), or the zero time before; and
	// CancelledBy that step, as /jobs/<its job's key>/steps/<its index>,
	// as in /jobs/build#1/steps/2, or, for a step of a run of the run's
	// tree, which can cancel a root's jobs, that run's ID, a colon and
	// that, as in 7:/jobs/main/steps/1.
	CancelledAt time.Time
	CancelledBy string
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
	_, err = tx.exec("INSERT INTO meridian_jobs (run_id, key, name, instance, supervisor_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
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
  outcome, reason, result, ended_at, ready_at, readied_stopped, any_choices, wake, place_tries, diagnostics,
  expansion, turn_at, fail_fast_at, cancelled_at, cancelled_by`

// Job returns the run's job with the key key.
func (tx *Tx) Job(run, key string) (*Job, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	j, err := scanJob(tx.queryRow("SELECT "+jobColumns+" FROM meridian_jobs WHERE run_id = ? AND key = ?", n, key))
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
	rows, err := tx.query("SELECT "+jobColumns+" FROM meridian_jobs WHERE run_id = ? ORDER BY key", n)
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
		j                              Job
		run, created                   int64
		instance, ended, ready, halted sql.NullInt64
		turn, failFast, cancelled      sql.NullInt64
		outcome, reason, cancelledBy   sql.NullString
		result, choices, diags, x      []byte
	)
	if err := row.Scan(&run, &j.Key, &j.Name, &instance, &j.SupervisorID, &j.Fence, &j.Attempts, &j.AttemptsStarted, &created,
		&outcome, &reason, &result, &ended, &ready, &halted, &choices, &j.Wake, &j.PlaceTries, &diags,
		&x, &turn, &failFast, &cancelled, &cancelledBy); err != nil {
		return nil, err
	}
	j.Run, j.CreatedAt, j.EndedAt = runID(run), time.UnixMicro(created).UTC(), timeOf(ended)
	j.ReadyAt, j.ReadiedStopped = timeOf(ready), halted.Int64 != 0
	j.TurnAt, j.FailFastAt = timeOf(turn), timeOf(failFast)
	j.CancelledAt, j.CancelledBy = timeOf(cancelled), cancelledBy.String
	if x != nil {
		j.Expansion = new(plan.Expansion)
		if err := workerapi.Unmarshal(x, j.Expansion); err != nil {
			return nil, fmt.Errorf("%w: the instances of run %s's job %q: %w", ErrMalformed, j.Run, j.Key, err)
		}
	}
	if choices != nil {
		if err := jsonv2.Unmarshal(choices, &j.AnyChoices, jsonv2.RejectUnknownMembers(true)); err != nil {
			return nil, fmt.Errorf("%w: the instances that met run %s's job %q's needs: %w", ErrMalformed, j.Run, j.Key, err)
		}
	}
	var err error
	if j.Diagnostics, err = decodeDiagnostics(diags); err != nil {
		return nil, fmt.Errorf("%w: the diagnostics of run %s's job %q: %w", ErrMalformed, j.Run, j.Key, err)
	}
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
	// Diagnostics are the job's problems, as Ready's or placement's, which
	// never show a value made from a variable or a password.
	Diagnostics problems.Diagnostics
}

// NoteJob adds diags, warnings that Ready gave the run's job as it was to
// run, such as job_ssh_host_empty, to its problems, but for those it has
// already, as when it is readied again for another attempt. Its end keeps
// them, before its own (EndJob). Like an end's, they never show a value
// made from a variable or a password. A job that has ended is ErrState.
func (tx *Tx) NoteJob(run, key string, diags problems.Diagnostics) error {
	j, err := tx.Job(run, key)
	if err != nil {
		return err
	}
	if j.Outcome != "" {
		return fmt.Errorf("%w: run %s's job %q has ended", ErrState, run, key)
	}
	// A diagnostic is told from another by its encoding, as the store
	// holds it, in which a param's number reads back as it was written.
	had := map[string]bool{}
	for _, d := range j.Diagnostics {
		data, err := encodeDiagnostics(problems.Diagnostics{d})
		if err != nil {
			return err
		}
		had[string(data)] = true
	}
	all := j.Diagnostics
	for _, d := range diags {
		data, err := encodeDiagnostics(problems.Diagnostics{d})
		if err != nil {
			return err
		}
		if !had[string(data)] {
			had[string(data)] = true
			all = append(all, d)
		}
	}
	if len(all) == len(j.Diagnostics) {
		return nil
	}
	data, err := encodeDiagnostics(all)
	if err != nil {
		return err
	}
	n, _ := runNumber(run)
	_, err = tx.exec("UPDATE meridian_jobs SET diagnostics = ? WHERE run_id = ? AND key = ?", data, n, key)
	return err
}

// EndJob records how the run's job ended, with its end's problems after
// those it has (NoteJob). A job that has ended already, or that has an
// attempt that hasn't, is ErrState.
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
	var diags []byte
	if all := slices.Concat(j.Diagnostics, end.Diagnostics); len(all) > 0 {
		if diags, err = encodeDiagnostics(all); err != nil {
			return err
		}
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	n, _ := runNumber(run)
	if _, err = tx.exec("UPDATE meridian_jobs SET outcome = ?, reason = ?, result = ?, diagnostics = ?, ended_at = ? WHERE run_id = ? AND key = ?",
		end.Result.Outcome, nullString(string(end.Reason)), result, diags, now, n, key); err != nil {
		return err
	}
	return tx.addEvent(n, EventJobFinished, key, "", 0)
}

// PinReady records, the first time the run's job is readied, what it was
// readied with: whether the run had been stopped, and for each need on any
// instance of a matrix job, the instance that met it, by job name. It
// returns the job, with what it was first readied with, which later calls
// give again whatever they are given, so that Ready, run again, gives the
// same job. A job that has ended is ErrState.
func (tx *Tx) PinReady(run, key string, stopped bool, anyChoices map[string]string) (*Job, error) {
	j, err := tx.Job(run, key)
	switch {
	case err != nil:
		return nil, err
	case j.Outcome != "":
		return nil, fmt.Errorf("%w: run %s's job %q has ended", ErrState, run, key)
	case !j.ReadyAt.IsZero():
		return j, nil
	}
	var choices []byte
	if len(anyChoices) > 0 {
		if choices, err = jsonv2.Marshal(anyChoices, jsonv2.Deterministic(true)); err != nil {
			return nil, err
		}
	}
	now, err := tx.now()
	if err != nil {
		return nil, err
	}
	n, _ := runNumber(run)
	if _, err := tx.exec("UPDATE meridian_jobs SET ready_at = ?, readied_stopped = ?, any_choices = ? WHERE run_id = ? AND key = ?",
		now, stopped, choices, n, key); err != nil {
		return nil, err
	}
	if err := tx.addEvent(n, EventJobReady, key, "", 0); err != nil {
		return nil, err
	}
	return tx.Job(run, key)
}

// ReadiedJobs returns the jobs that have been readied, and haven't ended,
// of the runs that are running, in order: those whose supervisors wait on
// hopper's schedule, as for a live attempt.
func (tx *Tx) ReadiedJobs() ([]*Job, error) {
	rows, err := tx.query("SELECT "+jobColumns+` FROM meridian_jobs
WHERE outcome IS NULL AND ready_at IS NOT NULL AND run_id IN (SELECT id FROM meridian_runs WHERE state = ?) ORDER BY run_id, key`, RunRunning)
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

// Wake records that something the run's job's supervisor waits for has
// happened, as an attempt's claim or end, and returns the job, whose
// supervisor the caller wakes once the transaction commits. A supervisor
// reads the job's Wake again before it waits, so that one woken as it was
// about to wait doesn't.
func (tx *Tx) Wake(run, key string) (*Job, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	res, err := tx.exec("UPDATE meridian_jobs SET wake = wake + 1 WHERE run_id = ? AND key = ?", n, key)
	if err != nil {
		return nil, err
	}
	if err := oneRow(res, ErrNotFound); err != nil {
		return nil, err
	}
	return tx.Job(run, key)
}

// PlaceTried records that a backend couldn't answer as the run's job was
// placed, and returns how many times in a row one hasn't, which choosing
// where its next attempt is placed (ChoosePlace), or counting it as a
// failure (PlaceFailed), starts again from 0. A job that has ended is
// ErrState.
func (tx *Tx) PlaceTried(run, key string) (int, error) {
	n, ok := runNumber(run)
	if !ok {
		return 0, ErrNotFound
	}
	var tries int
	err := tx.queryRow("UPDATE meridian_jobs SET place_tries = place_tries + 1 WHERE run_id = ? AND key = ? AND outcome IS NULL RETURNING place_tries",
		n, key).Scan(&tries)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.Job(run, key); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("%w: run %s's job %q has ended", ErrState, run, key)
	}
	return tries, err
}

// liveAttempt reports whether the job has an attempt that hasn't ended.
func (tx *Tx) liveAttempt(j *Job) (bool, error) {
	n, _ := runNumber(j.Run)
	var live bool
	err := tx.queryRow("SELECT EXISTS (SELECT 1 FROM meridian_attempts WHERE run_id = ? AND job_key = ? AND state <> ?)",
		n, j.Key, AttemptEnded).Scan(&live)
	return live, err
}
