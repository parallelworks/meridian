package store

import (
	"bytes"
	"database/sql"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A RunState is where a run is in its life.
type RunState string

// The states of a run.
const (
	// RunCreated is a run stored, whose Start hasn't kept its part yet.
	RunCreated RunState = "created"
	// RunRunning is a run whose Start has kept its part.
	RunRunning RunState = "running"
	// RunEnded is a run that has ended, with its outcome.
	RunEnded RunState = "ended"
)

// A Run is a run as the store holds it.
type Run struct {
	// ID is the run's number in the store, from 1, in decimal.
	ID        string
	CreatedAt time.Time
	// Plan is the run's plan, as plan.Encode encoded it, which never
	// changes; PlanVersion is its version, and PlanDigest its digest
	// (plan.Digest).
	Plan        []byte
	PlanVersion int
	PlanDigest  string
	// Name, Location and Release are the plan's: the run's name, its
	// workflow's location, and the release of Meridian that compiled it.
	Name     string
	Location string
	Release  string
	// DefaultGroup is the group a job runs in whose runs-on: names none,
	// and RunDir the run's directory.
	DefaultGroup string
	RunDir       string
	// WorkflowID is the run's hopper workflow. hopper deletes a workflow's
	// jobs some time after they finish (hopper.Config.CompletedRetention),
	// so it names the workflow only while the run is underway: what the
	// coordinator knows of a run is in its own tables.
	WorkflowID hopper.JobID
	// RecordGeneration is the generation of the run's secret record: 0
	// before it has one, 1 once it is stored, and one more each time it is
	// sealed again (Record).
	RecordGeneration uint64
	State            RunState
	// Outcome and Reason are how the run ended, once it has.
	Outcome   plan.Outcome
	Reason    plan.Reason
	StartedAt time.Time
	EndedAt   time.Time
	// StartState is what run state holds of what Start gave, once Start
	// has kept its part, or nil, and StartTries how many times the
	// coordinator tried to start the run and couldn't.
	StartState *compile.StartState
	StartTries int
	// Diagnostics are the run's problems: Start's, and the coordinator's.
	Diagnostics problems.Diagnostics
	// AnyJobFailedAt is when the first job of the run to fail ended, or the
	// zero time before (MarkJobFailed).
	AnyJobFailedAt time.Time
	// StoppedAt is when the run was stopped early, and StopReason why,
	// cancelled or timeout: its stop record (StopRun), or the zero time
	// and "".
	StoppedAt  time.Time
	StopReason plan.Reason
	// Parent is a child run's link to the try of the step that called it,
	// or nil for a run no step called.
	Parent *Link
	// FinisherID is the run's finisher, a hopper job, or zero for a run
	// stored before the store kept it; and WaitedAt is when it began to
	// wait for the runs of the run's tree to end (WaitForDescendants), or
	// the zero time.
	FinisherID hopper.JobID
	WaitedAt   time.Time
}

// A NewRun is a run to store.
type NewRun struct {
	// ID is the run's ID, which NewRunID gave.
	ID string
	// Plan is the run's plan. The store keeps its encoding, its version
	// and its digest, and its name, location and release.
	Plan         *plan.Plan
	DefaultGroup string
	RunDir       string
	// WorkflowID is the run's hopper workflow, inserted in the same
	// transaction, and FinisherID its finisher.
	WorkflowID hopper.JobID
	FinisherID hopper.JobID
	// Parent is a child run's link to the try of the step that called it,
	// whose attempt and runs are the store's, or nil.
	Parent *Link
}

// NewRunID gives the next run's ID: one more than the last the store gave,
// which no run of the store has had. Nothing has the number until the
// transaction commits, and a transaction rolled back gives it back.
func (tx *Tx) NewRunID() (string, error) {
	var last string
	err := tx.queryRow(`UPDATE meridian_store SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
WHERE key = 'last_run' RETURNING value`).Scan(&last)
	if err != nil {
		return "", err
	}
	n, ok := runNumber(last)
	if !ok {
		return "", fmt.Errorf("%w: the last run's number is %q", ErrMalformed, last)
	}
	return runID(n), nil
}

// InsertRun stores a run, in the state created. Its ID is one NewRunID
// gave that no run has.
func (tx *Tx) InsertRun(r NewRun) error {
	n, ok := runNumber(r.ID)
	if !ok {
		return fmt.Errorf("store: %q isn't a run's ID", r.ID)
	}
	switch {
	case r.Plan == nil:
		return errors.New("store: a run without a plan")
	case r.DefaultGroup == "":
		return errors.New("store: a run without a default group")
	case r.RunDir == "":
		return errors.New("store: a run without a directory")
	case r.WorkflowID.IsZero():
		return errors.New("store: a run without a hopper workflow")
	}
	// A run stored without its finisher, as a test's, has none to wake.
	var finisher any
	if !r.FinisherID.IsZero() {
		finisher = r.FinisherID
	}
	var link struct {
		run, root   sql.NullInt64
		job, attmpt sql.NullString
		step, try   sql.NullInt64
		depth       int
	}
	if l := r.Parent; l != nil {
		if err := l.check(); err != nil {
			return err
		}
		parent, ok := runNumber(l.Run)
		rootN, rootOK := runNumber(l.Root)
		if !ok || !rootOK {
			return fmt.Errorf("store: a child run's link to run %q, of the tree of %q", l.Run, l.Root)
		}
		link.run, link.root = sql.NullInt64{Int64: parent, Valid: true}, sql.NullInt64{Int64: rootN, Valid: true}
		link.job, link.attmpt = sql.NullString{String: l.Job, Valid: true}, sql.NullString{String: l.Attempt, Valid: true}
		link.step, link.try = sql.NullInt64{Int64: int64(l.Step), Valid: true}, sql.NullInt64{Int64: int64(l.Try), Valid: true}
		link.depth = l.Depth
	}
	encoded, err := plan.Encode(r.Plan)
	if err != nil {
		return err
	}
	digest, err := plan.Digest(r.Plan)
	if err != nil {
		return err
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec(`INSERT INTO meridian_runs (id, created_at, plan_version, plan_digest, plan,
  name, location, release, default_group, run_dir, workflow_id, state, finisher_id,
  parent_run, parent_job, parent_attempt, parent_step, parent_try, root_run, depth)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
WHERE ? <= (SELECT CAST(value AS INTEGER) FROM meridian_store WHERE key = 'last_run')`,
		n, now, r.Plan.Version, digest, encoded,
		r.Plan.Run.Name, r.Plan.Run.Source.Location, r.Plan.Run.Release, r.DefaultGroup, r.RunDir, r.WorkflowID, RunCreated, finisher,
		link.run, link.job, link.attmpt, link.step, link.try, link.root, link.depth, n)
	if err != nil {
		return err
	}
	return oneRow(res, fmt.Errorf("store: the run ID %s wasn't given by NewRunID", r.ID))
}

const runColumns = `id, created_at, plan, plan_version, plan_digest, name, location, release,
  default_group, run_dir, workflow_id, record_gen, state, outcome, reason, started_at, ended_at,
  start_state, start_tries, diagnostics, any_job_failed_at, stopped_at, stop_reason,
  finisher_id, waited_at, parent_run, parent_job, parent_attempt, parent_step, parent_try, root_run, depth`

// Run returns the run with the ID id.
func (tx *Tx) Run(id string) (*Run, error) {
	n, ok := runNumber(id)
	if !ok {
		return nil, ErrNotFound
	}
	var (
		r               Run
		number          int64
		created         int64
		outcome, reason sql.NullString
		started, ended  sql.NullInt64
		jobFailed       sql.NullInt64
		stopped         sql.NullInt64
		stopReason      sql.NullString
		state           string
		startState      []byte
		diags           []byte
		waited          sql.NullInt64
		parentRun, root sql.NullInt64
		job, attempt    sql.NullString
		step, try       sql.NullInt64
		depth           int
	)
	err := tx.queryRow("SELECT "+runColumns+" FROM meridian_runs WHERE id = ?", n).Scan(
		&number, &created, &r.Plan, &r.PlanVersion, &r.PlanDigest, &r.Name, &r.Location, &r.Release,
		&r.DefaultGroup, &r.RunDir, &r.WorkflowID, &r.RecordGeneration, &state, &outcome, &reason, &started, &ended,
		&startState, &r.StartTries, &diags, &jobFailed, &stopped, &stopReason,
		&r.FinisherID, &waited, &parentRun, &job, &attempt, &step, &try, &root, &depth)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.ID, r.CreatedAt, r.State = runID(number), time.UnixMicro(created).UTC(), RunState(state)
	r.Outcome, r.Reason = plan.Outcome(outcome.String), plan.Reason(reason.String)
	r.StartedAt, r.EndedAt, r.AnyJobFailedAt = timeOf(started), timeOf(ended), timeOf(jobFailed)
	r.StoppedAt, r.StopReason = timeOf(stopped), plan.Reason(stopReason.String)
	r.WaitedAt = timeOf(waited)
	if parentRun.Valid {
		r.Parent = &Link{
			Run: runID(parentRun.Int64), Job: job.String, Attempt: attempt.String,
			Step: int(step.Int64), Try: int(try.Int64), Root: runID(root.Int64), Depth: depth,
		}
	}
	if startState != nil {
		if r.StartState, err = tx.s.starts.decode(number, startState); err != nil {
			// One of a newer version is a newer release's to read, and is
			// plan.ErrVersion, not malformed.
			if errors.Is(err, plan.ErrVersion) {
				return nil, fmt.Errorf("store: run %s's start state: %w", id, err)
			}
			return nil, fmt.Errorf("%w: run %s's start state: %w", ErrMalformed, id, err)
		}
	}
	if r.Diagnostics, err = decodeDiagnostics(diags); err != nil {
		return nil, fmt.Errorf("%w: run %s's diagnostics: %w", ErrMalformed, id, err)
	}
	return &r, nil
}

// A startCache keeps the start states Tx.Run decoded, by run, each with
// the bytes it was decoded from. A run's start state is written once, as
// the run starts, and holds every matrix instance of the run: decoded on
// every read of the run, as each supervisor's look and each request of the
// worker API reads it, it would cost each of them as much as the run is
// large. A kept start state is shared by whoever reads the run, which
// mustn't change it, as a plan from the coordinator's cache of plans is.
//
// It keeps maxStarts runs' at most, and forgets the one it decoded first to
// keep another: a process that reads more runs than that in rotation
// decodes each start state at each read, as it did before any was kept.
type startCache struct {
	mu      sync.Mutex
	decoded map[int64]keptStart
	order   []int64
}

// A keptStart is a start state, decoded, with the bytes it was decoded
// from.
type keptStart struct {
	raw   []byte
	state *compile.StartState
}

// maxStarts is how many runs' start states a store keeps decoded at most.
const maxStarts = 64

// decode returns raw, run n's start state, decoded: the one kept of the run
// when it was decoded from the same bytes, as one a transaction that rolled
// back may not have been.
func (c *startCache) decode(n int64, raw []byte) (*compile.StartState, error) {
	c.mu.Lock()
	kept, ok := c.decoded[n]
	c.mu.Unlock()
	if ok && bytes.Equal(kept.raw, raw) {
		return kept.state, nil
	}
	state, err := compile.DecodeStartState(raw)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.decoded == nil {
		c.decoded = map[int64]keptStart{}
	}
	if _, ok := c.decoded[n]; !ok {
		if len(c.order) >= maxStarts {
			delete(c.decoded, c.order[0])
			c.order = c.order[1:]
		}
		c.order = append(c.order, n)
	}
	c.decoded[n] = keptStart{raw: raw, state: state}
	return state, nil
}

// decodeDiagnostics decodes a list of diagnostics as encodeDiagnostics
// encodes it, refusing a member a diagnostic doesn't have: nil for none.
func decodeDiagnostics(data []byte) (problems.Diagnostics, error) {
	if data == nil {
		return nil, nil
	}
	var diags problems.Diagnostics
	if err := jsonv2.Unmarshal(data, &diags, jsonv2.RejectUnknownMembers(true)); err != nil {
		return nil, err
	}
	for i, d := range diags {
		if d.Code == "" {
			return nil, fmt.Errorf("diagnostic %d has no code", i)
		}
	}
	return diags, nil
}

// encodeDiagnostics encodes a list of diagnostics, as JSON.
func encodeDiagnostics(diags problems.Diagnostics) ([]byte, error) {
	return jsonv2.Marshal(diags, jsonv2.Deterministic(true))
}

// AddDiagnostics adds diags to the run's problems, after those it has.
func (tx *Tx) AddDiagnostics(id string, diags problems.Diagnostics) error {
	if len(diags) == 0 {
		return nil
	}
	r, err := tx.Run(id)
	if err != nil {
		return err
	}
	data, err := encodeDiagnostics(append(r.Diagnostics, diags...))
	if err != nil {
		return err
	}
	n, _ := runNumber(id)
	_, err = tx.exec("UPDATE meridian_runs SET diagnostics = ? WHERE id = ?", data, n)
	return err
}

// StartTried records that the coordinator tried to start the run and
// couldn't, as when a resolver couldn't answer, and returns how many times
// it has. A run that isn't in the state created is ErrState.
func (tx *Tx) StartTried(id string) (int, error) {
	n, err := tx.runNumberOf(id)
	if err != nil {
		return 0, err
	}
	var tries int
	err = tx.queryRow("UPDATE meridian_runs SET start_tries = start_tries + 1 WHERE id = ? AND state = ? RETURNING start_tries",
		n, RunCreated).Scan(&tries)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: run %s isn't %s", ErrState, id, RunCreated)
	}
	return tries, err
}

// StartRun records that the run's Start has kept its part: what run state
// holds of what it gave, state, and its problems, diags, which are
// warnings. The run is running from now. A run that isn't in the state
// created is ErrState.
func (tx *Tx) StartRun(id string, state *compile.StartState, diags problems.Diagnostics) error {
	if state == nil {
		return errors.New("store: a run started without its start state")
	}
	data, err := compile.EncodeStartState(state)
	if err != nil {
		return err
	}
	n, err := tx.runNumberOf(id)
	if err != nil {
		return err
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec("UPDATE meridian_runs SET state = ?, started_at = ?, start_state = ? WHERE id = ? AND state = ?",
		RunRunning, now, data, n, RunCreated)
	if err != nil {
		return err
	}
	if err := oneRow(res, fmt.Errorf("%w: run %s isn't %s", ErrState, id, RunCreated)); err != nil {
		return err
	}
	if err := tx.addEvent(n, EventRunStarted, "", "", 0); err != nil {
		return err
	}
	return tx.AddDiagnostics(id, diags)
}

// EndRun records how the run ended, and deletes its secret record, its
// jobs' parts of it too (PutJobRecord), which nothing needs once the run
// has ended. The record is erased: overwritten
// with zeros as it is deleted, and, once the transaction commits, gone
// from the write-ahead log too, which the store checkpoints and
// truncates. A run that has ended already, or that has an attempt that
// hasn't, is ErrState.
func (tx *Tx) EndRun(id string, outcome plan.Outcome, reason plan.Reason) error {
	switch outcome {
	case plan.OutcomeSuccess, plan.OutcomeFailure, plan.OutcomeCancelled:
	default:
		return fmt.Errorf("store: %q isn't how a run ends", outcome)
	}
	n, err := tx.runNumberOf(id)
	if err != nil {
		return err
	}
	var live bool
	if err := tx.queryRow("SELECT EXISTS (SELECT 1 FROM meridian_attempts WHERE run_id = ? AND state <> ?)", n, AttemptEnded).Scan(&live); err != nil {
		return err
	}
	if live {
		return fmt.Errorf("%w: run %s has an attempt that hasn't ended", ErrState, id)
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec("UPDATE meridian_runs SET state = ?, outcome = ?, reason = ?, ended_at = ? WHERE id = ? AND state <> ?",
		RunEnded, outcome, nullString(string(reason)), now, n, RunEnded)
	if err != nil {
		return err
	}
	if err := oneRow(res, fmt.Errorf("%w: run %s has ended", ErrState, id)); err != nil {
		return err
	}
	if err := tx.addEvent(n, EventRunFinished, "", "", 0); err != nil {
		return err
	}
	tx.ended = append(tx.ended, id)
	if _, err := tx.exec("DELETE FROM meridian_records WHERE run_id = ?", n); err != nil {
		return err
	}
	if _, err := tx.exec("DELETE FROM meridian_job_records WHERE run_id = ?", n); err != nil {
		return err
	}
	tx.erased = true
	return nil
}

// StopRun stores the run's stop record: that it is stopped early now, for
// reason, cancelled or timeout, with the run's run.stopping event. The
// first stop of a run is its record, and a later one changes nothing: it
// reports whether this one stored it. A run that has ended is ErrState.
func (tx *Tx) StopRun(id string, reason plan.Reason) (bool, error) {
	if reason != plan.ReasonCancelled && reason != plan.ReasonTimeout {
		return false, fmt.Errorf("store: %q isn't why a run stops", reason)
	}
	r, err := tx.Run(id)
	switch {
	case err != nil:
		return false, err
	case r.State == RunEnded:
		return false, fmt.Errorf("%w: run %s has ended", ErrState, id)
	case !r.StoppedAt.IsZero():
		return false, nil
	}
	now, err := tx.now()
	if err != nil {
		return false, err
	}
	n, _ := runNumber(id)
	if _, err := tx.exec("UPDATE meridian_runs SET stopped_at = ?, stop_reason = ? WHERE id = ?", now, reason, n); err != nil {
		return false, err
	}
	return true, tx.addEvent(n, EventRunStopping, "", "", 0)
}

// LiveAttempts returns the run's attempts that haven't ended, in the order
// of their IDs.
func (tx *Tx) LiveAttempts(run string) ([]*Attempt, error) {
	n, err := tx.runNumberOf(run)
	if err != nil {
		return nil, err
	}
	rows, err := tx.query("SELECT id FROM meridian_attempts WHERE run_id = ? AND state <> ? ORDER BY id", n, AttemptEnded)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]*Attempt, 0, len(ids))
	for _, id := range ids {
		a, err := tx.Attempt(id)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// runNumberOf returns the number of the run with the ID id, which the store
// has: ErrNotFound if it hasn't.
func (tx *Tx) runNumberOf(id string) (int64, error) {
	n, ok := runNumber(id)
	if !ok {
		return 0, ErrNotFound
	}
	var exists bool
	if err := tx.queryRow("SELECT EXISTS (SELECT 1 FROM meridian_runs WHERE id = ?)", n).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, ErrNotFound
	}
	return n, nil
}

// liveRun returns the number of the run with the ID id, which the store has
// and which hasn't ended: ErrNotFound if the store hasn't it, ErrState if
// it has ended.
func (tx *Tx) liveRun(id string) (int64, error) {
	n, ok := runNumber(id)
	if !ok {
		return 0, ErrNotFound
	}
	var state string
	err := tx.queryRow("SELECT state FROM meridian_runs WHERE id = ?", n).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if RunState(state) == RunEnded {
		return 0, fmt.Errorf("%w: run %s has ended", ErrState, id)
	}
	return n, nil
}

// A Record is a run's secret record as the store holds it: sealed, under
// a data key of the run's, which Keys wrapped.
type Record struct {
	Run string
	// Generation is the record's generation, which it was sealed with:
	// 1 when it was stored, and one more each time it was sealed again.
	// The store keeps it with the run, apart from the sealed record.
	Generation uint64
	// WrappedKey is the record's data key, as Keys wrapped it, which never
	// changes, and Sealed the record, sealed under it.
	WrappedKey []byte
	Sealed     []byte
	UpdatedAt  time.Time
}

// InsertRecord stores the secret record of a run that has none, and never
// had one: sealed, with its data key wrapped, with generation, which is 1
// for a record first sealed. A run that has ended is ErrState, and so is
// one with a record, or whose record was deleted.
func (tx *Tx) InsertRecord(run string, generation uint64, wrappedKey, sealed []byte) error {
	switch {
	case generation != 1:
		return fmt.Errorf("store: a secret record first stored with generation %d, not 1", generation)
	case len(wrappedKey) == 0 || len(sealed) == 0:
		return errors.New("store: a secret record without its key or its bytes")
	}
	n, err := tx.liveRun(run)
	if err != nil {
		return err
	}
	res, err := tx.exec("UPDATE meridian_runs SET record_gen = 1 WHERE id = ? AND record_gen = 0", n)
	if err != nil {
		return err
	}
	if err := oneRow(res, fmt.Errorf("%w: run %s has had a secret record", ErrState, run)); err != nil {
		return err
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	_, err = tx.exec("INSERT INTO meridian_records (run_id, wrapped_key, sealed, updated_at) VALUES (?, ?, ?, ?)",
		n, wrappedKey, sealed, now)
	return err
}

// Record returns the run's secret record, with its generation:
// ErrNotFound for a run that has none, as one that has ended has none.
func (tx *Tx) Record(run string) (*Record, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	r := Record{Run: run}
	var updated int64
	err := tx.queryRow(`SELECT r.record_gen, s.wrapped_key, s.sealed, s.updated_at
FROM meridian_records s JOIN meridian_runs r ON r.id = s.run_id WHERE s.run_id = ?`, n).Scan(
		&r.Generation, &r.WrappedKey, &r.Sealed, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.UpdatedAt = time.UnixMicro(updated).UTC()
	return &r, nil
}

// Reseal replaces the run's sealed secret record with sealed, sealed again
// under the same data key with generation, which is one more than the
// record's: the record and its generation change together, or neither
// does. A generation that isn't the next is ErrState and ErrRecordChanged,
// as a reseal that raced another's is; a run without a record is
// ErrNotFound.
func (tx *Tx) Reseal(run string, generation uint64, sealed []byte) error {
	if len(sealed) == 0 {
		return errors.New("store: a secret record without its bytes")
	}
	current, err := tx.Record(run)
	if err != nil {
		return err
	}
	if generation != current.Generation+1 {
		return fmt.Errorf("%w: %w: generation %d isn't the next of run %s's secret record, %d", ErrState, ErrRecordChanged, generation, run, current.Generation+1)
	}
	n, _ := runNumber(run)
	now, err := tx.now()
	if err != nil {
		return err
	}
	// Only from the generation read, so that a reseal that raced this one
	// fails, where the database runs transactions side by side.
	res, err := tx.exec("UPDATE meridian_runs SET record_gen = ? WHERE id = ? AND record_gen = ?", generation, n, current.Generation)
	if err != nil {
		return err
	}
	if err := oneRow(res, fmt.Errorf("%w: %w: run %s", ErrState, ErrRecordChanged, run)); err != nil {
		return err
	}
	_, err = tx.exec("UPDATE meridian_records SET sealed = ?, updated_at = ? WHERE run_id = ?", sealed, now, n)
	return err
}

// oneRow returns nil if res changed one row, and otherwise err.
func oneRow(res sql.Result, err error) error {
	n, rerr := res.RowsAffected()
	if rerr != nil {
		return rerr
	}
	if n != 1 {
		return err
	}
	return nil
}

// nullString is s, or NULL for "".
func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// WrappedKey returns the data key of the run's secret record, as Keys
// wrapped it (Record.WrappedKey), without the record: ErrNotFound for a
// run that has none.
func (tx *Tx) WrappedKey(run string) ([]byte, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	var key []byte
	err := tx.queryRow("SELECT wrapped_key FROM meridian_records WHERE run_id = ?", n).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return key, err
}

// A JobRecord is a job's part of a run's secret record, as the store holds
// it: sealed under a key that the run's data key (Record.WrappedKey)
// derives, with its generation, which the store keeps with the job, apart from the part.
type JobRecord struct {
	Generation uint64
	Sealed     []byte
}

// JobRecords returns the parts of the run's secret record of those of the
// jobs keys that have one, by key: a job that hasn't ended with private
// outputs has none.
func (tx *Tx) JobRecords(run string, keys []string) (map[string]JobRecord, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	out := make(map[string]JobRecord, len(keys))
	for _, key := range keys {
		var r JobRecord
		err := tx.queryRow(`SELECT j.record_gen, p.sealed FROM meridian_job_records p
JOIN meridian_jobs j ON j.run_id = p.run_id AND j.key = p.job_key WHERE p.run_id = ? AND p.job_key = ?`, n, key).Scan(&r.Generation, &r.Sealed)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[key] = r
	}
	return out, nil
}

// PutJobRecord stores sealed as the run's job's part of its secret record,
// sealed with generation, which is one more than the part's, or 1 for the
// job's first: the part and its generation change together, or neither
// does. A generation that isn't the next is ErrState and ErrRecordChanged,
// as a write that raced another's is; a run that has ended, and has no
// secret record, is ErrState, and a job the run hasn't ErrNotFound.
func (tx *Tx) PutJobRecord(run, job string, generation uint64, sealed []byte) error {
	if len(sealed) == 0 {
		return errors.New("store: a job's part of a secret record without its bytes")
	}
	n, err := tx.liveRun(run)
	if err != nil {
		return err
	}
	var current uint64
	err = tx.queryRow("SELECT record_gen FROM meridian_jobs WHERE run_id = ? AND key = ?", n, job).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if generation != current+1 {
		return fmt.Errorf("%w: %w: generation %d isn't the next of run %s's part of job %q, %d", ErrState, ErrRecordChanged, generation, run, job, current+1)
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	// Only from the generation read, so that a write that raced this one
	// fails, where the database runs transactions side by side.
	res, err := tx.exec("UPDATE meridian_jobs SET record_gen = ? WHERE run_id = ? AND key = ? AND record_gen = ?", generation, n, job, current)
	if err != nil {
		return err
	}
	if err := oneRow(res, fmt.Errorf("%w: %w: run %s's part of job %q", ErrState, ErrRecordChanged, run, job)); err != nil {
		return err
	}
	_, err = tx.exec(`INSERT INTO meridian_job_records (run_id, job_key, sealed, updated_at) VALUES (?, ?, ?, ?)
ON CONFLICT (run_id, job_key) DO UPDATE SET sealed = excluded.sealed, updated_at = excluded.updated_at`, n, job, sealed, now)
	return err
}
