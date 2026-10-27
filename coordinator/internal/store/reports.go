package store

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/workerapi"
)

// A Placement is where an attempt was placed: what its backend placed, and
// the worker binary it started.
type Placement struct {
	// Group and Member are the group and the member it was placed on,
	// Backend the member's backend, and ID the backend's own name for the
	// placement (backend.Placement.ID); or "" where the attempt's placement
	// is private (Attempt.Private), whose coordinator holds them only
	// sealed, since an ID, as a pod's name, can say where it is.
	Group   string
	Member  string
	Backend string
	ID      string
	// WorkerDigest is sha256: and the hex of the SHA-256 digest of the
	// worker binary the placement started, which the worker's claim must
	// give (workerapi.Worker.Digest): every placement recorded has it, so
	// it says the placement is (Attempt.Placed).
	WorkerDigest string
}

// workerDigest is what a worker binary's digest is: sha256: and 64 hex
// digits.
var workerDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// RecordPlacement records where the attempt was placed, which its first
// claim waits for (ErrNotPlaced). A placement without the digest of the
// worker binary it started is refused: every backend says which binary it
// started, so that no claim is taken from another. So is one without its
// backend or the backend's ID for it, unless the attempt's placement is
// private, and one of those with its group, member, backend or ID, which
// are held only sealed: of a private placement, the store holds only the
// digest in the clear. An attempt that no longer holds its job is
// ErrFenced, and one that has been claimed ErrState.
func (tx *Tx) RecordPlacement(id string, p Placement) error {
	if !workerDigest.MatchString(p.WorkerDigest) {
		return fmt.Errorf("store: attempt %s placed without the digest of the worker binary it started", id)
	}
	a, err := tx.Attempt(id)
	if err != nil {
		return err
	}
	switch {
	case !a.Holds:
		return ErrFenced
	case a.State != AttemptPlacing:
		return fmt.Errorf("%w: attempt %s has been claimed", ErrState, id)
	case a.Private && (p.Group != "" || p.Member != "" || p.Backend != "" || p.ID != ""):
		return fmt.Errorf("store: attempt %s's placement is private, and its group, member, backend and the backend's ID for it are held only sealed", id)
	case !a.Private && (p.Backend == "" || p.ID == ""):
		return fmt.Errorf("store: attempt %s placed without its backend or the backend's ID for it", id)
	}
	_, err = tx.tx.ExecContext(tx.ctx, "UPDATE meridian_attempts SET grp = ?, member = ?, backend = ?, placement_id = ?, placed_digest = ? WHERE id = ?",
		nullString(p.Group), nullString(p.Member), nullString(p.Backend), nullString(p.ID), nullString(p.WorkerDigest), id)
	return err
}

// Lapsed reports whether the credential of the attempt a has lapsed now,
// by the store's clock: whether its job fences itself, and its lease has
// lapsed (ErrLapsed). Another job's credential lasts until its attempt
// ends.
func (tx *Tx) Lapsed(a *Attempt) (bool, error) {
	if !a.SelfFencing || a.RenewedAt.IsZero() {
		return false, nil
	}
	now, err := tx.now()
	if err != nil {
		return false, err
	}
	return now >= a.LeaseLapses().UnixMicro(), nil
}

// UpdateControl changes what the attempt id's control asks, as change says,
// keeping what it asked already, at the next version, which it returns: or
// leaves it as it is, at its version, when change reports it changed
// nothing. Every writer of a control goes through it, so that none drops
// what another asked, as a cancel AnyJobFailed. An attempt that has ended
// is ErrState.
func (tx *Tx) UpdateControl(id string, change func(c *workerapi.Control) bool) (int64, error) {
	c, err := tx.Control(id)
	if err != nil {
		return 0, err
	}
	if !change(&c) {
		return c.Version, nil
	}
	version, err := tx.setControl(id, c)
	if err != nil {
		return 0, err
	}
	if tx.controls == nil {
		tx.controls = map[string]bool{}
	}
	tx.controls[id] = true
	return version, nil
}

// setControl asks c of the attempt's worker in place of what it asked, at
// the next version, which it returns: UpdateControl's write. The version
// only grows. An attempt that has ended is ErrState.
func (tx *Tx) setControl(id string, c workerapi.Control) (int64, error) {
	a, err := tx.Attempt(id)
	if err != nil {
		return 0, err
	}
	if a.State == AttemptEnded {
		return 0, fmt.Errorf("%w: attempt %s has ended", ErrState, id)
	}
	var version int64
	if err := tx.tx.QueryRowContext(tx.ctx, "UPDATE meridian_attempts SET control_version = control_version + 1 WHERE id = ? RETURNING control_version",
		id).Scan(&version); err != nil {
		return 0, err
	}
	c.Version = version
	body, err := workerapi.Marshal(c)
	if err != nil {
		return 0, err
	}
	_, err = tx.tx.ExecContext(tx.ctx, "UPDATE meridian_attempts SET control = ? WHERE id = ?", body, id)
	return version, err
}

// Control returns what the coordinator asks of the attempt's worker, at
// its latest version: version 0, which asks nothing, until UpdateControl.
func (tx *Tx) Control(id string) (workerapi.Control, error) {
	var (
		version int64
		body    []byte
	)
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT control_version, control FROM meridian_attempts WHERE id = ?", id).Scan(&version, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return workerapi.Control{}, ErrNotFound
	}
	if err != nil {
		return workerapi.Control{}, err
	}
	var c workerapi.Control
	if body != nil {
		if err := workerapi.Unmarshal(body, &c); err != nil {
			return workerapi.Control{}, fmt.Errorf("%w: attempt %s's control: %w", ErrMalformed, id, err)
		}
	}
	c.Version = version
	return c, nil
}

// A GapError is what AppendEvents returns for events that skip a sequence
// number: it stored none of them, and expects Expected next.
type GapError struct {
	Expected int64
}

func (e *GapError) Error() string {
	return fmt.Sprintf("store: events out of order: expected event %d", e.Expected)
}

// AppendEvents stores the attempt's events that it hasn't yet, in the
// order of their sequence numbers, and returns the sequence number of the
// last it has. An event it has already is skipped, as one a worker sends
// again; events that skip one are a *GapError, and none of them is
// stored. A step's start makes the attempt running (StepStarted). Check
// that the attempt may report (Authorize) in the same transaction.
func (tx *Tx) AppendEvents(id string, events []workerapi.Event) (int64, error) {
	last, err := tx.LastEvent(id)
	if err != nil {
		return 0, err
	}
	next, started := last, false
	var fresh []workerapi.Event
	for _, e := range events {
		switch {
		case e.Seq <= next:
			continue
		case e.Seq != next+1:
			return last, &GapError{Expected: last + 1}
		}
		next = e.Seq
		fresh = append(fresh, e)
		started = started || !e.Finished
	}
	if len(fresh) == 0 {
		return last, nil
	}
	now, err := tx.now()
	if err != nil {
		return 0, err
	}
	var (
		run int64
		job string
	)
	if err := tx.tx.QueryRowContext(tx.ctx, "SELECT run_id, job_key FROM meridian_attempts WHERE id = ?", id).Scan(&run, &job); err != nil {
		return 0, err
	}
	var size int64
	for _, e := range fresh {
		body, err := workerapi.Marshal(e)
		if err != nil {
			return 0, err
		}
		if _, err := tx.tx.ExecContext(tx.ctx, "INSERT INTO meridian_events (attempt_id, seq, body, stored_at) VALUES (?, ?, ?, ?)",
			id, e.Seq, body, now); err != nil {
			return 0, err
		}
		size += int64(len(body))
		kind := EventStepStarted
		if e.Finished {
			kind = EventStepFinished
		}
		if err := tx.addEvent(run, kind, job, id, e.Seq); err != nil {
			return 0, err
		}
	}
	if _, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_attempts SET events_count = events_count + ?, events_bytes = events_bytes + ? WHERE id = ?",
		len(fresh), size, id); err != nil {
		return 0, err
	}
	if started {
		if err := tx.StepStarted(id); err != nil {
			return 0, err
		}
	}
	// A step that failed marks the run, for early-cancel: any-job-failed.
	if err := tx.markFailedStep(id, fresh); err != nil {
		return 0, err
	}
	return next, nil
}

// EventsStored returns how many events the attempt has, and how many bytes
// they take, as AppendEvents counts them as it stores them.
func (tx *Tx) EventsStored(id string) (count, size int64, err error) {
	err = tx.tx.QueryRowContext(tx.ctx, "SELECT events_count, events_bytes FROM meridian_attempts WHERE id = ?", id).Scan(&count, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, ErrNotFound
	}
	return count, size, err
}

// LogBytes returns how many bytes of logs the attempt has, as AddLogBytes
// counted them.
func (tx *Tx) LogBytes(id string) (int64, error) {
	var n int64
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT log_bytes FROM meridian_attempts WHERE id = ?", id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return n, err
}

// AddLogBytes counts n more bytes of the attempt's logs, which the store
// doesn't hold, and returns how many it has counted in all.
func (tx *Tx) AddLogBytes(id string, n int64) (int64, error) {
	if n < 0 {
		return 0, fmt.Errorf("store: %d bytes of logs", n)
	}
	var all int64
	err := tx.tx.QueryRowContext(tx.ctx, "UPDATE meridian_attempts SET log_bytes = log_bytes + ? WHERE id = ? RETURNING log_bytes", n, id).Scan(&all)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return all, err
}

// ControlVersions returns the version of the control of each of the
// attempts ids that the store has, by ID.
func (tx *Tx) ControlVersions(ids []string) (map[string]int64, error) {
	out := make(map[string]int64, len(ids))
	for chunk := range slices.Chunk(ids, 500) {
		// Only placeholders are joined into the query: the IDs are its
		// arguments.
		query := "SELECT id, control_version FROM meridian_attempts WHERE id IN (?" + strings.Repeat(", ?", len(chunk)-1) + ")" //nolint:gosec // placeholders only
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := tx.tx.QueryContext(tx.ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var v int64
			if err := rows.Scan(&id, &v); err != nil {
				rows.Close()
				return nil, err
			}
			out[id] = v
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// LastEvent returns the sequence number of the attempt's last event, or 0
// before its first.
func (tx *Tx) LastEvent(id string) (int64, error) {
	var last int64
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT coalesce(max(seq), 0) FROM meridian_events WHERE attempt_id = ?", id).Scan(&last)
	return last, err
}

// Events returns the attempt's events, in order.
func (tx *Tx) Events(id string) ([]workerapi.Event, error) {
	rows, err := tx.tx.QueryContext(tx.ctx, "SELECT body FROM meridian_events WHERE attempt_id = ? ORDER BY seq", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []workerapi.Event
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var e workerapi.Event
		if err := workerapi.Unmarshal(body, &e); err != nil {
			return nil, fmt.Errorf("%w: an event of attempt %s: %w", ErrMalformed, id, err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// StartedBefore returns how many of the attempts of a's job before it, by
// their numbers, started a step: what the job's retry: had used when a
// was made, which every claim of a gives the same (workerapi.Claim.Retry).
func (tx *Tx) StartedBefore(a *Attempt) (int, error) {
	n, ok := runNumber(a.Run)
	if !ok {
		return 0, ErrNotFound
	}
	var started int
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT count(*) FROM meridian_attempts WHERE run_id = ? AND job_key = ? AND number < ? AND step_started_at IS NOT NULL",
		n, a.Job, a.Number).Scan(&started)
	return started, err
}

// Complete returns the worker's report that completed the attempt, as the
// store holds it (AttemptEnd.Report), or nil for one that didn't complete.
func (a *Attempt) Complete() (*workerapi.Complete, error) {
	if a.End.Report == nil {
		return nil, nil
	}
	c := new(workerapi.Complete)
	if err := workerapi.Unmarshal(a.End.Report, c); err != nil {
		return nil, fmt.Errorf("%w: the report that completed attempt %s: %w", ErrMalformed, a.ID, err)
	}
	return c, nil
}
