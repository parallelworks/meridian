package store

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
)

// A Place is where an attempt is to be placed, as a supervisor chose it
// before it asked the member's backend: a member of a group, and its
// backend; or, when where the job runs is private, the same sealed under
// the run's data key (Private), which only the coordinator opens, and
// Group, Member and Backend "". Generation is the generation a private
// place was sealed with: 1 as it is chosen, and the next each time it is
// sealed again (ResealPlace), as with the backend's ID for the placement;
// and 0 for a place in the clear.
type Place struct {
	Group, Member, Backend string
	Private                []byte
	Generation             uint64
}

// check checks that p is one or the other, whole, as it is chosen.
func (p Place) check() error {
	inClear := p.Group != "" && p.Member != "" && p.Backend != ""
	switch {
	case inClear == (p.Private != nil) || !inClear && (p.Group != "" || p.Member != "" || p.Backend != ""):
		return errors.New("store: an attempt's place is its group, member and backend, or those sealed, and not both")
	case p.Private != nil && p.Generation != 1 || p.Private == nil && p.Generation != 0:
		return fmt.Errorf("store: an attempt's place chosen with generation %d, where a private one's is 1 and one in the clear has none", p.Generation)
	}
	return nil
}

// ChoosePlace records where the attempt is to be placed, p, before the
// backend is asked, so that a supervisor that ends before it records the
// placement (RecordPlacement) finds where it placed it; and starts the
// count of its job's tries at placing again from 0 (PlaceTried). An
// attempt that no longer holds its job is ErrFenced, and one whose place
// was chosen already, or that has been claimed, ErrState.
func (tx *Tx) ChoosePlace(id string, p Place) error {
	if err := p.check(); err != nil {
		return err
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
	}
	res, err := tx.tx.ExecContext(tx.ctx, `UPDATE meridian_attempts SET grp = ?, member = ?, backend = ?, private_placement = ?, placement_gen = ?
WHERE id = ? AND grp IS NULL AND private_placement IS NULL`,
		nullString(p.Group), nullString(p.Member), nullString(p.Backend), p.Private, p.Generation, id)
	if err != nil {
		return err
	}
	if err := oneRow(res, fmt.Errorf("%w: attempt %s's place was chosen", ErrState, id)); err != nil {
		return err
	}
	n, _ := runNumber(a.Run)
	_, err = tx.tx.ExecContext(tx.ctx, "UPDATE meridian_jobs SET place_tries = 0 WHERE run_id = ? AND key = ?", n, a.Job)
	return err
}

// ChosenPlace returns where the attempt is to be placed, as ChoosePlace
// recorded it, and false before it has.
func (tx *Tx) ChosenPlace(id string) (Place, bool, error) {
	var (
		p                      Place
		group, member, backend sql.NullString
	)
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT grp, member, backend, private_placement, placement_gen FROM meridian_attempts WHERE id = ?", id).
		Scan(&group, &member, &backend, &p.Private, &p.Generation)
	if errors.Is(err, sql.ErrNoRows) {
		return Place{}, false, ErrNotFound
	}
	if err != nil {
		return Place{}, false, err
	}
	p.Group, p.Member, p.Backend = group.String, member.String, backend.String
	return p, p.Group != "" || p.Private != nil, nil
}

// ResealPlace stores sealed as the attempt's private place in place of its
// last, sealed with generation, which is one more than the last's: as once
// its backend has placed it, with the backend's ID for the placement,
// which of a private placement the store holds only sealed
// (RecordPlacement). The place and its generation change together, or
// neither does: a generation that isn't the next is ErrState and
// ErrRecordChanged, as a write that raced another's is. An attempt that no
// longer holds its job is ErrFenced, and one that has been claimed, or
// whose place isn't private, ErrState.
func (tx *Tx) ResealPlace(id string, generation uint64, sealed []byte) error {
	if len(sealed) == 0 {
		return errors.New("store: an attempt's private place without its bytes")
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
	case !a.Private:
		return fmt.Errorf("%w: attempt %s's place isn't private", ErrState, id)
	}
	stale := fmt.Errorf("%w: %w: generation %d isn't the next of attempt %s's place", ErrState, ErrRecordChanged, generation, id)
	if generation < 2 {
		return stale
	}
	// Only from the generation before, so that a write that raced this one
	// fails, where the database runs transactions side by side.
	res, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_attempts SET private_placement = ?, placement_gen = ? WHERE id = ? AND placement_gen = ?",
		sealed, generation, id, generation-1)
	if err != nil {
		return err
	}
	return oneRow(res, stale)
}

// LatestAttempt returns the run's job's latest attempt, whose fence is its
// job's: ErrNotFound before its first.
func (tx *Tx) LatestAttempt(run, key string) (*Attempt, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	return tx.attemptWhere("a.run_id = ? AND a.job_key = ? AND a.fence = j.fence", n, key)
}

// SetupFailures returns the run's job's attempts that failed before a step
// started, in their order, each with the problem it ended with
// (AttemptEnd.Problem), or none.
func (tx *Tx) SetupFailures(run, key string) ([]*Attempt, error) {
	n, ok := runNumber(run)
	if !ok {
		return nil, ErrNotFound
	}
	rows, err := tx.tx.QueryContext(tx.ctx, "SELECT id FROM meridian_attempts WHERE run_id = ? AND job_key = ? AND reason = ? ORDER BY number",
		n, key, plan.ReasonSetupFailed)
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
	out := make([]*Attempt, len(ids))
	for i, id := range ids {
		if out[i], err = tx.Attempt(id); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// NewAttemptAfter starts a new attempt as NewAttempt does, but only of a
// job whose fence is still fence, as the caller saw it, which is 0 before
// its first attempt: a job another attempt was made of meanwhile, as by
// another supervisor of it, is ErrFenced, and none ends superseded.
func (tx *Tx) NewAttemptAfter(n NewAttempt, fence int64) (*Attempt, string, error) {
	j, err := tx.Job(n.Run, n.Job)
	if err != nil {
		return nil, "", err
	}
	if j.Fence != fence {
		return nil, "", fmt.Errorf("%w: run %s's job %q has had another attempt since", ErrFenced, n.Run, n.Job)
	}
	return tx.NewAttempt(n)
}

// Unplaced checks that the attempt id still holds its job, and is being
// placed, its placement not yet recorded: what a supervisor checks before
// it ends such an attempt, or gives it a new token, so that one that
// another supervisor has placed meanwhile is left as it is. An attempt that
// no longer holds its job is ErrFenced, and one placed or claimed ErrState.
func (tx *Tx) Unplaced(id string) error {
	a, err := tx.Attempt(id)
	switch {
	case err != nil:
		return err
	case !a.Holds:
		return ErrFenced
	case a.State != AttemptPlacing || a.Placed():
		return fmt.Errorf("%w: attempt %s has been placed", ErrState, id)
	}
	return nil
}

// PlaceFailed records that the run's job couldn't be placed, with p, as
// after its backend couldn't answer as many times as it may: it ends the
// job's attempt that hasn't ended, as long as its placement isn't
// recorded (Unplaced), or, when it has none, a new one, made for the
// record, as one that failed before its first step; and starts the count
// of the job's tries at placing again from 0. newAttempt is what a new one
// is made with, and fence the job's fence as the caller saw it, as
// NewAttemptAfter takes it. It returns the attempt.
func (tx *Tx) PlaceFailed(newAttempt NewAttempt, fence int64, p *problem.Problem) (*Attempt, error) {
	run, key := newAttempt.Run, newAttempt.Job
	a, err := tx.LatestAttempt(run, key)
	switch {
	case errors.Is(err, ErrNotFound) || err == nil && a.State == AttemptEnded:
		if a, _, err = tx.NewAttemptAfter(newAttempt, fence); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	case a.Fence != fence:
		return nil, fmt.Errorf("%w: run %s's job %q has had another attempt since", ErrFenced, run, key)
	default:
		if err := tx.Unplaced(a.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.EndAttempt(a.ID, AttemptEnd{Kind: EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: p}); err != nil {
		return nil, err
	}
	n, _ := runNumber(run)
	if _, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_jobs SET place_tries = 0 WHERE run_id = ? AND key = ?", n, key); err != nil {
		return nil, err
	}
	return tx.Attempt(a.ID)
}
