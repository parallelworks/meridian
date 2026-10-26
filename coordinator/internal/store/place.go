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
// Group, Member and Backend "".
type Place struct {
	Group, Member, Backend string
	Private                []byte
}

// check checks that p is one or the other, whole.
func (p Place) check() error {
	inClear := p.Group != "" && p.Member != "" && p.Backend != ""
	if inClear == (p.Private != nil) || !inClear && (p.Group != "" || p.Member != "" || p.Backend != "") {
		return errors.New("store: an attempt's place is its group, member and backend, or those sealed, and not both")
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
	res, err := tx.tx.ExecContext(tx.ctx, `UPDATE meridian_attempts SET grp = ?, member = ?, backend = ?, private_placement = ?
WHERE id = ? AND grp IS NULL AND private_placement IS NULL`,
		nullString(p.Group), nullString(p.Member), nullString(p.Backend), p.Private, id)
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
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT grp, member, backend, private_placement FROM meridian_attempts WHERE id = ?", id).
		Scan(&group, &member, &backend, &p.Private)
	if errors.Is(err, sql.ErrNoRows) {
		return Place{}, false, ErrNotFound
	}
	if err != nil {
		return Place{}, false, err
	}
	p.Group, p.Member, p.Backend = group.String, member.String, backend.String
	return p, p.Group != "" || p.Private != nil, nil
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
	case a.State != AttemptPlacing || a.Placement.ID != "":
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
