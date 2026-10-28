package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// A Link is a child run's link to the try of the step that called it, a
// step whose uses: names a workflow (plan.ParentRun), which the store
// keeps beside the run, so that a step's try finds its child again, and a
// root the runs of its tree.
type Link struct {
	// Run is the ID of the run whose step called it, Job the key of the
	// step's job, an instance's for a matrix job, and Attempt the ID of the
	// attempt that ran the step; Step is the step's index in its job, and
	// Try its try, from 0.
	Run, Job, Attempt string
	Step, Try         int
	// Root is the ID of the run at the top of the tree, which no step
	// called, and Depth how far below it the run is, from 1.
	Root  string
	Depth int
	// Cleanup is set for a run of the step's cleanup:, a step run, whose
	// Try is 0, which the step's tries' own runs, of the same step and
	// try, aren't.
	Cleanup bool
}

// check returns an error for a link that names no step's try.
func (l *Link) check() error {
	switch {
	case l.Job == "" || l.Attempt == "":
		return errors.New("store: a child run's link without its step's job or attempt")
	case l.Step < 0 || l.Try < 0 || l.Depth < 1:
		return fmt.Errorf("store: a child run's link to step %d, try %d, at depth %d", l.Step, l.Try, l.Depth)
	}
	return nil
}

// Child returns the ID of the child run of the try try of the step step
// that the attempt attempt ran: ErrNotFound if it has none.
func (tx *Tx) Child(attempt string, step, try int) (string, error) {
	return tx.ChildOf(attempt, step, try, false)
}

// ChildOf returns the ID of the child run of the try try of the step step
// that the attempt attempt ran, or, where cleanup is set, of the step's
// cleanup (Link.Cleanup): ErrNotFound if it has none.
func (tx *Tx) ChildOf(attempt string, step, try int, cleanup bool) (string, error) {
	var n int64
	err := tx.queryRow(`SELECT id FROM meridian_runs WHERE parent_attempt = ? AND parent_step = ? AND parent_try = ? AND parent_cleanup = ?`,
		attempt, step, try, cleanup).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return runID(n), nil
}

// A LiveChild is a child run that hasn't ended, of a try of a step of an
// attempt's job (Tx.LiveChildren).
type LiveChild struct {
	// ID is the run's ID, Attempt the ID of the attempt that ran the
	// step, Step the step's index in its job, and Try its try; and Cleanup
	// is set for a run of the step's cleanup.
	ID, Attempt string
	Step, Try   int
	Cleanup     bool
}

// LiveChildren returns the children of the steps' tries of the attempts
// ids that haven't ended, in the order of the attempts, and of their IDs.
func (tx *Tx) LiveChildren(ids ...string) ([]LiveChild, error) {
	var out []LiveChild
	for _, attempt := range ids {
		rows, err := tx.query(`SELECT id, parent_step, parent_try, parent_cleanup FROM meridian_runs WHERE parent_attempt = ? AND state <> ? ORDER BY id`,
			attempt, RunEnded)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var n int64
			c := LiveChild{Attempt: attempt}
			if err := rows.Scan(&n, &c.Step, &c.Try, &c.Cleanup); err != nil {
				rows.Close()
				return nil, err
			}
			c.ID = runID(n)
			out = append(out, c)
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

// LiveDescendants returns how many runs of the tree whose root is the run
// root haven't ended, at any depth.
func (tx *Tx) LiveDescendants(root string) (int, error) {
	n, ok := runNumber(root)
	if !ok {
		return 0, ErrNotFound
	}
	var count int
	err := tx.queryRow(`SELECT count(*) FROM meridian_runs WHERE root_run = ? AND state <> ?`, n, RunEnded).Scan(&count)
	return count, err
}

// WaitForDescendants notes that the finisher of the run root waits for the
// runs of its tree to end, which the end of each wakes (Run.WaitedAt).
func (tx *Tx) WaitForDescendants(root string) error {
	n, ok := runNumber(root)
	if !ok {
		return ErrNotFound
	}
	now, err := tx.now()
	if err != nil {
		return err
	}
	res, err := tx.exec(`UPDATE meridian_runs SET waited_at = coalesce(waited_at, ?) WHERE id = ? AND state <> ?`, now, n, RunEnded)
	if err != nil {
		return err
	}
	return oneRow(res, fmt.Errorf("%w: run %s has ended, or isn't there", ErrState, root))
}
