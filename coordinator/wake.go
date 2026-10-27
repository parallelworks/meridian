package coordinator

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/coordinator/internal/store"
)

// A waking is a supervisor, or a marker of a need on any instance, that a
// transaction woke while hopper ran it. A supervisor that is looking reads
// its job's wake again before it waits, unless it read last just before
// the wake, and is about to wait: so, once the transaction commits, it is
// woken again, briefly apart, until hopper no longer runs it (wakeAgain).
type waking struct {
	id       hopper.JobID
	run, key string
	// waits reports, in a transaction, whether it still waits: a
	// supervisor completes in the transaction that ends its job, and a
	// marker in the one that chooses, and hopper's JobRetry would run
	// either again once it has.
	waits func(*store.Tx) (bool, error)
}

// wakeJobTx wakes, in tx, the supervisor of the run's job key, once
// something it waits for has happened in tx, unless the job has ended: it counts the wake on the job (store.Tx.Wake), which a
// supervisor that is looking reads again before it waits, and has hopper
// run one that waits at once, once tx commits. It returns the supervisor if
// hopper was running it, for the caller to wake again once tx commits
// (wakeAgain), or nil.
func (c *Coordinator) wakeJobTx(ctx context.Context, tx *store.Tx, run, key string) (*waking, error) {
	j, err := tx.Wake(run, key)
	if err != nil || j.Outcome != "" {
		return nil, err
	}
	return c.retryTx(ctx, tx, &waking{id: j.SupervisorID, run: run, key: key, waits: unended(run, key)}), nil
}

// wakeAnyTx wakes, in tx, the marker of a need on any instance of the
// run's job name, if a job needs it so and it hasn't chosen, once one of
// the job's instances, or the job, has ended, as wakeJobTx wakes a
// supervisor.
func (c *Coordinator) wakeAnyTx(ctx context.Context, tx *store.Tx, run, name string) (*waking, error) {
	a, err := tx.Any(run, name)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, nil
	case err != nil || !a.ChosenAt.IsZero():
		return nil, err
	}
	return c.retryTx(ctx, tx, &waking{id: a.MarkerID, run: run, key: name, waits: unchosen(run, name)}), nil
}

// unended reports, in a transaction, whether the run's job key hasn't
// ended: whether its supervisor waits.
func unended(run, key string) func(*store.Tx) (bool, error) {
	return func(tx *store.Tx) (bool, error) {
		j, err := tx.Job(run, key)
		return err == nil && j.Outcome == "", err
	}
}

// unchosen reports, in a transaction, whether the marker of the need on
// any instance of the run's job name hasn't chosen: whether it waits.
func unchosen(run, name string) func(*store.Tx) (bool, error) {
	return func(tx *store.Tx) (bool, error) {
		a, err := tx.Any(run, name)
		return err == nil && a.ChosenAt.IsZero(), err
	}
}

// retryTx has hopper run w, which waits, at once, once tx commits: hopper
// wakes its queue in this process then. It returns w if hopper runs it now,
// and nil otherwise. A wake that fails is logged, and fails nothing else:
// the supervisor or marker looks again at the end of its wait.
func (c *Coordinator) retryTx(ctx context.Context, tx *store.Tx, w *waking) *waking {
	_, err := c.inserter.JobRetryTx(ctx, tx.SQL(), w.id)
	switch {
	case errors.Is(err, hopper.ErrJobRunning):
		return w
	case err != nil:
		c.log.DebugContext(ctx, "coordinator: wake a job's supervisor", "run", w.run, "job", w.key, "error", err)
	}
	return nil
}

// wakeAgain wakes ws again, those of the supervisors and markers a
// transaction woke that hopper was running (waking), each in a transaction
// of its own that reads first whether it still waits, wakeAgainAfter apart,
// until hopper runs none of them, wakeTries times in all with the first.
// Those that are nil are left out.
func (c *Coordinator) wakeAgain(ctx context.Context, ws ...*waking) {
	ws = slices.DeleteFunc(ws, func(w *waking) bool { return w == nil })
	for try := 1; try < wakeTries && len(ws) > 0; try++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wakeAgainAfter):
		}
		ws = slices.DeleteFunc(ws, func(w *waking) bool { return !c.wakeOnce(ctx, w) })
	}
}

// wakeOnce wakes w in a transaction of its own, if it still waits, and
// reports whether hopper runs it now.
func (c *Coordinator) wakeOnce(ctx context.Context, w *waking) bool {
	var running *waking
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		waits, err := w.waits(tx)
		if err != nil || !waits {
			return err
		}
		running = c.retryTx(ctx, tx, w)
		return nil
	}); err != nil {
		c.log.DebugContext(ctx, "coordinator: wake a job's supervisor", "run", w.run, "job", w.key, "error", err)
		return false
	}
	return running != nil
}

// A supervisor or marker that hopper runs as it is woken is woken again,
// wakeTries times in all, wakeAgainAfter apart.
const (
	wakeTries      = 5
	wakeAgainAfter = 20 * time.Millisecond
)
