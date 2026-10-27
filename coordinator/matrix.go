package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// matrixStep takes the next step of job, a matrix job, as its supervisor,
// as docs/design.md says under Matrices: it expands the job's matrix and
// inserts a supervisor for each instance, once; then, as its instances
// end, it stops the rest once one fails, as its strategy's fail-fast:
// asks; gives the next their turns, as many at a time as its
// max-parallel: allows, in the order of their indexes; and ends the job,
// once they all have, with their combined result (plan.Combine).
func (c *Coordinator) matrixStep(ctx context.Context, run *store.Run, job *store.Job, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	var instances []*store.Job
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		instances, err = tx.Instances(run.ID, job.Name)
		return err
	}); err != nil {
		return 0, err
	}
	if len(instances) == 0 {
		return c.expand(ctx, run, job, done)
	}
	x, rec, err := c.expansion(ctx, run, job)
	if err != nil {
		return 0, err
	}
	var unfinished []*store.Job
	stopped := false
	for _, j := range instances {
		switch j.Outcome {
		case "":
			unfinished = append(unfinished, j)
		case plan.OutcomeFailure, plan.OutcomeCancelled:
			// As format 1, only an instance whose steps ran stops the
			// rest: not one that failed before its first, as when Ready
			// or its setup failed.
			stopped = stopped || j.AttemptsStarted > 0
		}
	}
	// As format 1, an instance whose steps failed, or were cancelled,
	// stops the rest: each that hasn't ended, its supervisor ends.
	if x.FailFast && stopped && job.FailFastAt.IsZero() && len(unfinished) > 0 {
		var changed bool
		if err := c.store.Tx(ctx, func(tx *store.Tx) error {
			if _, err := tx.MarkFailFast(run.ID, job.Key); err != nil {
				return err
			}
			changed = tx.ControlsChanged()
			return nil
		}); err != nil {
			return 0, err
		}
		if changed {
			c.controlsChanged()
		}
		for _, j := range unfinished {
			c.wakeJob(ctx, run.ID, j.Key)
		}
		return 0, errAgain
	}
	if len(unfinished) == 0 {
		return 0, c.endMatrix(ctx, run, job, instances, rec, done)
	}
	turns := len(unfinished)
	if x.MaxParallel > 0 {
		turns = min(turns, x.MaxParallel)
	}
	if err := c.grantTurns(ctx, run.ID, keysOf(unfinished[:turns])); err != nil {
		return 0, err
	}
	return c.supervision.wait, nil
}

// expand expands job's matrix, as Start did, or else as Ready does with
// what the jobs it needs gave, and inserts a supervisor for each of its
// instances, in one transaction, which keeps the instances as run state
// does (plan.Expansion.Split), and gives the first their turns. A job that
// Ready skips, or whose matrix it can't expand, ends as Ready says.
func (c *Coordinator) expand(ctx context.Context, run *store.Run, job *store.Job, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	var (
		x              *plan.Expansion
		private, state *plan.Expansion
	)
	if run.StartState.Expansions[job.Name] != nil {
		var err error
		if x, _, err = c.expansion(ctx, run, job); err != nil {
			return 0, err
		}
	} else {
		r, err := c.readyJob(ctx, run, job.Key)
		if err != nil {
			return 0, err
		}
		if end, ok := r.end(); ok {
			return 0, c.endJob(ctx, run.ID, job.Key, end, done)
		}
		if x = r.result.Expansion; x == nil {
			return 0, fmt.Errorf("coordinator: Ready gave run %s's matrix job %q no instances", run.ID, job.Key)
		}
		private, state = x.Split()
	}
	params := make([]hopper.InsertParams, len(x.Instances))
	for i, instance := range x.Instances {
		params[i] = hopper.InsertParams{
			Args: jobArgs{Run: run.ID, Key: instance.Key},
			Opts: &hopper.InsertOpts{Queue: sharedQueue, MaxAttempts: c.retry.attempts},
		}
	}
	turns := len(x.Instances)
	if x.MaxParallel > 0 {
		turns = min(turns, x.MaxParallel)
	}
	var granted []string
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		if state != nil {
			if err := tx.SetExpansion(run.ID, job.Key, state); err != nil {
				return err
			}
			if err := c.keepPrivate(ctx, tx, run.ID, "", nil, job.Name, private); err != nil {
				return err
			}
		}
		res, err := c.inserter.InsertManyTx(ctx, tx.SQL(), params)
		if err != nil {
			return err
		}
		keys := make([]string, len(x.Instances))
		for i, instance := range x.Instances {
			keys[i] = instance.Key
			if err := tx.InsertJob(store.NewJob{Run: run.ID, Name: job.Name, Key: instance.Key, SupervisorID: res[i].Job.ID}); err != nil {
				return err
			}
		}
		granted, err = tx.GrantTurns(run.ID, keys[:turns])
		return err
	})
	if errors.Is(err, store.ErrState) {
		// Expanded meanwhile, as by another attempt of the supervisor.
		return 0, errAgain
	}
	if err != nil {
		return 0, err
	}
	for _, key := range granted {
		c.wakeJob(ctx, run.ID, key)
	}
	return 0, errAgain
}

// expansion returns the instances of job, a matrix job of run, whole, with
// the run's secret record when they are private, which it opens only then,
// and nil otherwise.
func (c *Coordinator) expansion(ctx context.Context, run *store.Run, job *store.Job) (*plan.Expansion, *plan.Record, error) {
	state := run.StartState.Expansions[job.Name]
	if state == nil {
		state = job.Expansion
	}
	if state != nil && !slices.ContainsFunc(state.Instances, func(i plan.Instance) bool { return i.Private }) {
		return state, nil, nil
	}
	rec, err := c.openRecord(ctx, run.ID)
	if err != nil {
		return nil, nil, err
	}
	x := expansionOf(run, job, rec)
	if x == nil {
		return nil, nil, fmt.Errorf("coordinator: run %s's matrix job %q has no instances", run.ID, job.Key)
	}
	return x, rec, nil
}

// openRecord opens the run's secret record.
func (c *Coordinator) openRecord(ctx context.Context, runID string) (*plan.Record, error) {
	var stored *store.Record
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = tx.Record(runID)
		return err
	}); err != nil {
		return nil, err
	}
	return c.sealer.Open(ctx, runID, stored.Generation, sealedOf(stored))
}

// grantTurns gives the run's instances keys their turns to be placed, and
// wakes those it gave one now.
func (c *Coordinator) grantTurns(ctx context.Context, runID string, keys []string) error {
	var granted []string
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		granted, err = tx.GrantTurns(runID, keys)
		return err
	}); err != nil {
		return err
	}
	for _, key := range granted {
		c.wakeJob(ctx, runID, key)
	}
	return nil
}

// keysOf returns the keys of jobs.
func keysOf(jobs []*store.Job) []string {
	keys := make([]string, len(jobs))
	for i, j := range jobs {
		keys[i] = j.Key
	}
	return keys
}

// endMatrix ends job, a matrix job all of whose instances have ended, with
// their combined result, its private half kept in the run's secret record,
// in the transaction that ends it. rec is the record, opened already, or
// nil.
func (c *Coordinator) endMatrix(ctx context.Context, run *store.Run, job *store.Job, instances []*store.Job, rec *plan.Record, done func(context.Context, *store.Tx) error) error {
	if rec == nil {
		var err error
		if rec, err = c.openRecord(ctx, run.ID); err != nil {
			return err
		}
	}
	results := make([]plan.JobResult, len(instances))
	for i, j := range instances {
		results[i] = neededResult(j, rec)
	}
	private, state := plan.Combine(results).Split()
	return c.endJobWith(ctx, run.ID, job.Key, store.JobEnd{Result: state}, func(ctx context.Context, tx *store.Tx) error {
		return c.keepPrivate(ctx, tx, run.ID, job.Key, &private, "", nil)
	}, done)
}

// instanceStep is what an instance's supervisor does before it steps as
// any job's does: an instance whose matrix job stopped its instances, as
// fail-fast: asks, ends cancelled with the reason fail-fast, its placement
// withdrawn if its worker hasn't claimed it, while one whose worker has
// was told to cancel, and is waited on; and one that hasn't had its turn
// under max-parallel: waits for it. It reports whether it handled the
// step.
func (c *Coordinator) instanceStep(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, bool, error) {
	var parent *store.Job
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		parent, err = tx.Job(run.ID, job.Name)
		return err
	}); err != nil {
		return 0, true, err
	}
	failFast := store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeCancelled}, Reason: plan.ReasonFailFast}
	switch {
	case parent.FailFastAt.IsZero():
		if a == nil && job.TurnAt.IsZero() {
			return c.supervision.wait, true, nil
		}
		return 0, false, nil
	case a != nil && a.State == store.AttemptEnded && a.End.Kind == store.EndCompleted && a.End.Reason != plan.ReasonSetupFailed:
		// It ends as its worker said, cancelled or not.
		return 0, false, nil
	case a == nil || a.State == store.AttemptEnded:
		return 0, true, c.endJob(ctx, run.ID, job.Key, failFast, done)
	case a.State == store.AttemptPlacing:
		return 0, true, c.withdraw(ctx, run, job, a, failFast, done)
	default:
		// Its worker is told to cancel the job, and says how it ended.
		// The attempt's control has said so since it was made, or since
		// the job was marked (store.Tx.StartControl, MarkFailFast); one
		// that doesn't yet is told now.
		var changed bool
		if err := c.store.Tx(ctx, func(tx *store.Tx) error {
			if err := tx.TellFailFast(a.ID); err != nil && !errors.Is(err, store.ErrState) {
				return err
			}
			changed = tx.ControlsChanged()
			return nil
		}); err != nil {
			return 0, true, err
		}
		if changed {
			c.controlsChanged()
		}
		return c.supervision.wait, true, nil
	}
}

// withdraw withdraws the attempt a of the run's job, which its worker
// hasn't claimed, and ends the job as end says, in one transaction: its
// placement, if it was recorded, its driver cancels first, as far as it
// can, and a worker that claims it later is refused. One claimed meanwhile
// is looked at again.
func (c *Coordinator) withdraw(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, end store.JobEnd, done func(context.Context, *store.Tx) error) error {
	if a.Placement.ID != "" {
		cand, _, private, err := c.placedOn(ctx, run, job, a)
		if err == nil {
			err = cand.driver.Cancel(ctx, cand.member, backend.Placement{Attempt: a.ID, ID: a.Placement.ID, WorkerDigest: a.Placement.WorkerDigest})
		}
		if err != nil && !private {
			c.log.WarnContext(ctx, "coordinator: couldn't cancel a placement it withdraws, whose worker is refused its claim", "run", run.ID, "job", job.Key, "error", err)
		}
	}
	return c.endJobWith(ctx, run.ID, job.Key, end, func(_ context.Context, tx *store.Tx) error {
		now, err := tx.Attempt(a.ID)
		switch {
		case err != nil:
			return err
		case !now.Holds || now.State != store.AttemptPlacing:
			return errAgain
		}
		return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndWithdrawn, Reason: end.Reason})
	}, done)
}

// anyWorker works the markers of runs' needs on any instance (anyArgs).
type anyWorker struct {
	hopper.WorkerDefaults[anyArgs]
	c *Coordinator
}

// Work chooses the instance the jobs that need the job on any instance
// read, as chooseAny does, and completes the marker in the transaction
// that records it.
func (w *anyWorker) Work(ctx context.Context, job *hopper.Job[anyArgs]) error {
	done := func(ctx context.Context, tx *store.Tx) error { return hopper.CompleteTx(ctx, tx.SQL(), job) }
	snooze, err := w.c.chooseAny(ctx, job.Args.Run, job.Args.Job, done)
	switch {
	case stopping(ctx):
		return hopper.Snooze(w.c.supervision.first)
	case errors.Is(err, errSettled):
		return w.c.complete(ctx, done)
	case errors.Is(err, hopper.ErrJobFenced):
		return nil
	case err != nil:
		return err
	case snooze > 0:
		return hopper.Snooze(snooze)
	}
	return nil
}

// NextRetry is when the marker looks again after the coordinator itself
// failed, as when its store did.
func (w *anyWorker) NextRetry(job *hopper.Job[anyArgs]) time.Time {
	return time.Now().Add(w.c.supervision.backoff(job.Attempt))
}

// chooseAny chooses, for the jobs of the run that need the job name on any
// instance, the instance they read, as format 1 waits for one: the first to
// succeed, or to be skipped by its own if:; or else, once every instance
// has ended, the first to fail, or the first to end; and the instances that
// had ended by then, whose outputs they read. It records the choice in a
// transaction that done writes to as well. A job that ended without
// instances, as one skipped before its matrix was expanded, or one without
// a matrix, is read as itself, once it has ended. It returns how long to
// wait before it looks again, while it can't choose yet; or errSettled for
// a choice made already, or a run that isn't running.
func (c *Coordinator) chooseAny(ctx context.Context, runID, name string, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	var (
		run       *store.Run
		any       *store.Any
		job       *store.Job
		instances []*store.Job
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if run, err = tx.Run(runID); err != nil || run.State != store.RunRunning {
			return err
		}
		if any, err = tx.Any(runID, name); err != nil || !any.ChosenAt.IsZero() {
			return err
		}
		if job, err = tx.Job(runID, name); err != nil {
			return err
		}
		instances, err = tx.Instances(runID, name)
		return err
	})
	switch {
	case errors.Is(err, plan.ErrVersion):
		return c.newer(ctx, runID), nil
	case err != nil:
		return 0, err
	case run.State != store.RunRunning || !any.ChosenAt.IsZero():
		return 0, errSettled
	}
	choice, chosen := pickAny(job, instances)
	if !chosen {
		return c.supervision.wait, nil
	}
	// The jobs that need it read the outputs of every instance that had
	// ended by then, as format 1 merges them.
	var ended []string
	for _, j := range instances {
		if j.Outcome != "" {
			ended = append(ended, j.Key)
		}
	}
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		if err := tx.ChooseAny(runID, name, choice, ended); err != nil {
			if errors.Is(err, store.ErrState) {
				return errSettled
			}
			return err
		}
		return done(ctx, tx)
	})
	return 0, err
}

// pickAny picks, of job and its instances, the instance the jobs that need
// it on any instance read, as chooseAny says, and "" for job itself, and
// reports whether it can yet.
func pickAny(job *store.Job, instances []*store.Job) (string, bool) {
	if len(instances) == 0 {
		return "", job.Outcome != ""
	}
	// first returns the first of instances to end that ok is true of: by
	// the store's clock, and in the order of their indexes as at once.
	first := func(ok func(*store.Job) bool) *store.Job {
		var out *store.Job
		for _, j := range instances {
			if j.Outcome != "" && ok(j) && (out == nil || j.EndedAt.Before(out.EndedAt)) {
				out = j
			}
		}
		return out
	}
	if j := first(func(j *store.Job) bool {
		return j.Outcome == plan.OutcomeSuccess || j.Outcome == plan.OutcomeSkipped && (j.Result == nil || !j.Result.NeedsFailed)
	}); j != nil {
		return j.Key, true
	}
	if slices.ContainsFunc(instances, func(j *store.Job) bool { return j.Outcome == "" }) {
		return "", false
	}
	if j := first(func(j *store.Job) bool { return j.Outcome == plan.OutcomeFailure }); j != nil {
		return j.Key, true
	}
	return first(func(*store.Job) bool { return true }).Key, true
}
