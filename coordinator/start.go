package coordinator

import (
	"cmp"
	"context"
	"errors"
	"time"

	"github.com/parallelworks/foundation/problem"
	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// errSettled is what startRun returns for a run that another attempt of
// its Start has started, or ended, meanwhile.
var errSettled = errors.New("coordinator: the run has started or ended")

// startWorker works runs' Start (startArgs).
type startWorker struct {
	hopper.WorkerDefaults[startArgs]
	c *Coordinator
}

// Work starts the run, as startRun does, and completes the job with what
// it stores. Once the run has started or ended, the coordinator stops
// working the run's start queue.
func (w *startWorker) Work(ctx context.Context, job *hopper.Job[startArgs]) error {
	done := func(ctx context.Context, tx *store.Tx) error { return hopper.CompleteTx(ctx, tx.SQL(), job) }
	snooze, err := w.c.startRun(ctx, job.Args.Run, done)
	if stopping(ctx) {
		// What cut the try short is the coordinator's stopping, not the
		// run: a snooze gives hopper its attempt back. A job completed
		// already stays so, whatever the work returns.
		return hopper.Snooze(w.c.retry.first)
	}
	switch {
	case errors.Is(err, errSettled):
		if err := w.c.complete(ctx, done); err != nil {
			return err
		}
	case err != nil:
		return err
	case snooze > 0:
		return hopper.Snooze(snooze)
	}
	w.c.retireQueue(ctx, job.Queue)
	return nil
}

// stopping reports whether ctx, a job's work's, ended because the
// coordinator is stopping, and not because the work ran past its time.
func stopping(ctx context.Context) bool {
	return ctx.Err() != nil && !errors.Is(context.Cause(ctx), hopper.ErrJobTimeout)
}

// Timeout bounds one try at starting a run: a try that takes longer counts
// as one a resolver couldn't answer.
func (w *startWorker) Timeout(*hopper.Job[startArgs]) time.Duration { return w.c.startTimeout }

// NextRetry is when the job runs again after the coordinator itself failed
// to start the run, as when its store did: as long after as the
// coordinator waits after as many tries that a resolver couldn't answer.
func (w *startWorker) NextRetry(job *hopper.Job[startArgs]) time.Time {
	return time.Now().Add(w.c.retry.backoff(job.Attempt))
}

// startRun starts the run id, as docs/design.md says under Starting a run:
// it decodes the run's plan, opens its secret record, runs compile.Start,
// keeps what Start gave, the private part in the record, which it seals
// again, and the rest as run state, and stores both, with Start's
// diagnostics, in one transaction, which done, if it isn't nil, writes to
// as well, as the hopper job's completion. A run whose Start fails ends
// failed, in such a transaction too.
//
// It returns how long to wait before trying again, when it couldn't start
// the run but may yet: a resolver that couldn't answer, or a plan only a
// newer release reads. errSettled is a run that another attempt started or
// ended meanwhile, and any other error is the coordinator's own, as its
// store's, which hopper tries again.
//
// Each step can run twice, as a Start that hopper rescued from a replica
// still running it does: the transaction stores only for a run that hasn't
// started, as the record's generation, which changes with each reseal, and
// the run's state say.
func (c *Coordinator) startRun(ctx context.Context, id string, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	var (
		run    *store.Run
		stored *store.Record
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if run, err = tx.Run(id); err != nil || run.State != store.RunCreated {
			return err
		}
		stored, err = tx.Record(id)
		return err
	})
	if err != nil {
		return 0, err
	}
	if run.State != store.RunCreated {
		return 0, errSettled
	}
	if run.StepRun {
		return c.startStepRun(ctx, run, done)
	}
	p, err := c.plans.get(run)
	if err != nil {
		return c.startFailed(ctx, run, causeStore, err, done)
	}
	sealed := sealedOf(stored)
	record, err := c.sealer.Open(ctx, id, stored.Generation, sealed)
	if err != nil {
		return c.startFailed(ctx, run, causeKeys, err, done)
	}
	// A child run's variables are those its root's Start read for the
	// tree, from the root's secret record (rootVariables).
	vars := c.opts.Variables
	if p.Run.Parent != nil {
		tree, err := c.rootVariables(ctx, p.Run.Parent.Root)
		if err != nil {
			return c.startFailed(ctx, run, causeKeys, err, done)
		}
		vars = tree
	}
	res, diags, err := compile.Start(ctx, p, compile.StartInput{
		RunID: id, Variables: vars, Record: *record, Inputs: c.opts.Inputs, Sessions: c.opts.Sessions,
	})
	if err != nil {
		return c.startFailed(ctx, run, causeResolver, err, done)
	}
	if diags.HasErrors() {
		return 0, c.endStart(ctx, id, diags, done)
	}
	state := res.Keep(record)
	resealed, gen, err := c.sealer.Reseal(ctx, id, stored.Generation, sealed, record)
	if err != nil {
		return c.startFailed(ctx, run, causeKeys, err, done)
	}
	return 0, c.store.Tx(ctx, func(tx *store.Tx) error {
		if r, err := tx.Run(id); err != nil || r.State != store.RunCreated {
			return cmp.Or(err, errSettled)
		}
		// Another try that resealed the record first fails here.
		if err := tx.Reseal(id, gen, resealed.Record); err != nil {
			if errors.Is(err, store.ErrState) {
				return errSettled
			}
			return err
		}
		if err := tx.StartRun(id, &state, diags); err != nil {
			return err
		}
		// A run with a timeout: has a job of its own that waits for it.
		if res.TimeoutSeconds > 0 {
			if _, err := c.inserter.InsertTx(ctx, tx.SQL(), timeoutArgs{Run: id}, &hopper.InsertOpts{Queue: sharedQueue}); err != nil {
				return err
			}
		}
		if done != nil {
			return done(ctx, tx)
		}
		return nil
	})
}

// The causes of a try that couldn't start a run, as the coordinator's log
// names them: never the error's text, which can quote what a resolver or
// Keys was given, a secret among it.
const (
	causeStore      = "store"
	causeKeys       = "keys"
	causeResolver   = "resolver"
	causeUnanswered = "unanswered"
	causeTimeout    = "timeout"
)

// startFailed handles err, what kept the coordinator from starting run at
// the step cause names, as the error says it should:
//
//   - a problem (problems.IsProblem), as one that Keys or the record gave,
//     a record that doesn't fit the plan (compile.ErrMismatch), and a plan
//     or a record that doesn't decode (plan.ErrMalformed), fail the run;
//   - a plan, or a record, only a newer release reads (plan.ErrVersion) is
//     left to one, as the coordinator says in its log;
//   - a try the coordinator's stopping cut short is tried again, soon, and
//     doesn't count;
//   - any other error, such as a resolver that couldn't answer
//     (compile.ErrUnanswered), or a try that ran out of time, is tried
//     again, after a while, until the run has been tried as many times as
//     it may be, when it fails. The log says which kind of try failed,
//     never what the error says.
func (c *Coordinator) startFailed(ctx context.Context, run *store.Run, cause string, err error, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	id := run.ID
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return 0, c.endStart(ctx, id, problems.Diagnostics{problemDiagnostic(p)}, done)
	}
	switch {
	case errors.Is(err, compile.ErrMismatch):
		return 0, c.endStart(ctx, id, problems.Diagnostics{diagnostic(problems.RunRecordMismatch, problems.Params{"run": id})}, done)
	case errors.Is(err, plan.ErrMalformed):
		return 0, c.endStart(ctx, id, problems.Diagnostics{diagnostic(problems.RunMalformed, problems.Params{"run": id})}, done)
	case errors.Is(err, plan.ErrVersion):
		newer := problems.RunPlanNewer.Problem(problems.Params{"run": id, "version": run.PlanVersion, "latest": plan.Version})
		c.log.WarnContext(ctx, newer.Detail, "run", id, "code", newer.Code)
		return c.retry.newer, nil
	case stopping(ctx):
		return c.retry.first, nil
	case errors.Is(err, compile.ErrUnanswered):
		cause = causeUnanswered
	case ctx.Err() != nil:
		cause = causeTimeout
	}
	c.log.WarnContext(ctx, "coordinator: couldn't start a run, and tries again later", "run", id, "cause", cause)
	// The count isn't fenced by Start's hopper job: an attempt hopper
	// rescued from a replica still running it can count a try as well.
	var tries int
	terr := c.store.Tx(context.WithoutCancel(ctx), func(tx *store.Tx) error {
		var err error
		tries, err = tx.StartTried(id)
		return err
	})
	switch {
	case errors.Is(terr, store.ErrState):
		return 0, errSettled
	case terr != nil:
		return 0, terr
	case tries >= c.retry.tries:
		return 0, c.endStart(ctx, id, problems.Diagnostics{diagnostic(problems.RunStartUnanswered, problems.Params{"run": id, "tries": tries})}, done)
	}
	return c.retry.backoff(tries), nil
}

// endStart ends the run id, failed, with diags, which say why, in a
// transaction that done, if it isn't nil, writes to as well. It ends only
// a run that hasn't started, which another try may have meanwhile:
// errSettled.
func (c *Coordinator) endStart(ctx context.Context, id string, diags problems.Diagnostics, done func(context.Context, *store.Tx) error) error {
	// A try that ran out of time ends the run all the same.
	ctx = context.WithoutCancel(ctx)
	return c.store.Tx(ctx, func(tx *store.Tx) error {
		if r, err := tx.Run(id); err != nil || r.State != store.RunCreated {
			return cmp.Or(err, errSettled)
		}
		if err := tx.AddDiagnostics(id, diags); err != nil {
			return err
		}
		if err := tx.EndRun(id, plan.OutcomeFailure, ""); err != nil {
			return err
		}
		if done != nil {
			return done(ctx, tx)
		}
		return nil
	})
}

// problemDiagnostic is a run's diagnostic of p, a problem that failed it,
// as from Keys: of its kind, or, for a kind this release doesn't know, as
// a platform's Keys can give, with its code and params as they are.
func problemDiagnostic(p *problem.Problem) problems.Diagnostic {
	if k, ok := problems.Lookup(p.Code); ok {
		return diagnostic(k, p.Params)
	}
	return problems.Diagnostic{Type: p.Type, Code: p.Code, Detail: p.Detail, Params: p.Params}
}

// sealedOf is the stored record r as the seal package takes it.
func sealedOf(r *store.Record) seal.Sealed {
	return seal.Sealed{Key: r.WrappedKey, Record: r.Sealed}
}
