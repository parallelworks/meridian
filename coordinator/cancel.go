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
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A run stops early when a person cancels it, or when it runs past its own
// timeout:, as docs/design.md says under Cancellation, timeouts and
// cleanup: its stop record is stored first, with the reason, cancelled or
// timeout, in the transaction that tells the running attempts of the jobs
// that don't outlive it to stop (workerapi.Control.Cancel); then each
// job's supervisor sees it through for its job (stoppedAttempt), and the
// finisher takes the run's outcome from it.

// timeoutArgs is the hopper job that watches a run's own timeout:, which
// Start inserts for a run that has one (watchTimeout).
type timeoutArgs struct {
	Run string `json:"run"`
}

func (timeoutArgs) Kind() string { return "meridian.timeout" }

// ErrRunEnded is what Cancel returns for a run that has ended, and
// ErrRunNotFound for one the store doesn't have. (canary's status.go,
// which the stack this is on doesn't have yet, has ErrRunNotFound too:
// the rebase onto it keeps that one.)
var (
	ErrRunEnded    = errors.New("coordinator: the run has ended")
	ErrRunNotFound = errors.New("coordinator: no such run")
)

// Cancel cancels the run id, as a person does: it stores the run's stop
// record, with the reason cancelled, and tells the workers of the jobs
// that don't outlive a cancel to stop; each job's supervisor then sees its
// job through. A job whose if: is cancel-aware runs on; the run's outcome
// is cancelled, whatever its jobs do. A run that hasn't started ends
// cancelled at once. Cancelling a run that was stopped already changes
// nothing; one that has ended is ErrRunEnded, and one the store doesn't
// have ErrRunNotFound.
func (c *Coordinator) Cancel(ctx context.Context, id string) error {
	return c.stop(ctx, id, plan.ReasonCancelled)
}

// stop stops the run id early, for reason, cancelled or timeout, as Cancel
// says.
func (c *Coordinator) stop(ctx context.Context, id string, reason plan.Reason) error {
	for try := 0; ; try++ {
		var run *store.Run
		err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			run, err = tx.Run(id)
			return err
		})
		switch {
		case errors.Is(err, store.ErrNotFound):
			return ErrRunNotFound
		case err != nil:
			return err
		case run.State == store.RunEnded:
			return ErrRunEnded
		case !run.StoppedAt.IsZero():
			return nil
		case run.State == store.RunCreated:
			err = c.stopCreated(ctx, id, reason)
		default:
			err = c.stopRunning(ctx, run, reason)
		}
		if !errors.Is(err, errAgain) || try >= 10 {
			return err
		}
	}
}

// stopCreated ends the run id, which hasn't started, as stopped for reason:
// its Start then leaves it as it is, and so does its finisher.
func (c *Coordinator) stopCreated(ctx context.Context, id string, reason plan.Reason) error {
	return c.store.Tx(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(id)
		switch {
		case err != nil:
			return err
		case r.State == store.RunEnded:
			return ErrRunEnded
		case r.State != store.RunCreated:
			return errAgain
		}
		if _, err := tx.StopRun(id, reason); err != nil {
			return err
		}
		return tx.EndRun(id, plan.OutcomeCancelled, reason)
	})
}

// stopRunning stores the stop record of run, which has started, and tells
// the running attempts of its jobs that don't outlive the stop to stop,
// in one transaction; then it wakes the supervisors of its readied jobs,
// and ends at once the jobs not yet readied whose if: takes them so
// (plan.CancelPathSkip).
func (c *Coordinator) stopRunning(ctx context.Context, run *store.Run, reason plan.Reason) error {
	paths, err := c.cancelPaths(ctx, run)
	if err != nil {
		return err
	}
	var (
		stopped bool
		changed bool
		readied []string
	)
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if stopped, err = tx.StopRun(run.ID, reason); err != nil || !stopped {
			return err
		}
		live, err := tx.LiveAttempts(run.ID)
		if err != nil {
			return err
		}
		for _, a := range live {
			// A job readied meanwhile has its supervisor decide.
			if outlives, known := paths.outlives[a.Job]; !known || outlives {
				continue
			}
			if err := tellStop(tx, a.ID, reason); err != nil {
				return err
			}
		}
		jobs, err := tx.Jobs(run.ID)
		if err != nil {
			return err
		}
		for _, j := range jobs {
			if j.Outcome == "" && !j.ReadyAt.IsZero() {
				readied = append(readied, j.Key)
			}
		}
		changed = tx.ControlsChanged()
		return nil
	})
	switch {
	case errors.Is(err, store.ErrState):
		return ErrRunEnded
	case err != nil || !stopped:
		return err
	}
	if changed {
		c.controlsChanged()
	}
	c.log.InfoContext(ctx, "coordinator: a run was stopped", "run", run.ID, "reason", reason)
	// A supervisor waiting for the jobs its job needs isn't woken: it
	// readies its job once they have finished, as stopped.
	for _, key := range readied {
		c.wakeJob(ctx, run.ID, key)
	}
	// The stop is stored: a job that can't be ended now, its supervisor
	// ends once its needs have, as stopped.
	run.StoppedAt, run.StopReason = time.Now(), reason
	for _, key := range paths.skipNow {
		if err := c.endUnreadied(ctx, run, key); err != nil {
			c.log.WarnContext(ctx, "coordinator: a job of a stopped run couldn't be ended at once, so its supervisor ends it", "run", run.ID, "job", key, "error", err)
		}
	}
	return nil
}

// cancelPaths is what becomes of a run's jobs once it stops, as
// cancelPaths says.
type cancelPaths struct {
	// outlives says, of each job readied or whose path is
	// plan.CancelPathOutlive, whether it outlives the stop.
	outlives map[string]bool
	// skipNow are the jobs not readied yet that end at once.
	skipNow []string
}

// cancelPaths returns what becomes of run's jobs that haven't ended once it
// stops: each job readied, or whose if: is cancel-aware, outlives the stop
// or doesn't; and the jobs not readied yet whose path ends them at once,
// skipped because their needs failed, without waiting for them.
func (c *Coordinator) cancelPaths(ctx context.Context, run *store.Run) (*cancelPaths, error) {
	p, err := c.plans.get(run)
	if err != nil {
		return nil, err
	}
	values, err := c.runValues(ctx, run)
	if err != nil {
		return nil, err
	}
	var jobs []*store.Job
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		jobs, err = tx.Jobs(run.ID)
		return err
	}); err != nil {
		return nil, err
	}
	out := &cancelPaths{outlives: map[string]bool{}}
	for _, j := range jobs {
		if j.Outcome != "" {
			continue
		}
		path, err := p.CancelPath(j.Name, values, nil)
		if err != nil {
			return nil, err
		}
		switch {
		case path == plan.CancelPathOutlive:
			out.outlives[j.Key] = true
		case j.ReadyAt.IsZero():
			if pj := planJob(p, j.Name); path == plan.CancelPathSkip && pj != nil && len(pj.Needs) > 0 {
				out.skipNow = append(out.skipNow, j.Key)
			}
		default:
			if out.outlives[j.Key], err = c.readiedOutlives(ctx, run, j, path); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// readiedOutlives reports whether the job j of run, readied, whose path is
// path, outlives the run's stop: whether Ready gave it with IfAlways, as a
// job whose if:'s value was the word always once its needs had ended.
func (c *Coordinator) readiedOutlives(ctx context.Context, run *store.Run, j *store.Job, path plan.CancelPath) (bool, error) {
	if path != plan.CancelPathEnd {
		return path == plan.CancelPathOutlive, nil
	}
	r, err := c.readyJob(ctx, run, j.Key)
	if err != nil {
		return false, err
	}
	return r.result.Job != nil && r.result.Job.IfAlways, nil
}

// jobOutlives reports whether the readied job j of run, which has been
// stopped, outlives the stop. The answer never changes once the run is
// stopped, and the coordinator keeps it, rather than open the run's
// secret record at each look.
func (c *Coordinator) jobOutlives(ctx context.Context, run *store.Run, j *store.Job) (bool, error) {
	id := run.ID + "/" + j.Key
	c.mu.Lock()
	outlives, ok := c.outlives[id]
	c.mu.Unlock()
	if ok {
		return outlives, nil
	}
	p, err := c.plans.get(run)
	if err != nil {
		return false, err
	}
	values, err := c.runValues(ctx, run)
	if err != nil {
		return false, err
	}
	path, err := p.CancelPath(j.Name, values, nil)
	if err != nil {
		return false, err
	}
	if outlives, err = c.readiedOutlives(ctx, run, j, path); err != nil {
		return false, err
	}
	c.mu.Lock()
	if len(c.outlives) >= maxTimeouts {
		clear(c.outlives)
	}
	c.outlives[id] = outlives
	c.mu.Unlock()
	return outlives, nil
}

// placesNoMore reports whether the job, whose latest attempt is a, or nil
// before its first, has no attempt that runs, nor one whose worker's end
// is the job's, and was readied before its run stopped, which it doesn't
// outlive: its supervisor
// places no attempt of it again, as after a failure before its first step,
// or once a backend answers, and ends it as the stop says (stopEnd).
func (c *Coordinator) placesNoMore(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (bool, error) {
	switch {
	case run.StoppedAt.IsZero(), job.ReadyAt.IsZero(), job.ReadiedStopped:
		// A job readied once the run had stopped is as Ready gave it.
		return false, nil
	case a == nil:
	case a.State != store.AttemptEnded:
		return false, nil
	case a.End.Kind == store.EndCompleted && a.End.Reason != plan.ReasonSetupFailed:
		// Its worker's end is the job's.
		return false, nil
	}
	outlives, err := c.jobOutlives(ctx, run, job)
	return !outlives, err
}

// stopEnd is how a job of a run stopped for reason that doesn't outlive the
// stop ends, as no worker ended it: cancelled after a cancel; failed, with
// the reason timeout, after the run's timeout.
func stopEnd(reason plan.Reason) store.JobEnd {
	if reason == plan.ReasonTimeout {
		return store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonTimeout}
	}
	return store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeCancelled}, Reason: plan.ReasonCancelled}
}

// runValues are run's values, the run's state's and its secret record's
// joined, as Start gave them.
func (c *Coordinator) runValues(ctx context.Context, run *store.Run) (plan.RunValues, error) {
	if run.StartState == nil {
		return plan.RunValues{}, fmt.Errorf("coordinator: run %s hasn't started", run.ID)
	}
	rec, err := c.openRecord(ctx, run.ID)
	if err != nil {
		return plan.RunValues{}, err
	}
	if rec.Run == nil {
		return run.StartState.Run, nil
	}
	return plan.JoinRunValues(*rec.Run, run.StartState.Run), nil
}

// openRecord opens the secret record of the run id.
func (c *Coordinator) openRecord(ctx context.Context, id string) (*plan.Record, error) {
	var stored *store.Record
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = tx.Record(id)
		return err
	}); err != nil {
		return nil, err
	}
	return c.sealer.Open(ctx, id, stored.Generation, sealedOf(stored))
}

// endUnreadied ends the job key of run, stopped, which hasn't been readied
// and whose path ends it at once: Ready, given the needs that have
// finished, skips it because they failed, or fails it with its problems.
// Its supervisor, which waits for the jobs it needs, then finds it ended.
func (c *Coordinator) endUnreadied(ctx context.Context, run *store.Run, key string) error {
	r, err := c.readyJob(ctx, run, key)
	if errors.Is(err, store.ErrState) {
		// Ended meanwhile.
		return nil
	}
	if err != nil {
		return err
	}
	end, ok := r.end()
	if !ok {
		return fmt.Errorf("coordinator: run %s's job %q, stopped before it was readied, was readied to run", run.ID, key)
	}
	err = c.endJob(ctx, run.ID, key, end, func(context.Context, *store.Tx) error { return nil })
	if errors.Is(err, errSettled) || errors.Is(err, errAgain) {
		return nil
	}
	return err
}

// tellStop asks the worker of the attempt id to stop its job, for reason:
// cancelled, or timeout, which it takes as its job's own timeout, keeping
// what its control asked already (store.Tx.UpdateControl). Its grace is
// the worker's own. An attempt told to stop already is left as it is, and
// so is one that has ended.
func tellStop(tx *store.Tx, id string, reason plan.Reason) error {
	_, err := tx.UpdateControl(id, func(ctl *workerapi.Control) bool {
		if ctl.Cancel {
			return false
		}
		ctl.Cancel, ctl.Reason, ctl.GraceSeconds = true, reason, 0
		return true
	})
	if errors.Is(err, store.ErrState) {
		return nil
	}
	return err
}

// tell asks the worker of the attempt id to stop its job, for reason, as
// tellStop does, in a transaction of its own.
func (c *Coordinator) tell(ctx context.Context, id string, reason plan.Reason) error {
	var changed bool
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		if err := tellStop(tx, id, reason); err != nil {
			return err
		}
		changed = tx.ControlsChanged()
		return nil
	})
	if changed {
		c.controlsChanged()
	}
	return err
}

// liveAttempt looks at the job's live attempt a, placed, claimed or
// running: its worker reports to the worker API, which wakes the
// supervisor once the attempt ends. Once the run is stopped, and the job
// doesn't outlive it, the supervisor sees the stop through for it
// (stoppedAttempt); and it enforces the job's timeout:, as a backstop for a
// worker that doesn't (backstop). It returns how long to wait before it
// looks again, or 0 once it has ended the job.
func (c *Coordinator) liveAttempt(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	if !run.StoppedAt.IsZero() {
		outlives, err := c.jobOutlives(ctx, run, job)
		if err != nil {
			return 0, err
		}
		if !outlives {
			return c.stoppedAttempt(ctx, run, job, a, done)
		}
	}
	if a.State == store.AttemptPlacing {
		return c.supervision.wait, nil
	}
	return c.backstop(ctx, run, job, a, done)
}

// stoppedAttempt sees the stop of run through for the job's live attempt
// a, which doesn't outlive it: after a cancel, a placement no worker has
// claimed is withdrawn, and the job ends cancelled; after the run's
// timeout, it isn't, and its worker is told at once, as it claims it, that
// the run's timeout has passed. A worker that claimed it is told to stop,
// and waited for while it renews its lease, since its cleanups have no
// timeout; once its lease lapses, its placement is withdrawn, and the job
// ends cancelled, or failed with the reason timeout.
func (c *Coordinator) stoppedAttempt(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	reason := run.StopReason
	end := stopEnd(reason)
	if reason == plan.ReasonCancelled && a.State == store.AttemptPlacing {
		return 0, c.withdraw(ctx, run, job, a, end, done)
	}
	if err := c.tell(ctx, a.ID, reason); err != nil {
		return 0, err
	}
	if a.State != store.AttemptPlacing {
		lapsed, err := c.leaseLapsed(ctx, a)
		if err != nil {
			return 0, err
		}
		if lapsed {
			return 0, c.withdraw(ctx, run, job, a, end, done)
		}
	}
	return c.supervision.wait, nil
}

// backstop enforces the job's timeout:, as a backstop for a worker that
// doesn't: once the job's timeout, the worker's own time to stop its
// steps, a heartbeat period and the fence margin have passed since the
// attempt a was claimed, without its end, it tells the worker to stop the
// job, with the reason timeout, and withdraws its placement once its lease
// lapses, failing the job with the reason timeout.
func (c *Coordinator) backstop(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	timeout, err := c.jobTimeout(ctx, run, job.Key, a.ID)
	if err != nil || timeout <= 0 {
		return c.supervision.wait, err
	}
	now, err := c.storeNow(ctx)
	if err != nil {
		return 0, err
	}
	due := a.ClaimedAt.Add(timeout + c.timing.WorkerStop + c.timing.Heartbeat + c.timing.FenceMargin)
	if now.Before(due) {
		return max(min(due.Sub(now), c.supervision.wait), time.Millisecond), nil
	}
	if err := c.tell(ctx, a.ID, plan.ReasonTimeout); err != nil {
		return 0, err
	}
	if lapsed, err := c.leaseLapsed(ctx, a); err != nil || !lapsed {
		return c.timing.Heartbeat, err
	}
	return 0, c.withdraw(ctx, run, job, a, store.JobEnd{
		Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonTimeout,
		Diagnostics: problems.Diagnostics{diagnostic(problems.JobTimeoutEnforced, problems.Params{"job": job.Key})},
	}, done)
}

// jobTimeout returns the job key's timeout:, as Ready gave it to the
// attempt id, or 0 for none; once per attempt, which the coordinator keeps.
func (c *Coordinator) jobTimeout(ctx context.Context, run *store.Run, key, id string) (time.Duration, error) {
	c.mu.Lock()
	timeout, ok := c.timeouts[id]
	c.mu.Unlock()
	if ok {
		return timeout, nil
	}
	r, err := c.readyJob(ctx, run, key)
	if err != nil {
		return 0, err
	}
	if r.result.Job != nil {
		timeout = time.Duration(r.result.Job.TimeoutSeconds) * time.Second
	}
	c.mu.Lock()
	if len(c.timeouts) >= maxTimeouts {
		clear(c.timeouts)
	}
	c.timeouts[id] = timeout
	c.mu.Unlock()
	return timeout, nil
}

// maxTimeouts bounds the attempts whose job's timeout the coordinator
// keeps.
const maxTimeouts = 4096

// storeNow returns the store's clock.
func (c *Coordinator) storeNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		now, err = tx.Now()
		return err
	})
	return now, err
}

// leaseLapsed reports whether the lease of the attempt a, claimed, has
// lapsed, by the store's clock.
func (c *Coordinator) leaseLapsed(ctx context.Context, a *store.Attempt) (bool, error) {
	if a.RenewedAt.IsZero() {
		return false, nil
	}
	now, err := c.storeNow(ctx)
	return err == nil && !now.Before(a.LeaseLapses()), err
}

// withdraw ends the job's live attempt a, withdrawn, and the job as end
// says, in one transaction, which done writes to as well, and then has
// the attempt's backend withdraw its placement: its worker's reports are
// refused from then on. An attempt changed since the supervisor looked at
// it, claimed or renewed, is errAgain.
func (c *Coordinator) withdraw(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, end store.JobEnd, done func(context.Context, *store.Tx) error) error {
	// Where it was placed is found while its job is as it was.
	m, d, private, merr := c.attemptMember(ctx, run, a)
	var changed bool
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		now, err := tx.Attempt(a.ID)
		switch {
		case err != nil:
			return err
		case !now.Holds || now.State != a.State || !now.RenewedAt.Equal(a.RenewedAt):
			return errAgain
		}
		if err := tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndWithdrawn, Reason: end.Reason}); err != nil {
			return err
		}
		if err := tx.EndJob(run.ID, job.Key, end); err != nil {
			return err
		}
		if end.Result.Outcome == plan.OutcomeFailure {
			if _, err := tx.MarkJobFailed(run.ID); err != nil {
				return err
			}
		}
		if err := done(ctx, tx); err != nil {
			return err
		}
		changed = tx.ControlsChanged()
		return nil
	})
	if err != nil {
		return err
	}
	if changed {
		c.controlsChanged()
	}
	err = merr
	if err == nil {
		err = d.Cancel(ctx, m, backend.Placement{Attempt: a.ID, ID: a.Placement.ID, WorkerDigest: a.Placement.WorkerDigest})
	}
	switch {
	case err == nil:
	case private:
		c.log.WarnContext(ctx, "coordinator: a placement withdrawn couldn't be cancelled with its backend", "attempt", a.ID)
	default:
		c.log.WarnContext(ctx, "coordinator: a placement withdrawn couldn't be cancelled with its backend", "attempt", a.ID, "error", err)
	}
	return nil
}

// attemptMember returns the member of run's group the attempt a was placed
// on, and its driver, and whether where its job runs is private; or an
// error when the member can't be told, as when its group no longer has
// it, or Groups can't answer.
func (c *Coordinator) attemptMember(ctx context.Context, run *store.Run, a *store.Attempt) (*backend.Member, backend.Driver, bool, error) {
	var (
		place  store.Place
		chosen bool
		stored *store.Record
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if place, chosen, err = tx.ChosenPlace(a.ID); err != nil || !chosen {
			return err
		}
		stored, err = tx.Record(run.ID)
		return err
	})
	switch {
	case err != nil:
		return nil, nil, false, err
	case !chosen:
		return nil, nil, false, fmt.Errorf("coordinator: attempt %s has no place", a.ID)
	}
	private := place.Private != nil
	if place, err = c.openPlace(ctx, run.ID, a.ID, stored, place); err != nil {
		return nil, nil, private, err
	}
	r, err := c.readyJob(ctx, run, a.Job)
	switch {
	case err != nil:
		return nil, nil, private, err
	case r.result.Job == nil:
		return nil, nil, private, fmt.Errorf("coordinator: run %s's job %q has an attempt, but Ready gives no job", run.ID, a.Job)
	}
	g, d, err := c.group(ctx, run, r.result.Job, private)
	switch {
	case err != nil:
		return nil, nil, private, err
	case d != nil || g.Name != place.Group:
		return nil, nil, private, fmt.Errorf("coordinator: attempt %s's group is gone", a.ID)
	}
	i := slices.IndexFunc(g.Members, func(m backend.Member) bool { return m.Name == place.Member && m.Backend == place.Backend })
	if i < 0 {
		return nil, nil, private, fmt.Errorf("coordinator: attempt %s's member is gone", a.ID)
	}
	m := &g.Members[i]
	return m, c.driver(m), private, nil
}

// timeoutWorker works runs' timeout watchers (timeoutArgs).
type timeoutWorker struct {
	hopper.WorkerDefaults[timeoutArgs]
	c *Coordinator
}

// Work watches the run's timeout, as watchTimeout does.
func (w *timeoutWorker) Work(ctx context.Context, job *hopper.Job[timeoutArgs]) error {
	wait, err := w.c.watchTimeout(ctx, job.Args.Run)
	switch {
	case stopping(ctx):
		return hopper.Snooze(w.c.retry.first)
	case err != nil:
		return err
	case wait > 0:
		return hopper.Snooze(wait)
	}
	return nil
}

// watchTimeout stops the run id, with the reason timeout, once its own
// timeout: has passed since it started, by the store's clock, and returns
// how long to wait before it looks again until then: until the timeout,
// or, for a timeout made from a variable or a password, a minute at most,
// so that hopper's schedule doesn't show it. It is done with a run that
// has ended, or was stopped.
func (c *Coordinator) watchTimeout(ctx context.Context, id string) (time.Duration, error) {
	var (
		run *store.Run
		now time.Time
	)
	err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		if run, err = tx.Run(id); err != nil {
			return err
		}
		now, err = tx.Now()
		return err
	})
	switch {
	case err != nil:
		return 0, err
	case run.State != store.RunRunning || !run.StoppedAt.IsZero() || run.StartState == nil:
		return 0, nil
	}
	timeout, err := c.runTimeout(ctx, run)
	if err != nil || timeout <= 0 {
		return 0, err
	}
	if deadline := run.StartedAt.Add(timeout); now.Before(deadline) {
		wait := deadline.Sub(now)
		if run.StartState.TimeoutPrivate {
			wait = min(wait, time.Minute)
		}
		return wait, nil
	}
	if err := c.stop(ctx, id, plan.ReasonTimeout); err != nil && !errors.Is(err, ErrRunEnded) {
		return 0, err
	}
	return 0, nil
}

// runTimeout returns run's timeout:, from its state, or, when it is
// private, from its secret record, which the coordinator keeps then, or 0
// for none.
func (c *Coordinator) runTimeout(ctx context.Context, run *store.Run) (time.Duration, error) {
	if !run.StartState.TimeoutPrivate {
		return time.Duration(run.StartState.TimeoutSeconds) * time.Second, nil
	}
	c.mu.Lock()
	timeout, ok := c.timeouts["run "+run.ID]
	c.mu.Unlock()
	if ok {
		return timeout, nil
	}
	rec, err := c.openRecord(ctx, run.ID)
	if err != nil {
		return 0, err
	}
	if rec.TimeoutSeconds != nil {
		timeout = time.Duration(*rec.TimeoutSeconds) * time.Second
	}
	c.mu.Lock()
	c.timeouts["run "+run.ID] = timeout
	c.mu.Unlock()
	return timeout, nil
}
