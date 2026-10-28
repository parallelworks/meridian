package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// watch looks at the live attempt a of a job of run: placed, claimed or
// running. Its worker reports to the worker API, which wakes the
// supervisor once the attempt is claimed or ends, and renews its lease;
// the supervisor watches the lease (docs/design.md, Leases and fencing),
// and returns how long to wait before it looks again:
//
//   - An attempt that hasn't been claimed has no lease: its backend says,
//     every heartbeat period, whether its worker will still claim it.
//   - Once the lease of an attempt that doesn't fence itself lapses, its
//     backend says, every heartbeat period, whether its worker still runs.
//     Only a backend that says it has ended loses it; one that says it
//     runs, or is queued, or can't decide, doesn't.
//   - An attempt whose job fences itself is lost once its lease has lapsed
//     and the fence margin passed: its worker has stopped the job by then.
//
// After a coordinator takes the endpoint workers reach it through, or its
// host wakes from sleep, or the store's clock is reset, no lapsed lease
// counts until the grace has passed, so that workers cut off meanwhile can
// reach it again.
func (c *Coordinator) watch(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (time.Duration, error) {
	now, graceEnds, err := c.leaseClock(ctx)
	if err != nil {
		return 0, err
	}
	switch {
	case a.State == store.AttemptPlacing:
		return c.checkWorker(ctx, run, job, a, now)
	case a.SelfFencing:
		lost := latest(a.LeaseLapses().Add(c.timing.FenceMargin), graceEnds)
		if now.Before(lost) {
			return c.waitFor(lost.Sub(now)), nil
		}
		return 0, c.lose(ctx, a, backend.Status{State: backend.Dead}, nil)
	}
	lapse := latest(a.LeaseLapses(), graceEnds)
	if now.Before(lapse) {
		return c.waitFor(lapse.Sub(now)), nil
	}
	return c.checkWorker(ctx, run, job, a, now)
}

// leaseClock returns the store's clock, and when the grace in which no
// lapsed lease counts ends: the grace after the latest time an endpoint of
// the store's was taken, or its holder's host woke, or a coordinator's
// host woke, or the store's clock was reset (store.Tx.GraceFrom); or the
// zero time if there is none.
//
// It reads them in a transaction that writes, so that a reset of the
// store's clock it comes on is recorded, with its grace, before the
// supervisor goes by what the store holds.
func (c *Coordinator) leaseClock(ctx context.Context) (now, graceEnds time.Time, err error) {
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if now, err = tx.Now(); err != nil {
			return err
		}
		from, err := tx.GraceFrom()
		if err != nil {
			return err
		}
		if !from.IsZero() {
			graceEnds = from.Add(c.timing.Grace)
		}
		return nil
	})
	return now, graceEnds, err
}

// latest returns the later of a and b.
func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// waitFor is how long the supervisor waits for what comes in d: d, but
// no longer than it waits for a live attempt, and some time, however
// little.
func (c *Coordinator) waitFor(d time.Duration) time.Duration {
	return max(min(d, c.supervision.wait), time.Millisecond)
}

// checkWorker asks the backend of the attempt a of the run's job what has
// become of its worker, now by the store's clock, and loses the attempt if
// the backend says its worker has ended; otherwise, as when the backend
// can't decide, it returns how long to wait before it asks again. An
// attempt whose member its group no longer has, whose backend can't be
// asked, is lost once the grace has passed since its lease lapsed, or
// since it was placed, for one not claimed: a failure before its first
// step, member_gone, which places the job again, when none had started.
//
// A check that gives a problem, a cause that won't pass by itself, as a
// host key that known_hosts doesn't have, counts as running, as one that
// can't decide does, but for CheckGrace at most, from the first such
// check since the backend last decided, or the attempt's lease was last
// renewed, as the store records it (store.Tx.CheckProblem), whichever
// replica checked: then the attempt is lost with the problem.
func (c *Coordinator) checkWorker(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, now time.Time) (time.Duration, error) {
	cand, p, err := c.placedMember(ctx, run, job, a)
	switch {
	case errors.Is(err, errMemberGone):
		since := a.CreatedAt
		if a.State != store.AttemptPlacing {
			since = a.LeaseLapses()
		}
		if now.Before(since.Add(c.timing.Grace)) {
			return c.waitFor(c.timing.Heartbeat), nil
		}
		return 0, c.lose(ctx, a, backend.Status{State: backend.Dead}, problems.MemberGone.Problem(problems.Params{"job": a.Job}))
	case err != nil && !errors.Is(err, errUncheckable):
		return 0, err
	}
	st := backend.Status{State: backend.Unknown}
	if err == nil {
		st, err = cand.driver.Check(ctx, cand.member, p)
		if prob, ok := errors.AsType[*problem.Problem](err); ok {
			var since time.Time
			err := c.store.Tx(ctx, func(tx *store.Tx) error {
				var err error
				since, err = tx.CheckProblem(a)
				return err
			})
			switch {
			case errors.Is(err, store.ErrState):
				// A heartbeat overtook the check: the worker reached the
				// coordinator since.
				return c.waitFor(c.timing.Heartbeat), nil
			case err != nil:
				return 0, err
			}
			if !now.Before(since.Add(c.timing.CheckGrace)) {
				return 0, c.lose(ctx, a, backend.Status{State: backend.Dead}, prob)
			}
		} else if err == nil && !a.ProblemSince.IsZero() {
			if err := c.store.Tx(ctx, func(tx *store.Tx) error { return tx.CheckDecided(a.ID) }); err != nil {
				return 0, err
			}
		}
		if err != nil {
			if a.Private {
				c.log.InfoContext(ctx, "coordinator: a backend couldn't say whether an attempt's worker runs, which counts as running", "attempt", a.ID)
			} else {
				c.log.InfoContext(ctx, "coordinator: a backend couldn't say whether an attempt's worker runs, which counts as running", "attempt", a.ID, "error", err)
			}
			st = backend.Status{State: backend.Unknown}
		}
	}
	if st.State != backend.Dead {
		return c.waitFor(c.timing.Heartbeat), nil
	}
	return 0, c.lose(ctx, a, st, nil)
}

// errUncheckable is what placedMember returns for an attempt whose
// worker's backend can't be asked now, as when Groups can't answer, which
// counts as one that can't decide; errMemberGone, placedOn's, is one whose
// member its group no longer has, which is definite.
var errUncheckable = errors.New("coordinator: the attempt's member can't be told now")

// A memberEntry is what placedMember found, until when the coordinator
// keeps it.
type memberEntry struct {
	cand      *candidate
	placement backend.Placement
	err       error
	until     time.Time
}

// memberFor is how long the coordinator keeps what placedMember found of an
// attempt's member, so that it readies the attempt's job and asks Groups
// once in that while, rather than every heartbeat period; and maxMembers
// how many attempts' it keeps.
const (
	memberFor  = time.Minute
	maxMembers = 4096
)

// placedMember returns the member the attempt a of the run's job was
// placed on, with its driver, and its placement, with the backend's ID for
// it, which a private attempt's place holds sealed (placedOn);
// errMemberGone when its group, or the group, no longer has the member;
// or errUncheckable when it can't be told now, as when Groups can't
// answer. What it finds, but the last, it keeps for a while (memberFor).
func (c *Coordinator) placedMember(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (*candidate, backend.Placement, error) {
	c.mu.Lock()
	e, ok := c.members[a.ID]
	c.mu.Unlock()
	if ok && time.Now().Before(e.until) {
		return e.cand, e.placement, e.err
	}
	cand, at, _, err := c.placedOn(ctx, run, job, a)
	if groups, ok := errors.AsType[*groupsError](err); ok {
		return nil, backend.Placement{}, fmt.Errorf("%w: %w", errUncheckable, groups.err)
	}
	p := backend.Placement{Attempt: a.ID, ID: a.Placement.ID, WorkerDigest: a.Placement.WorkerDigest}
	if a.Private {
		p.ID = at.ID
	}
	if err == nil || errors.Is(err, errMemberGone) {
		c.mu.Lock()
		if len(c.members) >= maxMembers {
			clear(c.members)
		}
		c.members[a.ID] = memberEntry{cand: cand, placement: p, err: err, until: time.Now().Add(memberFor)}
		c.mu.Unlock()
	}
	return cand, p, err
}

// lose ends the attempt a, whose worker was lost, as st, its backend's
// last word on it, says: with the reason timeout or out-of-memory when its
// scheduler, or the kernel, ended it so; with worker-lost once a step has
// started, with why, its problem, if it has one; and otherwise as a
// failure before the job's first step, with why, or worker_ended_early
// for nil, which places the job again. The
// attempt must still be as a was, not claimed nor renewed meanwhile, as a
// worker that reached the coordinator again would have it, or nothing is
// ended. Either way, the supervisor looks again.
func (c *Coordinator) lose(ctx context.Context, a *store.Attempt, st backend.Status, why *problem.Problem) error {
	end := store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost, Problem: why}
	switch {
	case st.End == backend.EndTimedOut:
		end.Reason = plan.ReasonTimeout
	case st.End == backend.EndOutOfMemory:
		end.Reason = plan.ReasonOutOfMemory
	case a.State != store.AttemptRunning:
		if why == nil {
			why = problems.WorkerEndedEarly.Problem(problems.Params{"job": a.Job})
		}
		end = store.AttemptEnd{Kind: store.EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: why}
	}
	ended := false
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		now, err := tx.Attempt(a.ID)
		switch {
		case err != nil:
			return err
		case !now.Holds || now.State != a.State || !now.RenewedAt.Equal(a.RenewedAt):
			return nil
		}
		ended = true
		return tx.EndAttempt(a.ID, end)
	})
	if err != nil && !errors.Is(err, store.ErrFenced) {
		return err
	}
	if ended && end.Kind == store.EndLost {
		c.log.InfoContext(ctx, "coordinator: an attempt's worker was lost", "run", a.Run, "job", a.Job, "attempt", a.ID, "reason", end.Reason)
	}
	return errAgain
}

// workerLost ends the job, whose latest attempt, a, was lost after a step
// started, or places it again after its retry:'s wait, by the store's
// clock, as its retry: asks: when it lists worker-lost, and has run the
// job again fewer times than its max-retries. Each new attempt waits its
// interval, twice as long for each before it, up to an hour; and each
// attempt that started a step counts, as the new attempt's claim says
// (workerapi.Claim.Retry). An attempt its scheduler, or the kernel, ended,
// at its time limit or for its memory, fails the job with that reason. A
// job that fails so has the problem the attempt was lost with, if it has
// one, among its diagnostics, as why.
func (c *Coordinator) workerLost(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, done func(context.Context, *store.Tx) error) (time.Duration, error) {
	failed := func(k *problems.Kind, params problems.Params) error {
		params["job"] = job.Key
		diags := problems.Diagnostics{diagnostic(k, params)}
		if p := a.End.Problem; p != nil {
			// Stored masked when where the job ran was private
			// (store.Tx.EndAttempt).
			diags = append(diags, problemDiagnostic(p))
		}
		return c.endJob(ctx, run.ID, job.Key, store.JobEnd{
			Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: a.End.Reason, Diagnostics: diags,
		}, done)
	}
	if a.End.Reason != plan.ReasonWorkerLost {
		return 0, failed(problems.WorkerKilled, problems.Params{"reason": string(a.End.Reason)})
	}
	r, err := c.readyJob(ctx, run, job.Key)
	switch {
	case err != nil:
		return 0, err
	case r.result.Job == nil:
		return 0, fmt.Errorf("coordinator: run %s's job %q has an attempt, but Ready gives no job", run.ID, job.Key)
	}
	retry := r.result.Job.Retry
	again, retried := retriesAgain(retry, a.End.Reason, job.AttemptsStarted)
	switch {
	case retry == nil || !slices.Contains(retry.Reasons, plan.ReasonWorkerLost):
		return 0, failed(problems.WorkerLost, problems.Params{})
	case !again:
		return 0, failed(problems.WorkerLostAgain, problems.Params{"retries": retried})
	}
	var now time.Time
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		now, err = tx.Now()
		return err
	}); err != nil {
		return 0, err
	}
	// The wait is never longer than it is from the end, which can be
	// ahead of now after a reset of the clock that no transaction has
	// recorded yet, as setupFailed's.
	pause := retryWait(retry, retried+1)
	if wait := a.EndedAt.Add(pause).Sub(now); wait > 0 {
		return min(wait, pause), nil
	}
	return c.placeJob(ctx, run, job, done)
}

// retriesAgain reports whether a job whose retry: is r, or nil for none,
// runs again after an attempt of it that ended for reason, once started
// of its attempts have started a step, that one among them: whether r
// lists reason, and has run the job again fewer times than its
// max-retries; and how many times it has, every attempt that started a
// step but the first. An end it runs the job again after isn't the job's,
// and doesn't fail it, or its run.
func retriesAgain(r *plan.ReadyJobRetry, reason plan.Reason, started int) (again bool, retried int) {
	retried = max(started-1, 0)
	return r != nil && slices.Contains(r.Reasons, reason) && retried < r.MaxRetries, retried
}

// retryWait is how long the n-th new attempt of a job whose retry: is r
// waits, from 1: its interval, twice as long for each one before it, up
// to an hour, or the interval itself when that is longer.
func retryWait(r *plan.ReadyJobRetry, n int) time.Duration {
	interval := time.Duration(r.IntervalSeconds) * time.Second
	most := max(interval, time.Hour)
	d := interval
	for range n - 1 {
		if d >= most {
			break
		}
		d *= 2
	}
	return min(d, most)
}

// watchWakes looks every so often for the coordinator's host waking from
// sleep, until ctx is done: when its wall clock has moved past its
// monotonic clock, which stops while the host sleeps, by more than
// Timing.WakeJump. Then no lapsed lease counts until the grace has passed,
// for any of the store's coordinators (woke).
//
// Go's monotonic clock stops while the host sleeps on Linux and macOS. On
// FreeBSD it is CLOCK_MONOTONIC, which reads the same kernel clock as
// CLOCK_UPTIME there, so reading the one in place of the other changes
// nothing: where that clock counts the time the host is suspended, a wake
// isn't seen there, and only the grace after an endpoint is taken applies.
func (c *Coordinator) watchWakes(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		if slept(now.Round(0).Sub(last.Round(0)), now.Sub(last), c.timing.WakeJump) {
			c.woke(ctx)
		}
		last = now
	}
}

// slept reports whether the host slept while the wall clock moved on by
// wall and the monotonic clock, which stops while the host sleeps, by
// mono: whether the wall clock moved past it by more than jump. A wall
// clock set back is no sleep.
func slept(wall, mono, jump time.Duration) bool {
	return wall-mono > jump
}

// wakeReadied wakes the supervisors of the readied jobs of the running
// runs, once the store has taken it that the host's clock was reset: a
// supervisor that waited on hopper's schedule while the clock was ahead
// may have been put off until the clock reaches that time again, and
// looks again now. One that is running as it is woken looks before it
// waits, by the right clock.
func (c *Coordinator) wakeReadied(ctx context.Context) {
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		jobs, err := tx.ReadiedJobs()
		for _, j := range jobs {
			c.retryTx(ctx, tx, &waking{id: j.SupervisorID, run: j.Run, key: j.Key})
		}
		return err
	}); err != nil {
		c.log.WarnContext(ctx, "coordinator: the store's clock was reset, but its readied supervisors couldn't be woken", "error", err)
	}
}

// woke records that the coordinator's host woke from sleep: a grace for
// every coordinator of the store starts now, by the store's clock
// (store.Tx.StartGrace).
func (c *Coordinator) woke(ctx context.Context) {
	if err := c.store.Tx(ctx, func(tx *store.Tx) error { return tx.StartGrace() }); err != nil {
		c.log.WarnContext(ctx, "coordinator: the host woke from sleep, but the grace couldn't be stored", "error", err)
		return
	}
	c.log.InfoContext(ctx, "coordinator: the host woke from sleep, so no lease that lapsed meanwhile counts until the grace has passed", "grace", c.timing.Grace)
}
