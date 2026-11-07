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
//     Its backend is asked how it ended, each heartbeat period, for a
//     while (fencedEnd), so that a scheduler's time limit or memory limit
//     is its reason.
//
// After a coordinator takes the endpoint workers reach it through, or its
// host wakes from sleep, or the store's clock is reset, no lapsed lease
// counts until the grace has passed, so that workers cut off meanwhile can
// reach it again.
func (c *Coordinator) watch(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (time.Duration, error) {
	wait, _, err := c.watchFenced(ctx, run, job, a)
	return wait, err
}

// watchFenced is watch, which also reports whether a is a self-fencing
// attempt past its lease and the fence margin, whose end its backend's
// word on its worker decides (fencedEnd).
func (c *Coordinator) watchFenced(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (time.Duration, bool, error) {
	now, graceEnds, err := c.leaseClock(ctx)
	if err != nil {
		return 0, false, err
	}
	switch {
	case a.State == store.AttemptPlacing:
		wait, err := c.checkWorker(ctx, run, job, a, now)
		return wait, false, err
	case a.SelfFencing:
		lost := latest(a.LeaseLapses().Add(c.timing.FenceMargin), graceEnds)
		if now.Before(lost) {
			return c.waitFor(lost.Sub(now)), false, nil
		}
		wait, err := c.fencedEnd(ctx, run, job, a, now, lost)
		return wait, true, err
	}
	lapse := latest(a.LeaseLapses(), graceEnds)
	if now.Before(lapse) {
		return c.waitFor(lapse.Sub(now)), false, nil
	}
	wait, err := c.checkWorker(ctx, run, job, a, now)
	return wait, false, err
}

// fencedEnd asks the backend of the attempt a, whose job fences itself,
// and whose lease and the fence margin passed at lost, how its worker
// ended, now by the store's clock, each call bounded (boundedCheck); and
// loses the attempt as a Dead status says, with its scheduler's end at its
// time limit or for its memory, or its problem, so that a retry: that
// lists worker-lost doesn't run a job its scheduler timed out again, or
// else with worker-lost. Any other answer, an error or none in time, it
// asks again a heartbeat period later, for as long as the backend can take
// to see how a worker ended (endWait), as a scheduler's whose snapshot of
// its queue, or the records it waits for, don't show the job's end yet:
// then it loses the attempt with worker-lost, and the problem the last
// check gave as its error, if it gave one, as checkWorker does once
// CheckGrace has passed. A backend that needs no while, as one whose
// workers are processes, decides at the first check. Waiting is safe: the
// worker has stopped the job, and the store refuses what it reports. A
// member its group no longer has can't be asked, and is worker-lost at
// once; one that can't be told now, as while Groups doesn't answer, is
// asked again only while its place's driver's endWait lasts.
func (c *Coordinator) fencedEnd(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt, now, lost time.Time) (time.Duration, error) {
	cand, p, err := c.placedMember(ctx, run, job, a)
	switch {
	case errors.Is(err, errMemberGone):
		return 0, c.lose(ctx, a, backend.Status{State: backend.Dead}, nil)
	case err != nil && !errors.Is(err, errUncheckable):
		return 0, err
	}
	st := backend.Status{State: backend.Unknown}
	if err == nil {
		st, err = c.boundedCheck(ctx, a.ID, cand, p)
	}
	if ctx.Err() != nil {
		// Stopped as it asked, as a coordinator that shuts down is.
		return 0, ctx.Err()
	}
	if err == nil && st.State == backend.Dead {
		return 0, c.lose(ctx, a, st, st.Problem)
	}
	if giveUp := lost.Add(c.endWait(cand.driver)); now.Before(giveUp) {
		return c.waitFor(min(c.timing.Heartbeat, giveUp.Sub(now))), nil
	}
	if a.Private {
		c.log.InfoContext(ctx, "coordinator: a self-fencing attempt's backend didn't say how its worker ended, which is lost", "attempt", a.ID)
	} else {
		c.log.InfoContext(ctx, "coordinator: a self-fencing attempt's backend didn't say how its worker ended, which is lost", "attempt", a.ID, "error", err)
	}
	why, _ := errors.AsType[*problem.Problem](err)
	return 0, c.lose(ctx, a, backend.Status{State: backend.Dead}, why)
}

// endWait is how long past its lease and the fence margin a self-fencing
// attempt whose worker's driver is d is asked how its worker ended: the
// time d says a check can take to see it (backend.EndWaiter), and a
// check's own bound and a heartbeat period more, since that time counts
// from a check, which can be the second; or none, for a driver that
// doesn't say.
func (c *Coordinator) endWait(d backend.Driver) time.Duration {
	w, ok := d.(backend.EndWaiter)
	if !ok || w.EndWait() == 0 {
		return 0
	}
	return w.EndWait() + c.fenceCheck + c.timing.Heartbeat
}

// A fenceCheck is a check of a self-fencing attempt's backend in flight
// (boundedCheck): done is closed once it has answered, st and err.
type fenceCheck struct {
	done chan struct{}
	st   backend.Status
	err  error
}

// boundedCheck asks cand's driver what has become of the placement p of
// the attempt id, for at most fenceCheck, whether or not the driver
// honours its context, as one that reads a network filesystem may not. A
// check that hasn't answered by then gives Unknown, with the context's
// error; it goes on, the attempt's one check in flight, whose answer the
// next look takes, rather than start another against a driver that is
// stuck. It keeps maxMembers checks at most, as placedMember does members.
func (c *Coordinator) boundedCheck(ctx context.Context, id string, cand *candidate, p backend.Placement) (backend.Status, error) {
	c.mu.Lock()
	fc := c.fenceChecks[id]
	if fc == nil {
		if len(c.fenceChecks) >= maxMembers {
			clear(c.fenceChecks)
		}
		fc = &fenceCheck{done: make(chan struct{})}
		c.fenceChecks[id] = fc
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.fenceCheck)
		go func() {
			defer cancel()
			fc.st, fc.err = cand.driver.Check(checkCtx, cand.member, p)
			close(fc.done)
		}()
	}
	c.mu.Unlock()
	timer := time.NewTimer(c.fenceCheck)
	defer timer.Stop()
	select {
	case <-fc.done:
		c.mu.Lock()
		if c.fenceChecks[id] == fc {
			delete(c.fenceChecks, id)
		}
		c.mu.Unlock()
		return fc.st, fc.err
	case <-timer.C:
		return backend.Status{State: backend.Unknown}, context.DeadlineExceeded
	case <-ctx.Done():
		return backend.Status{State: backend.Unknown}, ctx.Err()
	}
}

// dropCheck lets go of the attempt id's check in flight (boundedCheck), as
// of an attempt that has ended, whose answer nothing will take.
func (c *Coordinator) dropCheck(id string) {
	c.mu.Lock()
	delete(c.fenceChecks, id)
	c.mu.Unlock()
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
	return 0, c.lose(ctx, a, st, st.Problem)
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
// answer, with a candidate that has only the driver of the place's
// backend. What it finds, but the last, it keeps for a while (memberFor).
func (c *Coordinator) placedMember(ctx context.Context, run *store.Run, job *store.Job, a *store.Attempt) (*candidate, backend.Placement, error) {
	c.mu.Lock()
	e, ok := c.members[a.ID]
	c.mu.Unlock()
	if ok && time.Now().Before(e.until) {
		return e.cand, e.placement, e.err
	}
	cand, at, _, err := c.placedOn(ctx, run, job, a)
	if groups, ok := errors.AsType[*groupsError](err); ok {
		d := c.driver(&backend.Member{Backend: at.Backend})
		return &candidate{driver: d}, backend.Placement{}, fmt.Errorf("%w: %w", errUncheckable, groups.err)
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
// last word on it, says. Before a step has started, whatever ended the
// worker, it is a failure before the job's first step, which places the
// job again: none of the job's code ran, though its scheduler, or the
// kernel, ended it at its time limit or for its memory, as one that
// waited for the coordinator until then; its problem is why, or for nil,
// worker_killed_early for such an end, and worker_ended_early for any
// other. Once a step has started, it is lost with the reason timeout or
// out-of-memory when its scheduler, or the kernel, ended it so, and
// otherwise with worker-lost, with why, its problem, if it has one. The
// attempt must still be as a was, not claimed nor renewed meanwhile, as a
// worker that reached the coordinator again would have it, or nothing is
// ended. Either way, the supervisor looks again, and the attempt's check
// in flight, if any, is let go (dropCheck).
func (c *Coordinator) lose(ctx context.Context, a *store.Attempt, st backend.Status, why *problem.Problem) error {
	c.dropCheck(a.ID)
	reason, killed := killedFor(st.End)
	end := store.AttemptEnd{Kind: store.EndLost, Reason: plan.ReasonWorkerLost, Problem: why}
	switch {
	case a.State != store.AttemptRunning:
		switch {
		case why != nil:
		case killed:
			why = problems.WorkerKilledEarly.Problem(problems.Params{"job": a.Job, "reason": string(reason)})
		default:
			why = problems.WorkerEndedEarly.Problem(problems.Params{"job": a.Job})
		}
		end = store.AttemptEnd{Kind: store.EndSetupFailed, Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: why}
	case killed:
		end.Reason = reason
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

// killedFor returns the reason of an attempt whose worker its scheduler,
// or the kernel, ended as end says, and whether it did: timeout at its
// time limit, and out-of-memory for its memory.
func killedFor(end backend.End) (plan.Reason, bool) {
	switch end {
	case backend.EndTimedOut:
		return plan.ReasonTimeout, true
	case backend.EndOutOfMemory:
		return plan.ReasonOutOfMemory, true
	}
	return "", false
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
	case retry == nil || run.StepRun || !slices.Contains(retry.Reasons, plan.ReasonWorkerLost):
		// A step run is one try of a step, which its caller's step's
		// retry: tries again, not its job's.
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
