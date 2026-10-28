package worker

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os/exec"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// An attempt is the attempt a worker claimed, as it runs.
type attempt struct {
	claim   *workerapi.Claim
	client  *client
	tm      timing
	log     *slog.Logger
	files   *privateDir
	environ []string
	worker  workerapi.Worker
	events  *eventQueue
	spool   *spool
	// claimed is when the worker sent the claim the coordinator answered,
	// from which the job's timeout, and its lease, are counted.
	claimed time.Time

	// job is the job as the claim gives it, and jobEnv its env: as the
	// JobEnv phase gave it, for the attempt. mask masks the job's logs:
	// masked, the values the run and the phases made, and secrets, the
	// platform's secret variables (plan.JobState.Secrets).
	job     *plan.ReadyJob
	jobEnv  *plan.JobEnv
	mask    *plan.Masker
	masked  []string
	secrets []string
	// scripts counts the scripts written, to name each.
	scripts int
	// stepEnded, if it is set, is called as each step, or cleanup, has
	// ended (Config.stepEnded).
	stepEnded func(a *attempt, step int, cleanup bool)
	// firstStarted is set once the coordinator has stored that a step is
	// starting.
	firstStarted bool
	// cutReports is set once the worker has sent an event without what it
	// couldn't encode (reportNotEncoded), which fails the job.
	cutReports atomic.Bool
	// sentEnd is set once the worker is sending the attempt's end, which
	// may end the attempt before the worker has the answer (endedWithout).
	sentEnd atomic.Bool

	// stopMu guards what stops the job, and stopChanged, closed and
	// replaced whenever that changes.
	stopMu      sync.Mutex
	stopChanged chan struct{}
	// halted is fired when the job must stop at once, without its
	// cleanup: commands, and haltErr says why: the attempt ended without
	// the worker, or the worker is stopping. Every process group the
	// attempt started is sent SIGKILL at haltKill, and the worker waits
	// haltAfterKill for their output; reaped is closed once none is left.
	halted        *signal
	haltErr       error
	haltKill      time.Time
	haltAfterKill time.Duration
	reaped        chan struct{}
	// cancelled is fired when the coordinator cancels the job, with its
	// reason; a step it stops is sent SIGKILL at cancelKill.
	cancelled    *signal
	cancelReason plan.Reason
	cancelKill   time.Time
	// anyJobFailed is fired when a job of the run has failed: this one, or
	// another, as the coordinator says.
	anyJobFailed *signal
	// firstControl is closed once the worker has read its first control,
	// and done as it asks, or failed to read it (awaitFirstControl).
	firstControl chan struct{}
	// timedOut is fired when the job has run past its timeout:, or its
	// run past the run's, as the coordinator says; a step it stops is sent
	// SIGKILL at timeoutKill.
	timedOut    *signal
	timeoutKill time.Time
	// cancelJob ends the context the job's phases are evaluated in, and
	// cancelSend the one the worker sends with.
	cancelJob  context.CancelFunc
	cancelSend context.CancelFunc
	// stopBeating stops the heartbeats, once a job that fences itself has
	// stopped: renewing the lease would keep a new attempt from starting.
	stopBeating context.CancelFunc

	// groupsMu guards groups, the process groups the attempt started that
	// may have a process left, by number, with the leader's command, once
	// its step has ended, where the worker holds it unreaped
	// (holdsLeaders); and termed, those of them that were sent SIGTERM
	// (terminate).
	groupsMu sync.Mutex
	groups   map[int]*exec.Cmd
	termed   map[int]bool

	// leaseMu guards renewed, when the worker sent the last heartbeat that
	// succeeded, and renewal, closed and replaced when it changes.
	leaseMu sync.Mutex
	renewed time.Time
	renewal chan struct{}
}

// errLeaseLapsed is why a job that fences itself stops: the worker hasn't
// renewed its lease for long enough that it must stop before it lapses.
var errLeaseLapsed error = problems.AttemptLeaseLapsed.Problem(nil)

// run runs the attempt: its job, and what it sends of it, until the
// coordinator has stored its end, or the attempt ends without the worker,
// or ctx is done.
func (a *attempt) run(ctx context.Context) error {
	a.stopChanged = make(chan struct{})
	a.halted, a.cancelled, a.anyJobFailed, a.timedOut = newSignal(), newSignal(), newSignal(), newSignal()
	a.reaped, a.firstControl = make(chan struct{}), make(chan struct{})
	if c := a.claim.Control; c != nil {
		// A claim of version 2 or later holds the attempt's control as it
		// was then: the worker does as it asks before anything of the job
		// runs, and its polls go on from it (control).
		a.obey(c)
		close(a.firstControl)
	}
	a.groups, a.termed = map[int]*exec.Cmd{}, map[int]bool{}
	a.renewed, a.renewal = a.claimed, make(chan struct{})
	// What the worker sends goes on while its job runs and after: until
	// the coordinator has the attempt's end; or the attempt has ended
	// without the worker, or, for a job that fences itself, the lease has
	// lapsed; or, once ctx is done, for the flush period after the job's
	// steps have stopped, so that a step in its grace period keeps what it
	// prints last.
	send, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	jobCtx, cancelJob := context.WithCancel(send)
	beat, stopBeating := context.WithCancel(send)
	a.cancelSend, a.cancelJob, a.stopBeating = cancelSend, cancelJob, stopBeating
	stepsStopped := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		cancelSend()
		wg.Wait()
		if a.halted.fired() {
			<-a.reaped
		}
		a.reapLeaders()
		a.spool.close()
	}()
	defer context.AfterFunc(ctx, func() {
		a.stopJob(ctx.Err(), time.Now().Add(a.tm.grace), a.tm.afterKill)
		go func() {
			<-stepsStopped
			time.AfterFunc(a.tm.flush, cancelSend)
		}()
	})()

	wg.Go(func() { a.events.send(send, a.client, a.halt, a.reportNotEncoded) })
	wg.Go(func() { a.spool.send(send, a.client, a.halt) })
	wg.Go(func() { a.heartbeat(beat) })
	wg.Go(func() { a.control(send) })
	if a.claim.SelfFencing {
		wg.Go(func() { a.fence(send) })
	}

	end := a.execute(jobCtx)
	close(stepsStopped)
	if end != nil {
		err := a.deliver(send, end)
		if errors.Is(err, errFenced) || errors.Is(err, errUnauthorized) {
			// The attempt ended without this end: the worker stops what
			// its steps left running, as for a heartbeat that fails so.
			a.halt(err)
		}
		if err == nil || !a.halted.fired() {
			return err
		}
	}
	stopped := ctx.Err() != nil && errors.Is(a.haltErr, ctx.Err())
	if !stopped && !errors.Is(a.haltErr, errLeaseLapsed) {
		return a.haltErr
	}
	// Stopped from outside, as by its scheduler, before its job ended, or
	// before its lease lapsed: send what it has, while what it sends can
	// be stored, but not the attempt's end. The coordinator decides how
	// it ended.
	if a.events.wait(a.events.lastSeq(), send.Done()) == nil {
		_ = a.spool.wait(send.Done())
	}
	if !stopped {
		return a.haltErr
	}
	return ctx.Err()
}

// halt stops the job at once, as stopJob does, and stops sending: the
// attempt has ended without the worker, as err says, and nothing more it
// sends is stored. Its steps have the worker's grace period.
func (a *attempt) halt(err error) {
	a.stopJob(err, time.Now().Add(a.tm.grace), a.tm.afterKill)
	a.cancelSend()
}

// stopJob stops the job at once, for the reason err gives, without its
// cleanup: commands: every process group the attempt started is sent
// SIGTERM, and SIGKILL at kill, after which the worker waits afterKill for
// a step's output. A later stop can make SIGKILL sooner.
func (a *attempt) stopJob(err error, kill time.Time, afterKill time.Duration) {
	a.stopMu.Lock()
	switch {
	case !a.halted.fired():
		a.haltErr, a.haltKill, a.haltAfterKill = err, kill, afterKill
		a.fireLocked(a.halted)
		go a.reap()
	case kill.Before(a.haltKill):
		a.haltKill, a.haltAfterKill = kill, min(afterKill, a.haltAfterKill)
		a.stopsChanged()
	}
	a.stopMu.Unlock()
	a.cancelJob()
}

// fireLocked fires sig, which hasn't been fired. Its caller holds stopMu.
func (a *attempt) fireLocked(sig *signal) {
	sig.at = time.Now()
	close(sig.c)
	a.stopsChanged()
}

// cancel records the coordinator's cancellation of the job: a step it
// stops has its grace period, or the worker's for 0. A later one that
// gives less time makes SIGKILL sooner. One whose reason is timeout, the
// run's, isn't a cancellation: as in format 1, the run's deadline stops
// the job as the job's own timeout does (timedOut), so that the step it
// stops fails, and the job with it.
func (a *attempt) cancel(ctl *workerapi.Control) {
	grace := seconds(ctl.GraceSeconds)
	if grace <= 0 {
		grace = a.tm.grace
	}
	kill := time.Now().Add(grace)
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	if ctl.Reason == plan.ReasonTimeout {
		a.timeOutLocked(kill, "the run ran past its timeout")
		return
	}
	switch {
	case !a.cancelled.fired():
		a.cancelReason, a.cancelKill = ctl.Reason, kill
		a.fireLocked(a.cancelled)
		a.log.Info("the coordinator cancelled the job", "reason", ctl.Reason)
	case kill.Before(a.cancelKill):
		a.cancelKill = kill
		a.stopsChanged()
	}
}

// timeOut records that the job is past a deadline, its own timeout: or
// its run's, which says, for the worker's log: a step it stops is sent
// SIGKILL at kill, or at an earlier time an earlier deadline gave. A
// later deadline that gives less time makes SIGKILL sooner.
func (a *attempt) timeOut(kill time.Time, says string) {
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	a.timeOutLocked(kill, says)
}

// timeOutLocked is timeOut. Its caller holds stopMu.
func (a *attempt) timeOutLocked(kill time.Time, says string) {
	switch {
	case !a.timedOut.fired():
		a.timeoutKill = kill
		a.fireLocked(a.timedOut)
		a.log.Info(says)
	case kill.Before(a.timeoutKill):
		a.timeoutKill = kill
		a.stopsChanged()
	}
}

// terminate sends group, a process group the attempt started, SIGTERM,
// unless it was sent it already. A halt stops a step's group twice, as its
// step's stop and as the reap of every group the attempt started, and
// the step has its grace period from the first SIGTERM, as in format 1: a
// second would stop what its trap of SIGTERM runs, as a sleep in it, and
// cut its last words short.
func (a *attempt) terminate(group int) {
	a.groupsMu.Lock()
	sent := a.termed[group]
	a.termed[group] = true
	a.groupsMu.Unlock()
	if !sent {
		signalGroup(group, syscall.SIGTERM)
	}
}

// startedGroup records a process group the attempt started.
func (a *attempt) startedGroup(group int) {
	a.groupsMu.Lock()
	defer a.groupsMu.Unlock()
	a.groups[group] = nil
	// A new leader may reuse the number of a group signalled before,
	// where the worker doesn't hold its leaders: it hasn't been signalled.
	delete(a.termed, group)
}

// endedGroup records that the leader of a process group the attempt
// started has exited, unreaped where the worker holds it: cmd reaps it.
func (a *attempt) endedGroup(cmd *exec.Cmd) {
	if cmd.Process == nil || !holdsLeaders {
		return
	}
	a.groupsMu.Lock()
	defer a.groupsMu.Unlock()
	if _, ok := a.groups[cmd.Process.Pid]; ok {
		a.groups[cmd.Process.Pid] = cmd
	}
}

// liveGroups returns the process groups the attempt started that have a
// process left, and forgets the rest, reaping their leaders, so that it
// never signals a group whose number is given to another.
func (a *attempt) liveGroups() []int {
	a.groupsMu.Lock()
	defer a.groupsMu.Unlock()
	groups := slices.Collect(maps.Keys(a.groups))
	alive := liveGroupsOf(groups)
	var live []int
	for _, group := range groups {
		switch cmd := a.groups[group]; {
		case alive[group]:
			live = append(live, group)
		case cmd != nil:
			_ = cmd.Wait()
			delete(a.groups, group)
			delete(a.termed, group)
		case !holdsLeaders:
			delete(a.groups, group)
			delete(a.termed, group)
		}
	}
	return live
}

// reapLeaders reaps the leaders of the process groups the attempt started
// that the worker holds, at its end.
func (a *attempt) reapLeaders() {
	a.groupsMu.Lock()
	defer a.groupsMu.Unlock()
	for group, cmd := range a.groups {
		if cmd != nil {
			_ = cmd.Wait()
		}
		delete(a.groups, group)
		delete(a.termed, group)
	}
}

// reap stops every process group the attempt started, once its job is
// halted, those of the steps that have ended too, as what they left
// running in the background: it sends them SIGTERM, then SIGKILL when the
// halt says. It closes reaped once none has a process left, or it has
// sent them SIGKILL.
func (a *attempt) reap() {
	defer close(a.reaped)
	for _, group := range a.liveGroups() {
		a.terminate(group)
	}
	for {
		changed := a.changes()
		a.stopMu.Lock()
		kill := a.haltKill
		a.stopMu.Unlock()
		live := a.liveGroups()
		if len(live) == 0 {
			return
		}
		if !time.Now().Before(kill) {
			for _, group := range live {
				signalGroup(group, syscall.SIGKILL)
			}
			return
		}
		t := time.NewTimer(min(time.Until(kill), 200*time.Millisecond))
		select {
		case <-t.C:
		case <-changed:
		}
		t.Stop()
	}
}

// deliver sends end, the attempt's end, once the coordinator has stored
// every event and log, until ctx is done. An end the worker can't encode it
// sends without its outputs and its problems, as a failure (failEnd), and
// then, if it still can't, with only the code of the problem that failed
// the job before its first step, which placement compares, and its title.
func (a *attempt) deliver(ctx context.Context, end *workerapi.Complete) error {
	end.Seq = a.events.lastSeq()
	if a.events.wait(end.Seq, ctx.Done()) != nil || a.spool.wait(ctx.Done()) != nil {
		return errStopped
	}
	if a.cutReports.Load() {
		a.failEnd(end)
	}
	a.sentEnd.Store(true)
	err := a.client.complete(ctx, end)
	if enc, ok := errors.AsType[*encodeError](err); ok {
		a.log.Error("the attempt's end couldn't be encoded", "error", a.mask.Mask(enc.Error()))
		*end = workerapi.Complete{Seq: end.Seq, Result: plan.JobResult{Outcome: end.Result.Outcome}, Reason: end.Reason, Problem: end.Problem}
		a.failEnd(end)
		err = a.client.complete(ctx, end)
		if _, ok := errors.AsType[*encodeError](err); ok && end.Problem != nil {
			p := end.Problem
			end.Problem = &problem.Problem{Type: p.Type, Title: p.Title, Status: p.Status, Detail: p.Title, Code: p.Code}
			err = a.client.complete(ctx, end)
		}
	}
	if order, ok := errors.AsType[*outOfOrderError](err); ok {
		// The coordinator said it had stored every event up to end.Seq
		// before the worker sent it, so it has lost some since.
		return problems.CoordinatorLostData.Problem(problems.Params{"data": "events", "from": order.expected})
	}
	if err != nil {
		return err
	}
	a.log.Info("the attempt ended", "outcome", end.Result.Outcome, "reason", end.Reason)
	return nil
}

// reportNotEncoded returns e, an event the worker couldn't encode, as err
// says, as one it can: without its outputs and its problems, and, if it
// finished, with one that says so. That is the worker's own bug, which
// fails the job (failEnd), rather than stop the attempt, which the
// coordinator would take for a lost worker, and run again.
func (a *attempt) reportNotEncoded(e workerapi.Event, err error) workerapi.Event {
	a.log.Error("a step's event couldn't be encoded", "step", e.Step, "try", e.Try, "cleanup", e.Cleanup, "error", a.mask.Mask(err.Error()))
	a.cutReports.Store(true)
	e.Outputs, e.Diagnostics = nil, nil
	if e.Finished {
		e.Diagnostics = problems.Diagnostics{a.at(problems.ReportNotEncoded, a.job.Steps[e.Step].Path, nil)}
	}
	return e
}

// failEnd fails end, the attempt's end, for a report the worker couldn't
// encode, unless the job was cancelled or failed already, and says so in
// its problems.
func (a *attempt) failEnd(end *workerapi.Complete) {
	if end.Result.Outcome == plan.OutcomeSuccess || end.Result.Outcome == plan.OutcomeSkipped {
		end.Result.Outcome = plan.OutcomeFailure
	}
	path := ""
	if a.job != nil {
		path = a.job.Path
	}
	end.Diagnostics = append(end.Diagnostics, a.at(problems.ReportNotEncoded, path, nil))
}

// heartbeat renews the attempt's lease every heartbeat period, and stops
// the job if the coordinator says that the attempt has ended without the
// worker. For a job that fences itself, it renews it often enough to try
// three times before the worker must start stopping the job, more often
// than the coordinator asks if need be, and gives up on a heartbeat that
// hasn't been answered in a third of what is left then, to send another
// at once, as a connection that is half open would hold it.
func (a *attempt) heartbeat(ctx context.Context) {
	period := seconds(max(a.claim.HeartbeatSeconds, 1))
	timeout := period
	if a.claim.SelfFencing {
		stopAfter := a.leaseTimes().stopAfter
		if most := max(stopAfter/3, 100*time.Millisecond); period > most {
			a.log.Warn("heartbeating more often than the coordinator asks, to renew the lease before the job must stop",
				"asked", period, "every", most)
			period = most
		}
		timeout = max(min(period, (stopAfter-period)/3), 100*time.Millisecond)
	}
	wait, failures := period, 0
	for {
		if sleep(ctx, wait) != nil {
			return
		}
		sent := time.Now()
		err := a.client.heartbeat(ctx, timeout)
		switch {
		case err == nil:
			a.renew(sent)
			wait, failures = period-time.Since(sent), 0
		case errors.Is(err, errFenced), errors.Is(err, errUnauthorized):
			a.endedWithout(err)
			return
		case ctx.Err() != nil:
			return
		default:
			a.log.Debug("a heartbeat failed", "error", err)
			wait = min(a.tm.backoff(failures), period)
			failures++
		}
	}
}

// endedWithout is what a heartbeat or a control poll that fails with err,
// Fenced or 401, does: the attempt has ended without the worker, which
// halts. Once the worker is sending the attempt's end, though, that end may
// be what ended it, stored though its answer was lost: the worker stops
// heartbeating, or polling, and the answer to the end, which it repeats
// until it has one, says whether the attempt ended without it.
func (a *attempt) endedWithout(err error) {
	if a.sentEnd.Load() {
		a.log.Debug("the attempt has ended, perhaps with the end the worker sent", "error", err)
		return
	}
	a.halt(err)
}

// renew records that a heartbeat the worker sent at sent succeeded.
func (a *attempt) renew(sent time.Time) {
	a.leaseMu.Lock()
	defer a.leaseMu.Unlock()
	if sent.After(a.renewed) {
		a.renewed = sent
		close(a.renewal)
		a.renewal = make(chan struct{})
	}
}

// sinceRenewed returns how long it has been since the worker sent the last
// heartbeat that succeeded, on whichever of its monotonic and wall clocks
// has gone further, and a channel closed when it sends another: the
// monotonic clock stops while the host is suspended, and a wall clock
// that jumps ahead can only make the worker stop sooner.
func (a *attempt) sinceRenewed() (time.Duration, <-chan struct{}) {
	a.leaseMu.Lock()
	renewed, renewal := a.renewed, a.renewal
	a.leaseMu.Unlock()
	monotonic := time.Since(renewed)
	wall := time.Now().Round(0).Sub(renewed.Round(0))
	return max(monotonic, wall), renewal
}

// leaseTimes are when a job that fences itself stops, counted from the
// last renewal of its lease: from stopAfter, the worker stops it, with
// grace between SIGTERM and SIGKILL; by killAfter, it has sent every
// process group SIGKILL, and waits afterKill for their output, which ends
// a margin before the lease lapses (workerapi.Claim.SelfFencing).
type leaseTimes struct {
	lease, stopAfter, killAfter, afterKill, grace time.Duration
}

func (a *attempt) leaseTimes() leaseTimes {
	lease := seconds(a.claim.LeaseSeconds)
	margin := lease / 10
	t := leaseTimes{lease: lease, afterKill: min(a.tm.afterKill, lease/10), grace: min(a.tm.grace, lease/5)}
	t.stopAfter = lease - t.grace - t.afterKill - margin
	t.killAfter = lease - t.afterKill - margin
	return t
}

// fence stops the job of an attempt that fences itself, so that a new
// attempt can't run beside it, as a Kubernetes controller that can't
// renew its lease stops leading before it lapses: once so long has passed
// since the last heartbeat that succeeded that its processes have only
// their grace period, the time the worker waits for their output, and a
// margin left before the lease lapses, it stops them all, so that they
// have been sent SIGKILL, and the worker has stopped waiting for them, a
// tenth of the lease before it lapses. It stops renewing the lease, and
// goes on sending what it has until the lease lapses.
func (a *attempt) fence(ctx context.Context) {
	t := a.leaseTimes()
	// The monotonic clock a timer runs on stops while the host is
	// suspended, so the wall clock is read every so often too: a step can
	// run for up to that long once a host that was suspended past the
	// lease resumes.
	poll := min(max(t.lease/20, 10*time.Millisecond), time.Second)
	for {
		since, renewal := a.sinceRenewed()
		if since >= t.stopAfter {
			a.log.Warn("stopping the job before its lease lapses", "lease", t.lease, "since", since)
			a.stopBeating()
			a.stopJob(errLeaseLapsed, time.Now().Add(t.killAfter-since), t.afterKill)
			time.AfterFunc(t.lease-since, a.cancelSend)
			return
		}
		timer := time.NewTimer(min(t.stopAfter-since, poll))
		select {
		case <-timer.C:
		case <-renewal:
		case <-ctx.Done():
		}
		timer.Stop()
		if ctx.Err() != nil {
			return
		}
	}
}

// control waits for what the coordinator asks of the worker: to cancel the
// job, perhaps again with less time, or that another job of the run
// failed. In an attempt of version 2 or later of the API, the claim held
// the first control, which run has done as it asks, and the polls go on
// from its version. In one of version 1, the first poll names no version,
// -1, which the coordinator answers at once with the control as it is, so
// that what it asked before the claim, as when the run was stopped before
// the attempt was placed, applies from the job's first step, which waits
// for it (awaitFirstControl).
func (a *attempt) control(ctx context.Context) {
	version, read := int64(-1), sync.OnceFunc(func() { close(a.firstControl) })
	if c := a.claim.Control; c != nil {
		version, read = c.Version, func() {}
	}
	defer read()
	failures, quick := 0, 0
	for {
		asked := time.Now()
		ctl, err := a.client.control(ctx, version)
		switch {
		case err == nil:
			failures = 0
		case errors.Is(err, errFenced), errors.Is(err, errUnauthorized):
			a.endedWithout(err)
			return
		case ctx.Err() != nil:
			return
		default:
			a.log.Debug("polling for control failed", "error", err)
			// A first poll that fails leaves the control to the polls
			// after it, which go on beside the job's steps.
			read()
			if sleep(ctx, a.tm.backoff(failures)) != nil {
				return
			}
			failures++
			continue
		}
		if ctl.Version <= version {
			read()
			// A coordinator holds a poll until it has something newer;
			// one that answers at once is asked again after a while,
			// rather than at once.
			if time.Since(asked) < time.Second {
				if sleep(ctx, min(a.tm.backoff(quick), workerapi.ControlWaitSeconds*time.Second/6)) != nil {
					return
				}
				quick++
			} else {
				quick = 0
			}
			continue
		}
		version, quick = ctl.Version, 0
		a.obey(ctl)
		read()
	}
}

// obey does as a control asks: cancels the job, or fires that a job of the
// run failed.
func (a *attempt) obey(ctl *workerapi.Control) {
	if ctl.Cancel {
		a.cancel(ctl)
	}
	if ctl.AnyJobFailed {
		a.fire(a.anyJobFailed, nil)
	}
}

// awaitFirstControl waits until the worker has read its first control,
// and done as it asks, or failed to read it, for tm.firstControl at most,
// or until the job is halted: so that a job whose run was stopped, or a
// job of whose run failed, before its attempt was claimed doesn't start
// its first step as though it hadn't been, as format 1's job starts with
// its context already ended. A coordinator of version 1 of the API that
// doesn't answer in that time leaves what it asked to the polls after,
// which go on beside the job's steps. In an attempt of version 2 or later,
// the claim held the first control, so nothing waits.
func (a *attempt) awaitFirstControl() {
	t := time.NewTimer(a.tm.firstControl)
	defer t.Stop()
	select {
	case <-a.firstControl:
	case <-a.halted.c:
	case <-t.C:
		a.log.Info("the coordinator hasn't answered the first control poll, so the job's steps start without it", "waited", a.tm.firstControl)
	}
}

func seconds(s int64) time.Duration { return time.Duration(s) * time.Second }
