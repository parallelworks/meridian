package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// An attempt is the attempt a worker claimed, as it runs.
type attempt struct {
	claim   *workerapi.Claim
	client  *client
	ph      phases
	tm      timing
	log     *slog.Logger
	dir     string
	environ []string
	worker  workerapi.Worker
	events  *eventQueue
	spool   *spool
	// claimed is when the worker sent the claim the coordinator answered,
	// from which the job's timeout, and its lease, are counted.
	claimed time.Time

	// job is the job as the claim gives it, mask its masker, and masked
	// the values it masks.
	job    *plan.ReadyJob
	mask   masker
	masked []string
	// jobOutputs is the $OUTPUTS file of the job's steps that have no id,
	// which they share, as in format 1.
	jobOutputs string
	// scripts counts the scripts written, to name each.
	scripts int
	// firstStarted is set once the coordinator has stored that a step is
	// starting.
	firstStarted bool

	// halted is fired when the job must stop at once, and haltErr says
	// why: the attempt ended without the worker, or the worker's context
	// is done. cancelJob ends the context the job's phases are evaluated
	// in, and cancelSend what the worker sends with.
	halted     *signal
	haltErr    error
	cancelJob  context.CancelFunc
	cancelSend context.CancelFunc
	// cancelled is fired when the coordinator cancels the job, with its
	// reason and grace period.
	cancelled    *signal
	cancelReason plan.Reason
	cancelGrace  time.Duration
	// anyJobFailed is fired when a job of the run has failed: this one, or
	// another, as the coordinator says.
	anyJobFailed *signal
	// timedOut is fired when the job has run past its timeout:.
	timedOut *signal

	// leaseMu guards renewed, when the worker sent the last heartbeat that
	// succeeded, and renewal, closed and replaced when it changes.
	leaseMu sync.Mutex
	renewed time.Time
	renewal chan struct{}
}

// A signal is fired once, when what it stands for happens.
type signal struct {
	once sync.Once
	c    chan struct{}
}

func newSignal() *signal { return &signal{c: make(chan struct{})} }

// fire fires s, after set, if it hasn't been fired.
func (s *signal) fire(set func()) {
	s.once.Do(func() {
		if set != nil {
			set()
		}
		close(s.c)
	})
}

func (s *signal) done() <-chan struct{} { return s.c }

func (s *signal) fired() bool {
	select {
	case <-s.c:
		return true
	default:
		return false
	}
}

// errLeaseLapsed is why a job that fences itself stops: the worker hasn't
// renewed its lease for a lease period.
var errLeaseLapsed = errors.New("worker: the attempt's lease lapsed before the worker could renew it, so it stopped the job, which may run again")

// run runs the attempt: its job, and what it sends of it, until the
// coordinator has stored its end, or the attempt ends without the worker,
// or ctx is done.
func (a *attempt) run(ctx context.Context) error {
	a.halted, a.cancelled, a.anyJobFailed, a.timedOut = newSignal(), newSignal(), newSignal(), newSignal()
	a.renewed, a.renewal = a.claimed, make(chan struct{})
	// What the worker sends goes on while its job runs and after: until
	// the coordinator has the attempt's end, or the attempt has ended
	// without the worker. When ctx is done, it goes on a little longer, to
	// send what it has.
	send, cancelSend := context.WithCancel(context.WithoutCancel(ctx))
	jobCtx, cancelJob := context.WithCancel(send)
	a.cancelSend, a.cancelJob = cancelSend, cancelJob
	var wg sync.WaitGroup
	defer func() {
		cancelSend()
		wg.Wait()
		a.spool.close()
	}()
	defer context.AfterFunc(ctx, func() { a.haltJob(ctx.Err()) })()

	wg.Go(func() { a.events.send(send, a.client, a.halt) })
	wg.Go(func() { a.spool.send(send, a.client, a.halt) })
	wg.Go(func() { a.heartbeat(send) })
	wg.Go(func() { a.control(send) })
	if a.claim.SelfFencing {
		wg.Go(func() { a.fence(send) })
	}

	if end := a.execute(jobCtx); end != nil {
		if err := a.deliver(send, end); err == nil || !a.halted.fired() {
			return err
		}
	}
	if ctx.Err() == nil || !errors.Is(a.haltErr, ctx.Err()) {
		return a.haltErr
	}
	// Stopped from outside, as by its scheduler: send what it has,
	// briefly, but not the attempt's end. The coordinator decides how it
	// ended.
	timer := time.AfterFunc(a.tm.flush, cancelSend)
	defer timer.Stop()
	if a.events.wait(a.events.lastSeq(), send.Done()) == nil {
		_ = a.spool.wait(send.Done())
	}
	return fmt.Errorf("worker: stopped before the attempt ended: %w", ctx.Err())
}

// halt stops the job at once, its steps as a cancellation does, and stops
// sending: the attempt has ended without the worker, as err says, and
// nothing it sends is stored.
func (a *attempt) halt(err error) {
	a.haltJob(err)
	a.cancelSend()
}

// haltJob stops the job at once, its steps as a cancellation does, without
// its cleanup: commands, for the reason err gives.
func (a *attempt) haltJob(err error) {
	a.halted.fire(func() { a.haltErr = err })
	a.cancelJob()
}

// deliver sends end, the attempt's end, once the coordinator has stored
// every event and log.
func (a *attempt) deliver(ctx context.Context, end *workerapi.Complete) error {
	end.Seq = a.events.lastSeq()
	if a.events.wait(end.Seq, a.halted.done()) != nil || a.spool.wait(a.halted.done()) != nil {
		return a.haltErr
	}
	if err := a.client.complete(ctx, end); err != nil {
		if a.halted.fired() {
			return a.haltErr
		}
		return err
	}
	a.log.Info("the attempt ended", "outcome", end.Outcome, "reason", end.Reason)
	return nil
}

// heartbeat renews the attempt's lease every heartbeat period, and stops
// the job if the coordinator says that the attempt has ended without the
// worker.
func (a *attempt) heartbeat(ctx context.Context) {
	period := seconds(max(a.claim.HeartbeatSeconds, 1))
	wait, failures := period, 0
	for {
		if sleep(ctx, wait) != nil {
			return
		}
		sent := time.Now()
		err := a.client.heartbeat(ctx, period)
		switch {
		case err == nil:
			a.renew(sent)
			wait, failures = period-time.Since(sent), 0
		case errors.Is(err, errFenced), errors.Is(err, errUnauthorized):
			a.halt(err)
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

// fence stops the job of an attempt that fences itself once the worker
// hasn't renewed its lease for a lease period, counted on its monotonic
// clock from when it sent the last heartbeat that succeeded, so that a new
// attempt can't run beside it.
func (a *attempt) fence(ctx context.Context) {
	lease := seconds(a.claim.LeaseSeconds)
	for {
		a.leaseMu.Lock()
		renewed, renewal := a.renewed, a.renewal
		a.leaseMu.Unlock()
		left := lease - time.Since(renewed)
		if left <= 0 {
			a.log.Warn("stopping the job: its lease lapsed", "lease", lease)
			a.halt(errLeaseLapsed)
			return
		}
		t := time.NewTimer(left)
		select {
		case <-t.C:
		case <-renewal:
		case <-ctx.Done():
		}
		t.Stop()
		if ctx.Err() != nil {
			return
		}
	}
}

// control waits for what the coordinator asks of the worker: to cancel the
// job, or that another job of the run failed.
func (a *attempt) control(ctx context.Context) {
	var version int64
	failures := 0
	for {
		ctl, err := a.client.control(ctx, version)
		switch {
		case err == nil:
			failures = 0
		case errors.Is(err, errFenced), errors.Is(err, errUnauthorized):
			a.halt(err)
			return
		case ctx.Err() != nil:
			return
		default:
			a.log.Debug("polling for control failed", "error", err)
			if sleep(ctx, a.tm.backoff(failures)) != nil {
				return
			}
			failures++
			continue
		}
		if ctl.Version <= version {
			continue
		}
		version = ctl.Version
		if ctl.Cancel {
			a.cancelled.fire(func() {
				a.cancelReason, a.cancelGrace = ctl.Reason, seconds(ctl.GraceSeconds)
				a.log.Info("the coordinator cancelled the job", "reason", ctl.Reason)
			})
		}
		if ctl.AnyJobFailed {
			a.anyJobFailed.fire(nil)
		}
	}
}

func seconds(s int64) time.Duration { return time.Duration(s) * time.Second }
