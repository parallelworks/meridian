package worker

import "time"

// A stopCause is why a step's command was stopped before it ended by
// itself.
type stopCause int

const (
	noStop stopCause = iota
	// stopHalted is the attempt ending without the worker, or the worker
	// stopping.
	stopHalted
	// stopCancelled is the coordinator cancelling the job.
	stopCancelled
	// stopEarlyCancel is a job of the run failing, for a step whose
	// early-cancel: is any-job-failed.
	stopEarlyCancel
	// stopJobTimeout is the job running past its timeout:, or its run past
	// the run's.
	stopJobTimeout
	// stopStepTimeout is the step running past its timeout:, and
	// stopTryTimeout one of its tries past its retry:'s timeout.
	stopStepTimeout
	stopTryTimeout
)

// A stopRequest asks a running command to stop, for cause: to be sent
// SIGTERM now, if it hasn't been, and SIGKILL at kill, after which the
// worker waits afterKill for its output to close.
type stopRequest struct {
	cause     stopCause
	kill      time.Time
	afterKill time.Duration
}

// stops say what stops a step's try, besides the attempt ending without
// the worker, which stops everything.
type stops struct {
	// cancellable is set for a step that the job's cancellation and its
	// timeout stop: every step but one that outlives them
	// (plan.ReadyStep.OutlivesCancel).
	cancellable bool
	// earlyCancel is set for a step whose early-cancel: is
	// any-job-failed.
	earlyCancel bool
	// step and try are the step's timeout: and its retry:'s timeout, or 0
	// for none.
	step, try time.Duration
}

// A signal is fired once, when what it stands for happens, at at. The
// attempt fires it, under stopMu.
type signal struct {
	c  chan struct{}
	at time.Time
}

func newSignal() *signal { return &signal{c: make(chan struct{})} }

func (s *signal) fired() bool {
	select {
	case <-s.c:
		return true
	default:
		return false
	}
}

// fire fires sig, after set, if it hasn't been fired, and wakes whatever
// watches the job's stops. It reports whether it fired it.
func (a *attempt) fire(sig *signal, set func()) bool {
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	if sig.fired() {
		return false
	}
	if set != nil {
		set()
	}
	a.fireLocked(sig)
	return true
}

// stopsChanged wakes whatever watches the job's stops. Its caller holds
// stopMu.
func (a *attempt) stopsChanged() {
	close(a.stopChanged)
	a.stopChanged = make(chan struct{})
}

// changes returns a channel closed when the job's stops change.
func (a *attempt) changes() <-chan struct{} {
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	return a.stopChanged
}

// jobStop returns what of the job's stops that have fired asks a step's
// try watched as s to stop: the cause of the first to fire, and the
// soonest SIGKILL any asks for.
func (a *attempt) jobStop(s stops) stopWinner {
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	var w stopWinner
	if a.halted.fired() {
		w.consider(a.halted.at, stopRequest{stopHalted, a.haltKill, a.haltAfterKill})
	}
	if s.cancellable && a.cancelled.fired() {
		w.consider(a.cancelled.at, stopRequest{stopCancelled, a.cancelKill, a.tm.afterKill})
	}
	if s.cancellable && a.timedOut.fired() {
		w.consider(a.timedOut.at, stopRequest{stopJobTimeout, a.timeoutKill, a.tm.afterKill})
	}
	if s.earlyCancel && a.anyJobFailed.fired() {
		w.consider(a.anyJobFailed.at, stopRequest{stopEarlyCancel, a.anyJobFailed.at.Add(a.tm.grace), a.tm.afterKill})
	}
	return w
}

// stopCauseOf returns why the job's stops that have fired ask a step
// watched as s to stop, or noStop.
func (a *attempt) stopCauseOf(s stops) stopCause {
	return a.jobStop(s).req.cause
}

// A stopWinner gathers the stops that have fired: the first one's cause,
// and the soonest SIGKILL.
type stopWinner struct {
	req   stopRequest
	first time.Time
	ok    bool
}

func (w *stopWinner) consider(at time.Time, r stopRequest) {
	if !w.ok || at.Before(w.first) {
		w.first, w.req.cause = at, r.cause
	}
	if !w.ok || r.kill.Before(w.req.kill) {
		w.req.kill, w.req.afterKill = r.kill, r.afterKill
	}
	w.ok = true
}

// watch watches what stops a step's try, as s says: it sends what asks
// the try to stop on the channel it returns, and sends again whenever
// SIGKILL comes due sooner, until the function it returns is called. The
// channel holds only the latest request.
func (a *attempt) watch(s stops) (<-chan stopRequest, func()) {
	out := make(chan stopRequest, 1)
	done := make(chan struct{})
	go func() {
		stepTimer, tryTimer := after(s.step), after(s.try)
		defer stepTimer.Stop()
		defer tryTimer.Stop()
		var stepAt, tryAt time.Time
		var last stopRequest
		sent := false
		for {
			changed := a.changes()
			w := a.jobStop(s)
			if !stepAt.IsZero() {
				w.consider(stepAt, stopRequest{stopStepTimeout, stepAt.Add(a.tm.grace), a.tm.afterKill})
			}
			if !tryAt.IsZero() {
				w.consider(tryAt, stopRequest{stopTryTimeout, tryAt.Add(a.tm.grace), a.tm.afterKill})
			}
			if w.ok && (!sent || w.req.kill.Before(last.kill)) {
				select {
				case <-out:
				default:
				}
				out <- w.req
				last, sent = w.req, true
			}
			select {
			case <-changed:
			case <-stepTimer.C:
				stepAt = time.Now()
			case <-tryTimer.C:
				tryAt = time.Now()
			case <-done:
				return
			}
		}
	}()
	return out, func() { close(done) }
}

// after returns a timer that fires after d, or never for 0.
func after(d time.Duration) *time.Timer {
	if d <= 0 {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	}
	return time.NewTimer(d)
}

// await waits until ready says it is, and returns noStop, unless one of
// the job's stops that s watches fires first, and returns its cause. ready
// reports whether it is, and returns a channel closed when that may have
// changed.
func (a *attempt) await(s stops, ready func() (bool, <-chan struct{})) stopCause {
	for {
		changed := a.changes()
		if cause := a.stopCauseOf(s); cause != noStop {
			return cause
		}
		ok, readyChanged := ready()
		if ok {
			return noStop
		}
		select {
		case <-changed:
		case <-readyChanged:
		}
	}
}

// passed returns, for await, what is ready once d has passed, and a
// function that stops its timer.
func passed(d time.Duration) (func() (bool, <-chan struct{}), func()) {
	c := make(chan struct{})
	t := time.AfterFunc(d, func() { close(c) })
	return func() (bool, <-chan struct{}) {
		select {
		case <-c:
			return true, c
		default:
			return false, c
		}
	}, func() { t.Stop() }
}
