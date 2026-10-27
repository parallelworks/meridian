package coordinator

import (
	"fmt"
	"time"

	"github.com/parallelworks/meridian/internal/testtiming"
)

// Timing is how long an attempt's lease lasts, and the periods the
// coordinator waits around it (docs/design.md, Leases and fencing). A
// coordinator runs with DefaultTiming's, which nothing outside the
// coordinator's own tests changes: the worker's bounds are fractions of the
// lease, and these are what they were checked against.
//
// With a lease of 60 seconds, a worker whose job fences itself, which
// heartbeats every 10 seconds, has tried three times by 30 seconds after
// its last heartbeat that succeeded; it may start stopping the job from
// 36, and has stopped by 54 (workerapi.Claim.SelfFencing). The coordinator
// starts a new attempt of the job only at 75, by the store's clock, 21
// seconds after.
type Timing struct {
	// Lease is how long an attempt's lease lasts after its claim or a
	// heartbeat (workerapi.Claim.LeaseSeconds), by the store's clock.
	Lease time.Duration
	// Heartbeat is how often a worker sends a heartbeat
	// (workerapi.Claim.HeartbeatSeconds): at most a fifth of Lease, as a
	// job that fences itself needs, so that it tries three times before
	// it starts stopping the job.
	Heartbeat time.Duration
	// FenceMargin is how long the coordinator waits past the lease of an
	// attempt whose job fences itself before it takes the attempt to be
	// lost and starts another: long enough for the difference between the
	// rates of the worker's clock and the store's, for the worker to read
	// its wall clock, which it does once a second, after its host
	// resumes, and for a commit.
	FenceMargin time.Duration
	// Grace is how long the coordinator acts on no lapsed lease after it
	// takes the endpoint workers reach it through, or after its host slept:
	// long enough for every worker that was cut off to try to reach it at
	// least twice, since a worker waits 30 seconds at most between tries.
	Grace time.Duration
	// WakeJump is how far the wall clock must move past the monotonic
	// clock, which stops while the host sleeps, for the coordinator to
	// take it that its host slept.
	WakeJump time.Duration
	// CheckGrace is how long the coordinator takes the worker of an
	// attempt to run while its backend's check of it gives a problem, a
	// cause that won't pass by itself, as a host key known_hosts doesn't
	// have, before it loses the attempt with the problem: time for a
	// person to see it, and to fix it, as a host key can be, before the
	// job is placed again, or fails.
	CheckGrace time.Duration
	// WorkerStop is how long a worker takes to stop its job's steps once
	// told to: its grace between SIGTERM and SIGKILL, and the time it
	// waits for their output after SIGKILL. The coordinator waits that,
	// a heartbeat period and the fence margin past a job's timeout:
	// before it stops the job itself.
	WorkerStop time.Duration
}

// DefaultTiming returns the timing a coordinator runs with.
func DefaultTiming() Timing {
	return Timing{
		Lease:       60 * time.Second,
		Heartbeat:   10 * time.Second,
		FenceMargin: 15 * time.Second,
		Grace:       60 * time.Second,
		WakeJump:    10 * time.Second,
		CheckGrace:  5 * time.Minute,
		WorkerStop:  35 * time.Second,
	}
}

// openTiming is the timing a coordinator opens with: DefaultTiming's, but
// in a test of the meridian command that shortens it (testtiming.Scale),
// each of the coordinator's periods scaled, the lease and its heartbeat to
// whole seconds, which is how a worker is told them, and the heartbeat
// down, so that it stays a fifth of the lease at most. WorkerStop isn't
// scaled: it is the worker's own time, which no scale shortens. Each is
// scaled in place, so that a period added to Timing keeps its default
// rather than be left out.
func openTiming() (Timing, error) {
	tm, s := DefaultTiming(), testtiming.Scale
	switch {
	case s == 0:
		return tm, nil
	case s < 0.1 || s > 1:
		return Timing{}, fmt.Errorf("coordinator: a test's timing scale %v isn't from 0.1 to 1", s)
	}
	for _, d := range []*time.Duration{&tm.Lease, &tm.Heartbeat, &tm.FenceMargin, &tm.Grace, &tm.WakeJump, &tm.CheckGrace} {
		*d = time.Duration(float64(*d) * s)
	}
	tm.Lease = tm.Lease.Truncate(time.Second)
	tm.Heartbeat = tm.Heartbeat.Truncate(time.Second)
	return tm, nil
}
