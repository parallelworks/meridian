package coordinator

import "time"

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
}

// DefaultTiming returns the timing a coordinator runs with.
func DefaultTiming() Timing {
	return Timing{
		Lease:       60 * time.Second,
		Heartbeat:   10 * time.Second,
		FenceMargin: 15 * time.Second,
		Grace:       60 * time.Second,
		WakeJump:    10 * time.Second,
	}
}
