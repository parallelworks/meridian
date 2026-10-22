// Package coordinator stores runs, schedules and places their jobs, and
// serves the worker API (docs/design.md, The coordinator).
//
// What differs between a laptop and a platform reaches the coordinator
// through interfaces, besides the compiler's resolvers: Keys, which wraps
// the keys of runs' secret records; Logs, which stores step logs; and
// Events, which hears of runs, jobs and steps as they happen. Each gets
// the caller's context.
package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/parallelworks/meridian/plan"
)

// Keys wraps the data keys of runs' secret records: a platform's key
// management service, or locally a key file only the user can read. A
// secret record is encrypted with AES-256-GCM under a data key of its own,
// stored beside it wrapped. Keys uses FIPS-approved algorithms only.
type Keys interface {
	// Wrap encrypts key, the data key of the secret record of the run
	// runID, for storage beside the record. It binds runID to what it
	// gives, so that a wrapped key can't be moved to another run.
	Wrap(ctx context.Context, runID string, key []byte) ([]byte, error)
	// Unwrap decrypts a data key that Wrap wrapped for runID.
	Unwrap(ctx context.Context, runID string, wrapped []byte) ([]byte, error)
}

// ErrLogGap is what Logs.Append returns for an offset past a log's end.
var ErrLogGap = errors.New("coordinator: log write starts past its end")

// Logs stores step logs, which workers upload in parts, and reads them.
// Each attempt of a job, and each try of a step, keeps its own log. A log
// arrives masked: the worker masks secrets before they leave its host.
type Logs interface {
	// Append stores data as the part of the log at key that starts at
	// offset, and returns the log's length after it. It skips the bytes
	// before the log's end, which the log has already, so an upload
	// repeated after a dropped connection is stored once. An offset past
	// the end returns the length and ErrLogGap.
	Append(ctx context.Context, key LogKey, offset int64, data []byte) (int64, error)
	// Read returns the part of the log at key that starts at offset, at
	// most limit bytes. At the log's end it returns nothing, at once.
	Read(ctx context.Context, key LogKey, offset int64, limit int) ([]byte, error)
}

// A LogKey names one log.
type LogKey struct {
	Run string
	// Job is the job instance's key, as in build#2, and Attempt the
	// attempt's number, from 1.
	Job     string
	Attempt int
	// Step is the step's index in its job, from 0, and Try its retry: 0
	// for its first run. Cleanup is set for the log of the step's cleanup:
	// command.
	Step    int
	Try     int
	Cleanup bool
}

// Events hears of runs, jobs and steps as they happen, such as the
// command's progress display.
type Events interface {
	// Event reports e, once what it reports is stored. A coordinator that
	// repeats what it did, as one that took over a job does, can report an
	// event again, so a consumer that counts them keys them on their
	// fields. Event must not block: it holds up the job it reports on.
	Event(ctx context.Context, e Event)
}

// An Event is a change in a run, a job or a step.
type Event struct {
	Time time.Time
	Kind EventKind
	Run  string
	// Job is the job instance's key, for a job's or a step's event, and
	// Attempt the attempt's number, from 1, once the job is placed.
	Job     string
	Attempt int
	// Step, Try and Cleanup are a step's event's, as in LogKey.
	Step    int
	Try     int
	Cleanup bool
	// Member is where the attempt was placed, for JobPlacing and later.
	Member string
	// Outcome is a finished event's, and Reason why an attempt or a job
	// ended other than as its steps did, such as worker-lost, or "".
	Outcome plan.Outcome
	Reason  plan.Reason
	// ExitCode is a finished step's exit status, or -1 if it has none.
	ExitCode int
}

// An EventKind is what an event says happened.
type EventKind string

// The kinds of event, in the order a run's happen: a job is ready once its
// dependencies have finished, placing while a backend starts its worker,
// and running once the worker has claimed it.
const (
	RunStarted   EventKind = "run.started"
	JobReady     EventKind = "job.ready"
	JobPlacing   EventKind = "job.placing"
	JobRunning   EventKind = "job.running"
	StepStarted  EventKind = "step.started"
	StepFinished EventKind = "step.finished"
	JobFinished  EventKind = "job.finished"
	RunFinished  EventKind = "run.finished"
)
