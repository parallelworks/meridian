// Package coordinator stores runs, schedules and places their jobs, and
// serves the worker API (docs/design.md, The coordinator).
//
// What differs between a laptop and a platform reaches the coordinator
// through interfaces, besides the compiler's resolvers and the backends'
// drivers: Keys, which wraps the keys of runs' secret records; and Logs,
// which stores step logs. Each gets the caller's context. Whoever shows a
// run as it happens, as the command's progress display does, follows its
// events and its logs (Follow). An error from Keys that is
// a problem (problems.IsProblem), such as a wrapped key that can't be
// unwrapped for its run, is one trying again can't fix, and fails the run;
// any other error from Keys or Logs means it couldn't do what it was
// asked, and the coordinator tries again.
package coordinator

import (
	"context"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/runlog"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// Keys wraps the data keys of runs' secret records: a platform's key
// management service, or locally a key file only the user can read. A
// secret record is encrypted with AES-256-GCM under a data key of its own,
// stored beside it wrapped. Keys uses FIPS-approved algorithms only.
//
// The run ID Keys is given is the store's ID and the run's, joined with a
// slash, which names the run among every store's, since a store's run IDs
// can be another's too. Wrap doesn't keep the key it is given, and the key
// Unwrap gives is the caller's alone: the caller clears each once it is
// done with it. A problem, of a kind in problems, says that what Keys was
// given can't be wrapped or unwrapped, which trying again won't change.
type Keys interface {
	// Wrap encrypts key, the data key of the secret record of the run
	// runID, for storage beside the record. It binds runID to what it
	// gives, as associated data, so that a wrapped key can't be moved to
	// another run.
	Wrap(ctx context.Context, runID string, key []byte) ([]byte, error)
	// Unwrap decrypts a data key that Wrap wrapped for runID.
	Unwrap(ctx context.Context, runID string, wrapped []byte) ([]byte, error)
}

// ErrLogGap is what Logs.Append returns for an offset past a log's end, or
// for bytes past the end of a log that has ended.
var ErrLogGap = runlog.ErrGap

// Logs stores logs, which workers upload in parts, and reads them. Each
// attempt of a job, and each try of a step, keeps its own logs. A log
// arrives masked: the worker masks secrets before they leave its host.
// Its methods' rules are runlog.Logs'.
type Logs = runlog.Logs

// A LogKey names one log: its run, the job instance's key, as in build#2,
// the attempt's number, from 1, which of the attempt's logs it is, and for
// a step's or a cleanup's, the step's index and its try.
type LogKey = runlog.Key

// An Event is a change in a run, a job, an attempt or a step, as the
// store keeps it, which Follow reports.
type Event struct {
	// Seq numbers the run's events, from 1, in the order they happened.
	Seq int64
	// Time is when it happened: by the worker's clock for a step's event,
	// and by the store's for any other.
	Time time.Time
	Kind EventKind
	Run  string
	// Job is the job instance's key, for a job's, an attempt's or a step's
	// event, and Attempt the attempt's number, from 1, for an attempt's or
	// a step's.
	Job     string
	Attempt int
	// Step, Try and Cleanup are a step's event's: the step's index, its
	// try, and whether it is the step's cleanup: command.
	Step    int
	Try     int
	Cleanup bool
	// Member is where the attempt was placed, as its group and its member
	// joined with a slash, as in local/laptop, for an attempt's event, or
	// "" where that is private, which only the run's secret record holds.
	Member string
	// End is how an attempt ended, of AttemptEnded.
	End AttemptEnd
	// Outcome is a finished event's, and Reason why an attempt, a job or
	// the run ended other than as its steps did, such as worker-lost, or "".
	Outcome plan.Outcome
	Reason  plan.Reason
	// ExitCode is a finished step's exit status, or -1 if it has none.
	ExitCode int
	// Diagnostics are a finished event's problems: a step's, as its worker
	// reported them; the problem an attempt that failed before its first
	// step ended with; a job's, as Ready's or placement's; or the run's.
	// Like the logs, they show no secret: a worker masks what it reports,
	// and the coordinator masks what an attempt reports when where it runs
	// is private.
	Diagnostics problems.Diagnostics
}

// An EventKind is what an event says happened.
type EventKind string

// The kinds of event, in the order a run's happen: a job is ready once its
// dependencies have finished, placing once a member is chosen for its
// attempt, and running once the attempt's worker has claimed it; an
// attempt ends, completed or otherwise, before its job ends, as one that
// another supersedes does.
const (
	RunStarted   EventKind = store.EventRunStarted
	JobReady     EventKind = store.EventJobReady
	JobPlacing   EventKind = store.EventJobPlacing
	JobRunning   EventKind = store.EventJobRunning
	StepStarted  EventKind = store.EventStepStarted
	StepFinished EventKind = store.EventStepFinished
	AttemptEnded EventKind = store.EventAttemptEnded
	JobFinished  EventKind = store.EventJobFinished
	RunFinished  EventKind = store.EventRunFinished
)

// An AttemptEnd is how an attempt ended.
type AttemptEnd string

// The ways an attempt ends: its worker completed it, as it does one that
// failed before its first step too; or the coordinator ended it, as one
// whose worker was lost, one a newer attempt of its job superseded, or one
// whose placement was withdrawn.
const (
	AttemptCompleted   AttemptEnd = AttemptEnd(store.EndCompleted)
	AttemptSetupFailed AttemptEnd = AttemptEnd(store.EndSetupFailed)
	AttemptLost        AttemptEnd = AttemptEnd(store.EndLost)
	AttemptSuperseded  AttemptEnd = AttemptEnd(store.EndSuperseded)
	AttemptWithdrawn   AttemptEnd = AttemptEnd(store.EndWithdrawn)
)
