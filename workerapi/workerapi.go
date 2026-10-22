// Package workerapi defines the worker API: the HTTP API through which a
// worker claims an attempt of a job and reports on it, which the
// coordinator serves, and a platform that embeds Meridian serves the same
// (docs/design.md, The worker API).
//
// The API is versioned in its paths, under /v1/. Bodies are JSON, but a
// log upload's, which is the log's bytes. A duration is in whole seconds
// and a time is RFC 3339, so that encoding/json and encoding/json/v2 each
// read what the other writes of every type here. An error is an RFC 9457
// problem, a foundation problem.Problem: one of foundation's for its
// status, such as unauthenticated for a 401, or one of the codes below.
//
// # Credentials and fencing
//
// A worker starts with a bootstrap token, which it sends to claim its
// attempt as an Authorization: Bearer token. The claim gives it the
// attempt's credential and fencing number, and every later request
// carries the credential as its bearer token and the number in
// HeaderFence. The credential expires unless heartbeats renew it. Once
// another attempt of the job has superseded this one, every request fails
// with Fenced, and the worker stops its job's steps as a cancellation
// does, and exits: nothing it sends after that is stored.
//
// # Retries and idempotency
//
// Every request is safe to repeat, so a worker repeats one that failed
// without an answer, or with a 5xx or 429 status, with backoff, and a
// report is never stored twice. A claim repeats with its ClaimRequest.Key.
// Events carry sequence numbers, from 1 for an attempt's first, and the
// coordinator stores each once, in order. A log upload names the byte
// offset it starts at, and the coordinator stores the bytes it doesn't
// have yet. Complete stores the attempt's end once: repeating it with the
// same report changes nothing, and a different one is refused, 409
// Conflict.
//
// While the coordinator can't be reached, a worker keeps running its
// steps, spools its events and logs to disk up to a limit, and sends them
// in order once it can, unless its job fences itself (Claim.SelfFencing).
// But before the first of the job's steps that runs, it sends that step's
// started event and waits until the coordinator has stored it, so whether
// the workflow's code may have run is always known.
//
// Child runs, through which a step runs a workflow its uses: names, come
// with the Sources resolver, and aren't defined yet.
package workerapi

import (
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// The API's paths. Control is a GET; the others are POSTs.
const (
	// PathClaim exchanges a bootstrap token for an attempt: ClaimRequest,
	// answered with a Claim.
	PathClaim = "/v1/claim"
	// PathHeartbeat renews the attempt's lease and credential. It has no
	// body, and is answered with 204 No Content.
	PathHeartbeat = "/v1/heartbeat"
	// PathControl is a long poll for what the coordinator asks of the
	// worker, answered with a Control. Its query names the last version
	// the worker has seen (QueryVersion), and the coordinator answers once
	// it has a newer one, or after a wait of its choosing with the same
	// one. The worker polls again at once.
	PathControl = "/v1/control"
	// PathEvents reports step events: Events, answered with an EventsAck.
	PathEvents = "/v1/events"
	// PathLogs uploads part of one log: its bytes, as
	// application/octet-stream, with the log and the offset they start at
	// in the query (QueryStep, QueryTry, QueryCleanup, QueryOffset).
	// Answered with a LogAck.
	PathLogs = "/v1/logs"
	// PathComplete reports the attempt's end: Complete, answered with 204
	// No Content.
	PathComplete = "/v1/complete"
)

// HeaderFence is the header that carries the attempt's fencing number, in
// decimal, on every request after the claim.
const HeaderFence = "Meridian-Fence"

// The environment variables a backend starts meridian worker with, which
// say how it reaches the coordinator and what it claims. The bootstrap
// token is in one of them, or in a file one names, and never on the
// command line, where other users could read it. The worker removes them
// all from the environment its steps get.
const (
	// EnvCoordinator is the coordinator's address: a Unix socket's path,
	// which starts with /, or a host and port, as in login1:7443, for TLS
	// 1.3.
	EnvCoordinator = "MERIDIAN_COORDINATOR"
	// EnvKeyHash is sha256: and the hex of the SHA-256 digest of the
	// coordinator's TLS public key, as DER SubjectPublicKeyInfo, which the
	// worker checks the TLS connection against. It is unset for a Unix
	// socket.
	EnvKeyHash = "MERIDIAN_COORDINATOR_KEY"
	// EnvBootstrapToken is the attempt's bootstrap token, and
	// EnvBootstrapTokenFile the path of a file only the user can read that
	// holds it, which the worker deletes once it has read it. One of the
	// two is set.
	EnvBootstrapToken     = "MERIDIAN_BOOTSTRAP_TOKEN"
	EnvBootstrapTokenFile = "MERIDIAN_BOOTSTRAP_TOKEN_FILE"
)

// The query parameters of PathControl and PathLogs.
const (
	QueryVersion = "version"
	QueryStep    = "step"
	QueryTry     = "try"
	QueryCleanup = "cleanup"
	QueryOffset  = "offset"
)

// The codes of the API's own problems. Each will be a kind in package
// problems, with its messages and page, once the coordinator reports it.
const (
	// Fenced (409 Conflict) is a request from an attempt that another
	// attempt of its job has superseded, or a report that isn't a repeat
	// after the attempt completed. The worker stops its job's steps as a
	// cancellation does, and exits.
	Fenced problem.Code = "fenced"
	// OutOfOrder (409 Conflict) is events that skip a sequence number, a
	// log upload that starts past the end of the log, or a Complete before
	// the coordinator has every event up to its Seq. Its param expected is
	// the sequence number or the byte offset the coordinator expects next,
	// which the worker sends from again.
	OutOfOrder problem.Code = "out_of_order"
)

// A ClaimRequest claims the attempt a bootstrap token is for.
type ClaimRequest struct {
	// Key is a random value the worker makes once, at least 128 bits as
	// hex, so that a claim it repeats after a dropped connection is given
	// the same Claim. The bootstrap token is single-use: a claim with
	// another key is refused, 401 Unauthorized.
	Key string `json:"key"`
	// Worker is the worker that claims, for the attempt's record.
	Worker Worker `json:"worker"`
}

// A Worker is the meridian binary that runs an attempt.
type Worker struct {
	// Version is its release.
	Version string `json:"version"`
	// Digest is sha256: and the hex of its binary's SHA-256 digest.
	Digest string `json:"digest"`
}

// A Claim is an attempt that a worker claimed: its credential, and the job
// as Ready evaluated it, with all of the run that the job reads.
type Claim struct {
	// RunID and AttemptID identify the run and the attempt.
	RunID     string `json:"runID"`
	AttemptID string `json:"attemptID"`
	// Credential is the attempt's credential: opaque, random, and good for
	// reporting on this attempt only. The worker keeps it out of its
	// steps' environment and out of logs.
	Credential string `json:"credential"`
	// Fence is the attempt's fencing number, which grows with every
	// attempt of the job.
	Fence int64 `json:"fence"`
	// LeaseSeconds is how long the lease lasts after a heartbeat, and
	// HeartbeatSeconds how often the worker sends one.
	LeaseSeconds     int64 `json:"leaseSeconds"`
	HeartbeatSeconds int64 `json:"heartbeatSeconds"`
	// SelfFencing is set for a job whose retry: lists worker-lost, which
	// can have a new attempt while this one may still run: the worker
	// stops the job once it hasn't renewed its lease for LeaseSeconds,
	// counted on its monotonic clock from when it sent the last heartbeat
	// that succeeded.
	SelfFencing bool `json:"selfFencing,omitzero"`
	// Job is the job as Ready evaluated it.
	Job *plan.ReadyJob `json:"job"`
	// JobContext is the job context, with the attempt and where it runs.
	JobContext plan.JobContext `json:"jobContext"`
	// Run is the part of the run that the job reads: its inputs' values,
	// variables and secrets among them, which Run.Secrets names. The
	// worker masks each secret's value in its logs and outputs.
	Run plan.RunValues `json:"run"`
	// Needs are the results of the jobs this one needs, by job name.
	Needs map[string]plan.JobResult `json:"needs,omitempty"`
	// RunDir is the run's directory on the worker's host (PW_JOB_DIR),
	// where steps run by default.
	RunDir string `json:"runDir"`
	// Container is the container the job's steps run in, or nil for steps
	// that run on the host.
	Container *Container `json:"container,omitempty"`
}

// A Container is the container a job's steps run in. The worker runs on
// the host, creates it, runs each step in it with the runtime's exec, and
// removes it when the job ends.
type Container struct {
	// Runtime is the container runtime: podman, docker, apptainer,
	// singularity or container.
	Runtime string `json:"runtime"`
	// Image is the image, by its digest.
	Image string `json:"image"`
}

// Control is what the coordinator asks of a worker.
type Control struct {
	// Version grows each time the coordinator asks something more.
	Version int64 `json:"version"`
	// Cancel asks the worker to stop its job: to send each running step
	// SIGTERM, then SIGKILL after GraceSeconds, run the cleanup commands,
	// and complete the attempt as cancelled.
	Cancel       bool  `json:"cancel,omitzero"`
	GraceSeconds int64 `json:"graceSeconds,omitzero"`
	// AnyJobFailed says that another job of the run failed: the worker
	// stops each step whose early-cancel: is any-job-failed, as Cancel
	// stops a step.
	AnyJobFailed bool `json:"anyJobFailed,omitzero"`
}

// Events are step events, in the order of their sequence numbers.
type Events struct {
	Events []Event `json:"events"`
}

// An Event is a step that started or finished. A step whose if: is false
// has only a finished event, with the outcome skipped.
type Event struct {
	// Seq numbers the attempt's events, from 1, one more each time.
	Seq int64 `json:"seq"`
	// Time is when it happened, by the worker's clock.
	Time time.Time `json:"time"`
	Kind EventKind `json:"kind"`
	// Step is the step's index in its job, from 0, and Try its retry: 0
	// for its first run. Cleanup is set for the step's cleanup: command.
	Step    int  `json:"step"`
	Try     int  `json:"try,omitzero"`
	Cleanup bool `json:"cleanup,omitzero"`
	// Outcome, ExitCode, Outputs and Diagnostics are a finished event's:
	// the step's outcome as its job counts it; its command's exit status,
	// or -1 if it has none, as for a command a signal ended or a uses:
	// step; what it wrote to $OUTPUTS, with secrets masked; and Step's
	// problems with it.
	Outcome     plan.Outcome         `json:"outcome,omitempty"`
	ExitCode    int                  `json:"exitCode,omitzero"`
	Outputs     map[string]string    `json:"outputs,omitempty"`
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}

// An EventKind is what an event says of a step.
type EventKind string

// The kinds of event.
const (
	StepStarted  EventKind = "step.started"
	StepFinished EventKind = "step.finished"
)

// An EventsAck is the coordinator's answer to Events.
type EventsAck struct {
	// Seq is the sequence number of the last event the coordinator has
	// stored.
	Seq int64 `json:"seq"`
}

// A LogAck is the coordinator's answer to a log upload.
type LogAck struct {
	// Length is the log's length now, in bytes: where the next upload
	// starts.
	Length int64 `json:"length"`
}

// Complete is an attempt's end.
type Complete struct {
	// Seq is the sequence number of the attempt's last event.
	Seq int64 `json:"seq"`
	// Outcome is the job's outcome, and Reason why it ended other than as
	// its steps did, such as timeout, or "".
	Outcome plan.Outcome `json:"outcome"`
	Reason  plan.Reason  `json:"reason,omitempty"`
	// Outputs are the job's outputs, as End gave them.
	Outputs map[string]string `json:"outputs,omitempty"`
	// Diagnostics are End's problems with the job's outputs.
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}
