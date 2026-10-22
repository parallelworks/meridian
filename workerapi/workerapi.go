// Package workerapi defines the worker API: the HTTP API through which a
// worker claims an attempt of a job and reports on it, which the
// coordinator serves, and a platform that embeds Meridian serves the same
// (docs/design.md, The worker API).
//
// The API's paths are under /v1/. Bodies are JSON, but a log upload's,
// which is the log's bytes, and both sides encode and decode them with
// Marshal and Unmarshal, which refuse a member the type doesn't have: a
// worker never runs a job without a part it doesn't understand, such as
// the container its steps run in. A duration is in whole seconds and a
// time is RFC 3339. An error is an RFC 9457 problem, a foundation
// problem.Problem: one of foundation's for its status, such as
// unauthenticated for a 401, or one of the codes below.
//
// # Versions
//
// The bodies are versioned by the run's plan version (Claim.PlanVersion):
// a change to a body, as to any type of package plan in one, is a change
// to the plan's encoding, and raises plan.Version. A member added in a
// later version is left out when it is empty, and each side sends it only
// for a run whose version has it, so each reads the other's bodies
// strictly and never meets a member it doesn't know.
//
// As it claims, a worker says the newest version it reads
// (ClaimRequest.PlanVersion), and the coordinator refuses one that doesn't
// read the run's, with PlanTooNew, before it sends anything of the run. A
// coordinator replica answers a worker only for a run whose version it
// reads, as it claims only such runs; otherwise it answers 503 Service
// Unavailable, and the worker tries again, which reaches another replica.
// A ClaimRequest is sent before the worker knows the run's version, so it
// never changes: what a later worker has to say comes after the claim.
//
// # Credentials
//
// A backend starts a worker with the attempt's bootstrap token, which is
// random and single-use, and valid until the attempt is claimed or its
// placement withdrawn. The worker claims the attempt with it as an
// Authorization: Bearer token, and with a credential it makes itself
// (ClaimRequest.Credential), which is the attempt's from then on: the
// worker sends it as the bearer token on every later request, and the
// coordinator stores only its SHA-256 digest, as it stores the bootstrap
// token's. The coordinator checks every write against the attempt's
// fencing number, so a write from an attempt that another has superseded
// fails with Fenced.
//
// The credential is good until the attempt ends. For a job that fences
// itself (Claim.SelfFencing) it is good only while heartbeats renew it,
// and lapses with the lease. A worker whose request fails with 401
// Unauthorized after its claim, or with Fenced, stops its job's steps as a
// cancellation does, and exits: its attempt has ended without it, and
// nothing it sends is stored.
//
// # Retries and idempotency
//
// Every request is safe to repeat, so a worker repeats one that failed
// without an answer, or with a 5xx or 429 status, with backoff, and a
// report is never stored twice. A claim repeated with the same bootstrap
// token and credential is given the same Claim. Events carry sequence
// numbers, from 1 for an attempt's first, and the coordinator stores each
// once, in order. A log upload names the byte offset it starts at, and the
// coordinator stores the bytes it doesn't have yet. Complete stores the
// attempt's end once: repeating it with the same report changes nothing,
// and a different one is refused, 409 Conflict.
//
// While the coordinator can't be reached, a worker keeps running its
// steps, spools its events and logs to disk up to a limit, and sends them
// in order once it can, unless its job fences itself. But before the
// first of the job's steps that runs, it sends that step's started event
// and waits until the coordinator has stored it, so whether the workflow's
// code may have run is always known.
//
// Child runs, through which a step runs a workflow its uses: names, come
// with the Sources resolver, and aren't defined yet.
package workerapi

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// Marshal encodes a body as the API writes it: encoding/json/v2's
// defaults, with mappings in the order of their keys. A nil list or
// mapping is written as an empty one.
func Marshal(v any) ([]byte, error) {
	return jsonv2.Marshal(v, jsonv2.Deterministic(true))
}

// Unmarshal decodes a body as the API reads it: a member the type doesn't
// have, or one given twice, is an error.
func Unmarshal(data []byte, v any) error {
	return jsonv2.Unmarshal(data, v, jsonv2.RejectUnknownMembers(true))
}

// The API's paths. Control is a GET; the others are POSTs.
const (
	// PathClaim claims the attempt a bootstrap token is for: ClaimRequest,
	// answered with a Claim.
	PathClaim = "/v1/claim"
	// PathHeartbeat renews the attempt's lease. It has no body, and is
	// answered with 204 No Content.
	PathHeartbeat = "/v1/heartbeat"
	// PathControl is a long poll for what the coordinator asks of the
	// worker, answered with a Control. Its query names the last version
	// the worker has seen (QueryVersion), and the coordinator answers once
	// it has a newer one, or after ControlWaitSeconds with the same one.
	// The worker polls again at once.
	PathControl = "/v1/control"
	// PathEvents reports step events: Events, answered with an EventsAck.
	PathEvents = "/v1/events"
	// PathLogs uploads part of one log: at most MaxLogUpload of its bytes,
	// as application/octet-stream, with the log, the offset they start at,
	// and whether they end it, in the query (QueryPart, QueryStep,
	// QueryTry, QueryOffset, QueryEnd). Answered with a LogAck.
	PathLogs = "/v1/logs"
	// PathComplete reports the attempt's end: Complete, answered with 204
	// No Content.
	PathComplete = "/v1/complete"
)

// ControlWaitSeconds is the longest the coordinator holds a control poll
// before it answers.
const ControlWaitSeconds = 30

// MaxLogUpload is the most bytes one log upload holds.
const MaxLogUpload = 1 << 20

// The query parameters of PathControl and PathLogs.
const (
	QueryVersion = "version"
	// QueryPart is the log's LogPart; QueryStep and QueryTry are the
	// step's index and its try, for a step's and a cleanup's.
	QueryPart = "part"
	QueryStep = "step"
	QueryTry  = "try"
	// QueryOffset is the byte offset the upload starts at, and QueryEnd,
	// true, says that its bytes end the log: the log's length is final.
	QueryOffset = "offset"
	QueryEnd    = "end"
)

// A LogPart is which of an attempt's logs one is.
type LogPart string

// The logs of an attempt.
const (
	// LogSetup is the job's setup before its first step: the worker
	// starting, its container made, its image pulled.
	LogSetup LogPart = "setup"
	// LogStep is a step's, for each of its tries: 0 for its first run, 1
	// for its first retry.
	LogStep LogPart = "step"
	// LogCleanup is a step's cleanup: command's.
	LogCleanup LogPart = "cleanup"
)

// The environment variables a backend starts meridian worker with, which
// say how it reaches the coordinator and what it claims. The bootstrap
// token is in one of them, or in a file one names, and never on the
// command line, where other users could read it. The worker removes them
// all from the environment its steps get.
const (
	// EnvCoordinator is the coordinator's address: a Unix socket's path,
	// which starts with /, or a host and port, as in login1:7443, for TLS
	// 1.3 (backend.Endpoint).
	EnvCoordinator = "MERIDIAN_COORDINATOR"
	// EnvKeySHA256 is sha256: and the hex of the SHA-256 digest of the
	// coordinator's TLS public key, as DER SubjectPublicKeyInfo, which the
	// worker checks the TLS connection against. It is unset for a Unix
	// socket.
	EnvKeySHA256 = "MERIDIAN_COORDINATOR_KEY_SHA256"
	// EnvBootstrapToken is the attempt's bootstrap token, and
	// EnvBootstrapTokenFile the path of a file only the user can read that
	// holds it, which the worker deletes once it has read it. One of the
	// two is set.
	EnvBootstrapToken     = "MERIDIAN_BOOTSTRAP_TOKEN"
	EnvBootstrapTokenFile = "MERIDIAN_BOOTSTRAP_TOKEN_FILE"
)

// The codes of the API's own problems. The coordinator defines each as a
// kind of package problems, in a registry of its own named worker, so
// that its type URI is /problems/worker/<code>, with its messages and its
// page, when it first reports one.
const (
	// Fenced (409 Conflict) is a request from an attempt that another
	// attempt of its job has superseded, or a report that isn't a repeat
	// after the attempt completed. The worker stops its job's steps as a
	// cancellation does, and exits.
	Fenced problem.Code = "fenced"
	// OutOfOrder (409 Conflict) is events that skip a sequence number, a
	// log upload that starts past the end of the log or comes after its
	// end, or a Complete before the coordinator has every event up to its
	// Seq. Its param ParamExpected is the sequence number or the byte
	// offset the coordinator expects next, which the worker sends from
	// again.
	OutOfOrder problem.Code = "out_of_order"
	// PlanTooNew (409 Conflict) is a claim from a worker that doesn't read
	// the version of the run's plan (ClaimRequest.PlanVersion). The
	// coordinator refuses it before it sends anything of the run, and the
	// worker exits; the attempt failed before its first step, and is
	// placed again.
	PlanTooNew problem.Code = "plan_too_new"
)

// ParamExpected is the param of an OutOfOrder problem.
const ParamExpected = "expected"

// A ClaimRequest claims the attempt a bootstrap token is for. Its members
// never change, since a worker sends it before it knows which version the
// run's bodies are in.
type ClaimRequest struct {
	// Credential is the credential the worker makes for the attempt: 32
	// bytes from crypto/rand, as hex. The coordinator stores only its
	// SHA-256 digest, and a claim with the same bootstrap token and another
	// credential is refused, 401 Unauthorized.
	Credential string `json:"credential"`
	// PlanVersion is the newest plan version the worker reads (plan.Version).
	PlanVersion int `json:"planVersion"`
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

// A Claim is an attempt that a worker claimed: the job as Ready evaluated
// it, with all of the run that the job reads.
type Claim struct {
	// RunID and AttemptID identify the run and the attempt.
	RunID     string `json:"runID"`
	AttemptID string `json:"attemptID"`
	// PlanVersion is the version of the run's plan, which Job is encoded
	// in.
	PlanVersion int `json:"planVersion"`
	// Job is the job as Ready evaluated it, as plan.EncodeReadyJob encodes
	// it: the worker decodes it with plan.DecodeReadyJob(PlanVersion, Job),
	// which is as strict as a plan's decoding.
	Job jsontext.Value `json:"job"`
	// Attempt is the attempt's number, from 1, and Group and Member where it
	// was placed: with the job's Name and Key, its job context.
	Attempt int    `json:"attempt"`
	Group   string `json:"group,omitempty"`
	Member  string `json:"member,omitempty"`
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
	// Run is the part of the run that the job reads (plan.Job.Reads): its
	// inputs' values, variables and secrets among them. The worker masks
	// what Run.Secrets names (plan.RunValues.Masked).
	Run plan.RunValues `json:"run"`
	// Needs are the results of the jobs this one needs, by job name.
	Needs map[string]plan.JobResult `json:"needs,omitempty"`
	// RunDir is the run's directory on the worker's host (PW_JOB_DIR),
	// where steps run by default.
	RunDir string `json:"runDir"`
	// Container is the container the job's steps run in, or nil for steps
	// that run on the host.
	Container *Container `json:"container,omitempty"`
	// TraceParent is the W3C traceparent of the attempt's span, which the
	// worker continues, or "".
	TraceParent string `json:"traceParent,omitempty"`
}

// A Container is the container a job's steps run in. The worker runs on
// the host, creates it, runs each step in it with the runtime's exec, and
// removes it when the job ends.
type Container struct {
	// Runtime is the container runtime: podman, docker, apptainer,
	// singularity or container.
	Runtime string `json:"runtime"`
	// Image is the image as repository@sha256: and its digest, as in
	// ghcr.io/parallelworks/meridian-base@sha256:….
	Image string `json:"image"`
}

// Control is what the coordinator asks of a worker.
type Control struct {
	// Version grows each time the coordinator asks something more.
	Version int64 `json:"version"`
	// Cancel asks the worker to stop its job: to send each running step
	// SIGTERM, then SIGKILL after GraceSeconds, run the cleanup commands,
	// and complete the attempt as cancelled. Reason says why: cancelled,
	// timeout for the run's, or fail-fast.
	Cancel       bool        `json:"cancel,omitzero"`
	Reason       plan.Reason `json:"reason,omitempty"`
	GraceSeconds int64       `json:"graceSeconds,omitzero"`
	// AnyJobFailed says that another job of the run failed: the worker
	// stops each step whose early-cancel: is any-job-failed, as Cancel
	// stops a step. The step ends cancelled, which cancels the rest of its
	// job, as format 1 has it: a later step runs only if it outlives a
	// cancel (plan.ReadyStep.OutlivesCancel), cancelled() is true, and the
	// job ends cancelled unless a step of it failed.
	AnyJobFailed bool `json:"anyJobFailed,omitzero"`
}

// Events are step events, in the order of their sequence numbers.
type Events struct {
	Events []Event `json:"events"`
}

// An Event is a step, or a step's cleanup: command, that started or
// finished. A step whose if: is false has only a finished event, with the
// outcome skipped.
type Event struct {
	// Seq numbers the attempt's events, from 1, one more each time.
	Seq int64 `json:"seq"`
	// Time is when it happened, by the worker's clock.
	Time time.Time `json:"time"`
	// Finished is set when the step finished, and unset when it started.
	Finished bool `json:"finished,omitzero"`
	// Step is the step's index in its job, from 0, and Try its retry: 0
	// for its first run. Cleanup is set for the step's cleanup: command,
	// whose outputs are the job's (plan.StepResult.CleanupOutputs).
	Step    int  `json:"step"`
	Try     int  `json:"try,omitzero"`
	Cleanup bool `json:"cleanup,omitzero"`
	// Outcome, ExitCode, Outputs and Diagnostics are a finished event's:
	// the step's outcome as its job counts it; its command's exit status,
	// or -1 if it has none, as for a command a signal ended or a uses:
	// step; what it wrote to $OUTPUTS, masked; and Step's problems with it.
	Outcome     plan.Outcome         `json:"outcome,omitempty"`
	ExitCode    int                  `json:"exitCode,omitzero"`
	Outputs     map[string]string    `json:"outputs,omitempty"`
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}

// An EventsAck is the coordinator's answer to Events.
type EventsAck struct {
	// Seq is the sequence number of the last event the coordinator has
	// stored.
	Seq int64 `json:"seq"`
}

// A LogAck is the coordinator's answer to a log upload.
type LogAck struct {
	// Length is the log's length now, in bytes: where the next upload
	// starts. Ended is set once an upload has ended the log, and Length
	// is final.
	Length int64 `json:"length"`
	Ended  bool  `json:"ended,omitzero"`
}

// Complete is an attempt's end. The worker sends it once it has sent its
// events, and ended its logs.
type Complete struct {
	// Seq is the sequence number of the attempt's last event, or 0 for
	// none.
	Seq int64 `json:"seq"`
	// Outcome is the job's outcome, and Reason why it ended other than as
	// its steps did, such as timeout, or "".
	Outcome plan.Outcome `json:"outcome"`
	Reason  plan.Reason  `json:"reason,omitempty"`
	// Problem is what failed before the first step started, with the
	// outcome failure and the reason setup-failed: an image that couldn't
	// be pulled, or a container runtime that is missing. None of the
	// workflow's code ran, so the coordinator places the job again; its
	// Code is the reason placement compares, so a job two members fail for
	// the same reason fails. It is nil otherwise.
	Problem *problem.Problem `json:"problem,omitempty"`
	// Outputs are the job's outputs, as End gave them, and PrivateOutputs
	// names those that are private (plan.EndResult.Private), whose values
	// the coordinator keeps only in the run's secret record.
	Outputs        map[string]string `json:"outputs,omitempty"`
	PrivateOutputs []string          `json:"privateOutputs,omitempty"`
	// Diagnostics are End's problems with the job's outputs.
	Diagnostics problems.Diagnostics `json:"diagnostics,omitempty"`
}
