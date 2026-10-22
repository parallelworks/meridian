package plan

import (
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/problems"
)

// An Outcome is how a job or a step ended: what needs.<job>.result reads.
type Outcome string

// The outcomes.
const (
	OutcomeSuccess   Outcome = "success"
	OutcomeFailure   Outcome = "failure"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeSkipped   Outcome = "skipped"
)

// A Reason is why an attempt ended other than as its steps did, or why a
// worker was told to stop, which the attempt records. A job's retry: lists
// the reasons it may run again for.
type Reason string

// The reasons.
const (
	// ReasonWorkerLost is a worker lost after a step started: its backend
	// says it ended without completing its attempt, or a job that fences
	// itself let its lease lapse. A job whose retry: lists it runs again.
	ReasonWorkerLost Reason = "worker-lost"
	// ReasonSetupFailed is an attempt that failed before its first step
	// started, such as an image that couldn't be pulled: none of the
	// workflow's code ran, so the job is placed again, on another member of
	// its group when there is one.
	ReasonSetupFailed Reason = "setup-failed"
	// ReasonTimeout is a job or a run that ran past its timeout:, which a
	// worker or the coordinator stopped, or a scheduler ended at its time
	// limit.
	ReasonTimeout Reason = "timeout"
	// ReasonOutOfMemory is a job its scheduler or the kernel ended for
	// using more memory than it may.
	ReasonOutOfMemory Reason = "out-of-memory"
	// ReasonCancelled is a run that a person cancelled.
	ReasonCancelled Reason = "cancelled"
	// ReasonFailFast is a matrix instance cancelled because another
	// failed, as its strategy's fail-fast: asks.
	ReasonFailFast Reason = "fail-fast"
)

// A JobResult is how a job ended, as the jobs that need it read it: its
// outcome, which needs.<job>.result reads, and its outputs.
type JobResult struct {
	Outcome Outcome `json:"outcome"`
	// Outputs are the job's outputs, by name, with secrets masked.
	Outputs map[string]string `json:"outputs,omitempty"`
	// Steps are the outputs of its steps that have ids, by id, with secrets
	// masked: needs.<job>.steps.<id>.outputs.<name>.
	Steps map[string]map[string]string `json:"steps,omitempty"`
}

// Combine returns a matrix job's result from its instances', given in the
// order of their indexes, as docs/design.md says under Matrices: its
// outcome is failure if an instance failed, otherwise cancelled if one was
// cancelled, skipped if all were skipped, and success otherwise; an output,
// or a step's, that more than one instance set has the value of the one
// with the highest index.
//
// It isn't built yet, and stops its caller.
func Combine(instances []JobResult) JobResult {
	notImplemented("Combine")
	return JobResult{}
}

// Reads returns the part of run that the job reads, which is what an
// attempt of it receives when it is claimed: the inputs, variables, env
// values and secrets its expressions read, directly or through an input
// made from one, as the plan lists them for the job. The coordinator never
// gives an attempt more.
//
// It isn't built yet, and stops its caller: the plan doesn't list each
// job's reads yet.
func (j *Job) Reads(run RunValues) RunValues {
	notImplemented("Job.Reads")
	return RunValues{}
}

// ReadyInput is what the coordinator knows when a job's dependencies have
// finished.
type ReadyInput struct {
	// Run is what the run knows as a whole, from Start.
	Run RunValues
	// Needs are the results of the jobs the job needs, by job name: for a
	// need on any instance, the instance that finished first.
	Needs map[string]JobResult
	// Cancelled is set once the run is being cancelled, for cancelled().
	Cancelled bool
	// Instance is the matrix instance to ready, for a job whose matrix is
	// expanded: by Start, or by an earlier call to Ready. It is nil for a
	// job without a strategy:, and for one whose matrix Ready is to expand.
	Instance *Instance
	// Limits are the plan's (Run.Limits).
	Limits Limits
}

// ReadyResult is what Ready gives: a matrix's instances, a job skipped, or
// a job ready to place. Exactly one of its fields is set.
type ReadyResult struct {
	// Expansion holds the job's matrix instances, when Ready expanded its
	// matrix. The coordinator keeps them, and readies each instance on its
	// own, with ReadyInput.Instance.
	Expansion *Expansion
	// Skipped is set when the job's if: is false, and the job, or the
	// instance, ends skipped. For a matrix job whose if: doesn't read
	// matrix, it is set before the matrix is expanded, and the job has no
	// instances.
	Skipped bool
	// Job is the job as Ready evaluated it, to place and to run.
	Job *ReadyJob
}

// A ReadyJob is a job instance as Ready evaluated it: what the coordinator
// places, and the job a worker receives when it claims an attempt
// (EncodeReadyJob). What Ready evaluates is final, and never evaluated
// again; the job's env:, steps and outputs: stay the plan's, for Step and
// End to evaluate on the worker.
type ReadyJob struct {
	// Key is the instance's key: the job's name, or for a matrix instance
	// its name, # and index, as in build#2.
	Key string `json:"key"`
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Matrix holds the instance's matrix values, a mapping, or nothing for
	// a job without a matrix.
	Matrix Data `json:"matrix,omitzero"`
	// Target is where the job runs, from its runs-on: or its format 1
	// ssh:.
	Target Target `json:"target,omitzero"`
	// Resources are what the job's resources: asks for.
	Resources backend.Resources `json:"resources,omitzero"`
	// Retry is the job's retry:, or nil without one.
	Retry *ReadyJobRetry `json:"retry,omitzero"`
	// TimeoutSeconds is the job's timeout:, in seconds, or 0 for none.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitzero"`
	// WorkingDirectory is the job's working-directory:, or "" for the run
	// directory.
	WorkingDirectory string `json:"workingDirectory,omitempty"`
	// Env, Steps and Outputs are the job's env:, steps and outputs:, as the
	// plan holds them.
	Env     map[string]Value `json:"env,omitempty"`
	Steps   []Step           `json:"steps"`
	Outputs map[string]Value `json:"outputs,omitempty"`
	// Private and Secrets name the contexts Ready gives the job that were
	// made from a variable or a password, and from a secret, which Step and
	// End mark (Contexts.Private and Contexts.Secrets): matrix, for an
	// instance whose matrix values were (Instance.Private and
	// Instance.Secret).
	Private []string `json:"private,omitempty"`
	Secrets []string `json:"secrets,omitempty"`
	// Masked are the values Ready made from a secret, as a Masker takes
	// them: the worker masks them, as it masks the run's secrets.
	Masked []string `json:"masked,omitempty"`
	// Path is the JSON Pointer of the job in the workflow, for messages.
	Path string `json:"path"`
}

// A Target is where a job runs, from its runs-on: or its format 1 ssh:. At
// most one of its fields is set; with none, the job runs in the group that
// whoever runs the workflow chose.
type Target struct {
	// Group names an execution group: a runs-on: execution-group:, or a
	// runs-on: that reads an execution-groups input, whose value is a
	// group's name.
	Group string `json:"group,omitempty"`
	// Environment is a runs-on: environment:, which only a platform's
	// Groups resolver knows.
	Environment *Environment `json:"environment,omitzero"`
	// ComputeTarget is a runs-on: that reads a compute-target input, which
	// only a platform's Groups resolver knows.
	ComputeTarget *ComputeTarget `json:"computeTarget,omitzero"`
	// Inline is the group a format 1 ssh: makes: one member, named ssh,
	// with the ssh backend and host isolation, whose settings are the ssh:
	// mapping, evaluated.
	Inline *backend.Group `json:"inline,omitzero"`
}

// An Environment is a platform's scheduler environment, which a format 1
// runs-on: environment: names.
type Environment struct {
	// Cluster is the cluster's name, optionally prefixed with its owner, as
	// owner/cluster, and Name the environment's in it.
	Cluster string `json:"cluster"`
	Name    string `json:"name"`
	// Params are its runs-on: params:, the scheduling parameters, a
	// mapping, or nothing.
	Params Data `json:"params,omitzero"`
}

// A ComputeTarget is where a job runs as a compute-target input gives it,
// in a format 1 runs-on:.
type ComputeTarget struct {
	// Mode is environment, login or worker.
	Mode string `json:"mode"`
	// TargetID is the cluster, or user-workspace for the user workspace.
	TargetID      string `json:"targetId,omitempty"`
	EnvironmentID string `json:"environmentId,omitempty"`
	// SchedulingParams are the environment's scheduling parameters, a
	// mapping, or nothing.
	SchedulingParams Data   `json:"schedulingParams,omitzero"`
	NewWorker        bool   `json:"newWorker,omitzero"`
	WorkerID         string `json:"workerId,omitempty"`
}

// ReadyJobRetry is a job's retry: as Ready evaluated it, with the design's
// defaults: 3 retries, and a minute before the first, doubling up to an
// hour. The schema's descriptions still give 10 and 5 seconds, and change
// with the first change to format/.
type ReadyJobRetry struct {
	// Reasons are why an attempt may run again.
	Reasons []Reason `json:"reasons"`
	// MaxRetries is how many times the job may run again.
	MaxRetries int `json:"maxRetries"`
	// IntervalSeconds is how long the first new attempt waits, in seconds.
	// Each one after it waits twice as long as the one before, up to an
	// hour.
	IntervalSeconds int64 `json:"intervalSeconds"`
}

// Ready is the Ready phase: it evaluates what job leaves to the point at
// which its dependencies have finished, in the coordinator, with the
// status functions true or false as in.Needs and in.Cancelled say.
//
// For a job with a strategy: and no in.Instance, it first evaluates the
// job's if:, if it doesn't read matrix, and gives Skipped if it is false;
// otherwise it expands the matrix with Expand, and gives the Expansion. For
// an instance, or a job without a strategy:, it evaluates the job's if:,
// unless it was evaluated before the matrix was expanded, and gives
// Skipped if it is false; otherwise it gives the job, with its runs-on:,
// resources:, retry:, timeout:, working-directory: and ssh: evaluated. A
// job's if: may read matrix in format 1, which GitHub Actions' doesn't:
// the compiler accepts it, and changing that would change what a workflow
// means.
//
// Ready reaches nothing: the same job and input give the same result, byte
// for byte. Its diagnostics are the job's problems at the job's path, with
// no file position, and never show a value made from a variable or a
// password: an expression that fails, or a value that doesn't fit its key,
// such as resources: memory that isn't a quantity, which is a kind of its
// own, added with Ready. An error among them fails the job. It returns an error
// only when ctx is done before it finishes, and then ctx.Err().
//
// It isn't built yet, and returns an error that says so.
func (j *Job) Ready(ctx context.Context, in ReadyInput) (*ReadyResult, problems.Diagnostics, error) {
	return nil, nil, errNotImplemented
}

// EncodeReadyJob encodes a ready job as JSON, as Encode encodes a plan's
// values, for a worker API claim.
func EncodeReadyJob(j *ReadyJob) ([]byte, error) {
	return jsonv2.Marshal(j, encoding)
}

// DecodeReadyJob decodes a ready job that EncodeReadyJob encoded, from a run
// whose plan is of version. It is as strict as Decode: it fails on a
// version this release doesn't read, with ErrVersion, and on a key it
// doesn't know, or one given twice.
func DecodeReadyJob(version int, data []byte) (*ReadyJob, error) {
	if version != Version {
		return nil, fmt.Errorf("%w %d; this release reads version %d", ErrVersion, version, Version)
	}
	j := new(ReadyJob)
	if err := jsonv2.Unmarshal(data, j, decoding, jsonv2.RejectUnknownMembers(true)); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return j, nil
}
