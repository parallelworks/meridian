package plan

import (
	"context"

	"github.com/parallelworks/meridian/problems"
)

// An Outcome is how a job or a step ended: what needs.<job>.result reads.
type Outcome string

// The outcomes.
const (
	Success   Outcome = "success"
	Failure   Outcome = "failure"
	Cancelled Outcome = "cancelled"
	Skipped   Outcome = "skipped"
)

// A Reason is why an attempt ended other than as its steps did, which the
// attempt records, and which a job's retry: lists.
type Reason string

// The reasons.
const (
	// ReasonWorkerLost is a worker lost after a step started: its backend
	// says it is dead, or a job that fences itself let its lease lapse. A
	// job whose retry: lists it runs again.
	ReasonWorkerLost Reason = "worker-lost"
	// ReasonTimeout is a job that ran past its timeout:, which its worker
	// stopped, or a scheduler ended at its time limit.
	ReasonTimeout Reason = "timeout"
)

// A JobResult is how a job ended, as the jobs that need it read it: its
// outcome, which needs.<job>.result reads, and its outputs. A matrix job's
// is its instances', combined as docs/design.md says under Matrices.
type JobResult struct {
	Outcome Outcome `json:"outcome"`
	// Outputs are the job's outputs, by name, with secrets masked.
	Outputs map[string]string `json:"outputs,omitempty"`
	// Steps are the outputs of its steps that have ids, by id, with secrets
	// masked: needs.<job>.steps.<id>.outputs.<name>.
	Steps map[string]map[string]string `json:"steps,omitempty"`
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
	// MaxInstances is the limit on a matrix's instances, or 0 for
	// DefaultMaxInstances.
	MaxInstances int
}

// ReadyResult is what Ready gives: a matrix's instances, a job skipped, or
// a job ready to place. Exactly one of its fields is set.
type ReadyResult struct {
	// Expansion holds the job's matrix instances, when Ready expanded its
	// matrix. The coordinator keeps them, and readies each instance on its
	// own, with ReadyInput.Instance.
	Expansion *Expansion
	// Skipped is set when the job's if: is false. The job ends skipped.
	Skipped bool
	// Job is the job as Ready evaluated it, to place and to run.
	Job *ReadyJob
}

// A ReadyJob is a job instance as Ready evaluated it: what the coordinator
// places, and the job a worker receives when it claims an attempt. What
// Ready evaluates is plain JSON, never evaluated again; the job's env:,
// steps and outputs: stay the plan's, for Step and End to evaluate on the
// worker.
type ReadyJob struct {
	// Key is the instance's key: the job's name, or for a matrix instance
	// its name, # and index, as in build#2.
	Key string `json:"key"`
	// Name is the job's name in the workflow.
	Name string `json:"name"`
	// Matrix holds the instance's matrix values, or nothing for a job
	// without a matrix.
	Matrix map[string]any `json:"matrix,omitempty"`
	// RunsOn, Resources and SSH are the job's runs-on:, resources: and
	// format 1 ssh:, evaluated, in the shapes the schema gives them, for
	// the coordinator to place the job by. Each is nil when the job doesn't
	// set it.
	RunsOn    any `json:"runsOn,omitempty"`
	Resources any `json:"resources,omitempty"`
	SSH       any `json:"ssh,omitempty"`
	// Retry is the job's retry:, with its defaults, or nil without one.
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
	// Secrets names the contexts Ready gives the job that were made from a
	// secret, which Step and End mark (Contexts.Secrets): matrix, for an
	// instance whose matrix values were (Instance.Secret).
	Secrets []string `json:"secrets,omitempty"`
	// Path is the JSON Pointer of the job in the workflow, for messages.
	Path string `json:"path"`
}

// ReadyJobRetry is a job's retry: as Ready evaluated it.
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
// which its dependencies have finished, in the coordinator. For a job with
// a strategy: and no in.Instance, it expands the matrix with Expand and
// gives the Expansion. Otherwise it evaluates the job's if:, with the
// status functions true or false as in.Needs and in.Cancelled say, and
// gives Skipped if it is false, or else the job, with its runs-on:,
// resources:, retry:, timeout:, working-directory: and ssh: evaluated.
//
// Ready reaches nothing: the same job and input give the same result, byte
// for byte. Its diagnostics are the job's problems, such as an expression
// that fails, at the job's path and with no file position, and never show
// a value made from a variable or a password; an error among them fails
// the job. It returns an error only when ctx is done before it finishes,
// and then ctx.Err().
//
// It isn't built yet, and returns an error that says so.
func (j *Job) Ready(ctx context.Context, in ReadyInput) (*ReadyResult, problems.Diagnostics, error) {
	return nil, nil, errNotImplemented
}
