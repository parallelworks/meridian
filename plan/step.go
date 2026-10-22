package plan

import (
	"context"

	"github.com/parallelworks/meridian/problems"
)

// JobState is what a worker knows of its job as it runs it, which Step
// and End read.
type JobState struct {
	// Run is the part of the run that the job reads, as the attempt
	// received it when it was claimed.
	Run RunValues
	// Needs are the results of the jobs the job needs, by job name.
	Needs map[string]JobResult
	// Job is the job context.
	Job JobContext
	// Steps are the results of the job's steps that have had their turn,
	// by index: each step before the one about to run, for Step, and every
	// step, for End.
	Steps []StepResult
	// Cancelled is set once the job is being cancelled, for cancelled().
	Cancelled bool
	// Masked are the values the worker masks, as a Masker takes them: the
	// run's secrets' (RunValues.Masked), the job's (ReadyJob.Masked), and
	// those of each step so far (ReadyStep.Masked). End masks the job's
	// outputs with them, as the worker masks its logs and its steps'
	// outputs.
	Masked []string
}

// A StepResult is how a step ended.
type StepResult struct {
	// Outcome is the step's outcome as its job counts it: a step that
	// failed with ignore-errors is a success, and one whose if: was false
	// is skipped.
	Outcome Outcome
	// Outputs are what the step wrote to $OUTPUTS, by name, as written:
	// the job's later steps and its outputs: read them unmasked.
	Outputs map[string]string
}

// A ReadyStep is a step as Step evaluated it, ready to run: plain values,
// never evaluated again.
type ReadyStep struct {
	// Index is the step's index in its job, from 0, and ID its id:, if it
	// has one.
	Index int    `json:"index"`
	ID    string `json:"id,omitempty"`
	// Name is the step's name:, or "" without one.
	Name string `json:"name,omitempty"`
	// Skipped is set when the step's if: is false: the step doesn't run,
	// its outcome is skipped, and nothing below is set.
	Skipped bool `json:"skipped,omitzero"`
	// Run is the command a run: step runs.
	Run string `json:"run,omitempty"`
	// Uses names the built-in action or the workflow a uses: step runs, as
	// the plan holds it, and With is its with:, a mapping.
	Uses string `json:"uses,omitempty"`
	With Data   `json:"with,omitzero"`
	// Shell is the step's shell:, in which {0} is the script's path, or ""
	// for the default, a bash that isn't a login shell.
	Shell string `json:"shell,omitempty"`
	// Env is the environment the workflow gives the step: the workflow's
	// env:, then the job's over it, then the step's over that. The
	// variables format 1 defines, such as OUTPUTS and PW_JOB_DIR, are the
	// worker's to add.
	Env map[string]string `json:"env,omitempty"`
	// WorkingDirectory is where the step runs: its working-directory:, or
	// its job's when it has none, or "" for the run directory, as a step's
	// working-directory: null asks.
	WorkingDirectory string `json:"workingDirectory,omitempty"`
	// SSH is the host a format 1 step's ssh: runs it on, outside any
	// container, as a job's is (Target.Inline), or nil without one.
	SSH *Inline `json:"ssh,omitzero"`
	// TimeoutSeconds is the step's timeout:, in seconds, or 0 for none.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitzero"`
	// Retry is the step's retry:, with its defaults, or nil without one.
	Retry *ReadyStepRetry `json:"retry,omitzero"`
	// IgnoreErrors is the step's ignore-errors:: a non-zero exit doesn't
	// fail it.
	IgnoreErrors bool `json:"ignoreErrors,omitzero"`
	// EarlyCancel is the step's early-cancel:, any-job-failed or "".
	EarlyCancel string `json:"earlyCancel,omitempty"`
	// Cleanup is the step's cleanup: command, which runs at the end of its
	// job if the step ran, or "".
	Cleanup string `json:"cleanup,omitempty"`
	// Masked are the values above that Step made from a secret, such as an
	// env value that reads one, as a Masker takes them: the worker masks
	// each wherever it appears in the job's logs and outputs, as it masks
	// the secrets themselves.
	Masked []string `json:"masked,omitempty"`
}

// ReadyStepRetry is a step's retry: as Step evaluated it, with format 1's
// defaults, which reruns the step on the same worker when it fails: its
// max-retries:, its interval: and timeout:, in seconds, and its
// clean-on-retry:, off where format 1 forces it off.
type ReadyStepRetry struct {
	MaxRetries      int   `json:"maxRetries"`
	IntervalSeconds int64 `json:"intervalSeconds"`
	TimeoutSeconds  int64 `json:"timeoutSeconds"`
	CleanOnRetry    bool  `json:"cleanOnRetry,omitzero"`
}

// Step is the Step phase: it evaluates the step at index in the job, on
// the job's worker, as the step is about to run. It evaluates the job's
// env: first, with the job context, then the step's if:, with the status
// functions true or false as state.Steps and state.Cancelled say, and
// gives the step Skipped if it is false. Otherwise it evaluates the step's
// env: and then the rest of the step, and gives the step.
//
// Step reaches nothing, and gives the same result for the same job, index
// and state, byte for byte. Its diagnostics are the step's problems, such
// as an expression that fails, at the step's path and with no file
// position, and never show a value made from a variable or a password; an
// error among them fails the step. It returns an error when ctx is done
// before it finishes, ctx.Err(), and for an index that isn't one of the
// job's steps, or state.Steps that aren't one for each step before it,
// which are its caller's mistakes.
//
// It isn't built yet, and returns an error that says so.
func (j *ReadyJob) Step(ctx context.Context, index int, state JobState) (*ReadyStep, problems.Diagnostics, error) {
	return nil, nil, errNotImplemented
}

// End is the End phase: it evaluates the job's outputs:, on its worker,
// once the job's steps have run, and gives them by name. It masks each
// with a Masker of state.Masked, and masks the whole of one that was made
// from a secret: an output that holds a secret is given with it masked,
// problems.Masked, and a warning that names the output.
//
// End reaches nothing, and gives the same outputs for the same job and
// state, byte for byte. Its diagnostics are as Step's, at the job's
// outputs:; an error among them fails the job. It returns an error when
// ctx is done before it finishes, ctx.Err(), and for state.Steps that
// aren't one for each of the job's steps.
//
// It isn't built yet, and returns an error that says so.
func (j *ReadyJob) End(ctx context.Context, state JobState) (map[string]string, problems.Diagnostics, error) {
	return nil, nil, errNotImplemented
}
