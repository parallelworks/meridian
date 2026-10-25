package plan

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
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
	// Cancelled is set once the job is being cancelled, for cancelled():
	// once the run is. A step of the job that ended cancelled cancels the
	// job too, as format 1 has it, which Step and End read from Steps. A
	// job that runs past its timeout isn't cancelled: the step it was
	// running fails, so failure() is true and cancelled() false after it,
	// as in format 1.
	Cancelled bool
	// Try is the step's try, from 0, for Step: the worker has Step evaluate
	// the step again for each try, as format 1 does, so that what reads
	// env.PW_WORKFLOW_STEP_CURRENT_RETRY reads the try's. FirstTry is the
	// step as Step gave it at its first try, for a later one, which carries
	// over what the first decided.
	Try      int
	FirstTry *ReadyStep
	// Env holds the variables the worker defines for the step, or for the
	// job's env: and outputs:, such as PW_JOB_DIR, PW_PARENT_JOB_DIR,
	// OUTPUTS and, for a try, PW_WORKFLOW_STEP_CURRENT_RETRY, which env
	// reads over the workflow's, the job's and the step's, as format 1 lays
	// them. The plan reads only those format 1 defines (IsFormatVariable),
	// and in the job's env:, working-directory: and outputs: only those it
	// defines as the job starts (IsJobStartVariable), so a credential the
	// worker passes, such as the platform's API key that format 1 gives a
	// step's environment as PW_API_KEY, reaches no expression.
	Env map[string]string
	// JobEnv is the job's env: as the JobEnv phase gave it, as the job
	// started, which Step, Cleanup and End read rather than evaluate the
	// job's env: again. Without it, each evaluates the job's env: itself,
	// as JobEnv does, with state.
	JobEnv *JobEnv
	// Masked are the values the worker masks, as a Masker takes them: the
	// run's secrets' (RunValues.Masked), the job's (ReadyJob.Masked), and
	// those of each step so far (ReadyStep.Masked). End and the steps tell
	// with them which of the job's outputs, and its steps', hold a secret,
	// as the worker masks its logs with them.
	Masked []string
	// Secrets are the texts of secrets the worker knows of that don't come
	// from the run, such as the secret variables a platform gives a job's
	// steps, which no expression reads. The phases count each as a secret's
	// own text, as they count the run's: masked however short, own to End,
	// which names an output that holds one a secret and not derived, as the
	// jobs that need the job then read it, and read for the credential
	// fields it holds (credentialFields), which JobEnv gives in its Masked,
	// for the worker to mask in the job's logs.
	Secrets []string
}

// A JobEnv is the job's env: as the JobEnv phase evaluated it, as the job
// started on its worker.
type JobEnv struct {
	// Env holds the values of the job's env:, by name.
	Env map[string]string `json:"env,omitempty"`
	// Private and Secrets name those made from an organization or user
	// variable or a password, and from a secret.
	Private []string `json:"private,omitempty"`
	Secrets []string `json:"secrets,omitempty"`
	// Masked are the strings it made from a secret, as ReadyStep.Masked
	// holds them, and the texts of the secrets that don't come from the run
	// (JobState.Secrets), with the credential fields they hold, which the
	// worker masks; each ReadyStep.Masked holds them too.
	Masked []string `json:"masked,omitempty"`
	// own are the job's secrets' texts and what masks them, as JobEnv found
	// and built them for the attempt, which Step, Cleanup and End share
	// rather than find and build again, with what they find of the job's
	// steps' outputs (outputChecks), when their state's secrets are the
	// same (secretsSum).
	own *secretTexts
}

// formatVariables are the names of the variables format 1 gives a step,
// which env reads over the workflow's, the job's and the step's
// (JobState.Env), as the worker defines them: all of format 1's but its
// credential, PW_API_KEY, which no expression reads. A job's retry: adds
// the last two, Meridian's.
var formatVariables = map[string]bool{
	"PW_USER": true, "PW_PARENT_JOB_DIR": true, "PW_JOB_DIR": true, "PW_PARENT_NAME": true,
	"PW_WORKFLOW_NAME": true, "PW_JOB_NUMBER": true, "PW_JOBS_DIR": true, "PW_JOB_ID": true,
	"PW_PLATFORM_HOST": true, "PW_RUN_SLUG": true, "OUTPUTS": true, "PW_STEP_SUMMARY": true,
	"PW_WORKFLOW_STEP_CURRENT_RETRY": true, "PW_WORKFLOW_STEP_MAX_RETRIES": true, "PW_MATRIX_INDEX": true,
	"PW_WORKFLOW_JOB_CURRENT_RETRY": true, "PW_WORKFLOW_JOB_MAX_RETRIES": true,
}

// stepVariables are the variables format 1 defines for a step alone, which
// a job's env:, working-directory: and outputs: don't read, as they are
// evaluated as the job starts or with what it started with.
var stepVariables = map[string]bool{
	"OUTPUTS": true, "PW_STEP_SUMMARY": true, "PW_WORKFLOW_STEP_CURRENT_RETRY": true, "PW_WORKFLOW_STEP_MAX_RETRIES": true,
}

// IsJobStartVariable reports whether name is one of the variables format 1
// defines as a job starts, which a job's env:, working-directory: and
// outputs: read (IsFormatVariable), and not one it defines for a step
// alone, such as OUTPUTS.
func IsJobStartVariable(name string) bool { return formatVariables[name] && !stepVariables[name] }

// IsFormatVariable reports whether name is one of the variables format 1
// gives a step, which env reads (JobState.Env), such as PW_JOB_DIR and
// OUTPUTS. Its credential, PW_API_KEY, isn't one: the worker gives it to a
// step's process alone.
func IsFormatVariable(name string) bool { return formatVariables[name] }

// A StepResult is how a step ended.
type StepResult struct {
	// Outcome is the step's outcome as its job counts it: a step that
	// failed with ignore-errors is a success, one whose if: was false is
	// skipped, and one that was stopped, as early-cancel: stops one, is
	// cancelled, which cancels the rest of its job (JobState.Cancelled),
	// and one that was running when its job ran past its timeout fails. A
	// uses: step whose run was cancelled from within it while its job goes
	// on counts as a success, as format 1 goes on with the job after one
	// and cleans it up.
	Outcome Outcome
	// Outputs are what the step wrote to $OUTPUTS, by name, as written:
	// the job's later steps and its outputs: read them unmasked.
	Outputs map[string]string
	// CleanupOutcome and CleanupOutputs are how the step's cleanup: ended,
	// once it has run, and what it wrote to $OUTPUTS. A cleanup that failed
	// fails the job, and one that was cancelled cancels it, as a step does,
	// for the cleanups after it and End. Its outputs are the job's, over
	// its steps', as format 1 has them, merged in the order the cleanups
	// ran, the reverse of their steps'.
	CleanupOutcome Outcome
	CleanupOutputs map[string]string
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
	// IfFailed is set when the step's if: failed to evaluate, which fails
	// the step, with Step's errors: the step didn't run, as format 1 counts
	// it, which counts a step as run once its if: has passed, so its
	// cleanup: is skipped. Nothing below is set.
	IfFailed bool `json:"ifFailed,omitzero"`
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
	// worker's to lay over it (JobState.Env).
	Env map[string]string `json:"env,omitempty"`
	// WorkingDirectory is where the step runs: its working-directory:, or
	// its job's when it has none, or "" for the run directory, as a step's
	// working-directory: null asks.
	WorkingDirectory string `json:"workingDirectory,omitempty"`
	// SSH is the host a format 1 step's ssh: runs it on, outside any
	// container, as a job's is (Target.Inline), or nil without one. An
	// ssh: whose remoteHost is empty or null runs the step where it would
	// run without one, as format 1 does, which notes so in the step's log.
	SSH *Inline `json:"ssh,omitzero"`
	// NoSSH is set when the step's ssh: is null: format 1 runs it where
	// its job runs without its ssh:, not on the job's host
	// (Target.Inline).
	NoSSH bool `json:"noSSH,omitzero"`
	// TimeoutSeconds is the step's timeout:, in seconds, or 0 for none.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitzero"`
	// Retry is the step's retry:, with its defaults, or nil without one.
	Retry *ReadyStepRetry `json:"retry,omitzero"`
	// IgnoreErrors is the step's ignore-errors:: a non-zero exit doesn't
	// fail it.
	IgnoreErrors bool `json:"ignoreErrors,omitzero"`
	// EarlyCancel is the step's early-cancel:, any-job-failed or "".
	EarlyCancel string `json:"earlyCancel,omitempty"`
	// Masked are the strings Step made from a secret, as a Masker takes
	// them: each string that a template of the job's env: or the step's
	// that read one gave, such as an env value that reads one, and that
	// each ${{ }} in one that read one gave, such as a field of a
	// structured secret, but not the rest of a value, nor a boolean, a
	// number or null (Masker). Each is at least 8 bytes, or a credential
	// field of a secret's own text (credentialFields). They hold too the
	// outputs of the jobs it needs that hold a secret: those that hold a
	// secret's own text however short, and those only made from one as what
	// is made from a secret is (JobResult.Derived). The worker masks each
	// wherever it appears in the job's logs and outputs, as it masks the
	// secrets themselves.
	Masked []string `json:"masked,omitempty"`
	// OutlivesCancel is set when the step's if: can be true once its job is
	// stopping: it is the word always, or calls always() or cancelled(),
	// or negates success() or failure(). The worker runs such a step even
	// once the job is being cancelled, or has run past its timeout, and
	// lets it run its course, as format 1 does.
	OutlivesCancel bool `json:"outlivesCancel,omitzero"`
	// IfAlways is set when the step's if: is the word always: format 1
	// reads an output that nothing wrote as "" in the step, rather than
	// fail it (Contexts.StrictOutputs). A later try carries it over.
	IfAlways bool `json:"ifAlways,omitzero"`
}

// ReadyStepRetry is a step's retry: as Step evaluated it, which reruns the
// step on the same worker when it fails: its max-retries:, its interval:
// and timeout:, in seconds, and its clean-on-retry:, each with format 1's
// default: clean-on-retry: is true unless the workflow says otherwise.
type ReadyStepRetry struct {
	MaxRetries      int   `json:"maxRetries"`
	IntervalSeconds int64 `json:"intervalSeconds"`
	TimeoutSeconds  int64 `json:"timeoutSeconds"`
	CleanOnRetry    bool  `json:"cleanOnRetry,omitzero"`
}

// Step is the Step phase: it evaluates the step at index in the job, on
// the job's worker, as a try of the step is about to run. It reads the
// job's env: as JobEnv gave it (state.JobEnv), or evaluates it first as
// JobEnv does, and evaluates the job's working-directory: as the job
// starts, then the step's if:, with the status functions true or
// false as state.Steps and state.Cancelled say, and gives the step Skipped
// if it is false. Otherwise it evaluates the step's env: and then the rest
// of the step, and gives the step. The worker has Step evaluate the step
// again for each try, with the try in state.Try and state.Env: a later try
// doesn't evaluate the if: or retry:, but carries over what the first
// decided (state.FirstTry), and has no Retry, which the worker reads from
// the first, as format 1 does. A step's cleanup: is the Cleanup phase's.
//
// The step fails, for failure(), when one before it failed as its job
// counts it, and is cancelled, for cancelled(), when state.Cancelled says
// so or one before it was cancelled, as one early-cancel: stops is; its
// if: is true as a job's is (Job.Ready). In its expressions, needs holds
// the job itself too, whose outputs are those its steps without an id have
// written so far, the later step's where two wrote the same name, and
// whose steps are those with one, as format 1 has them: a step with an id
// writes outputs of its own. env holds the workflow's env:, the job's over
// it, the step's over that and the worker's variables over all
// (state.Env): only those format 1 defines (IsFormatVariable), and in the
// job's env: and working-directory: only those it defines as the job
// starts. A value of an env: that reads another of it reads that one's,
// in the order they read each other, as format 1 lays them; one that reads
// itself, or one that reads it, reads what is under it. A value that isn't
// text, where text goes, such as a run: or an env: value, is its text, as
// a template makes it. Its defaults are format 1's: a retry: tries 10 more
// times, 5 seconds apart, each try for at most 30 seconds.
//
// What reads an output that no step or job wrote fails, as format 1 reads
// one strictly (Contexts.StrictOutputs), but in an if:, and in a step whose
// if: is the word always, or in the job's values when the job's is
// (ReadyJob.IfAlways). A working directory that holds a shell variable,
// such as $HOME, fails the step (working_directory_variable), since no
// shell expands it.
//
// Step reaches nothing, and gives the same result for the same job, index
// and state, byte for byte. Its diagnostics are the step's problems, such
// as an expression that fails, at the step's path and with no file
// position, and never show a value made from a variable or a password; an
// error among them fails the step, and there is no step, but for one whose
// if: failed to evaluate, which Step gives with only its index, its id and
// IfFailed set. What they show is
// masked with state.Masked, and a value read from a step's output that
// holds a secret, which the job's later steps read as it was written, is
// masked whole. It returns an error when ctx is done before it finishes,
// ctx.Err(), and for an index that isn't one of the job's steps,
// state.Steps that aren't one for each step before it, or a later try
// without state.FirstTry, which are its caller's mistakes.
func (j *ReadyJob) Step(ctx context.Context, index int, state JobState) (*ReadyStep, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	switch {
	case index < 0 || index >= len(j.Steps):
		return nil, nil, fmt.Errorf("plan: the job %s has no step %d", j.Key, index)
	case len(state.Steps) != index:
		return nil, nil, fmt.Errorf("plan: step %d of the job %s is given the results of %d steps before it", index, j.Key, len(state.Steps))
	case state.Try > 0 && state.FirstTry == nil:
		return nil, nil, fmt.Errorf("plan: try %d of step %d of the job %s is given no first try", state.Try, index, j.Key)
	}
	return j.step(ctx, index, state, false)
}

// Cleanup is the phase of a step's cleanup:: it evaluates the cleanup: of
// the step at index in the job, on the job's worker, once the job's steps
// have run, as a step of its own that runs the cleanup: command, as format
// 1 runs a copy of the step: one that runs even once the job is being
// cancelled or past its timeout (OutlivesCancel), has no timeout or retry:
// of its own, and no early-cancel:, and is named POST and the step's name,
// or Step and its index when the name isn't text. It is skipped if the
// step didn't run, and if the step's if:, evaluated again with the job's
// status as it ends, is false, without success(): a step with no if: is
// cleaned up whatever happened to the job. The worker runs the job's
// cleanups in the reverse order of their steps, and what a cleanup writes
// to $OUTPUTS is the job's (StepResult.CleanupOutputs), as format 1 has
// them. It reads its job's contexts as Step does, with every step's
// outputs and those of the cleanups that ran before it, its own step's
// among them, and reads an output that nothing wrote as "", as format 1
// reads a cleanup:'s.
//
// Cleanup reaches nothing, and gives the same result for the same job,
// index and state, byte for byte. Its diagnostics are as Step's, and an
// error among them fails the cleanup. It returns an error when ctx is done
// before it finishes, ctx.Err(), and for an index that isn't one of the
// job's steps or one that has no cleanup:, or state.Steps that aren't one
// for each of the job's steps, which are its caller's mistakes.
func (j *ReadyJob) Cleanup(ctx context.Context, index int, state JobState) (*ReadyStep, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	switch {
	case index < 0 || index >= len(j.Steps):
		return nil, nil, fmt.Errorf("plan: the job %s has no step %d", j.Key, index)
	case j.Steps[index].Cleanup.IsZero():
		return nil, nil, fmt.Errorf("plan: step %d of the job %s has no cleanup", index, j.Key)
	case len(state.Steps) != len(j.Steps):
		return nil, nil, fmt.Errorf("plan: the job %s has %d steps, and is given the results of %d", j.Key, len(j.Steps), len(state.Steps))
	case state.Steps[index].Outcome == OutcomeSkipped:
		return &ReadyStep{Index: index, Skipped: true}, nil, nil
	}
	return j.step(ctx, index, state, true)
}

// step evaluates the step at index in the job for Step, or its cleanup:
// for Cleanup.
func (j *ReadyJob) step(ctx context.Context, index int, state JobState, cleanup bool) (*ReadyStep, problems.Diagnostics, error) {
	s := &j.Steps[index]
	c, e := j.jobEnv(state)
	if e.diags.HasErrors() {
		return nil, e.diags, nil
	}
	c.addSelf(j, state, e, false)
	e = c.evaluation(PhaseStep, e)
	jobDir := e.directory(j.WorkingDirectory, at(j.Path, "working-directory"))
	// The step reads all of the worker's variables, and its if: reads an
	// output that nothing wrote as "".
	c.WorkerEnv, c.StrictOutputs = c.stepVariables, false
	rs := &ReadyStep{Index: index}
	var d decision
	switch {
	case cleanup:
		// A cleanup evaluates its step's if: again, with the job's status as
		// it ends, as format 1 runs a copy of the step.
		e = c.evaluation(PhaseStep, e)
		var ok bool
		if d, ok = e.cleanupCondition(s.If, at(s.Path, "if")); !ok {
			return nil, e.diags, nil
		}
		if !d.run {
			rs.Skipped = true
			return rs, e.diags, nil
		}
		d.always, d.outlivesCancel = false, true
	case state.Try > 0:
		d = decision{run: true, always: state.FirstTry.IfAlways, outlivesCancel: state.FirstTry.OutlivesCancel}
	default:
		e = c.evaluation(PhaseStep, e)
		var ok bool
		if d, ok = e.condition(s.If, at(s.Path, "if")); !ok {
			return &ReadyStep{Index: index, ID: s.ID, IfFailed: true}, e.diags, nil
		}
		if !d.run {
			rs.ID, rs.Skipped = s.ID, true
			return rs, e.diags, nil
		}
	}
	rs.OutlivesCancel, rs.IfAlways = d.outlivesCancel, d.always && !cleanup
	c.StrictOutputs = !d.always && !cleanup
	c.StepEnv, c.stepMarks, e = c.layered(PhaseStep, s.Env, at(s.Path, "env"), e)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	e = c.evaluation(PhaseStep, e)
	rs.Env = c.env()
	rs.Name, _, _ = e.text(s.Name, at(s.Path, "name"))
	rs.Shell = s.Shell
	if cleanup {
		rs.Name = postName(s.Name, rs.Name, index)
		rs.Run, _, _ = e.text(s.Cleanup, at(s.Path, "cleanup"))
	} else {
		rs.ID, rs.Uses, rs.EarlyCancel = s.ID, s.Uses, s.EarlyCancel
		if !s.Run.IsZero() {
			rs.Run, _, _ = e.text(s.Run, at(s.Path, "run"))
		}
		if !s.With.IsZero() {
			pointer := at(s.Path, "with")
			if out, read, ok := e.value(s.With, pointer); ok {
				if rs.With, ok = mappingData(out); !ok {
					e.reportType(pointer, out, read, "object")
				}
			}
		}
	}
	dir := jobDir
	if stepDir := e.directory(s.WorkingDirectory, at(s.Path, "working-directory")); stepDir.null {
		dir = workingDirectory{}
	} else if stepDir.dir != "" {
		dir = stepDir
	}
	rs.WorkingDirectory = dir.dir
	e.checkDirectory(dir)
	if !s.SSH.IsZero() {
		pointer := at(s.Path, "ssh")
		if out, _, ok := e.value(s.SSH, pointer); ok && out == nil {
			rs.NoSSH = true
		} else if ok {
			rs.SSH = e.inline(out, pointer)
		}
	}
	if !cleanup {
		rs.TimeoutSeconds = e.duration(s.Timeout, at(s.Path, "timeout"), "timeout", 0)
	}
	if r := s.Retry; r != nil && state.Try == 0 && !cleanup {
		pointer := at(s.Path, "retry")
		rs.Retry = &ReadyStepRetry{
			MaxRetries:      e.count(r.MaxRetries, at(pointer, "max-retries"), "max-retries", defaultStepRetries),
			IntervalSeconds: e.retryDuration(r.Interval, at(pointer, "interval"), "interval", defaultStepInterval),
			TimeoutSeconds:  e.retryDuration(r.Timeout, at(pointer, "timeout"), "timeout", defaultStepTryTimeout),
			CleanOnRetry:    e.cleanOnRetry(r.CleanOnRetry, at(pointer, "clean-on-retry")),
		}
	}
	if !s.IgnoreErrors.IsZero() {
		if out, _, ok := e.value(s.IgnoreErrors, at(s.Path, "ignore-errors")); ok {
			rs.IgnoreErrors = readBool(out, false)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if e.diags.HasErrors() {
		return nil, e.diags, nil
	}
	rs.Masked = e.masked
	return rs, e.diags, nil
}

// postName returns the name of the step that runs the cleanup: of the step
// at index: POST and the step's name, evaluated, name, or Step and its
// index if the name it is written with isn't text or is empty, as format 1
// names its copy of the step.
func postName(written Value, name string, index int) string {
	text := false
	switch v := written.v.(type) {
	case string:
		text = v != ""
	case *expr.Template:
		text = v.String() != ""
	}
	if !text {
		return "POST Step " + strconv.Itoa(index)
	}
	return "POST " + name
}

// The defaults of a step's retry:, format 1's.
const (
	defaultStepRetries    = 10
	defaultStepInterval   = 5
	defaultStepTryTimeout = 30
)

// EndResult is what End gives of a job: its outputs, and which of them,
// and of its steps' outputs, are private, or hold a secret.
type EndResult struct {
	// Outputs are the job's outputs, by name, as format 1 has them: what
	// its steps without an id wrote, the later step's where two wrote the
	// same name, what its cleanups wrote over that, and its outputs: over
	// all, as they were made, none of them masked, as format 1 passes them
	// to the jobs that need the job. A step with an id writes outputs of
	// its own, which the worker gives as its steps wrote them
	// (JobResult.Steps).
	Outputs map[string]string
	// Private names the outputs that are private: made from an
	// organization or user variable, a password or a secret, or holding a
	// secret. The coordinator keeps their values only in the run's secret
	// record, never in run state, and gives them to the jobs that need the
	// job, which read them as private (JobResult.Private).
	Private []string
	// Secrets names those of them that hold a secret: made from one, or
	// holding one's text, as the job's logs show it. The jobs that need the
	// job read them as secrets (JobResult.Secrets): what they make of one is
	// masked in their logs and problems, and private. Derived names those of
	// them that hold no secret's own text, nor a credential field of one,
	// but only what was made from one, such as its length, which the jobs
	// that need the job mask as what is made from a secret, where they mask
	// the rest however short.
	Secrets []string
	Derived []string
	// SecretSteps names, by step id, the outputs of the job's steps that
	// have ids that hold a secret's text, which are private, and secrets to
	// the jobs that need the job (JobResult.SecretSteps), and DerivedSteps
	// those of them that hold only what was made from one, as Derived does.
	SecretSteps  map[string][]string
	DerivedSteps map[string][]string
}

// End is the End phase: it gives the job's outputs, on its worker, once the
// job's steps and their cleanups have run: what its steps without an id
// wrote, merged in their order, what its cleanups wrote over them, in the
// order they ran, and its outputs:, evaluated, over all, each as it was
// made, as format 1 gives them. An output made from a secret, or that
// holds a secret's text, as the job's logs show one but for a line of it,
// is private and a secret (EndResult.Secrets), and so is a step's output
// that holds one (EndResult.SecretSteps): the coordinator keeps them out
// of run state, and the jobs that need the job read them as secrets. One
// made from a variable or a password is private. Its expressions read the job's env: and needs, with the job
// itself, as Step's do, and steps, with each of the job's steps that has
// an id. They read an output that nothing wrote strictly once the job's
// steps succeeded (Contexts.StrictOutputs), as format 1 does.
//
// End reaches nothing, and gives the same outputs for the same job and
// state, byte for byte. Its diagnostics are as Step's, at the job's
// outputs:, and never show a secret: an error among them fails the job,
// and an output whose expression fails is left out. It returns an error
// when ctx is done before it finishes, ctx.Err(), and for state.Steps that
// aren't one for each of the job's steps.
func (j *ReadyJob) End(ctx context.Context, state JobState) (*EndResult, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(state.Steps) != len(j.Steps) {
		return nil, nil, fmt.Errorf("plan: the job %s has %d steps, and is given the results of %d", j.Key, len(j.Steps), len(state.Steps))
	}
	c, e := j.jobEnv(state)
	if e.diags.HasErrors() {
		return nil, e.diags, nil
	}
	c.addSelf(j, state, e, len(j.Outputs) > 0)
	// The job's outputs: read an output that nothing wrote strictly once
	// its steps succeeded, as format 1 does.
	c.StrictOutputs = !c.Status.Failed && !c.Status.Cancelled
	e = c.evaluation(PhaseEnd, e)
	out := &EndResult{Outputs: maps.Clone(c.Needs[j.Name].Outputs)}
	private, secret := map[string]bool{}, maps.Clone(c.secretOutputs)
	for name, v := range jsonvalue.Sorted(j.Outputs) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		pointer := at(j.Path, "outputs", name)
		value, read, err := v.EvalRead(e.scope)
		if err != nil {
			e.reportError(pointer, err, read)
			continue
		}
		text := expr.Text(value)
		if out.Outputs == nil {
			out.Outputs = map[string]string{}
		}
		out.Outputs[name] = text
		private[name] = read.Private
		secret[name] = text != "" && (read.Marked || c.leak.mask(text) != text)
	}
	// What holds a secret's own text, or a credential field of one, the
	// jobs that need the job mask however short; the rest only as what is
	// made from a secret.
	own := e.secrets.leakMasker()
	holdsOwn := func(v string) bool { return own.Mask(v) != v }
	for name, s := range jsonvalue.Sorted(secret) {
		if s {
			out.Secrets = append(out.Secrets, name)
			if !holdsOwn(out.Outputs[name]) {
				out.Derived = append(out.Derived, name)
			}
		}
	}
	for name := range jsonvalue.Sorted(out.Outputs) {
		if private[name] || secret[name] {
			out.Private = append(out.Private, name)
		}
	}
	for i := range j.Steps {
		if names := c.secretSteps[j.Steps[i].ID]; j.Steps[i].ID != "" && len(names) > 0 {
			id := j.Steps[i].ID
			if out.SecretSteps == nil {
				out.SecretSteps = map[string][]string{}
			}
			out.SecretSteps[id] = names
			for _, name := range names {
				if !holdsOwn(c.Steps[id][name]) {
					if out.DerivedSteps == nil {
						out.DerivedSteps = map[string][]string{}
					}
					out.DerivedSteps[id] = append(out.DerivedSteps[id], name)
				}
			}
		}
	}
	return out, e.diags, nil
}

// stepContexts are the contexts of a job's steps: the ready job's, and its
// env: and the step's, with which of their values are private or secret.
type stepContexts struct {
	Contexts
	// private and secrets are the ready job's Private and Secrets, and
	// jobMarks and stepMarks say how the values of the job's env: and the
	// step's were made, by name, for those made from a private value.
	private, secrets    []string
	jobMarks, stepMarks map[string]expr.Read
	// leaks are the paths of the job's steps' outputs that hold a secret,
	// as they were written, which every evaluation marks as a secret, so
	// that a problem with what reads one doesn't show it: secretSteps, by
	// step id, and secretOutputs, of the job's own, by name. leak is what
	// tells them, which End is given, to tell the outputs: that hold one.
	leaks         [][]string
	secretSteps   map[string][]string
	secretOutputs map[string]bool
	leak          finder
	// stepVariables are the worker's variables a step reads: those of
	// JobState.Env that format 1 defines.
	stepVariables map[string]string
	// shared are the needs and steps contexts of Needs and Steps, which
	// each evaluation's scope shares until they change (addSelf).
	shared sharedContexts
}

// JobEnv is the JobEnv phase: it evaluates the job's env:, on the job's
// worker, once, as the job starts, with the job context, and only the
// worker's variables that format 1 defines as a job starts
// (IsJobStartVariable), as format 1 does: what it gives is the same for
// each step, which reads it in state.JobEnv. Its values that read each
// other are evaluated in the order they read each other (Step). It
// reaches nothing, and gives the same result for the same job and state,
// byte for byte. Its diagnostics are the problems of the job's env:, as
// Step's are; an error among them fails the job, and there is no result.
// It returns an error when ctx is done before it finishes, ctx.Err().
func (j *ReadyJob) JobEnv(ctx context.Context, state JobState) (*JobEnv, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	state.JobEnv = nil
	c, e := j.jobEnv(state)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if e.diags.HasErrors() {
		return nil, e.diags, nil
	}
	out := &JobEnv{Env: c.JobEnv, Masked: e.masked, own: e.secrets}
	for name, read := range jsonvalue.Sorted(c.jobMarks) {
		if read.Private {
			out.Private = append(out.Private, name)
		}
		if read.Marked {
			out.Secrets = append(out.Secrets, name)
		}
	}
	return out, e.diags, nil
}

// jobEnv gives the job's env:, with the job context, as the job starts:
// state.JobEnv, or else evaluated as JobEnv does, and returns the contexts
// of its steps, without needs or steps, and the evaluation, which holds
// its problems and the text of what it made from a secret. The job fails,
// for failure(), when a step of it, or a cleanup that has run, failed, and
// is cancelled, for cancelled(), when state.Cancelled says so or a step
// or a cleanup of it was cancelled, as one early-cancel: stops is.
func (j *ReadyJob) jobEnv(state JobState) (*stepContexts, *evaluation) {
	c := &stepContexts{
		Run: state.Run, Needs: state.Needs, Status: expr.Status{Cancelled: state.Cancelled},
		Matrix: j.Matrix, Job: state.Job, Private: j.Private, Secrets: j.Secrets,
		WorkerEnv: variables(state.Env, IsJobStartVariable), StrictOutputs: !j.IfAlways,
		private: j.Private, secrets: j.Secrets, stepVariables: variables(state.Env, IsFormatVariable),
	}
	e := newEvaluation(PhaseJobEnv, c.Contexts)
	e.secrets.other = state.Secrets
	if given := state.JobEnv; given != nil && given.own != nil && given.own.sameAs(state.Run, state.Secrets, state.Needs) {
		// The secrets' texts JobEnv found for this attempt, and what masks
		// them, which it built once; not those of another run or attempt.
		e.secrets = given.own
	}
	// The job's Masker extends one of the secrets' own texts, which an
	// attempt builds once.
	e.masker = e.secrets.leakMasker().extend(true)
	e.masker.Add(state.Masked...)
	if given := state.JobEnv; given != nil {
		e.masker.Add(given.Masked...)
		c.JobEnv = maps.Clone(given.Env)
		c.jobMarks = map[string]expr.Read{}
		for _, name := range given.Private {
			c.jobMarks[name] = expr.Read{Private: true}
		}
		for _, name := range given.Secrets {
			c.jobMarks[name] = expr.Read{Private: true, Marked: true}
		}
		for _, s := range given.Masked {
			e.maskOwn(s)
		}
	} else {
		for _, s := range e.secrets.needsMasked() {
			e.maskOwn(s)
		}
		// The worker masks what it was given whole; JobEnv gives it the
		// credential fields of it too.
		for _, s := range e.secrets.otherMasked() {
			e.maskOwn(s)
		}
		c.JobEnv, c.jobMarks, e = c.layered(PhaseJobEnv, j.Env, at(j.Path, "env"), e)
	}
	for _, r := range state.Steps {
		c.Status.Failed = c.Status.Failed || r.Outcome == OutcomeFailure || r.CleanupOutcome == OutcomeFailure
		c.Status.Cancelled = c.Status.Cancelled || r.Outcome == OutcomeCancelled || r.CleanupOutcome == OutcomeCancelled
	}
	return c, e
}

// variables returns the variables of env that keep says to.
func variables(env map[string]string, keep func(string) bool) map[string]string {
	out := map[string]string{}
	for name, v := range env {
		if keep(name) {
			out[name] = v
		}
	}
	return out
}

// evaluation returns an evaluation in phase of what c holds now, which
// keeps the problems and the text made from a secret of the one before
// it, e.
func (c *stepContexts) evaluation(phase Phase, e *evaluation) *evaluation {
	marks := maps.Clone(c.jobMarks)
	if marks == nil {
		marks = map[string]expr.Read{}
	}
	for name := range c.StepEnv {
		marks[name] = c.stepMarks[name]
	}
	c.Private, c.Secrets = slices.Clone(c.private), slices.Clone(c.secrets)
	for name, read := range jsonvalue.Sorted(marks) {
		if read.Private {
			c.Private = append(c.Private, "env."+name)
		}
		if read.Marked {
			c.Secrets = append(c.Secrets, "env."+name)
		}
	}
	next := &evaluation{
		phase: phase, scope: newScope(phase, c.Contexts, &c.shared), diags: e.diags, masked: e.masked, seen: e.seen, masker: e.masker, secrets: e.secrets,
	}
	for _, path := range c.leaks {
		next.scope.Marked.Add(path)
		next.scope.Private.Add(path)
	}
	return next
}

// layered evaluates an env:, env, at pointer, in phase, after e, as
// format 1 lays it: each value reads the others it reads once they are
// evaluated, in the order they read each other, over what env: is laid on:
// the job's in JobEnv, with JobEnv the values so far, and the step's in
// Step, with StepEnv. A value that reads itself, and the values that read
// each other, read what is under env: for each other, where format 1
// recursed until it failed. It returns the values, how those made from a
// private value were made, and the evaluation that evaluated the last of
// them.
func (c *stepContexts) layered(phase Phase, env map[string]Value, pointer string, e *evaluation) (map[string]string, map[string]expr.Read, *evaluation) {
	values, marks := map[string]string{}, map[string]expr.Read{}
	for _, names := range readOrder(env) {
		if phase == PhaseJobEnv {
			c.JobEnv, c.jobMarks = values, marks
		} else {
			c.StepEnv, c.stepMarks = values, marks
		}
		e = c.evaluation(phase, e)
		group := make(map[string]Value, len(names))
		for _, name := range names {
			group[name] = env[name]
		}
		more, moreMarks := e.env(group, pointer)
		values, marks = maps.Clone(values), maps.Clone(marks)
		maps.Copy(values, more)
		maps.Copy(marks, moreMarks)
	}
	if len(values) == 0 {
		values = nil
	}
	if len(marks) == 0 {
		marks = nil
	}
	return values, marks, e
}

// readOrder returns the names of env in groups to evaluate in order: each
// group's values read only those of the groups before it, but for those of
// a group that read each other, which is evaluated as one. A value that
// reads env whole, as env.* does, reads each of the others. The groups are
// the strongly connected components of what reads what, in an order of
// Tarjan's that puts what is read first, with names in sorted order.
func readOrder(env map[string]Value) [][]string {
	names := slices.Sorted(maps.Keys(env))
	reads := make(map[string][]string, len(env))
	for _, name := range names {
		for _, x := range env[name].Exprs() {
			for _, r := range x.Refs() {
				if r.Context != "env" {
					continue
				}
				if len(r.Path) == 0 || r.Path[0] == "*" || r.Path[0] == "[index]" {
					reads[name] = names
					break
				}
				if _, ok := env[r.Path[0]]; ok {
					reads[name] = append(reads[name], r.Path[0])
				}
			}
		}
		slices.Sort(reads[name])
		reads[name] = slices.Compact(reads[name])
	}
	var (
		order   [][]string
		stack   []string
		index   = map[string]int{}
		low     = map[string]int{}
		onStack = map[string]bool{}
		visit   func(name string)
	)
	visit = func(name string) {
		index[name], low[name] = len(index), len(index)
		stack, onStack[name] = append(stack, name), true
		for _, next := range reads[name] {
			if _, seen := index[next]; !seen {
				visit(next)
				low[name] = min(low[name], low[next])
			} else if onStack[next] {
				low[name] = min(low[name], index[next])
			}
		}
		if low[name] != index[name] {
			return
		}
		var group []string
		for {
			top := stack[len(stack)-1]
			stack, onStack[top] = stack[:len(stack)-1], false
			group = append(group, top)
			if top == name {
				break
			}
		}
		slices.Sort(group)
		order = append(order, group)
	}
	for _, name := range names {
		if _, seen := index[name]; !seen {
			visit(name)
		}
	}
	return order
}

// env returns the environment c gives a step: the workflow's env:, then
// the job's over it, then the step's over that.
func (c *stepContexts) env() map[string]string {
	env := maps.Clone(c.Run.Env)
	for _, layer := range []map[string]string{c.JobEnv, c.StepEnv} {
		if len(layer) > 0 && env == nil {
			env = map[string]string{}
		}
		maps.Copy(env, layer)
	}
	return env
}

// env evaluates an env:, at pointer, each value as a group of its own, and
// returns its values' text, and how each was made, if it was made from a
// private value.
func (e *evaluation) env(env map[string]Value, pointer string) (map[string]string, map[string]expr.Read) {
	if len(env) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(env))
	var marks map[string]expr.Read
	for name, v := range jsonvalue.Sorted(env) {
		text, read, ok := e.text(v, at(pointer, name))
		if !ok {
			continue
		}
		out[name] = text
		if read.Private {
			if marks == nil {
				marks = map[string]expr.Read{}
			}
			marks[name] = read
		}
	}
	return out, marks
}

// addSelf gives c the needs of the job's steps, with the job itself, and
// its steps' outputs by id, from what its steps so far, state.Steps, and
// the cleanups that have run wrote, and notes the outputs that hold a
// secret: the job's secrets and the outputs of the jobs it needs that hold
// one (secretTexts), what the worker masks (state.Masked), and what the
// evaluation, e, made from one so far, as each can show, but for its
// lines, which plain text can hold too, and, for End, if end is set, gives
// c what tells them (leak). The phases of an attempt share what they find
// of each output (outputChecks), so that one reads an output again only
// for what was added to mask since another read it.
func (c *stepContexts) addSelf(j *ReadyJob, state JobState, e *evaluation, end bool) {
	needs := maps.Clone(state.Needs)
	if needs == nil {
		needs = map[string]JobResult{}
	}
	checks := &e.secrets.checks
	checks.mu.Lock()
	defer checks.mu.Unlock()
	// What the evaluation made from a secret is the same for each phase of
	// an attempt, and the worker adds to what it masks.
	checks.use(e.secrets, slices.Concat(e.masked, state.Masked))
	self := JobResult{Outputs: map[string]string{}}
	selfLeaks := map[string]bool{}
	steps := map[string]map[string]string{}
	stepsCtx := map[string]any{}
	for i, r := range state.Steps {
		id := j.Steps[i].ID
		if id != "" {
			// The phases only read what the step wrote.
			outputs := r.Outputs
			if outputs == nil {
				outputs = map[string]string{}
			}
			steps[id], stepsCtx[id] = outputs, checks.stepContext(i, r.Outputs)
		}
		var leaked []string
		for name, v := range r.Outputs {
			holds := checks.leaked(outputKey{step: i, name: name}, v)
			switch {
			case id == "":
				// A step with an id writes outputs of its own, and only one
				// without one writes the job's.
				self.Outputs[name], selfLeaks[name] = v, holds
			case holds:
				leaked = append(leaked, name)
			}
		}
		slices.Sort(leaked)
		for _, name := range leaked {
			c.leaks = append(c.leaks, []string{"steps", id, "outputs", name}, []string{"needs", j.Name, "steps", id, "outputs", name})
			if c.secretSteps == nil {
				c.secretSteps = map[string][]string{}
			}
			c.secretSteps[id] = append(c.secretSteps[id], name)
		}
	}
	// The cleanups ran in the reverse order of their steps, and write the
	// job's outputs, whatever their step's id.
	for i, r := range slices.Backward(state.Steps) {
		for name, v := range r.CleanupOutputs {
			self.Outputs[name], selfLeaks[name] = v, checks.leaked(outputKey{step: i, cleanup: true, name: name}, v)
		}
	}
	c.secretOutputs = map[string]bool{}
	for name, leaked := range jsonvalue.Sorted(selfLeaks) {
		if leaked {
			c.leaks = append(c.leaks, []string{"needs", j.Name, "outputs", name})
			c.secretOutputs[name] = true
		}
	}
	if end {
		c.leak = checks.finder()
	}
	self.Steps = steps
	needs[j.Name] = self
	c.Needs, c.Steps = needs, steps
	// Each scope of the phase reads the job's steps' outputs, in steps and
	// in needs alike.
	needsCtx := needsContext(state.Needs)
	needsCtx[j.Name] = jobContext(self, stepsCtx)
	c.shared = sharedContexts{needs: needsCtx, steps: stepsCtx}
}
