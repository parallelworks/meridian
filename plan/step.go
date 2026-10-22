package plan

import (
	"context"
	"fmt"
	"maps"
	"slices"

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
// The step fails, for failure(), when one before it failed as its job
// counts it, and is cancelled, for cancelled(), when state.Cancelled says
// so; its if: is true as a job's is (Job.Ready). In its expressions, needs
// holds the job itself too, whose outputs are those its steps have written
// so far, the later step's where two wrote the same, and whose steps are
// its steps' that have ids. A value that isn't text, where text goes, such
// as a run: or an env: value, is its text, as a template makes it. Its
// defaults are format 1's: a retry: tries 10 more times, 5 seconds apart,
// each try for at most 30 seconds.
//
// Step reaches nothing, and gives the same result for the same job, index
// and state, byte for byte. Its diagnostics are the step's problems, such
// as an expression that fails, at the step's path and with no file
// position, and never show a value made from a variable or a password; an
// error among them fails the step, and there is no step. What they show is
// masked with state.Masked, as a step's outputs, which the job's later
// steps read as they were written, can hold a secret. It returns an
// error when ctx is done before it finishes, ctx.Err(), and for an index
// that isn't one of the job's steps, or state.Steps that aren't one for
// each step before it, which are its caller's mistakes.
func (j *ReadyJob) Step(ctx context.Context, index int, state JobState) (*ReadyStep, problems.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if index < 0 || index >= len(j.Steps) {
		return nil, nil, fmt.Errorf("plan: the job %s has no step %d", j.Key, index)
	}
	if len(state.Steps) != index {
		return nil, nil, fmt.Errorf("plan: step %d of the job %s is given the results of %d steps before it", index, j.Key, len(state.Steps))
	}
	s := &j.Steps[index]
	c, e := j.jobEnv(state)
	if e.diags.HasErrors() {
		return nil, e.diags, nil
	}
	c.Needs, c.Steps = j.withSelf(state)
	e = c.evaluation(PhaseStep, e)
	rs := &ReadyStep{Index: index, ID: s.ID}
	run, ok := e.condition(s.If, at(s.Path, "if"))
	switch {
	case !ok:
		return nil, e.diags, nil
	case !run:
		rs.Skipped = true
		return rs, e.diags, nil
	}
	// The step's env: is evaluated over the job's, and the rest of the
	// step over the step's.
	c.StepEnv, c.stepMarks = e.env(s.Env, at(s.Path, "env"))
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	e = c.evaluation(PhaseStep, e)
	rs.Env = c.env()
	rs.Name, _, _ = e.text(s.Name, at(s.Path, "name"))
	if !s.Run.IsZero() {
		rs.Run, _, _ = e.text(s.Run, at(s.Path, "run"))
	}
	rs.Uses, rs.Shell, rs.EarlyCancel = s.Uses, s.Shell, s.EarlyCancel
	if !s.With.IsZero() {
		pointer := at(s.Path, "with")
		if out, read, ok := e.value(s.With, pointer); ok {
			if rs.With, ok = mappingData(out); !ok {
				e.reportType(pointer, out, read, "object")
			}
		}
	}
	rs.WorkingDirectory = j.WorkingDirectory
	if dir, null := e.directory(s.WorkingDirectory, at(s.Path, "working-directory")); null {
		rs.WorkingDirectory = ""
	} else if dir != "" {
		rs.WorkingDirectory = dir
	}
	if !s.SSH.IsZero() {
		rs.SSH = e.inline(s.SSH, at(s.Path, "ssh"))
	}
	rs.TimeoutSeconds = e.duration(s.Timeout, at(s.Path, "timeout"), "timeout", 0)
	if r := s.Retry; r != nil {
		pointer := at(s.Path, "retry")
		rs.Retry = &ReadyStepRetry{
			MaxRetries:      e.count(r.MaxRetries, at(pointer, "max-retries"), "max-retries", defaultStepRetries),
			IntervalSeconds: e.duration(r.Interval, at(pointer, "interval"), "interval", defaultStepInterval),
			TimeoutSeconds:  e.duration(r.Timeout, at(pointer, "timeout"), "timeout", defaultStepTryTimeout),
			CleanOnRetry:    r.CleanOnRetry,
		}
	}
	if !s.IgnoreErrors.IsZero() {
		if out, _, ok := e.value(s.IgnoreErrors, at(s.Path, "ignore-errors")); ok {
			rs.IgnoreErrors = readBool(out, false)
		}
	}
	if !s.Cleanup.IsZero() {
		rs.Cleanup, _, _ = e.text(s.Cleanup, at(s.Path, "cleanup"))
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

// The defaults of a step's retry:, format 1's.
const (
	defaultStepRetries    = 10
	defaultStepInterval   = 5
	defaultStepTryTimeout = 30
)

// End is the End phase: it evaluates the job's outputs:, on its worker,
// once the job's steps have run, and gives them by name. It masks each
// with a Masker of state.Masked, and of what it made from a secret, such
// as a value of the job's env:, and masks the whole of one that was made
// from a secret: an output that holds a secret is given with it masked,
// problems.Masked, and a warning that names the output (output_masked).
// Its expressions read the job's env: and needs, with the job itself, as
// Step's do, and steps, with each of the job's steps that has an id.
//
// End reaches nothing, and gives the same outputs for the same job and
// state, byte for byte. Its diagnostics are as Step's, at the job's
// outputs:; an error among them fails the job, and an output whose
// expression fails is left out. It returns an error when ctx is done
// before it finishes, ctx.Err(), and for state.Steps that aren't one for
// each of the job's steps.
func (j *ReadyJob) End(ctx context.Context, state JobState) (map[string]string, problems.Diagnostics, error) {
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
	c.Needs, c.Steps = j.withSelf(state)
	e = c.evaluation(PhaseEnd, e)
	var outputs map[string]string
	for name, v := range jsonvalue.Sorted(j.Outputs) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		pointer := at(j.Path, "outputs", name)
		text, read, ok := e.text(v, pointer)
		if !ok {
			continue
		}
		masked := e.masker.Mask(text)
		if read.Marked && text != "" {
			masked = problems.Masked
		}
		if masked != text {
			e.report(problems.OutputMasked, pointer, problems.Params{"name": name}, expr.Read{})
		}
		if outputs == nil {
			outputs = map[string]string{}
		}
		outputs[name] = masked
	}
	return outputs, e.diags, nil
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
}

// jobEnv evaluates the job's env:, with the job context, and returns the
// contexts of its steps, without needs or steps, and the evaluation, which
// holds its problems and the text of what it made from a secret.
func (j *ReadyJob) jobEnv(state JobState) (*stepContexts, *evaluation) {
	c := &stepContexts{
		Run: state.Run, Needs: state.Needs, Status: expr.Status{Cancelled: state.Cancelled},
		Matrix: j.Matrix, Job: state.Job, Private: j.Private, Secrets: j.Secrets,
		private: j.Private, secrets: j.Secrets,
	}
	e := newEvaluation(PhaseJobEnv, c.Contexts)
	e.masker = NewMasker(state.Masked...)
	c.JobEnv, c.jobMarks = e.env(j.Env, at(j.Path, "env"))
	for _, r := range state.Steps {
		c.Status.Failed = c.Status.Failed || r.Outcome == OutcomeFailure
	}
	return c, e
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
	next := newEvaluation(phase, c.Contexts)
	next.diags, next.masked, next.seen, next.masker = e.diags, e.masked, e.seen, e.masker
	return next
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

// withSelf returns the needs of the job's steps, with the job itself, and
// its steps' outputs by id, from what its steps so far, state.Steps, wrote.
func (j *ReadyJob) withSelf(state JobState) (map[string]JobResult, map[string]map[string]string) {
	needs := maps.Clone(state.Needs)
	if needs == nil {
		needs = map[string]JobResult{}
	}
	self := JobResult{Outputs: map[string]string{}}
	steps := map[string]map[string]string{}
	for i, r := range state.Steps {
		maps.Copy(self.Outputs, r.Outputs)
		if id := j.Steps[i].ID; id != "" {
			outputs := maps.Clone(r.Outputs)
			if outputs == nil {
				outputs = map[string]string{}
			}
			steps[id] = outputs
		}
	}
	self.Steps = steps
	needs[j.Name] = self
	return needs, steps
}
