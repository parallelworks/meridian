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
	// Try is the step's try, from 0, for Step: the worker has Step evaluate
	// the step again for each try, as format 1 does, so that what reads
	// env.PW_WORKFLOW_STEP_CURRENT_RETRY reads the try's.
	Try int
	// Env holds the variables the worker defines for the step, or for the
	// job's env: and outputs:, such as PW_JOB_DIR, PW_PARENT_JOB_DIR,
	// OUTPUTS and, for a try, PW_WORKFLOW_STEP_CURRENT_RETRY, which env
	// reads over the workflow's, the job's and the step's, as format 1 lays
	// them. It holds no credential, such as the platform's API key that
	// format 1 gives a step's environment as PW_API_KEY: no workflow we
	// know of reads one in an expression, so no expression reads one, and
	// the worker gives it only to the step's process.
	Env map[string]string
	// Masked are the values the worker masks, as a Masker takes them: the
	// run's secrets' (RunValues.Masked), the job's (ReadyJob.Masked), and
	// those of each step so far (ReadyStep.Masked). End masks the job's
	// outputs with them, as the worker masks its logs and its steps'
	// outputs.
	Masked []string
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

// IsFormatVariable reports whether name is one of the variables format 1
// gives a step, which env reads (JobState.Env), such as PW_JOB_DIR and
// OUTPUTS. Its credential, PW_API_KEY, isn't one: the worker gives it to a
// step's process alone.
func IsFormatVariable(name string) bool { return formatVariables[name] }

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
	// Cleanup is the step's cleanup: command, which runs at the end of its
	// job if the step ran, or "".
	Cleanup string `json:"cleanup,omitempty"`
	// Masked are the strings Step made from a secret, as a Masker takes
	// them: each string that a template of the job's env: or the step's
	// that read one gave, such as an env value that reads one, but not the
	// rest of a value, nor a boolean, a number or null (Masker). The
	// worker masks each wherever it appears in the job's logs and outputs,
	// as it masks the secrets themselves.
	Masked []string `json:"masked,omitempty"`
	// OutlivesCancel is set when the step's if: can be true once its job is
	// stopping: it is the word always, or calls always() or cancelled(),
	// or negates success() or failure(). The worker runs such a step even
	// once the job is being cancelled, or has run past its timeout, and
	// lets it run its course, as format 1 does.
	OutlivesCancel bool `json:"outlivesCancel,omitzero"`
}

// ReadyStepRetry is a step's retry: as Step evaluated it, which reruns the
// step on the same worker when it fails: its max-retries:, its interval:
// and timeout:, in seconds, with format 1's defaults, and its
// clean-on-retry: as the plan holds it.
type ReadyStepRetry struct {
	MaxRetries      int   `json:"maxRetries"`
	IntervalSeconds int64 `json:"intervalSeconds"`
	TimeoutSeconds  int64 `json:"timeoutSeconds"`
	CleanOnRetry    bool  `json:"cleanOnRetry,omitzero"`
}

// Step is the Step phase: it evaluates the step at index in the job, on
// the job's worker, as a try of the step is about to run. It evaluates the
// job's env: first, with the job context, and the job's working-directory:,
// then the step's if:, with the status functions true or false as
// state.Steps and state.Cancelled say, and gives the step Skipped if it is
// false at its first try. Otherwise it evaluates the step's env: and then
// the rest of the step, and gives the step. The worker has Step evaluate
// the step again for each try, with the try in state.Try and state.Env:
// a later try evaluates its if: too, but runs whatever it gives, and has no
// Retry, which the worker reads from the first.
//
// The step fails, for failure(), when one before it failed as its job
// counts it, and is cancelled, for cancelled(), when state.Cancelled says
// so; its if: is true as a job's is (Job.Ready). In its expressions, needs
// holds the job itself too, whose outputs are those its steps without an
// id have written so far, the later step's where two wrote the same name,
// and whose steps are those with one, as format 1 has them: a step with an
// id writes outputs of its own. env holds the workflow's env:, the job's
// over it, the step's over that and the worker's variables over all
// (state.Env). A value of the step's env: that reads env reads the step's
// values that don't, as format 1 lays them. A value that isn't text, where
// text goes, such as a run: or an env: value, is its text, as a template
// makes it. Its defaults are format 1's: a retry: tries 10 more times, 5
// seconds apart, each try for at most 30 seconds.
//
// What reads an output that no step or job wrote fails, as format 1 reads
// one strictly (Contexts.StrictOutputs), but in an if:, and in the step of
// one whose if: is the word always, or in the job's values when the job's
// is (ReadyJob.IfAlways). A working directory that holds a shell variable,
// such as $HOME, fails the step (working_directory_variable), since no
// shell expands it.
//
// Step reaches nothing, and gives the same result for the same job, index
// and state, byte for byte. Its diagnostics are the step's problems, such
// as an expression that fails, at the step's path and with no file
// position, and never show a value made from a variable or a password; an
// error among them fails the step, and there is no step. What they show is
// masked with state.Masked, and a value read from a step's output that
// holds a secret, which the job's later steps read as it was written, is
// masked whole. It returns an error when ctx is done before it finishes,
// ctx.Err(), and for an index that isn't one of the job's steps, or
// state.Steps that aren't one for each step before it, which are its
// caller's mistakes.
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
	c.addSelf(j, state, e.masker)
	e = c.evaluation(PhaseStep, e)
	jobDir := e.directory(j.WorkingDirectory, at(j.Path, "working-directory"))
	rs := &ReadyStep{Index: index, ID: s.ID}
	// An if: reads an output that nothing wrote as "".
	c.StrictOutputs = false
	e = c.evaluation(PhaseStep, e)
	d, ok := e.condition(s.If, at(s.Path, "if"))
	switch {
	case !ok:
		return nil, e.diags, nil
	case !d.run && state.Try == 0:
		rs.Skipped = true
		return rs, e.diags, nil
	}
	rs.OutlivesCancel = d.outlivesCancel
	c.StrictOutputs = !d.always
	c.StepEnv, c.stepMarks, e = c.layered(PhaseStep, s.Env, at(s.Path, "env"), e)
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
	rs.TimeoutSeconds = e.duration(s.Timeout, at(s.Path, "timeout"), "timeout", 0)
	if r := s.Retry; r != nil && state.Try == 0 {
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

// EndResult is what End gives of a job: its outputs, and which of them are
// private.
type EndResult struct {
	// Outputs are the job's outputs, by name, as format 1 has them: what
	// its steps without an id wrote, the later step's where two wrote the
	// same name, and its outputs: over that, each masked. A step with an id
	// writes outputs of its own, which the worker gives as its own
	// (JobResult.Steps).
	Outputs map[string]string
	// Private names the outputs: made from an organization or user variable
	// or a password, and not from a secret, which is masked whole. The
	// coordinator keeps their values only in the run's secret record, and
	// gives them to the jobs that need the job, which read them as private
	// (JobResult.Private).
	Private []string
}

// End is the End phase: it gives the job's outputs, on its worker, once the
// job's steps have run: what its steps without an id wrote, merged in
// their order, and its outputs:, evaluated, over them. It masks each with a Masker of
// state.Masked, and of what it made from a secret, such as a value of the
// job's env:, and masks the whole of an output: that was made from a
// secret, with a warning that names it (output_masked), and one when a
// value of its holds a secret. What one of its outputs: is made of masks
// none of the others. A step's output that held a secret is masked too,
// and its warning is the worker's, which reports the step's outputs. Its
// expressions read the job's env: and needs, with the job itself, as
// Step's do, and steps, with each of the job's steps that has an id. They
// read an output that nothing wrote strictly once the job's steps
// succeeded (Contexts.StrictOutputs), as format 1 does.
//
// End reaches nothing, and gives the same outputs for the same job and
// state, byte for byte. Its diagnostics are as Step's, at the job's
// outputs:; an error among them fails the job, and an output whose
// expression fails is left out. It returns an error when ctx is done
// before it finishes, ctx.Err(), and for state.Steps that aren't one for
// each of the job's steps.
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
	c.addSelf(j, state, e.masker)
	// The job's outputs: read an output that nothing wrote strictly once
	// its steps succeeded, as format 1 does.
	c.StrictOutputs = !c.Status.Failed && !c.Status.Cancelled
	e = c.evaluation(PhaseEnd, e)
	out := &EndResult{}
	set := func(name, v string) {
		if out.Outputs == nil {
			out.Outputs = map[string]string{}
		}
		out.Outputs[name] = v
	}
	for name, v := range jsonvalue.Sorted(c.Needs[j.Name].Outputs) {
		set(name, e.masker.Mask(v))
	}
	for name, v := range jsonvalue.Sorted(j.Outputs) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		// An output's value is evaluated without noting what it is made of
		// to mask, so that it masks none of the others.
		pointer := at(j.Path, "outputs", name)
		value, read, err := v.EvalRead(e.scope)
		if err != nil {
			e.reportError(pointer, err, read)
			continue
		}
		text := expr.Text(value)
		masked := e.masker.Mask(text)
		if read.Marked && text != "" {
			masked = problems.Masked
		}
		if masked != text {
			e.report(problems.OutputMasked, pointer, problems.Params{"name": name}, expr.Read{})
		}
		if read.Private && !read.Marked {
			out.Private = append(out.Private, name)
		}
		set(name, masked)
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
	// that a problem with what reads one doesn't show it.
	leaks [][]string
}

// jobEnv evaluates the job's env:, with the job context, and returns the
// contexts of its steps, without needs or steps, and the evaluation, which
// holds its problems and the text of what it made from a secret.
func (j *ReadyJob) jobEnv(state JobState) (*stepContexts, *evaluation) {
	c := &stepContexts{
		Run: state.Run, Needs: state.Needs, Status: expr.Status{Cancelled: state.Cancelled},
		Matrix: j.Matrix, Job: state.Job, Private: j.Private, Secrets: j.Secrets, WorkerEnv: state.Env,
		StrictOutputs: !j.IfAlways,
		private:       j.Private, secrets: j.Secrets,
	}
	e := newEvaluation(PhaseJobEnv, c.Contexts)
	e.masker = NewMasker(state.Masked...)
	c.JobEnv, c.jobMarks, e = c.layered(PhaseJobEnv, j.Env, at(j.Path, "env"), e)
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
	for _, path := range c.leaks {
		next.scope.Marked.Add(path)
		next.scope.Private.Add(path)
	}
	return next
}

// layered evaluates an env:, env, at pointer, in phase, after e: first
// each of its values that doesn't read env, and then each that does, over
// those, as format 1 lays them: the job's in JobEnv, with JobEnv the
// values so far, and the step's in Step, with StepEnv. It returns the
// values, how those made from a private value were made, and the
// evaluation that evaluated the last of them.
func (c *stepContexts) layered(phase Phase, env map[string]Value, pointer string, e *evaluation) (map[string]string, map[string]expr.Read, *evaluation) {
	first, later := map[string]Value{}, map[string]Value{}
	for name, v := range env {
		if readsContext(v, "env") {
			later[name] = v
		} else {
			first[name] = v
		}
	}
	values, marks := e.env(first, pointer)
	if len(later) == 0 {
		return values, marks, e
	}
	if phase == PhaseJobEnv {
		c.JobEnv, c.jobMarks = values, marks
	} else {
		c.StepEnv, c.stepMarks = values, marks
	}
	e = c.evaluation(phase, e)
	more, moreMarks := e.env(later, pointer)
	if values == nil {
		values = map[string]string{}
	}
	maps.Copy(values, more)
	if len(moreMarks) > 0 && marks == nil {
		marks = map[string]expr.Read{}
	}
	maps.Copy(marks, moreMarks)
	return values, marks, e
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
// its steps' outputs by id, from what its steps so far, state.Steps, wrote,
// and notes the outputs that hold a secret masker masks.
func (c *stepContexts) addSelf(j *ReadyJob, state JobState, masker *Masker) {
	needs := maps.Clone(state.Needs)
	if needs == nil {
		needs = map[string]JobResult{}
	}
	self := JobResult{Outputs: map[string]string{}}
	steps := map[string]map[string]string{}
	leaks := func(outputs map[string]string, path ...string) {
		for name, v := range jsonvalue.Sorted(outputs) {
			if masker.Mask(v) != v {
				c.leaks = append(c.leaks, append(slices.Clone(path), name))
			}
		}
	}
	for i, r := range state.Steps {
		if j.Steps[i].ID == "" {
			// A step with an id writes outputs of its own, and only one
			// without one writes the job's.
			maps.Copy(self.Outputs, r.Outputs)
		}
		if id := j.Steps[i].ID; id != "" {
			outputs := maps.Clone(r.Outputs)
			if outputs == nil {
				outputs = map[string]string{}
			}
			steps[id] = outputs
			leaks(outputs, "steps", id, "outputs")
			leaks(outputs, "needs", j.Name, "steps", id, "outputs")
		}
	}
	leaks(self.Outputs, "needs", j.Name, "outputs")
	self.Steps = steps
	needs[j.Name] = self
	c.Needs, c.Steps = needs, steps
}
