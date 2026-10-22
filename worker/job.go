package worker

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// execute runs the attempt's job: it sets it up, runs its steps and their
// cleanup: commands, and evaluates its outputs. It returns the attempt's
// end, or nil if the job was halted.
func (a *attempt) execute(ctx context.Context) *workerapi.Complete {
	a.masked = a.ph.masked(a.claim.Run)
	for _, name := range slices.Sorted(maps.Keys(a.claim.Env)) {
		if v := a.claim.Env[name]; v.Secret {
			a.masked = append(a.masked, v.Value)
		}
	}
	a.mask = a.ph.masker(a.masked...)
	problem := a.setUp()
	if a.halted.fired() {
		return nil
	}
	if problem != nil {
		return &workerapi.Complete{Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: problem}
	}
	if a.job.TimeoutSeconds > 0 {
		t := time.AfterFunc(seconds(a.job.TimeoutSeconds)-time.Since(a.claimed), func() { a.fire(a.timedOut, nil) })
		defer t.Stop()
	}

	var (
		results  []plan.StepResult
		cleanups []*cleanup
		// failed is set once a step has failed; ran once one has run.
		failed, ran bool
		// stopped is set once the job is stopping, for why says: its
		// steps after that don't run, but those that outlive it.
		stopped bool
		why     stopCause
	)
	for i := range a.job.Steps {
		if !stopped {
			switch {
			case a.cancelled.fired():
				stopped, why = true, stopCancelled
			case a.timedOut.fired():
				stopped, why = true, stopJobTimeout
			}
		}
		// As in format 1, a job past its timeout isn't cancelled: its
		// steps' cancelled() is false, and success() too, as one failed.
		cancelled := why == stopCancelled || why == stopEarlyCancel
		rs, diags, err := a.ph.step(a.job, ctx, i, a.state(results, cancelled))
		if a.halted.fired() {
			return nil
		}
		path := a.job.Steps[i].Path
		var outcome plan.Outcome
		switch {
		case err != nil:
			// Not the workflow's fault, but the step can't run.
			a.log.Error("readying a step failed", "step", i, "error", a.mask.Mask(err.Error()))
			outcome = plan.OutcomeFailure
		case diags.HasErrors() || rs == nil:
			outcome = plan.OutcomeFailure
		case rs.Skipped:
			outcome = plan.OutcomeSkipped
		default:
			a.mask.Add(rs.Masked...)
			a.masked = append(a.masked, rs.Masked...)
			switch {
			case stopped && !rs.OutlivesCancel && why == stopJobTimeout:
				// As in format 1, a step that would run once its job has
				// timed out fails, unless it outlives that.
				outcome = plan.OutcomeFailure
			case stopped && !rs.OutlivesCancel:
				outcome = plan.OutcomeCancelled
			case rs.Uses != "":
				diags = append(diags, problems.UsesNotSupported.At("", problems.Position{}, path, problems.Params{"uses": rs.Uses}))
				outcome = plan.OutcomeFailure
			case rs.SSH != nil:
				diags = append(diags, problems.StepSSHNotSupported.At("", problems.Position{}, path, nil))
				outcome = plan.OutcomeFailure
			}
		}
		var result plan.StepResult
		if outcome != "" {
			a.finished(workerapi.Event{Step: i, Outcome: outcome, ExitCode: -1, Diagnostics: diags})
			result = plan.StepResult{Outcome: outcome}
		} else {
			end := a.runStep(i, rs, diags)
			if a.halted.fired() {
				return nil
			}
			result = end.result
			if end.started {
				ran = true
				if rs.Cleanup != "" {
					cleanups = append(cleanups, end.cleanup)
				}
			}
			if !stopped && (end.cause == stopCancelled || end.cause == stopEarlyCancel || end.cause == stopJobTimeout) {
				stopped, why = true, end.cause
			}
		}
		results = append(results, result)
		if result.Outcome == plan.OutcomeFailure {
			failed = true
			a.fire(a.anyJobFailed, nil)
		}
	}

	for _, c := range slices.Backward(cleanups) {
		outcome := a.runCleanup(c)
		if a.halted.fired() {
			return nil
		}
		// As in format 1, a cleanup that fails fails its job.
		failed = failed || outcome == plan.OutcomeFailure
	}

	end := &workerapi.Complete{Outcome: plan.OutcomeSuccess}
	switch {
	case why == stopJobTimeout:
		end.Outcome, end.Reason = plan.OutcomeFailure, plan.ReasonTimeout
	case failed:
		end.Outcome = plan.OutcomeFailure
	case why == stopCancelled:
		end.Outcome, end.Reason = plan.OutcomeCancelled, a.cancelReason
	case why == stopEarlyCancel:
		end.Outcome = plan.OutcomeCancelled
	case !ran && len(a.job.Steps) > 0:
		end.Outcome = plan.OutcomeSkipped
	}
	// Once the job's steps have ended, its outputs are evaluated, and its
	// end sent, even if the worker is being stopped from outside, as at
	// its scheduler's time limit, unless the attempt has ended without it.
	outputs, diags, err := a.ph.end(a.job, context.WithoutCancel(ctx), a.state(results, why == stopCancelled || why == stopEarlyCancel))
	if a.lost() {
		return nil
	}
	if err != nil {
		a.log.Error("evaluating the job's outputs failed", "error", a.mask.Mask(err.Error()))
	}
	if (err != nil || diags.HasErrors()) && end.Outcome != plan.OutcomeCancelled {
		end.Outcome = plan.OutcomeFailure
	}
	end.Outputs, end.Diagnostics = outputs, a.maskDiagnostics(diags)
	return end
}

// lost reports whether the attempt has ended without the worker: whether
// it was halted other than by its own context ending.
func (a *attempt) lost() bool {
	if !a.halted.fired() {
		return false
	}
	return !errors.Is(a.haltErr, context.Canceled) && !errors.Is(a.haltErr, context.DeadlineExceeded)
}

// setUp makes ready to run the job's steps: it reads the job, refuses one
// whose steps are to run in a container, and makes the run's directory. It
// writes what it does to the job's setup log, and returns the problem that
// failed the job, if one did.
func (a *attempt) setUp() *problem.Problem {
	stream := a.spool.open(logKey{part: workerapi.LogSetup})
	w := a.mask.Writer(stream)
	defer stream.Close()
	defer w.Close()
	setupLog := slog.New(slog.NewTextHandler(w, nil))
	logs := []*slog.Logger{a.log, setupLog}
	info := func(msg string, args ...any) {
		for _, l := range logs {
			l.Info(msg, args...)
		}
	}
	fail := func(k *problems.Kind, params problems.Params) *problem.Problem {
		p := a.maskProblem(k.Type().New(k.Message(params)), params)
		for _, l := range logs {
			l.Error("the job failed before its first step", "problem", p.Detail)
		}
		return p
	}
	info("claimed the attempt", "run", a.claim.RunID, "attempt", a.claim.AttemptID, "number", a.claim.Attempt,
		"worker", a.worker.Version, "digest", a.worker.Digest)

	job, err := plan.DecodeReadyJob(a.claim.PlanVersion, a.claim.Job)
	if err != nil {
		return fail(problems.JobUnreadable, problems.Params{"reason": err.Error()})
	}
	a.job = job
	a.mask.Add(job.Masked...)
	a.masked = append(a.masked, job.Masked...)
	info("read the job", "key", job.Key)
	for _, name := range slices.Sorted(maps.Keys(a.claim.Env)) {
		if name == "" || strings.ContainsAny(name, "=\x00") {
			return fail(problems.EnvNameInvalid, problems.Params{"name": problems.Show(name)})
		}
	}
	if c := a.claim.Container; c != nil {
		// Never weaker isolation than asked for: the steps don't run on
		// the host instead.
		return fail(problems.ContainerNotSupported, problems.Params{"runtime": c.Runtime, "image": c.Image})
	}
	if err := os.MkdirAll(a.claim.RunDir, 0o700); err != nil {
		return fail(problems.RunDirUnusable, problems.Params{"path": a.claim.RunDir, "reason": err.Error()})
	}
	info("running the job's steps on the host", "dir", a.claim.RunDir)
	return nil
}

// state returns what the worker knows of its job, with the results of the
// steps that have had their turn, for the Step and End phases.
func (a *attempt) state(results []plan.StepResult, cancelled bool) plan.JobState {
	return plan.JobState{
		Run:   a.claim.Run,
		Needs: a.claim.Needs,
		Job: plan.JobContext{
			Name: a.job.Name, Key: a.job.Key, Attempt: a.claim.Attempt, Group: a.claim.Group, Member: a.claim.Member,
		},
		Steps:     slices.Clone(results),
		Cancelled: cancelled,
		Masked:    slices.Clone(a.masked),
	}
}

// A stepEnd is how a step that the worker ran ended.
type stepEnd struct {
	result plan.StepResult
	// started is set if a try of the step started, cause is why its last
	// try was stopped, if it was, and cleanup is how to run its cleanup:
	// command.
	started bool
	cause   stopCause
	cleanup *cleanup
}

// A cleanup is a step's cleanup: command, as the worker runs it.
type cleanup struct {
	index   int
	step    *plan.ReadyStep
	dir     string
	outputs string
}

// runStep runs a step, with format 1's retry:, and reports its events. It
// returns how the step ended: if the job was halted, how isn't known.
func (a *attempt) runStep(i int, rs *plan.ReadyStep, diags problems.Diagnostics) stepEnd {
	path := a.job.Steps[i].Path
	refuse := func(d problems.Diagnostic) stepEnd {
		diags = append(diags, d)
		a.finished(workerapi.Event{Step: i, Outcome: plan.OutcomeFailure, ExitCode: -1, Diagnostics: diags})
		return stepEnd{result: plan.StepResult{Outcome: plan.OutcomeFailure}}
	}
	notStarted := func(err error) stepEnd {
		return refuse(problems.StepNotStarted.At("", problems.Position{}, path, problems.Params{"reason": err.Error()}))
	}
	if shell, ok := shellOK(rs.Shell); !ok {
		return refuse(problems.ShellNotSupported.At("", problems.Position{}, path, problems.Params{"shell": shell}))
	}
	// A step with an id has $OUTPUTS of its own, its steps.<id>.outputs;
	// the steps without one share the job's, as in format 1, and each
	// wrote what it changed.
	outputsName := jobOutputs
	var before map[string]string
	var beforeErr error
	if rs.ID != "" {
		outputsName = "outputs-" + strconv.Itoa(i)
	}
	outputsPath, err := a.makeOutputs(outputsName, rs.ID != "")
	if err != nil {
		return notStarted(err)
	}
	if rs.ID == "" {
		before, _, beforeErr = readOutputs(a.files, outputsName)
	}
	dir := a.workingDirectory(rs)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return notStarted(err)
	}

	// As format 1 runs a step: one that outlives its job's cancellation
	// and timeout outlives its own timeout: too, but not its retry:'s
	// timeout or its early-cancel:, and the wait between tries stops for
	// the job's cancellation and timeout only.
	s := stops{cancellable: !rs.OutlivesCancel, earlyCancel: rs.EarlyCancel == "any-job-failed"}
	if !rs.OutlivesCancel {
		s.step = seconds(rs.TimeoutSeconds)
	}
	tries := 1
	if rs.Retry != nil {
		tries += max(rs.Retry.MaxRetries, 0)
		s.try = seconds(rs.Retry.TimeoutSeconds)
	}
	between := stops{cancellable: s.cancellable}
	end := stepEnd{cleanup: &cleanup{index: i, step: rs, dir: dir, outputs: outputsPath}}
	for try := range tries {
		if try > 0 {
			ready, stop := passed(seconds(rs.Retry.IntervalSeconds))
			cause := a.await(between, ready)
			stop()
			if cause != noStop {
				return a.notRun(i, try, end, cause, diags)
			}
		}
		if cause := a.stopCauseOf(s); cause != noStop {
			return a.notRun(i, try, end, cause, diags)
		}
		seq := a.events.add(workerapi.Event{Time: now(), Step: i, Try: try})
		if !a.firstStarted {
			// Whether the workflow's code may have run must always be
			// known: the coordinator stores that the first step is
			// starting before it starts.
			if cause := a.await(s, func() (bool, <-chan struct{}) { return a.events.isStored(seq) }); cause != noStop {
				return a.notRun(i, try, end, cause, diags)
			}
			a.firstStarted = true
		}
		end.started = true

		var outputs map[string]string
		exit, cause, tryDiags := a.runCommand(logKey{workerapi.LogStep, i, try}, rs, rs.Run, dir, a.stepEnv(rs, outputsPath, try), s)
		if a.halted.fired() {
			return end
		}
		// What a try wrote is its step's, however it ended, as in format 1.
		got, size, err := readOutputs(a.files, outputsName)
		switch {
		case errors.Is(err, errOutputsNotFile):
			tryDiags = append(tryDiags, problems.OutputsNotFile.At("", problems.Position{}, path, nil))
		case errors.Is(err, errOutputsTooLarge):
			// Only the step that took the shared file past the limit
			// fails; those after it can't add to it.
			if rs.ID != "" || !errors.Is(beforeErr, errOutputsTooLarge) {
				tryDiags = append(tryDiags, problems.OutputsTooLarge.At("", problems.Position{}, path, problems.Params{"size": size, "limit": maxOutputs}))
			}
		case errors.Is(err, errOutputsLineTooLong):
			tryDiags = append(tryDiags, problems.OutputsLineTooLong.At("", problems.Position{}, path, problems.Params{"limit": maxOutputsLine + 1}))
			outputs = map[string]string{}
		case err != nil:
		case rs.ID == "":
			outputs = written(before, got)
		default:
			outputs = got
		}

		last := try == tries-1
		outcome, retry := plan.OutcomeFailure, false
		switch {
		case cause == stopCancelled || cause == stopEarlyCancel:
			outcome = plan.OutcomeCancelled
		case cause == stopJobTimeout || tryDiags.HasErrors():
		case cause == noStop && exit == 0:
			outcome = plan.OutcomeSuccess
		case rs.IgnoreErrors && last:
			// As format 1: a step whose last try fails, even past its
			// timeout, succeeds.
			outcome = plan.OutcomeSuccess
		case cause == stopStepTimeout:
			// A step past its own timeout: isn't tried again.
			tryDiags = append(tryDiags, problems.StepTimedOut.At("", problems.Position{}, path, problems.Params{"limit": rs.TimeoutSeconds}))
		case cause == stopTryTimeout:
			tryDiags = append(tryDiags, problems.StepTimedOut.At("", problems.Position{}, path, problems.Params{"limit": rs.Retry.TimeoutSeconds}))
			retry = !last
		default:
			retry = !last
		}
		if !retry {
			tryDiags = append(slices.Clone(diags), tryDiags...)
		}
		a.finished(workerapi.Event{Step: i, Try: try, Outcome: outcome, ExitCode: exit, Outputs: outputs, Diagnostics: tryDiags})
		end.result = plan.StepResult{Outcome: outcome, Outputs: outputs}
		end.cause = cause
		if !retry {
			break
		}
	}
	return end
}

// notRun reports the try of a step that didn't start, because the job is
// stopping for cause, and returns how the step ended: cancelled, or
// failed for the job's timeout, as in format 1.
func (a *attempt) notRun(i, try int, end stepEnd, cause stopCause, diags problems.Diagnostics) stepEnd {
	if cause == stopHalted {
		return end
	}
	outcome := plan.OutcomeCancelled
	if cause == stopJobTimeout {
		outcome = plan.OutcomeFailure
	}
	a.finished(workerapi.Event{Step: i, Try: try, Outcome: outcome, ExitCode: -1, Diagnostics: diags})
	end.result, end.cause = plan.StepResult{Outcome: outcome}, cause
	return end
}

// runCleanup runs a step's cleanup: command, as format 1 runs it: as a
// step of its own, with the step's shell, working directory, environment
// and ignore-errors:, but no timeout or retry, which only the attempt
// ending without the worker stops. It reports its events, and returns its
// outcome, or "" if the job was halted.
func (a *attempt) runCleanup(c *cleanup) plan.Outcome {
	if a.halted.fired() {
		return ""
	}
	a.events.add(workerapi.Event{Time: now(), Step: c.index, Cleanup: true})
	env := a.stepEnv(c.step, c.outputs, -1)
	exit, _, diags := a.runCommand(logKey{workerapi.LogCleanup, c.index, 0}, c.step, c.step.Cleanup, c.dir, env, stops{})
	if a.halted.fired() {
		return ""
	}
	outcome := plan.OutcomeSuccess
	if diags.HasErrors() || exit != 0 && !c.step.IgnoreErrors {
		outcome = plan.OutcomeFailure
	}
	a.finished(workerapi.Event{Step: c.index, Cleanup: true, Outcome: outcome, ExitCode: exit, Diagnostics: diags})
	return outcome
}

// runCommand runs script, a step's run: or cleanup: command, with the
// step's shell, in dir with env, and its output, masked, in the log key,
// until it ends, or what s says stops it. It returns its exit status, why
// it was stopped, and the problems with running it: a command that didn't
// start, or a log that wasn't kept whole.
func (a *attempt) runCommand(key logKey, rs *plan.ReadyStep, script, dir string, env []string, s stops) (int, stopCause, problems.Diagnostics) {
	path := a.job.Steps[key.step].Path
	var diags problems.Diagnostics
	stream := a.spool.open(key)
	w := a.mask.Writer(stream)
	exit, cause, err := func() (int, stopCause, error) {
		name, file, err := a.writeScript(script)
		if err != nil {
			return -1, noStop, err
		}
		defer a.files.remove(name)
		cmd := shellCommand(rs.Shell, file)
		cmd.Dir, cmd.Env = dir, env
		requests, unwatch := a.watch(s)
		defer unwatch()
		defer a.endedGroup(cmd)
		return runProcess(cmd, w, requests, a.startedGroup)
	}()
	w.Close()
	stream.Close()
	a.liveGroups()
	if err != nil {
		diags = append(diags, problems.StepNotStarted.At("", problems.Position{}, path, problems.Params{"reason": err.Error()}))
	}
	if dropped := stream.droppedBytes(); dropped > 0 {
		diags = append(diags, problems.LogNotKept.At("", problems.Position{}, path, problems.Params{"size": dropped, "limit": a.spool.limit}))
	}
	return exit, cause, diags
}

// jobOutputs is the name of the $OUTPUTS file the job's steps without an
// id share, as in format 1.
const jobOutputs = "outputs"

// writeScript writes a command to a script of its own, in the worker's
// directory, which only its user can read, since it can hold secrets, and
// returns its name and its path.
func (a *attempt) writeScript(command string) (string, string, error) {
	a.scripts++
	name := "script-" + strconv.Itoa(a.scripts) + ".sh"
	f, path, err := a.files.create(name, os.O_WRONLY|os.O_EXCL)
	if err != nil {
		return "", "", err
	}
	_, err = f.WriteString(command + "\n")
	return name, path, errors.Join(err, f.Close())
}

// makeOutputs makes name, in the worker's directory, an $OUTPUTS file, and
// returns its path: an empty one if empty is set, or else the one there. A
// step may have removed it, or put something else in its place: a
// symbolic link to a regular file is kept, as format 1 follows it, and
// anything else is replaced.
func (a *attempt) makeOutputs(name string, empty bool) (string, error) {
	path, root, err := a.files.current()
	if err != nil {
		return "", err
	}
	if info, err := root.Lstat(name); err == nil {
		keep := !empty && info.Mode().IsRegular()
		if !empty && info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Stat(filepath.Join(path, name))
			keep = err == nil && target.Mode().IsRegular()
		}
		if keep {
			return filepath.Join(path, name), nil
		}
		if err := root.Remove(name); err != nil {
			return "", err
		}
	}
	f, path, err := a.files.create(name, os.O_WRONLY|os.O_EXCL)
	if err != nil {
		return "", err
	}
	return path, f.Close()
}

// shellOK reports whether a step's shell: is one format 1 runs, bash or
// sh, by name or path, with the program it names.
func shellOK(shell string) (string, bool) {
	args := strings.Fields(shell)
	if len(args) == 0 {
		return "/bin/bash", true
	}
	base := path.Base(args[0])
	return args[0], base == "bash" || base == "sh"
}

// shellCommand returns the command that runs script with shell, a step's
// shell:, in which {0} is the script's path, or, without one, after which
// it comes, or "" for format 1's default, a bash that isn't a login shell.
func shellCommand(shell, script string) *exec.Cmd {
	args := strings.Fields(shell)
	if len(args) == 0 {
		args = []string{"/bin/bash"}
	}
	placed := false
	for k, arg := range args {
		if strings.Contains(arg, "{0}") {
			args[k], placed = strings.ReplaceAll(arg, "{0}", script), true
		}
	}
	if !placed {
		args = append(args, script)
	}
	return exec.Command(args[0], args[1:]...) //nolint:gosec,noctx // the workflow's shell runs its step; runProcess stops it
}

// workingDirectory returns where a step runs: its working directory, as
// Step resolved it, under the run's directory if it is relative, or the
// run's directory.
func (a *attempt) workingDirectory(rs *plan.ReadyStep) string {
	switch {
	case rs.WorkingDirectory == "":
		return a.claim.RunDir
	case filepath.IsAbs(rs.WorkingDirectory):
		return rs.WorkingDirectory
	}
	return filepath.Join(a.claim.RunDir, rs.WorkingDirectory)
}

// stepEnv returns the environment a step's try runs with, or its cleanup:
// command for a try of -1: the worker's, but for what told it how to reach
// the coordinator; the environment the workflow gives the step over it;
// and over that, as format 1 lays them, the variables a platform gives the
// job (Claim.Env), and the worker's own.
func (a *attempt) stepEnv(rs *plan.ReadyStep, outputs string, try int) []string {
	env := map[string]string{}
	for _, kv := range a.environ {
		name, value, _ := strings.Cut(kv, "=")
		env[name] = value
	}
	env["CI"] = "true"
	maps.Copy(env, rs.Env)
	// For a run of its own, as format 1 has them, unless the platform
	// says otherwise.
	env["PW_JOB_ID"] = a.claim.RunID
	env["PW_PARENT_JOB_DIR"] = a.claim.RunDir
	for name, v := range a.claim.Env {
		env[name] = v.Value
	}
	env["OUTPUTS"] = outputs
	env["PW_JOB_DIR"] = a.claim.RunDir
	if k := strings.LastIndexByte(a.job.Key, '#'); k >= 0 && !a.job.Matrix.IsZero() {
		env["PW_MATRIX_INDEX"] = a.job.Key[k+1:]
	}
	if rs.Retry != nil {
		// A cleanup isn't retried: format 1 tells it 0 of 0.
		current, most := try, rs.Retry.MaxRetries
		if try < 0 {
			current, most = 0, 0
		}
		env["PW_WORKFLOW_STEP_CURRENT_RETRY"] = strconv.Itoa(current)
		env["PW_WORKFLOW_STEP_MAX_RETRIES"] = strconv.Itoa(most)
	}
	if a.job.Retry != nil {
		env["PW_WORKFLOW_JOB_CURRENT_RETRY"] = strconv.Itoa(a.claim.Retry)
		env["PW_WORKFLOW_JOB_MAX_RETRIES"] = strconv.Itoa(a.job.Retry.MaxRetries)
	}
	out := make([]string, 0, len(env))
	for _, name := range slices.Sorted(maps.Keys(env)) {
		out = append(out, name+"="+env[name])
	}
	return out
}

// finished reports a step's, or its cleanup's, finished event, with its
// outputs and its problems masked.
func (a *attempt) finished(e workerapi.Event) {
	e.Time, e.Finished = now(), true
	if e.Outputs != nil {
		masked := make(map[string]string, len(e.Outputs))
		for name, value := range e.Outputs {
			masked[name] = a.mask.Mask(value)
		}
		e.Outputs = masked
	}
	e.Diagnostics = a.maskDiagnostics(e.Diagnostics)
	a.events.add(e)
}

// maskDiagnostics returns diagnostics with what they say masked: a
// problem the worker makes from an error, such as a directory it couldn't
// make, can quote a value made from a secret.
func (a *attempt) maskDiagnostics(ds problems.Diagnostics) problems.Diagnostics {
	if len(ds) == 0 {
		return ds
	}
	out := make(problems.Diagnostics, len(ds))
	for k, d := range ds {
		d.Detail = a.mask.Mask(d.Detail)
		d.Params = a.maskParams(d.Params)
		out[k] = d
	}
	return out
}

// maskProblem returns p, with params, and what it says masked.
func (a *attempt) maskProblem(p *problem.Problem, params problems.Params) *problem.Problem {
	p.Detail = a.mask.Mask(p.Detail)
	p.Params = a.maskParams(params)
	return p
}

func (a *attempt) maskParams(params map[string]any) map[string]any {
	if params == nil {
		return nil
	}
	out := make(map[string]any, len(params))
	for name, v := range params {
		if s, ok := v.(string); ok {
			v = a.mask.Mask(s)
		}
		out[name] = v
	}
	return out
}

func now() time.Time { return time.Now().UTC() }
