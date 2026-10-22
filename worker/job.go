package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
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

// A stopCause is why a step was stopped before it ended by itself.
type stopCause int

const (
	noStop stopCause = iota
	// stopHalted is the attempt ending without the worker, or the worker
	// stopping.
	stopHalted
	// stopCancelled is the coordinator cancelling the job.
	stopCancelled
	// stopEarlyCancel is another job of the run failing, for a step whose
	// early-cancel: is any-job-failed.
	stopEarlyCancel
	// stopJobTimeout is the job running past its timeout:.
	stopJobTimeout
	// stopStepTimeout is the step running past its timeout:, and
	// stopTryTimeout one of its tries past its retry:'s timeout.
	stopStepTimeout
	stopTryTimeout
)

// execute runs the attempt's job: it sets it up, runs its steps and their
// cleanup: commands, and evaluates its outputs. It returns the attempt's
// end, or nil if the job was halted.
func (a *attempt) execute(ctx context.Context) *workerapi.Complete {
	a.masked = a.ph.masked(a.claim.Run)
	a.mask = a.ph.masker(a.masked...)
	problem := a.setUp()
	if a.halted.fired() {
		return nil
	}
	if problem != nil {
		return &workerapi.Complete{Outcome: plan.OutcomeFailure, Reason: plan.ReasonSetupFailed, Problem: problem}
	}
	if a.job.TimeoutSeconds > 0 {
		t := time.AfterFunc(seconds(a.job.TimeoutSeconds)-time.Since(a.claimed), func() { a.timedOut.fire(nil) })
		defer t.Stop()
	}

	var (
		results  []plan.StepResult
		cleanups []*cleanup
		// failed is set once a step has failed; ran once one has run.
		failed, ran bool
		// stopped is set once the job is stopping, for why says: its
		// steps after that don't run.
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
		rs, diags, err := a.ph.step(a.job, ctx, i, a.state(results, stopped))
		if a.halted.fired() {
			return nil
		}
		path := a.job.Steps[i].Path
		var outcome plan.Outcome
		switch {
		case err != nil:
			// Not the workflow's fault, but the step can't run.
			a.log.Error("readying a step failed", "step", i, "error", err)
			outcome = plan.OutcomeFailure
		case diags.HasErrors() || rs == nil:
			outcome = plan.OutcomeFailure
		case rs.Skipped:
			outcome = plan.OutcomeSkipped
		default:
			a.mask.Add(rs.Masked...)
			a.masked = append(a.masked, rs.Masked...)
			switch {
			case stopped && why == stopJobTimeout:
				// As in format 1, a step that would run once the job has
				// timed out fails.
				outcome = plan.OutcomeFailure
			case stopped:
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
			if end.cause == stopCancelled || end.cause == stopEarlyCancel || end.cause == stopJobTimeout {
				stopped, why = true, end.cause
			}
		}
		results = append(results, result)
		if result.Outcome == plan.OutcomeFailure {
			failed = true
			a.anyJobFailed.fire(nil)
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
	outputs, diags, err := a.ph.end(a.job, ctx, a.state(results, stopped))
	if a.halted.fired() {
		return nil
	}
	if err != nil {
		a.log.Error("evaluating the job's outputs failed", "error", err)
	}
	if (err != nil || diags.HasErrors()) && end.Outcome != plan.OutcomeCancelled {
		end.Outcome = plan.OutcomeFailure
	}
	end.Outputs, end.Diagnostics = outputs, diags
	return end
}

// setUp makes ready to run the job's steps: it reads the job, refuses one
// whose steps are to run in a container, and makes the run's directory. It
// writes what it does to the job's setup log, and returns the problem that
// failed the job, if one did.
func (a *attempt) setUp() *problem.Problem {
	stream, err := a.spool.open(logKey{part: workerapi.LogSetup})
	if err != nil {
		a.halt(err)
		return nil
	}
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
		p := k.Type().New(k.Message(params))
		p.Params = maps.Clone(params)
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
	if c := a.claim.Container; c != nil {
		// Never weaker isolation than asked for: the steps don't run on
		// the host instead.
		return fail(problems.ContainerNotSupported, problems.Params{"runtime": c.Runtime, "image": c.Image})
	}
	if err := os.MkdirAll(a.claim.RunDir, 0o700); err != nil {
		return fail(problems.RunDirUnusable, problems.Params{"path": a.claim.RunDir, "reason": err.Error()})
	}
	a.jobOutputs = filepath.Join(a.dir, "outputs")
	if err := os.WriteFile(a.jobOutputs, nil, 0o600); err != nil {
		a.halt(fmt.Errorf("worker: making $OUTPUTS: %w", err))
		return nil
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
	notStarted := func(err error) stepEnd {
		diags = append(diags, problems.StepNotStarted.At("", problems.Position{}, path, problems.Params{"reason": err.Error()}))
		a.finished(workerapi.Event{Step: i, Outcome: plan.OutcomeFailure, ExitCode: -1, Diagnostics: diags})
		return stepEnd{result: plan.StepResult{Outcome: plan.OutcomeFailure}}
	}
	// A step with an id has $OUTPUTS of its own, its steps.<id>.outputs;
	// the steps without one share the job's, as in format 1, and each
	// wrote what it changed.
	outputsPath := a.jobOutputs
	var before map[string]string
	if rs.ID != "" {
		outputsPath = filepath.Join(a.dir, "outputs-"+strconv.Itoa(i))
		if err := os.WriteFile(outputsPath, nil, 0o600); err != nil {
			return notStarted(err)
		}
	} else {
		before, _, _ = readOutputs(outputsPath)
	}
	dir := a.workingDirectory(rs)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return notStarted(err)
	}
	end := stepEnd{cleanup: &cleanup{index: i, step: rs, dir: dir, outputs: outputsPath}}
	tries := 1
	if rs.Retry != nil {
		tries += max(rs.Retry.MaxRetries, 0)
	}
	for try := range tries {
		stops := []source{
			{a.halted.done(), stopHalted},
			{a.cancelled.done(), stopCancelled},
			{a.timedOut.done(), stopJobTimeout},
		}
		if rs.EarlyCancel == "any-job-failed" {
			stops = append(stops, source{a.anyJobFailed.done(), stopEarlyCancel})
		}
		if try > 0 {
			// Between tries, the step is as its last try ended, unless
			// the worker is halted.
			if cause := waitFor(seconds(rs.Retry.IntervalSeconds), stops); cause != noStop {
				end.cause = cause
				return end
			}
		}
		if cause := firstFired(stops); cause != noStop {
			return a.notRun(i, try, end, cause, diags)
		}
		seq := a.events.add(workerapi.Event{Time: now(), Step: i, Try: try})
		if !a.firstStarted {
			// Whether the workflow's code may have run must always be
			// known: the coordinator stores that the first step is
			// starting before it starts.
			if cause := a.waitStored(seq, stops); cause != noStop {
				return a.notRun(i, try, end, cause, diags)
			}
			a.firstStarted = true
		}
		end.started = true

		timeouts := []time.Duration{seconds(rs.TimeoutSeconds), 0}
		if rs.Retry != nil {
			timeouts[1] = seconds(rs.Retry.TimeoutSeconds)
		}
		var outputs map[string]string
		exit, cause, tryDiags := a.runCommand(logKey{workerapi.LogStep, i, try}, rs, rs.Run, dir, a.stepEnv(rs, outputsPath, try), stops, timeouts)
		if a.halted.fired() {
			return end
		}
		// What a try wrote is its step's, however it ended, as in format 1.
		got, size, err := readOutputs(outputsPath)
		switch {
		case errors.Is(err, errOutputsTooLarge):
			tryDiags = append(tryDiags, problems.OutputsTooLarge.At("", problems.Position{}, path, problems.Params{"size": size, "limit": maxOutputs}))
		case errors.Is(err, errOutputsLineTooLong):
			tryDiags = append(tryDiags, problems.OutputsLineTooLong.At("", problems.Position{}, path, problems.Params{"limit": maxOutputsLine + 1}))
			outputs = map[string]string{}
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
// stopping for cause, and returns how the step ended.
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

// waitStored waits until the coordinator has stored the event seq, or one
// of stops fires, and returns which.
func (a *attempt) waitStored(seq int64, stops []source) stopCause {
	stopped, unwatch := watch(stops, nil)
	defer unwatch()
	for {
		stored, changed := a.events.isStored(seq)
		if stored {
			return noStop
		}
		select {
		case <-changed:
		case cause := <-stopped:
			return cause
		}
	}
}

// runCleanup runs a step's cleanup: command, as format 1 runs it: as a
// step of its own, with the step's shell, working directory, environment
// and ignore-errors:, but no timeout or retry, which a cancellation
// doesn't stop. It reports its events, and returns its outcome, or ""
// if the job was halted.
func (a *attempt) runCleanup(c *cleanup) plan.Outcome {
	stops := []source{{a.halted.done(), stopHalted}}
	if firstFired(stops) != noStop {
		return ""
	}
	a.events.add(workerapi.Event{Time: now(), Step: c.index, Cleanup: true})
	env := a.stepEnv(c.step, c.outputs, -1)
	exit, _, diags := a.runCommand(logKey{workerapi.LogCleanup, c.index, 0}, c.step, c.step.Cleanup, c.dir, env, stops, nil)
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
// until it ends or one of stops or timeouts, the step's and its try's,
// stops it. It returns its exit status, why it was stopped, and the
// problems with running it: a command that didn't start, or a log that
// wasn't kept whole.
func (a *attempt) runCommand(key logKey, rs *plan.ReadyStep, script, dir string, env []string, stops []source, timeouts []time.Duration) (int, stopCause, problems.Diagnostics) {
	path := a.job.Steps[key.step].Path
	var diags problems.Diagnostics
	stream, err := a.spool.open(key)
	if err != nil {
		a.halt(err)
		return -1, stopHalted, nil
	}
	w := a.mask.Writer(stream)
	exit, cause, err := func() (int, stopCause, error) {
		file, err := a.writeScript(script)
		if err != nil {
			return -1, noStop, err
		}
		defer os.Remove(file)
		cmd := shellCommand(rs.Shell, file)
		cmd.Dir, cmd.Env = dir, env
		var deadlines []*time.Timer
		stops := slices.Clip(stops)
		for k, d := range timeouts {
			if d > 0 {
				c := make(chan struct{})
				deadlines = append(deadlines, time.AfterFunc(d, func() { close(c) }))
				stops = append(stops, source{c, []stopCause{stopStepTimeout, stopTryTimeout}[k]})
			}
		}
		stopped, unwatch := watch(stops, deadlines)
		defer unwatch()
		return runProcess(cmd, w, stopped, a.grace, a.tm.afterKill)
	}()
	w.Close()
	stream.Close()
	if err != nil {
		diags = append(diags, problems.StepNotStarted.At("", problems.Position{}, path, problems.Params{"reason": err.Error()}))
	}
	if dropped := stream.droppedBytes(); dropped > 0 {
		diags = append(diags, problems.LogNotKept.At("", problems.Position{}, path, problems.Params{"size": dropped, "limit": a.spool.limit}))
	}
	return exit, cause, diags
}

// grace is how long a step stopped for cause has after SIGTERM before
// SIGKILL: the coordinator's grace period for its cancellation, and the
// worker's otherwise.
func (a *attempt) grace(cause stopCause) time.Duration {
	if cause == stopCancelled && a.cancelGrace > 0 {
		return a.cancelGrace
	}
	return a.tm.grace
}

// writeScript writes a command to a script of its own, which only the
// worker's user can read, since it can hold secrets, and returns its path.
func (a *attempt) writeScript(command string) (string, error) {
	a.scripts++
	path := filepath.Join(a.dir, "script-"+strconv.Itoa(a.scripts)+".sh")
	if err := os.WriteFile(path, []byte(command+"\n"), 0o600); err != nil {
		return "", err
	}
	return path, nil
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
// the coordinator; the environment the workflow gives the step over it; and
// the variables format 1 defines over that.
func (a *attempt) stepEnv(rs *plan.ReadyStep, outputs string, try int) []string {
	env := map[string]string{}
	for _, kv := range a.environ {
		name, value, _ := strings.Cut(kv, "=")
		env[name] = value
	}
	env["CI"] = "true"
	maps.Copy(env, rs.Env)
	env["OUTPUTS"] = outputs
	env["PW_JOB_DIR"] = a.claim.RunDir
	env["PW_JOB_ID"] = a.claim.RunID
	if _, index, ok := strings.Cut(a.job.Key, "#"); ok && !a.job.Matrix.IsZero() {
		env["PW_MATRIX_INDEX"] = index
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
		env["PW_WORKFLOW_JOB_CURRENT_RETRY"] = strconv.Itoa(a.claim.Attempt - 1)
		env["PW_WORKFLOW_JOB_MAX_RETRIES"] = strconv.Itoa(a.job.Retry.MaxRetries)
	}
	out := make([]string, 0, len(env))
	for _, name := range slices.Sorted(maps.Keys(env)) {
		out = append(out, name+"="+env[name])
	}
	return out
}

// finished reports a step's, or its cleanup's, finished event, with its
// outputs masked.
func (a *attempt) finished(e workerapi.Event) {
	e.Time, e.Finished = now(), true
	if e.Outputs != nil {
		masked := make(map[string]string, len(e.Outputs))
		for name, value := range e.Outputs {
			masked[name] = a.mask.Mask(value)
		}
		e.Outputs = masked
	}
	a.events.add(e)
}

func now() time.Time { return time.Now().UTC() }

// A source is a channel closed when a step is to stop, for cause.
type source struct {
	c     <-chan struct{}
	cause stopCause
}

// firstFired returns the cause of the first of sources that has fired, or
// noStop.
func firstFired(sources []source) stopCause {
	for _, s := range sources {
		select {
		case <-s.c:
			return s.cause
		default:
		}
	}
	return noStop
}

// watch returns a channel that receives the cause of the first of sources
// that fires, and a function that stops watching them, and timers.
func watch(sources []source, timers []*time.Timer) (<-chan stopCause, func()) {
	fired := make(chan stopCause, 1)
	done := make(chan struct{})
	for _, s := range sources {
		go func() {
			select {
			case <-s.c:
				select {
				case fired <- s.cause:
				default:
				}
			case <-done:
			}
		}()
	}
	return fired, func() {
		close(done)
		for _, t := range timers {
			t.Stop()
		}
	}
}

// waitFor waits for d, and returns noStop, unless one of sources fires
// first, and returns its cause.
func waitFor(d time.Duration, sources []source) stopCause {
	if cause := firstFired(sources); cause != noStop {
		return cause
	}
	stopped, unwatch := watch(sources, nil)
	defer unwatch()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return noStop
	case cause := <-stopped:
		return cause
	}
}
