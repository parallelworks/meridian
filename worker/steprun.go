package worker

import (
	"context"
	"errors"
	"os"
	"slices"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A step whose ssh: puts it on another host than its job's runs there as a
// step run, a run of its own (docs/design.md, Steps on another host). Its
// job's worker, the caller, asks the coordinator for it, waits for its end,
// stops it as the step is stopped, and ends the try as it ended, appending
// what its step wrote to $OUTPUTS there to the file the step would have
// written here; the step run's worker evaluates and runs that one step,
// with its own variables, over what the caller's job passes between its
// steps, and reports what it wrote.

// stepRunRequest returns what the worker asks for the step run of the try
// try of the step rs, or of its cleanup:, for cleanup, in state, the job's
// state as the step is evaluated, whose first try was first, nil for a
// cleanup: the step's index and the try; the ssh: the try evaluated, or
// nil for one that is null or names no host; of the job's state only what
// format 1 passes between steps, never a variable of the environment or a
// path; and the strings the worker masks, which the step run's masks too.
func (a *attempt) stepRunRequest(rs *plan.ReadyStep, try int, cleanup bool, state plan.JobState, first *plan.ReadyStep) *workerapi.StepRunRequest {
	st := workerapi.StepRunState{Cancelled: state.Cancelled}
	st.JobRetry, st.JobAttempt = a.jobCounters()
	for _, r := range state.Steps {
		st.Steps = append(st.Steps, workerapi.StepRunResult{Outcome: r.Outcome, Outputs: r.Outputs, CleanupOutcome: r.CleanupOutcome, CleanupOutputs: r.CleanupOutputs})
	}
	if first != nil {
		st.IfAlways = first.IfAlways
		if first.Retry != nil {
			st.MaxRetries = first.Retry.MaxRetries
		}
	}
	var ssh *plan.Inline
	if rs.SSH != nil {
		if named, _ := rs.SSH.SSH.Get().(map[string]any); !plan.NoRemoteHost(named) {
			ssh = rs.SSH
		}
	}
	masked := slices.Compact(slices.Sorted(slices.Values(slices.Concat(a.masked, a.secrets))))
	if len(masked) > 0 && masked[0] == "" {
		masked = masked[1:]
	}
	return &workerapi.StepRunRequest{Step: rs.Index, Try: try, Cleanup: cleanup, SSH: ssh, State: st, Masked: masked}
}

// An elsewhere is how a step run that a step's try, or its cleanup, asked
// for ended, as runElsewhere ran it: the stop the worker sent it, if it
// did; its end, nil if there is none; the problems its end gave, which
// leave the step's retry: and ignore-errors: to say what follows, as a
// command's exit status does; and the problems of the try itself, which
// fail the step.
type elsewhere struct {
	cause    stopCause
	end      *workerapi.StepRun
	acted    problems.Diagnostics
	tryDiags problems.Diagnostics
}

// runElsewhere runs a try of a step, or its cleanup, whose log is key, as
// a step run, as req asks for it, as runChild runs a child: what s watches
// stops it from its start; a try stopped before it asked asks for none;
// and it waits for the step run's end, stopping it as s asks (awaitRun).
// The step's log holds note, if there is one, and the problems the end
// gave, and nothing else: the step's own log is its step run's. A
// coordinator that answers that the attempt no longer holds its job halts
// it.
func (a *attempt) runElsewhere(ctx context.Context, key logKey, req *workerapi.StepRunRequest, s stops, note *problems.Diagnostic) elsewhere {
	path := a.job.Steps[key.step].Path
	stopping, unwatch := a.watch(s)
	defer unwatch()
	requestFailed := func(err error, what string) elsewhere {
		if errors.Is(err, errFenced) || errors.Is(err, errUnauthorized) {
			a.halt(err)
			return elsewhere{cause: stopHalted}
		}
		if a.halted.fired() {
			return elsewhere{cause: stopHalted}
		}
		a.log.Error("a step's request for its step run failed", "step", key.step, "try", key.try, "request", what, "error", a.mask.Mask(err.Error()))
		d := a.at(problems.StepRunRequestFailed, path, problems.Params{"reason": requestFailure(err)})
		return elsewhere{tryDiags: append(problems.Diagnostics{d}, a.writeLog(key, noted(note, problems.Diagnostics{d}))...)}
	}
	select {
	case stop := <-stopping:
		// Stopped before it asked: it asks for none.
		if stop.cause == stopHalted {
			return elsewhere{cause: stopHalted}
		}
		return elsewhere{cause: stop.cause, tryDiags: a.writeLog(key, noted(note, nil))}
	default:
	}
	end, err := a.client.stepRun(ctx, req)
	if err != nil {
		return requestFailed(err, "step run")
	}
	var cause stopCause
	if !end.Ended {
		cause, end, err = awaitRun(ctx, a, key, "step run", stopping,
			func(ctx context.Context) (*workerapi.StepRun, bool, error) {
				end, err := a.client.stepRunEnd(ctx, req.Step, req.Try, req.Cleanup)
				return end, err == nil && end.Ended, err
			},
			func(ctx context.Context, reason plan.Reason) error {
				return a.client.stopStepRun(ctx, &workerapi.StepRunStop{Step: req.Step, Try: req.Try, Cleanup: req.Cleanup, Reason: reason})
			})
		switch {
		case cause == stopHalted:
			return elsewhere{cause: stopHalted}
		case err != nil:
			return requestFailed(err, "end")
		}
	}
	if end.Run == "" && end.Outcome == plan.OutcomeCancelled {
		// The coordinator didn't make the step run, as the worker would
		// have stopped the step had it read the attempt's control first:
		// the try ends as that stop ends it, as runChild's does.
		cause = a.heedNotMade(end.Reason)
	}
	return elsewhere{cause: cause, end: end, acted: end.Diagnostics, tryDiags: a.writeLog(key, noted(note, end.Diagnostics))}
}

// heedNotMade returns why a step's try, whose run the coordinator didn't
// make, cancelled for reason, as the attempt's control would have stopped
// it, was stopped, and has the worker do as that control asks from now on,
// as though it had read it: no reason is a job of the run having failed,
// for a step whose early-cancel: is any-job-failed.
func (a *attempt) heedNotMade(reason plan.Reason) stopCause {
	switch reason {
	case "":
		a.fire(a.anyJobFailed, nil)
		return stopEarlyCancel
	case plan.ReasonTimeout:
		a.obey(&workerapi.Control{Cancel: true, Reason: reason})
		return stopJobTimeout
	}
	a.obey(&workerapi.Control{Cancel: true, Reason: reason})
	return stopCancelled
}

// runStepRunTry runs t, a try of a step whose ssh: puts it elsewhere, as a
// step run, in state, its job's as the step was evaluated, as runOwnTry
// runs a command: its exit status and its outcome are its step run's, and
// what its step wrote to $OUTPUTS there, once it succeeded, is appended to
// the file the step would have written here (takeOutputs), which is read
// as a command's. It returns how the step ended, from end, and whether to
// try it again; or that it was cut short, if the job was halted.
func (a *attempt) runStepRunTry(ctx context.Context, t *startedTry, state plan.JobState, end stepEnd) (stepEnd, bool) {
	e := a.runElsewhere(ctx, t.key, a.stepRunRequest(t.rs, t.try, false, state, t.first), t.s, t.note)
	if a.halted.fired() || e.cause == stopHalted {
		end.cut = true
		return end, false
	}
	exit, succeeded, tryDiags := a.endedElsewhere(e, t.out, t.path)
	return a.endOwnTry(t, end, exit, e.cause, succeeded, e.acted, tryDiags)
}

// endedElsewhere returns, of a step run that ended as e, its step's exit
// status, or -1 for none, whether it succeeded, and the problems of the
// try: e's own, and those of what its step wrote to $OUTPUTS, which, once
// it succeeded, it appends to f, the file the step, at path, would have
// written here (takeOutputs).
func (a *attempt) endedElsewhere(e elsewhere, f outputsFile, path string) (int, bool, problems.Diagnostics) {
	if e.end == nil {
		return -1, false, e.tryDiags
	}
	succeeded := e.cause == noStop && e.end.Outcome == plan.OutcomeSuccess
	tryDiags := e.tryDiags
	if succeeded {
		tryDiags = append(tryDiags, a.takeOutputs(f, path, e.end)...)
	}
	return e.end.ExitCode, succeeded, tryDiags
}

// takeOutputs makes what a step run's step wrote to $OUTPUTS, end's, its
// step's here: it masks each that its step run says is private or holds a
// secret as a secret of the job's, at least, so that the job's logs never
// show it, and an output that holds it is a secret (plan.JobState.Secrets),
// whatever else it holds, but one shorter than the phases mask of what is
// made from a secret (plan.MinDerived), which would mask common text, as
// its step run's only says it holds what the worker masks already; and it
// appends them all to f, the file the step, at path, would have written
// (appendOutputs), whose problem, if it had one, it returns: none of them
// for one whose line would be too long to read. A file that isn't one,
// readOutputs reports as it reads it.
func (a *attempt) takeOutputs(f outputsFile, path string, end *workerapi.StepRun) problems.Diagnostics {
	for _, name := range slices.Concat(end.Private, end.Secrets) {
		if v := end.Outputs[name]; len(v) >= plan.MinDerived && !slices.Contains(a.secrets, v) {
			a.secrets = append(a.secrets, v)
			a.mask.Add(v)
		}
	}
	err := appendOutputs(a.files, f.name, end.Outputs)
	if long, ok := errors.AsType[*outputLineTooLongError](err); ok {
		return problems.Diagnostics{a.at(problems.StepRunOutputLineTooLong, path, problems.Params{"name": long.name, "limit": maxOutputsLine})}
	}
	if err != nil && !errors.Is(err, errOutputsNotFile) {
		a.log.Error("a step run's outputs couldn't be appended to the step's $OUTPUTS", "path", path, "error", a.mask.Mask(err.Error()))
		return problems.Diagnostics{a.at(problems.StepRunOutputsUnwritten, path, problems.Params{"reason": err.Error()})}
	}
	return nil
}

// runCleanupElsewhere runs the cleanup: of the step at index i, at path,
// readied as rs, in state, as a step run of its own, as runCleanup runs a
// cleanup: here: only the attempt ending without the worker stops it, and
// what its step wrote to $OUTPUTS is the job's, out. It reports its
// events, with diags, Cleanup's, and returns its outcome and its outputs,
// or no outcome if the job was halted. Its ignore-errors: lets a cleanup
// whose command exited other than 0 succeed, as here.
func (a *attempt) runCleanupElsewhere(ctx context.Context, i int, path string, rs *plan.ReadyStep, state plan.JobState, out outputsFile, diags problems.Diagnostics) (plan.Outcome, map[string]string) {
	a.events.add(workerapi.Event{Time: now(), Step: i, Cleanup: true})
	e := a.runElsewhere(ctx, logKey{workerapi.LogCleanup, i, 0}, a.stepRunRequest(rs, 0, true, state, nil), stops{}, nil)
	if a.halted.fired() || e.cause == stopHalted {
		return "", nil
	}
	exit, succeeded, tryDiags := a.endedElsewhere(e, out, path)
	outputs, outputsDiags := a.outputsWritten(out, path)
	tryDiags = append(tryDiags, outputsDiags...)
	outcome := plan.OutcomeFailure
	if !tryDiags.HasErrors() && (succeeded || rs.IgnoreErrors && exit > 0 && !e.acted.HasErrors()) {
		outcome = plan.OutcomeSuccess
	}
	a.finished(workerapi.Event{Step: i, Cleanup: true, Outcome: outcome, ExitCode: exit, Outputs: outputs, Diagnostics: slices.Concat(diags, e.acted, tryDiags)})
	return outcome, outputs
}

// jobCounters returns the job's retry counter and its attempt's number, as
// the attempt's claim gives them, or, for a step run's, as its caller's
// claim did (workerapi.StepRunState), which its step reads as its job's.
func (a *attempt) jobCounters() (retry, attempt int) {
	if sr := a.claim.StepRun; sr != nil {
		return sr.State.JobRetry, sr.State.JobAttempt
	}
	return a.claim.Retry, a.claim.Attempt
}

// runAsStepRun runs the attempt's job as a step run's (workerapi.Claim.StepRun):
// its one step, or its cleanup:, as the step at its index in the job, over
// the state of its caller's job that the claim gives, which the job's
// other steps, there by their id alone, never run. It evaluates the step
// with its own variables, as format 1 does once it has connected to the
// host, but not its if:, nor its cleanup's, which its caller decided
// (plan.JobState.FirstTry),
// and runs it, with an $OUTPUTS of its own, empty, in its run's directory,
// as any step; but one told to stop, as its caller's stop of it, or the
// end of its caller's attempt, tells it, never runs, or is stopped,
// whatever its if: says. It returns the attempt's end: how its step ended,
// as it ran, never as its ignore-errors: or retry: would make it, which
// its caller applies; what its step wrote to $OUTPUTS, never the job's
// outputs:, which are its caller's, with those that hold what the worker
// masks named private and secret; and the step's problems, after those of
// the job's env:, jobDiags. It returns nil if the job was halted.
func (a *attempt) runAsStepRun(ctx context.Context, jobDiags problems.Diagnostics) *workerapi.Complete {
	sr := a.claim.StepRun
	i, try := sr.Step, sr.Try
	if sr.Cleanup {
		try = 0
	}
	ended := func(outcome plan.Outcome, exit int, outputs map[string]string, diags problems.Diagnostics) *workerapi.Complete {
		a.finished(workerapi.Event{Step: i, Try: try, Cleanup: sr.Cleanup, Outcome: outcome, ExitCode: exit, Outputs: outputs, Diagnostics: diags})
		end := &workerapi.Complete{Result: plan.JobResult{Outcome: outcome, Outputs: outputs}, Diagnostics: a.maskDiagnostics(slices.Concat(jobDiags, diags))}
		for name, v := range outputs {
			if a.mask.Mask(v) != v {
				end.Result.Private = append(end.Result.Private, name)
			}
		}
		slices.Sort(end.Result.Private)
		end.Result.Secrets = slices.Clone(end.Result.Private)
		return end
	}
	if i < 0 || i >= len(a.job.Steps) || sr.Cleanup && a.job.Steps[i].Cleanup.IsZero() || len(sr.State.Steps) != i && !sr.Cleanup || sr.Cleanup && len(sr.State.Steps) != len(a.job.Steps) {
		a.log.Error("a step run's claim names a step its job doesn't have, or a state not of the steps before it", "step", i, "cleanup", sr.Cleanup, "results", len(sr.State.Steps))
		return &workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: a.maskDiagnostics(append(jobDiags, a.at(problems.EvaluationFailed, a.job.Path, nil)))}
	}
	path := a.job.Steps[i].Path
	key := logKey{workerapi.LogStep, i, try}
	if sr.Cleanup {
		key.part = workerapi.LogCleanup
	}
	// What stops it is its own control alone, which tells it its caller's
	// stop of it, whatever its step's if: says: whether the caller's job
	// is being cancelled (StepRunState.Cancelled) is the step's
	// cancelled(), never a stop of its own.
	s := stops{cancellable: true}
	if cause := a.stopCauseOf(s); cause != noStop {
		if cause == stopHalted {
			return nil
		}
		return ended(plan.OutcomeCancelled, -1, nil, nil)
	}
	results := make([]plan.StepResult, len(sr.State.Steps))
	for k, r := range sr.State.Steps {
		results[k] = plan.StepResult{Outcome: r.Outcome, Outputs: r.Outputs, CleanupOutcome: r.CleanupOutcome, CleanupOutputs: r.CleanupOutputs}
	}
	state := a.state(results, sr.State.Cancelled, nil)
	id := a.job.Steps[i].ID
	if sr.Cleanup {
		id = ""
	}
	out := a.outputsFor(i, id)
	var counters *stepTry
	if a.job.Steps[i].Retry != nil {
		// Format 1 tells a cleanup of a step with a retry: that it is its
		// first try of none.
		counters = &stepTry{}
		if !sr.Cleanup {
			counters = &stepTry{try, sr.State.MaxRetries}
		}
	}
	state.Env = a.variables(out.path, counters)
	var (
		rs    *plan.ReadyStep
		diags problems.Diagnostics
		err   error
	)
	// Its if: is its caller's to decide, as format 1 decides it before it
	// connects: the step's, which carries over the word always, or its
	// cleanup's, which its caller's Cleanup decided runs.
	state.FirstTry = &plan.ReadyStep{IfAlways: sr.State.IfAlways}
	if sr.Cleanup {
		rs, diags, err = a.job.Cleanup(ctx, i, state)
	} else {
		state.Try = try
		rs, diags, err = a.job.Step(ctx, i, state)
	}
	if a.halted.fired() {
		return nil
	}
	switch {
	case err != nil:
		a.log.Error("readying a step run's step failed", "step", i, "error", a.mask.Mask(err.Error()))
		return ended(plan.OutcomeFailure, -1, nil, append(diags, a.at(problems.EvaluationFailed, path, nil)))
	case diags.HasErrors() || rs == nil:
		return ended(plan.OutcomeFailure, -1, nil, diags)
	case rs.Skipped:
		// A cleanup of a step the state says was skipped, which its caller
		// would have asked none for.
		a.log.Error("a step run's claim is of the cleanup of a step that was skipped", "step", i)
		return ended(plan.OutcomeFailure, -1, nil, append(diags, a.at(problems.EvaluationFailed, path, nil)))
	}
	a.addMasked(rs.Masked...)
	// Its job is where the coordinator put its step, as it evaluated its
	// ssh:, which the step runs on, whatever runnable makes of it.
	note, refused, _ := a.runnable(rs, path)
	if refused != nil {
		return ended(plan.OutcomeFailure, -1, nil, append(diags, *refused))
	}
	if out.err != nil {
		return ended(plan.OutcomeFailure, -1, nil, append(diags, a.notStarted(out.err, path)))
	}
	dir := a.workingDirectory(rs)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ended(plan.OutcomeFailure, -1, nil, append(diags, a.notStarted(err, path)))
	}
	if note != nil {
		diags = append(diags, *note)
	}
	seq := a.events.add(workerapi.Event{Time: now(), Step: i, Try: try, Cleanup: sr.Cleanup})
	if !a.firstStarted {
		// Whether the workflow's code may have run must always be known.
		if cause := a.await(s, func() (bool, <-chan struct{}) { return a.events.isStored(seq) }); cause != noStop {
			if cause == stopHalted {
				return nil
			}
			return ended(plan.OutcomeCancelled, -1, nil, diags)
		}
		a.firstStarted = true
	}
	var (
		exit      = -1
		cause     stopCause
		succeeded bool
		acted     problems.Diagnostics
		tryDiags  problems.Diagnostics
	)
	switch {
	case slices.Contains(platformActions, rs.Uses):
		acted = problems.Diagnostics{a.at(problems.ActionNeedsPlatform, path, problems.Params{"uses": rs.Uses})}
		tryDiags = a.writeLog(key, noted(note, acted))
	case rs.Uses != "":
		succeeded, cause, acted, tryDiags = a.runAction(ctx, key, rs, s, note)
	default:
		exit, cause, tryDiags = a.runCommand(key, rs, rs.Run, dir, a.stepEnv(rs, state.Env), s, note)
		succeeded = cause == noStop && exit == 0
	}
	if a.halted.fired() {
		return nil
	}
	outputs, outputsDiags := a.outputsWritten(out, path)
	diags = slices.Concat(diags, acted, tryDiags, outputsDiags)
	outcome := plan.OutcomeFailure
	switch {
	case cause != noStop:
		outcome = plan.OutcomeCancelled
	case diags.HasErrors():
	case succeeded:
		outcome = plan.OutcomeSuccess
	}
	return ended(outcome, exit, outputs, diags)
}
