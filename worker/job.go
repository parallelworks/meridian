package worker

import (
	"cmp"
	"context"
	"errors"
	"io"
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

// execute runs the attempt's job: it sets it up, evaluates its env:, runs
// its steps, then their cleanup: commands, and evaluates its outputs. It
// returns the attempt's end, or nil if the job was halted.
func (a *attempt) execute(ctx context.Context) *workerapi.Complete {
	a.maskSecrets()
	problem := a.setUp()
	if a.halted.fired() {
		return nil
	}
	if problem != nil {
		return &workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonSetupFailed, Problem: problem}
	}
	if a.job.TimeoutSeconds > 0 {
		t := time.AfterFunc(seconds(a.job.TimeoutSeconds)-time.Since(a.claimed), func() {
			a.timeOut(time.Now().Add(a.tm.grace), "the job ran past its timeout")
		})
		defer t.Stop()
	}
	jobDiags, end, done := a.evaluateJobEnv(ctx)
	if done {
		return end
	}
	// What the coordinator asked before the claim applies from the first
	// step: a cancel, the run's timeout, or a job of the run failing.
	a.awaitFirstControl()

	// A job halted before a step's turn, or a cleanup's, hasn't ended, and
	// neither has one halted as a step, or a cleanup, ran, which it cut
	// short. One halted once the last has ended, and been reported, has:
	// stopped from outside, as by its scheduler's time limit, it still
	// evaluates its outputs and sends its end, in the flush period; ended
	// without the worker (lost), it doesn't.
	r := a.runSteps(ctx)
	if r == nil || !a.runCleanups(ctx, r) {
		return nil
	}
	return a.endJob(ctx, r, jobDiags)
}

// maskSecrets masks the run's secrets from now on (addMasked), and the
// job's own, which the phases count as a secret's own text, as they count
// the run's (attempt.secrets).
func (a *attempt) maskSecrets() {
	a.mask = plan.NewMasker()
	a.addMasked(a.claim.Run.Masked()...)
	// The platform's secret variables are secrets of the job's, which the
	// phases count as a secret's own text, as they count the run's.
	for _, name := range slices.Sorted(maps.Keys(a.claim.Env)) {
		if v := a.claim.Env[name]; v.Secret {
			a.secrets = append(a.secrets, v.Value)
		}
	}
	// So are, for a child run's job, the strings the worker of the step
	// that called it masked (workerapi.Tree.Masked), in each phase, so that
	// one of them, however short, is masked in the job's logs, and an
	// output that holds one is a secret.
	if tree := a.claim.Tree; tree != nil {
		for _, s := range tree.Masked {
			if s != "" {
				a.secrets = append(a.secrets, s)
			}
		}
	}
	a.mask.Add(a.secrets...)
}

// evaluateJobEnv evaluates the job's env: once for the attempt, as the job
// starts, and its steps, their cleanups and its outputs read what it gave.
// It returns the problems it had; and done, with the attempt's end, when
// the job ends here: nil if it was halted, and a failure for an env: that
// failed, which fails the job before any step.
func (a *attempt) evaluateJobEnv(ctx context.Context) (diags problems.Diagnostics, end *workerapi.Complete, done bool) {
	jobEnv, jobDiags, err := a.job.JobEnv(ctx, a.state(nil, false, a.variables("", nil)))
	if a.halted.fired() {
		return nil, nil, true
	}
	if err != nil {
		a.log.Error("evaluating the job's env: failed", "error", a.mask.Mask(err.Error()))
		jobDiags = append(jobDiags, a.at(problems.EvaluationFailed, a.job.Path, nil))
	}
	if jobDiags.HasErrors() {
		return nil, &workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: a.maskDiagnostics(jobDiags)}, true
	}
	a.jobEnv = jobEnv
	a.addMasked(jobEnv.Masked...)
	return jobDiags, nil, false
}

// A stepsRun is what the job's steps came to, as runSteps ran them, which
// their cleanups and the job's end read.
type stepsRun struct {
	results []plan.StepResult
	// ifFailed is set for each step whose if: failed to evaluate.
	ifFailed []bool
	// cancelled is set once a step has ended cancelled because the
	// coordinator cancelled the job: cancelled() is true for the steps
	// after, as the Cancelled of their state. As in format 1, a cancel
	// cancels the job through the step it stops, or the next that comes
	// to run, which ends cancelled without starting; until then
	// cancelled() is false.
	cancelled bool
	// stopped is set once the job is stopping, for why says: its steps
	// after that don't run, but those that outlive it.
	stopped bool
	why     stopCause
	// deadlineFailed is set once the job's timeout:, or the run's, has
	// failed a step: one it stopped, or one that came to run after it, but
	// for a step that ignores its errors on its last try, which succeeds.
	deadlineFailed bool
	// cancelReason is why the step that cancelled the job did, when it
	// says: a step that calls a workflow that the job's timeout, or the
	// run's, stopped cancels the job, for the reason timeout, as format 1
	// ends it, rather than fail it. Otherwise the job is cancelled for the
	// coordinator's reason.
	cancelReason plan.Reason
}

// runSteps runs the job's steps in turn (runStep), and returns what they
// came to, or nil if the job was halted before a step's turn, or as one
// ran.
func (a *attempt) runSteps(ctx context.Context) *stepsRun {
	r := &stepsRun{ifFailed: make([]bool, len(a.job.Steps))}
	for i := range a.job.Steps {
		if a.halted.fired() {
			return nil
		}
		if !r.stopped {
			// The first of the job's stops to fire says why it stops, as
			// format 1's context keeps the reason it ended with first.
			switch cause := a.stopCauseOf(stops{cancellable: true}); cause {
			case stopCancelled, stopJobTimeout:
				r.stopped, r.why = true, cause
			}
		}
		// As in format 1, a job past its timeout isn't cancelled, nor is
		// one whose run is past the run's: the step it was running failed,
		// or succeeded if it ignores its errors, and cancelled() is false
		// for the steps after it, even if the coordinator cancels it
		// after. A step that ended cancelled, as a cancel or early-cancel:
		// stops one, makes cancelled() true for those after it, as the
		// Step phase reads from their results.
		end := a.runStep(ctx, i, a.state(r.results, r.cancelled, nil), r.stopped, r.why)
		if a.stepEnded != nil {
			a.stepEnded(a, i, false)
		}
		if end.cut {
			return nil
		}
		r.results = append(r.results, end.result)
		r.ifFailed[i] = end.ifFailed
		if end.cause == stopJobTimeout && end.result.Outcome == plan.OutcomeFailure {
			r.deadlineFailed = true
		}
		if end.cause == stopCancelled && !r.cancelled {
			r.cancelled, r.cancelReason = true, end.reason
		}
		if !r.stopped && (end.cause == stopCancelled || end.cause == stopEarlyCancel || end.cause == stopJobTimeout) {
			r.stopped, r.why = true, end.cause
		}
		if end.result.Outcome == plan.OutcomeFailure {
			a.fire(a.anyJobFailed, nil)
		}
	}
	return r
}

// runCleanups runs the steps' cleanup: commands, as format 1 does once
// the job's steps have, in reverse order, each as a step of its own,
// whatever happened to the job: the Cleanup phase skips one whose step was
// skipped, and the worker one whose step's if: failed to evaluate, which
// format 1 counts as not having run either. It records how each ended in
// r, and reports whether the job goes on to its end: not if it was halted
// before a cleanup's turn, or as one ran.
func (a *attempt) runCleanups(ctx context.Context, r *stepsRun) bool {
	for i, s := range slices.Backward(a.job.Steps) {
		if s.Cleanup.IsZero() {
			continue
		}
		if a.halted.fired() {
			return false
		}
		if r.ifFailed[i] {
			a.finished(workerapi.Event{Step: i, Cleanup: true, Outcome: plan.OutcomeSkipped, ExitCode: -1})
			r.results[i].CleanupOutcome = plan.OutcomeSkipped
			continue
		}
		outcome, outputs := a.runCleanup(ctx, i, a.state(r.results, r.cancelled, nil))
		if a.stepEnded != nil {
			a.stepEnded(a, i, true)
		}
		if outcome == "" {
			return false
		}
		r.results[i].CleanupOutcome, r.results[i].CleanupOutputs = outcome, outputs
	}
	return true
}

// jobOutcome returns how the job ended, as its steps and their cleanups
// did, r, and, for one that failed past its deadline, or was cancelled,
// why.
func (a *attempt) jobOutcome(r *stepsRun) (plan.Outcome, plan.Reason) {
	failed := slices.ContainsFunc(r.results, func(s plan.StepResult) bool {
		// As in format 1, a cleanup that fails fails its job.
		return s.Outcome == plan.OutcomeFailure || s.CleanupOutcome == plan.OutcomeFailure
	})
	skipped := len(r.results) > 0 && !slices.ContainsFunc(r.results, func(s plan.StepResult) bool {
		return s.Outcome != plan.OutcomeSkipped
	})
	// As in format 1, a job is cancelled only by a step that ended
	// cancelled: one whose steps after a cancel all outlive it, or are
	// skipped, ends as they did.
	stepCancelled := slices.ContainsFunc(r.results, func(s plan.StepResult) bool {
		return s.Outcome == plan.OutcomeCancelled
	})
	switch {
	case r.why == stopJobTimeout && r.deadlineFailed:
		// A job past its timeout, or whose run is, fails, for the reason
		// timeout, when the deadline failed a step. One whose deadline
		// failed none, as when it stopped only steps that ignore their
		// errors, which succeed, ends as its steps did, as in format 1.
		return plan.OutcomeFailure, plan.ReasonTimeout
	case failed:
		return plan.OutcomeFailure, ""
	case stepCancelled:
		if !r.cancelled {
			return plan.OutcomeCancelled, ""
		}
		return plan.OutcomeCancelled, cmp.Or(r.cancelReason, a.cancelReason)
	case skipped:
		// As in format 1, a job whose steps were all skipped is.
		return plan.OutcomeSkipped, ""
	}
	return plan.OutcomeSuccess, ""
}

// endJob evaluates the job's outputs once its steps have ended, as r says
// they did, and returns the attempt's end, with jobDiags, the problems of
// its env:, even if the worker is being stopped from outside, as at its
// scheduler's time limit; or nil if the attempt has ended without it.
func (a *attempt) endJob(ctx context.Context, r *stepsRun, jobDiags problems.Diagnostics) *workerapi.Complete {
	outcome, reason := a.jobOutcome(r)
	end := &workerapi.Complete{Reason: reason}
	res, diags, err := a.job.End(context.WithoutCancel(ctx), a.state(r.results, r.cancelled, a.variables("", nil)))
	if a.lost() {
		return nil
	}
	if err != nil {
		a.log.Error("evaluating the job's outputs failed", "error", a.mask.Mask(err.Error()))
		diags = append(diags, a.at(problems.EvaluationFailed, a.job.Path, nil))
	}
	if diags.HasErrors() && outcome != plan.OutcomeCancelled {
		outcome = plan.OutcomeFailure
	}
	end.Diagnostics = a.maskDiagnostics(append(jobDiags, diags...))
	if res == nil {
		// Without End's word on which outputs hold a secret, none is
		// reported, lest one reach run state.
		end.Result.Outcome = outcome
		return end
	}
	// The outputs, the job's and its steps', are as the steps wrote them
	// and End made them, none of them masked, as format 1 passes them to
	// the jobs that need the job: End says which are private and which
	// hold a secret, which the coordinator keeps out of run state.
	end.Result = res.Result(outcome)
	return end
}

// addMasked masks values from now on, in the job's logs, and in what the
// phases tell holds a secret (plan.JobState.Masked).
func (a *attempt) addMasked(values ...string) {
	a.mask.Add(values...)
	a.masked = append(a.masked, values...)
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
		// Masked before the message is made of them, so that it can't
		// quote a secret, escaped or not.
		p := a.maskProblem(k.Problem(a.maskParams(params)))
		for _, l := range logs {
			l.Error("the job failed before its first step", "problem", p.Detail)
		}
		return p
	}
	info("claimed the attempt", "run", a.claim.RunID, "attempt", a.claim.AttemptID, "number", a.claim.Attempt,
		"worker", a.worker.Version, "digest", a.worker.Digest)

	job, err := plan.DecodeReadyJob(a.claim.PlanVersion, a.claim.Job)
	if err != nil {
		k, params := decodeFailure(a.claim.PlanVersion, err)
		return fail(k, params)
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
	if err := a.makeRunDir(); err != nil {
		return fail(problems.RunDirUnusable, problems.Params{"path": a.claim.RunDir, "reason": err.Error()})
	}
	info("running the job's steps on the host", "dir", a.claim.RunDir)
	return nil
}

// makeRunDir makes the run's directory, as the job's steps run in it: a
// child run's, in its root's directory, where its caller's worker made it
// (makeChildDir), as that worker makes it, never following a symbolic
// link, which a step of its tree could have left in the way since; and any
// other's as it is, as the meridian command makes a root's private.
func (a *attempt) makeRunDir() error {
	if tree := a.claim.Tree; tree != nil {
		if rel, ok := inDir(tree.RootDir, a.claim.RunDir); ok {
			return makeDirIn(tree.RootDir, rel, false)
		}
	}
	return os.MkdirAll(a.claim.RunDir, 0o700)
}

// decodeFailure says why a job of plan version version didn't decode,
// from err, DecodeReadyJob's, as a kind of problem, without the decoder's
// own text, which can quote the job, and in it a value made from a secret,
// before the worker knows what masks it (ReadyJob.Masked). The job is
// JSON, as the claim that holds it decoded. One of a version the worker
// reads that names what it doesn't know, as a function a newer release's
// format has, is one it can't read, as one encoded otherwise is.
func decodeFailure(version int, err error) (*problems.Kind, problems.Params) {
	if errors.Is(err, plan.ErrVersion) && version > plan.Version {
		return problems.JobVersionUnsupported, problems.Params{"version": version, "supported": plan.Version}
	}
	return problems.JobUnreadable, nil
}

// state returns what the worker knows of its job, for its phases: the
// results of the steps that have had their turn, whether its steps have
// seen it cancelled, and the variables the worker defines (variables).
func (a *attempt) state(results []plan.StepResult, cancelled bool, env map[string]string) plan.JobState {
	return plan.JobState{
		Run:   a.claim.Run,
		Needs: a.claim.Needs,
		Job: plan.JobContext{
			Name: a.job.Name, Key: a.job.Key, Attempt: a.claim.Attempt, Group: a.claim.Group, Member: a.claim.Member,
		},
		Steps:     slices.Clip(results),
		Cancelled: cancelled,
		Env:       env,
		JobEnv:    a.jobEnv,
		Masked:    slices.Clip(a.masked),
		Secrets:   a.secrets,
	}
}

// A stepEnd is how a step that the worker readied ended: its result, why
// its last try was stopped, if it was, as its job heeds it, and the reason
// the job is cancelled for, for a step that cancels it with a reason of
// its own; and whether its if: failed to evaluate; or that the job was
// halted before it ended, cut short, when how it ended isn't known, and
// nothing reports it.
type stepEnd struct {
	result   plan.StepResult
	cause    stopCause
	reason   plan.Reason
	ifFailed bool
	cut      bool
}

// runStep readies the step at index i with the Step phase, in state, for
// each of its tries, as format 1 evaluates a step again for each, runs it,
// with format 1's retry:, and reports its events. The job is stopping, if
// stopped is set, for why. It returns how the step ended, or, if the job
// was halted before it did, that it was cut short.
func (a *attempt) runStep(ctx context.Context, i int, state plan.JobState, stopped bool, why stopCause) stepEnd {
	out := a.outputsFor(i, a.job.Steps[i].ID)
	var (
		end stepEnd
		// first is the step as Step readied its first try: a later try
		// takes its if:, retry: and whether it outlives a cancel from it.
		first *plan.ReadyStep
	)
	for try := 0; ; try++ {
		if try > 0 {
			// The wait between tries stops for the job's cancellation and
			// timeout only.
			ready, stop := passed(seconds(first.Retry.IntervalSeconds))
			cause := a.await(stops{cancellable: !first.OutlivesCancel}, ready)
			stop()
			if cause != noStop {
				// As in format 1, a deadline that passes as a step waits to
				// try again fails it, whatever its ignore-errors: says.
				return a.notRun(i, try, end, cause, nil, false)
			}
		}
		t, ended := a.prepareTry(ctx, i, try, state, first, stopped, why, out, end)
		if t == nil {
			return ended
		}
		first = t.first
		if ended, started := a.beginTry(t, end); !started {
			return ended
		}
		retry := false
		if t.calls {
			end, retry = a.runCallTry(ctx, t, end)
		} else {
			end, retry = a.runOwnTry(ctx, t, end)
		}
		if end.cut || !retry {
			return end
		}
	}
}

// A startedTry is a try of a step that prepareTry readied and beginTry
// started, which runCallTry or runOwnTry runs.
type startedTry struct {
	// i is the step's index, try the try's, from 0, and path the step's.
	i, try int
	path   string
	// rs is the try as Step readied it, and first the step's first try,
	// rs too for that one; vars are the variables the worker defines for
	// it, and out the step's $OUTPUTS.
	rs, first *plan.ReadyStep
	vars      map[string]string
	out       outputsFile
	// diags are Step's problems with the try, and the note runnable made of
	// it, if it made one, which note is too.
	diags problems.Diagnostics
	note  *problems.Diagnostic
	// calls is set for a step whose uses: names a workflow, and dir is where
	// one that doesn't runs.
	calls bool
	dir   string
	// s says what stops the try, and last whether it is its step's last:
	// beginTry sets both, and until it has, they are zero.
	s    stops
	last bool
	key  logKey
}

// prepareTry readies the try try of the step at index i with the Step
// phase, in state, as readyTry does, given first, the step's first try,
// nil for that one, and checks that the worker can run it, and that the
// job's stopping, if stopped is set, for why, doesn't stop it, as it
// stops a first try that doesn't outlive it. It returns the try, which
// beginTry starts; or, for one that can't start, nil and how the step
// ended, from end, what its earlier tries made of it: failed, skipped or
// not run (notRun), or cut short if the job was halted. out is the step's
// $OUTPUTS.
func (a *attempt) prepareTry(ctx context.Context, i, try int, state plan.JobState, first *plan.ReadyStep, stopped bool, why stopCause, out outputsFile, end stepEnd) (*startedTry, stepEnd) {
	path := a.job.Steps[i].Path
	rs, vars, diags, err := a.readyTry(ctx, i, state, try, first, out.path)
	if a.halted.fired() {
		end.cut = true
		return nil, end
	}
	// A try that can't start fails its step, which isn't tried again:
	// what stopped it would stop the next.
	fail := func(more ...problems.Diagnostic) (*startedTry, stepEnd) {
		a.finished(workerapi.Event{Step: i, Try: try, LastTry: true, Outcome: plan.OutcomeFailure, ExitCode: -1, Diagnostics: append(diags, more...)})
		end.result.Outcome = plan.OutcomeFailure
		return nil, end
	}
	switch {
	case err != nil:
		// Not the workflow's fault, but the step can't run.
		a.log.Error("readying a step failed", "step", i, "try", try, "error", a.mask.Mask(err.Error()))
		return fail(a.at(problems.EvaluationFailed, path, nil))
	case diags.HasErrors() || rs == nil:
		end.ifFailed = rs != nil && rs.IfFailed
		return fail()
	case rs.Skipped:
		a.finished(workerapi.Event{Step: i, Outcome: plan.OutcomeSkipped, ExitCode: -1, Diagnostics: diags})
		end.result.Outcome = plan.OutcomeSkipped
		return nil, end
	}
	a.addMasked(rs.Masked...)
	if try == 0 {
		first = rs
		if stopped && !rs.OutlivesCancel {
			// The job stopped as the step was readied: as in format
			// 1, it ends cancelled, or failed past the job's timeout,
			// unless it outlives that.
			return nil, a.notRun(i, try, end, why, diags, rs.IgnoreErrors && (rs.Retry == nil || rs.Retry.MaxRetries == 0))
		}
	}
	note, refused := a.runnable(rs, path)
	if refused != nil {
		return fail(*refused)
	}
	if out.err != nil {
		return fail(a.notStarted(out.err, path))
	}
	// A step that calls a workflow runs none of its own: its child's
	// directory is made in the root's, a directory at a time
	// (runChild).
	calls := a.callsWorkflow(i)
	dir := a.workingDirectory(rs)
	if !calls {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fail(a.notStarted(err, path))
		}
	}
	if note != nil {
		diags = append(diags, *note)
	}
	return &startedTry{
		i: i, try: try, path: path, rs: rs, first: first, vars: vars, out: out, diags: diags, note: note,
		calls: calls, dir: dir, key: logKey{workerapi.LogStep, i, try},
	}, end
}

// beginTry starts t, a try that prepareTry readied, unless what stops it
// already has: it sets what stops it, and whether it is its step's last,
// and reports that it starts, waiting, for the job's first step, until the
// coordinator has stored that. It reports whether the try started, and,
// if it didn't, how the step ended, from end (notRun).
func (a *attempt) beginTry(t *startedTry, end stepEnd) (stepEnd, bool) {
	rs, first := t.rs, t.first
	// As format 1 runs a step: one that outlives its job's
	// cancellation and timeout outlives its own timeout: too, but not
	// its retry:'s timeout or its early-cancel:.
	s := stops{cancellable: !first.OutlivesCancel, earlyCancel: rs.EarlyCancel == plan.EarlyCancelAnyJobFailed}
	if !first.OutlivesCancel {
		s.step = seconds(rs.TimeoutSeconds)
	}
	last := true
	if first.Retry != nil {
		s.try = seconds(first.Retry.TimeoutSeconds)
		last = t.try >= first.Retry.MaxRetries
	}
	t.s, t.last = s, last
	if cause := a.stopCauseOf(s); cause != noStop {
		return a.notRun(t.i, t.try, end, cause, t.diags, rs.IgnoreErrors && last), false
	}
	seq := a.events.add(workerapi.Event{Time: now(), Step: t.i, Try: t.try})
	if !a.firstStarted {
		// Whether the workflow's code may have run must always be
		// known: the coordinator stores that the first step is
		// starting before it starts.
		if cause := a.await(s, func() (bool, <-chan struct{}) { return a.events.isStored(seq) }); cause != noStop {
			return a.notRun(t.i, t.try, end, cause, t.diags, rs.IgnoreErrors && last), false
		}
		a.firstStarted = true
	}
	return end, true
}

// runCallTry runs t, a try of a step that calls a workflow, whose child is
// a run of its own, whose end is the try's, as format 1 maps it
// (called.end), and reports its end. Format 1 cleans the child's directory
// before each try after the first, as its clean-on-retry: says. It
// returns how the step ended, from end, and whether to try it again; or
// that it was cut short, if the job was halted.
func (a *attempt) runCallTry(ctx context.Context, t *startedTry, end stepEnd) (stepEnd, bool) {
	c := a.runChild(ctx, t.key, t.rs, t.s, t.note, t.vars, t.try > 0 && t.first.Retry != nil && t.first.Retry.CleanOnRetry)
	if a.halted.fired() || c.cause == stopHalted {
		end.cut = true
		return end, false
	}
	timedOut := func(limit int64) problems.Diagnostic {
		return a.at(problems.StepTimedOut, t.path, problems.Params{"limit": limit})
	}
	event, result, cause, reason, retry, tryDiags := c.end(t.rs, t.first, t.last, timedOut)
	a.finished(workerapi.Event{Step: t.i, Try: t.try, LastTry: event == plan.OutcomeFailure && !retry, Outcome: event, ExitCode: -1, Diagnostics: slices.Concat(t.diags, tryDiags)})
	end.result = plan.StepResult{Outcome: result}
	end.cause, end.reason = cause, reason
	return end, retry
}

// runOwnTry runs t, a try of a step that runs a command or uses an action,
// reports its end, with what it wrote to $OUTPUTS, and returns how the
// step ended, from end, and whether to try it again; or that it was cut
// short, if the job was halted.
func (a *attempt) runOwnTry(ctx context.Context, t *startedTry, end stepEnd) (stepEnd, bool) {
	var (
		exit     int
		cause    stopCause
		tryDiags problems.Diagnostics
		// acted are the problems an action reported: why its try
		// failed, which, unlike a command's problems, leaves its
		// retry: and its ignore-errors: to say what follows, as they
		// do for a command that exits other than 0, and its warnings.
		acted problems.Diagnostics
		// succeeded is set for a try that succeeded: a command that
		// exited 0, or an action that did, by itself.
		succeeded bool
	)
	rs := t.rs
	switch {
	case slices.Contains(platformActions, rs.Uses):
		// As in format 1, an action fails as it runs, and the try
		// with it, as a command that fails does: no platform here
		// provides this one. Its log says why, as a note is.
		acted = problems.Diagnostics{a.at(problems.ActionNeedsPlatform, t.path, problems.Params{"uses": rs.Uses})}
		exit, tryDiags = -1, a.writeLog(t.key, noted(t.note, acted))
	case rs.Uses != "":
		// runnable lets through only an action the coordinator
		// performs.
		exit = -1
		succeeded, cause, acted, tryDiags = a.runAction(ctx, t.key, rs, t.s, t.note)
	default:
		exit, cause, tryDiags = a.runCommand(t.key, rs, rs.Run, t.dir, a.stepEnv(rs, t.vars), t.s, t.note)
		succeeded = cause == noStop && exit == 0
	}
	if a.halted.fired() {
		end.cut = true
		return end, false
	}
	// What a try wrote is its step's, however it ended, as in format 1.
	outputs, outputsDiags := a.outputsWritten(t.out, t.path)
	tryDiags = append(tryDiags, outputsDiags...)

	outcome, retry := plan.OutcomeFailure, false
	switch {
	case cause == stopCancelled || cause == stopEarlyCancel:
		outcome = plan.OutcomeCancelled
	case tryDiags.HasErrors():
	case succeeded:
		outcome = plan.OutcomeSuccess
	case rs.IgnoreErrors && t.last:
		// As format 1: a step whose last try fails succeeds, even one
		// a deadline stopped: its own timeout:, the job's or the
		// run's.
		outcome = plan.OutcomeSuccess
	case cause == stopJobTimeout:
		// Nor is a step a deadline stopped tried again.
	case cause == stopStepTimeout:
		// A step past its own timeout: isn't tried again.
		tryDiags = append(tryDiags, a.at(problems.StepTimedOut, t.path, problems.Params{"limit": rs.TimeoutSeconds}))
	case cause == stopTryTimeout:
		tryDiags = append(tryDiags, a.at(problems.StepTimedOut, t.path, problems.Params{"limit": t.first.Retry.TimeoutSeconds}))
		retry = !t.last
	default:
		retry = !t.last
	}
	a.finished(workerapi.Event{Step: t.i, Try: t.try, LastTry: outcome == plan.OutcomeFailure && !retry, Outcome: outcome, ExitCode: exit, Outputs: outputs, Diagnostics: slices.Concat(t.diags, acted, tryDiags)})
	end.result = plan.StepResult{Outcome: outcome, Outputs: outputs}
	end.cause = cause
	return end, retry
}

// A stepTry is a try of a step with a retry:, from 0, and how many more
// times it may run, or -1 when that isn't known yet, as format 1 tells a
// step in PW_WORKFLOW_STEP_CURRENT_RETRY and PW_WORKFLOW_STEP_MAX_RETRIES.
type stepTry struct{ current, most int }

// readyTry evaluates a try of the step at index i with the Step phase, in
// state, with the variables the worker defines for it, which it returns
// too, and the path of its $OUTPUTS. A later try is given the first, from
// which it takes what that decided.
func (a *attempt) readyTry(ctx context.Context, i int, state plan.JobState, try int, first *plan.ReadyStep, outputs string) (*plan.ReadyStep, map[string]string, problems.Diagnostics, error) {
	var counters *stepTry
	switch {
	case first != nil && first.Retry != nil:
		counters = &stepTry{try, first.Retry.MaxRetries}
	case a.job.Steps[i].Retry != nil:
		counters = &stepTry{0, -1}
	}
	state.Try, state.FirstTry = try, first
	state.Env = a.variables(outputs, counters)
	rs, diags, err := a.job.Step(ctx, i, state)
	if try == 0 && err == nil && rs != nil && rs.Retry != nil {
		// How many more times the step may run is known once Step has
		// read its retry:, as its first try is evaluated: that try is
		// evaluated again, with it, as format 1 gives it.
		state.Env = a.variables(outputs, &stepTry{0, rs.Retry.MaxRetries})
		rs, diags, err = a.job.Step(ctx, i, state)
	}
	return rs, state.Env, diags, err
}

// notRun reports the try of a step that didn't start, because the job is
// stopping for cause, with diags, Step's, and returns how the step ended,
// as in format 1, with the outputs its earlier tries wrote: cancelled; or,
// for the job's timeout or the run's, failed, as format 1 fails a step it
// starts once their deadline has passed, unless ignored is set, for a try
// that is its step's last of a step whose ignore-errors: is true, which
// succeeds, as format 1's does.
func (a *attempt) notRun(i, try int, end stepEnd, cause stopCause, diags problems.Diagnostics, ignored bool) stepEnd {
	if cause == stopHalted {
		end.cut = true
		return end
	}
	outcome := plan.OutcomeCancelled
	switch {
	case cause == stopJobTimeout && ignored:
		outcome = plan.OutcomeSuccess
	case cause == stopJobTimeout:
		outcome = plan.OutcomeFailure
	}
	a.finished(workerapi.Event{Step: i, Try: try, LastTry: outcome == plan.OutcomeFailure, Outcome: outcome, ExitCode: -1, Diagnostics: diags})
	end.result.Outcome, end.cause = outcome, cause
	return end
}

// runCleanup readies the cleanup: of the step at index i with the Cleanup
// phase, in state, and runs it as format 1 runs it: as a step of its own,
// which the job's cancellation and its timeout don't stop, with no timeout
// or retry, which only the attempt ending without the worker stops. What
// it writes to $OUTPUTS is the job's. It reports its events, and returns
// its outcome and its outputs, or no outcome if the job was halted.
func (a *attempt) runCleanup(ctx context.Context, i int, state plan.JobState) (plan.Outcome, map[string]string) {
	path := a.job.Steps[i].Path
	out := a.outputsFor(i, "")
	// Format 1 tells the cleanup of a step with a retry: that it is its
	// first try of none.
	var counters *stepTry
	if a.job.Steps[i].Retry != nil {
		counters = &stepTry{}
	}
	vars := a.variables(out.path, counters)
	state.Env = vars
	rs, diags, err := a.job.Cleanup(ctx, i, state)
	if a.halted.fired() {
		return "", nil
	}
	fail := func(more ...problems.Diagnostic) (plan.Outcome, map[string]string) {
		a.finished(workerapi.Event{Step: i, Cleanup: true, Outcome: plan.OutcomeFailure, ExitCode: -1, Diagnostics: append(diags, more...)})
		return plan.OutcomeFailure, nil
	}
	switch {
	case err != nil:
		a.log.Error("readying a cleanup failed", "step", i, "error", a.mask.Mask(err.Error()))
		return fail(a.at(problems.EvaluationFailed, path, nil))
	case diags.HasErrors() || rs == nil:
		return fail()
	case rs.Skipped:
		a.finished(workerapi.Event{Step: i, Cleanup: true, Outcome: plan.OutcomeSkipped, ExitCode: -1, Diagnostics: diags})
		return plan.OutcomeSkipped, nil
	}
	a.addMasked(rs.Masked...)
	note, refused := a.runnable(rs, path)
	if refused != nil {
		return fail(*refused)
	}
	if out.err != nil {
		return fail(a.notStarted(out.err, path))
	}
	dir := a.workingDirectory(rs)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fail(a.notStarted(err, path))
	}
	if note != nil {
		diags = append(diags, *note)
	}
	a.events.add(workerapi.Event{Time: now(), Step: i, Cleanup: true})
	exit, _, runDiags := a.runCommand(logKey{workerapi.LogCleanup, i, 0}, rs, rs.Run, dir, a.stepEnv(rs, vars), stops{}, note)
	if a.halted.fired() {
		return "", nil
	}
	outputs, outputsDiags := a.outputsWritten(out, path)
	diags = append(append(diags, runDiags...), outputsDiags...)
	outcome := plan.OutcomeSuccess
	if diags.HasErrors() || exit != 0 && !rs.IgnoreErrors {
		outcome = plan.OutcomeFailure
	}
	a.finished(workerapi.Event{Step: i, Cleanup: true, Outcome: outcome, ExitCode: exit, Outputs: outputs, Diagnostics: diags})
	return outcome, outputs
}

// runCommand runs script, a step's run: or cleanup: command, with the
// step's shell, in dir with env, and its output, masked, in the log key,
// after note, if there is one, until it ends, or what s says stops it. It
// returns its exit status, why it was stopped, and the problems with
// running it: a command that didn't start, or a log that wasn't kept
// whole.
func (a *attempt) runCommand(key logKey, rs *plan.ReadyStep, script, dir string, env []string, s stops, note *problems.Diagnostic) (int, stopCause, problems.Diagnostics) {
	path := a.job.Steps[key.step].Path
	var diags problems.Diagnostics
	stream := a.spool.open(key)
	w := a.mask.Writer(stream)
	exit, cause, err := func() (int, stopCause, error) {
		if note != nil {
			// What the log doesn't keep is a problem of its own (LogNotKept).
			_, _ = io.WriteString(w, note.Message()+"\n")
		}
		made, file, err := a.writeScript(script)
		if err != nil {
			return -1, noStop, err
		}
		defer made.remove()
		cmd := shellCommand(rs.Shell, file)
		cmd.Dir, cmd.Env = dir, env
		requests, unwatch := a.watch(s)
		defer unwatch()
		defer a.endedGroup(cmd)
		return runProcess(cmd, w, requests, a.startedGroup, a.terminate)
	}()
	w.Close()
	stream.Close()
	a.liveGroups()
	if err != nil {
		diags = append(diags, a.notStarted(err, path))
	}
	if dropped := stream.droppedBytes(); dropped > 0 {
		diags = append(diags, a.at(problems.LogNotKept, path, problems.Params{"size": dropped, "limit": a.spool.limit()}))
	}
	return exit, cause, diags
}

// writeLog writes the messages of ds, a line each, masked, as the whole
// log key, of a step's try that runs no command, as runCommand writes a
// note before what its command prints. It returns the problem of a log
// that wasn't kept whole.
func (a *attempt) writeLog(key logKey, ds problems.Diagnostics) problems.Diagnostics {
	stream := a.spool.open(key)
	w := a.mask.Writer(stream)
	for _, d := range ds {
		_, _ = io.WriteString(w, d.Message()+"\n")
	}
	w.Close()
	stream.Close()
	if dropped := stream.droppedBytes(); dropped > 0 {
		return problems.Diagnostics{a.at(problems.LogNotKept, a.job.Steps[key.step].Path, problems.Params{"size": dropped, "limit": a.spool.limit()})}
	}
	return nil
}

// jobOutputs is the name of the $OUTPUTS file the job's steps without an
// id share, as in format 1.
const jobOutputs = "outputs"

// writeScript writes a command to a script of its own, in the worker's
// directory, which only its user can read, since it can hold secrets, and
// returns how to remove it, and its path.
func (a *attempt) writeScript(command string) (madeFile, string, error) {
	a.scripts++
	name := "script-" + strconv.Itoa(a.scripts) + ".sh"
	f, made, path, err := a.files.create(name, os.O_WRONLY|os.O_EXCL)
	if err != nil {
		return madeFile{}, "", err
	}
	_, err = f.WriteString(command + "\n")
	return made, path, errors.Join(err, f.Close())
}

// makeOutputs makes name, in the worker's directory, an $OUTPUTS file, and
// returns its path, and the directory's generation: an empty one if empty
// is set, or else the one there. A step may have removed it, or put
// something else in its place: a symbolic link to a regular file is kept,
// as format 1 follows it, and anything else is replaced.
func (a *attempt) makeOutputs(name string, empty bool) (string, int, error) {
	path, root, gen, err := a.files.current()
	if err != nil {
		return "", 0, err
	}
	if info, err := root.Lstat(name); err == nil {
		keep := !empty && info.Mode().IsRegular()
		if !empty && info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Stat(filepath.Join(path, name))
			keep = err == nil && target.Mode().IsRegular()
		}
		if keep {
			return filepath.Join(path, name), gen, nil
		}
		if err := root.Remove(name); err != nil {
			return "", 0, err
		}
	}
	f, _, path, err := a.files.create(name, os.O_WRONLY|os.O_EXCL)
	if err != nil {
		return "", 0, err
	}
	return path, gen, f.Close()
}

// An outputsFile is the $OUTPUTS file a step, or a cleanup, writes to, in
// the worker's directory: a step's own, for a step with an id, or else the
// job's, which the steps without one and the cleanups share, as in format
// 1.
type outputsFile struct {
	name string
	own  bool
	// path is its path, and gen the generation of the directory it is in,
	// or err why it couldn't be made.
	path string
	gen  int
	err  error
	// before is what the job's file held before the step ran, and
	// beforeErr why it couldn't be read, from which what the step wrote is
	// told.
	before    map[string]string
	beforeErr error
}

// outputsFor makes the $OUTPUTS file of the step at index i, whose id is
// id, or of a cleanup, for "", which writes the job's.
func (a *attempt) outputsFor(i int, id string) outputsFile {
	f := outputsFile{name: jobOutputs, own: id != ""}
	if f.own {
		f.name = "outputs-" + strconv.Itoa(i)
	}
	f.path, f.gen, f.err = a.makeOutputs(f.name, f.own)
	if f.err == nil && !f.own {
		f.before, _, _, f.beforeErr = readOutputs(a.files, f.name)
	}
	return f
}

// outputsWritten returns the outputs a step, at path, or a cleanup, wrote to f:
// what its own file holds, or what it changed of the job's. It returns the
// problems with them too, as diagnostics at path. The worker doesn't trust
// what is at its path once the directory f is in went as the step ran.
func (a *attempt) outputsWritten(f outputsFile, path string) (map[string]string, problems.Diagnostics) {
	var diags problems.Diagnostics
	if !a.files.still(f.gen) {
		diags = append(diags, a.at(problems.OutputsRemoved, path, nil))
	}
	got, notUTF8, size, err := readOutputs(a.files, f.name)
	switch {
	case errors.Is(err, errOutputsNotFile):
		diags = append(diags, a.at(problems.OutputsNotFile, path, nil))
	case errors.Is(err, errOutputsTooLarge):
		// Only the step that took the shared file past the limit fails;
		// those after it can't add to it.
		if f.own || !errors.Is(f.beforeErr, errOutputsTooLarge) {
			diags = append(diags, a.at(problems.OutputsTooLarge, path, problems.Params{"size": size, "limit": maxOutputs}))
		}
	case errors.Is(err, errOutputsLineTooLong):
		return map[string]string{}, append(diags, a.at(problems.OutputsLineTooLong, path, problems.Params{"limit": maxOutputsLine + 1}))
	case err != nil:
	case f.own:
		return got, append(diags, a.outputsNotUTF8(got, notUTF8, path)...)
	default:
		got = written(f.before, got)
		return got, append(diags, a.outputsNotUTF8(got, notUTF8, path)...)
	}
	return nil, diags
}

// outputsNotUTF8 returns a warning, at path, of those of a step's outputs
// that notUTF8 names, in which the worker replaced bytes that aren't UTF-8
// (readOutputs), or none: it counts them, and names the first of them as
// their names sort. Format 1 replaces them as silently as it stores
// its outputs, but the jobs that need the job read what the step didn't
// write.
func (a *attempt) outputsNotUTF8(outputs map[string]string, notUTF8 map[string]bool, path string) problems.Diagnostics {
	var names []string
	for name := range outputs {
		if notUTF8[name] {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return problems.Diagnostics{a.at(problems.OutputsNotUTF8, path, problems.Params{"name": slices.Min(names), "count": len(names)})}
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

// variables returns the variables the worker defines, as format 1 lays
// them over the environment the workflow gives a step: PW_JOB_ID and
// PW_PARENT_JOB_DIR for a run of its own, or its root's for a child run
// (workerapi.Tree), unless the platform gives them; the platform's
// (workerapi.Claim.Env); and the worker's own over them: a child run's
// PW_WORKFLOW_NAME, format 1's name for the call that made it,
// PW_JOB_DIR, PW_MATRIX_INDEX for a matrix instance, and the job's retry
// counters for a job with a retry:, and, for a step, OUTPUTS, when outputs
// isn't "", and the step's retry counters, for a try of a step with a
// retry:. The phases read those format 1 defines, and in the job's env:,
// working-directory: and outputs: only those it defines as the job starts
// (plan.JobState.Env); a step's process has them all.
func (a *attempt) variables(outputs string, try *stepTry) map[string]string {
	env := map[string]string{"PW_JOB_ID": a.claim.RunID, "PW_PARENT_JOB_DIR": a.claim.RunDir}
	tree := a.claim.Tree
	if tree != nil {
		env["PW_JOB_ID"], env["PW_PARENT_JOB_DIR"] = tree.JobID, tree.RootDir
	}
	for name, v := range a.claim.Env {
		env[name] = v.Value
	}
	if tree != nil && tree.WorkflowName != "" {
		env["PW_WORKFLOW_NAME"] = tree.WorkflowName
	}
	env["PW_JOB_DIR"] = a.claim.RunDir
	if k := strings.LastIndexByte(a.job.Key, '#'); k >= 0 && !a.job.Matrix.IsZero() {
		env["PW_MATRIX_INDEX"] = a.job.Key[k+1:]
	}
	if a.job.Retry != nil {
		env["PW_WORKFLOW_JOB_CURRENT_RETRY"] = strconv.Itoa(a.claim.Retry)
		env["PW_WORKFLOW_JOB_MAX_RETRIES"] = strconv.Itoa(a.job.Retry.MaxRetries)
	}
	if outputs != "" {
		env["OUTPUTS"] = outputs
	}
	if try != nil {
		env["PW_WORKFLOW_STEP_CURRENT_RETRY"] = strconv.Itoa(try.current)
		if try.most >= 0 {
			env["PW_WORKFLOW_STEP_MAX_RETRIES"] = strconv.Itoa(try.most)
		}
	}
	return env
}

// stepEnv returns the environment a step's try, or its cleanup: command,
// runs with: the worker's, but for what told it how to reach the
// coordinator; the environment the workflow gives the step over it; and
// over that, as format 1 lays them, vars, the variables the worker
// defines for it (variables).
func (a *attempt) stepEnv(rs *plan.ReadyStep, vars map[string]string) []string {
	env := map[string]string{}
	for _, kv := range a.environ {
		name, value, _ := strings.Cut(kv, "=")
		env[name] = value
	}
	env["CI"] = "true"
	maps.Copy(env, rs.Env)
	maps.Copy(env, vars)
	out := make([]string, 0, len(env))
	for _, name := range slices.Sorted(maps.Keys(env)) {
		out = append(out, name+"="+env[name])
	}
	return out
}

// finished reports a step's, or its cleanup's, finished event, with its
// outputs and its problems masked, and without LastTry in an attempt of a
// version of the API that doesn't have it.
func (a *attempt) finished(e workerapi.Event) {
	e.Time, e.Finished = now(), true
	e.LastTry = e.LastTry && a.claim.APIVersion >= workerapi.Version2
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

// at returns a diagnostic of kind k at path, made of params masked, so
// that its message can't quote a secret, escaped or not.
func (a *attempt) at(k *problems.Kind, path string, params problems.Params) problems.Diagnostic {
	return k.At("", problems.Position{}, path, a.maskParams(params))
}

// notStarted returns why a step, or a cleanup, at path couldn't start, as
// err says. A problem of the worker's own directory's, which are the
// meridian command's (dirProblems), as when the worker can make its
// directory in none of its parents, is WorkerDirUnusable, which names the
// first of them by its code and the directory its path param names, and
// the worker's log has each. Anything else is StepNotStarted: with what
// the operating system said, which is all else that reaches it today, or,
// for a problem of another kind, its code, so that no sentence of
// Meridian's own is its reason.
func (a *attempt) notStarted(err error, path string) problems.Diagnostic {
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		if dirProblems[p.Code] {
			dir, _ := p.Params["path"].(string)
			return a.at(problems.WorkerDirUnusable, path, problems.Params{"path": dir, "code": string(p.Code)})
		}
		return a.at(problems.StepNotStarted, path, problems.Params{"reason": string(p.Code)})
	}
	return a.at(problems.StepNotStarted, path, problems.Params{"reason": err.Error()})
}

// dirProblems are the codes of the problems the worker's own directory can
// have: safepath's, and its own.
var dirProblems = map[problem.Code]bool{
	problems.DirectoryUnavailable.Code(): true,
	problems.NotADirectory.Code():        true,
	problems.DirectoryNotOwned.Code():    true,
	problems.DirectoryNotPrivate.Code():  true,
	problems.DirectoryReplaced.Code():    true,
	problems.PathUnsafe.Code():           true,
	problems.PathLinksTooMany.Code():     true,
	problems.OwnerUnmapped.Code():        true,
	problems.OwnerUnknown.Code():         true,
}

// maskProblem returns p with what it says masked.
func (a *attempt) maskProblem(p *problem.Problem) *problem.Problem {
	p.Detail = a.mask.Mask(p.Detail)
	p.Params = a.maskParams(p.Params)
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
