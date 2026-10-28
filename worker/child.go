package worker

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// childRequest returns what the worker asks for the child run of the try
// try of the step rs, whose uses: names a workflow, with: its with: and
// what of it is private, as Step gave them; its environment, Step's
// (plan.ReadyStep.Env), with vars over it, the variables the worker
// defines for the step (variables), but only those format 1 defines
// (plan.IsFormatVariable), and never the step's process's own, which holds
// the platform's credential, PW_API_KEY, which is no such variable; and
// masked, the strings the worker masks for the step, which the child's
// jobs mask too. As in format 1, which hands a called workflow its calling
// step's whole environment, the step's own variables are among them, as
// OUTPUTS and its retry counters: the child's worker sets its own over
// them for each step of the child's, but the counters of a step without a
// retry:, which read its caller's, as format 1's do.
func childRequest(rs *plan.ReadyStep, try int, vars map[string]string, masked []string) *workerapi.ChildRequest {
	env := maps.Clone(rs.Env)
	for name, v := range vars {
		if plan.IsFormatVariable(name) {
			if env == nil {
				env = map[string]string{}
			}
			env[name] = v
		}
	}
	masked = slices.Compact(slices.Sorted(slices.Values(masked)))
	if len(masked) > 0 && masked[0] == "" {
		masked = masked[1:]
	}
	return &workerapi.ChildRequest{
		Step: rs.Index, Try: try, With: rs.With, Env: env,
		WithPrivate: rs.WithPrivate, WithSecrets: rs.WithSecrets, EnvPrivate: rs.EnvPrivate, EnvSecrets: rs.EnvSecrets,
		Masked: masked, WorkingDirectory: rs.WorkingDirectory,
	}
}

// callsWorkflow reports whether the step at index i of the attempt's job
// calls a workflow: its uses: names one, which the plan pinned
// (plan.Step.Source), in an attempt of version 3 of the worker API or
// later, which has the paths for its child. Of an earlier one, the step
// can't run, as before the API had them.
func (a *attempt) callsWorkflow(i int) bool {
	return a.job.Steps[i].Source != nil && a.claim.APIVersion >= workerapi.Version3
}

// A called is how the try of a step that calls a workflow ended, as
// runChild ran it: the stop the worker sent its child, if it did; whether
// the child ended by itself as a success, or cancelled, from within it or
// by a person's cancel of it; the problems the coordinator gave of a child
// it didn't make, which leave the step's retry: and ignore-errors: to say
// what follows, as a command's exit status does; and the problems of the
// try itself, which fail the step.
type called struct {
	cause     stopCause
	succeeded bool
	cancelled bool
	acted     problems.Diagnostics
	tryDiags  problems.Diagnostics
}

// runChild runs a try of the step rs, whose uses: names a workflow, whose
// log is key, as a run of its own (docs/design.md, Workflows a step
// calls). What s watches stops the try from its start, as a command's: a
// timer of the step's runs from then. It makes the child's directory,
// cleaned first when clean is set where it may be (makeChildDir), asks
// the coordinator for the child with exactly what childRequest builds, of
// Step's env and vars, the variables the worker defines for the step, and
// waits for the child's end, heartbeating, as the attempt does throughout
// (awaitChild).
// A try stopped before it asked asks for no child. The step's log holds
// note, if there is one, and the problems the coordinator gave of a child
// it didn't make, and nothing else: the child's logs are its own run's. A
// coordinator that answers that the attempt no longer holds its job halts
// it.
func (a *attempt) runChild(ctx context.Context, key logKey, rs *plan.ReadyStep, s stops, note *problems.Diagnostic, vars map[string]string, clean bool) called {
	path := a.job.Steps[key.step].Path
	stopping, unwatch := a.watch(s)
	defer unwatch()
	failed := func(d problems.Diagnostic) called {
		return called{tryDiags: append(problems.Diagnostics{d}, a.writeLog(key, noted(note, problems.Diagnostics{d}))...)}
	}
	requestFailed := func(err error, what string) called {
		if errors.Is(err, errFenced) || errors.Is(err, errUnauthorized) {
			a.halt(err)
			return called{cause: stopHalted}
		}
		if a.halted.fired() {
			return called{cause: stopHalted}
		}
		a.log.Error("a step's request for its child failed", "step", key.step, "try", key.try, "request", what, "error", a.mask.Mask(err.Error()))
		return failed(a.at(problems.ChildRequestFailed, path, problems.Params{"uses": rs.Uses, "reason": requestFailure(err)}))
	}
	dir := plan.ChildDir(a.claim.RunDir, rs.WorkingDirectory, a.job.Key, key.step)
	if err := a.makeChildDir(dir, clean); err != nil {
		return failed(a.at(problems.ChildDirUnusable, path, problems.Params{"dir": dir, "reason": err.Error()}))
	}
	select {
	case req := <-stopping:
		// Stopped before it asked: it asks for none.
		if req.cause == stopHalted {
			return called{cause: stopHalted}
		}
		return called{cause: req.cause, tryDiags: a.writeLog(key, noted(note, nil))}
	default:
	}
	child, err := a.client.child(ctx, childRequest(rs, key.try, vars, slices.Concat(a.masked, a.secrets)))
	if err != nil {
		return requestFailed(err, "child")
	}
	var cause stopCause
	if !child.Ended {
		cause, child, err = a.awaitChild(ctx, key, stopping)
		switch {
		case cause == stopHalted:
			return called{cause: stopHalted}
		case err != nil:
			return requestFailed(err, "end")
		}
	}
	if child.Run == "" && child.Outcome == plan.OutcomeCancelled {
		// The coordinator didn't make the child, as the worker would have
		// stopped the step had it read the attempt's control first: the
		// try ends as that stop ends it, and the worker does as the control
		// asks from now on, as though it had read it.
		cause = a.heedNotMade(child.Reason)
	}
	acted := child.Diagnostics
	return called{
		cause:     cause,
		succeeded: cause == noStop && child.Outcome == plan.OutcomeSuccess,
		cancelled: cause == noStop && child.Outcome == plan.OutcomeCancelled,
		acted:     acted,
		tryDiags:  a.writeLog(key, noted(note, acted)),
	}
}

// awaitChild waits for the end of the child of the step's try, key's, and
// returns it, and why the worker stopped it, if it did, as awaitRun says.
func (a *attempt) awaitChild(ctx context.Context, key logKey, stopping <-chan stopRequest) (stopCause, *workerapi.Child, error) {
	return awaitRun(ctx, a, key, "child", stopping,
		func(ctx context.Context) (*workerapi.Child, bool, error) {
			child, err := a.client.childEnd(ctx, key.step, key.try)
			return child, err == nil && child.Ended, err
		},
		func(ctx context.Context, reason plan.Reason) error {
			return a.client.stopChild(ctx, &workerapi.ChildStop{Step: key.step, Try: key.try, Reason: reason})
		})
}

// awaitRun waits for the end of a run that a step's try, key's, asked for,
// its child or its step run, what names which, as poll gives it, polled
// until it says it has ended, and returns it, and why the worker stopped
// it, if it did: on the first of the requests stopping sends, the try's
// stops, it stops it (stop), for its reason, once, and waits on. A poll
// for its end that fails for good stops it first, cancelled, if nothing
// has, so that the run doesn't outlive the step that waits for it; one
// whose stop fails too runs on until its attempt ends (docs/design.md,
// Stopping it). It returns stopHalted once the attempt has ended without
// the worker, or the worker is stopping, and the error of a request that
// failed.
func awaitRun[T any](ctx context.Context, a *attempt, key logKey, what string, stopping <-chan stopRequest, poll func(context.Context) (T, bool, error), stop func(context.Context, plan.Reason) error) (stopCause, T, error) {
	var zero T
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type polled struct {
		run T
		err error
	}
	ended := make(chan polled, 1)
	go func() {
		for {
			run, done, err := poll(pollCtx)
			if err != nil || done {
				ended <- polled{run, err}
				return
			}
		}
	}()
	cause := noStop
	stopRun := func(reason plan.Reason) error {
		a.log.Info("stopping a step's "+what, "step", key.step, "try", key.try, "reason", reason)
		return stop(ctx, reason)
	}
	for {
		select {
		case req := <-stopping:
			if req.cause == stopHalted {
				return stopHalted, zero, nil
			}
			if cause != noStop {
				// A stop that comes sooner, as a deadline after a cancel,
				// stops the run no more: it was asked to stop once.
				continue
			}
			cause = req.cause
			reason := plan.ReasonCancelled
			if cause == stopJobTimeout || cause == stopStepTimeout || cause == stopTryTimeout {
				reason = plan.ReasonTimeout
			}
			if err := stopRun(reason); err != nil {
				if a.halted.fired() {
					return stopHalted, zero, nil
				}
				return cause, zero, err
			}
		case p := <-ended:
			if a.halted.fired() {
				return stopHalted, zero, nil
			}
			if p.err != nil && cause == noStop && !errors.Is(p.err, errFenced) && !errors.Is(p.err, errUnauthorized) {
				if err := stopRun(plan.ReasonCancelled); err != nil {
					a.log.Error("a step's "+what+" couldn't be stopped, so it runs on until its attempt ends", "step", key.step, "try", key.try, "error", a.mask.Mask(err.Error()))
				}
			}
			return cause, p.run, p.err
		}
	}
}

// end returns how the try of a step that calls a workflow ends, from how
// runChild ran it, c, for the step rs, readied for the try, as its first
// try, first, decided, and last, whether no try is left after it, with
// timedOut, the problem of a try that ran past limit, its step's
// timeout: or its retry:'s: the outcome its event reports; the step's
// result, as its job counts it; why the try was stopped, as its job heeds
// it; the reason the job ends for, for a step whose stop cancels its job
// as only a call's does; and whether the step is tried again. As format 1
// maps a call's end, from the child's and its stop rather than the child's
// outcome alone:
//
//   - A child the worker stopped for the job's cancellation ends the try
//     cancelled, as any step it stops, which cancels the job; one stopped
//     for the job's timeout, or the run's, ends it cancelled too, with the
//     reason timeout, and the job with it, rather than failed, as a
//     command the deadline stops is; and one stopped for the step's
//     early-cancel: ends it cancelled while the job goes on, the step
//     counting as a success for it. So does one stopped for the step's
//     timeout:, of a step without a retry:.
//   - A deadline of the try's own otherwise, its retry:'s timeout, or the
//     timeout: of a step with a retry:, which always has a timeout of its
//     tries, ends it as a command that ran out of time: failed, with
//     step_timed_out, and tried again for its retry:'s timeout while a try
//     is left, but not for its timeout:; or, on its last try, a success
//     if its ignore-errors: says so.
//   - A child that ended cancelled by itself, from within or by a person's
//     cancel of it, ends the step cancelled while its job goes on.
//   - One that failed, or that the coordinator didn't make, fails the try,
//     which its retry: tries again, and its ignore-errors: lets the last
//     succeed; one that succeeded succeeds.
func (c called) end(rs, first *plan.ReadyStep, last bool, timedOut func(limit int64) problems.Diagnostic) (event, result plan.Outcome, cause stopCause, reason plan.Reason, retry bool, diags problems.Diagnostics) {
	diags = slices.Concat(c.acted, c.tryDiags)
	triesTimed := first.Retry != nil && first.Retry.TimeoutSeconds > 0
	switch {
	case c.tryDiags.HasErrors():
		return plan.OutcomeFailure, plan.OutcomeFailure, noStop, "", false, diags
	case c.cause == stopCancelled:
		return plan.OutcomeCancelled, plan.OutcomeCancelled, stopCancelled, "", false, diags
	case c.cause == stopJobTimeout:
		return plan.OutcomeCancelled, plan.OutcomeCancelled, stopCancelled, plan.ReasonTimeout, false, diags
	case c.cause == stopTryTimeout, c.cause == stopStepTimeout && triesTimed:
		if rs.IgnoreErrors && last {
			return plan.OutcomeSuccess, plan.OutcomeSuccess, noStop, "", false, diags
		}
		if c.cause == stopStepTimeout {
			return plan.OutcomeFailure, plan.OutcomeFailure, noStop, "", false, append(diags, timedOut(rs.TimeoutSeconds))
		}
		return plan.OutcomeFailure, plan.OutcomeFailure, noStop, "", !last, append(diags, timedOut(first.Retry.TimeoutSeconds))
	case c.cause != noStop, c.cancelled:
		// The step's early-cancel:, the timeout: of a step without a
		// retry:, or a cancel of the child alone.
		return plan.OutcomeCancelled, plan.OutcomeSuccess, noStop, "", false, diags
	case c.succeeded, rs.IgnoreErrors && last:
		return plan.OutcomeSuccess, plan.OutcomeSuccess, noStop, "", false, diags
	}
	return plan.OutcomeFailure, plan.OutcomeFailure, noStop, "", !last, diags
}

// makeChildDir makes dir, the directory of the run a step calls, and
// cleans it first when clean is set, as the step's clean-on-retry: says,
// but never where it is the attempt's run directory or holds it, nor in
// a run directory its member chose (workerapi.Claim.RunDirOnMember):
// format 1 cleans a called run's directory only on the machine the run
// started on, never its copy on a host, and never its root's directory,
// which, as the attempt's is in its root's, this spares too. It makes it
// in the directory of the step's root run, where the coordinator puts it
// (plan.ChildDir), a directory at a time from there, never following a
// symbolic link, which the run's own steps could have left in the way
// (makeDirIn). A directory outside the root's it leaves for the
// coordinator, which refuses it (child_dir_outside).
func (a *attempt) makeChildDir(dir string, clean bool) error {
	root := a.claim.RunDir
	if tree := a.claim.Tree; tree != nil {
		root = tree.RootDir
	}
	rel, ok := plan.InDir(root, dir)
	if !ok {
		return nil
	}
	_, holdsRunDir := plan.InDir(dir, a.claim.RunDir)
	return makeDirIn(root, rel, clean && !a.claim.RunDirOnMember && !holdsRunDir)
}

// beforeOpen, if a test sets it, is called by makeDirIn with the path of
// each directory it has checked, before it opens it.
var beforeOpen atomic.Pointer[func(at string)]

// makeDirIn makes the directory rel, relative to the directory root, which
// must be there, a directory at a time, readable by its user alone, and
// cleans it first, with what it holds, when clean is set. Each directory
// it makes, or finds, is opened as a root of its own (os.Root) before the
// next is made in it, and checked to be the directory it found, not a
// symbolic link, nor one that a link put there since: one that is a link
// is an error, too many levels of symbolic links, as a system that
// doesn't follow links says, and one that isn't a directory is an error
// too. Nothing it does reaches outside root, whatever links root holds.
func makeDirIn(root, rel string, clean bool) error {
	r, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { r.Close() }()
	if rel == "." {
		return nil
	}
	parts := strings.Split(rel, string(filepath.Separator))
	for i, name := range parts {
		at := filepath.Join(root, filepath.Join(parts[:i+1]...))
		checked := func() (fs.FileInfo, error) {
			fi, err := r.Lstat(name)
			switch {
			case err != nil:
				return nil, err
			case fi.Mode()&fs.ModeSymlink != 0:
				return nil, &fs.PathError{Op: "open", Path: at, Err: syscall.ELOOP}
			case !fi.IsDir():
				return nil, &fs.PathError{Op: "open", Path: at, Err: syscall.ENOTDIR}
			}
			return fi, nil
		}
		if clean && i == len(parts)-1 {
			// A link is never followed, nor removed: it is an error, as on
			// the way.
			if _, err := checked(); err == nil {
				if err := r.RemoveAll(name); err != nil {
					return err
				}
			} else if !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
		if err := r.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		fi, err := checked()
		if err != nil {
			return err
		}
		if hook := beforeOpen.Load(); hook != nil {
			(*hook)(at)
		}
		sub, err := r.OpenRoot(name)
		if err != nil {
			return err
		}
		if opened, err := sub.Stat("."); err != nil || !os.SameFile(fi, opened) {
			sub.Close()
			return &fs.PathError{Op: "open", Path: at, Err: syscall.ELOOP}
		}
		r.Close()
		r = sub
	}
	return nil
}
