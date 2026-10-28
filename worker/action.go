package worker

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// runnable checks that the worker can run a step, as Step or Cleanup
// readied it: not one that uses: an action or a workflow, nor one with a
// shell format 1 doesn't run, which it can't run yet, but for an action
// only a platform provides, which fails as it runs (platformActions), one
// the coordinator performs, in an attempt of a version of the worker API
// that has it (asksCoordinator), and a workflow, in such an attempt
// (callsWorkflow); and where format 1 runs it: where its
// job runs, unless its ssh: names a host, which this
// worker can't run a step on yet, but for one whose ssh: names the host
// and login its job's does, which placed the worker there (sameSSH). A
// step whose ssh: is null, or gives no remoteHost, runs where its job
// would run without its ssh:, which is here, unless the job's own ssh:
// placed it on that host; one that gives no remoteHost has a note in its
// log, as format 1 writes. It returns the note, and the problem that
// refuses the step.
func (a *attempt) runnable(rs *plan.ReadyStep, path string) (note, refused *problems.Diagnostic) {
	diagnostic := func(k *problems.Kind, params problems.Params) *problems.Diagnostic {
		d := a.at(k, path, params)
		return &d
	}
	if rs.Uses != "" && !slices.Contains(platformActions, rs.Uses) && !a.asksCoordinator(rs.Uses) && !a.callsWorkflow(rs.Index) {
		return nil, diagnostic(problems.UsesNotSupported, problems.Params{"uses": rs.Uses})
	}
	if shell, ok := shellOK(rs.Shell); !ok {
		return nil, diagnostic(problems.ShellNotSupported, problems.Params{"shell": shell})
	}
	named := false
	if rs.SSH != nil {
		ssh, _ := rs.SSH.SSH.Get().(map[string]any)
		named = !plan.NoRemoteHost(ssh)
	}
	job := a.job.Target.Inline
	switch {
	case named && job != nil && sameSSH(rs.SSH.SSH, job.SSH):
		// A step whose ssh: names its job's host and login runs where its
		// job runs: here.
		return nil, nil
	case named:
		return nil, diagnostic(problems.StepSSHNotSupported, nil)
	case (rs.NoSSH || rs.SSH != nil) && job != nil:
		return nil, diagnostic(problems.StepOffJobHost, nil)
	case rs.SSH != nil:
		return diagnostic(problems.SSHHostEmpty, nil), nil
	}
	return nil, nil
}

// platformActions are the built-in actions only a platform provides: an
// agent of its own on a cluster's compute node, which scheduler-agent
// starts and wait-for-agent waits for. A step that uses one fails each
// try as it runs, as an action that fails does in format 1, and only
// then: an if: can skip it, or its job; its retry: tries it again; and
// its ignore-errors: lets its last try succeed.
var platformActions = []string{"parallelworks/scheduler-agent", "parallelworks/wait-for-agent"}

// asksCoordinator reports whether a step that uses uses asks the
// coordinator for it: an action the coordinator performs
// (plan.CoordinatorAction), in an attempt of version 3 of the worker API
// or later, which has the path. Of an earlier one, the step can't run, as
// before the API had it.
func (a *attempt) asksCoordinator(uses string) bool {
	return plan.CoordinatorAction(uses) && a.claim.APIVersion >= workerapi.Version3
}

// runAction asks the coordinator for the action that a step's try, readied
// as rs, uses, whose log is key, as runCommand runs a command: it returns
// whether the action succeeded, why the try was stopped, if it was, the
// problems the action reported, which say why it failed and leave the
// step's retry: and ignore-errors: to say what follows, as a command's
// exit status does, and the problems of the try itself, which fail the
// step. The log holds note, if there is one, and the action's problems.
//
// The coordinator answers cancelled for a step that doesn't outlive its
// job's cancellation, of a job it is cancelling, which the worker hasn't
// heard of yet, and says so too of an action that cancelled the step's
// own job: the job is cancelled from then on, as though the worker had
// read the control that says so. It answers cancelled, with no reason,
// for a step whose early-cancel: is any-job-failed, once a job of the run
// has failed: the step ends as that stops it.
//
// The coordinator performs an action as one change, so the worker waits
// for its answer, asking again as it does any request, though what s
// watches asks the try to stop, until SIGKILL would be due for a command:
// an answer by then says how the try ended, and the stop applies from the
// step after. Past then, the try is stopped without the answer, though
// the coordinator may have performed the action, as its own record says.
// A coordinator that answers that the attempt no longer holds its job
// halts it.
func (a *attempt) runAction(ctx context.Context, key logKey, rs *plan.ReadyStep, s stops, note *problems.Diagnostic) (bool, stopCause, problems.Diagnostics, problems.Diagnostics) {
	path := a.job.Steps[key.step].Path
	// What stops the try ends the request only once its time is up.
	reqCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	type result struct {
		answer *workerapi.ActionAnswer
		err    error
	}
	// One request, which a checkout's tree is asked for with too, unchanged:
	// the coordinator serves the tree only for the request it answered.
	req := &workerapi.ActionRequest{
		Step: key.step, Try: key.try, With: rs.With,
		Private: rs.WithRead.Private, Secret: rs.WithRead.Marked,
	}
	answered := make(chan result, 1)
	go func() {
		answer, err := a.client.action(reqCtx, req)
		answered <- result{answer, err}
	}()
	stopping, unwatch := a.watch(s)
	defer unwatch()
	var (
		stop  stopRequest
		timer = after(0)
	)
	defer func() { timer.Stop() }()
	for {
		select {
		case req := <-stopping:
			stop = req
			timer.Stop()
			timer = time.NewTimer(time.Until(req.kill))
			continue
		case <-timer.C:
			cancel()
			<-answered
			a.log.Info("a step's action was stopped before the coordinator answered", "step", key.step, "try", key.try)
			return false, stop.cause, nil, a.writeLog(key, noted(note, nil))
		case res := <-answered:
			if errors.Is(res.err, errFenced) || errors.Is(res.err, errUnauthorized) {
				a.halt(res.err)
				return false, stopHalted, nil, nil
			}
			if res.err != nil {
				failed := a.at(problems.ActionRequestFailed, path, problems.Params{"uses": rs.Uses, "reason": requestFailure(res.err)})
				a.log.Error("a step's action couldn't be asked for", "step", key.step, "try", key.try, "error", a.mask.Mask(res.err.Error()))
				return false, noStop, nil, append(problems.Diagnostics{failed}, a.writeLog(key, noted(note, problems.Diagnostics{failed}))...)
			}
			// The event's diagnostics, and the log, are masked as the
			// worker's own are (finished, writeLog).
			acted := res.answer.Diagnostics
			if res.answer.Reason != "" {
				// The coordinator is cancelling the job, as the control the
				// worker may not have read yet says: the worker does as it
				// would have, had it read it, before the next step.
				a.cancel(&workerapi.Control{Cancel: true, Reason: res.answer.Reason})
			}
			cause := noStop
			switch {
			case res.answer.Outcome != plan.OutcomeCancelled:
			case res.answer.Reason != "":
				cause = stopCancelled
			default:
				// The step's early-cancel:, as a job of the run failed,
				// which the worker heeds from then on, as once its control
				// says so.
				a.fire(a.anyJobFailed, nil)
				cause = stopEarlyCancel
			}
			succeeded := res.answer.Outcome == plan.OutcomeSuccess
			if succeeded && cause == noStop && rs.Uses == plan.CheckoutUses {
				// The checkout's tree, once its action succeeded: but not
				// once a stop came, which applies to it as to a command.
				var wrote problems.Diagnostics
				if cause, wrote = stop.cause, nil; cause == noStop {
					cause, wrote = a.checkout(ctx, key, rs, req, stopping)
				}
				if cause == stopHalted {
					return false, stopHalted, nil, nil
				}
				acted = append(slices.Clone(acted), wrote...)
				succeeded = cause == noStop && !wrote.HasErrors()
			}
			return succeeded, cause, acted, a.writeLog(key, noted(note, acted))
		}
	}
}

// requestFailure returns what made a request fail, err, as a problem's
// param: the code of the problem the coordinator refused it with, or of
// the one the worker made of it, or what the system said.
func requestFailure(err error) string {
	if p, ok := errors.AsType[*problem.Problem](err); ok {
		return string(p.Code)
	}
	if _, ok := errors.AsType[*encodeError](err); ok {
		return string(problems.ReportNotEncoded.Code())
	}
	return err.Error()
}

// noted returns ds, after note, if there is one, as a step's log has them.
func noted(note *problems.Diagnostic, ds problems.Diagnostics) problems.Diagnostics {
	if note == nil {
		return ds
	}
	return append(problems.Diagnostics{*note}, ds...)
}

// sshKeys are the keys of a format 1 ssh: that say which host, and which
// login on it, a step runs on, by which ACTIVATE keys the connections it
// keeps, and reuses a job's for a step whose are the same. Its others, such
// as disconnect-timeout, don't change where the step runs. remoteHost is
// as written: ACTIVATE reads host:22 apart from host, as host:port is
// where it connects, and host an alias its ssh_config can resolve.
var sshKeys = []string{"remoteHost", "remoteUser", "jumpNodeHost", "jumpNodeUser"}

// sameSSH reports whether two ssh: mappings, a step's and its job's, name
// the same host and login (sshKeys), each as text, as ACTIVATE reads them,
// in which null, and a key that isn't there, are "".
func sameSSH(a, b plan.Data) bool {
	am, _ := a.Get().(map[string]any)
	bm, _ := b.Get().(map[string]any)
	for _, k := range sshKeys {
		if expr.Text(am[k]) != expr.Text(bm[k]) {
			return false
		}
	}
	return true
}
