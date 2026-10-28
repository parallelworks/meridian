package workerapisrv

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// Actions performs the built-in actions that attempts' steps ask the
// coordinator for (workerapi.PathAction): the coordinator's, which reads
// which action a step uses from its run's plan.
type Actions interface {
	// Act performs the action that the step call names uses, as the plan
	// of call's run has it, with the request's with:, and returns its
	// answer, which it gives through call.Commit, with the step. Commit
	// stores it, in the
	// transaction in which Act changes the store, if it does, as the
	// answer to the request and to every repeat of it. What Act does
	// outside that transaction, as asking a platform, it may do again for
	// a repeat that comes meanwhile, which is given the answer stored
	// first.
	//
	// Act returns an error that wraps ErrNotAction for a step that uses no
	// action the coordinator performs, which the server answers 400;
	// Commit's errors, which the server answers as it answers any
	// request's; and any other error for an action it can't perform now,
	// as for a platform that doesn't answer, which the server answers 500,
	// having stored nothing, and the worker asks again.
	Act(ctx context.Context, call *ActionCall) (workerapi.ActionAnswer, error)
}

// ErrNotAction is what Actions.Act returns, wrapped, for a step that uses
// no action the coordinator performs, as one the worker never asks for,
// which the server answers 400.
var ErrNotAction = errors.New("workerapisrv: the step uses no action the coordinator performs")

// An ActionCall is a request for the action a step's try uses, from the
// attempt Attempt of the run Run, which Actions.Act answers.
type ActionCall struct {
	Run     *store.Run
	Attempt *store.Attempt
	Request workerapi.ActionRequest

	s          *Server
	credential string
	digest     [sha256.Size]byte
	committed  bool
}

// A checkError is an error of what Commit checks before it calls act: the
// attempt no longer holds its job, or its credential doesn't take, or the
// try was answered for another request. The server answers it as it does
// any request's (writeError).
type checkError struct{ err error }

func (e *checkError) Error() string { return e.err.Error() }
func (e *checkError) Unwrap() error { return e.err }

// Commit runs act in a transaction of the store's, and stores the answer it
// returns in it, as the answer to the call and to every repeat of it. It
// first checks, in that transaction, that the attempt that asked still
// holds its job, and that the step's try hasn't been answered: for a
// repeat of the request that was, it returns that answer, without calling
// act; for another request for the same try, store.ErrFenced. Nor does it
// call act for step, the call's as the run's plan has it, whose if:
// doesn't outlive its job's cancellation (plan.OutlivesCancel), which it
// reads from the plan, never from the worker, of an attempt whose control
// asks to cancel its job, for any reason but the run's timeout (cancel):
// its answer is cancelled, for the control's reason, as though the worker
// had read the control before the step; nor for one whose early-cancel:
// is any-job-failed, once a job of the run has failed, as its control
// says (Control.AnyJobFailed): its answer is cancelled, with no reason, as
// the worker would have stopped the step. act answers success or failure,
// and Commit adds the cancel the attempt's control asks once act is done,
// as when the action cancelled the step's own job, so that the worker
// cancels it before the next step. An error act returns ends the
// transaction, which then stores nothing; so does an answer act can't
// give.
func (c *ActionCall) Commit(ctx context.Context, step *plan.Step, act func(tx *store.Tx) (workerapi.ActionAnswer, error)) (workerapi.ActionAnswer, error) {
	var answer workerapi.ActionAnswer
	err := c.s.store.Tx(ctx, func(tx *store.Tx) error {
		stored, err := c.answered(tx)
		switch {
		case err != nil:
			return err
		case stored != nil:
			answer = *stored
			return nil
		}
		ctl, err := tx.Control(c.Attempt.ID)
		if err != nil {
			return err
		}
		switch cancel := cancelOf(ctl); {
		case cancel != "" && !plan.OutlivesCancel(step.If):
			answer = workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled, Reason: cancel}
		case ctl.AnyJobFailed && step.EarlyCancel == plan.EarlyCancelAnyJobFailed:
			answer = workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled}
		default:
			if answer, err = act(tx); err != nil {
				return err
			}
			if answer.Outcome != plan.OutcomeSuccess && answer.Outcome != plan.OutcomeFailure || answer.Reason != "" {
				return fmt.Errorf("workerapisrv: an action's answer with the outcome %q and the reason %q", answer.Outcome, answer.Reason)
			}
			if ctl, err = tx.Control(c.Attempt.ID); err != nil {
				return err
			}
			answer.Reason = cancelOf(ctl)
		}
		return tx.RecordAction(c.Attempt.ID, c.Request.Step, c.Request.Try, store.Action{Digest: c.digest, Answer: answer})
	})
	if err != nil {
		return workerapi.ActionAnswer{}, err
	}
	c.committed = true
	return answer, nil
}

// answered checks, in tx, that the attempt that made the call still holds
// its job, and returns the answer stored for the step's try, if it has one,
// to a repeat of the request it answered: a checkError otherwise.
func (c *ActionCall) answered(tx *store.Tx) (*workerapi.ActionAnswer, error) {
	if _, err := tx.Authorize(c.credential); err != nil {
		return nil, &checkError{err}
	}
	stored, err := tx.Action(c.Attempt.ID, c.Request.Step, c.Request.Try)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	case !stored.SameRequest(c.digest):
		return nil, &checkError{store.ErrFenced}
	}
	return &stored.Answer, nil
}

// cancelOf returns why ctl asks its worker to cancel its job, or "" for
// none, as for the run's timeout, which stops the job as its own timeout
// does, rather than cancel it.
func cancelOf(ctl workerapi.Control) plan.Reason {
	if !ctl.Cancel || ctl.Reason == plan.ReasonTimeout {
		return ""
	}
	return ctl.Reason
}

// serveAction answers the request of a step's try for the action it uses,
// once, as Actions performs it: a repeat of the request, once it has an
// answer, is given that answer at once, and another request for the same
// try is fenced, as is any of an attempt that no longer holds its job. It
// reads the request as a body of the attempt's version of the worker API.
func (s *Server) serveAction(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	ctx := r.Context()
	body, ok := s.readBody(w, r, workerapi.MaxActionRequest)
	if !ok {
		return
	}
	call := &ActionCall{Run: run, Attempt: a, s: s, credential: bearer(r)}
	if err := workerapi.UnmarshalAt(a.APIVersion, body, &call.Request); err != nil {
		s.badRequest(w, r, "an action's request that isn't")
		return
	}
	if call.Request.Step < 0 || call.Request.Try < 0 {
		s.badRequest(w, r, "an action of a step or a try that is no step's or try's")
		return
	}
	// The digest of the request as the API writes it, which a repeat
	// matches, however its JSON was written.
	canonical, err := workerapi.Marshal(&call.Request)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	call.digest = sha256.Sum256(canonical)
	// A repeat is answered from what is stored, without asking Actions
	// again, which might ask a platform again.
	var stored *workerapi.ActionAnswer
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		stored, err = call.answered(tx)
		return err
	}); err != nil {
		s.writeActionError(w, r, err)
		return
	}
	if stored != nil {
		writeBody(w, a.APIVersion, stored)
		return
	}
	answer, err := s.actions.Act(ctx, call)
	switch {
	case err != nil:
		s.writeActionError(w, r, err)
	case !call.committed:
		s.log.ErrorContext(ctx, "workerapisrv: an action was answered without being stored", "run", run.ID, "attempt", a.ID, "step", call.Request.Step)
		writeStatus(w, http.StatusInternalServerError)
	default:
		writeBody(w, a.APIVersion, &answer)
	}
}

// writeActionError answers with what err, of an action's request, says of
// it: 400 for a step that uses no action the coordinator performs; what
// writeError answers for what Commit checks; and 500 for the rest, which
// the server logs, and which the worker tries again.
func (s *Server) writeActionError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, ErrNotAction) {
		s.badRequest(w, r, "an action of a step that uses none the coordinator performs")
		return
	}
	if check, ok := errors.AsType[*checkError](err); ok {
		s.writeError(w, r, check.err)
		return
	}
	s.log.ErrorContext(r.Context(), "workerapisrv: an action failed", "error", err)
	writeStatus(w, http.StatusInternalServerError)
}
