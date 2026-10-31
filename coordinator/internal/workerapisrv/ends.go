package workerapisrv

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// childOf returns the ID of the child run of the try try of the step step
// of the attempt a, or of the step's cleanup:, of the kind a path asks
// for: a step run, where stepRun is set, or else a workflow the step
// calls. It answers 404 for a try that has no child, or one of the other
// kind, and false, so that an attempt reaches only its own children, and
// each only through its own kind's paths.
func (s *Server) childOf(w http.ResponseWriter, r *http.Request, a *store.Attempt, step, try int, cleanup, stepRun bool) (string, bool) {
	var id string
	if err := s.store.View(r.Context(), func(tx *store.Tx) error {
		var err error
		if id, err = tx.ChildOf(a.ID, step, try, cleanup); err != nil {
			return err
		}
		child, err := tx.Run(id)
		if err == nil && child.StepRun != stepRun {
			err = store.ErrNotFound
		}
		return err
	}); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeStatus(w, http.StatusNotFound)
			return "", false
		}
		s.writeError(w, r, err)
		return "", false
	}
	return id, true
}

// awaitEnd answers a poll of the attempt a for the end of its child run
// id: once the run has ended, as Server.Ended says, or the store does when
// it is read again, every childRecheck, with what ended gives of it; or
// after the hold, with held, the run as it is.
func (s *Server) awaitEnd(w http.ResponseWriter, r *http.Request, a *store.Attempt, id string, ended func(context.Context, *store.Run) (any, error), held any) {
	ctx := r.Context()
	hold := time.NewTimer(s.hold)
	defer hold.Stop()
	tick := time.NewTicker(s.childRecheck)
	defer tick.Stop()
	for {
		// The poll waits before the store is read, so that an end stored
		// after the read wakes it.
		woken, done := s.ends.wait(id)
		var run *store.Run
		err := s.store.View(ctx, func(tx *store.Tx) error {
			var err error
			run, err = tx.Run(id)
			return err
		})
		switch {
		case err != nil:
			done()
			s.writeError(w, r, err)
			return
		case run.State == store.RunEnded:
			done()
			body, err := ended(ctx, run)
			if err != nil {
				s.writeError(w, r, err)
				return
			}
			writeBody(w, a.APIVersion, body)
			return
		}
		select {
		case <-woken:
		case <-tick.C:
		case <-hold.C:
			done()
			writeBody(w, a.APIVersion, held)
			return
		case <-ctx.Done():
			done()
			return
		}
		done()
	}
}

// stopOf stops the child run of the try try of the step step of the
// attempt a, or of the step's cleanup:, of the kind childOf finds, for
// reason, as Children does, and answers 204 once its stop is stored. A try
// that has no such child is 404, and a stop of no step's try, or for a
// reason a step isn't stopped for, 400, where what names the stop. An
// attempt that no longer holds its job may stop its step's child too:
// stopping changes nothing that runs but the child.
func (s *Server) stopOf(w http.ResponseWriter, r *http.Request, a *store.Attempt, what string, step, try int, cleanup, stepRun bool, reason plan.Reason) {
	if step < 0 || try < 0 || reason != plan.ReasonCancelled && reason != plan.ReasonTimeout {
		s.badRequest(w, r, what+" of no step's try, or for no reason a step is stopped for")
		return
	}
	id, ok := s.childOf(w, r, a, step, try, cleanup, stepRun)
	if !ok {
		return
	}
	if err := s.children.StopChild(r.Context(), id, reason); err != nil {
		s.writeError(w, r, err)
		return
	}
	writeStatus(w, http.StatusNoContent)
}
