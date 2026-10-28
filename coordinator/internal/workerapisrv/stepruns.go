package workerapisrv

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// serveStepRun answers a step's ask for the step run of its try, or of its
// cleanup: (workerapi.PathStepRun): the StepRun that Children gives, once
// the attempt is seen to hold its job.
func (s *Server) serveStepRun(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	body, ok := s.readBody(w, r, workerapi.MaxStepRunRequest)
	if !ok {
		return
	}
	var req workerapi.StepRunRequest
	if err := workerapi.UnmarshalAt(a.APIVersion, body, &req); err != nil {
		s.badRequest(w, r, "a step run's request that isn't one")
		return
	}
	masked := 0
	for _, m := range req.Masked {
		masked += len(m)
	}
	switch {
	case req.Step < 0 || req.Try < 0 || req.Cleanup && req.Try != 0:
		s.badRequest(w, r, "a step run of a step or a try that is no step's or try's")
		return
	case masked > workerapi.MaxStepRunMasked || len(req.Masked) > workerapi.MaxStepRunMasks:
		s.badRequest(w, r, "a step run's request that masks more than it may")
		return
	}
	if !a.Holds {
		problem.Write(w, fenced())
		return
	}
	sr, err := s.children.StepRun(r.Context(), run, a, &req)
	switch {
	case errors.Is(err, ErrNotStepRun):
		s.badRequest(w, r, "a step run of a step, a try or a host that can't have one")
		return
	case err != nil:
		// store.ErrFenced among them, which it answers fenced.
		s.writeError(w, r, err)
		return
	}
	writeBody(w, a.APIVersion, sr)
}

// stepRunOf returns the ID of the step run of the try try of the step
// step, or of its cleanup:, of the attempt a, answering 404 for one it
// hasn't, as for a try whose child is a workflow its step calls, and
// false.
func (s *Server) stepRunOf(w http.ResponseWriter, r *http.Request, a *store.Attempt, step, try int, cleanup bool) (string, bool) {
	var id string
	if err := s.store.View(r.Context(), func(tx *store.Tx) error {
		var err error
		if id, err = tx.ChildOf(a.ID, step, try, cleanup); err != nil {
			return err
		}
		child, err := tx.Run(id)
		if err == nil && !child.StepRun {
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

// serveStepRunEnd answers a poll for the end of the step run of a step's
// try, or of its cleanup: (workerapi.PathStepRunEnd), named in the query,
// as serveChildEnd answers one for a child's: once it has ended, how, as
// Children gives it; or as it is, after the hold.
func (s *Server) serveStepRunEnd(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	ctx := r.Context()
	q := r.URL.Query()
	step, err1 := strconv.Atoi(q.Get(workerapi.QueryStep))
	try, err2 := strconv.Atoi(q.Get(workerapi.QueryTry))
	if err1 != nil || err2 != nil || step < 0 || try < 0 {
		s.badRequest(w, r, "a poll for a step run's end without its step and try")
		return
	}
	cleanup := false
	if v := q.Get(workerapi.QueryCleanup); v != "" {
		var err error
		if cleanup, err = strconv.ParseBool(v); err != nil {
			s.badRequest(w, r, "a poll for a step run's end whose cleanup isn't true or false")
			return
		}
	}
	id, ok := s.stepRunOf(w, r, a, step, try, cleanup)
	if !ok {
		return
	}
	hold := time.NewTimer(s.hold)
	defer hold.Stop()
	tick := time.NewTicker(s.childRecheck)
	defer tick.Stop()
	for {
		ended, done := s.ends.wait(id)
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
			sr, err := s.children.StepRunEnd(ctx, id)
			if err != nil {
				s.writeError(w, r, err)
				return
			}
			writeBody(w, a.APIVersion, sr)
			return
		}
		select {
		case <-ended:
		case <-tick.C:
		case <-hold.C:
			done()
			writeBody(w, a.APIVersion, &workerapi.StepRun{Run: id})
			return
		case <-ctx.Done():
			done()
			return
		}
		done()
	}
}

// serveStepRunStop stops the step run of a step's try, or of its cleanup:
// (workerapi.PathStepRunStop), as Children stops a child.
func (s *Server) serveStepRunStop(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	body, ok := s.readBody(w, r, MaxClaimBody)
	if !ok {
		return
	}
	var stop workerapi.StepRunStop
	if err := workerapi.UnmarshalAt(a.APIVersion, body, &stop); err != nil {
		s.badRequest(w, r, "a step run's stop that isn't one")
		return
	}
	if stop.Step < 0 || stop.Try < 0 || stop.Reason != plan.ReasonCancelled && stop.Reason != plan.ReasonTimeout {
		s.badRequest(w, r, "a step run's stop of no step's try, or for no reason a step is stopped for")
		return
	}
	id, ok := s.stepRunOf(w, r, a, stop.Step, stop.Try, stop.Cleanup)
	if !ok {
		return
	}
	if err := s.children.StopChild(r.Context(), id, stop.Reason); err != nil {
		s.writeError(w, r, err)
		return
	}
	writeStatus(w, http.StatusNoContent)
}
