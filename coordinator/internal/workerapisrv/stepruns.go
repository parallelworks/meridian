package workerapisrv

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
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
	switch {
	case req.Step < 0 || req.Try < 0 || req.Cleanup && req.Try != 0:
		s.badRequest(w, r, "a step run of a step or a try that is no step's or try's")
		return
	case !maskedWithin(req.Masked):
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

// serveStepRunEnd answers a poll for the end of the step run of a step's
// try, or of its cleanup: (workerapi.PathStepRunEnd), named in the query,
// as awaitEnd does: once it has ended, how, as Children gives it; or as it
// is, after the hold. A try that has no step run, as one whose child is a
// workflow its step calls, is 404.
func (s *Server) serveStepRunEnd(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
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
	id, ok := s.childOf(w, r, a, step, try, cleanup, true)
	if !ok {
		return
	}
	s.awaitEnd(w, r, a, id, func(ctx context.Context, _ *store.Run) (any, error) {
		return s.children.StepRunEnd(ctx, id)
	}, &workerapi.StepRun{Run: id})
}

// serveStepRunStop stops the step run of a step's try, or of its cleanup:
// (workerapi.PathStepRunStop), as stopOf does.
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
	s.stopOf(w, r, a, "a step run's stop", stop.Step, stop.Try, stop.Cleanup, true, stop.Reason)
}
