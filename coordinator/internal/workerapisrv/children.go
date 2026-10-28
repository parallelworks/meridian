package workerapisrv

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// Children makes, and stops, the child runs of the steps of attempts' jobs
// whose uses: names a workflow, as the coordinator does
// (coordinator.CreateChild): the server gives it what a worker asks.
type Children interface {
	// Child returns the child run of the try of a step of the attempt a,
	// of run, that req asks for: the one the try has, or one made now; or
	// how the step's child wasn't made, ended. It returns store.ErrFenced
	// for an attempt that no longer holds its job, which the server answers
	// Fenced, and nothing is made; and an error that wraps ErrNotChild for
	// a step whose uses: names no workflow, or a try past its retry:.
	Child(ctx context.Context, run *store.Run, a *store.Attempt, req *workerapi.ChildRequest) (*workerapi.Child, error)
	// StopChild stops the child run id, for reason, cancelled or timeout,
	// as its step asks: one that has ended, or was stopped already, is
	// left as it is.
	StopChild(ctx context.Context, id string, reason plan.Reason) error
}

// ErrNotChild is what Children.Child returns, wrapped, for a step whose
// uses: names no workflow, or a try past the step's retry:, as one the
// worker never asks for, which the server answers 400.
var ErrNotChild = errors.New("workerapisrv: the step's try has no child")

// childRecheck is how often a poll for a child's end reads the store again
// besides, for a child another process ended, which can't tell the server
// (Server.Ended).
const childRecheck = time.Second

// serveChild answers a step's ask for its try's child (workerapi.PathChild):
// the Child that Children gives, once the attempt is seen to hold its job.
func (s *Server) serveChild(w http.ResponseWriter, r *http.Request, a *store.Attempt, run *store.Run) {
	body, ok := s.readBody(w, r, workerapi.MaxChildRequest)
	if !ok {
		return
	}
	var req workerapi.ChildRequest
	if err := workerapi.UnmarshalAt(a.APIVersion, body, &req); err != nil {
		s.badRequest(w, r, "a child's request that isn't one")
		return
	}
	if req.Step < 0 || req.Try < 0 {
		s.badRequest(w, r, "a child of a step or a try that is no step's or try's")
		return
	}
	if !a.Holds {
		problem.Write(w, fenced())
		return
	}
	child, err := s.children.Child(r.Context(), run, a, &req)
	switch {
	case errors.Is(err, ErrNotChild):
		s.badRequest(w, r, "a child of a step that calls no workflow, or of a try it doesn't have")
		return
	case err != nil:
		s.writeError(w, r, err)
		return
	}
	writeBody(w, a.APIVersion, child)
}

// serveChildEnd answers a poll for the end of the child of a step's try
// (workerapi.PathChildEnd), named in the query: once the child has ended,
// as Server.Ended says, or the store does when it is read again, every
// childRecheck; or after the hold, with the child as it is. A try that has
// no child is 404.
func (s *Server) serveChildEnd(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	ctx := r.Context()
	step, err1 := strconv.Atoi(r.URL.Query().Get(workerapi.QueryStep))
	try, err2 := strconv.Atoi(r.URL.Query().Get(workerapi.QueryTry))
	if err1 != nil || err2 != nil || step < 0 || try < 0 {
		s.badRequest(w, r, "a poll for a child's end without its step and try")
		return
	}
	var id string
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		id, err = tx.Child(a.ID, step, try)
		return err
	}); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeStatus(w, http.StatusNotFound)
			return
		}
		s.writeError(w, r, err)
		return
	}
	hold := time.NewTimer(s.hold)
	defer hold.Stop()
	tick := time.NewTicker(s.childRecheck)
	defer tick.Stop()
	for {
		// The poll waits before the store is read, so that an end stored
		// after the read wakes it.
		ended, done := s.ends.wait(id)
		var child *store.Run
		err := s.store.View(ctx, func(tx *store.Tx) error {
			var err error
			child, err = tx.Run(id)
			return err
		})
		switch {
		case err != nil:
			done()
			s.writeError(w, r, err)
			return
		case child.State == store.RunEnded:
			done()
			writeBody(w, a.APIVersion, &workerapi.Child{Run: id, Ended: true, Outcome: child.Outcome, Reason: child.Reason})
			return
		}
		select {
		case <-ended:
		case <-tick.C:
		case <-hold.C:
			done()
			writeBody(w, a.APIVersion, &workerapi.Child{Run: id})
			return
		case <-ctx.Done():
			done()
			return
		}
		done()
	}
}

// serveChildStop stops the child of a step's try (workerapi.PathChildStop),
// as Children does, and answers 204 once its stop is stored. A try that
// has no child is 404. An attempt that no longer holds its job may stop
// its step's child too: stopping changes nothing that runs but the child.
func (s *Server) serveChildStop(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	ctx := r.Context()
	body, ok := s.readBody(w, r, MaxClaimBody)
	if !ok {
		return
	}
	var stop workerapi.ChildStop
	if err := workerapi.UnmarshalAt(a.APIVersion, body, &stop); err != nil {
		s.badRequest(w, r, "a child's stop that isn't one")
		return
	}
	if stop.Step < 0 || stop.Try < 0 || stop.Reason != plan.ReasonCancelled && stop.Reason != plan.ReasonTimeout {
		s.badRequest(w, r, "a child's stop of no step's try, or for no reason a step is stopped for")
		return
	}
	var id string
	if err := s.store.View(ctx, func(tx *store.Tx) error {
		var err error
		id, err = tx.Child(a.ID, stop.Step, stop.Try)
		return err
	}); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeStatus(w, http.StatusNotFound)
			return
		}
		s.writeError(w, r, err)
		return
	}
	if err := s.children.StopChild(ctx, id, stop.Reason); err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Ended answers the polls for the ends of the child runs ids that the
// server holds: whoever opened the store calls it once a transaction of
// this process's that ended runs has committed (store.Options.OnRunsEnded).
// The polls read the store again every second besides, for a run another
// process ended.
func (s *Server) Ended(ids ...string) { s.ends.ended(ids) }

// endWaits are the polls for the ends of child runs the server holds, by
// the run's ID: each waits on a channel closed once the run has ended.
type endWaits struct {
	mu    sync.Mutex
	waits map[string]*endWait
}

// An endWait is a channel the polls for a run's end wait on, and how many
// do.
type endWait struct {
	ch chan struct{}
	n  int
}

// wait returns a channel closed once the run id has ended (ended), and
// what to call once the poll no longer waits.
func (e *endWaits) wait(id string) (<-chan struct{}, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.waits == nil {
		e.waits = map[string]*endWait{}
	}
	w := e.waits[id]
	if w == nil {
		w = &endWait{ch: make(chan struct{})}
		e.waits[id] = w
	}
	w.n++
	var once sync.Once
	return w.ch, func() {
		once.Do(func() {
			e.mu.Lock()
			defer e.mu.Unlock()
			if w.n--; w.n == 0 && e.waits[id] == w {
				delete(e.waits, id)
			}
		})
	}
}

// ended wakes the polls for the ends of the runs ids.
func (e *endWaits) ended(ids []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, id := range ids {
		if w := e.waits[id]; w != nil {
			close(w.ch)
			delete(e.waits, id)
		}
	}
}
