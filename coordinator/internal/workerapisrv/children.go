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
// whose uses: names a workflow, as the coordinator does: the server gives
// it what a worker asks, once it has authorized the attempt and seen it
// hold its job.
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
	// StepRun returns the step run of the try of a step of the attempt a,
	// or of its cleanup:, of run, that req asks for: the one it has, or one
	// made now; or how it wasn't made, ended (docs/design.md, Steps on
	// another host). It returns store.ErrFenced for an attempt that no
	// longer holds its job, which the server answers Fenced, and an error
	// that wraps ErrNotStepRun for a step, a try or a host that can't
	// have one.
	StepRun(ctx context.Context, run *store.Run, a *store.Attempt, req *workerapi.StepRunRequest) (*workerapi.StepRun, error)
	// StepRunEnd returns how the step run id, which has ended, ended.
	StepRunEnd(ctx context.Context, id string) (*workerapi.StepRun, error)
}

// ErrNotStepRun is what Children.StepRun returns, wrapped, for a step, a
// try or a host that can't have a step run, as one the worker never asks
// for, which the server answers 400.
var ErrNotStepRun = errors.New("workerapisrv: the step's try has no step run")

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
	switch {
	case req.Step < 0 || req.Try < 0:
		s.badRequest(w, r, "a child of a step or a try that is no step's or try's")
		return
	case !maskedWithin(req.Masked):
		s.badRequest(w, r, "a child's request that masks more than it may")
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
// (workerapi.PathChildEnd), named in the query, a workflow its step calls,
// as awaitEnd does: once it has ended, how; or after the hold, as it is. A
// try that has no such child, as one whose child is a step run, is 404.
func (s *Server) serveChildEnd(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	step, err1 := strconv.Atoi(r.URL.Query().Get(workerapi.QueryStep))
	try, err2 := strconv.Atoi(r.URL.Query().Get(workerapi.QueryTry))
	if err1 != nil || err2 != nil || step < 0 || try < 0 {
		s.badRequest(w, r, "a poll for a child's end without its step and try")
		return
	}
	id, ok := s.childOf(w, r, a, step, try, false, false)
	if !ok {
		return
	}
	s.awaitEnd(w, r, a, id, func(_ context.Context, child *store.Run) (any, error) {
		return &workerapi.Child{Run: id, Ended: true, Outcome: child.Outcome, Reason: child.Reason}, nil
	}, &workerapi.Child{Run: id})
}

// serveChildStop stops the child of a step's try (workerapi.PathChildStop),
// a workflow its step calls, as stopOf does.
func (s *Server) serveChildStop(w http.ResponseWriter, r *http.Request, a *store.Attempt) {
	body, ok := s.readBody(w, r, MaxClaimBody)
	if !ok {
		return
	}
	var stop workerapi.ChildStop
	if err := workerapi.UnmarshalAt(a.APIVersion, body, &stop); err != nil {
		s.badRequest(w, r, "a child's stop that isn't one")
		return
	}
	s.stopOf(w, r, a, "a child's stop", stop.Step, stop.Try, false, false, stop.Reason)
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
