package coordinator

import (
	"context"
	"errors"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// StoreID returns the store's ID, which is random, and which keeps what is
// named after a run's ID, such as its directory or its logs, apart from
// what a store made again, whose runs are numbered from 1 again, names
// after the same ID (RunOptions.RunDir).
func (c *Coordinator) StoreID() string { return c.store.ID() }

// ErrRunNotFound is what Status returns for a run the store doesn't have.
var ErrRunNotFound = errors.New("coordinator: no such run")

// A RunState is where a run is in its life.
type RunState string

// The states of a run.
const (
	// RunCreated is a run stored, which hasn't started.
	RunCreated RunState = RunState(store.RunCreated)
	// RunRunning is a run that has started.
	RunRunning RunState = RunState(store.RunRunning)
	// RunEnded is a run that has ended, with its outcome.
	RunEnded RunState = RunState(store.RunEnded)
)

// A RunStatus is a run as the store holds it.
type RunStatus struct {
	ID    string
	State RunState
	// Outcome and Reason are how the run ended, once it has.
	Outcome plan.Outcome
	Reason  plan.Reason
	// RunDir is the run's directory.
	RunDir string
	// Diagnostics are the run's problems: Start's, and the coordinator's.
	Diagnostics problems.Diagnostics
}

// Status returns the run id as the store holds it now, or ErrRunNotFound.
func (c *Coordinator) Status(ctx context.Context, id string) (*RunStatus, error) {
	var s *RunStatus
	err := c.store.View(ctx, func(tx *store.Tx) error {
		r, err := tx.Run(id)
		if errors.Is(err, store.ErrNotFound) {
			return ErrRunNotFound
		}
		if err != nil {
			return err
		}
		s = &RunStatus{
			ID: r.ID, State: RunState(r.State), Outcome: r.Outcome, Reason: r.Reason, RunDir: r.RunDir,
			Diagnostics: r.Diagnostics,
		}
		return nil
	})
	return s, err
}

// An EndpointHold is an endpoint that workers reach the coordinators of
// its store through, such as the local socket, which this process holds,
// until it releases it, closes the coordinator, or ends (HoldEndpoint).
type EndpointHold struct{ h *store.Hold }

// HoldEndpoint takes the endpoint named name, which is lower-case letters,
// digits and dashes, for holder, which says who holds it, as a person can
// read it. Only one process holds an endpoint at a time, under a lock on a
// file beside the store's database, which the system releases when the
// process ends, however it ends: HoldEndpoint waits for the process that
// holds it for as long as ctx allows. The store records who holds it and
// since when, from which the grace after a takeover is counted.
func (c *Coordinator) HoldEndpoint(ctx context.Context, name, holder string) (*EndpointHold, error) {
	h, err := c.store.HoldEndpoint(ctx, name, holder)
	if err != nil {
		return nil, err
	}
	return &EndpointHold{h: h}, nil
}

// Release stops holding the endpoint, even if ctx has ended.
func (e *EndpointHold) Release(ctx context.Context) error { return e.h.Release(ctx) }
