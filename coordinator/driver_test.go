package coordinator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// testBackend is the backend of testGroups' members, which testDriver
// places workers with.
const testBackend = "test"

// testGroups has the groups local, of the member m1, and pair, of m1 and
// m2, each of testBackend.
type testGroups struct{}

func (testGroups) Group(_ context.Context, name string) (*backend.Group, error) {
	switch name {
	case "local":
		return &backend.Group{Name: name, Members: []backend.Member{{Name: "m1", Backend: testBackend}}}, nil
	case "pair":
		return &backend.Group{Name: name, Members: []backend.Member{{Name: "m1", Backend: testBackend}, {Name: "m2", Backend: testBackend}}}, nil
	}
	return nil, compile.ErrNotFound
}

func (testGroups) Environment(context.Context, plan.Environment) (*backend.Group, error) {
	return nil, compile.ErrNotFound
}

func (testGroups) ComputeTarget(context.Context, plan.ComputeTarget) (*backend.Group, error) {
	return nil, compile.ErrNotFound
}

// A testEnd is what the worker of an attempt testDriver placed does: it
// completes the attempt with result; or, with problem, ends it as one that
// failed before its first step; or, with never, never claims it.
type testEnd struct {
	result  plan.JobResult
	problem *problem.Problem
	never   bool
}

// testDriver is a backend.Driver in process, for tests. Its members can run
// every job, unless probe says otherwise. Each worker it places does what
// worker says, through the coordinator's store, as the worker API would
// store it, and wakes the attempt's supervisor: by default, it claims the
// attempt and completes it with success.
type testDriver struct {
	c *Coordinator

	mu         sync.Mutex
	probe      func(m *backend.Member) (backend.Fit, error)
	place      func(req backend.PlaceRequest) error
	find       func(attempt string) error
	worker     func(req backend.PlaceRequest) testEnd
	placed     []backend.PlaceRequest
	members    []string
	placements map[string]backend.Placement
	connects   int
	workers    sync.WaitGroup
}

var _ backend.Driver = (*testDriver)(nil)

func newTestDriver() *testDriver {
	return &testDriver{placements: map[string]backend.Placement{}}
}

// set changes what d does, under its lock.
func (d *testDriver) set(f func(d *testDriver)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f(d)
}

// requests returns the requests d placed workers with, and the members it
// placed them on.
func (d *testDriver) requests() ([]backend.PlaceRequest, []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]backend.PlaceRequest(nil), d.placed...), append([]string(nil), d.members...)
}

func (d *testDriver) Probe(_ context.Context, m *backend.Member, _ backend.Demand) (backend.Fit, error) {
	d.mu.Lock()
	probe := d.probe
	d.mu.Unlock()
	if probe != nil {
		return probe(m)
	}
	return backend.Fit{Availability: backend.StartsNow}, nil
}

func (d *testDriver) Connect(context.Context, *backend.Member, backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.connects++
	return backend.Endpoint{Address: "/meridian-test.sock"}, nil, nil
}

func (d *testDriver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	d.mu.Lock()
	place, worker := d.place, d.worker
	d.placed = append(d.placed, req)
	d.members = append(d.members, m.Name)
	d.mu.Unlock()
	if place != nil {
		if err := place(req); err != nil {
			return backend.Placement{}, err
		}
	}
	p := backend.Placement{Attempt: req.Attempt, ID: "test-" + req.Attempt, WorkerDigest: "sha256:" + hex.EncodeToString(make([]byte, 32))}
	d.mu.Lock()
	d.placements[req.Attempt] = p
	d.mu.Unlock()
	end := testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	if worker != nil {
		end = worker(req)
	}
	if !end.never {
		// The worker outlives the call that placed it.
		ctx := context.WithoutCancel(ctx)
		d.workers.Go(func() { d.work(ctx, req, p, end) })
	}
	return p, nil
}

func (d *testDriver) Find(_ context.Context, _ *backend.Member, attempt string) (backend.Placement, bool, error) {
	d.mu.Lock()
	find := d.find
	p, ok := d.placements[attempt]
	d.mu.Unlock()
	if find != nil {
		if err := find(attempt); err != nil {
			return backend.Placement{}, false, err
		}
	}
	return p, ok, nil
}

func (d *testDriver) Check(context.Context, *backend.Member, backend.Placement) (backend.Status, error) {
	return backend.Status{State: backend.Unknown}, nil
}

func (d *testDriver) Cancel(context.Context, *backend.Member, backend.Placement) error { return nil }

func (d *testDriver) Forget(context.Context, *backend.Member, backend.Placement) error { return nil }

// work is the worker of the attempt req is for, placed as p: once the
// placement is recorded, as the worker API's claim waits for, it claims
// the attempt and ends it as end says, and wakes the attempt's supervisor.
func (d *testDriver) work(ctx context.Context, req backend.PlaceRequest, p backend.Placement, end testEnd) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var a *store.Attempt
	for {
		err := d.c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			a, err = tx.Attempt(req.Attempt)
			return err
		})
		switch {
		case err != nil:
			return
		case a.Placement.ID != "" || a.State != store.AttemptPlacing:
		default:
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			continue
		}
		break
	}
	credential := make([]byte, 32)
	_, _ = rand.Read(credential)
	report := workerapi.Complete{Result: end.result}
	if end.problem != nil {
		report = workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonSetupFailed, Problem: end.problem}
	}
	err := d.c.store.Tx(ctx, func(tx *store.Tx) error {
		claimed, err := tx.Claim(req.BootstrapToken, hex.EncodeToString(credential), workerapi.Worker{Version: "test", Digest: p.WorkerDigest})
		if err != nil {
			return err
		}
		if end.problem == nil {
			if err := tx.StepStarted(claimed.ID); err != nil {
				return err
			}
		}
		body, err := workerapi.Marshal(&report)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		return tx.EndAttempt(claimed.ID, store.AttemptEnd{
			Kind: store.EndCompleted, Outcome: report.Result.Outcome, Reason: report.Reason, CompleteDigest: sum[:], Report: body, Problem: report.Problem,
		})
	})
	if err != nil {
		return
	}
	d.c.wakeJob(ctx, a.Run, a.Job)
}
