package coordinator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// testBackend is the backend of testGroups' members, which testDriver
// places workers with.
const testBackend = "test"

// testDigest is the digest of the worker binary testDriver says it starts.
var testDigest = "sha256:" + strings.Repeat("00", 32)

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
// failed before its first step; or, with never, never claims it; or, with
// running, claims it, starts a step, and leaves it running, closing
// started then. It waits for wait first, unless it is nil.
type testEnd struct {
	result  plan.JobResult
	problem *problem.Problem
	never   bool
	running bool
	started chan<- struct{}
	wait    <-chan struct{}
}

// testDriver is a backend.Driver in process, for tests. Its members can run
// every job, unless probe says otherwise. Each worker it places does what
// worker says, through the coordinator's store, as the worker API would
// store it, and wakes the attempt's supervisor: by default, it claims the
// attempt and completes it with success.
type testDriver struct {
	c *Coordinator
	// ctx is what its workers run with: the test's, which carries nothing
	// of the supervisor that placed them, as a worker process's doesn't.
	ctx context.Context //nolint:containedctx // the test's, which its workers outlive the calls that place them in

	mu         sync.Mutex
	probe      func(m *backend.Member) (backend.Fit, error)
	place      func(req backend.PlaceRequest) error
	find       func(attempt string) error
	worker     func(req backend.PlaceRequest) testEnd
	placed     []backend.PlaceRequest
	members    []string
	placements map[string]backend.Placement
	connects   int
	// cancelled are the placements Cancel cancelled.
	cancelled []backend.Placement
	workers   sync.WaitGroup
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
	p := backend.Placement{Attempt: req.Attempt, ID: "test-" + req.Attempt, WorkerDigest: testDigest}
	d.mu.Lock()
	d.placements[req.Attempt] = p
	d.mu.Unlock()
	end := testEnd{result: plan.JobResult{Outcome: plan.OutcomeSuccess}}
	if worker != nil {
		end = worker(req)
	}
	if !end.never {
		// The worker outlives the call that placed it, and is apart from
		// it, as a process of its own is: its context carries nothing of
		// the supervisor's, whose hopper job attempt the store's driver
		// knows a context by, and refuses a write in another transaction
		// while one of the attempt's is open, so a claim made while the
		// supervisor recorded the placement was refused, and the attempt
		// never claimed.
		d.workers.Go(func() { d.work(d.ctx, req, p, end) })
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

func (d *testDriver) Cancel(_ context.Context, _ *backend.Member, p backend.Placement) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cancelled = append(d.cancelled, p)
	return nil
}

func (d *testDriver) Forget(context.Context, *backend.Member, backend.Placement) error { return nil }

// work is the worker of the attempt req is for, placed as p: once the
// placement is recorded, as the worker API's claim waits for, it claims
// the attempt and ends it as end says, and wakes the attempt's supervisor.
// It stores the end as the worker API does: the part of its result that
// run state holds with the attempt, and the private part as the job's part
// of the run's secret record.
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
		case a.Placed() || a.State != store.AttemptPlacing:
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
	if end.wait != nil {
		select {
		case <-ctx.Done():
			return
		case <-end.wait:
		}
	}
	credential := make([]byte, 32)
	_, _ = rand.Read(credential)
	if end.running {
		if err := d.c.store.Tx(ctx, func(tx *store.Tx) error {
			claimed, err := tx.Claim(req.BootstrapToken, hex.EncodeToString(credential), workerapi.Worker{Version: "test", Digest: p.WorkerDigest})
			if err != nil {
				return err
			}
			return tx.StepStarted(claimed.ID)
		}); err == nil && end.started != nil {
			close(end.started)
		}
		return
	}
	report := workerapi.Complete{Result: end.result}
	if end.problem != nil {
		report = workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonSetupFailed, Problem: end.problem}
	}
	private, state := report.Result.Split()
	outcome := report.Result.Outcome
	report.Result = state
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
		if len(private.Outputs) > 0 || len(private.Steps) > 0 {
			if err := d.putJobRecord(ctx, tx, claimed, private); err != nil {
				return err
			}
		}
		if claimed.Private {
			report.Problem, report.Diagnostics = problems.MaskProblem(report.Problem), problems.MaskDiagnostics(report.Diagnostics)
		}
		body, err := workerapi.Marshal(&report)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		return tx.EndAttempt(claimed.ID, store.AttemptEnd{
			Kind: store.EndCompleted, Outcome: outcome, Reason: report.Reason, CompleteDigest: sum[:], Report: body, Problem: report.Problem,
		})
	})
	if err != nil {
		return
	}
	d.c.wakeJob(ctx, a.Run, a.Job)
}

// putJobRecord stores private, the private part of the result the attempt
// a completed with, as its job's part of its run's secret record, at the
// part's next generation.
func (d *testDriver) putJobRecord(ctx context.Context, tx *store.Tx, a *store.Attempt, private plan.JobRecord) error {
	wrapped, err := tx.WrappedKey(a.Run)
	if err != nil {
		return err
	}
	parts, err := tx.JobRecords(a.Run, []string{a.Job})
	if err != nil {
		return err
	}
	gen := parts[a.Job].Generation + 1
	sealed, err := d.c.sealer.SealJob(ctx, a.Run, a.Job, gen, wrapped, private)
	if err != nil {
		return err
	}
	return tx.PutJobRecord(a.Run, a.Job, gen, sealed)
}
