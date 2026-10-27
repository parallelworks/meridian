package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/coordinator/internal/runlog/runlogtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/worker"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// TestRunsConformance runs each case of the conformance suite's runs area
// from its start to its end, in process: the coordinator stores the run,
// starts it, and places each job's attempt with workerDriver, whose worker
// is meridian's, which runs the job's steps on this host and reports to
// the coordinator's server of the worker API on a Unix socket.
func TestRunsConformance(t *testing.T) {
	t.Parallel()
	cases, err := conformance.Cases("runs")
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("the runs area has no cases")
	}
	for _, cs := range cases {
		t.Run(strings.TrimPrefix(cs.Name, "runs/"), func(t *testing.T) {
			parallel(t)
			want := cs.Expect.Run
			if want == nil {
				t.Fatal("the case says nothing of how the run ends")
			}
			r, id := runCase(t, cs)
			c, logs := r.c, r.logs
			run := storedRun(t, c, id)
			if string(run.Outcome) != want.Outcome {
				t.Errorf("the run ended with %s, want %s", run.Outcome, want.Outcome)
			}
			var jobs []*store.Job
			if err := c.store.View(t.Context(), func(tx *store.Tx) error {
				var err error
				jobs, err = tx.Jobs(id)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if len(jobs) != len(want.Jobs) {
				t.Errorf("the run has %d jobs, want %d", len(jobs), len(want.Jobs))
			}
			if slices.ContainsFunc(jobs, func(j *store.Job) bool { return j.Attempts > 0 }) && r.claims.Load() == 0 {
				t.Error("no job placed was checked against its claim")
			}
			for _, j := range jobs {
				w, ok := want.Jobs[j.Key]
				switch {
				case !ok:
					t.Errorf("the run has the job %s, which the case doesn't", j.Key)
					continue
				case string(j.Outcome) != w.Outcome:
					t.Errorf("the job %s ended with %s, want %s (%v)", j.Key, j.Outcome, w.Outcome, j.Diagnostics)
				}
				got := logs.Of(id, j.Key, j.Attempts)
				for _, l := range w.Logs {
					key := workerapitest.LogKey{Part: workerapi.LogStep, Step: l.Step, Try: l.Try}
					if l.Cleanup {
						key.Part = workerapi.LogCleanup
					}
					log, ok := got[key]
					switch {
					case !ok:
						t.Errorf("the job %s has no log %+v: it has %v", j.Name, key, logKeys(got))
					case string(log.Data) != l.Text || !log.Ended:
						t.Errorf("the job %s's log %+v is %q, ended %v; want %q", j.Name, key, log.Data, log.Ended, l.Text)
					}
				}
			}
		})
	}
}

// logKeys returns which logs logs holds.
func logKeys(logs map[workerapitest.LogKey]workerapitest.Log) []string {
	var out []string
	for k := range logs {
		out = append(out, fmt.Sprintf("%+v", k))
	}
	return out
}

// A workerRig is a coordinator whose jobs' workers are meridian's: it
// places each attempt with a workerDriver, and serves the worker API to
// the workers on a Unix socket, storing their logs in logs. It checks that
// the claim of each attempt it places gives the job its supervisor placed,
// byte for byte, and counts those it checked in claims.
type workerRig struct {
	c    *Coordinator
	d    *workerDriver
	logs *runlogtest.Logs
	// runs is where runs' directories are.
	runs   string
	claims atomic.Int64
}

// newWorkerRig returns a workerRig whose runs read vars as they start.
func newWorkerRig(t *testing.T, vars compile.Variables) *workerRig {
	t.Helper()
	// A Unix socket's path is short: the test's own temporary directory
	// can be too long for one.
	dir, err := os.MkdirTemp("", "meridian-run-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s")
	d := &workerDriver{dir: dir, log: t.Logf, placements: map[string]backend.Placement{}, cancels: map[string]context.CancelFunc{}}
	c := open(t, Options{
		Variables: vars,
		Drivers:   map[string]backend.Driver{testBackend: d},
		Endpoint:  backend.Endpoint{Address: socket},
	})
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	logs := &runlogtest.Logs{}
	r := &workerRig{c: c, d: d, logs: logs, runs: filepath.Join(dir, "runs")}
	c.placed = func(run *store.Run, attempt string, job *plan.ReadyJob) {
		if checkClaimed(t, c, run, attempt, job) {
			r.claims.Add(1)
		}
	}
	ctx, stop := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.ServeWorkers(ctx, ln, logs) }()
	t.Cleanup(func() {
		// The workers end before the server they report to stops: each
		// has once its run has ended, but in a test that failed.
		d.mu.Lock()
		for _, cancel := range d.cancels {
			cancel()
		}
		d.mu.Unlock()
		d.workers.Wait()
		stop()
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	return r
}

// checkClaimed checks that a claim of the attempt of run gives job, which
// its supervisor placed, byte for byte, as the worker API's server readies
// it (claimReadier), and reports whether it could.
func checkClaimed(t *testing.T, c *Coordinator, run *store.Run, attempt string, job *plan.ReadyJob) bool {
	ctx := context.WithoutCancel(t.Context())
	var a *store.Attempt
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		a, err = tx.Attempt(attempt)
		return err
	}); err != nil {
		t.Error(err)
		return false
	}
	claimed, err := claimReadier{c}.ReadyAttempt(ctx, run, a)
	if err != nil {
		t.Errorf("the claim of attempt %s of run %s's job %s: %v", attempt, run.ID, a.Job, err)
		return false
	}
	placed, err := plan.EncodeReadyJob(job)
	if err != nil {
		t.Error(err)
		return false
	}
	got, err := plan.EncodeReadyJob(claimed.Job)
	if err != nil {
		t.Error(err)
		return false
	}
	if !bytes.Equal(placed, got) {
		t.Errorf("run %s's job %s was placed as\n%s\nand its claim gives\n%s", run.ID, a.Job, placed, got)
	}
	return true
}

// create stores a run of workflow, as Create compiles it with inputs and
// contexts, and returns its ID.
func (r *workerRig) create(t *testing.T, workflow []byte, inputs, contexts map[string]any) string {
	t.Helper()
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", workflow, inputs,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Contexts: contexts})
	if err != nil {
		t.Fatal(err)
	}
	if diags.HasErrors() || created == nil {
		t.Fatalf("the workflow has errors: %v", diags)
	}
	id, err := r.c.CreateRun(t.Context(), created, RunOptions{DefaultGroup: "local", RunDir: func(id string) string { return filepath.Join(r.runs, id) }})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// runCase runs the case's workflow from its start to its end, as
// TestRunsConformance says, and returns the rig that ran it, and the
// run's ID.
func runCase(t *testing.T, cs conformance.Case) (*workerRig, string) {
	t.Helper()
	r, id := startCase(t, cs)
	waitEnded(t, r.c, id)
	return r, id
}

// startCase stores and starts the case's run, as runCase does, and returns
// the rig that runs it, and the run's ID, as it runs.
func startCase(t *testing.T, cs conformance.Case) (*workerRig, string) {
	t.Helper()
	data, err := cs.Workflow()
	if err != nil {
		t.Fatal(err)
	}
	r := newWorkerRig(t, conformancetest.StartVariables(cs))
	id := r.create(t, data, cs.Inputs, conformancetest.CreateContexts(cs))
	serve(t, r.c)
	return r, id
}

// TestNeedNeverEnded checks that a job a job needs that never ended, as
// one whose supervisor hopper gave up on, fails as the job is readied,
// with coordinator_job_failed: its attempt is withdrawn, and its
// placement cancelled; and the job, whose if: is always(), is placed, and
// its attempt claimed, the server readying it with the need ended, as the
// coordinator did.
func TestNeedNeverEnded(t *testing.T) {
	parallel(t)
	r := newWorkerRig(t, nil)
	c := r.c
	// a's worker never claims its attempt, which would wake its
	// supervisor, and so have hopper run it again.
	r.d.idle = func(req backend.PlaceRequest) bool { return strings.HasSuffix(req.Name, "-a") }
	id := r.create(t, []byte(`
jobs:
  a:
    steps:
      - run: sleep 600
  b:
    needs: [a]
    if: ${{ always() }}
    steps:
      - run: echo b ${{ needs.a.result }}
`), nil, nil)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	// a is placed, and then hopper gives up on its supervisor.
	if _, err := c.supervise(t.Context(), id, "a", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.inserter.JobCancel(t.Context(), storedJob(t, c, id, "a").SupervisorID); err != nil {
		t.Fatal(err)
	}
	serve(t, c)
	run := waitEnded(t, c, id)
	a, b := storedJob(t, c, id, "a"), storedJob(t, c, id, "b")
	attempt := latestAttempt(t, c, id, "a")
	switch {
	case run.Outcome != plan.OutcomeFailure:
		t.Errorf("the run ended with %s", run.Outcome)
	case a.Outcome != plan.OutcomeFailure || !hasCode(a.Diagnostics, problems.CoordinatorJobFailed) || attempt.End.Kind != store.EndWithdrawn:
		t.Errorf("a ended with %s, %v, and its attempt %s", a.Outcome, a.Diagnostics, attempt.End.Kind)
	case b.Outcome != plan.OutcomeSuccess:
		t.Errorf("b ended with %s, %v", b.Outcome, b.Diagnostics)
	case run.AnyJobFailedAt.IsZero():
		t.Error("the run isn't marked as one a job of which failed")
	}
	if log := r.logs.Of(id, "b", 1)[workerapitest.LogKey{Part: workerapi.LogStep}]; string(log.Data) != "b failure\n" {
		t.Errorf("b's step's log is %q", log.Data)
	}
	r.d.mu.Lock()
	cancelled := r.d.cancelled
	r.d.mu.Unlock()
	if !slices.Equal(cancelled, []string{"worker-" + attempt.ID}) {
		t.Errorf("the placements cancelled are %v", cancelled)
	}
}

// workerDriver is a backend.Driver in process, for tests, whose members
// can run every job, and whose workers are meridian's (worker.Run), each
// in a goroutine of its own, with the bootstrap token in a file of its
// own, under dir. log hears of a worker that ended with an error.
type workerDriver struct {
	dir string
	log func(format string, args ...any)
	// idle, if it is set, says which placements start no worker; and gate,
	// if it is set, holds each worker back from claiming its attempt until
	// it is closed.
	idle    func(req backend.PlaceRequest) bool
	gate    <-chan struct{}
	workers sync.WaitGroup

	mu         sync.Mutex
	placements map[string]backend.Placement
	cancels    map[string]context.CancelFunc
	// cancelled are the IDs of the placements Cancel cancelled.
	cancelled []string
}

var _ backend.Driver = (*workerDriver)(nil)

func (d *workerDriver) Probe(context.Context, *backend.Member, backend.Demand) (backend.Fit, error) {
	return backend.Fit{Availability: backend.StartsNow}, nil
}

func (d *workerDriver) Connect(_ context.Context, _ *backend.Member, coordinator backend.Endpoint) (backend.Endpoint, io.Closer, error) {
	return coordinator, nil, nil
}

func (d *workerDriver) Place(ctx context.Context, _ *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	dir := filepath.Join(d.dir, "workers", req.Attempt)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return backend.Placement{}, err
	}
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte(req.BootstrapToken), 0o600); err != nil {
		return backend.Placement{}, err
	}
	var environ []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "MERIDIAN_") {
			environ = append(environ, kv)
		}
	}
	environ = append(environ, workerapi.EnvCoordinator+"="+req.Coordinator.Address, workerapi.EnvBootstrapTokenFile+"="+token)
	p := backend.Placement{Attempt: req.Attempt, ID: "worker-" + req.Attempt, WorkerDigest: testDigest}
	// The worker outlives the call that placed it.
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	d.mu.Lock()
	d.placements[req.Attempt] = p
	d.cancels[req.Attempt] = cancel
	d.mu.Unlock()
	if d.idle != nil && d.idle(req) {
		return p, nil
	}
	gate := d.gate
	d.workers.Go(func() {
		defer cancel()
		if gate != nil {
			select {
			case <-ctx.Done():
				return
			case <-gate:
			}
		}
		err := worker.Run(ctx, worker.Config{Worker: workerapi.Worker{Version: "test", Digest: testDigest}, Environ: environ, Dir: dir})
		if err != nil {
			d.log("the worker of attempt %s: %v", req.Attempt, err)
		}
	})
	return p, nil
}

func (d *workerDriver) Find(_ context.Context, _ *backend.Member, attempt string) (backend.Placement, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p, ok := d.placements[attempt]
	return p, ok, nil
}

func (d *workerDriver) Check(context.Context, *backend.Member, backend.Placement) (backend.Status, error) {
	return backend.Status{State: backend.Unknown}, nil
}

func (d *workerDriver) Cancel(_ context.Context, _ *backend.Member, p backend.Placement) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cancelled = append(d.cancelled, p.ID)
	if cancel := d.cancels[p.Attempt]; cancel != nil {
		cancel()
	}
	return nil
}

func (d *workerDriver) Forget(context.Context, *backend.Member, backend.Placement) error { return nil }

// TestCancelUnclaimed checks that a job placed, whose worker hadn't
// claimed its attempt when its run was cancelled, isn't withdrawn: its
// worker reads the cancel before its first step, so, as format 1 would
// have started the job already, its plain step ends cancelled, with its
// cleanup, and its always() step runs; and the job ends cancelled.
func TestCancelUnclaimed(t *testing.T) {
	parallel(t)
	r := newWorkerRig(t, nil)
	gate := make(chan struct{})
	r.d.gate = gate
	c := r.c
	id := r.create(t, []byte(`
jobs:
  main:
    steps:
      - run: echo plain
        cleanup: echo cleanup
      - if: ${{ always() }}
        run: echo always
`), nil, nil)
	serve(t, c)
	a := latestIs(t, c, id, "main", func(a *store.Attempt) bool { return a.Placed() })
	if err := c.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if ctl := controlOf(t, c, a.ID); !ctl.Cancel || ctl.Reason != plan.ReasonCancelled {
		t.Errorf("the placement's worker is told %+v", ctl)
	}
	close(gate)
	run := waitEnded(t, c, id)
	j := storedJob(t, c, id, "main")
	switch got := latestAttempt(t, c, id, "main"); {
	case run.Outcome != plan.OutcomeCancelled:
		t.Errorf("the run ended %s", run.Outcome)
	case j.Outcome != plan.OutcomeCancelled:
		t.Errorf("the job is %+v", j)
	case got.ID != a.ID || got.End.Kind != store.EndCompleted:
		t.Errorf("the attempt is %+v", got)
	}
	logs := r.logs.Of(id, "main", 1)
	for key, want := range map[workerapitest.LogKey]string{
		{Part: workerapi.LogCleanup, Step: 0}: "cleanup\n",
		{Part: workerapi.LogStep, Step: 1}:    "always\n",
	} {
		if got := string(logs[key].Data); got != want {
			t.Errorf("the log %+v is %q, want %q", key, got, want)
		}
	}
	if _, ran := logs[workerapitest.LogKey{Part: workerapi.LogStep, Step: 0}]; ran {
		t.Error("the plain step ran")
	}
	r.d.mu.Lock()
	cancelled := r.d.cancelled
	r.d.mu.Unlock()
	if len(cancelled) != 0 {
		t.Errorf("the placements cancelled are %v", cancelled)
	}
}
