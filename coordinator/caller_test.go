package coordinator

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// schedBackend is the backend of the test groups' batch scheduler's
// member, sched, of the groups batch, whose next member is m1, and
// batch-only (testGroups).
const schedBackend = "sched"

// callerDriver is a workerDriver whose members of schedBackend say they
// are a batch scheduler's (backend.Fit.Batch), as a batch scheduler's
// driver does, and which records the member each attempt was placed on.
type callerDriver struct {
	*workerDriver

	mu     sync.Mutex
	placed map[string]string
}

func (d *callerDriver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	fit, err := d.workerDriver.Probe(ctx, m, demand)
	fit.Batch = m.Backend == schedBackend
	return fit, err
}

func (d *callerDriver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	d.mu.Lock()
	d.placed[req.Attempt] = m.Name
	d.mu.Unlock()
	return d.workerDriver.Place(ctx, m, req)
}

// callerSources are the workflows the tests' callers call: leaf, which
// calls none, and mid, which calls leaf.
var callerSources = childSources(map[string]string{
	"github/t/leaf@main": "jobs:\n  leaf:\n    steps:\n      - run: echo leaf\n",
	"github/t/mid@main":  "jobs:\n  mid:\n    steps:\n      - uses: github/t/leaf@main\n        with: {$yaml: w.yaml}\n",
})

// TestCallerOffScheduler checks that a job that calls a workflow is never
// placed on a batch scheduler's member, which would hold its allocation
// while the run it calls ran: the member's driver says it is a batch
// scheduler's, and the coordinator refuses the job there, so placement
// leaves the member out, and puts the job on its group's next member,
// while the called run's jobs, which call none, go to the scheduler's
// member, the group's first. So it is with a calling step that its if:
// skips, with each instance of a matrix job, and with a caller that a step
// called; and a caller whose group has only the scheduler's member fails
// as it is placed, with the member's reason, before any of its code runs.
func TestCallerOffScheduler(t *testing.T) {
	t.Parallel()
	const call = "      - uses: github/t/leaf@main\n        with: {$yaml: w.yaml}\n"
	for _, tc := range []struct {
		name, group, workflow string
		// on are the members the attempts of each job ran on, by the
		// job's name, as instances' and child runs' jobs are named too.
		on map[string][]string
	}{
		{
			name: "caller", group: "batch", workflow: "jobs:\n  main:\n    steps:\n" + call,
			on: map[string][]string{"main": {"m1"}, "leaf": {"sched"}},
		},
		{
			name: "skipped call", group: "batch",
			workflow: "jobs:\n  main:\n    steps:\n      - run: echo main\n      - if: ${{ false }}\n        uses: github/t/leaf@main\n        with: {$yaml: w.yaml}\n",
			on:       map[string][]string{"main": {"m1"}},
		},
		{
			name: "matrix", group: "batch", workflow: "jobs:\n  fan:\n    strategy:\n      matrix: {n: [1, 2]}\n    steps:\n" + call,
			on: map[string][]string{"fan": {"m1", "m1"}, "leaf": {"sched", "sched"}},
		},
		{
			name: "nested", group: "batch", workflow: "jobs:\n  main:\n    steps:\n      - uses: github/t/mid@main\n        with: {$yaml: w.yaml}\n",
			on: map[string][]string{"main": {"m1"}, "mid": {"m1"}, "leaf": {"sched"}},
		},
		{
			name: "no other member", group: "batch-only", workflow: "jobs:\n  main:\n    steps:\n" + call,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel(t)
			r := newWorkerRig(t, nil)
			d := &callerDriver{workerDriver: r.d, placed: map[string]string{}}
			r.c.opts.Drivers[testBackend], r.c.opts.Drivers[schedBackend] = d, d
			created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(tc.workflow), nil,
				compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: callerSources})
			if err != nil || diags.HasErrors() {
				t.Fatal(err, diags)
			}
			id, err := r.c.CreateRun(t.Context(), created, RunOptions{DefaultGroup: tc.group, RunDir: func(id string) string { return filepath.Join(r.runs, id) }})
			if err != nil {
				t.Fatal(err)
			}
			serve(t, r.c)
			run := waitEnded(t, r.c, id)
			d.mu.Lock()
			defer d.mu.Unlock()
			if tc.on == nil {
				j := storedJob(t, r.c, id, "main")
				if run.Outcome != plan.OutcomeFailure || j.Outcome != plan.OutcomeFailure ||
					!slices.Equal(jobCodes(j), []string{string(problems.PlacementImpossible.Code()), string(problems.SchedulerCallerRefused.Code())}) {
					t.Errorf("the run ended %s, and its job %s, with %v", run.Outcome, j.Outcome, jobCodes(j))
				}
				if len(d.placed) != 0 {
					t.Errorf("attempts were placed: %v", d.placed)
				}
				return
			}
			if run.Outcome != plan.OutcomeSuccess {
				t.Fatalf("the run ended %s", run.Outcome)
			}
			on := map[string][]string{}
			if err := r.c.store.View(t.Context(), func(tx *store.Tx) error {
				for attempt, member := range d.placed {
					a, err := tx.Attempt(attempt)
					if err != nil {
						return err
					}
					name, _, _ := strings.Cut(a.Job, "#")
					on[name] = append(on[name], member)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for name, members := range tc.on {
				if got := on[name]; !slices.Equal(got, members) {
					t.Errorf("job %s ran on %v, want %v", name, got, members)
				}
			}
			if len(on) != len(tc.on) {
				t.Errorf("the jobs ran on %v, want %v", on, tc.on)
			}
		})
	}
}

// TestFitProblem checks the rules the coordinator holds a member's fit to:
// a job that calls a workflow is refused a batch scheduler's member, with
// scheduler_caller_refused, and not another's, nor is a job that calls
// none; the fit's own problem comes first, and the walltime rule after.
func TestFitProblem(t *testing.T) {
	t.Parallel()
	m := &backend.Member{Name: "m1"}
	unhealthy := problems.BackendNotBuilt.Problem(problems.Params{"member": "m1", "backend": "slurm"})
	short := backend.Fit{MaxWalltimeSeconds: 60}
	for _, tc := range []struct {
		name  string
		calls bool
		fit   backend.Fit
		want  *problems.Kind
	}{
		{name: "a caller on a batch scheduler's member", calls: true, fit: backend.Fit{Batch: true}, want: problems.SchedulerCallerRefused},
		{name: "a caller on another member", calls: true, fit: backend.Fit{}},
		{name: "no caller on a batch scheduler's member", fit: backend.Fit{Batch: true}},
		{name: "the fit's problem first", calls: true, fit: backend.Fit{Batch: true, Problem: unhealthy}, want: problems.BackendNotBuilt},
		{name: "the walltime after", calls: true, fit: backend.Fit{Batch: true, MaxWalltimeSeconds: short.MaxWalltimeSeconds}, want: problems.SchedulerCallerRefused},
		{name: "the walltime of another member's caller", calls: true, fit: short, want: problems.WalltimeExceedsMember},
	} {
		got := fitProblem(m, backend.Demand{CallsWorkflow: tc.calls, TimeoutSeconds: 600}, tc.fit)
		switch {
		case tc.want == nil && got != nil:
			t.Errorf("%s: %v", tc.name, got)
		case tc.want != nil && (got == nil || got.Code != tc.want.Code()):
			t.Errorf("%s: %v, want %s", tc.name, got, tc.want.Code())
		case tc.want == problems.SchedulerCallerRefused && got.Params["member"] != "m1":
			t.Errorf("%s: the problem's params are %v", tc.name, got.Params)
		}
	}
}

// TestProbeProblem checks that a job that calls a workflow is refused a
// batch scheduler's member whose Probe couldn't answer, at once, rather
// than asked about again; that another job keeps the Probe's error; and
// that a Probe that answers is held to fitProblem's rules.
func TestProbeProblem(t *testing.T) {
	t.Parallel()
	m := &backend.Member{Name: "m1"}
	busy := errors.New("test: the controller is busy")
	caller := backend.Demand{CallsWorkflow: true}
	err := probeProblem(m, caller, backend.Fit{Batch: true}, busy)
	if p := kindtest.Want(t, err, problems.SchedulerCallerRefused); p.Params["member"] != "m1" {
		t.Errorf("the problem's params are %v", p.Params)
	}
	if err := probeProblem(m, backend.Demand{}, backend.Fit{Batch: true}, busy); !errors.Is(err, busy) {
		t.Errorf("a job that calls no workflow: %v", err)
	}
	if err := probeProblem(m, caller, backend.Fit{}, busy); !errors.Is(err, busy) {
		t.Errorf("a caller on another member: %v", err)
	}
	kindtest.Want(t, probeProblem(m, caller, backend.Fit{Batch: true}, nil), problems.SchedulerCallerRefused)
	if err := probeProblem(m, backend.Demand{}, backend.Fit{}, nil); err != nil {
		t.Errorf("a member that can run the job: %v", err)
	}
}
