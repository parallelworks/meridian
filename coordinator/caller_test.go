package coordinator

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// schedBackend is the backend of the test groups' batch scheduler's
// member, sched, of the groups batch, whose next member is m1, and
// batch-only (testGroups).
const schedBackend = "sched"

// callerDriver is a workerDriver whose members of schedBackend refuse a
// job that calls a workflow, as a batch scheduler's driver does, and start
// any other now, as its other members do; and which records the member
// each attempt was placed on.
type callerDriver struct {
	*workerDriver

	mu     sync.Mutex
	placed map[string]string
}

func (d *callerDriver) Probe(ctx context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	if m.Backend == schedBackend && demand.CallsWorkflow {
		return backend.Fit{Problem: problems.SchedulerCallerRefused.Problem(problems.Params{"member": m.Name})}, nil
	}
	return d.workerDriver.Probe(ctx, m, demand)
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
// while the run it calls ran: the coordinator tells the member's driver
// that the job calls one, which refuses it, so placement leaves the
// member out, and puts the job on its group's next member, while the
// called run's jobs, which call none, go to the scheduler's member, the
// group's first. So it is with a calling step that its if: skips, with
// each instance of a matrix job, and with a caller that a step called; and
// a caller whose group has only the scheduler's member fails as it is
// placed, with the member's reason, before any of its code runs.
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
