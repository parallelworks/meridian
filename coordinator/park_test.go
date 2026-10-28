package coordinator

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// capacityDriver is a workerDriver whose members each run one job at a
// time, as a member that offers what one job asks for does: its Probe
// says a member starts a job now when it runs none, but those that are
// parked, and has capacity for it otherwise, and records what each Probe
// saw.
type capacityDriver struct {
	*workerDriver

	mu sync.Mutex
	// running are the members of the attempts whose workers run, and
	// placed of every attempt placed, by attempt.
	running, placed map[string]string
	probes          []probed
}

// probed is what a Probe of a member saw, and said.
type probed struct {
	member string
	parked []string
	says   backend.Availability
}

func newCapacityDriver(d *workerDriver) *capacityDriver {
	c := &capacityDriver{workerDriver: d, running: map[string]string{}, placed: map[string]string{}}
	d.ended = c.workerEnded
	return c
}

func (d *capacityDriver) Probe(_ context.Context, m *backend.Member, demand backend.Demand) (backend.Fit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	fit := backend.Fit{Availability: backend.StartsNow}
	for attempt, member := range d.running {
		if member == m.Name && !slices.Contains(demand.Parked, attempt) {
			fit.Availability = backend.HasCapacity
		}
	}
	d.probes = append(d.probes, probed{member: m.Name, parked: slices.Clone(demand.Parked), says: fit.Availability})
	return fit, nil
}

func (d *capacityDriver) Place(ctx context.Context, m *backend.Member, req backend.PlaceRequest) (backend.Placement, error) {
	d.mu.Lock()
	d.running[req.Attempt], d.placed[req.Attempt] = m.Name, m.Name
	d.mu.Unlock()
	return d.workerDriver.Place(ctx, m, req)
}

func (d *capacityDriver) workerEnded(attempt string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.running, attempt)
}

// TestParkedCaller checks that a job whose step waits for a child run is
// parked, and lends its member's capacity to the child: placing the
// child's job, the coordinator tells each member's driver that the
// caller's attempt is parked, so that a member that runs one job at a
// time, and runs only the caller, starts the child's job now. On a member
// that runs one job at a time, whose job calls a child that needs the
// member, the run ends. In a group of two, whose first runs the caller,
// both start the child's job now, and the group's order puts it on the
// first, where its caller's files are; were the caller counted against
// the first, the child's job would go to the second. Keeping a child on
// its caller's member whatever the order isn't built.
func TestParkedCaller(t *testing.T) {
	parallel(t)
	cases, err := conformance.Cases("runs")
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(cases, func(c conformance.Case) bool { return c.Name == "runs/calls" })
	if i < 0 {
		t.Fatal("no case runs/calls, whose workflow the test calls")
	}
	workflow := []byte(`
jobs:
  main:
    steps:
      - uses: github/conformance/calls
        with:
          $yaml: greet.yaml
          greeting: hi
          token: none
      - run: cat subworkflows/main/step_0/said
`)
	for _, group := range []string{"local", "pair"} {
		t.Run(group, func(t *testing.T) {
			parallel(t)
			r := newWorkerRig(t, nil)
			r.sources = conformancetest.Sources(cases[i])
			d := newCapacityDriver(r.d)
			r.c.opts.Drivers[testBackend] = d
			created, diags, err := compile.Create(t.Context(), "workflow.yaml", workflow, nil,
				compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: r.sources})
			if err != nil || diags.HasErrors() {
				t.Fatal(err, diags)
			}
			id, err := r.c.CreateRun(t.Context(), created, RunOptions{DefaultGroup: group, RunDir: func(id string) string { return filepath.Join(r.runs, id) }})
			if err != nil {
				t.Fatal(err)
			}
			serve(t, r.c)
			if run := waitEnded(t, r.c, id); run.Outcome != plan.OutcomeSuccess {
				t.Fatalf("the run ended %s", run.Outcome)
			}
			caller := latestAttempt(t, r.c, id, "main")
			var child string
			if err := r.c.store.View(t.Context(), func(tx *store.Tx) error {
				var err error
				child, err = tx.Child(caller.ID, 0, 0)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			called := latestAttempt(t, r.c, child, "greet")
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.placed[caller.ID] != "m1" || d.placed[called.ID] != "m1" {
				t.Errorf("the caller ran on %q, and the child's job on %q, not both on m1", d.placed[caller.ID], d.placed[called.ID])
			}
			saw := slices.ContainsFunc(d.probes, func(p probed) bool {
				return p.member == "m1" && slices.Contains(p.parked, caller.ID) && p.says == backend.StartsNow
			})
			if !saw {
				t.Errorf("no Probe of m1 saw the caller parked, and started the child's job now: %+v", d.probes)
			}
		})
	}
}

// TestParkedFor checks which attempts are parked for a job: its run's
// callers alone, up to its root. A root's job has none; a child's, the
// attempt whose step called it; a grandchild's, that of the child's step
// too; and another tree's job, none of this one's, so that a caller lends
// its room to its own descendants alone, and no member's driver hears of
// another tree's callers.
func TestParkedFor(t *testing.T) {
	parallel(t)
	vars := conformancetest.Variables{"vars.CHILD_VAR": {Value: "v"}}
	c := open(t, Options{Variables: vars})
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	byJob(c, map[string]testEnd{"build": {wait: hold}, "main": {wait: hold}, "g": {wait: hold}})
	root, _ := createTree(t, c, vars)
	other, _ := createTree(t, c, vars)
	serve(t, c)
	latest := func(run, job string) *store.Attempt {
		t.Helper()
		var a *store.Attempt
		waitFor(t, func() bool {
			return c.store.View(t.Context(), func(tx *store.Tx) error {
				var err error
				a, err = tx.LatestAttempt(run, job)
				return err
			}) == nil
		})
		return a
	}
	caller := latest(root, "build")
	child, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: caller.ID, Step: 0, Try: 0, With: map[string]any{"$yaml": "w.yaml", "region": "east"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(child, diags, err)
	}
	childCaller := latest(child, "main")
	grand, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: childCaller.ID, Step: 1, Try: 0, With: map[string]any{"$yaml": "w.yaml"}})
	if err != nil || diags.HasErrors() {
		t.Fatal(grand, diags, err)
	}
	latest(other, "build")
	for _, tc := range []struct {
		what, run string
		want      []string
	}{
		{"the root's job", root, nil},
		{"the child's job", child, []string{caller.ID}},
		{"the grandchild's job", grand, []string{childCaller.ID, caller.ID}},
		{"another tree's job", other, nil},
	} {
		demand, err := c.demandOf(t.Context(), storedRun(t, c, tc.run), &plan.ReadyJob{})
		if err != nil || !slices.Equal(demand.Parked, tc.want) {
			t.Errorf("%s has %v parked for it, %v; want %v", tc.what, demand.Parked, err, tc.want)
		}
	}
}
