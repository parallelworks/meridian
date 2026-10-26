package coordinator

import (
	"context"
	jsonv2 "encoding/json/v2"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// passThrough is a platform that takes every value it's given as it is.
type passThrough struct{}

func (passThrough) ResolveInput(_ context.Context, _ string, _ map[string]any, v any) (any, error) {
	return v, nil
}

// TestStartConformance starts each case of the conformance suite's start
// area through the coordinator: the run created, stored with its plan and
// its secret record sealed, both read back and decoded, and started, with
// what Start gave stored and read back too. What it stores is what Start
// gives in memory: run state, the secret record, and the diagnostics, byte
// for byte.
func TestStartConformance(t *testing.T) {
	cases, err := conformance.Cases("start")
	if err != nil {
		t.Fatal(err)
	}
	// Some cases start, and some fail as they start: the test checks
	// both. A cleanup runs once the cases have.
	var started, failed atomic.Int64
	t.Cleanup(func() {
		t.Logf("%d cases started, and %d failed as they started, of %d", started.Load(), failed.Load(), len(cases))
		if started.Load() == 0 || failed.Load() == 0 {
			t.Error("the cases didn't both start and fail")
		}
	})
	for _, cs := range cases {
		t.Run(strings.TrimPrefix(cs.Name, "start/"), func(t *testing.T) {
			t.Parallel()
			data, err := cs.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			opts := compile.Options{Contexts: conformancetest.CreateContexts(cs)}
			var inputs compile.InputResolver
			if !cs.NoPlatform {
				inputs, opts.Inputs = passThrough{}, passThrough{}
			}
			created, _, err := compile.Create(t.Context(), "workflow.yaml", data, cs.Inputs, opts)
			if err != nil {
				t.Fatal(err)
			}
			if created == nil {
				return // Create's errors: there is no run to start
			}
			// Start's own record: a copy of what Create gave, which Keep
			// adds to.
			mine := copyRecord(t, &created.Record)

			c := open(t, Options{Variables: conformancetest.StartVariables(cs), Inputs: inputs})
			id, err := c.CreateRun(t.Context(), created, runOptions)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.startRun(t.Context(), id, nil); err != nil {
				t.Fatal(err)
			}

			res, diags, err := compile.Start(t.Context(), created.Plan, compile.StartInput{
				RunID: id, Variables: conformancetest.StartVariables(cs), Record: *mine, Inputs: inputs,
			})
			if err != nil {
				t.Fatal(err)
			}
			r := storedRun(t, c, id)
			same(t, "diagnostics", r.Diagnostics, diags)
			if diags.HasErrors() {
				failed.Add(1)
				if r.State != store.RunEnded || r.Outcome != plan.OutcomeFailure || storedRecord(t, c, id) != nil {
					t.Errorf("a run whose Start failed is %s, %s", r.State, r.Outcome)
				}
				return
			}
			started.Add(1)
			state := res.Keep(mine)
			if r.State != store.RunRunning {
				t.Fatalf("the run is %s", r.State)
			}
			stored, err := compile.EncodeStartState(r.StartState)
			if err != nil {
				t.Fatal(err)
			}
			want, err := compile.EncodeStartState(&state)
			if err != nil {
				t.Fatal(err)
			}
			if string(stored) != string(want) {
				t.Errorf("the run's start state is\n%s\nwant\n%s", stored, want)
			}
			record, err := plan.EncodeRecord(storedRecord(t, c, id))
			if err != nil {
				t.Fatal(err)
			}
			wantRecord, err := plan.EncodeRecord(mine)
			if err != nil {
				t.Fatal(err)
			}
			if string(record) != string(wantRecord) {
				t.Error("the run's secret record isn't what Start keeps in memory")
			}
		})
	}
}

// copyRecord returns a copy of r that shares nothing with it.
func copyRecord(t *testing.T, r *plan.Record) *plan.Record {
	t.Helper()
	data, err := plan.EncodeRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	out, err := plan.DecodeRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// same checks that got and want encode the same, as JSON.
func same(t *testing.T, what string, got, want any) {
	t.Helper()
	g, err := jsonv2.Marshal(got, jsonv2.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	w, err := jsonv2.Marshal(want, jsonv2.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if string(g) != string(w) {
		t.Errorf("%s =\n%s\nwant\n%s", what, g, w)
	}
}
