package compile_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
)

// What Meridian encodes of each conformance case's run fits the plan's
// schemas (plan.Schemas): the plan Create gives, and its secret record;
// once the run starts, the record with what Start keeps in it, and the
// run's values, whole and as run state; and each job Ready gives,
// instance by instance, as if what it needs succeeded, with each
// expansion Ready makes.
func TestEncodingsFitTheSchemas(t *testing.T) {
	plans := schematest.Schema(t, "plan/schema/1/plan.schema.json")
	records := schematest.Schema(t, "plan/schema/1/record.schema.json")
	runValues := schematest.Schema(t, "plan/schema/1/runvalues.schema.json")
	jobs := schematest.Schema(t, "plan/schema/1/readyjob.schema.json")
	expansions := schematest.Schema(t, "plan/schema/1/plan.schema.json#/$defs/Expansion")
	checked, started, readied := 0, 0, 0
	for _, area := range []string{"compile", "contexts", "format", "inputs", "start"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			opts := compile.Options{Contexts: contexts(c)}
			if !c.NoPlatform {
				opts.Inputs = passThrough{}
			}
			ctx := t.Context()
			created, _, err := compile.Create(ctx, "workflow.yaml", data, c.Inputs, opts)
			if err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
			if created == nil {
				continue
			}
			checked++
			schematest.Validate(t, plans, c.Name+"'s plan", encoded(t, plan.Encode, created.Plan))
			schematest.Validate(t, records, c.Name+"'s record", encoded(t, plan.EncodeRecord, &created.Record))
			res, _, err := compile.Start(ctx, created.Plan, compile.StartInput{
				RunID: c.Name, Variables: startVariables(c), Record: created.Record, Inputs: opts.Inputs, Sessions: &testSessions{},
			})
			if err != nil {
				t.Fatalf("%s: %v", c.Name, err)
			}
			if res == nil {
				continue
			}
			started++
			record := created.Record
			state := res.Keep(&record)
			schematest.Validate(t, records, c.Name+"'s record as the run starts", encoded(t, plan.EncodeRecord, &record))
			schematest.Validate(t, runValues, c.Name+"'s run values", encoded(t, marshal, &res.Run))
			schematest.Validate(t, runValues, c.Name+"'s run values in run state", encoded(t, marshal, &state.Run))
			for _, j := range created.Plan.Jobs {
				needs := map[string]plan.JobResult{}
				for _, n := range j.Needs {
					needs[n.Job] = plan.JobResult{Outcome: plan.OutcomeSuccess}
				}
				in := plan.ReadyInput{Run: res.Run, Needs: needs}
				var instances []plan.Instance
				if x := res.Expansions[j.Name]; x != nil {
					instances = x.Instances
				}
				ready, _, err := created.Plan.Ready(ctx, j.Name, in)
				if err != nil {
					t.Fatalf("%s, %s: %v", c.Name, j.Name, err)
				}
				if ready != nil && ready.Expansion != nil {
					schematest.Validate(t, expansions, c.Name+"'s expansion of "+j.Name, encoded(t, marshal, ready.Expansion))
					instances = ready.Expansion.Instances
				}
				for _, i := range instances {
					in.Instance = &i
					if ready, _, err = created.Plan.Ready(ctx, j.Name, in); err != nil {
						t.Fatalf("%s, %s: %v", c.Name, i.Key, err)
					}
					if ready != nil && ready.Job != nil {
						readied++
						schematest.Validate(t, jobs, c.Name+"'s job "+i.Key, encoded(t, plan.EncodeReadyJob, ready.Job))
					}
				}
				if instances == nil && ready != nil && ready.Job != nil {
					readied++
					schematest.Validate(t, jobs, c.Name+"'s job "+j.Name, encoded(t, plan.EncodeReadyJob, ready.Job))
				}
			}
		}
	}
	if checked < 150 || started < 140 || readied < 150 {
		t.Errorf("checked %d runs, started %d and readied %d jobs, want more", checked, started, readied)
	}

}

// marshal encodes v as the worker API does (workerapi.Marshal).
func marshal[T any](v *T) ([]byte, error) { return json.Marshal(v, json.Deterministic(true)) }

func encoded[T any](t *testing.T, encode func(*T) ([]byte, error), v *T) []byte {
	t.Helper()
	data, err := encode(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
