package plan_test

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestWorkerValues checks that what a claim gives a job covers each value
// its worker evaluates: every Value a ReadyJob holds, which JobEnv, Step,
// Cleanup and End evaluate, as the Job's field of the same name holds it.
// Each gets a reference to an output of its own, which ReadsNeeds must
// grant, as Reads must what it reads of the run, from the same walk. A
// field added to a step, to its retry:, or to what a ReadyJob holds of the
// plan can't be missed.
func TestWorkerValues(t *testing.T) {
	valueType := reflect.TypeFor[plan.Value]()
	var fields []string
	var fill func(v reflect.Value, name string)
	fill = func(v reflect.Value, name string) {
		switch {
		case v.Type() == valueType:
			v.Set(reflect.ValueOf(mustValue(t, fmt.Sprintf("${{ needs.a.outputs.v%d }}", len(fields)))))
			fields = append(fields, name)
		case v.Type() == reflect.TypeFor[*plan.Plan]():
			// A step's Workflow is unused, and goes with the plan's next
			// version.
		case v.Kind() == reflect.Struct:
			for i := range v.NumField() {
				if f := v.Type().Field(i); f.IsExported() {
					fill(v.Field(i), name+"."+f.Name)
				}
			}
		case v.Kind() == reflect.Pointer:
			v.Set(reflect.New(v.Type().Elem()))
			fill(v.Elem(), name)
		case v.Kind() == reflect.Slice:
			v.Set(reflect.MakeSlice(v.Type(), 1, 1))
			fill(v.Index(0), name+"[0]")
		case v.Kind() == reflect.Map:
			e := reflect.New(v.Type().Elem()).Elem()
			fill(e, name+"[k]")
			v.Set(reflect.MakeMap(v.Type()))
			v.SetMapIndex(reflect.ValueOf("k").Convert(v.Type().Key()), e)
		}
	}
	var j plan.Job
	jv := reflect.ValueOf(&j).Elem()
	for f := range reflect.TypeFor[plan.ReadyJob]().Fields() {
		n := len(fields)
		holder := reflect.New(f.Type).Elem()
		fill(holder, f.Name)
		if len(fields) == n {
			continue // it holds no Value
		}
		dst := jv.FieldByName(f.Name)
		if !dst.IsValid() || dst.Type() != f.Type {
			t.Fatalf("ReadyJob.%s holds values the worker evaluates, and Job has no %s of its type", f.Name, f.Name)
		}
		dst.Set(holder)
	}
	if len(fields) < 15 {
		t.Fatalf("filled only %d values: %v", len(fields), fields)
	}
	outputs := map[string]string{}
	for i := range fields {
		outputs[fmt.Sprintf("v%d", i)] = "x"
	}
	got := j.ReadsNeeds(map[string]plan.JobResult{"a": {Outcome: plan.OutcomeSuccess, Outputs: outputs}})["a"].Outputs
	for i, name := range fields {
		if _, ok := got[fmt.Sprintf("v%d", i)]; !ok {
			t.Errorf("what %s reads isn't granted: it isn't among the values the worker evaluates (workerValues)", name)
		}
	}
}

// A step's retry: clean-on-retry: is evaluated on the worker, by Step, so
// what it reads of the needs, and of the run, is granted: it evaluates the
// same with what a claim carries as with every value.
func TestReadsCleanOnRetry(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      keep: {type: boolean, default: false}
jobs:
  a:
    steps:
      - run: echo
    outputs:
      x: ${{ 'x' }}
  main:
    needs: [a]
    steps:
      - run: echo
        retry:
          max-retries: 1
          clean-on-retry: ${{ needs.a.outputs.x == 'no' || inputs.keep }}
`, nil)
	run := runValues(t, p, nil)
	needs := map[string]plan.JobResult{"a": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "no"}}}
	j := job(t, p, "main")
	ready, _, err := j.Ready(t.Context(), plan.ReadyInput{Run: run, Needs: needs})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	full := plan.JobState{Run: run, Needs: needs}
	claimed := plan.JobState{Run: j.Reads(run), Needs: j.ReadsNeeds(needs)}
	var got []string
	for _, st := range []plan.JobState{full, claimed} {
		s, diags, err := ready.Job.Step(t.Context(), 0, st)
		if err != nil {
			t.Fatal(err)
		}
		r := "no step"
		if s != nil && s.Retry != nil {
			r = strconv.FormatBool(s.Retry.CleanOnRetry)
		}
		got = append(got, fmt.Sprintf("%s %v", r, codes(diags)))
	}
	if got[0] != "true []" || got[1] != got[0] {
		t.Errorf("with every value: %s; with what the claim carries: %s; want true []", got[0], got[1])
	}
}
