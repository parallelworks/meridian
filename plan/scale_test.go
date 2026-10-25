package plan_test

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
)

// A scaledJob is a job of steps, run with a list input of rows, as a
// worker runs it: JobEnv, then each step, then End. The job's env: reads a
// secret variable, every fourth step's run: reads it in a template, which
// gives the step a string of its own to mask, and every other step has an
// id. Each step writes an output of 2 KiB.
type scaledJob struct {
	job *plan.ReadyJob
	run plan.RunValues
	// outputs are what each step writes.
	outputs []map[string]string
}

// newScaledJob returns a scaledJob of steps steps, with rows rows.
func newScaledJob(t testing.TB, steps, rows int) scaledJob {
	t.Helper()
	var b strings.Builder
	b.WriteString(`on:
  execute:
    inputs:
      rows: {type: list, template: {name: {type: string}, note: {type: string}}}
jobs:
  main:
    env:
      TOKEN: ${{ vars.token }}
      FIRST: ${{ inputs.rows[0].name }}
    steps:
`)
	for i := range steps {
		b.WriteString("      - ")
		if i%2 == 0 {
			fmt.Fprintf(&b, "id: s%d\n        ", i)
		}
		if i%4 == 1 {
			fmt.Fprintf(&b, "run: ./deploy.sh --token ${{ env.TOKEN }} --part %d\n", i)
		} else {
			fmt.Fprintf(&b, "run: ./step.sh %d \"$FIRST\"\n", i)
		}
	}
	b.WriteString("    outputs:\n      first: ${{ steps.s0.outputs.out }}\n      last: ${{ needs.main.outputs.out }}\n")
	list := make([]any, rows)
	for i := range list {
		list[i] = map[string]any{"name": fmt.Sprintf("row-%d", i), "note": strings.Repeat("n", 40)}
	}
	token := "tok_" + strings.Repeat("Q", 36)
	p, diags := compile.Compile("workflow.yaml", []byte(b.String()), map[string]any{"rows": list},
		compile.Options{Contexts: map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}}})
	if diags.HasErrors() {
		t.Fatalf("compiling: %v", diags)
	}
	run := runValues(t, p, map[string]string{"token": token})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	outputs := make([]map[string]string, steps)
	for i := range outputs {
		line := fmt.Sprintf("step %d wrote this line of its output, which holds nothing secret\n", i)
		outputs[i] = map[string]string{"out": strings.Repeat(line, 2048/len(line)+1)[:2048]}
	}
	return scaledJob{job: ready.Job, run: run, outputs: outputs}
}

// runIt runs the job's phases as the worker does: JobEnv, then each step,
// given the results of those before it and what they gave to mask, then
// End.
func (s scaledJob) runIt(t testing.TB) {
	t.Helper()
	ctx := t.Context()
	state := plan.JobState{Run: s.run, Masked: s.run.Masked()}
	env, diags, err := s.job.JobEnv(ctx, state)
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	state.JobEnv = env
	masked := append(slices.Clone(state.Masked), env.Masked...)
	results := make([]plan.StepResult, 0, len(s.job.Steps))
	for i := range s.job.Steps {
		state.Steps, state.Masked = slices.Clip(results), slices.Clip(masked)
		rs, diags, err := s.job.Step(ctx, i, state)
		if err != nil || diags.HasErrors() {
			t.Fatal(diags, err)
		}
		masked = append(masked, rs.Masked...)
		results = append(results, plan.StepResult{Outcome: plan.OutcomeSuccess, Outputs: s.outputs[i]})
	}
	state.Steps, state.Masked = results, masked
	if _, diags, err := s.job.End(ctx, state); err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
}

// measure returns how many bytes running the job allocates, and the
// shortest time of five that it takes.
func (s scaledJob) measure(t *testing.T) (uint64, time.Duration) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	s.runIt(t)
	runtime.ReadMemStats(&after)
	var best time.Duration
	for i := range 5 {
		runtime.GC()
		start := time.Now()
		s.runIt(t)
		if took := time.Since(start); i == 0 || took < best {
			best = took
		}
	}
	return after.TotalAlloc - before.TotalAlloc, best
}

// TestJobScales checks that running a job's phases costs as much more as
// the job has more steps, and no more as its run has more input rows that
// its steps don't read: at 20 times the steps, the job allocates no more
// than 40 times as much, and takes no more than 50 times as long, the best
// of five runs each, and at 100 times the rows, no more than twice as much
// and three times as long. A step that read each output of the steps
// before it again, or copied the run's inputs, made a job cost as the
// square of its steps, or as its steps times its rows.
func TestJobScales(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		smallSteps, largeSteps int
		smallRows, largeRows   int
		// allocated and took are how many times as much the larger job can
		// allocate, and how many times as long it can take, as the smaller:
		// twice its linear growth, and more for time, which is noisier.
		allocated, took float64
	}{
		{name: "steps", smallSteps: 10, largeSteps: 200, smallRows: 10, largeRows: 10, allocated: 40, took: 50},
		{name: "rows", smallSteps: 10, largeSteps: 10, smallRows: 100, largeRows: 10000, allocated: 2, took: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			small, smallTime := newScaledJob(t, tc.smallSteps, tc.smallRows).measure(t)
			large, largeTime := newScaledJob(t, tc.largeSteps, tc.largeRows).measure(t)
			if ratio := float64(large) / float64(small); ratio > tc.allocated {
				t.Errorf("%d steps and %d rows allocate %d bytes, and %d steps and %d rows %d: %.1f times as much",
					tc.smallSteps, tc.smallRows, small, tc.largeSteps, tc.largeRows, large, ratio)
			}
			if ratio := float64(largeTime) / float64(smallTime); ratio > tc.took {
				t.Errorf("%d steps and %d rows take %v, and %d steps and %d rows %v: %.1f times as long",
					tc.smallSteps, tc.smallRows, smallTime, tc.largeSteps, tc.largeRows, largeTime, ratio)
			}
		})
	}
}

// BenchmarkJobSteps runs the phases of a job of 10, 50 and 200 steps, as a
// worker does (scaledJob).
func BenchmarkJobSteps(b *testing.B) {
	for _, n := range []int{10, 50, 200} {
		s := newScaledJob(b, n, 10)
		b.Run(fmt.Sprintf("%d steps", n), func(b *testing.B) {
			for b.Loop() {
				s.runIt(b)
			}
		})
	}
}

// BenchmarkJobRows runs the phases of a job of 10 steps whose run has 100
// and 10,000 input rows, as a worker does (scaledJob).
func BenchmarkJobRows(b *testing.B) {
	for _, n := range []int{100, 10000} {
		s := newScaledJob(b, 10, n)
		b.Run(fmt.Sprintf("%d rows", n), func(b *testing.B) {
			for b.Loop() {
				s.runIt(b)
			}
		})
	}
}
