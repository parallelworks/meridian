package compile_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
)

// A startScaled is a run whose size grows with n, which Start completes:
// its workflow, the values submitted for it and its variables.
type startScaled struct {
	name     string
	n        int
	workload func(n int) (string, map[string]any, testVariables)
}

// startScaledRuns grow with what Start completes: the parts of the inputs
// held for it, the rows it fills in, the passwords it puts in their
// places, and the workflow's env:.
var startScaledRuns = []startScaled{
	{name: "submitted rows whose field reads a secret variable and the row", n: 1000, workload: func(n int) (string, map[string]any, testVariables) {
		return workflow(`rows: {type: list, template: {name: {type: string}, at: {type: string, default: '${{ vars.S }}-${{ inputs.rows[index].name }}'}}}`),
			map[string]any{"rows": rows(n, func() map[string]any { return map[string]any{"name": "a"} })},
			testVariables{"vars.S": {Value: "s", Secret: true}}
	}},
	{name: "rows known only at Start, whose fields read the row", n: 1000, workload: func(n int) (string, map[string]any, testVariables) {
		list := "[" + strings.TrimSuffix(strings.Repeat(`{"name":"a"},`, n), ",") + "]"
		return workflow(`rows: {type: list, default: '${{ fromJSON(vars.ROWS) }}', template: {name: {type: string}, at: {type: string, default: '${{ org.O }}-${{ inputs.rows[index].name }}'}}}`),
			nil, testVariables{"vars.ROWS": {Value: list, Secret: true}, "org.O": {Value: "o"}}
	}},
	{name: "passwords in submitted rows held for Start", n: 1000, workload: func(n int) (string, map[string]any, testVariables) {
		return workflow(`rows: {type: list, template: {pass: {type: password}, at: {type: string, default: '${{ vars.S }}'}}}`),
			map[string]any{"rows": rows(n, func() map[string]any { return map[string]any{"pass": "p"} })},
			testVariables{"vars.S": {Value: "s", Secret: true}}
	}},
	{name: "defaults that each read the next, the last a secret's, against the inputs' order", n: 250, workload: func(n int) (string, map[string]any, testVariables) {
		var defs strings.Builder
		for i := range n - 1 {
			fmt.Fprintf(&defs, "i%05d: {type: string, default: '${{ inputs.i%05d }}-'}\n", i, i+1)
		}
		fmt.Fprintf(&defs, "i%05d: {type: string, default: '${{ vars.X }}'}\n", n-1)
		return workflow(defs.String()), nil, testVariables{"vars.X": {Value: "x", Secret: true}}
	}},
	{name: "env values that read an input held for Start", n: 250, workload: func(n int) (string, map[string]any, testVariables) {
		var env strings.Builder
		env.WriteString("env:\n")
		for i := range n {
			fmt.Fprintf(&env, "  E%d: ${{ inputs.a }}-%d\n", i, i)
		}
		return workflow(`a: {type: string, default: '${{ vars.A }}'}`, env.String()), nil, testVariables{"vars.A": {Value: "a"}}
	}},
}

// TestStartScales checks that starting each of startScaledRuns at four
// times the size allocates about four times as much, not sixteen, and
// takes less than ten times as long, the best of three runs each.
func TestStartScales(t *testing.T) {
	for _, w := range startScaledRuns {
		t.Run(w.name, func(t *testing.T) {
			small, smallTime := w.measure(t, w.n)
			large, largeTime := w.measure(t, 4*w.n)
			if ratio := float64(large) / float64(small); ratio > 8 {
				t.Errorf("at %d, Start allocates %d bytes, and at %d, %d: %.1f times as much", w.n, small, 4*w.n, large, ratio)
			}
			if ratio := float64(largeTime) / float64(smallTime); ratio > 10 {
				t.Errorf("at %d, Start takes %v, and at %d, %v: %.1f times as long", w.n, smallTime, 4*w.n, largeTime, ratio)
			}
		})
	}
}

// measure returns how many bytes starting the run at size n allocates, and
// the shortest time of three that starting it takes.
func (w startScaled) measure(t *testing.T, n int) (uint64, time.Duration) {
	t.Helper()
	src, submitted, vars := w.workload(n)
	c := createRun(t, src, submitted, vars)
	start := func() {
		if res, diags := startRun(t, c, vars, compile.StartInput{}); res == nil {
			t.Fatal(diags)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start()
	runtime.ReadMemStats(&after)
	var best time.Duration
	for i := range 3 {
		runtime.GC()
		began := time.Now()
		start()
		if took := time.Since(began); i == 0 || took < best {
			best = took
		}
	}
	return after.TotalAlloc - before.TotalAlloc, best
}

// BenchmarkStart starts each of startScaledRuns, at its smaller size.
func BenchmarkStart(b *testing.B) {
	for _, w := range startScaledRuns {
		b.Run(w.name, func(b *testing.B) {
			src, submitted, vars := w.workload(w.n)
			c := createRun(b, src, submitted, vars)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = startRun(b, c, vars, compile.StartInput{})
			}
		})
	}
}
