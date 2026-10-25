package compile_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
)

// A startScaled is a run whose size grows with n, which Start completes:
// its workflow, the values submitted for it and its variables. timed is
// set to check the time Start takes as well, for a run whose cost, once,
// was work that allocated nothing.
type startScaled struct {
	name     string
	n        int
	workload func(n int) (string, map[string]any, testVariables)
	timed    bool
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
	{name: "rows each of whose three fields reads a secret variable", n: 1000, workload: func(n int) (string, map[string]any, testVariables) {
		return workflow(`rows: {type: list, template: {name: {type: string}, a: {type: string, default: '${{ vars.S }}-a'}, b: {type: string, default: '${{ vars.S }}-b'}, c: {type: string, default: '${{ vars.S }}-c'}}}`),
			map[string]any{"rows": rows(n, func() map[string]any { return map[string]any{"name": "a"} })},
			testVariables{"vars.S": {Value: "s", Secret: true}}
	}},
	{name: "defaults that each read the next through a definition", n: 100, workload: func(n int) (string, map[string]any, testVariables) {
		var defs strings.Builder
		for i := range n {
			fmt.Fprintf(&defs, "x%05d: {type: string, default: '${{ ..h%05d.default }}${{ vars.S }}'}\n", i, i)
			next := fmt.Sprintf("${{ inputs.x%05d }}", i+1)
			if i == n-1 {
				next = "${{ vars.S }}"
			}
			fmt.Fprintf(&defs, "h%05d: {type: string, optional: true, hidden: true, default: '%s'}\n", i, next)
		}
		return workflow(defs.String()), nil, testVariables{"vars.S": {Value: "s", Secret: true}}
	}},
	{name: "env values that read an input held for Start", n: 250, workload: func(n int) (string, map[string]any, testVariables) {
		var env strings.Builder
		env.WriteString("env:\n")
		for i := range n {
			fmt.Fprintf(&env, "  E%d: ${{ inputs.a }}-%d\n", i, i)
		}
		return workflow(`a: {type: string, default: '${{ vars.A }}'}`, env.String()), nil, testVariables{"vars.A": {Value: "a"}}
	}},
	{name: "rows whose field reads the whole of another list, each list's rows held for Start", n: 250, workload: func(n int) (string, map[string]any, testVariables) {
		return workflow(`m: {type: list, template: {v: {type: string, optional: true, default: '${{ vars.S }}x'}}}
l: {type: list, template: {u: {type: string}, h: {type: string, default: '${{ vars.S }}${{ length(inputs.m) }}'}}}`),
			map[string]any{"m": rows(n, emptyRow), "l": rows(n, func() map[string]any { return map[string]any{"u": "x"} })},
			testVariables{"vars.S": {Value: "s", Secret: true}}
	}},
	{name: "lists whose rows are held for Start, beside a long list", n: 200, timed: true, workload: func(n int) (string, map[string]any, testVariables) {
		var defs strings.Builder
		submitted := map[string]any{"big": rows(100*n, func() map[string]any { return map[string]any{"u": "x"} })}
		for i := range n {
			fmt.Fprintf(&defs, "l%d: {type: list, template: {a: {optional: true, default: '${{vars.S}}'}}}\n", i)
			submitted[fmt.Sprintf("l%d", i)] = []any{map[string]any{}}
		}
		defs.WriteString("big: {type: list, template: {u: {type: string}}}\n")
		return workflow(defs.String()), submitted, testVariables{"vars.S": {Value: "s", Secret: true}}
	}},
}

// TestStartScales checks that starting each of startScaledRuns at four
// times the size allocates about four times as much, in bytes and in
// allocations, not sixteen, and for one that is timed, takes no more than
// eight times as long, the best of five runs each, as TestCompileScales
// times compiling.
func TestStartScales(t *testing.T) {
	for _, w := range startScaledRuns {
		t.Run(w.name, func(t *testing.T) {
			smallBytes, smallAllocs, startSmall := w.measure(t, w.n)
			largeBytes, largeAllocs, startLarge := w.measure(t, 4*w.n)
			if ratio := float64(largeBytes) / float64(smallBytes); ratio > 8 {
				t.Errorf("at %d, Start allocates %d bytes, and at %d, %d: %.1f times as much", w.n, smallBytes, 4*w.n, largeBytes, ratio)
			}
			if ratio := largeAllocs / smallAllocs; ratio > 8 {
				t.Errorf("at %d, Start allocates %.0f times, and at %d, %.0f: %.1f times as often", w.n, smallAllocs, 4*w.n, largeAllocs, ratio)
			}
			if !w.timed {
				return
			}
			times := fastest(startSmall, startLarge)
			if ratio := float64(times[1]) / float64(times[0]); ratio > 8 {
				t.Errorf("at %d, Start takes %v, and at %d, %v: %.1f times as long", w.n, times[0], 4*w.n, times[1], ratio)
			}
		})
	}
}

// measure returns how many bytes starting the run at size n allocates, and
// how many allocations it makes, on average, and a function that starts
// it.
func (w startScaled) measure(t *testing.T, n int) (bytes uint64, allocs float64, start func()) {
	t.Helper()
	src, submitted, vars := w.workload(n)
	c := createRun(t, src, submitted, vars)
	start = func() {
		if res, diags := startRun(t, c, vars, compile.StartInput{}); res == nil {
			t.Fatal(diags)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc, testing.AllocsPerRun(2, start), start
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
