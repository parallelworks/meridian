package compile_test

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/problems"
)

// A startScaled is a run whose size grows with n, which Start completes:
// its workflow, the values submitted for it and its variables, and the
// variables as it starts, if they changed since (changed). timed is set to
// check the time Start takes as well, for a run whose cost, once, was work
// that allocated nothing. roundWork is set for a run whose rounds of
// resolving its inputs are each as large as it is, one for each link of a
// chain: it is what each round after the first counts toward the budget
// for each input, row field and env: value (compile.SetRoundWork), so that
// at either size the rounds pass the budget, and Start fails the run with
// expressions_too_much_work, having done no more than the budget allows.
type startScaled struct {
	name      string
	n         int
	workload  func(n int) (string, map[string]any, testVariables)
	changed   testVariables
	timed     bool
	roundWork int
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
	// Each of l's rows was given what its default gives, which Create
	// compares with what it gives once, and Start evaluates once: each
	// evaluation reads all of m.
	{name: "submitted rows whose field is what a default that reads all of another list gives", n: 1000, timed: true, workload: func(n int) (string, map[string]any, testVariables) {
		return workflow(`m: {type: list, template: {v: {type: string, optional: true, default: '${{ vars.V }}x'}}}
l: {type: list, template: {u: {type: string}, h: {type: string, default: '${{ vars.V != '''' && length(toJSON(inputs.m)) > 0 && ''yes'' || ''no'' }}'}}}`),
			map[string]any{"m": rows(n, emptyRow), "l": rows(n, func() map[string]any { return map[string]any{"u": "x", "h": "yes"} })},
			testVariables{"vars.V": {Value: "VARV"}}
	}},
	// Each value a form made from an expression that reads the next value
	// and a variable that changed since, the last the variable's own: Start
	// evaluates each once, in the order they read each other, though the
	// inputs' order is the other way.
	{name: "values a form made, each reading the next, of a variable that changed since, against the inputs' order", n: 250, timed: true, changed: testVariables{"vars.V": {Value: "w"}}, workload: func(n int) (string, map[string]any, testVariables) {
		var defs strings.Builder
		submitted := map[string]any{}
		for i := range n - 1 {
			fmt.Fprintf(&defs, "a%05d: {type: string, default: '${{ inputs.a%05d == vars.V && vars.V || ''x'' }}'}\n", i, i+1)
			submitted[fmt.Sprintf("a%05d", i)] = "v"
		}
		fmt.Fprintf(&defs, "a%05d: {type: string, default: '${{ vars.V }}'}\n", n-1)
		submitted[fmt.Sprintf("a%05d", n-1)] = "v"
		return workflow(defs.String()), submitted, testVariables{"vars.V": {Value: "v"}}
	}},
	// Inputs and env: values that read each other in turn: Start evaluates
	// each once, in the order they read each other, in either of the
	// inputs' orders.
	{name: "inputs and env values that read each other in turn", n: 250, timed: true, workload: func(n int) (string, map[string]any, testVariables) {
		return envInTurn(n, false)
	}},
	{name: "inputs and env values that read each other in turn, against the inputs' order", n: 250, timed: true, workload: func(n int) (string, map[string]any, testVariables) {
		return envInTurn(n, true)
	}},
	// A round of resolving the inputs for each link of a chain: env: values
	// that each read the input before and an input a condition drops, which
	// Start evaluates only once a round has filled the values in; and values
	// a form made, each reading the next through a relative reference, of a
	// variable that changed since. Beside a long list, each round is as
	// large as the list, and the budget stops them.
	{name: "env values that each read an input a condition drops and the input before, beside a long list", n: 500, roundWork: 32 << 10, workload: func(n int) (string, map[string]any, testVariables) {
		var defs, env strings.Builder
		env.WriteString("env:\n")
		for i := range 50 {
			read := "${{ vars.S }}"
			if i > 0 {
				read = fmt.Sprintf("${{ env.E%05d }}+", i-1)
			}
			fmt.Fprintf(&defs, "a%05d: {type: string, default: '%s'}\n", i, read)
			fmt.Fprintf(&defs, "d%05d: {type: string, optional: true, default: d, ignore: '${{ vars.SKIP == ''yes'' }}'}\n", i)
			fmt.Fprintf(&env, "  E%05d: ${{ inputs.d%05d }}${{ inputs.a%05d }}\n", i, i, i)
		}
		defs.WriteString("big: {type: list, template: {u: {type: string}}}\n")
		return workflow(defs.String(), env.String()), map[string]any{"big": rows(n, func() map[string]any { return map[string]any{"u": "x"} })},
			testVariables{"vars.S": {Value: "s", Secret: true}, "vars.SKIP": {Value: "yes"}}
	}},
	{name: "values a form made, each reading the next through a relative reference, of a variable that changed since, beside a long list", n: 500, roundWork: 32 << 10, changed: testVariables{"vars.V": {Value: "w"}}, workload: func(n int) (string, map[string]any, testVariables) {
		var defs strings.Builder
		submitted := map[string]any{"big": rows(n, func() map[string]any { return map[string]any{"u": "x"} })}
		for i := range 50 {
			submitted[fmt.Sprintf("x%05d", i)] = "v"
			if i == 49 {
				fmt.Fprintf(&defs, "x%05d: {type: string, default: '${{ vars.V }}'}\n", i)
				continue
			}
			submitted[fmt.Sprintf("h%05d", i)] = "v"
			fmt.Fprintf(&defs, "x%05d: {type: string, default: '${{ ..h%05d.default }}'}\n", i, i)
			fmt.Fprintf(&defs, "h%05d: {type: string, optional: true, hidden: true, default: '${{ inputs.x%05d == vars.V && vars.V || ''x'' }}'}\n", i, i+1)
		}
		defs.WriteString("big: {type: list, template: {u: {type: string}}}\n")
		return workflow(defs.String()), submitted, testVariables{"vars.V": {Value: "v"}}
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

// envInTurn is n inputs, each of whose defaults reads an env: value that
// reads the input before, the first a secret variable, in that order, or
// with reverse, the other way.
func envInTurn(n int, reverse bool) (string, map[string]any, testVariables) {
	var defs, env strings.Builder
	env.WriteString("env:\n")
	for j := range n {
		i := j
		if reverse {
			i = n - 1 - j
		}
		if i == 0 {
			fmt.Fprintf(&defs, "a%05d: {type: string, default: '${{ vars.S }}'}\n", i)
		} else {
			fmt.Fprintf(&defs, "a%05d: {type: string, default: '${{ env.E%05d }}'}\n", i, i-1)
		}
		fmt.Fprintf(&env, "  E%05d: ${{ inputs.a%05d }}\n", i, i)
	}
	return workflow(defs.String(), env.String()), nil, testVariables{"vars.S": {Value: "s", Secret: true}}
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
	if w.changed != nil {
		vars = w.changed
	}
	if w.roundWork != 0 {
		defer compile.SetRoundWork(w.roundWork)()
	}
	start = func() {
		res, diags := startRun(t, c, vars, compile.StartInput{})
		switch {
		case w.roundWork != 0:
			if res != nil || !slices.ContainsFunc(diags, func(d problems.Diagnostic) bool { return d.Code == problems.ExpressionsTooMuchWork.Code() }) {
				t.Fatalf("at %d, the rounds keep within the budget: %v", n, diags)
			}
		case res == nil:
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
			if w.changed != nil {
				vars = w.changed
			}
			b.ReportAllocs()
			for b.Loop() {
				_, _ = startRun(b, c, vars, compile.StartInput{})
			}
		})
	}
}
