package compile_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/format"
)

// A scaled is a workload whose size grows with n: a workflow, and the values
// submitted for it.
type scaled struct {
	name string
	// n is the smaller size, and workload makes it at a size.
	n        int
	workload func(n int) ([]byte, map[string]any)
	// stageOnly is set to count what compiling allocates beyond loading,
	// and timed to check the time compiling takes as well, for a workload
	// whose cost, once, was work that allocated nothing.
	stageOnly, timed bool
}

// scaledWorkloads grow with what a workflow, or a run's submitted values,
// holds, and each once took time and memory that grew with its square.
var scaledWorkloads = []scaled{
	{name: "list rows whose template's default reads an input", n: 1000, workload: listRows},
	{name: "rows nested in a row, whose fields read a variable", n: 2500, workload: nestedRows, timed: true},
	{name: "inputs whose ignore is an expression", n: 250, workload: ignoredInputs},
	{name: "defaults that are expressions, and a long submitted list", n: 50, workload: defaultsAndRows},
	{name: "defaults that each read the next", n: 250, workload: chainedDefaults},
	{name: "jobs whose needs is an expression, and a long submitted list", n: 50, workload: needsAndRows},
	{name: "a multi-dropdown's options and its submitted items", n: 250, workload: multiDropdown},
	{name: "references to inputs that aren't there, near those that are", n: 300, workload: nearMissReferences},
	{name: "a value nested deep in a matrix", n: 500, workload: deepValue, stageOnly: true},
	{name: "a form's values for rows whose defaults read their passwords", n: 1000, workload: formRows, timed: true},
	{name: "defaults that read rows of a list held for the run's start", n: 300, workload: heldRowReads, timed: true},
}

// TestCompileScales checks that compiling each of scaledWorkloads at four
// times the size allocates about four times as much, not sixteen, and for
// one that is timed, takes less than ten times as long, the best of three
// runs each.
func TestCompileScales(t *testing.T) {
	for _, w := range scaledWorkloads {
		t.Run(w.name, func(t *testing.T) {
			small, smallTime := w.measure(t, w.n)
			large, largeTime := w.measure(t, 4*w.n)
			if ratio := float64(large) / float64(small); ratio > 8 {
				t.Errorf("at %d, compiling allocates %d bytes, and at %d, %d: %.1f times as much",
					w.n, small, 4*w.n, large, ratio)
			}
			if ratio := float64(largeTime) / float64(smallTime); w.timed && ratio > 10 {
				t.Errorf("at %d, compiling takes %v, and at %d, %v: %.1f times as long",
					w.n, smallTime, 4*w.n, largeTime, ratio)
			}
		})
	}
}

// measure returns how many bytes compiling the workload at size n
// allocates, beyond loading it for one that counts the compile stage only,
// and if it is timed, the shortest time of three that compiling it takes.
func (w scaled) measure(t *testing.T, n int) (uint64, time.Duration) {
	t.Helper()
	data, submitted := w.workload(n)
	if _, diags := format.Load("workflow.yaml", data); diags.HasErrors() {
		t.Fatal(diags)
	}
	allocated := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	compileIt := func() { _, _ = compile.Compile("workflow.yaml", data, submitted, compile.Options{}) }
	bytes := allocated(compileIt)
	if w.stageOnly {
		bytes -= allocated(func() { _, _ = format.Load("workflow.yaml", data) })
	}
	var best time.Duration
	for i := 0; w.timed && i < 3; i++ {
		runtime.GC()
		start := time.Now()
		compileIt()
		if took := time.Since(start); i == 0 || took < best {
			best = took
		}
	}
	return bytes, best
}

// withInputs returns a workflow with inputs, each a line of their
// definitions, and jobs, or one job.
func withInputs(inputs []string, jobs string) []byte {
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n")
	for _, line := range inputs {
		b.WriteString("      " + line + "\n")
	}
	if jobs == "" {
		jobs = "jobs:\n  main:\n    steps:\n      - run: echo\n"
	}
	b.WriteString(jobs)
	return []byte(b.String())
}

// rows returns n list rows, each with what row gives it.
func rows(n int, row func() map[string]any) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = row()
	}
	return out
}

func emptyRow() map[string]any { return map[string]any{} }

// listRows is n submitted rows of a list whose template's field has a
// default that reads another input.
func listRows(n int) ([]byte, map[string]any) {
	return withInputs([]string{
		"x: {type: string, default: y}",
		"rows: {type: list, template: {a: {type: string, default: '${{ inputs.x }}'}}}",
	}, ""), map[string]any{"rows": rows(n, emptyRow)}
}

// nestedRows is n rows of a list in one row of another, whose field's
// default reads a variable, and so is evaluated as the run starts: each
// row's part of the outer list's value is under its first row.
func nestedRows(n int) ([]byte, map[string]any) {
	return withInputs([]string{
		"outer: {type: list, template: {inner: {type: list, template: {x: {type: string, default: '${{ vars.A }}'}}}}}",
	}, ""), map[string]any{"outer": []any{map[string]any{"inner": rows(n, emptyRow)}}}
}

// ignoredInputs is n inputs, each ignored by an expression.
func ignoredInputs(n int) ([]byte, map[string]any) {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("a%d: {type: string, ignore: '${{ true }}'}", i)
	}
	return withInputs(lines, ""), nil
}

// defaultsAndRows is n inputs whose defaults are expressions that read
// nothing, beside a list of 10n submitted rows.
func defaultsAndRows(n int) ([]byte, map[string]any) {
	lines := []string{"big: {type: list, template: {x: {type: string}}}"}
	for i := range n {
		lines = append(lines, fmt.Sprintf(`i%d: {type: string, default: '${{ "x" }}'}`, i))
	}
	return withInputs(lines, ""), map[string]any{"big": rows(10*n, func() map[string]any { return map[string]any{"x": "y"} })}
}

// chainedDefaults is n inputs whose defaults each read the next one's.
func chainedDefaults(n int) ([]byte, map[string]any) {
	lines := make([]string, n)
	for i := range lines {
		if i == n-1 {
			lines[i] = fmt.Sprintf("a%d: {type: string, default: x}", i)
		} else {
			lines[i] = fmt.Sprintf("a%d: {type: string, default: '${{ inputs.a%d }}'}", i, i+1)
		}
	}
	return withInputs(lines, ""), nil
}

// needsAndRows is n jobs whose needs is an expression, beside a list of 10n
// submitted rows.
func needsAndRows(n int) ([]byte, map[string]any) {
	var jobs strings.Builder
	jobs.WriteString("jobs:\n  main:\n    steps: [{run: echo}]\n")
	for i := range n {
		fmt.Fprintf(&jobs, "  j%d:\n    needs: ${{ 'main' }}\n    steps: [{run: echo}]\n", i)
	}
	return withInputs([]string{"big: {type: list, template: {x: {type: string}}}"}, jobs.String()),
		map[string]any{"big": rows(10*n, func() map[string]any { return map[string]any{"x": "y"} })}
}

// multiDropdown is a multi-dropdown with n options, given n items, each its
// last option.
func multiDropdown(n int) ([]byte, map[string]any) {
	options := make([]string, n)
	for i := range options {
		options[i] = fmt.Sprintf("o%d", i)
	}
	items := make([]any, n)
	for i := range items {
		items[i] = options[n-1]
	}
	return withInputs([]string{"md: {type: multi-dropdown, options: [" + strings.Join(options, ", ") + "]}"}, ""),
		map[string]any{"md": items}
}

// nearMissReferences is n inputs, and a step that reads 2n inputs that
// aren't there, each a letter off one that is.
func nearMissReferences(n int) ([]byte, map[string]any) {
	lines := make([]string, n)
	var run strings.Builder
	for i := range lines {
		lines[i] = fmt.Sprintf("in%04d: {default: x}", i)
		fmt.Fprintf(&run, "${{inputs.ni%04d}}${{inputs.in%04dx}}", i, i)
	}
	return withInputs(lines, "jobs:\n  main:\n    steps:\n      - run: "+run.String()+"\n"), nil
}

// formRows is n rows a form submits for a list whose template's field has
// a default that reads the row's password, each with what the default
// gives, which the compiler compares with it.
func formRows(n int) ([]byte, map[string]any) {
	return withInputs([]string{
		"creds: {type: list, template: {pass: {type: password}, hint: {type: string, default: '${{ inputs.creds.[index].pass }}'}}}",
	}, ""), map[string]any{"creds": rows(n, func() map[string]any { return map[string]any{"pass": "s", "hint": "s"} })}
}

// heldRowReads is a list of n rows held for the run's start, as a field of
// its rows reads env, which the compiler doesn't have, and n
// inputs whose defaults each read another row of it.
func heldRowReads(n int) ([]byte, map[string]any) {
	lines := []string{"rows: {type: list, template: {t: {type: string, default: '${{ env.HOME }}'}, x: {type: string, default: b}}}"}
	for i := range n {
		lines = append(lines, fmt.Sprintf("i%d: {default: '${{ inputs.rows[%d].x }}'}", i, i))
	}
	return withInputs(lines, ""), map[string]any{"rows": rows(n, emptyRow)}
}

// deepValue is a matrix whose include holds a mapping nested n deep.
func deepValue(n int) ([]byte, map[string]any) {
	value := strings.Repeat("{a: ", n) + "1" + strings.Repeat("}", n)
	return []byte("jobs:\n  a:\n    strategy:\n      matrix:\n        include: [" + value + "]\n    steps:\n      - run: echo\n"), nil
}
