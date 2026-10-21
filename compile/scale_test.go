package compile_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

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
	// stageOnly is set to count what compiling allocates beyond loading.
	stageOnly bool
}

// scaledWorkloads grow with what a workflow, or a run's submitted values,
// holds, and each once took time and memory that grew with its square.
var scaledWorkloads = []scaled{
	{name: "list rows whose template's default reads an input", n: 1000, workload: listRows},
	{name: "inputs whose ignore is an expression", n: 250, workload: ignoredInputs},
	{name: "defaults that are expressions, and a long submitted list", n: 50, workload: defaultsAndRows},
	{name: "defaults that each read the next", n: 250, workload: chainedDefaults},
	{name: "jobs whose needs is an expression, and a long submitted list", n: 50, workload: needsAndRows},
	{name: "a multi-dropdown's options and its submitted items", n: 250, workload: multiDropdown},
	{name: "references to inputs that aren't there, near those that are", n: 300, workload: nearMissReferences},
	{name: "a value nested deep in a matrix", n: 500, workload: deepValue, stageOnly: true},
}

// TestCompileScales checks that compiling each of scaledWorkloads at four
// times the size allocates about four times as much, not sixteen.
func TestCompileScales(t *testing.T) {
	for _, w := range scaledWorkloads {
		t.Run(w.name, func(t *testing.T) {
			small, large := w.allocated(t, w.n), w.allocated(t, 4*w.n)
			if ratio := float64(large) / float64(small); ratio > 8 {
				t.Errorf("at %d, compiling allocates %d bytes, and at %d, %d: %.1f times as much",
					w.n, small, 4*w.n, large, ratio)
			}
		})
	}
}

// allocated returns how many bytes compiling the workload at size n
// allocates, beyond loading it for one that counts the compile stage only.
func (w scaled) allocated(t *testing.T, n int) uint64 {
	t.Helper()
	data, submitted := w.workload(n)
	if _, diags := format.Load("workflow.yaml", data); diags.HasErrors() {
		t.Fatal(diags)
	}
	measure := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	compiled := measure(func() { _, _ = compile.Compile("workflow.yaml", data, submitted, compile.Options{}) })
	if !w.stageOnly {
		return compiled
	}
	return compiled - measure(func() { _, _ = format.Load("workflow.yaml", data) })
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

// deepValue is a matrix whose include holds a mapping nested n deep.
func deepValue(n int) ([]byte, map[string]any) {
	value := strings.Repeat("{a: ", n) + "1" + strings.Repeat("}", n)
	return []byte("jobs:\n  a:\n    strategy:\n      matrix:\n        include: [" + value + "]\n    steps:\n      - run: echo\n"), nil
}
