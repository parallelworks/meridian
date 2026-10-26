package compile_test

import (
	"fmt"
	"runtime"
	"runtime/debug"
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
	// At 1000 rows, this one compiles in a few milliseconds, too few to time
	// it well.
	{name: "submitted rows whose template's default reads the whole list", n: 4000, workload: crossRows, timed: true},
	{name: "submitted rows whose template's default reads the whole list, with passwords", n: 1000, workload: crossPasswordRows, timed: true},
	{name: "submitted rows whose template's default gives the whole list's JSON and a password", n: 1000, workload: wholeListRows, timed: true},
	{name: "values for unknown inputs taken for passwords, beside a form's rows", n: 1000, workload: unknownPasswordsAndRows, timed: true},
	{name: "submitted defaults that each read the next, the last a password", n: 250, workload: chainedPasswordDefaults, timed: true},
	{name: "submitted defaults that each read the one before by a relative reference", n: 250, workload: relativeDefaults, timed: true},
	{name: "submitted rows whose default reads another list's rows, each evaluated", n: 1000, workload: rowsReadingRows, timed: true},
	{name: "submitted defaults that each read the one before, each what its default gives", n: 250, workload: givenChain(false), timed: true},
	{name: "submitted defaults that each read the next, each what its default gives", n: 250, workload: givenChain(true), timed: true},
	{name: "rows that each read the length of a list whose rows have inputs with no value", n: 500, workload: lengthOfEmptyRows},
}

// TestCompileScales checks that compiling each of scaledWorkloads at four
// times the size allocates about four times as much, not sixteen, and for
// one that is timed, takes no more than eight times as long, the best of
// five runs each: the geometric mean of four, as compiling grows with the
// size, and sixteen, as it grows with the size's square, so that a runner
// that is slow for a while doesn't fail a workload that grows as it
// should, nor pass one that grows with the square.
func TestCompileScales(t *testing.T) {
	for _, w := range scaledWorkloads {
		t.Run(w.name, func(t *testing.T) {
			small, compileSmall := w.measure(t, w.n)
			large, compileLarge := w.measure(t, 4*w.n)
			if ratio := float64(large) / float64(small); ratio > 8 {
				t.Errorf("at %d, compiling allocates %d bytes, and at %d, %d: %.1f times as much",
					w.n, small, 4*w.n, large, ratio)
			}
			if !w.timed {
				return
			}
			times := fastest(compileSmall, compileLarge)
			if ratio := float64(times[1]) / float64(times[0]); ratio > 8 {
				t.Errorf("at %d, compiling takes %v, and at %d, %v: %.1f times as long",
					w.n, times[0], 4*w.n, times[1], ratio)
			}
		})
	}
}

// measure returns how many bytes compiling the workload at size n
// allocates, beyond loading it for one that counts the compile stage only,
// and a function that compiles it.
func (w scaled) measure(t *testing.T, n int) (uint64, func()) {
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
	return bytes, compileIt
}

// fastest returns the shortest time of five that each of fs takes. It runs
// them in turn, each once a round, so that a runner that slows down for a
// while slows each of them alike. It times the work each does, without
// the garbage collector, which collects before each run and not during
// one: what collecting costs depends on what else the process holds, and
// on how many cores it has to itself, and on a runner of two, busy, the
// larger of two runs that grow as they should took nine times as long as
// the smaller at four times its size. The allocation checks say how much
// each allocates.
func fastest(fs ...func()) []time.Duration {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	best := make([]time.Duration, len(fs))
	for i := range 5 {
		for j, f := range fs {
			runtime.GC()
			start := time.Now()
			f()
			if took := time.Since(start); i == 0 || took < best[j] {
				best[j] = took
			}
		}
	}
	return best
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

// crossRows is n submitted rows of a list whose template's field has a
// default that reads the whole list, each with a value for it.
func crossRows(n int) ([]byte, map[string]any) {
	return withInputs([]string{
		"l: {type: list, template: {name: {type: string}, total: {type: number, default: '${{ length(inputs.l) }}'}}}",
	}, ""), map[string]any{"l": rows(n, func() map[string]any { return map[string]any{"name": "a", "total": n} })}
}

// lengthOfEmptyRows is n rows of a list whose template's default reads
// the length of another list of n rows, whose inputs have no value: what
// the evaluation reads of that list holds a Placeholder for each, which
// reading its length doesn't copy it to leave out.
func lengthOfEmptyRows(n int) ([]byte, map[string]any) {
	return withInputs([]string{
		"m: {type: list, optional: true, template: {u: {type: string, optional: true}, b: {type: boolean, optional: true}}}",
		"l: {type: list, optional: true, template: {h: {type: string, optional: true, default: '${{ length(inputs.m) }}'}}}",
	}, ""), map[string]any{
		"m": rows(n, func() map[string]any { return map[string]any{} }),
		"l": rows(n, func() map[string]any { return map[string]any{} }),
	}
}

// crossPasswordRows is crossRows with a password field in each row, so
// that each row's value for the field that reads the list is compared with
// what its default gives.
func crossPasswordRows(n int) ([]byte, map[string]any) {
	return withInputs([]string{
		"l: {type: list, template: {pass: {type: password}, total: {type: number, default: '${{ length(inputs.l) }}'}}}",
	}, ""), map[string]any{"l": rows(n, func() map[string]any { return map[string]any{"pass": "s", "total": n} })}
}

// wholeListRows is n submitted rows of a list whose template's field has a
// default that gives the whole list's JSON and a password, each with a
// value of its own for the field, which is compared with what the default
// gives, a text as long as the list.
func wholeListRows(n int) ([]byte, map[string]any) {
	list := make([]any, n)
	for i := range list {
		list[i] = map[string]any{"p": "s", "h": fmt.Sprintf("v%d", i)}
	}
	return withInputs([]string{
		"pw: {type: password}",
		"l: {type: list, template: {p: {type: password}, h: {type: string, default: '${{ toJSON(inputs.l) }}${{ inputs.pw }}'}}}",
	}, ""), map[string]any{"pw": "s", "l": list}
}

// unknownPasswordsAndRows is formRows, beside n values for inputs the
// workflow doesn't define, each a mapping that holds a password field's
// name, and so taken for a password.
func unknownPasswordsAndRows(n int) ([]byte, map[string]any) {
	data, submitted := formRows(n)
	for i := range n {
		submitted[fmt.Sprintf("u%d", i)] = map[string]any{"pass": "s"}
	}
	return data, submitted
}

// chainedPasswordDefaults is n inputs whose defaults each read the next
// input, the last a password, each given a value: what can read the
// password is found from the last.
func chainedPasswordDefaults(n int) ([]byte, map[string]any) {
	lines := []string{"pw: {type: password}"}
	submitted := map[string]any{"pw": "s"}
	for i := range n {
		next := fmt.Sprintf("a%d", i+1)
		if i == n-1 {
			next = "pw"
		}
		lines = append(lines, fmt.Sprintf("a%d: {type: string, default: '${{ inputs.%s }}'}", i, next))
		submitted[fmt.Sprintf("a%d", i)] = "v"
	}
	return withInputs(lines, ""), submitted
}

// relativeDefaults is n inputs whose defaults each read the one before's
// by a relative reference, each given a value.
func relativeDefaults(n int) ([]byte, map[string]any) {
	lines := []string{"a0: {type: string, default: x}"}
	submitted := map[string]any{}
	for i := 1; i < n; i++ {
		lines = append(lines, fmt.Sprintf("a%d: {type: string, default: '${{ ..a%d.default }}'}", i, i-1))
		submitted[fmt.Sprintf("a%d", i)] = "x"
	}
	return withInputs(lines, ""), submitted
}

// rowsReadingRows is n submitted rows of a list whose template's field has
// a default that reads a password and another list of n rows, each row's
// field evaluated from a default, and each with what the default gives:
// comparing each with the default goes through what the other list's rows
// read only if something read the row.
func rowsReadingRows(n int) ([]byte, map[string]any) {
	l := rows(n, func() map[string]any { return map[string]any{"u": "x", "h": "yes"} })
	return withInputs([]string{
		"pw: {type: password}",
		"x: {type: string, default: y}",
		"m: {type: list, template: {v: {type: string, optional: true, default: '${{ inputs.x }}x'}}}",
		"l: {type: list, template: {u: {type: string}, h: {type: string, default: '${{ inputs.pw != '''' && length(inputs.m) > 0 && ''yes'' || ''no'' }}'}}}",
	}, ""), map[string]any{"pw": "s", "m": rows(n, emptyRow), "l": l}
}

// givenChain returns n inputs whose defaults each read the one before, the
// first a password, or with next each the one after, the last a password,
// each given what its default gives: each comparison goes through what one
// side of the chain reads only if nothing on the other reads it.
func givenChain(next bool) func(n int) ([]byte, map[string]any) {
	return func(n int) ([]byte, map[string]any) {
		lines := []string{"pw: {type: password}"}
		submitted := map[string]any{"pw": "s"}
		for i := range n {
			read := fmt.Sprintf("a%d", i-1)
			switch {
			case next && i == n-1, !next && i == 0:
				read = "pw"
			case next:
				read = fmt.Sprintf("a%d", i+1)
			}
			lines = append(lines, fmt.Sprintf("a%d: {type: string, default: '${{ inputs.%s }}'}", i, read))
			submitted[fmt.Sprintf("a%d", i)] = "s"
		}
		return withInputs(lines, ""), submitted
	}
}

// deepValue is a matrix whose include holds a mapping nested n deep.
func deepValue(n int) ([]byte, map[string]any) {
	value := strings.Repeat("{a: ", n) + "1" + strings.Repeat("}", n)
	return []byte("jobs:\n  a:\n    strategy:\n      matrix:\n        include: [" + value + "]\n    steps:\n      - run: echo\n"), nil
}
