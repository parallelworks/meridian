package compile_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/format"
)

// inputsWorkflow is a workflow with n inputs, each written by input from
// its index.
func inputsWorkflow(n int, input func(i int) string) []byte {
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n")
	for i := range n {
		b.WriteString(input(i))
	}
	b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo\n")
	return []byte(b.String())
}

func benchmarkResolve(b *testing.B, data []byte, submitted map[string]any) {
	b.Helper()
	doc, diags := format.Load("workflow.yaml", data)
	if diags.HasErrors() {
		b.Fatal(diags)
	}
	for b.Loop() {
		_, _ = compile.ResolveInputs(doc, submitted, compile.Options{})
	}
}

func benchmarkCompile(b *testing.B, data []byte) {
	b.Helper()
	for b.Loop() {
		_, _ = compile.Compile("workflow.yaml", data, nil, compile.Options{})
	}
}

// Each input's default reads the next one's, so that they are filled in
// from the last.
func BenchmarkChainedDefaults(b *testing.B) {
	for _, n := range []int{200, 800, 1600} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkResolve(b, inputsWorkflow(n, func(i int) string {
				if i == n-1 {
					return fmt.Sprintf("      a%04d: {type: string, default: x}\n", i)
				}
				return fmt.Sprintf("      a%04d: {type: string, default: '${{ inputs.a%04d }}'}\n", i, i+1)
			}), nil)
		})
	}
}

// Each input's hidden reads the next one's.
func BenchmarkReadHidden(b *testing.B) {
	for _, n := range []int{200, 800} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkResolve(b, inputsWorkflow(n, func(i int) string {
				hidden := "false"
				if i < n-1 {
					hidden = fmt.Sprintf("'${{ ..a%04d.hidden }}'", i+1)
				}
				return fmt.Sprintf("      a%04d: {type: string, optional: true, hidden: %s}\n", i, hidden)
			}), nil)
		})
	}
}

// Each input's hidden reads only its own value.
func BenchmarkConditions(b *testing.B) {
	benchmarkResolve(b, inputsWorkflow(800, func(i int) string {
		return fmt.Sprintf("      a%04d: {type: string, optional: true, hidden: '${{ inputs.a%04d == 1 }}'}\n", i, i)
	}), nil)
}

// A run's submitted values name inputs the workflow doesn't have, each
// close to one it has.
func BenchmarkUnknownInputs(b *testing.B) {
	submitted := map[string]any{}
	for i := range 4000 {
		submitted[fmt.Sprintf("ni%04d", i)] = "x"
	}
	benchmarkResolve(b, inputsWorkflow(1000, func(i int) string {
		return fmt.Sprintf("      in%04d: {type: string, optional: true}\n", i)
	}), submitted)
}

// Each job needs jobs that the workflow doesn't have, each close to one it
// has.
func BenchmarkMisspelledNeeds(b *testing.B) {
	var w strings.Builder
	w.WriteString("jobs:\n")
	for i := range 500 {
		fmt.Fprintf(&w, "  job%03d:\n    needs: [", i)
		for k := range 10 {
			fmt.Fprintf(&w, "jbo%03d, ", (i+k+1)%500)
		}
		w.WriteString("]\n    steps:\n      - run: echo\n")
	}
	benchmarkCompile(b, []byte(w.String()))
}

// One long string holds many expressions that each have a warning: on one
// line, and in a literal block.
func BenchmarkWarnings(b *testing.B) {
	b.Run("line", func(b *testing.B) {
		benchmarkCompile(b, []byte("jobs:\n  main:\n    steps:\n      - run: "+strings.Repeat("${{ a.b }} ", 8000)+"\n"))
	})
	b.Run("block", func(b *testing.B) {
		benchmarkCompile(b, []byte("jobs:\n  main:\n    steps:\n      - run: |\n"+strings.Repeat("          echo ${{ a.b }}\n", 3500)))
	})
}
