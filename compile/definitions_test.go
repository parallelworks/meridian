package compile

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/format"
)

// What a chain of relative references reads through its definitions is
// found in one pass over the chain: at four times the chain, finding it
// for the chain's first default allocates about four times as much, not
// sixteen, as copying what was found for each reference found did.
func TestThroughDefinitionsScales(t *testing.T) {
	allocated := func(n int) uint64 {
		var b strings.Builder
		b.WriteString("on:\n  execute:\n    inputs:\n")
		for i := range n {
			if i == n-1 {
				fmt.Fprintf(&b, "      a%d: {default: x}\n", i)
			} else {
				fmt.Fprintf(&b, "      a%d: {default: '${{ ..a%d.default }}'}\n", i, i+1)
			}
		}
		b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo\n")
		doc, diags := format.Load("workflow.yaml", []byte(b.String()))
		if diags.HasErrors() {
			t.Fatal(diags)
		}
		r := newInputResolver(doc, nil, Options{})
		loc := []string{"on", "execute", "inputs", "a0", "default"}
		exprs, err := r.workflow.Exprs(loc)
		if err != nil {
			t.Fatal(err)
		}
		// Each expression is parsed once, before it is measured.
		if got := len(r.throughDefinitions(exprs, loc)); got != n-1 {
			t.Fatalf("a chain of %d reads %d expressions through its definitions, want %d", n, got, n-1)
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		r.throughDefinitions(exprs, loc)
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	small, large := allocated(500), allocated(2000)
	if ratio := float64(large) / float64(small); ratio > 8 {
		t.Errorf("for a chain of 500, finding what it reads allocates %d bytes, and of 2000, %d: %.1f times as much", small, large, ratio)
	}
}
