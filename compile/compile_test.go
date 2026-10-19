package compile_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

const sample = `on:
  execute:
    inputs:
      n:
        type: number
        default: 2
jobs:
  build:
    steps:
      - id: make
        run: make -j${{ inputs.n }}
  test:
    needs: [build]
    if: ${{ needs.build.result == 'success' }}
    steps:
      - run: make test
`

// The same workflow and inputs compile to the same bytes, which decode to
// the same plan.
func TestCompileIsDeterministic(t *testing.T) {
	first, diags := compile.Compile("workflow.yaml", []byte(sample), map[string]any{"n": 4}, compile.Options{})
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	a, err := plan.Encode(first)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		again, _ := compile.Compile("workflow.yaml", []byte(sample), map[string]any{"n": 4}, compile.Options{})
		if b, _ := plan.Encode(again); string(b) != string(a) {
			t.Fatalf("compiled differently:\n%s\n%s", a, b)
		}
	}
	decoded, err := plan.Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := plan.Encode(decoded); string(b) != string(a) {
		t.Errorf("decoding changed the plan")
	}
}

func TestSource(t *testing.T) {
	p, _ := compile.Compile("workflow.yaml", []byte(sample), nil, compile.Options{})
	sum := sha256.Sum256([]byte(sample))
	if want := "sha256:" + hex.EncodeToString(sum[:]); p.Run.Source != (plan.Source{Location: "workflow.yaml", Revision: want}) {
		t.Errorf("source = %+v, want the file and the SHA-256 of its text", p.Run.Source)
	}
	p, _ = compile.Compile("workflow.yaml", []byte(sample), nil, compile.Options{
		Source: plan.Source{Location: "https://example.com/repo.git#workflow.yaml", Revision: "0123abc"},
	})
	if p.Run.Source.Revision != "0123abc" || !strings.HasPrefix(p.Run.Source.Location, "https://") {
		t.Errorf("source = %+v, want the caller's", p.Run.Source)
	}
}

func TestNoPlanWithErrors(t *testing.T) {
	p, diags := compile.Compile("workflow.yaml", []byte(strings.Replace(sample, "needs: [build]", "needs: [buld]", 1)), nil, compile.Options{})
	if p != nil || !diags.HasErrors() {
		t.Errorf("got a plan %v with %v", p, diags)
	}
	p, diags = compile.Compile("workflow.yaml", []byte("jobs: ["), nil, compile.Options{})
	if p != nil || !diags.HasErrors() {
		t.Errorf("a file that doesn't load: plan %v, %v", p, diags)
	}
}
