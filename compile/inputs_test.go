package compile_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/problems"
)

func load(t *testing.T, src string) *format.Document {
	t.Helper()
	doc, diags := format.Load("workflow.yaml", []byte(src))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	return doc
}

const clusterWorkflow = `on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
jobs:
  main:
    steps:
      - run: echo ok
`

// platform resolves clusters by name, and provides no other type.
type platform struct{}

func (platform) ResolveInput(typ string, _ map[string]any, v any) (any, error) {
	if typ != "compute-clusters" {
		return nil, compile.ErrInputType
	}
	if v == "missing" {
		return nil, errors.New("there is no cluster named missing")
	}
	return map[string]any{"name": v, "ip": "10.0.0.1"}, nil
}

func TestPlatformInputs(t *testing.T) {
	doc := load(t, clusterWorkflow)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"cluster": "a"}, compile.Options{Inputs: platform{}})
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if got := inputs.Values["cluster"].(map[string]any)["ip"]; got != "10.0.0.1" {
		t.Errorf("cluster = %v, want the resolver's details", inputs.Values)
	}

	_, diags = compile.ResolveInputs(doc, map[string]any{"cluster": "missing"}, compile.Options{Inputs: platform{}})
	if len(diags) != 1 || diags[0].Code != problems.InputRejected.Code() || !strings.Contains(diags[0].Message(), "no cluster named missing") {
		t.Errorf("a cluster the platform rejects: %v", diags)
	}

	doc = load(t, strings.Replace(clusterWorkflow, "compute-clusters", "slurm-partitions\n        resource: ${{ inputs.other }}", 1))
	_, diags = compile.ResolveInputs(doc, map[string]any{"cluster": "debug"}, compile.Options{Inputs: platform{}})
	if len(diags) != 1 || diags[0].Code != problems.PlatformInput.Code() {
		t.Errorf("a type the platform doesn't provide: %v", diags)
	}
}

func TestDeferredDefaultsEncode(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      dir:
        type: string
        default: ${{ env.HOME }}/work
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, nil, compile.Options{})
	if len(diags) != 0 {
		t.Fatal(diags)
	}
	if _, ok := inputs.Values["dir"]; ok {
		t.Error("a default that reads env has a value")
	}
	data, err := json.Marshal(inputs.Deferred["dir"])
	if err != nil || !strings.HasPrefix(string(data), `{"${{":[{"src":" env.HOME "`) {
		t.Errorf("the deferred default encodes as %s, %v", data, err)
	}
}

// A run's submitted values aren't changed by resolving them.
func TestSubmittedValuesUnchanged(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      group:
        type: group
        items:
          n:
            type: number
          extra:
            type: string
            default: x
jobs:
  main:
    steps:
      - run: echo ok
`)
	submitted := map[string]any{"group": map[string]any{"n": "3"}}
	if _, diags := compile.ResolveInputs(doc, submitted, compile.Options{}); len(diags) != 0 {
		t.Fatal(diags)
	}
	if got := submitted["group"].(map[string]any); got["n"] != "3" || len(got) != 1 {
		t.Errorf("submitted values changed to %v", submitted)
	}
}

// A value of the wrong type in a group drops only that value.
func TestNestedValueError(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      g:
        type: string
      s:
        type: group
        items:
          g:
            type: group
            items:
              x:
                type: string
                optional: true
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"g": "mine", "s": map[string]any{"g": "notamap"}}, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.InputGroupNotMapping.Code() || diags[0].Path() != "/on/execute/inputs/s/items/g" {
		t.Fatalf("diagnostics = %v", diags)
	}
	if inputs.Values["g"] != "mine" {
		t.Errorf("g = %#v, want the value given for it", inputs.Values["g"])
	}
	if s, _ := inputs.Values["s"].(map[string]any); s == nil || s["g"] != nil {
		t.Errorf("s = %#v, want s.g dropped", inputs.Values["s"])
	}
}

// Submitted values that aren't JSON, which a plan can't hold, are errors.
func TestValuesMustBeJSON(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      s:
        type: string
        optional: true
jobs:
  main:
    steps:
      - run: echo ok
`)
	for name, v := range map[string]any{
		"NaN":         math.NaN(),
		"infinity":    []any{math.Inf(1)},
		"a channel":   make(chan int),
		"bad UTF-8":   map[string]any{"k": "\xff"},
		"a uint":      uint(1),
		"a bad key":   map[string]any{"\xff": 1},
		"a Go struct": struct{}{},
	} {
		for _, key := range []string{"s", "unknown"} {
			inputs, diags := compile.ResolveInputs(doc, map[string]any{key: v}, compile.Options{})
			if !diags.HasErrors() || diags[0].Code != problems.InputNotJSON.Code() {
				t.Errorf("%s in %s: %v", name, key, diags)
			}
			if _, ok := inputs.Values[key]; ok {
				t.Errorf("%s in %s: the value was kept", name, key)
			}
		}
	}
}

// The expressions of all the inputs' defaults are one group, which builds
// at most 64 MiB, though each default can build 8 MiB.
func TestDefaultsShareABudget(t *testing.T) {
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n      half:\n        type: string\n")
	for i := range 9 {
		fmt.Fprintf(&b, "      d%d:\n        type: string\n        default: ${{ inputs.half + inputs.half }}\n", i)
	}
	b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo ok\n")
	doc := load(t, b.String())
	_, diags := compile.ResolveInputs(doc, map[string]any{"half": strings.Repeat("x", 4<<20)}, compile.Options{})
	if len(diags) != 1 || diags[0].Code != problems.ExpressionsBuildTooMuch.Code() {
		t.Errorf("nine defaults that each build 8 MiB: %v, want %s", diags, problems.ExpressionsBuildTooMuch.Code())
	}
}

// A default doesn't count what the default of an input it reads built.
func TestDefaultsCountOnTheirOwn(t *testing.T) {
	doc := load(t, `on:
  execute:
    inputs:
      half:
        type: string
      a:
        type: string
        default: ${{ inputs.half + inputs.half }}
      b:
        type: string
        default: ${{ inputs.a + inputs.half }}
jobs:
  main:
    steps:
      - run: echo ok
`)
	inputs, diags := compile.ResolveInputs(doc, map[string]any{"half": strings.Repeat("x", 5<<19)}, compile.Options{})
	if b, _ := inputs.Values["b"].(string); len(diags) != 0 || len(b) != 15<<19 {
		t.Errorf("b, 7.5 MiB, reads a, 5 MiB: %d bytes, %v", len(b), diags)
	}
}
