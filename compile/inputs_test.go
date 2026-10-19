package compile_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/format"
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
	values, _ := inputs.Values.Literal()
	if got := values.(map[string]any)["cluster"].(map[string]any)["ip"]; got != "10.0.0.1" {
		t.Errorf("cluster = %v, want the resolver's details", values)
	}

	_, diags = compile.ResolveInputs(doc, map[string]any{"cluster": "missing"}, compile.Options{Inputs: platform{}})
	if len(diags) != 1 || diags[0].Code != compile.CodeInputValue || !strings.Contains(diags[0].Message, "no cluster named missing") {
		t.Errorf("a cluster the platform rejects: %v", diags)
	}

	doc = load(t, strings.Replace(clusterWorkflow, "compute-clusters", "slurm-partitions\n        resource: ${{ inputs.other }}", 1))
	_, diags = compile.ResolveInputs(doc, map[string]any{"cluster": "debug"}, compile.Options{Inputs: platform{}})
	if len(diags) != 1 || diags[0].Code != compile.CodePlatform {
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
	if _, ok := inputs.Values.Literal(); ok {
		t.Error("a default that reads env is literal")
	}
	data, err := json.Marshal(inputs.Values)
	if err != nil || !strings.Contains(string(data), `{"dir":{"${{":[{"src":" env.HOME "`) {
		t.Errorf("Values encode as %s, %v", data, err)
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
