package format

import (
	"math/rand/v2"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// The probe, which validate tries first, matches the values the schema
// matches: those of every conformance case's workflow, and of each with a
// part of it changed.
func TestProbeMatchesSchema(t *testing.T) {
	s := schemas[1]()
	rng := rand.New(rand.NewPCG(1, 2))
	replacements := []any{nil, true, int64(3), 2.5, "", "text", "${{ inputs.x }}", []any{}, []any{"a"}, map[string]any{}, map[string]any{"type": "string"}}
	for _, area := range []string{"format", "inputs", "compile"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			p := &parser{file: "workflow.yaml", keys: map[string]problems.Position{}, values: map[string]problems.Position{}, styles: map[string]yaml.Style{}}
			v := p.parse(data, DefaultMaxSize)
			for i := range 50 {
				w := v
				if i > 0 {
					w = jsonvalue.Clone(v)
					w = change(rng, w, replacements)
				}
				full, probe := s.Validate(w) == nil, s.probe.Validate(w) == nil
				if full != probe {
					t.Fatalf("%s, change %d: the schema matches it %v, the probe %v", c.Name, i, full, probe)
				}
			}
		}
	}
}

// change replaces one value in v, at random, and returns v.
func change(rng *rand.Rand, v any, replacements []any) any {
	r := jsonvalue.Clone(replacements[rng.IntN(len(replacements))])
	switch c := v.(type) {
	case map[string]any:
		if len(c) == 0 || rng.IntN(4) == 0 {
			return r
		}
		keys := make([]string, 0, len(c))
		for k := range jsonvalue.Sorted(c) {
			keys = append(keys, k)
		}
		k := keys[rng.IntN(len(keys))]
		if rng.IntN(5) == 0 {
			delete(c, k)
		} else {
			c[k] = change(rng, c[k], replacements)
		}
		return c
	case []any:
		if len(c) == 0 || rng.IntN(4) == 0 {
			return r
		}
		i := rng.IntN(len(c))
		c[i] = change(rng, c[i], replacements)
		return c
	}
	return r
}
