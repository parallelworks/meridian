package format

import (
	"hash/fnv"
	"math/rand/v2"
	"strconv"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// The probe, which validate tries first, matches the values the schema
// matches: those of every conformance case's workflow, and of each with a
// part of it changed. The changes to a case's workflow are random, from a
// seed that is the case's name, so that they are the same each time
// whatever other cases there are.
func TestProbeMatchesSchema(t *testing.T) {
	s := schemas[1]()
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
			name := fnv.New64a()
			name.Write([]byte(c.Name))
			rng := rand.New(rand.NewPCG(name.Sum64(), 2))
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

// The schema spells each of its shared values once, in $defs, and every
// key that takes one refers to it: a ${{ }} expression, whose pattern
// message.go tells apart by its text, and a duration. A schema that has a
// pattern has its own type, as a form of an anyOf or oneOf has, so that a
// tool that reads it alone, as generators of types read each form, sees a
// string rather than any value. An if's schemas, which only test a value,
// are the exception.
func TestSchemaSpellsValuesOnce(t *testing.T) {
	doc := schemas[1]().doc
	defs, _ := doc.(map[string]any)["$defs"].(map[string]any)
	shared := map[string]string{}
	for _, name := range []string{"expression", "duration"} {
		def, _ := defs[name].(map[string]any)
		p, ok := def["pattern"].(string)
		if !ok {
			t.Fatalf("$defs/%s has no pattern", name)
		}
		shared[p] = jsonptr.Append("/$defs", name)
	}
	if shared[expression] != "/$defs/expression" {
		t.Errorf("$defs/expression's pattern isn't message.go's, %q", expression)
	}
	// walk checks the schema s, at at, and the schemas in it; test says
	// that it is in an if.
	var walk func(s any, at string, test bool)
	walk = func(s any, at string, test bool) {
		schema, ok := s.(map[string]any)
		if !ok {
			return
		}
		if p, ok := schema["pattern"].(string); ok {
			if shared[p] != "" && shared[p] != at {
				t.Errorf("%s spells out the pattern of %s, which it can refer to", at, shared[p])
			}
			if _, typed := schema["type"]; !typed && !test {
				t.Errorf("%s has a pattern and no type", at)
			}
		}
		for k, v := range schema {
			at := jsonptr.Append(at, k)
			switch k {
			case "properties", "patternProperties", "dependentSchemas", "$defs":
				named, _ := v.(map[string]any)
				for name, s := range named {
					walk(s, jsonptr.Append(at, name), test)
				}
			case "allOf", "anyOf", "oneOf", "prefixItems":
				list, _ := v.([]any)
				for i, s := range list {
					walk(s, jsonptr.Append(at, strconv.Itoa(i)), test)
				}
			case "if":
				walk(v, at, true)
			case "then", "else", "not", "items", "contains", "additionalProperties", "propertyNames", "unevaluatedItems", "unevaluatedProperties":
				walk(v, at, test)
			}
		}
	}
	walk(doc, "", false)
}
