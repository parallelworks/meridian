package compile

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
)

// TestViewIsEvalValues checks that the view of the values that expressions
// read, which the resolver changes only where the values, or what is known
// of them, change, is each time what evalValues makes anew: with each
// inputs and compile conformance case's workflow, values and contexts, and
// with random workflows whose inputs read each other.
func TestViewIsEvalValues(t *testing.T) {
	// checked counts the workflows that load, and changes the changes to
	// the view checked, so that the test checks something.
	checked, changes := 0, 0
	check := func(t *testing.T, name string, data []byte, submitted map[string]any, opts Options) {
		t.Helper()
		doc, diags := format.Load("workflow.yaml", data)
		if diags.HasErrors() {
			return
		}
		checked++
		r := newInputResolver(doc, submitted, opts)
		r.checkView = func() {
			changes++
			if want := r.evalValues(); !sameJSON(r.view, want) {
				t.Fatalf("%s: after change %d, the view is\n%v\nand evalValues makes\n%v\nworkflow:\n%s\nsubmitted: %v",
					name, changes, r.view, want, data, submitted)
			}
		}
		r.resolve()
		if r.view != nil && !sameJSON(r.view, r.evalValues()) {
			t.Fatalf("%s: once resolved, the view is\n%v\nand evalValues makes\n%v", name, r.view, r.evalValues())
		}
	}
	for _, area := range []string{"inputs", "compile"} {
		cases, err := conformance.Cases(area)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			contexts := maps.Clone(c.Contexts)
			if len(c.SecretVariables) > 0 {
				if contexts == nil {
					contexts = map[string]any{}
				}
				vars, _ := contexts["vars"].(map[string]any)
				vars = maps.Clone(vars)
				if vars == nil {
					vars = map[string]any{}
				}
				for _, name := range c.SecretVariables {
					vars[name] = expr.Unknown{Name: "vars"}
				}
				contexts["vars"] = vars
			}
			check(t, c.Name, data, c.Inputs, Options{Contexts: contexts, Inputs: passThrough{}})
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	opts := Options{Inputs: passThrough{}, Contexts: map[string]any{
		"vars": map[string]any{"A": "a", "S": expr.Unknown{Name: "vars"}},
	}}
	checked = 0
	for i := range 2000 {
		data, submitted := randomWorkflow(rng)
		check(t, fmt.Sprintf("random workflow %d", i), []byte(data), submitted, opts)
	}
	if checked < 1900 || changes < 10000 {
		t.Errorf("only %d of the 2,000 random workflows load, and the view changed %d times in all", checked, changes)
	}
}

type passThrough struct{}

func (passThrough) ResolveInput(_ string, _ map[string]any, v any) (any, error) { return v, nil }

// sameJSON reports whether a and b are the same values, a NaN the same as
// another, which a default can give.
func sameJSON(a, b any) bool {
	switch a := a.(type) {
	case map[string]any:
		m, ok := b.(map[string]any)
		if !ok || len(a) != len(m) {
			return false
		}
		for k, v := range a {
			w, ok := m[k]
			if !ok || !sameJSON(v, w) {
				return false
			}
		}
		return true
	case []any:
		l, ok := b.([]any)
		if !ok || len(a) != len(l) {
			return false
		}
		for i := range a {
			if !sameJSON(a[i], l[i]) {
				return false
			}
		}
		return true
	case float64:
		f, ok := b.(float64)
		return ok && (a == f || math.IsNaN(a) && math.IsNaN(f))
	}
	return a == b
}

// randomWorkflow returns a random workflow whose inputs read each other,
// and values submitted for it, or nil. Its inputs are of each kind the
// resolver fills in differently: in groups, wizard steps that are
// flattened or not, whose input a has the path of the input a when they
// are, repeated steps, lists with nested lists and passwords in their rows,
// dropdowns and multi-dropdowns with options that are expressions, and
// headers. Their defaults and conditions read each other, rows, relative
// references, and variables known and secret, and the submitted values
// have rows, texts of expressions, values of the wrong type and unknown
// inputs.
func randomWorkflow(rng *rand.Rand) (string, map[string]any) {
	g := &generator{rng: rng}
	return g.workflow()
}

type generator struct {
	rng *rand.Rand
	b   strings.Builder
}

var randomNames = []string{"a", "b", "c", "d", "e"}

func (g *generator) pick(xs ...string) string { return xs[g.rng.IntN(len(xs))] }

func (g *generator) chance(n int) bool { return g.rng.IntN(n) == 0 }

// ref returns an expression that reads inputs, variables or definitions.
func (g *generator) ref(inList bool) string {
	n := g.pick(randomNames...)
	refs := []string{
		"inputs." + n, "inputs." + n + " + '-' + inputs." + g.pick(randomNames...), "inputs.l[0].x", "inputs.l",
		"toJSON(inputs)", ".." + n + ".default", "vars.A", "vars.S", "inputs.g.y", "inputs.g",
		"length(inputs.l)", "inputs." + n + " ?? 'none'", "inputs.l[1].y", "inputs.pw", "inputs.m",
		"inputs.r[0].u", "inputs.w", "''", "inputs.l.*.x", "inputs.l[0:1]", "inputs.l[0].n[0].q",
		"fromJSON(inputs." + n + ")", "inputs.k", "vars.S == 'x' ? inputs." + n + " : 'y'", "inputs.l[0].p",
		"inputs.r", "inputs.dd", "format('{0}', inputs." + n + ")", "0/0", "inputs.s", "inputs.s.w",
	}
	if inList {
		refs = append(refs, "inputs.l[index].y", "inputs.l[index].x + inputs."+n, "inputs.r[index].u")
	}
	return g.pick(refs...)
}

func (g *generator) expression(inList bool) string { return `"${{ ` + g.ref(inList) + ` }}"` }

// dflt returns a default for an input of type typ: a literal of the type,
// or an expression, which is all a password's can be.
func (g *generator) dflt(inList bool, typ string) string {
	if typ == "password" || g.chance(2) {
		return g.expression(inList)
	}
	switch typ {
	case "boolean":
		return g.pick("true", "false")
	case "number":
		return strconv.Itoa(g.rng.IntN(9))
	}
	return g.pick(`""`, `"x"`, `"7"`, `"${{ vars.S }}"`)
}

func (g *generator) condition() string {
	n := g.pick(randomNames...)
	return g.pick("true", "'${{ .hidden }}'", "'${{ .."+n+".hidden }}'", "'${{ inputs."+n+" == 1 }}'",
		"'${{ vars.S == 1 }}'", "'${{ inputs.l[0].y == 1 }}'", "'${{ length(inputs.l) > 1 }}'",
		"'${{ inputs.pw == 1 }}'", "'${{ !inputs."+n+" }}'", "'${{ inputs.s.w == 1 }}'")
}

// input writes an input's definition at indent, and returns its type.
func (g *generator) input(indent, name string, inList bool) string {
	typ := g.pick("string", "string", "number", "boolean", "password")
	fmt.Fprintf(&g.b, "%s%s:\n%s  type: %s\n", indent, name, indent, typ)
	optional := false
	if !g.chance(3) {
		fmt.Fprintf(&g.b, "%s  default: %s\n", indent, g.dflt(inList, typ))
	} else if !g.chance(3) {
		fmt.Fprintf(&g.b, "%s  optional: true\n", indent)
		optional = true
	}
	for _, key := range []string{"hidden", "ignore", "optional"} {
		if g.chance(5) && (!optional || key != "optional") {
			fmt.Fprintf(&g.b, "%s  %s: %s\n", indent, key, g.condition())
		}
	}
	return typ
}

func (g *generator) workflow() (string, map[string]any) {
	b := &g.b
	b.WriteString("on:\n  execute:\n    inputs:\n")
	if g.chance(4) {
		b.WriteString("      $meta:\n        wizard:\n          mode: wizard\n          flatten: false\n")
	}
	n := 2 + g.rng.IntN(len(randomNames)-1)
	types := map[string]string{}
	for _, name := range randomNames[:n] {
		types[name] = g.input("      ", name, false)
	}
	if g.chance(2) {
		b.WriteString("      g:\n        type: group\n        items:\n")
		g.input("          ", "y", false)
		g.input("          ", "z", false)
		for _, key := range []string{"hidden", "ignore"} {
			if g.chance(3) {
				fmt.Fprintf(b, "        %s: %s\n", key, g.condition())
			}
		}
	}
	if g.chance(3) {
		b.WriteString("      s:\n        type: step\n        title: Step\n        options:\n")
		g.input("          ", "a", false)
		g.input("          ", "w", false)
	}
	if g.chance(4) {
		b.WriteString("      h:\n        type: header\n        text: Header\n")
	}
	if !g.chance(3) {
		b.WriteString("      l:\n        type: list\n        optional: true\n        template:\n")
		g.input("          ", "x", true)
		g.input("          ", "y", true)
		if g.chance(3) {
			b.WriteString("          p:\n            type: password\n")
			if g.chance(2) {
				fmt.Fprintf(b, "            default: %s\n", g.expression(true))
			}
		}
		if g.chance(3) {
			b.WriteString("          n:\n            type: list\n            template:\n")
			g.input("              ", "q", true)
			if g.chance(2) {
				b.WriteString("            default: [{}, {q: '${{ inputs.a }}'}]\n")
			}
		}
		switch g.rng.IntN(4) {
		case 0:
			b.WriteString("        default:\n")
			for range 1 + g.rng.IntN(3) {
				fmt.Fprintf(b, "          - x: %s\n", g.dflt(false, "string"))
				if g.chance(2) {
					fmt.Fprintf(b, "            y: %s\n", g.dflt(false, "string"))
				}
			}
		case 1:
			fmt.Fprintf(b, "        default: %s\n", g.pick("'${{ fromJSON(vars.S) }}'", "'${{ fromJSON(vars.A) }}'", "[{x: 1}, {}]"))
		}
		if g.chance(4) {
			b.WriteString("        ignore: " + g.condition() + "\n")
		}
	}
	if g.chance(3) {
		b.WriteString("      r:\n        type: step\n        title: Repeated\n        multi: true\n        options:\n")
		g.input("          ", "u", true)
		g.input("          ", "v", true)
	}
	if g.chance(3) {
		b.WriteString("      m:\n        type: multi-dropdown\n        optional: true\n        options:\n")
		b.WriteString("          - x\n          - 8\n          - \"${{ inputs.a }}\"\n")
		if g.chance(2) {
			b.WriteString("          - value: \"${{ vars.S }}\"\n            label: S\n")
		}
	}
	if g.chance(3) {
		b.WriteString("      dd:\n        type: dropdown\n        optional: true\n        options: [1, \"2\", x, \"\"]\n")
		if g.chance(2) {
			fmt.Fprintf(b, "        default: %s\n", g.pick(`"x"`, `""`, `"1"`, g.expression(false)))
		}
	}
	if g.chance(4) {
		b.WriteString("      pw:\n        type: password\n        optional: true\n")
	}
	if g.chance(5) {
		b.WriteString("      k:\n        type: boolean\n")
	}
	b.WriteString("jobs:\n  main:\n    steps:\n      - run: echo\n")

	if g.chance(3) {
		return b.String(), nil
	}
	submitted := map[string]any{}
	for _, name := range randomNames[:n] {
		switch g.rng.IntN(7) {
		case 0:
			submitted[name] = map[string]any{"number": 5.0, "boolean": true}[types[name]]
			if submitted[name] == nil || g.chance(5) {
				submitted[name] = "v" + name
			}
		case 1:
			submitted[name] = ""
		case 2:
			submitted[name] = float64(g.rng.IntN(3))
		case 3:
			submitted[name] = "${{ " + g.ref(false) + " }}"
		}
	}
	if g.chance(3) {
		var rows []any
		for range g.rng.IntN(5) {
			row := map[string]any{}
			if g.chance(2) {
				row["x"] = "rx"
			}
			if g.chance(3) {
				row["p"] = []any{"secret", 7.0, []any{1.0}}[g.rng.IntN(3)]
			}
			if g.chance(4) {
				row["n"] = []any{map[string]any{}, map[string]any{"q": "nq"}}
			}
			if g.chance(6) {
				rows = append(rows, "not a row")
				continue
			}
			rows = append(rows, row)
		}
		submitted["l"] = rows
	}
	if g.chance(4) {
		submitted["r"] = []any{map[string]any{"u": "ru"}, map[string]any{}}
	}
	if g.chance(3) {
		submitted["m"] = []any{"x", "${{ inputs.a }}", g.pick("${{ vars.S }}", "8", "nope")}
	}
	if g.chance(4) {
		submitted["g"] = []any{map[string]any{"y": "gy", "q": 1.0}, "not a group"}[g.rng.IntN(2)]
	}
	if g.chance(5) {
		submitted["s"] = map[string]any{"w": "sw"}
	}
	if g.chance(4) {
		submitted["unknown"] = 1.0
	}
	if g.chance(5) {
		submitted["dd"] = []any{1.0, "1", 2.0, "x", ""}[g.rng.IntN(5)]
	}
	if g.chance(5) {
		submitted["pw"] = []any{"hunter2", 5.0, map[string]any{}}[g.rng.IntN(3)]
	}
	return b.String(), submitted
}
