package compile

import (
	"context"
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
// of them, change, is what evalValues makes anew: each time it changes, and
// each time an evaluation is to read it, a job's needs too. It does with
// each inputs and compile conformance case's workflow, values and
// contexts, with workflows whose inputs share paths, and with random
// workflows whose inputs read each other.
func TestViewIsEvalValues(t *testing.T) {
	// checked counts the workflows that load, and checks the times the view
	// is checked, so that the test checks something.
	checked, checks := 0, 0
	check := func(t *testing.T, name string, data []byte, submitted map[string]any, opts Options) {
		t.Helper()
		doc, diags := format.Load("workflow.yaml", data)
		if diags.HasErrors() {
			return
		}
		checked++
		r := newInputResolver(doc, submitted, opts)
		r.checkView = func() {
			checks++
			if want := r.evalValues(); !sameJSON(r.view, want) {
				t.Fatalf("%s: at check %d, the view is\n%v\nand evalValues makes\n%v\nworkflow:\n%s\nsubmitted: %v",
					name, checks, r.view, want, data, submitted)
			}
		}
		r.resolve()
		c := &compiler{doc: doc, opts: opts, inputs: r, secrets: map[string]bool{}}
		c.jobs()
		if r.view != nil && !sameJSON(r.view, r.evalValues()) {
			t.Fatalf("%s: once compiled, the view is\n%v\nand evalValues makes\n%v", name, r.view, r.evalValues())
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
	opts := Options{Inputs: passThrough{}, Contexts: map[string]any{
		"vars": map[string]any{"A": "a", "S": expr.Unknown{Name: "vars"}},
	}}
	loaded := checked
	// The input a and the wizard step's input a share a path, and so do
	// their rows' fields: the step's are collected first, since a waits for
	// its form's text, so of the two y with no value, the step's is filled
	// in first.
	check(t, "lists at one path", []byte(`on:
  execute:
    inputs:
      a:
        type: list
        template:
          x: {type: string}
          y: {type: boolean}
        default: [{x: "${{ 'q' }}"}]
      s:
        type: step
        title: Step
        options:
          a: {type: list, template: {y: {type: string}}}
jobs:
  main:
    steps: [{run: echo}]
`), map[string]any{"a": []any{map[string]any{"x": "${{ 'q' }}"}}}, opts)
	if checked == loaded {
		t.Fatal("the workflow with lists at one path doesn't load")
	}
	rng := rand.New(rand.NewPCG(1, 2))
	checked, checks = 0, 0
	for i := range 2000 {
		data, submitted := randomWorkflow(rng)
		check(t, fmt.Sprintf("random workflow %d", i), []byte(data), submitted, opts)
	}
	if checked < 1900 || checks < 20000 {
		t.Errorf("only %d of the 2,000 random workflows load, and the view was checked %d times in all", checked, checks)
	}
}

type passThrough struct{}

func (passThrough) ResolveInput(_ context.Context, _ string, _ map[string]any, v any) (any, error) {
	return v, nil
}

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
// resolver fills in differently: in groups, and wizard steps that are
// flattened or not, whose input a has the path of the input a when they
// are; repeated steps; lists, in groups, in steps, in repeated steps and in
// lists' rows, whose templates hold groups, steps and passwords; dropdowns
// and multi-dropdowns with options that are expressions; and headers. Their
// defaults, conditions and jobs' needs read each other, rows, relative
// references, and variables known and secret; and the submitted values
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

// ref returns an expression that reads inputs, variables or definitions,
// in the rows of the list ctx names, if any.
func (g *generator) ref(ctx string) string {
	n := g.pick(randomNames...)
	refs := []string{
		"inputs." + n, "inputs." + n + " + '-' + inputs." + g.pick(randomNames...), "vars.A", "vars.S", "inputs.pw",
		"inputs.g.y", "inputs.g", "inputs.g.gl[0].q", "inputs.g.gl[0].tg.a", "inputs.g.gl", "length(inputs.g.gl)",
		"inputs.l[0].x", "inputs.l", "inputs.l[0].grp.u", "inputs.l[0].k", "inputs.l[1].grp", "inputs.l.*.x",
		"inputs.l[0:1]", "inputs.l[0].n[0].q", "inputs.l['0'].x", "inputs.l[0]", "inputs.l[1].y", "length(inputs.l)",
		"inputs.r[0].u", "inputs.r[0].rl[0].q", "inputs.r[1].rl", "inputs.r.*.rl", "inputs.r",
		"inputs.w", "inputs.sl[0].q", "inputs.sl", "inputs.s", "inputs.s.w",
		"toJSON(inputs)", ".." + n + ".default", "''", "0/0", "inputs." + n + " ?? 'none'",
		"vars.S == 'x' ? inputs." + n + " : 'y'", "fromJSON(inputs." + n + ")", "inputs.m", "inputs.dd", "inputs.k",
		"format('{0}', inputs." + n + ")",
	}
	switch ctx {
	case "l":
		refs = append(refs, "inputs.l[index].y", "inputs.l[index].grp.u", "inputs.l[index].x + inputs."+n, "inputs.l[index].k")
	case "r":
		refs = append(refs, "inputs.r[index].u", "inputs.r[index].rl", "inputs.r[index].v")
	case "gl":
		refs = append(refs, "inputs.g.gl[index].q", "inputs.g.gl[index].tg.b")
	case "rl":
		refs = append(refs, "inputs.r[index].u", "inputs.r[0].rl[index].q")
	}
	return g.pick(refs...)
}

func (g *generator) expression(ctx string) string { return `"${{ ` + g.ref(ctx) + ` }}"` }

// dflt returns a default for an input of type typ: a literal of the type,
// or an expression, which is all a password's can be.
func (g *generator) dflt(ctx, typ string) string {
	if typ == "password" || g.chance(2) {
		return g.expression(ctx)
	}
	switch typ {
	case "boolean":
		return g.pick("true", "false")
	case "number":
		return strconv.Itoa(g.rng.IntN(9))
	}
	return g.pick(`""`, `"x"`, `"7"`, `"${{ vars.S }}"`, `"${{ vars.A }}"`)
}

func (g *generator) condition() string {
	n := g.pick(randomNames...)
	return g.pick("true", "'${{ .hidden }}'", "'${{ .."+n+".hidden }}'", "'${{ inputs."+n+" == 1 }}'",
		"'${{ vars.S == 1 }}'", "'${{ vars.A == \"a\" }}'", "'${{ inputs.l[0].x == 1 }}'", "'${{ length(inputs.l) > 1 }}'",
		"'${{ inputs.pw == 1 }}'", "'${{ !inputs."+n+" }}'", "'${{ inputs.s.w == 1 }}'", "'${{ inputs.g.gl[0].q == 1 }}'",
		"'${{ inputs.r[0].rl[0].q == 1 }}'", "'${{ inputs.l[0].grp.u == 1 }}'")
}

// input writes an input's definition at indent, in the rows of the list
// ctx names, if any, and returns its type.
func (g *generator) input(indent, name, ctx string) string {
	typ := g.pick("string", "string", "number", "boolean", "password")
	fmt.Fprintf(&g.b, "%s%s:\n%s  type: %s\n", indent, name, indent, typ)
	optional := false
	if !g.chance(3) {
		fmt.Fprintf(&g.b, "%s  default: %s\n", indent, g.dflt(ctx, typ))
	} else if !g.chance(3) {
		fmt.Fprintf(&g.b, "%s  optional: true\n", indent)
		optional = true
	}
	for _, key := range []string{"hidden", "ignore", "optional"} {
		if g.chance(6) && (!optional || key != "optional") {
			fmt.Fprintf(&g.b, "%s  %s: %s\n", indent, key, g.condition())
		}
	}
	return typ
}

// list writes a list's definition at indent, whose template's fields
// fields writes.
func (g *generator) list(indent, name, ctx string, fields func(indent string)) {
	fmt.Fprintf(&g.b, "%s%s:\n%s  type: list\n%s  optional: true\n%s  template:\n", indent, name, indent, indent, indent)
	fields(indent + "    ")
	if g.chance(3) {
		fmt.Fprintf(&g.b, "%s  default: [{}, {q: %s}]\n", indent, g.expression(ctx))
	}
	if g.chance(6) {
		fmt.Fprintf(&g.b, "%s  ignore: %s\n", indent, g.condition())
	}
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
		types[name] = g.input("      ", name, "")
	}
	if g.chance(2) {
		b.WriteString("      g:\n        type: group\n        items:\n")
		g.input("          ", "y", "")
		g.input("          ", "z", "")
		if g.chance(2) {
			g.list("          ", "gl", "gl", func(in string) {
				g.input(in, "q", "gl")
				if g.chance(2) {
					fmt.Fprintf(b, "%stp:\n%s  type: password\n", in, in)
					if g.chance(2) {
						fmt.Fprintf(b, "%s  default: %s\n", in, g.expression("gl"))
					}
				}
				if g.chance(2) {
					fmt.Fprintf(b, "%stg:\n%s  type: group\n%s  items:\n", in, in, in)
					g.input(in+"    ", "a", "gl")
					g.input(in+"    ", "b", "gl")
				}
			})
		}
		for _, key := range []string{"hidden", "ignore"} {
			if g.chance(4) {
				fmt.Fprintf(b, "        %s: %s\n", key, g.condition())
			}
		}
	}
	if g.chance(3) {
		// A wizard step, whose a has the path of the input a when steps
		// are flattened.
		b.WriteString("      s:\n        type: step\n        title: Step\n        options:\n")
		g.input("          ", "a", "")
		g.input("          ", "w", "")
		if g.chance(2) {
			g.list("          ", "sl", "", func(in string) { g.input(in, "q", "") })
		}
	}
	if g.chance(4) {
		b.WriteString("      h:\n        type: header\n        text: Header\n")
	}
	if !g.chance(4) {
		b.WriteString("      l:\n        type: list\n        optional: true\n        template:\n")
		g.input("          ", "x", "l")
		g.input("          ", "y", "l")
		if g.chance(3) {
			b.WriteString("          p:\n            type: password\n")
			if g.chance(2) {
				fmt.Fprintf(b, "            default: %s\n", g.expression("l"))
			}
		}
		if g.chance(3) {
			b.WriteString("          n:\n            type: list\n            template:\n")
			g.input("              ", "q", "l")
			if g.chance(2) {
				b.WriteString("            default: [{}, {q: '${{ inputs.a }}'}]\n")
			}
		}
		if g.chance(2) {
			b.WriteString("          grp:\n            type: group\n            items:\n")
			g.input("              ", "u", "l")
			g.input("              ", "v", "l")
		}
		if g.chance(3) {
			b.WriteString("          stp:\n            type: step\n            title: Step\n            options:\n")
			g.input("              ", "x", "l")
			g.input("              ", "k", "l")
		}
		switch g.rng.IntN(4) {
		case 0:
			b.WriteString("        default:\n")
			for range 1 + g.rng.IntN(3) {
				fmt.Fprintf(b, "          - x: %s\n", g.dflt("", "string"))
				if g.chance(2) {
					fmt.Fprintf(b, "            y: %s\n", g.dflt("", "string"))
				}
				if g.chance(3) {
					fmt.Fprintf(b, "            grp: {u: %s}\n", g.dflt("", "string"))
				}
			}
		case 1:
			fmt.Fprintf(b, "        default: %s\n", g.pick("'${{ fromJSON(vars.S) }}'", "'${{ fromJSON(vars.A) }}'",
				"[{x: 1}, {}]", "'${{ fromJSON(\"[{},{}]\") }}'"))
		}
		if g.chance(5) {
			b.WriteString("        ignore: " + g.condition() + "\n")
		}
	}
	if g.chance(2) {
		b.WriteString("      r:\n        type: step\n        title: Repeated\n        multi: true\n        options:\n")
		g.input("          ", "u", "r")
		g.input("          ", "v", "r")
		if g.chance(2) {
			g.list("          ", "rl", "rl", func(in string) {
				g.input(in, "q", "rl")
				if g.chance(2) {
					fmt.Fprintf(b, "%srp:\n%s  type: password\n", in, in)
				}
			})
		}
		if g.chance(3) {
			b.WriteString("          rg:\n            type: group\n            items:\n")
			g.input("              ", "t", "r")
		}
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
			fmt.Fprintf(b, "        default: %s\n", g.pick(`"x"`, `""`, `"1"`, g.expression("")))
		}
	}
	if g.chance(3) {
		b.WriteString("      pw:\n        type: password\n        optional: true\n")
	}
	if g.chance(5) {
		b.WriteString("      k:\n        type: boolean\n")
	}
	b.WriteString("jobs:\n  main:\n    steps: [{run: echo}]\n")
	for i := range g.rng.IntN(3) {
		fmt.Fprintf(b, "  j%d:\n    needs: \"${{ %s && 'main' || '' }}\"\n    steps: [{run: echo}]\n", i, g.ref(""))
	}

	if g.chance(4) {
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
			submitted[name] = "${{ " + g.ref("") + " }}"
		}
	}
	row := func(keys ...string) any {
		if g.chance(7) {
			return "not a row"
		}
		m := map[string]any{}
		for _, k := range keys {
			if !g.chance(2) {
				continue
			}
			switch k {
			case "n", "rl", "gl", "sl":
				var rows []any
				for range g.rng.IntN(3) {
					rows = append(rows, map[string]any{"q": g.pick("nq", "${{ inputs.a }}", ""), "rp": "secret"})
				}
				m[k] = rows
			case "grp", "tg", "rg":
				m[k] = map[string]any{"u": "gu", "a": "ta", "t": "rt"}
			case "p", "tp":
				m[k] = []any{"secret", 7.0, []any{1.0}}[g.rng.IntN(3)]
			default:
				m[k] = g.pick("r"+k, "", "${{ inputs.a }}")
			}
		}
		return m
	}
	rows := func(keys ...string) []any {
		var out []any
		for range g.rng.IntN(4) {
			out = append(out, row(keys...))
		}
		return out
	}
	if g.chance(2) {
		submitted["l"] = rows("x", "y", "p", "n", "grp", "k")
	}
	if g.chance(3) {
		submitted["r"] = rows("u", "v", "rl", "rg")
	}
	if g.chance(3) {
		group := map[string]any{"y": "gy"}
		if g.chance(2) {
			group["gl"] = rows("q", "tp", "tg")
		}
		submitted["g"] = []any{group, "not a group"}[g.rng.IntN(2)]
	}
	if g.chance(4) {
		submitted["sl"] = rows("q")
	}
	if g.chance(4) {
		submitted["s"] = map[string]any{"w": "sw"}
	}
	if g.chance(3) {
		submitted["m"] = []any{"x", "${{ inputs.a }}", g.pick("${{ vars.S }}", "8", "nope")}
	}
	if g.chance(5) {
		submitted["dd"] = []any{1.0, "1", 2.0, "x", ""}[g.rng.IntN(5)]
	}
	if g.chance(4) {
		submitted["pw"] = []any{"hunter2", 5.0, map[string]any{}}[g.rng.IntN(3)]
	}
	if g.chance(5) {
		submitted["unknown"] = 1.0
	}
	return b.String(), submitted
}
