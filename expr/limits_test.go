package expr_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// templateLimit is the language's limit on what one template builds, and
// groupLimit Meridian's on what the templates of a group build together.
const (
	templateLimit = 8 << 20
	groupLimit    = 64 << 20
)

func xs(n int) string { return strings.Repeat("x", n) }

func tooLarge(err error) bool {
	ee, ok := errors.AsType[*expr.EvalError](err)
	return ok && ee.Kind == problems.ExpressionTooLarge
}

func overBudget(err error) bool {
	_, ok := errors.AsType[*expr.BudgetError](err)
	return ok
}

func inputs(values map[string]any) expr.Scope {
	return expr.Scope{Contexts: map[string]any{"inputs": values}}
}

// Each operation that builds a value counts the value's size against its
// template's limit, and nothing else counts. Each case's inputs make its
// template build n bytes: it passes at the limit, and fails a byte over.
func TestBuildLimit(t *testing.T) {
	keys := map[string]any{}
	for i := range 1000 {
		keys[strconv.Itoa(i)] = xs(1000)
	}
	for _, tc := range []struct {
		name     string
		template string
		// inputs returns the inputs with which the template builds n bytes.
		inputs func(n int) map[string]any
	}{
		{
			// Each value's text counts, and the text around it doesn't.
			"template text", "(${{ inputs.a }}, ${{ inputs.b }})",
			func(n int) map[string]any { return map[string]any{"a": xs(n / 2), "b": xs(n - n/2)} },
		},
		{
			"+", "${{ inputs.a + inputs.b }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n - 1), "b": "y"} },
		},
		{
			"+ with a number", "${{ inputs.a + 1 }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n - 1)} },
		},
		{
			// The string, once: two a's, a -, and b.
			"format", "${{ format('{0}{0}-{1}', inputs.a, inputs.b) }}",
			func(n int) map[string]any { return map[string]any{"a": xs(n / 4), "b": xs(n - 2*(n/4) - 1)} },
		},
		{
			"join", "${{ join(inputs.l, '-') }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n / 2), xs(n - n/2 - 1)}} },
		},
		{
			"the join operator", "${{ inputs.l join '-' }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n / 2), xs(n - n/2 - 1)}} },
		},
		{
			// The string's bytes, and 8 for each of its two parts.
			"split", "${{ inputs.s split ',' }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n-17) + ","} },
		},
		{
			// A part for each character: a of one byte and b of two count
			// 9a + 10b.
			"split by ''", "${{ inputs.s split '' }}",
			func(n int) map[string]any {
				b := n % 9
				return map[string]any{"s": xs((n-10*b)/9) + strings.Repeat("é", b)}
			},
		},
		{
			// An empty string splits into no parts by ''.
			"split of an empty string by ''", "${{ length(inputs.s + '') + length('' split '') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n)} },
		},
		{
			// An empty string splits into one part by anything else.
			"split of an empty string", "${{ length(inputs.s + '') + length('' split ',') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 8)} },
		},
		{
			// Indented: [, a line break, two spaces, the string in quotes, a
			// line break and ].
			"toJSON", "${{ toJSON(inputs.l) }}",
			func(n int) map[string]any { return map[string]any{"l": []any{xs(n - 8)}} },
		},
		{
			// A list of one string: 8 for the item, and the string's bytes.
			"fromJSON", "${{ fromJSON(inputs.j) }}",
			func(n int) map[string]any { return map[string]any{"j": `["` + xs(n-8) + `"]`} },
		},
		{
			"upper", "${{ upper inputs.s }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n)} },
		},
		{
			"lower", "${{ lower inputs.s }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n)} },
		},
		{
			// 16 for each number.
			"range", "${{ length(range 10000) + length(inputs.s + '') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 16*10000)} },
		},
		{
			"the range operator", "${{ length(5 range 10005) + length(inputs.s + '') }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 16*10000)} },
		},
		{
			// The slice operand 0:(end) builds 0: and end's text.
			"slice operand", "${{ 'abc' get 0:(inputs.s) }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n - 2)} },
		},
		{
			"keys, values and trim", "${{ length(inputs.s + '') + length(keys inputs.m) + length(values inputs.m) + length(trim inputs.t) }}",
			func(n int) map[string]any { return map[string]any{"s": xs(n), "m": keys, "t": " " + xs(1<<20) + " "} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := expr.ParseTemplate(tc.template)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tmpl.Eval(inputs(tc.inputs(templateLimit))); err != nil {
				t.Errorf("at the limit: %v", err)
			}
			if _, err := tmpl.Eval(inputs(tc.inputs(templateLimit + 1))); !tooLarge(err) {
				t.Errorf("a byte over the limit: %v, want %s", err, problems.ExpressionTooLarge.Code())
			}
		})
	}
}

// join and split count what they build before building it, so a long
// separator fails without being repeated.
func TestBuildLimitCountsFirst(t *testing.T) {
	scope := inputs(map[string]any{"s": xs(1 << 20)})
	for _, src := range []string{"(range 10000) join inputs.s", "join(range(10000), inputs.s)", "(inputs.s + inputs.s) split ''"} {
		if _, err := mustParse(t, src).Eval(scope); !tooLarge(err) {
			t.Errorf("%s: %v, want %s", src, err, problems.ExpressionTooLarge.Code())
		}
	}
}

// A template doesn't count what a template it reads builds, such as an
// input's default or a key beside it: each has its own count.
func TestReadTemplatesCountOnTheirOwn(t *testing.T) {
	w := &expr.Workflow{
		Root: map[string]any{
			"on": map[string]any{"execute": map[string]any{"inputs": map[string]any{
				"a": map[string]any{"type": "string", "default": "${{ inputs.s + inputs.s }}"},
			}}},
			"b": "${{ inputs.s + inputs.s }}",
		},
		Definitions: []string{"on", "execute", "inputs"},
	}
	scope := inputs(map[string]any{"s": xs(5 << 19)})
	// Each template read builds 5 MiB, and each reader 7.5 MiB.
	for _, src := range []string{"inputs.a.default + inputs.s", ".b + inputs.s"} {
		got, err := mustParse(t, src).Eval(w.Scope([]string{"c"}, scope))
		if s, _ := got.(string); err != nil || len(s) != 15<<19 {
			t.Errorf("%s: %d bytes, %v; want 7.5 MiB", src, len(s), err)
		}
	}
}

// The templates of a group, which share a budget, build at most 64 MiB
// together, even though each is within the language's limit.
func TestGroupBudget(t *testing.T) {
	root := map[string]any{"byte": "${{ 'x' + '' }}"}
	reads := make([]string, 8)
	for i := range reads {
		root[fmt.Sprintf("v%d", i)] = "${{ inputs.s + inputs.s }}"
		reads[i] = fmt.Sprintf("length(.v%d)", i)
	}
	w := &expr.Workflow{Root: root}
	scope := inputs(map[string]any{"s": xs(templateLimit / 2)})
	// Eight templates that each build 8 MiB, the most one can, build the
	// group's 64 MiB.
	all := strings.Join(reads, " + ")
	got, err := mustParse(t, all).Eval(w.Scope([]string{"x"}, scope))
	if err != nil || got != float64(groupLimit) {
		t.Errorf("eight templates of 8 MiB: %v, %v; want %d", got, err, groupLimit)
	}
	_, err = mustParse(t, all+" + length(.byte)").Eval(w.Scope([]string{"x"}, scope))
	if !overBudget(err) || !strings.Contains(err.Error(), "64 MiB") {
		t.Errorf("a byte more: %v, want %s", err, problems.ExpressionsBuildTooMuch.Code())
	}
	// A scope without a budget makes each evaluation a group of its own.
	for range 9 {
		if _, err := mustParse(t, "inputs.s + inputs.s").Eval(scope); err != nil {
			t.Fatalf("a template on its own: %v", err)
		}
	}
}
