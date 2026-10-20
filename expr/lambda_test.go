package expr_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// Each mistake in writing a lambda is a syntax error of its own kind, at
// what is wrong.
func TestLambdaSyntaxErrors(t *testing.T) {
	for _, tc := range []struct {
		src    string
		kind   *problems.Kind
		offset int
		params problems.Params
	}{
		{"map(inputs.l, inputs.l)", problems.LambdaExpected, 14, problems.Params{"name": "map"}},
		{"filter(inputs.l, (a b) => a)", problems.LambdaExpected, 17, problems.Params{"name": "filter"}},
		{"map(inputs.l, () => 1)", problems.LambdaExpected, 14, nil},
		{"map(inputs.l, (x)", problems.LambdaExpected, 14, nil},
		{"map(inputs.l, )", problems.LambdaExpected, 14, nil},
		{"map(inputs.l, (a, b, c) => a)", problems.LambdaTooManyParameters, 14, problems.Params{"name": "map", "count": 3}},
		{"map(inputs.l, (x, inputs) => x)", problems.LambdaParameterReserved, 18, problems.Params{"name": "inputs"}},
		{"map(inputs.l,  len => 1)", problems.LambdaParameterReserved, 15, problems.Params{"name": "len"}},
		{"map(inputs.l, (x, x) => x)", problems.LambdaParameterTwice, 18, problems.Params{"name": "x"}},
		{"x => 1", problems.LambdaMisplaced, 2, nil},
		{"=> 1", problems.LambdaMisplaced, 0, nil},
		{"map(x => x, inputs.l)", problems.LambdaMisplaced, 6, nil},
		{"contains(inputs.l, x => x)", problems.LambdaMisplaced, 21, nil},
		{"map(inputs.l, x => y => 1)", problems.LambdaMisplaced, 21, nil},
		{"map(inputs.l)", problems.WrongArgumentCount, 0, problems.Params{"name": "map", "count": 1}},
		{"map(inputs.l, x => x, 1)", problems.WrongArgumentCount, 0, problems.Params{"count": 3}},
		{"Map(inputs.l, x => x)", problems.NotAFunction, 0, nil},
	} {
		_, err := expr.Parse(tc.src)
		se, ok := errors.AsType[*expr.SyntaxError](err)
		if !ok || se.Kind != tc.kind || se.Offset != tc.offset {
			t.Errorf("%s: %#v, want %s at %d", tc.src, err, tc.kind.Code(), tc.offset)
			continue
		}
		for k, v := range tc.params {
			if se.Params[k] != v {
				t.Errorf("%s: params %v, want %s %v", tc.src, se.Params, k, v)
			}
		}
	}
}

// A parameter can't be named after anything a word already means: each
// context, keyword and word operator, NaN, Infinity and index, with the
// keywords, and, or, NaN and Infinity in any case. Anything else can name
// one, such as a context's name in another case or a function's name.
func TestLambdaParameterNames(t *testing.T) {
	reserved := []string{
		"inputs", "needs", "steps", "matrix", "env", "job", "org", "vars", "var", "remote", "sessions",
		"true", "TRUE", "false", "False", "null", "Null", "undefined", "UNDEFINED",
		"and", "AND", "or", "Or", "in", "includes", "startsWith", "endsWith", "get", "range", "mod", "exp", "split", "join",
		"length", "len", "upper", "lower", "trim", "keys", "values", "abs", "floor", "ceil", "round", "fromJSON",
		"NaN", "nan", "NAN", "Infinity", "infinity", "INFINITY", "index",
	}
	for _, name := range reserved {
		_, err := expr.Parse("map(inputs.l, " + name + " => 1)")
		if se, ok := errors.AsType[*expr.SyntaxError](err); !ok || se.Kind != problems.LambdaParameterReserved {
			t.Errorf("%s: %v, want %s", name, err, problems.LambdaParameterReserved.Code())
		}
	}
	scope := inputs(map[string]any{"l": []any{"a"}})
	for _, name := range []string{"x", "_", "_x1", "item", "Inputs", "JOB", "Index", "Len", "IN", "inf", "format", "map", "filter", "success", "always"} {
		got, err := mustParse(t, "map(inputs.l, "+name+" => "+name+")").Eval(scope)
		if err != nil || !equal(got, []any{"a"}) {
			t.Errorf("%s: %v, %v; want [a]", name, got, err)
		}
	}
}

// The messages of lambdas' problems say what is wrong in words.
func TestLambdaMessages(t *testing.T) {
	for src, want := range map[string]string{
		"map(inputs.l, inputs.l)":         "The function map() takes a lambda such as x => x + 1 as its last argument.",
		"map(inputs.l, (a, b, c) => a)":   "A lambda of map() takes an item and an optional index, not 3 parameters.",
		"map(inputs.l, true => 1)":        `A lambda's parameter can't be named "true", which is reserved for a context, a keyword, an operator or a number, or for [index].`,
		"filter(inputs.l, (x, x) => 1)":   `A lambda's two parameters are both named "x".`,
		"x => 1":                          "A lambda such as x => x + 1 can only be the last argument of map() or filter().",
		"map(inputs.l, (x, x) => x)[0].y": `A lambda's two parameters are both named "x".`,
	} {
		if _, err := expr.Parse(src); err == nil || err.Error() != want {
			t.Errorf("%s: %v, want %q", src, err, want)
		}
	}
	_, err := mustParse(t, "filter(inputs.n, x => x)").Eval(inputs(map[string]any{"n": 3.0}))
	ee, ok := errors.AsType[*expr.EvalError](err)
	if !ok || ee.Kind != problems.NotListOrMapping || ee.Offset != 0 || err.Error() != "The function filter() takes a list or a mapping, not 3." {
		t.Errorf("filter of a number: %#v", err)
	}
	_, err = mustParse(t, "1 + length(map('text', x => x))").Eval(expr.Scope{})
	if ee, ok := errors.AsType[*expr.EvalError](err); !ok || ee.Kind != problems.NotListOrMapping || ee.Offset != 11 || ee.Params["value"] != `"text"` {
		t.Errorf("map of text: %#v", err)
	}
}

// A parameter reads the item, and the index or key, of the innermost lambda
// that has it; the item's path reads it as a reference does. Elsewhere its
// name is a word.
func TestLambdaParameters(t *testing.T) {
	scope := expr.Scope{Contexts: map[string]any{
		"inputs": map[string]any{
			"hosts": []any{
				map[string]any{"name": "a", "tags": []any{"x", "y"}, "my-key": 1.0},
				map[string]any{"name": "b", "tags": []any{}, "my-key": 2.0},
			},
			"ports": map[string]any{"web": 80.0, "db": 5432.0},
			"lists": []any{[]any{1.0, 2.0}, []any{3.0}},
			"json":  `[{"a": 1}, {"a": 2}]`,
		},
	}, Index: []int{1}}
	for src, want := range map[string]any{
		"map(inputs.hosts, h => h.name)":                        []any{"a", "b"},
		"map(inputs.hosts, h => h['my-key'])":                   []any{1.0, 2.0},
		"map(inputs.hosts, h => h.tags[0])":                     []any{"x", nil},
		"map(inputs.hosts, h => h.tags.*)":                      []any{[]any{"x", "y"}, []any{}},
		"map(inputs.hosts, h => h.)":                            []any{map[string]any{"name": "a", "tags": []any{"x", "y"}, "my-key": 1.0}, map[string]any{"name": "b", "tags": []any{}, "my-key": 2.0}},
		"map(inputs.hosts, h => h.tags[index])":                 []any{"y", nil},
		"map(inputs.hosts, (h, i) => i)":                        []any{0.0, 1.0},
		"map(inputs.ports, (p, k) => k + p)":                    []any{"db5432", "web80"},
		"map(inputs.json, j => j.a)":                            []any{1.0, 2.0},
		"map(inputs.lists, x => map(x, x => x * 10))":           []any{[]any{10.0, 20.0}, []any{30.0}},
		"map(inputs.lists, (x, i) => map(x, y => i))":           []any{[]any{0.0, 0.0}, []any{1.0}},
		"map(inputs.lists, x => length(map(x, x => 0)) + x[0])": []any{3.0, 4.0},
		"map(inputs.lists, x => !x)":                            []any{false, false},
		"map(inputs.lists, x => x) == x":                        false,
		"map(inputs.lists, x=>x1)":                              []any{"x1", "x1"},
		"map(inputs.lists, x => x$)":                            []any{"x$", "x$"},
		"map(inputs.lists, x => x:1)":                           []any{"x:1", "x:1"},
	} {
		got, err := mustParse(t, src).Eval(scope)
		if err != nil || !equal(got, want) {
			t.Errorf("%s = %s, %v; want %s", src, show(got), err, show(want))
		}
	}
	// A template's expressions are each parsed on their own, and so are the
	// templates a body reads.
	tmpl, err := expr.ParseTemplate("${{ map(inputs.lists, x => x) }} ${{ x }}")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := tmpl.Eval(scope); err != nil || got != "[[1,2],[3]] x" {
		t.Errorf("a template = %q, %v", got, err)
	}
	w := &expr.Workflow{Root: map[string]any{"label": "${{ x }}"}}
	got, err := mustParse(t, "map(inputs.lists, x => .label)").Eval(w.Scope([]string{"y"}, scope))
	if err != nil || !equal(got, []any{"x", "x"}) {
		t.Errorf("a body that reads a template = %s, %v", show(got), err)
	}
}

// A body that reads something not known yet is unknown, unless it never
// runs. An error in a body stops the call, and leaves the lambdas around it
// as they were.
func TestLambdaBodies(t *testing.T) {
	scope := inputs(map[string]any{"l": []any{1.0, 2.0}, "s": `{"a": 1}`})
	_, err := mustParse(t, "1 + length(map(inputs.l, x => needs.a.outputs.b))").Eval(scope)
	if ue, ok := errors.AsType[*expr.UnknownError](err); !ok || ue.Name != "needs" || ue.Offset != 30 {
		t.Errorf("a body that reads needs: %#v", err)
	}
	if got, err := mustParse(t, "map([], x => needs.a.outputs.b)").Eval(scope); err != nil || !equal(got, []any{}) {
		t.Errorf("a body that never runs: %v, %v", got, err)
	}
	if _, err := mustParse(t, "filter(inputs.l, x => success())").Eval(scope); !expr.IsUnknown(err) {
		t.Errorf("a status function in a body: %v", err)
	}
	_, err = mustParse(t, "map(inputs.l, x => map(x, y => y))").Eval(scope)
	if ee, ok := errors.AsType[*expr.EvalError](err); !ok || ee.Kind != problems.NotListOrMapping || ee.Offset != 19 {
		t.Errorf("an error in a body: %#v", err)
	}
	got, err := mustParse(t, "map(inputs.l, x => join(map(inputs.s, (v, x) => x), '') + x)").Eval(scope)
	if err != nil || !equal(got, []any{"a1", "a2"}) {
		t.Errorf("an outer parameter after an inner one hid it: %s, %v", show(got), err)
	}
}

// A body's reads of contexts are the expression's, and a body is checked as
// the rest of the expression is: a word in it that looks like a reference is
// a probable mistake, and a parameter isn't.
func TestLambdaChecks(t *testing.T) {
	e := mustParse(t, "map(inputs.hosts, h => h.name + matrix.os + needs.a.outputs.b + h)")
	var refs []string
	for _, r := range e.Refs() {
		refs = append(refs, fmt.Sprintf("%d %s %s", r.Offset, r.Context, strings.Join(r.Path, "/")))
	}
	if want := []string{"4 inputs hosts", "32 matrix os", "44 needs a/outputs/b"}; !slices.Equal(refs, want) {
		t.Errorf("Refs = %q, want %q", refs, want)
	}
	if !mustParse(t, "map(inputs.l, x => .sep)").Relative() {
		t.Error("Relative: a body's relative reference")
	}
	if used, afterCancel := mustParse(t, "filter(inputs.l, x => !success())").StatusChecks(); !used || !afterCancel {
		t.Errorf("StatusChecks of a body = %v, %v", used, afterCancel)
	}
	for src, want := range map[string][]int{
		"map(inputs.hosts, h => h.name)":           nil,
		"map(inputs.hosts, h => host.name)":        {23},
		"map(inputs.hosts, h => h) == h.name":      {29},
		"map(inputs.hosts, h => inputs.pw-user)":   {23},
		"map(inputs.hosts, h => map(h, x => h.x))": nil,
	} {
		var got []int
		for _, p := range mustParse(t, src).Check() {
			got = append(got, p.Offset)
		}
		if !slices.Equal(got, want) {
			t.Errorf("Check(%s) at %v, want %v", src, got, want)
		}
	}
	// A parameter's path that subtracts a word is NaN, as a reference's is,
	// and the problem suggests brackets. A parameter is no context, so it
	// has no Meant.
	found := mustParse(t, "map(inputs.hosts, h => h.pw-user)").Check()
	if len(found) != 1 || found[0].Kind != problems.HyphenatedName || found[0].Offset != 23 || found[0].Meant != nil ||
		!strings.Contains(found[0].Message(), "To read pw-user, write h['pw-user'].") {
		t.Errorf("a parameter that subtracts a word: %+v", found)
	}
	if found := mustParse(t, "map(inputs.l, x => x-y)").Check(); len(found) != 0 {
		t.Errorf("a parameter without a path that subtracts a word: %+v", found)
	}
}

// A lambda's body nests in its call: the call is one level for the parser,
// and the call and the lambda two of the tree.
func TestLambdaDepth(t *testing.T) {
	for _, tc := range []struct {
		src string
		ok  bool
	}{
		{"map(l, x => " + strings.Repeat("(", 199) + "x" + strings.Repeat(")", 199) + ")", true},
		{"map(l, x => " + strings.Repeat("(", 200) + "x" + strings.Repeat(")", 200) + ")", false},
		{"map(l, x => " + strings.Repeat("!", 198) + "x)", true},
		{"map(l, x => " + strings.Repeat("!", 199) + "x)", false},
		{"map(l, x => map(x, y => " + strings.Repeat("!", 196) + "y))", true},
		{"map(l, x => map(x, y => " + strings.Repeat("!", 197) + "y))", false},
	} {
		_, err := expr.Parse(tc.src)
		se, isSyntax := errors.AsType[*expr.SyntaxError](err)
		if tc.ok && err != nil || !tc.ok && (!isSyntax || se.Kind != problems.ExpressionTooDeep) {
			t.Errorf("%.40s…: %v, want ok %v", tc.src, err, tc.ok)
		}
	}
}
