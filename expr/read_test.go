package expr_test

import (
	"fmt"
	"testing"

	"github.com/parallelworks/meridian/expr"
)

// An evaluation reports reading each of a scope's two sets of marks apart:
// a plain variable is private and not marked, and a secret is both. What it
// reports of Marked is what EvalMarked reports, its value and error are
// Eval's, and it counts what Eval counts. A group that evaluates many
// templates, which indexes both sets, finds the same.
func TestEvalRead(t *testing.T) {
	contexts := map[string]any{
		"vars":   map[string]any{"token": "s3cret", "region": "us-east"},
		"inputs": map[string]any{"hosts": []any{map[string]any{"name": "a", "key": "k"}}, "plain": "p"},
	}
	scope := expr.Scope{
		Contexts: contexts,
		Marked:   expr.NewMarks([]string{"vars", "token"}, []string{"inputs", "hosts", "0", "key"}),
		Private:  expr.NewMarks([]string{"vars"}, []string{"inputs", "hosts", "0", "key"}, []string{"inputs", "hosts", "0", "name"}),
	}
	many := scope.Budgeted()
	for range 100 {
		if _, err := mustParse(t, "inputs.plain").Eval(many); err != nil {
			t.Fatal(err)
		}
	}
	for src, want := range map[string]expr.Read{
		"${{ vars.token }}":                         {Marked: true, Private: true},
		"${{ vars.region }}":                        {Private: true},
		"${{ vars.missing }}":                       {Private: true},
		"${{ vars.region }}-${{ vars.token }}":      {Marked: true, Private: true},
		"${{ inputs.hosts[0].name }}":               {Private: true},
		"${{ inputs.hosts[0].key }}":                {Marked: true, Private: true},
		"${{ inputs.hosts.*.name }}":                {Private: true},
		"${{ inputs.hosts }}":                       {Marked: true, Private: true},
		"${{ inputs.plain }}":                       {},
		"${{ inputs.plain == 'p' && vars.region }}": {Private: true},
		"${{ false && vars.token }}":                {},
		"${{ fromJSON(vars.token) }}":               {Marked: true, Private: true},
		"text":                                      {},
	} {
		tmpl, err := expr.ParseTemplate(src)
		if err != nil {
			t.Fatal(err)
		}
		plain := expr.Scope{Contexts: contexts}.Budgeted()
		v, vErr := tmpl.Eval(plain)
		marked := scope.Budgeted()
		got, read, err := tmpl.EvalRead(marked)
		if read != want || fmt.Sprint(got, err) != fmt.Sprint(v, vErr) {
			t.Errorf("%s: read %+v, %v, %v; want %+v, and %v, %v as Eval gives", src, read, got, err, want, v, vErr)
		}
		b1, w1, s1 := expr.Counted(plain)
		b2, w2, s2 := expr.Counted(marked)
		if b1 != b2 || w1 != w2 || s1 != s2 {
			t.Errorf("%s counted %d built, %d work, %d steps; want %d, %d, %d as Eval does", src, b2, w2, s2, b1, w1, s1)
		}
		if _, m, _ := tmpl.EvalMarked(scope); m != want.Marked {
			t.Errorf("%s: EvalMarked gives %v; want %v", src, m, want.Marked)
		}
		if _, read, _ := tmpl.EvalRead(many); read != want {
			t.Errorf("%s in a group that evaluates many: read %+v, want %+v", src, read, want)
		}
	}
}

// What an evaluation reads through the workflow around it is what the
// strings it reads read, both marks apart, whether they were evaluated in it
// or in an earlier evaluation of the same Evaluator.
func TestEvalReadThroughWorkflow(t *testing.T) {
	w := &expr.Workflow{
		Root: map[string]any{
			"w": map[string]any{
				"secret": "${{ vars.token }}", "private": "${{ vars.region }}", "plain": "${{ 1 }}",
				"both": []any{"${{ ..private }}", "${{ ..secret }}"},
			},
		},
	}
	scope := expr.Scope{
		Contexts: map[string]any{"vars": map[string]any{"token": "t", "region": "r"}},
		Marked:   expr.NewMarks([]string{"vars", "token"}),
		Private:  expr.NewMarks([]string{"vars"}),
	}
	ev := w.Evaluator(scope)
	for _, tc := range []struct {
		at   string
		want expr.Read
	}{
		{"secret", expr.Read{Marked: true, Private: true}},
		{"private", expr.Read{Private: true}},
		{"plain", expr.Read{}},
		{"both", expr.Read{Marked: true, Private: true}},
		{"private", expr.Read{Private: true}},
		{"secret", expr.Read{Marked: true, Private: true}},
		{"plain", expr.Read{}},
	} {
		loc := []string{"w", tc.at}
		if _, read, err := ev.EvalRead(loc); err != nil || read != tc.want {
			t.Errorf("%s: read %+v, %v; want %+v", tc.at, read, err, tc.want)
		}
		if _, read, err := w.Evaluator(scope).EvalRead(loc); err != nil || read != tc.want {
			t.Errorf("%s in an evaluator of its own: read %+v, %v; want %+v", tc.at, read, err, tc.want)
		}
		if _, marked, err := w.Evaluator(scope).EvalMarked(loc); err != nil || marked != tc.want.Marked {
			t.Errorf("%s: EvalMarked gives %v, %v; want %v", tc.at, marked, err, tc.want.Marked)
		}
	}
}

// No operator or function drops a mark: what is made of a secret, or
// decided by one, reads both marks.
func TestEvalReadThroughOperators(t *testing.T) {
	scope := expr.Scope{
		Contexts: map[string]any{
			"vars":   map[string]any{"token": "s3cret", "json": `{"a":"s3cret","b":[1,2]}`, "key": "a", "region": "eu"},
			"inputs": map[string]any{"m": map[string]any{"a": "x", "b": "y"}, "l": []any{"p", "q"}, "hosts": []any{map[string]any{"n": "h", "pw": "s3cret"}}},
		},
		Marked:  expr.NewMarks([]string{"vars", "token"}, []string{"vars", "json"}, []string{"vars", "key"}, []string{"inputs", "hosts", "0", "pw"}),
		Private: expr.NewMarks([]string{"vars"}, []string{"inputs", "hosts", "0", "pw"}),
	}
	for _, src := range []string{
		"${{ format('{0}!', vars.token) }}",
		"${{ join(fromJSON(vars.json).b, ',') }}",
		"${{ toJSON(vars.*) }}",
		"${{ fromJSON(vars.json).a }}",
		"${{ contains(vars.token, 's') }}",
		"${{ 'a' + vars.token }}",
		"${{ vars.token || 'x' }}",
		"${{ 'x' && vars.token }}",
		"${{ inputs.hosts.*.pw }}",
		"${{ inputs.hosts[0] }}",
		"${{ inputs.hosts }}",
		"${{ vars.token == 'x' }}",
		"${{ vars.token != '' ? 'set' : 'unset' }}",
		"${{ map(inputs.hosts, h => h.n) }}",
		"${{ filter(inputs.hosts, h => h.pw == 'x') }}",
		"${{ length(vars.token) }}",
		"${{ startsWith(vars.token, 's') }}",
		"${{ var.token }}",
		"${{ fromJSON(vars.json)['a'] }}",
		"${{ vars.*.length }}",
	} {
		tmpl, err := expr.ParseTemplate(src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if v, read, err := tmpl.EvalRead(scope.Budgeted()); !read.Marked || !read.Private {
			t.Errorf("%s = %v, %v: read %+v", src, v, err, read)
		}
	}
}
