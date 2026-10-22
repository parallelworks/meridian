package expr_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
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

// EvalParts reports each expression's value and what it read, apart, and
// gives what EvalRead gives.
func TestEvalParts(t *testing.T) {
	contexts := map[string]any{"vars": map[string]any{"creds": `{"token":"tok_abc","user":"svc"}`, "region": "us-east"}}
	scope := expr.Scope{
		Contexts: contexts,
		Marked:   expr.NewMarks([]string{"vars", "creds"}),
		Private:  expr.NewMarks([]string{"vars"}),
	}
	for src, want := range map[string]string{
		"deploy --token ${{ fromJSON(vars.creds).token }} in ${{ vars.region }} ${{ 1 }}": `tok_abc {true true}; us-east {false true}; 1 {false false}; `,
		"${{ fromJSON(vars.creds).user }}":                                                `svc {true true}; `,
		"${{ vars.region }}-${{ fromJSON(vars.creds) }}":                                  `us-east {false true}; map[token:tok_abc user:svc] {true true}; `,
		"text": ``,
		"${{ vars.region }} ${{ fromJSON('x') }}": `us-east {false true}; `,
	} {
		tmpl, err := expr.ParseTemplate(src)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		v, read, err := tmpl.EvalParts(scope, func(v any, read expr.Read) {
			got += fmt.Sprintf("%v {%v %v}; ", v, read.Marked, read.Private)
		})
		wv, wread, werr := tmpl.EvalRead(scope)
		if got != want || fmt.Sprint(v, read, err) != fmt.Sprint(wv, wread, werr) {
			t.Errorf("%s: parts %q, want %q; %v, %+v, %v, and EvalRead %v, %+v, %v", src, got, want, v, read, err, wv, wread, werr)
		}
	}
}

// EvalParts gives what EvalRead gives for random templates, its value, its
// error, what it read and what it counted, and its expressions' reads
// together are the template's.
func TestEvalPartsRandom(t *testing.T) {
	contexts := map[string]any{
		"vars":   map[string]any{"creds": `{"token":"tok_abc","n":5,"l":["a","b"]}`, "region": "us-east", "bad": "x"},
		"inputs": map[string]any{"s": "plain", "l": []any{"p", "q"}, "m": map[string]any{"k": "v"}},
	}
	scope := expr.Scope{
		Contexts: contexts,
		Marked:   expr.NewMarks([]string{"vars", "creds"}),
		Private:  expr.NewMarks([]string{"vars"}),
	}
	exprs := []string{
		"fromJSON(vars.creds).token", "vars.region", "inputs.s", "1 + 2", "fromJSON(vars.bad)",
		"fromJSON(vars.creds)", "inputs.l", "format('{0}-{1}', inputs.s, vars.region)",
		"vars.creds == '' ? 'no' : 'yes'", "fromJSON(vars.creds).n", "join(fromJSON(vars.creds).l, ',')",
		"inputs.m", "'${{ not }}'", "null", "toJSON(inputs.l)",
	}
	texts := []string{"", " ", "x ", " y", "--a=", "${", "}}", "$ {{", "\\"}
	r := rand.New(rand.NewPCG(8, 9))
	for range 5000 {
		var b strings.Builder
		for range 1 + r.IntN(4) {
			b.WriteString(texts[r.IntN(len(texts))])
			if r.IntN(4) > 0 {
				b.WriteString("${{ " + exprs[r.IntN(len(exprs))] + " }}")
			}
		}
		src := b.String()
		tmpl, err := expr.ParseTemplate(src)
		if err != nil {
			continue
		}
		var all expr.Read
		var parts []string
		s1 := scope.Budgeted()
		v, read, err := tmpl.EvalParts(s1, func(v any, r expr.Read) {
			all = all.Or(r)
			parts = append(parts, fmt.Sprint(v))
		})
		s2 := scope.Budgeted()
		wv, wread, werr := tmpl.EvalRead(s2)
		if fmt.Sprint(v, read, err) != fmt.Sprint(wv, wread, werr) {
			t.Fatalf("%q: %v %+v %v, EvalRead %v %+v %v", src, v, read, err, wv, wread, werr)
		}
		b1, w1, st1 := expr.Counted(s1)
		b2, w2, st2 := expr.Counted(s2)
		if b1 != b2 || w1 != w2 || st1 != st2 {
			t.Fatalf("%q: counted %d %d %d, EvalRead %d %d %d", src, b1, w1, st1, b2, w2, st2)
		}
		if err == nil && all != read {
			t.Fatalf("%q: parts read %+v, the whole %+v", src, all, read)
		}
	}
}

// A scope's Text hook makes a reference to a path its context doesn't
// have read as its own text, as format 1 reads a word it doesn't know.
func TestScopeText(t *testing.T) {
	contexts := map[string]any{"steps": map[string]any{"a": map[string]any{"outputs": map[string]any{"x": "1"}}}}
	text := func(context string, path []string) bool {
		return context == "steps" && len(path) >= 2 && path[1] != "outputs"
	}
	for src, want := range map[string]any{
		"steps.a.result":          "steps.a.result",
		"steps['a'].result":       "steps['a'].result",
		"steps.b.conclusion.more": "steps.b.conclusion.more",
		"steps.a.outputs.x":       "1",
		"steps.a.outputs.y":       "",
	} {
		v, err := mustParse(t, src).Eval(expr.Scope{Contexts: contexts, Text: text})
		if err != nil || v != want {
			t.Errorf("%s = %#v, %v, want %#v", src, v, err, want)
		}
	}
	if v, err := mustParse(t, "steps.a.result").Eval(expr.Scope{Contexts: contexts}); err != nil || v != "" {
		t.Errorf("without Text: %#v, %v", v, err)
	}
}
