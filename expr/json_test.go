package expr_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
)

// Every expression in the conformance suite evaluates the same after a trip
// through JSON.
func TestJSONRoundTrip(t *testing.T) {
	files, err := conformance.ExpressionFiles()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range files {
		for _, c := range f.Cases {
			src := c.Template
			if c.Expr != "" {
				src = "${{" + c.Expr + "}}"
			}
			tmpl, err := expr.ParseTemplate(src)
			if src == "" || err != nil {
				continue
			}
			data, err := json.Marshal(tmpl)
			if err != nil {
				t.Fatalf("%s: Marshal: %v", src, err)
			}
			var back expr.Template
			if err := json.Unmarshal(data, &back); err != nil {
				t.Fatalf("%s: Unmarshal %s: %v", src, data, err)
			}
			again, err := json.Marshal(&back)
			if err != nil || string(again) != string(data) {
				t.Fatalf("%s: encodes as %s, then %s (%v)", src, data, again, err)
			}
			s := f.ScopeFor(&c)
			scope := expr.Scope{Contexts: s.Contexts, Index: s.Index}
			if s.Status != nil {
				scope.Status = &expr.Status{Failed: s.Status.Failed, Cancelled: s.Status.Cancelled}
			}
			if f.Workflow != nil {
				w := &expr.Workflow{Root: f.Workflow, Definitions: []string{"on", "execute", "inputs"}}
				scope = w.Scope(nil, scope)
			}
			want, wantErr := tmpl.Eval(scope)
			got, gotErr := back.Eval(scope)
			if (wantErr == nil) != (gotErr == nil) || wantErr == nil && !equal(got, want) {
				t.Errorf("%s: after JSON %s, %v; want %s, %v", src, show(got), gotErr, show(want), wantErr)
			}
			n++
		}
	}
	if n < 400 {
		t.Errorf("round-tripped only %d expressions", n)
	}
}

func TestJSONEncoding(t *testing.T) {
	tmpl, err := expr.ParseTemplate("echo ${{ !inputs['my-key'].x[index] || NaN }}!")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(tmpl)
	if err != nil {
		t.Fatal(err)
	}
	want := `["echo ",{"src":" !inputs['my-key'].x[index] || NaN ","tree":{"op":"||","x":{"op":"!","x":{"path":[{"quoted":"my-key"},"x",{"index":true}],"ref":"inputs"}},"y":{"num":"NaN"}}},"!"]`
	if string(data) != want {
		t.Errorf("got  %s\nwant %s", data, want)
	}
	var back expr.Template
	if err := json.Unmarshal(data, &back); err != nil || back.String() != tmpl.String() {
		t.Errorf("decoded %q, %v; want %q", back.String(), err, tmpl.String())
	}
}

func TestJSONDecodeErrors(t *testing.T) {
	for _, tc := range []struct{ name, data, want string }{
		{"not an object", `{"src":"x","tree":1}`, "a node must be an object"},
		{"no kind", `{"src":"x","tree":{}}`, "a node has no kind"},
		{"unknown context", `{"src":"x","tree":{"ref":"secrets","path":[]}}`, `"secrets" isn't a context`},
		{"var spelled var", `{"src":"x","tree":{"ref":"var","path":[]}}`, `"var" isn't a context`},
		{"unknown function", `{"src":"x","tree":{"call":"hashFiles","args":[]}}`, "hashFiles isn't a function"},
		{"wrong arity", `{"src":"x","tree":{"call":"contains","args":[{"lit":1}]}}`, "contains() can't take 1 arguments"},
		{"unknown binary operator", `{"src":"x","tree":{"op":"=","x":{"lit":1},"y":{"lit":2}}}`, `"=" isn't a binary operator`},
		{"unknown unary operator", `{"src":"x","tree":{"op":"+","x":{"lit":1}}}`, `"+" isn't a unary operator`},
		{"negated ==", `{"src":"x","tree":{"op":"==","not":true,"x":{"lit":1},"y":{"lit":2}}}`, `"==" can't be negated`},
		{"finite num", `{"src":"x","tree":{"num":"1"}}`, `"1" isn't NaN or an infinity`},
		{"zero up", `{"src":"x","tree":{"up":0,"path":["x"]}}`, "goes up at least one level"},
		{"bad segment", `{"src":"x","tree":{"ref":"inputs","path":[{"what":1}]}}`, "unknown path segment"},
		{"unknown key", `{"src":"x","tree":{"lit":1},"extra":1}`, `unknown field "extra"`},
		{"a key in another case", `{"SRC":"x","tree":{"lit":1}}`, `unknown field "SRC"`},
		{"a key twice", `{"src":"x","src":"y","tree":{"lit":1}}`, "duplicate object member name"},
		{"a node of two kinds", `{"src":"x","tree":{"lit":1,"word":"x"}}`, "a node is both lit and word"},
		{"a node's unknown key", `{"src":"x","tree":{"ref":"inputs","path":[],"later":1}}`, `a ref node has no "later"`},
		{"a node's key twice", `{"src":"x","tree":{"lit":1,"lit":2}}`, "duplicate object member name"},
		{"too deep", `{"src":"x","tree":` + strings.Repeat(`{"op":"!","x":`, 300) + `{"lit":1}` + strings.Repeat(`}`, 300) + `}`, "nests more than 200 levels deep"},
	} {
		var e expr.Expr
		if err := json.Unmarshal([]byte(tc.data), &e); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error with %q", tc.name, err, tc.want)
		}
	}
}
