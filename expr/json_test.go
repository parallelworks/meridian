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

	data, err = json.Marshal(mustParse(t, "map(inputs.l, (h, i) => h.name + i)"))
	if err != nil {
		t.Fatal(err)
	}
	want = `{"src":"map(inputs.l, (h, i) =\u003e h.name + i)","tree":{"args":[{"path":["l"],"ref":"inputs"},` +
		`{"body":{"op":"+","x":{"param":"h","path":["name"]},"y":{"param":"i","path":[]}},"lambda":["h","i"]}],"call":"map"}}`
	if string(data) != want {
		t.Errorf("got  %s\nwant %s", data, want)
	}
}

// A lambda's body counts toward a decoded tree's depth as it does a parsed
// one's: the call and the lambda are two levels.
func TestJSONLambdaDepth(t *testing.T) {
	data, err := json.Marshal(mustParse(t, "map(l, x => "+strings.Repeat("!", 198)+"x)"))
	if err != nil {
		t.Fatal(err)
	}
	var e expr.Expr
	if err := json.Unmarshal(data, &e); err != nil {
		t.Errorf("198 ! in a lambda: %v", err)
	}
	deeper := strings.Replace(string(data), `"body":`, `"body":{"op":"!","x":`, 1)
	deeper = strings.Replace(deeper, `,"lambda"`, `},"lambda"`, 1)
	if err := json.Unmarshal([]byte(deeper), &e); err == nil || !strings.Contains(err.Error(), "nests more than 200 levels deep") {
		t.Errorf("199: %v, want the depth limit", err)
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
		{"finite num", `{"src":"x","tree":{"num":"1"}}`, `"1" isn't NaN, Infinity or -Infinity`},
		{"num in another case", `{"src":"x","tree":{"num":"nan"}}`, `"nan" isn't NaN, Infinity or -Infinity`},
		{"zero up", `{"src":"x","tree":{"up":0,"path":["x"]}}`, "goes up at least one level"},
		{"bad segment", `{"src":"x","tree":{"ref":"inputs","path":[{"what":1}]}}`, "unknown path segment"},
		{"unknown key", `{"src":"x","tree":{"lit":1},"extra":1}`, `unknown field "extra"`},
		{"a key in another case", `{"SRC":"x","tree":{"lit":1}}`, `unknown field "SRC"`},
		{"a key twice", `{"src":"x","src":"y","tree":{"lit":1}}`, "duplicate object member name"},
		{"a node of two kinds", `{"src":"x","tree":{"lit":1,"word":"x"}}`, "a node is both lit and word"},
		{"a node's unknown key", `{"src":"x","tree":{"ref":"inputs","path":[],"later":1}}`, `a ref node has no "later"`},
		{"a node's key twice", `{"src":"x","tree":{"lit":1,"lit":2}}`, "duplicate object member name"},
		{"too deep", `{"src":"x","tree":` + strings.Repeat(`{"op":"!","x":`, 300) + `{"lit":1}` + strings.Repeat(`}`, 300) + `}`, "nests more than 200 levels deep"},
		{"a lambda alone", `{"src":"x","tree":{"lambda":["x"],"body":{"lit":1}}}`, "a lambda is the last argument of map() and filter(), and nothing else"},
		{"a lambda first", `{"src":"x","tree":{"call":"map","args":[{"lambda":["x"],"body":{"lit":1}},{"lambda":["x"],"body":{"lit":1}}]}}`, "and nothing else"},
		{"a lambda in contains", `{"src":"x","tree":{"call":"contains","args":[{"lit":[]},{"lambda":["x"],"body":{"lit":1}}]}}`, "and nothing else"},
		{"map without a lambda", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lit":1}]}}`, "and nothing else"},
		{"a lambda in an operator", `{"src":"x","tree":{"op":"!","x":{"lambda":["x"],"body":{"lit":1}}}}`, "and nothing else"},
		{"no parameters", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":[],"body":{"lit":1}}]}}`, "a lambda has one or two parameters"},
		{"three parameters", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":["a","b","c"],"body":{"lit":1}}]}}`, "a lambda has one or two parameters"},
		{"parameters that aren't a list", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":"x","body":{"lit":1}}]}}`, "a lambda has one or two parameters"},
		{"a parameter that isn't a name", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":["1x"],"body":{"lit":1}}]}}`, `"1x" isn't a parameter's name`},
		{"a reserved parameter", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":["Index","NAN"],"body":{"lit":1}}]}}`, `can't be named "NAN"`},
		{"a parameter twice", `{"src":"x","tree":{"call":"filter","args":[{"lit":[]},{"lambda":["x","x"],"body":{"lit":1}}]}}`, `both named "x"`},
		{"a lambda without a body", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":["x"]}]}}`, `a node is missing "body"`},
		{"a lambda's unknown key", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":["x"],"body":{"lit":1},"x":{"lit":1}}]}}`, `a lambda node has no "x"`},
		{"a parameter outside a lambda", `{"src":"x","tree":{"param":"x","path":[]}}`, `"x" isn't a parameter of a lambda around it`},
		{"another lambda's parameter", `{"src":"x","tree":{"call":"map","args":[{"call":"map","args":[{"lit":[]},{"lambda":["y"],"body":{"param":"y"}}]},{"lambda":["x"],"body":{"param":"y"}}]}}`, `"y" isn't a parameter of a lambda around it`},
	} {
		var e expr.Expr
		if err := json.Unmarshal([]byte(tc.data), &e); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error with %q", tc.name, err, tc.want)
		}
	}
}
