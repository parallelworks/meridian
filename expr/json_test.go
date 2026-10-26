package expr_test

import (
	jsonv2 "encoding/json/v2"
	"fmt"
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
			data, err := tmpl.MarshalJSON()
			if err != nil {
				t.Fatalf("%s: Marshal: %v", src, err)
			}
			var back expr.Template
			if err := back.UnmarshalJSON(data); err != nil {
				t.Fatalf("%s: Unmarshal %s: %v", src, data, err)
			}
			again, err := back.MarshalJSON()
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
	data, err := tmpl.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want := `["echo ",{"src":" !inputs['my-key'].x[index] || NaN ","tree":{"op":"||","x":{"op":"!","x":{"path":[{"quoted":"my-key"},"x",{"index":true}],"ref":"inputs"}},"y":{"num":"NaN"}}},"!"]`
	if string(data) != want {
		t.Errorf("got  %s\nwant %s", data, want)
	}
	var back expr.Template
	if err := back.UnmarshalJSON(data); err != nil || back.String() != tmpl.String() {
		t.Errorf("decoded %q, %v; want %q", back.String(), err, tmpl.String())
	}

	data, err = mustParse(t, "map(inputs.l, (h, i) => h.name + i)").MarshalJSON()
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
	data, err := mustParse(t, "map(l, x => "+strings.Repeat("!", 198)+"x)").MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var e expr.Expr
	if err := e.UnmarshalJSON(data); err != nil {
		t.Errorf("198 ! in a lambda: %v", err)
	}
	deeper := strings.Replace(string(data), `"body":`, `"body":{"op":"!","x":`, 1)
	deeper = strings.Replace(deeper, `,"lambda"`, `},"lambda"`, 1)
	if err := e.UnmarshalJSON([]byte(deeper)); err == nil || !strings.Contains(err.Error(), "nests more than 200 levels deep") {
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
		{"another lambda's parameter", `{"src":"x","tree":{"call":"map","args":[{"call":"map","args":[{"lit":[]},{"lambda":["y"],"body":{"param":"y","path":[]}}]},{"lambda":["x"],"body":{"param":"y","path":[]}}]}}`, `"y" isn't a parameter of a lambda around it`},
		{"a parameter read before its lambda", `{"src":"x","tree":{"call":"map","args":[{"param":"x","path":[]},{"body":{"param":"x","path":[]},"lambda":["x"]}]}}`, `"x" isn't a parameter of a lambda around it`},
		{"an empty key", `{"src":"x","tree":{"ref":"inputs","path":[],"":1}}`, `a ref node has no ""`},
		{"a key of a node in another case", `{"src":"x","tree":{"Lit":1}}`, "a node has no kind"},
		{"no tree", `{"src":"x"}`, `has no "tree"`},
		{"a tree that is null", `{"src":"x","tree":null}`, "a node has no kind"},
		{"a key twice in a segment", `{"src":"x","tree":{"ref":"inputs","path":[{"quoted":"a","quoted":"b"}]}}`, "duplicate object member name"},
		{"a segment of two keys", `{"src":"x","tree":{"ref":"inputs","path":[{"quoted":"a","bare":"b"}]}}`, "a path segment is a name or an object with one key"},
		{"a segment that is a number", `{"src":"x","tree":{"ref":"inputs","path":[1]}}`, "a path segment is a name or an object with one key"},
		{"a negation that isn't true or false", `{"src":"x","tree":{"op":"in","not":1,"x":{"lit":1},"y":{"lit":2}}}`, "not is true, or left out"},
		{"up that isn't whole", `{"src":"x","tree":{"up":1.0,"path":[]}}`, "goes up at least one level"},
		// What the tree's encoding never holds, which encoding/json read: null
		// where a string, a number, a list or a node is, and a member a node
		// always has left out.
		{"no src", `{"tree":{"lit":1}}`, `has no "src"`},
		{"src that is null", `{"src":null,"tree":{"lit":1}}`, "want a string"},
		{"a word that is null", `{"src":"x","tree":{"word":null}}`, "want a string"},
		{"a concat that is null", `{"src":"x","tree":{"concat":null,"x":{"lit":1}}}`, "want a string"},
		{"a negation that is false", `{"src":"x","tree":{"op":"in","not":false,"x":{"lit":1},"y":{"lit":[]}}}`, "not is true, or left out"},
		{"a negation that is null", `{"src":"x","tree":{"op":"in","not":null,"x":{"lit":1},"y":{"lit":[]}}}`, "not is true, or left out"},
		{"a unary operator negated", `{"src":"x","tree":{"op":"!","not":true,"x":{"lit":1}}}`, "a unary operator isn't negated"},
		{"a reference without a path", `{"src":"x","tree":{"ref":"inputs"}}`, `a node is missing "path"`},
		{"a relative reference without a path", `{"src":"x","tree":{"up":1}}`, `a node is missing "path"`},
		{"a member without a path", `{"src":"x","tree":{"member":{"lit":{}}}}`, `a node is missing "path"`},
		{"a parameter without a path", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":["x"],"body":{"param":"x"}}]}}`, `a node is missing "path"`},
		{"a path that is null", `{"src":"x","tree":{"ref":"inputs","path":null}}`, "a path is a list"},
		{"a segment that is null", `{"src":"x","tree":{"ref":"inputs","path":[null]}}`, "a path segment is a name or an object with one key"},
		{"a quoted segment that is null", `{"src":"x","tree":{"ref":"inputs","path":[{"quoted":null}]}}`, "want a string"},
		{"an index that isn't true", `{"src":"x","tree":{"ref":"inputs","path":[{"index":-1}]}}`, "[index] and .* are written true"},
		{"an each that isn't true", `{"src":"x","tree":{"ref":"inputs","path":[{"each":[]}]}}`, "[index] and .* are written true"},
		{"a call without arguments", `{"src":"x","tree":{"call":"success"}}`, `a node is missing "args"`},
		{"arguments that are null", `{"src":"x","tree":{"call":"success","args":null}}`, "a call's arguments are a list"},
		{"parameters that are null", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":null,"body":{"lit":1}}]}}`, "a lambda has one or two parameters"},
		{"a parameter that is null", `{"src":"x","tree":{"call":"map","args":[{"lit":[]},{"lambda":[null],"body":{"lit":1}}]}}`, "a lambda has one or two parameters"},
		{"up that is null", `{"src":"x","tree":{"up":null,"path":[]}}`, "goes up at least one level"},
		{"up past 32 bits", `{"src":"x","tree":{"up":2147483648,"path":[]}}`, "goes up at least one level"},
		{"a path that isn't a list", `{"src":"x","tree":{"ref":"inputs","path":"x"}}`, "a path is a list"},
		{"an empty slice", `{"src":"x","tree":{"slice":[]}}`, "a slice is [start, end]"},
		{"a slice of one", `{"src":"x","tree":{"slice":[{"lit":1}]}}`, "a slice is [start, end]"},
		{"a slice of three", `{"src":"x","tree":{"slice":[{"lit":1},null,{"lit":2}]}}`, "a slice is [start, end]"},
		{"a literal out of range", `{"src":"x","tree":{"lit":[1e400]}}`, "out of range"},
		{"invalid UTF-8", "{\"src\":\"\xff\",\"tree\":{\"lit\":1}}", "invalid UTF-8"},
		{"a lone surrogate", `{"src":"\ud800","tree":{"lit":1}}`, "surrogate"},
		{"more than one value", `{"src":"x","tree":{"lit":1}} {}`, "more than one JSON value"},
	} {
		var e expr.Expr
		if err := e.UnmarshalJSON([]byte(tc.data)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error with %q", tc.name, err, tc.want)
		}
	}
	for _, data := range []string{`{}`, `[1]`, `[[]]`, `["a",{"src":"x"}]`, `["a"] ["b"]`, `null`, `[null,"a"]`} {
		var tmpl expr.Template
		if err := tmpl.UnmarshalJSON([]byte(data)); err == nil {
			t.Errorf("template %s: decoded", data)
		}
	}
}

// A tree decodes whatever the order of its keys, and a template of no
// parts.
func TestJSONDecodeAccepts(t *testing.T) {
	var e expr.Expr
	if err := e.UnmarshalJSON([]byte(`{"tree":{"lit":{"b":[1,{"c":null}],"a":"\u0041"}},"src":"x"}`)); err != nil {
		t.Error(err)
	}
	var tmpl expr.Template
	if err := tmpl.UnmarshalJSON([]byte(`[]`)); err != nil {
		t.Error(err)
	}
}

// Each kind of node encodes as encoding/json wrote it: keys in sorted order,
// and <, >, &, U+2028 and U+2029 escaped.
func TestJSONEncodesEachKind(t *testing.T) {
	for _, tc := range []struct{ src, want string }{
		{"'it''s <b>' && 'a\u2028\tb'", `{"src":"'it''s \u003cb\u003e' \u0026\u0026 'a\u2028\tb'","tree":{"op":"\u0026\u0026","x":{"lit":"it's \u003cb\u003e"},"y":{"lit":"a\u2028\tb"}}}`},
		{`[1, 1e21, 1e-7, 0.5, -2, {"b": null, "a": [true, "<"]}]`, `{"src":"[1, 1e21, 1e-7, 0.5, -2, {\"b\": null, \"a\": [true, \"\u003c\"]}]","tree":{"lit":[1,1e+21,1e-7,0.5,-2,{"a":[true,"\u003c"],"b":null}]}}`},
		{`NaN || Infinity || -Infinity`, `{"src":"NaN || Infinity || -Infinity","tree":{"op":"||","x":{"op":"||","x":{"num":"NaN"},"y":{"num":"Infinity"}},"y":{"op":"-","x":{"num":"Infinity"}}}}`},
		{`debug`, `{"src":"debug","tree":{"word":"debug"}}`},
		{`inputs['my-key'].x[index].*[hidden]`, `{"src":"inputs['my-key'].x[index].*[hidden]","tree":{"path":[{"quoted":"my-key"},"x",{"index":true},{"each":true},{"bare":"hidden"}],"ref":"inputs"}}`},
		{`..x.y`, `{"src":"..x.y","tree":{"path":["x","y"],"up":2}}`},
		{`a !in b`, `{"src":"a !in b","tree":{"not":true,"op":"in","x":{"word":"a"},"y":{"word":"b"}}}`},
		{`length inputs.x`, `{"src":"length inputs.x","tree":{"op":"length","x":{"path":["x"],"ref":"inputs"}}}`},
		{`a ? b : c`, `{"src":"a ? b : c","tree":{"else":{"word":"c"},"if":{"word":"a"},"then":{"word":"b"}}}`},
		{`(fromJSON inputs.j).a[0]`, `{"src":"(fromJSON inputs.j).a[0]","tree":{"member":{"op":"fromJSON","x":{"path":["j"],"ref":"inputs"}},"path":["a","0"]}}`},
		{`inputs.x get (1):(2)`, `{"src":"inputs.x get (1):(2)","tree":{"op":"get","x":{"path":["x"],"ref":"inputs"},"y":{"slice":[{"lit":1},{"lit":2}]}}}`},
		{`inputs.x get (1):`, `{"src":"inputs.x get (1):","tree":{"op":"get","x":{"path":["x"],"ref":"inputs"},"y":{"slice":[{"lit":1},null]}}}`},
		{`2:(len inputs.x)`, `{"src":"2:(len inputs.x)","tree":{"concat":"2:","x":{"op":"len","x":{"path":["x"],"ref":"inputs"}}}}`},
		{`filter(inputs.j, v => v > 1).*`, `{"src":"filter(inputs.j, v =\u003e v \u003e 1).*","tree":{"member":{"args":[{"path":["j"],"ref":"inputs"},{"body":{"op":"\u003e","x":{"param":"v","path":[]},"y":{"lit":1}},"lambda":["v"]}],"call":"filter"},"path":[{"each":true}]}}`},
		{`case(a, 1, 2)`, `{"src":"case(a, 1, 2)","tree":{"args":[{"word":"a"},{"lit":1},{"lit":2}],"call":"case"}}`},
	} {
		data, err := mustParse(t, tc.src).MarshalJSON()
		if err != nil || string(data) != tc.want {
			t.Errorf("%s: got  %s, %v\nwant %s", tc.src, data, err, tc.want)
		}
		var back expr.Expr
		if err := back.UnmarshalJSON([]byte(tc.want)); err != nil {
			t.Errorf("%s: %v", tc.src, err)
		}
		if again, err := back.MarshalJSON(); err != nil || string(again) != tc.want {
			t.Errorf("%s: decoded and encoded as %s, %v", tc.src, again, err)
		}
	}
}

// chain returns an expression that nests depth levels deep, the most being
// 200: a chain of additions, each a level under the one after it.
func chain(depth int) string { return strings.Repeat("1+", depth) + "1" }

func BenchmarkMarshal(b *testing.B) {
	for _, depth := range []int{10, 200} {
		e := mustParse(b, chain(depth))
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := jsonv2.Marshal(e); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for _, depth := range []int{10, 200} {
		data, err := jsonv2.Marshal(mustParse(b, chain(depth)))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				var e expr.Expr
				if err := jsonv2.Unmarshal(data, &e); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
