package problems

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/parallelworks/foundation/problem"
	"github.com/parallelworks/foundation/problem/problemtest"
)

func TestCatalog(t *testing.T) {
	problemtest.CheckGoCatalog(t, Catalog, Registry)
	for _, lang := range Catalog.Languages() {
		for code := range Catalog.Messages(lang) {
			if _, ok := Lookup(code); !ok {
				t.Errorf("%s catalog has a message for %s, which isn't a kind", lang, code)
			}
		}
	}
}

// TestEveryKindIsReported checks that Meridian reports each kind somewhere:
// that the code outside this package, tests aside, names it. A kind no code
// reports is a message no one sees.
func TestEveryKindIsReported(t *testing.T) {
	fset := token.NewFileSet()
	kindsFile, err := parser.ParseFile(fset, "kinds.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defined := map[string]bool{}
	ast.Inspect(kindsFile, func(n ast.Node) bool {
		if spec, ok := n.(*ast.ValueSpec); ok {
			for _, name := range spec.Names {
				defined[name.Name] = false
			}
		}
		return true
	})
	if len(defined) != len(Kinds()) {
		t.Fatalf("kinds.go names %d kinds, and Kinds() returns %d", len(defined), len(Kinds()))
	}
	for _, dir := range []string{"../format", "../compile", "../expr", "../plan"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "problems" {
						if _, ok := defined[sel.Sel.Name]; ok {
							defined[sel.Sel.Name] = true
						}
					}
				}
				return true
			})
		}
	}
	for _, name := range slices.Sorted(maps.Keys(defined)) {
		if !defined[name] {
			t.Errorf("nothing reports problems.%s", name)
		}
	}
}

func TestDocs(t *testing.T) {
	data, err := files.ReadFile("docs/en.json")
	if err != nil {
		t.Fatal(err)
	}
	var docs map[problem.Code]problem.Doc
	if err := json.Unmarshal(data, &docs); err != nil {
		t.Fatal(err)
	}
	for _, k := range Kinds() {
		if _, ours := Registry.Lookup(k.Code()); !ours {
			continue // foundation documents its rules
		}
		d, ok := docs[k.Code()]
		if !ok || d.Why == "" || len(d.Fix) == 0 {
			t.Errorf("docs/en.json has no why and fix for %s", k.Code())
		}
		delete(docs, k.Code())
	}
	for code := range docs {
		t.Errorf("docs/en.json has %s, which isn't a kind", code)
	}
}

// sampleParams returns every set of params that takes each branch of a
// kind's message once.
func sampleParams(k *Kind) []Params {
	base := Params{}
	branches := map[string][]any{}
	for _, name := range k.Type().Params {
		switch name {
		case "suggested", "has_got", "exclusive":
			branches[name] = []any{true, false}
		case "count":
			branches[name] = []any{1, 2, 3}
		case "what":
			branches[name] = []any{"number", "text", "key", "go_type"}
		case "arity":
			branches[name] = []any{"none", "exact", "at_least", "range"}
		case "defines":
			branches[name] = []any{"input", "job", "step", "matrix", "session", "none"}
		case "phase":
			branches[name] = []any{"run_created", "strategy", "job_ready", "job_env", "other"}
		case "type1", "type2", "type3", "got":
			branches[name] = []any{"string", "number", "integer", "boolean", "object", "array", "null"}
		case "min", "max", "size", "limit", "length":
			base[name] = 2
		default:
			base[name] = "<" + name + ">"
		}
	}
	out := []Params{base}
	for _, name := range slices.Sorted(maps.Keys(branches)) {
		var next []Params
		for _, p := range out {
			for _, v := range branches[name] {
				q := maps.Clone(p)
				q[name] = v
				next = append(next, q)
			}
		}
		out = next
	}
	return out
}

func TestEveryMessageRenders(t *testing.T) {
	for _, k := range Kinds() {
		for _, params := range sampleParams(k) {
			for _, lang := range Catalog.Languages() {
				msg, ok := localizer.Message(lang, k.Code(), params)
				if !ok {
					t.Errorf("%s %s doesn't render with %v", lang, k.Code(), params)
					continue
				}
				if strings.ContainsAny(strings.ReplaceAll(msg, "${{", ""), "{}") && !strings.Contains(string(k.Code()), "format_") &&
					!strings.Contains(msg, "${{") {
					t.Errorf("%s %s renders %q, with a brace left in it", lang, k.Code(), msg)
				}
				// Messages are sentences, as foundation's are, since they
				// show beside them.
				if r, _ := utf8.DecodeRuneInString(msg); lang == "en" && (!unicode.IsUpper(r) || !strings.HasSuffix(msg, ".") && !strings.HasSuffix(msg, "?")) {
					t.Errorf("%s %s renders %q, which isn't a sentence", lang, k.Code(), msg)
				}
			}
		}
	}
}

func TestMessages(t *testing.T) {
	for _, tc := range []struct {
		kind   *Kind
		params Params
		want   string
	}{
		{UnclosedTemplate, nil, "The ${{ has no }} to close it. Write ${{ '${{' }} for the text ${{."},
		{FormatUnclosedBrace, Params{"template": `"{0"`}, `The format() template "{0" has a '{' with no '}'.`},
		{FormatUnopenedBrace, Params{"template": `"0}"`}, `The format() template "0}" has a '}' with no '{'.`},
		{NotExpression, Params{"value": `"x"`}, `Must be a ${{ }} expression, not "x".`},
		{
			WrongType,
			Params{"count": 2, "type1": "object", "type2": "array", "type3": "", "has_got": true, "got": "string"},
			"Must be a mapping or a list, not a string.",
		},
		{
			WrongType,
			Params{"count": 1, "type1": "boolean", "type2": "", "type3": "", "has_got": false, "got": ""},
			"Must be true or false.",
		},
		{
			UnknownKey,
			Params{"key": "nmea", "suggested": true, "suggestion": "name"},
			`The key "nmea" isn't known here, so it's ignored. Did you mean "name"?`,
		},
		{UnknownKey, Params{"key": "x", "suggested": false, "suggestion": ""}, `The key "x" isn't known here, so it's ignored.`},
		{
			WrongArgumentCount,
			Params{"name": "contains", "arity": "exact", "min": 2, "max": 2, "count": 1},
			"The function contains() takes 2 arguments, not 1.",
		},
		{
			WrongArgumentCount,
			Params{"name": "toJSON", "arity": "exact", "min": 1, "max": 1, "count": 3},
			"The function toJSON() takes 1 argument, not 3.",
		},
		{
			ContextNotKnown,
			Params{"context": "steps", "phase": "job_ready"},
			"The context steps isn't known yet: this is evaluated before the job's steps run.",
		},
		{TernaryMissingColon, Params{"at": "4"}, "The '?' at offset 4 has no ':' after it."},
		{CallMissingComma, Params{"name": "contains"}, "The call to contains() needs a ',' between its arguments, or a ')' after them."},
		{ExpectedCloseParen, nil, "A '(' has no ')' to close it."},
		{
			StatusNotKnown,
			Params{"phase": "run_created"},
			"Status functions such as failure() aren't known yet: this is evaluated when the run is created, before any job runs.",
		},
		{ContextNotKnown, Params{"context": "matrix", "phase": "other"}, "The context matrix isn't known here."},
		{NeedsCycle, Params{"jobs": QuoteAll([]string{"a", "b"})}, `The jobs "a", "b" need each other.`},
		{
			UndefinedInput,
			Params{"name": "rundir", "suggested": true, "suggestion": "run_dir"},
			`There's no input "rundir". Did you mean "run_dir"?`,
		},
		{FileTooLarge, Params{"size": 102401, "limit": 102400}, "The file is 102,401 bytes, more than the limit of 102,400."},
	} {
		if got := tc.kind.Message(tc.params); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.kind.Code(), got, tc.want)
		}
	}
}

func TestDiagnostic(t *testing.T) {
	d := UnknownKey.At("w.yaml", Position{3, 5}, "/jobs/a b/x~1y", Params{"key": "x/y", "suggested": false, "suggestion": ""})
	if d.Pointer != "#/jobs/a%20b/x~1y" || d.Path() != "/jobs/a b/x~1y" {
		t.Errorf("Pointer %q, Path %q", d.Pointer, d.Path())
	}
	if d.File() != "w.yaml" || d.Position() != (Position{3, 5}) || d.Severity() != Warning {
		t.Errorf("File %q, Position %v, Severity %v", d.File(), d.Position(), d.Severity())
	}
	if want := `w.yaml:3:5: warning: The key "x/y" isn't known here, so it's ignored. (unknown_key)`; d.String() != want {
		t.Errorf("String() = %q, want %q", d.String(), want)
	}
	if d.Type != "/problems/workflow/unknown_key" {
		t.Errorf("Type = %q", d.Type)
	}

	// A plan keeps the code and params, without the detail, and decodes
	// with encoding/json/v2.
	d.Detail = ""
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var back Diagnostic
	if err := jsonv2.Unmarshal(data, &back, jsonv2.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("%s: %v", data, err)
	}
	if back.Message() != `The key "x/y" isn't known here, so it's ignored.` || back.Position() != (Position{3, 5}) || back.File() != "w.yaml" {
		t.Errorf("decoded %s: %q at %v in %q", data, back.Message(), back.Position(), back.File())
	}
}

func TestSort(t *testing.T) {
	a := YAMLAlias.At("w.yaml", Position{2, 1}, "/a", nil)
	b := YAMLAlias.At("w.yaml", Position{1, 1}, "/b", nil)
	ds := Diagnostics{a, b, a}
	ds.Sort()
	if len(ds) != 2 || ds[0].Path() != "/b" || ds[1].Path() != "/a" {
		t.Errorf("sorted %v", ds)
	}
}

func TestShow(t *testing.T) {
	for _, tc := range []struct {
		v    any
		want string
	}{
		{nil, "null"},
		{"a<b", `"a<b"`},
		{map[string]any{"a": 1}, `{"a":1}`},
		{[]any{1, "x"}, `[1,"x"]`},
		{strings.Repeat("x", 50), `"` + strings.Repeat("x", 38) + "…"},
	} {
		if got := Show(tc.v); got != tc.want {
			t.Errorf("Show(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
}
