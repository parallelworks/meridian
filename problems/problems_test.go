package problems

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/parallelworks/foundation/problem"
	"github.com/parallelworks/foundation/problem/problemtest"
)

// languages are the languages Meridian's messages and pages are in: those
// foundation's own catalogs have.
var languages = []string{"en", "es", "ja", "ko", "zh"}

func TestCatalog(t *testing.T) {
	problemtest.CheckGoCatalog(t, Catalog, Registry, CommandRegistry)
	if got := Catalog.Languages(); !slices.Equal(got, languages) {
		t.Errorf("the catalog is in %v, want %v", got, languages)
	}
	for _, lang := range Catalog.Languages() {
		msgs := Catalog.Messages(lang)
		for code, msg := range msgs {
			if _, ok := Lookup(code); !ok {
				t.Errorf("%s catalog has a message for %s, which isn't a kind", lang, code)
			}
			// Each language quotes a name with its own quotation marks,
			// so English's straight quotes around a param mean a
			// translation was missed.
			if lang != "en" && (strings.Contains(msg, `"{`) || strings.Contains(msg, `}"`)) {
				t.Errorf("%s catalog's %s quotes a param in English quotation marks: %q", lang, code, msg)
			}
		}
		// The localizer falls back to English for a code a language
		// lacks, so a missing translation renders without an error.
		for _, typ := range slices.Concat(Registry.Types(), CommandRegistry.Types()) {
			if _, ok := msgs[typ.Code]; !ok {
				t.Errorf("%s catalog has no message for %s", lang, typ.Code)
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
	for _, dir := range []string{"../format", "../compile", "../expr", "../plan", "../cmd/meridian", "../cmd/meridian/internal/local"} {
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

// TestDocs checks that every kind's page has its text in every language.
// English takes its title from the kind; a translation has its own, and as
// many steps to fix the problem as the English page.
func TestDocs(t *testing.T) {
	read := func(lang string) map[problem.Code]problem.Doc {
		data, err := files.ReadFile("docs/" + lang + ".json")
		if err != nil {
			t.Fatal(err)
		}
		var docs map[problem.Code]problem.Doc
		if err := jsonv2.Unmarshal(data, &docs); err != nil {
			t.Fatal(err)
		}
		return docs
	}
	english := read("en")
	for _, lang := range languages {
		docs := read(lang)
		for _, k := range Kinds() {
			_, workflow := Registry.Lookup(k.Code())
			_, command := CommandRegistry.Lookup(k.Code())
			if !workflow && !command {
				continue // foundation documents its rules
			}
			d, ok := docs[k.Code()]
			switch {
			case !ok || d.Why == "" || len(d.Fix) == 0:
				t.Errorf("docs/%s.json has no why and fix for %s", lang, k.Code())
			case lang != "en" && d.Title == "":
				t.Errorf("docs/%s.json has no title for %s", lang, k.Code())
			case len(d.Fix) != len(english[k.Code()].Fix):
				t.Errorf("docs/%s.json has %d steps to fix %s, and docs/en.json has %d", lang, len(d.Fix), k.Code(), len(english[k.Code()].Fix))
			}
			delete(docs, k.Code())
		}
		for code := range docs {
			t.Errorf("docs/%s.json has %s, which isn't a kind", lang, code)
		}
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
			branches[name] = []any{"true", "false"}
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

// TestParamsAreStringsAndNumbers checks that a diagnostic's params are what
// foundation's clients keep: strings and numbers. A boolean would be dropped,
// and the select it switches would take its other branch.
func TestParamsAreStringsAndNumbers(t *testing.T) {
	for _, k := range Kinds() {
		for _, params := range sampleParams(k) {
			params["suggested"] = true // as a caller might pass it
			d := k.At("w.yaml", Position{1, 2}, "/jobs", params)
			for name, v := range d.Params {
				switch v.(type) {
				case string, int, int64, float64:
				default:
					t.Errorf("%s's param %s is %T, not a string or a number", k.Code(), name, v)
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
			Params{"count": 2, "type1": "object", "type2": "array", "type3": "", "has_got": "true", "got": "string"},
			"Must be a mapping or a list, not a string.",
		},
		{
			WrongType,
			Params{"count": 1, "type1": "boolean", "type2": "", "type3": "", "has_got": "false", "got": ""},
			"Must be true or false.",
		},
		{
			UnknownKey,
			Params{"key": "nmea", "suggested": "true", "suggestion": "name"},
			`The key "nmea" isn't known here, so it's ignored. Did you mean "name"?`,
		},
		{UnknownKey, Params{"key": "x", "suggested": "false", "suggestion": ""}, `The key "x" isn't known here, so it's ignored.`},
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
			Params{"name": "rundir", "suggested": "true", "suggestion": "run_dir"},
			`There's no input "rundir". Did you mean "run_dir"?`,
		},
		{FileTooLarge, Params{"size": 102401, "limit": 102400}, "The file is 102,401 bytes, more than the limit of 102,400."},
	} {
		if got := tc.kind.Message(tc.params); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.kind.Code(), got, tc.want)
		}
	}
}

// TestLocalize pins a few messages in each language word for word: the
// quotation marks each puts around a name, a suggestion, a plural, the type
// names wrong_type selects, and English for a language the catalog lacks.
func TestLocalize(t *testing.T) {
	for _, tc := range []struct {
		kind   *Kind
		params Params
		want   map[string]string
	}{
		{
			UndefinedInput,
			Params{"name": "rundir", "suggested": "true", "suggestion": "run_dir"},
			map[string]string{
				"es": "No hay ninguna entrada “rundir”. ¿Quisiste decir “run_dir”?",
				"ja": "入力「rundir」はありません。「run_dir」の間違いではありませんか？",
				"ko": "‘rundir’ 입력이 없습니다. ‘run_dir’을(를) 의도하셨나요?",
				"zh": "没有输入“rundir”。您是不是想用“run_dir”？",
				"fr": `There's no input "rundir". Did you mean "run_dir"?`,
			},
		},
		{
			WrongType,
			Params{"count": 2, "type1": "object", "type2": "array", "type3": "", "has_got": "true", "got": "string"},
			map[string]string{
				"es": "Debe ser un mapa o una lista, no una cadena.",
				"ja": "文字列ではなく、マッピングかリストにしてください。",
				"ko": "문자열이 아니라 매핑 또는 목록이어야 합니다.",
				"zh": "必须是映射或列表，而不是字符串。",
			},
		},
		{
			TooFewKeys,
			Params{"min": 1, "count": 0},
			map[string]string{
				"es": "Necesita al menos 1 clave. Tiene 0.",
				"ja": "キーが 1 個以上必要ですが、0 個しかありません。",
				"ko": "키가 1개 이상 필요하지만 0개만 있습니다.",
				"zh": "至少需要 1 个键，但只有 0 个。",
			},
		},
		{
			WrongArgumentCount,
			Params{"name": "contains", "arity": "exact", "min": 2, "max": 2, "count": 1},
			map[string]string{
				"es": "La función contains() recibe 2 argumentos, pero se le pasó 1.", //nolint:misspell // Spanish, not a misspelling of arguments
				"ja": "関数 contains() の引数は 2 個ですが、1 個指定されています。",
				"ko": "contains() 함수는 인수를 2개 받지만 1개가 지정되었습니다.",
				"zh": "函数 contains() 接受 2 个参数，但传入了 1 个。",
			},
		},
		{
			UnclosedTemplate,
			nil,
			map[string]string{
				"es": "El ${{ no tiene un }} que lo cierre. Escribe ${{ '${{' }} para el texto ${{.",
				"ja": "${{ を閉じる }} がありません。テキストとしての ${{ は ${{ '${{' }} と書いてください。",
				"ko": "${{ 기호를 닫는 }} 기호가 없습니다. 텍스트로 ${{ 기호를 쓰려면 ${{ '${{' }} 형태로 쓰세요.",
				"zh": "${{ 没有以 }} 结束。要表示文本 ${{，请写成 ${{ '${{' }}。",
			},
		},
	} {
		d := tc.kind.At("w.yaml", Position{1, 1}, "/a", tc.params)
		for lang, want := range tc.want {
			if got := d.Localize(lang); got != want {
				t.Errorf("%s in %s: got %q, want %q", tc.kind.Code(), lang, got, want)
			}
		}
	}
}

func TestDiagnostic(t *testing.T) {
	d := UnknownKey.At("w.yaml", Position{3, 5}, "/jobs/a b/x~1y", Params{"key": "x/y", "suggested": "false", "suggestion": ""})
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
	data, err := jsonv2.Marshal(d)
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
		{map[string]any{"b": []any(nil), "a": 1e21}, `{"a":1e+21,"b":null}`},
		{"line\u2028end", `"line\u2028end"`},
		{"bad \xff byte", "\"bad \ufffd byte\""},
		{math.NaN(), "NaN"},
		{strings.Repeat("x", 50), `"` + strings.Repeat("x", 38) + "…"},
	} {
		if got := Show(tc.v); got != tc.want {
			t.Errorf("Show(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

// showWhole is how Show showed a value before it wrote only what it shows:
// its whole text as fmt prints it, and its whole JSON, if it has any.
func showWhole(v any) string {
	s := fmt.Sprint(v)
	if b, err := jsonv2.Marshal(v, shown); err == nil {
		s = string(b)
	}
	if utf8.RuneCountInString(s) > maxShown {
		r := []rune(s)
		s = string(r[:maxShown-1]) + "…"
	}
	return s
}

// Show shows what it showed when it wrote values whole: random values of
// all kinds and sizes, the characters JSON escapes, and values that aren't
// JSON, whose text fmt prints.
func TestShowAsWhole(t *testing.T) {
	deep := func(n int) any {
		var v any = "x"
		for range n {
			v = []any{v}
		}
		return v
	}
	values := []any{
		nil, true, "", 0, int64(-1) << 62, 1.5, math.Copysign(0, -1), 1e21, 1e-7, math.MaxFloat64,
		math.NaN(), math.Inf(1), float32(1.5), complex(1, 2), []any(nil), map[string]any(nil),
		[]string{"a"},
		map[string]string{"a": "b"},
		struct{ A int }{1},
		make(chan int),
		[]any{math.NaN()},
		map[string]any{"a": math.Inf(-1)},
		deep(maxPlainDepth - 1), deep(maxPlainDepth), deep(maxPlainDepth + 1), deep(10001),
		strings.Repeat("é", 50), strings.Repeat("😀", 50), strings.Repeat("\xff", 50), strings.Repeat("\xe2\x80\xa8", 50),
		append(slices.Repeat([]any{"item"}, 100), math.NaN()),
	}
	pieces := []string{"a", "é", "😀", "\xff", "\xe2\x80", "\xe2\x80\xa8", "\xe2\x80\xa9", "<", "&", `"`, `\`, "\n", "\x00", "\x1f", "\x7f", " "}
	r := rand.New(rand.NewPCG(5, 6))
	text := func() string {
		var b strings.Builder
		for range r.IntN(30) {
			b.WriteString(pieces[r.IntN(len(pieces))])
		}
		return b.String()
	}
	var random func(depth int) any
	random = func(depth int) any {
		switch k := r.IntN(10); {
		case k == 0:
			return nil
		case k == 1:
			return r.IntN(2) == 0
		case k == 2:
			return []float64{0, 1e21, 1e20, -2.5e-7, 123456789, 0.1, math.NaN(), math.Inf(1)}[r.IntN(8)]
		case k == 3:
			return r.IntN(1000) - 500
		case k <= 6 || depth > 3:
			return text()
		case k <= 8:
			l := make([]any, r.IntN(12))
			for i := range l {
				l[i] = random(depth + 1)
			}
			return l
		}
		m := map[string]any{}
		for range r.IntN(8) {
			m[text()] = random(depth + 1)
		}
		return m
	}
	for range 20000 {
		values = append(values, random(0))
	}
	for _, v := range values {
		if got, want := Show(v), showWhole(v); got != want {
			t.Fatalf("Show(%v) = %q, want %q", v, got, want)
		}
	}
}

// Show writes no more of a value than it shows.
func BenchmarkShow(b *testing.B) {
	l := slices.Repeat([]any{"item"}, 1_000_000)
	b.ReportAllocs()
	for b.Loop() {
		Show(l)
	}
}

// TestControlCharactersEscaped checks that a message shows a param's
// control characters escaped, so that a key in a workflow can't move a
// terminal's cursor or start a line that reads as another diagnostic,
// while the params themselves keep what the workflow has.
func TestControlCharactersEscaped(t *testing.T) {
	key := "a\x1b[2J\a\x7f\u0085b\nsrc/main.go:1:1: error: injected"
	params := Params{"key": key, "suggested": "false", "suggestion": ""}
	want := `The key "a\x1b[2J\a\x7f\u0085b\nsrc/main.go:1:1: error: injected" isn't known here, so it's ignored.`
	if got := UnknownKey.Message(params); got != want {
		t.Errorf("Message = %q, want %q", got, want)
	}
	d := UnknownKey.At("w\n.yaml", Position{1, 2}, "/a", params)
	if d.Params["key"] != key {
		t.Errorf("the param is %q, not the key as it is", d.Params["key"])
	}
	for _, lang := range languages {
		if got := d.Localize(lang); strings.ContainsFunc(got, isControl) {
			t.Errorf("%s message %q has a control character", lang, got)
		}
	}
	if got := d.String(); strings.ContainsFunc(got, isControl) || !strings.HasPrefix(got, `w\n.yaml:1:2: warning: `) {
		t.Errorf("String = %q", got)
	}
	for in, want := range map[string]string{"plain": "plain", "tab\there": `tab\there`, "\r\n": `\r\n`, "é and 日本": "é and 日本"} {
		if got := EscapeControl(in); got != want {
			t.Errorf("EscapeControl(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKindProblem(t *testing.T) {
	p := MemberInvalid.Problem(Params{"group": "gpu", "key": "cpu", "value": "lots", ParamFile: "groups.yaml", ParamLine: 4})
	if p.Code != "member_invalid" || p.Type != "/problems/command/member_invalid" || p.Status != 422 {
		t.Errorf("Problem = %+v", p)
	}
	if want := `A member of the group "gpu" has cpu: lots, which isn't valid.`; p.Detail != want {
		t.Errorf("Detail = %q, want %q", p.Detail, want)
	}
	if p.Params[ParamLine] != 4 || p.Params[ParamFile] != "groups.yaml" {
		t.Errorf("Params = %v", p.Params)
	}
	if p := ExecutionGroupUnknown.Problem(Params{"value": "cpu", "groups": `"local"`}); p.Type != "/problems/workflow/execution_group_unknown" {
		t.Errorf("a workflow kind's problem has type %s", p.Type)
	}
}
