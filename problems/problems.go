// Package problems defines the problems Meridian reports, and the
// diagnostics that report them.
//
// Each Kind of problem has a stable code, a severity, and the params its
// message shows. Its message is in Catalog, one ICU MessageFormat message per
// language, and its documentation page's text is in Docs. A Diagnostic is
// foundation's problem.FieldError, so a platform shows it, translates it and
// links it to its page like any other problem: the code's type URI is
// /problems/workflow/<code>.
//
// The meridian command's own problems, with its directories, its config,
// its key and its arguments, are kinds too, in CommandRegistry, whose type
// URIs are /problems/command/<code>. A resolver returns a kind's Problem for
// what is wrong, so that its caller reports it with its code and params.
// So are the worker API's own, which a coordinator answers a worker's
// request with, in WorkerRegistry, whose type URIs are
// /problems/worker/<code>.
//
// A message shows a param's control characters escaped, as \n or \x1b,
// rather than as themselves: a key in a workflow could otherwise move a
// terminal's cursor, or start a line that reads as another diagnostic.
package problems

import (
	"cmp"
	"embed"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/jsonshape"
)

// Severity says whether a diagnostic stops a workflow from running.
type Severity int

const (
	// Error stops the workflow from loading or compiling.
	Error Severity = iota
	// Warning reports something the workflow probably doesn't mean, such
	// as a key the format ignores. The workflow still runs.
	Warning
)

func (s Severity) String() string {
	switch s {
	case Error:
		return "error"
	case Warning:
		return "warning"
	default:
		return fmt.Sprintf("Severity(%d)", int(s))
	}
}

// A Position is a place in a workflow file. Line and Column start at 1, and
// Column counts characters (Unicode code points). The zero Position means
// the whole file.
type Position struct {
	Line   int `json:"line,omitempty"`
	Column int `json:"column,omitempty"`
}

// String formats p as line:column, as line alone when the column is unknown,
// or as "" for the whole file.
func (p Position) String() string {
	switch {
	case p.Line == 0:
		return ""
	case p.Column == 0:
		return strconv.Itoa(p.Line)
	default:
		return fmt.Sprintf("%d:%d", p.Line, p.Column)
	}
}

// Compare orders positions by line, then column.
func (p Position) Compare(q Position) int {
	return cmp.Or(cmp.Compare(p.Line, q.Line), cmp.Compare(p.Column, q.Column))
}

// Params are the values a diagnostic's message shows, by name.
type Params = map[string]any

// maxShown is how many characters of a value a message shows.
const maxShown = 40

// QuoteAll shows a list of names or keys in a message: each in neutral
// double quotes, joined with ", ". A message quotes a single name itself,
// with its language's quotation marks, but ICU can't quote each item of a
// list, so a list's items carry quotes that read the same in every
// language.
func QuoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = `"` + n + `"`
	}
	return List(q)
}

// WithSuggestion sets params' suggested and suggestion: whether ok, as
// "true" or "false", that name was probably meant, and the name. It returns
// params.
func WithSuggestion(params Params, name string, ok bool) Params {
	params["suggested"] = strconv.FormatBool(ok)
	params["suggestion"] = ""
	if ok {
		params["suggestion"] = name
	}
	return params
}

// WrongTypeParams returns the params of a WrongType problem: of a value
// that must have one of the types want, at most three, and has the type
// got, each by its name in a schema, such as string; or got "" for a value
// whose type the problem doesn't say. A type1, type2 or type3 that want
// doesn't fill is "", and has_got says, as "true" or "false", whether got
// does.
func WrongTypeParams(got string, want ...string) Params {
	params := Params{"count": len(want), "type1": "", "type2": "", "type3": "", "has_got": strconv.FormatBool(got != ""), "got": got}
	for i, t := range want {
		params["type"+strconv.Itoa(i+1)] = t
	}
	return params
}

// List joins items a message shows with ", ". The items are data, such as
// names and values, so the list reads the same in every language.
func List(items []string) string { return strings.Join(items, ", ") }

// Show shows a value from a workflow or a run in a message: as compact JSON,
// cut to 40 characters, or as fmt prints it if it isn't JSON, as NaN isn't.
func Show(v any) string {
	var s string
	if plain(v, 0) {
		// Only the JSON that is shown is written: as much as holds the
		// characters shown and one more, so a list of a million items
		// isn't written whole. A plain value is JSON, and the only error
		// is the writer's once it has what it needs.
		var w shownWriter
		_ = jsonv2.MarshalWrite(&w, v, shown)
		s = string(w)
	} else if b, err := jsonv2.Marshal(v, shown); err == nil {
		s = string(b)
	} else {
		s = fmt.Sprint(v)
	}
	if utf8.RuneCountInString(s) > maxShown {
		r := []rune(s)
		s = string(r[:maxShown-1]) + "…"
	}
	return s
}

// plain reports whether v, depth lists and mappings deep, is made of values
// that are JSON, as a workflow's are: null, booleans, strings, finite
// float64s, ints and int64s, and lists and mappings of them, nested less
// than maxPlainDepth deep. Show writes no more of a plain value than it
// shows, and the rest of it as it always has.
func plain(v any, depth int) bool {
	switch v := v.(type) {
	case nil, bool, string, int, int64:
		return true
	case float64:
		return !math.IsNaN(v) && !math.IsInf(v, 0)
	case []any:
		if depth >= maxPlainDepth {
			return false
		}
		for _, item := range v {
			if !plain(item, depth+1) {
				return false
			}
		}
		return true
	case map[string]any:
		if depth >= maxPlainDepth {
			return false
		}
		for _, item := range v {
			if !plain(item, depth+1) {
				return false
			}
		}
		return true
	}
	return false
}

// maxPlainDepth is how deeply a plain value's lists and mappings nest, well
// within how deeply JSON's encoder nests them.
const maxPlainDepth = 1000

// A shownWriter keeps the start of what is written to it: enough for the
// characters Show shows and one more, each of at most 4 bytes. It fails a
// write past that.
type shownWriter []byte

// errShown is a shownWriter's error once it has what Show needs.
var errShown = errors.New("problems: written as much as is shown")

func (w *shownWriter) Write(p []byte) (int, error) {
	n := min(len(p), 4*(maxShown+1)-len(*w))
	*w = append(*w, p[:n]...)
	if n < len(p) {
		return n, errShown
	}
	return n, nil
}

// Masked is what a param shows in place of a value that came from an
// organization or user variable or a password input, or was made from one:
// a variable can hold a secret, and a password is one, so a message never
// quotes one.
const Masked = "***"

// workflowParams are the params of the problems a value can have that show
// the workflow, such as an input's or a job's name, or Meridian's own
// limits, rather than the value.
var workflowParams = map[string]bool{
	"input": true, "job": true, "key": true, "name": true, "type": true, "what": true,
	"options": true, "min": true, "max": true, "limit": true, "op": true,
}

// MaskValues returns params, those of a problem with a value that read a
// variable or a password, or that holds a password, with what they show of
// the value masked: each param but those that show the workflow, such as
// key, is Masked, and there is no suggestion, which would be near the
// value. A variable can hold a secret, and a password is one, so a
// diagnostic never quotes one.
func MaskValues(params Params) Params {
	out := make(Params, len(params))
	for name, v := range params {
		if !workflowParams[name] {
			v = Masked
		}
		out[name] = v
	}
	if _, ok := params["suggested"]; ok {
		WithSuggestion(out, "", false)
	}
	return out
}

// MaskProblem returns a copy of p, a problem that can show a value that
// read a variable or a password, with what it shows of the value masked:
// its params and its field errors' as MaskValues masks them, its detail
// and theirs the kind's message with those, or its title for a kind this
// release doesn't know, and no instance. Its type, title, status and code
// are kept, which show only what kind of problem it is.
func MaskProblem(p *problem.Problem) *problem.Problem {
	if p == nil {
		return nil
	}
	out := &problem.Problem{Type: p.Type, Title: p.Title, Status: p.Status, Code: p.Code, Detail: p.Title}
	if p.Params != nil {
		out.Params = MaskValues(p.Params)
	}
	if k, ok := Lookup(p.Code); ok {
		out.Detail = k.Message(out.Params)
	}
	for _, fe := range p.Errors {
		masked := maskFieldError(*fe)
		out.Errors = append(out.Errors, &masked)
	}
	return out
}

// MaskDiagnostics returns a copy of ds, diagnostics that can show a value
// that read a variable or a password, each masked as MaskProblem masks a
// problem's field errors: its params as MaskValues masks them, where it is
// among them, and its detail its kind's message with those, or none for a
// kind this release doesn't know. Its code and its pointer, which show
// only what kind of problem it is and where in the workflow, are kept.
func MaskDiagnostics(ds Diagnostics) Diagnostics {
	if ds == nil {
		return nil
	}
	out := make(Diagnostics, len(ds))
	for i, d := range ds {
		out[i] = Diagnostic{maskFieldError(d.FieldError)}
	}
	return out
}

// maskFieldError returns fe with its params masked (MaskValues), and its
// detail its kind's message with them, or none.
func maskFieldError(fe problem.FieldError) problem.FieldError {
	masked := problem.FieldError{Type: fe.Type, Code: fe.Code, Pointer: fe.Pointer, Parameter: fe.Parameter, In: fe.In}
	if fe.Params != nil {
		masked.Params = MaskValues(fe.Params)
	}
	if k, ok := Lookup(fe.Code); ok {
		masked.Detail = k.Message(masked.Params)
	}
	return masked
}

// shown is how Show encodes a value: as encoding/json always has, with
// mappings in the order of their keys, a nil list or mapping as null, U+2028
// and U+2029 escaped and invalid UTF-8 replaced, but without escaping <, >
// and &, which mean nothing special in a message.
var shown = jsonv2.JoinOptions(
	jsonv2.Deterministic(true),
	jsonv2.FormatNilMapAsNull(true),
	jsonv2.FormatNilSliceAsNull(true),
	jsontext.EscapeForJS(true),
	jsontext.AllowInvalidUTF8(true),
	jsontext.AllowDuplicateNames(true),
	jsontext.PreserveRawStrings(true),
)

// The params every diagnostic may have besides its kind's: where it is. No
// kind lists them, and no message shows them.
const (
	ParamFile   = "file"
	ParamLine   = "line"
	ParamColumn = "column"
)

//go:embed messages/*.json docs/*.json
var files embed.FS

var (
	// Registry holds the problem types of the kinds a workflow or a run can
	// have, for a platform's problem pages.
	Registry = problem.NewRegistry("workflow")
	// CommandRegistry holds the problem types of the meridian command's own
	// kinds: its directories, its config, its key and its arguments.
	CommandRegistry = problem.NewRegistry("command")
	// WorkerRegistry holds the problem types of the worker API's own kinds,
	// which a coordinator answers a worker's request with (workerapi).
	WorkerRegistry = problem.NewRegistry("worker")
	// Catalog holds each kind's message, in each language.
	Catalog = problem.MustCatalog(sub("messages"))
	// Docs holds the text of each kind's documentation page, in each
	// language.
	Docs = problem.MustDocs(sub("docs"))

	localizer = &problem.Localizer{Catalogs: []*problem.Catalog{Catalog}}
	kinds     = map[problem.Code]*Kind{}
	// all holds every kind, in the order they are defined.
	all []*Kind
)

func sub(dir string) fs.FS {
	f, err := fs.Sub(files, dir)
	if err != nil {
		panic(err) //nolint:forbidigo // the directory is embedded, so this is a bug in the build
	}
	return f
}

// A Kind is a kind of problem: a code, its severity, and the params its
// message shows.
type Kind struct {
	typ      *problem.Type
	severity Severity
}

// define adds a kind to the registry. Every diagnostic of the kind has each
// of params.
func define(severity Severity, code, title string, params ...string) *Kind {
	return add(Registry.Define(problem.Type{Code: problem.Code(code), Status: 422, Title: title, Params: params}), severity)
}

// defineCommand adds one of the meridian command's own kinds, an error, to
// CommandRegistry. Every problem of the kind has each of params.
func defineCommand(code, title string, params ...string) *Kind {
	return add(CommandRegistry.Define(problem.Type{Code: problem.Code(code), Status: 422, Title: title, Params: params}), Error)
}

// defineWorker adds one of the worker API's own kinds, an error, to
// WorkerRegistry, which a coordinator answers with 409 Conflict. Every
// problem of the kind has each of params.
func defineWorker(code, title string, params ...string) *Kind {
	return add(WorkerRegistry.Define(problem.Type{Code: problem.Code(code), Status: 409, Title: title, Params: params}), Error)
}

// shared makes one of foundation's rules, which every API shares, a kind of
// error.
func shared(rule *problem.Type) *Kind {
	return add(rule, Error)
}

func add(t *problem.Type, severity Severity) *Kind {
	k := &Kind{typ: t, severity: severity}
	kinds[t.Code] = k
	all = append(all, k)
	return k
}

// Lookup returns the kind with a code.
func Lookup(code problem.Code) (*Kind, bool) {
	k, ok := kinds[code]
	return k, ok
}

// Kinds returns every kind, foundation's rules among them, in the order they
// are defined.
func Kinds() []*Kind { return slices.Clone(all) }

// Code is the kind's stable code.
func (k *Kind) Code() problem.Code { return k.typ.Code }

// Type is the kind's problem type.
func (k *Kind) Type() *problem.Type { return k.typ }

// Severity is the severity of every diagnostic of the kind.
func (k *Kind) Severity() Severity { return k.severity }

// Message returns the kind's message with params, in English.
func (k *Kind) Message(params Params) string {
	if msg, ok := localizer.Message("en", k.typ.Code, displayed(params)); ok {
		return msg
	}
	return string(k.typ.Code)
}

// mustHaveDeclared ends the test binary, in a build with the paramcheck
// tag (checkDeclared), as the tests are built, if params lacks a param the
// kind declares, but filled, if it isn't "", which its caller fills
// (undeclared): foundation sends a problem that lacks one as its invalid,
// with no params, so whoever reads it never learns what was wrong. Every
// other build checks nothing. What builds a problem without Problem or
// At, as MaskProblem and the worker's last-resort end of an attempt do
// from one that was, isn't checked.
func (k *Kind) mustHaveDeclared(params Params, filled string) {
	if !checkDeclared {
		return
	}
	for _, name := range k.typ.Params {
		if _, ok := params[name]; !ok && name != filled {
			undeclared(k.typ.Code, name)
		}
	}
}

// Problem returns a problem of the kind with params, whose detail is its
// message in English, for a resolver to return for what is wrong: its
// caller reports it with its code and params, which each reader sees in
// their own language. Params may hold where it is, as ParamFile and
// ParamLine, besides the kind's own.
func (k *Kind) Problem(params Params) *problem.Problem {
	k.mustHaveDeclared(params, "")
	return k.problem(params)
}

// InputProblem returns a problem of the kind with params, as Problem does,
// for an Inputs resolver to return for an input's value, which doesn't
// know the input's name: its params leave out input, which the compiler
// fills with the input's name as it reports the problem
// (compile.InputResolver).
func (k *Kind) InputProblem(params Params) *problem.Problem {
	k.mustHaveDeclared(params, "input")
	return k.problem(params)
}

// problem is Problem, without its check.
func (k *Kind) problem(params Params) *problem.Problem {
	p := k.typ.New("")
	p.Params = make(Params, len(params))
	for name, v := range params {
		if b, ok := v.(bool); ok {
			v = strconv.FormatBool(b)
		}
		p.Params[name] = v
	}
	p.Detail = k.Message(p.Params)
	return p
}

// IsProblem reports whether err is, or wraps, a problem: what is wrong,
// which says why with a code and params, rather than what the operating
// system or a library said.
func IsProblem(err error) bool {
	_, ok := errors.AsType[*problem.Problem](err)
	return ok
}

// displayed returns params with each text's control characters escaped,
// however deep in a list or a mapping, keys too, for a message to show:
// params itself when none has any.
func displayed(params Params) Params {
	var out Params
	for name, v := range params {
		e, changed := escapedValue(v)
		if !changed {
			continue
		}
		if out == nil {
			out = maps.Clone(params)
		}
		out[name] = e
	}
	if out == nil {
		return params
	}
	return out
}

// escapedValue returns v with each text in it escaped (EscapeControl),
// however deep in lists and mappings, the mappings' keys too, and whether
// that changed it; v itself when it didn't.
func escapedValue(v any) (any, bool) {
	switch v := v.(type) {
	case string:
		if !needsEscape(v) {
			return v, false
		}
		return EscapeControl(v), true
	case []string:
		if !slices.ContainsFunc(v, needsEscape) {
			return v, false
		}
		out := make([]string, len(v))
		for i, s := range v {
			out[i] = EscapeControl(s)
		}
		return out, true
	case []any:
		var out []any
		for i, e := range v {
			if x, changed := escapedValue(e); changed {
				if out == nil {
					out = slices.Clone(v)
				}
				out[i] = x
			}
		}
		if out == nil {
			return v, false
		}
		return out, true
	case map[string]any:
		changed := false
		out := make(map[string]any, len(v))
		for k, e := range v {
			x, c := escapedValue(e)
			changed = changed || c || needsEscape(k)
			out[EscapeControl(k)] = x
		}
		if !changed {
			return v, false
		}
		return out, true
	}
	return v, false
}

// escaped says whether EscapeControl escapes r: a control character, C0,
// DEL or C1; a format character, such as a bidirectional override or a
// zero-width joiner; or a line or paragraph separator.
func escaped(r rune) bool {
	return r < 0x20 || r >= 0x7f && r <= 0x9f || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp)
}

// needsEscape says whether EscapeControl changes s.
func needsEscape(s string) bool {
	return !utf8.ValidString(s) || strings.ContainsFunc(s, escaped)
}

// EscapeControl returns s with what a terminal or an editor would act on,
// or show other than as it is, escaped as Go writes it in a string: each
// control character, C0, DEL or C1, as \n, \t or \x1b; each format
// character, such as a bidirectional override, and each line or paragraph
// separator, as \u202e or \u2028; and each byte that isn't UTF-8, such as
// 0x9b, an 8-bit CSI, as \x9b. Showing s then shows them.
func EscapeControl(s string) string {
	if !needsEscape(s) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			fmt.Fprintf(&b, `\x%02x`, s[i])
		case !escaped(r):
			b.WriteString(s[i : i+size])
		case r == '\a', r == '\b', r == '\f', r == '\n', r == '\r', r == '\t', r == '\v':
			q := strconv.QuoteRune(r)
			b.WriteString(q[1 : len(q)-1])
		case r < 0x80:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r <= 0xffff:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
		i += size
	}
	return b.String()
}

// At returns a diagnostic of the kind about the value at pointer, a JSON
// Pointer (RFC 6901), in file, at pos. Its detail is its message in English.
func (k *Kind) At(file string, pos Position, pointer string, params Params) Diagnostic {
	k.mustHaveDeclared(params, "")
	fe := k.typ.At(fragment(pointer), "")
	fe.Params = make(Params, len(params)+3)
	for name, v := range params {
		// foundation's clients keep only string and number params, so a
		// boolean that switches a select travels as "true" or "false".
		if b, ok := v.(bool); ok {
			v = strconv.FormatBool(b)
		}
		fe.Params[name] = v
	}
	fe.Detail = k.Message(fe.Params)
	if file != "" {
		fe.Params[ParamFile] = file
	}
	if pos.Line > 0 {
		fe.Params[ParamLine] = pos.Line
	}
	if pos.Column > 0 {
		fe.Params[ParamColumn] = pos.Column
	}
	return Diagnostic{*fe}
}

// fragment writes a JSON Pointer as the URI fragment problem details use, as
// problem.Pointer does: /jobs/a b is #/jobs/a%20b.
func fragment(pointer string) string {
	tokens := strings.Split(pointer, "/")
	for i, t := range tokens {
		tokens[i] = url.PathEscape(t)
	}
	return "#" + strings.Join(tokens, "/")
}

// A Diagnostic is an error or a warning about a workflow file. It is a
// foundation problem.FieldError: Code names its kind, Pointer is where in
// the workflow it is, as a URI fragment, and Params are the values its
// message shows, with the file, line and column besides. Detail is its
// message in English, or empty in a plan, which keeps a diagnostic's code
// and params so that each reader sees it in their own language.
type Diagnostic struct {
	problem.FieldError
}

// UnmarshalJSON decodes a diagnostic as problem.FieldError's MarshalJSON
// encodes it, refusing members it doesn't have, its type or its code
// missing, null, and an in that isn't one of those its tag lists.
func (d *Diagnostic) UnmarshalJSON(data []byte) error {
	type wire problem.FieldError
	var w wire
	if err := jsonv2.Unmarshal(data, &w, jsonv2.RejectUnknownMembers(true)); err != nil {
		return err
	}
	if err := jsonshape.Check(data, reflect.TypeFor[wire](), &jsonshape.Options{}); err != nil {
		return err
	}
	d.FieldError = problem.FieldError(w)
	return nil
}

// Kind returns the diagnostic's kind, which a newer version of Meridian may
// have and this one doesn't.
func (d Diagnostic) Kind() (*Kind, bool) { return Lookup(d.Code) }

// Severity is the severity of the diagnostic's kind. A kind this version
// doesn't know is an error.
func (d Diagnostic) Severity() Severity {
	if k, ok := d.Kind(); ok {
		return k.severity
	}
	return Error
}

// File is the name of the workflow file the diagnostic is about.
func (d Diagnostic) File() string {
	s, _ := d.Params[ParamFile].(string)
	return s
}

// Position is where in the file the diagnostic is, or the zero Position for
// the whole file.
func (d Diagnostic) Position() Position {
	return Position{Line: intParam(d.Params[ParamLine]), Column: intParam(d.Params[ParamColumn])}
}

func intParam(v any) int {
	switch v := v.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

// Path is the JSON Pointer (RFC 6901) of what the diagnostic is about, or ""
// for the whole file.
func (d Diagnostic) Path() string {
	p, err := url.PathUnescape(strings.TrimPrefix(d.Pointer, "#"))
	if err != nil {
		return strings.TrimPrefix(d.Pointer, "#")
	}
	return p
}

// Message returns the diagnostic's message in English: its detail, or its
// kind's message when it has none.
func (d Diagnostic) Message() string {
	if d.Detail != "" {
		return d.Detail
	}
	return d.Localize("en")
}

// Localize returns the diagnostic's message in lang, such as "ja", or in
// English when Catalog doesn't have lang. A kind this version doesn't know
// gives its detail.
func (d Diagnostic) Localize(lang string) string {
	if msg, ok := localizer.Message(lang, d.Code, displayed(d.Params)); ok {
		return msg
	}
	if d.Detail != "" {
		return d.Detail
	}
	return string(d.Code)
}

// String formats d as compilers do, which editors and CI systems parse:
//
//	file:line:column: severity: message (code)
func (d Diagnostic) String() string {
	var b strings.Builder
	if f := d.File(); f != "" {
		b.WriteString(EscapeControl(f))
		b.WriteString(":")
	}
	if p := d.Position().String(); p != "" {
		b.WriteString(p)
		b.WriteString(":")
	}
	if b.Len() > 0 {
		b.WriteString(" ")
	}
	// Its message and code can come from elsewhere, as a coordinator's
	// problem the worker passes on, so each is escaped as a param is.
	fmt.Fprintf(&b, "%s: %s (%s)", d.Severity(), EscapeControl(d.Message()), EscapeControl(string(d.Code)))
	return b.String()
}

// Diagnostics is a list of diagnostics, in the order of their positions.
type Diagnostics []Diagnostic

// HasErrors reports whether any diagnostic is an error.
func (ds Diagnostics) HasErrors() bool {
	return slices.ContainsFunc(ds, func(d Diagnostic) bool { return d.Severity() == Error })
}

// Errors returns the errors in ds.
func (ds Diagnostics) Errors() Diagnostics {
	return slices.DeleteFunc(slices.Clone(ds), func(d Diagnostic) bool { return d.Severity() != Error })
}

// Warnings returns the warnings in ds.
func (ds Diagnostics) Warnings() Diagnostics {
	return slices.DeleteFunc(slices.Clone(ds), func(d Diagnostic) bool { return d.Severity() != Warning })
}

// Err returns the errors in ds, without the warnings, or nil if there are
// none.
func (ds Diagnostics) Err() error {
	if errs := ds.Errors(); len(errs) > 0 {
		return errs
	}
	return nil
}

// Error formats the diagnostics one per line.
func (ds Diagnostics) Error() string {
	lines := make([]string, len(ds))
	for i, d := range ds {
		lines[i] = d.String()
	}
	return strings.Join(lines, "\n")
}

// Sort orders the diagnostics by position, then path, code and message, and
// drops any that is the same as the one before it: an expression that two
// others read reports its error once.
func (ds *Diagnostics) Sort() {
	slices.SortFunc(*ds, compare)
	*ds = slices.CompactFunc(*ds, func(a, b Diagnostic) bool {
		return compare(a, b) == 0 && a.File() == b.File() && a.Pointer == b.Pointer
	})
}

func compare(a, b Diagnostic) int {
	return cmp.Or(
		a.Position().Compare(b.Position()),
		strings.Compare(a.Path(), b.Path()),
		strings.Compare(string(a.Code), string(b.Code)),
		strings.Compare(a.Message(), b.Message()),
	)
}
