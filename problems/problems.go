// Package problems defines the problems Meridian reports about a workflow,
// and the diagnostics that report them.
//
// Each Kind of problem has a stable code, a severity, and the params its
// message shows. Its message is in Catalog, one ICU MessageFormat message per
// language, and its documentation page's text is in Docs. A Diagnostic is
// foundation's problem.FieldError, so a platform shows it, translates it and
// links it to its page like any other problem: the code's type URI is
// /problems/workflow/<code>.
package problems

import (
	"cmp"
	"embed"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io/fs"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/parallelworks/foundation/problem"
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

// List joins items a message shows with ", ". The items are data, such as
// names and values, so the list reads the same in every language.
func List(items []string) string { return strings.Join(items, ", ") }

// Show shows a value from a workflow or a run in a message: as compact JSON,
// cut to 40 characters.
func Show(v any) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	s := fmt.Sprint(v)
	if err := enc.Encode(v); err == nil {
		s = strings.TrimSuffix(b.String(), "\n")
	}
	if utf8.RuneCountInString(s) > maxShown {
		r := []rune(s)
		s = string(r[:maxShown-1]) + "…"
	}
	return s
}

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
	// Registry holds every kind's problem type, for a platform's problem
	// pages.
	Registry = problem.NewRegistry("workflow")
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
	if msg, ok := localizer.Message("en", k.typ.Code, params); ok {
		return msg
	}
	return string(k.typ.Code)
}

// At returns a diagnostic of the kind about the value at pointer, a JSON
// Pointer (RFC 6901), in file, at pos. Its detail is its message in English.
func (k *Kind) At(file string, pos Position, pointer string, params Params) Diagnostic {
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
// encodes it, refusing members it doesn't have.
func (d *Diagnostic) UnmarshalJSON(data []byte) error {
	type wire problem.FieldError
	var w wire
	if err := jsonv2.Unmarshal(data, &w, jsonv2.RejectUnknownMembers(true)); err != nil {
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
	if msg, ok := localizer.Message(lang, d.Code, d.Params); ok {
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
		b.WriteString(f)
		b.WriteString(":")
	}
	if p := d.Position().String(); p != "" {
		b.WriteString(p)
		b.WriteString(":")
	}
	if b.Len() > 0 {
		b.WriteString(" ")
	}
	fmt.Fprintf(&b, "%s: %s (%s)", d.Severity(), d.Message(), d.Code)
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
