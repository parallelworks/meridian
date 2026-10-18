package format

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Severity says whether a diagnostic stops a workflow from running.
type Severity int

const (
	// Error stops the workflow from loading.
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

// A Code names the kind of problem a diagnostic reports. Codes are stable,
// so tools and conformance cases can match on them.
type Code string

const (
	// CodeSyntax means the file isn't valid YAML.
	CodeSyntax Code = "syntax"
	// CodeTooLarge means the file is larger than the loader accepts.
	CodeTooLarge Code = "too-large"
	// CodeUnsupported means valid YAML that the format doesn't allow: an
	// alias, a merge key, a key that isn't a string, or an infinite number.
	CodeUnsupported Code = "unsupported"
	// CodeFormat means the format key names a format this package doesn't
	// know.
	CodeFormat Code = "format"
	// CodeSchema means the workflow doesn't match its format's schema.
	CodeSchema Code = "schema"
	// CodeUnknownKey means a key the schema doesn't define, which is
	// ignored.
	CodeUnknownKey Code = "unknown-key"
	// CodeDuplicateKey means a key that appears more than once in a
	// mapping. The last value is used.
	CodeDuplicateKey Code = "duplicate-key"
	// CodeExtraDocument means the file has more than one YAML document.
	// Only the first is read.
	CodeExtraDocument Code = "extra-document"
)

// A Position is a place in a workflow file. Line and Column start at 1, and
// Column counts characters (Unicode code points). The zero Position means
// the whole file.
type Position struct {
	Line, Column int
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

func (p Position) compare(q Position) int {
	return cmp.Or(cmp.Compare(p.Line, q.Line), cmp.Compare(p.Column, q.Column))
}

// A Diagnostic is an error or warning about a workflow file.
type Diagnostic struct {
	File     string
	Position Position
	Severity Severity
	Code     Code
	// Path is the JSON Pointer (RFC 6901) of the value or key the
	// diagnostic is about, or "" for the whole file.
	Path    string
	Message string
}

// String formats d as compilers do, which editors and CI systems parse:
//
//	file:line:column: severity: message (code)
func (d Diagnostic) String() string {
	var b strings.Builder
	if d.File != "" {
		b.WriteString(d.File)
		b.WriteString(":")
	}
	if p := d.Position.String(); p != "" {
		b.WriteString(p)
		b.WriteString(":")
	}
	if b.Len() > 0 {
		b.WriteString(" ")
	}
	fmt.Fprintf(&b, "%s: %s (%s)", d.Severity, d.Message, d.Code)
	return b.String()
}

// Diagnostics is a list of diagnostics, in the order of their positions.
type Diagnostics []Diagnostic

// HasErrors reports whether any diagnostic is an error.
func (ds Diagnostics) HasErrors() bool {
	return slices.ContainsFunc(ds, func(d Diagnostic) bool { return d.Severity == Error })
}

// Err returns the errors in ds, without the warnings, or nil if there are
// none.
func (ds Diagnostics) Err() error {
	errs := slices.DeleteFunc(slices.Clone(ds), func(d Diagnostic) bool { return d.Severity != Error })
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// Error formats the diagnostics one per line.
func (ds Diagnostics) Error() string {
	lines := make([]string, len(ds))
	for i, d := range ds {
		lines[i] = d.String()
	}
	return strings.Join(lines, "\n")
}

func (ds Diagnostics) sort() {
	slices.SortFunc(ds, func(a, b Diagnostic) int {
		return cmp.Or(a.Position.compare(b.Position), strings.Compare(a.Path, b.Path), strings.Compare(a.Message, b.Message))
	})
}
