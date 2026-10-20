// Package conformance holds the workflow format's conformance suite: cases
// that pin what workflows mean, for any implementation of the format to run.
//
// In the format, inputs and compile areas, each case is a directory, such as
// format/yaml-alias, with a workflow.yaml and a case.yaml that says what the
// workflow must produce. A case's directory can hold other files the
// workflow refers to. In the expressions area, each file holds cases that
// pin what expressions evaluate to.
package conformance

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"strings"

	"go.yaml.in/yaml/v3"
)

// FS holds the cases.
//
//go:embed format expressions inputs compile
var FS embed.FS

// A Case is one conformance case.
type Case struct {
	// Name is the case's directory, such as format/yaml-alias.
	Name string
	// Description says what the case pins.
	Description string `yaml:"description"`
	// Source is where the case comes from: ACTIVATE's own tests, a public
	// workflow at a commit, or Meridian.
	Source string `yaml:"source"`
	// License is the license of a workflow copied from a public repository.
	License string `yaml:"license,omitempty"`
	// Inputs are the input values submitted for a run, in the inputs area.
	Inputs map[string]any `yaml:"inputs,omitempty"`
	// Contexts are what the run's expressions read besides inputs, such as
	// org and vars.
	Contexts map[string]any `yaml:"contexts,omitempty"`
	// SecretVariables names the user variables in vars that are secret,
	// whose values the compiler doesn't have.
	SecretVariables []string `yaml:"secret-variables,omitempty"`
	// NoPlatform says there is no platform to provide the input types that
	// name a platform's resources. Otherwise a platform takes every value
	// given for one as it is.
	NoPlatform bool `yaml:"no-platform,omitempty"`
	// Expect is what the workflow must produce.
	Expect Expect `yaml:"expect"`
}

// Expect is what a case's workflow must produce.
type Expect struct {
	// Diagnostics are every error and warning loading the workflow
	// produces, in any order. None means it loads cleanly.
	Diagnostics []Diagnostic `yaml:"diagnostics,omitempty"`
	// Values are values the loaded workflow must have, by JSON Pointer.
	Values map[string]any `yaml:"values,omitempty"`
	// Inputs are the run's input values, in the inputs area. A value that
	// stays an expression until the run is underway is its text.
	Inputs map[string]any `yaml:"inputs,omitempty"`
	// Secrets are the values of password inputs, by their path.
	Secrets map[string]any `yaml:"secrets,omitempty"`
	// Plan holds values the compiled plan must have, by JSON Pointer into
	// its JSON encoding, in the compile area.
	Plan map[string]any `yaml:"plan,omitempty"`
}

// A Diagnostic is an error or warning a case expects. Line and Column are
// checked when they are set.
type Diagnostic struct {
	Severity string `yaml:"severity"`
	Code     string `yaml:"code"`
	Path     string `yaml:"path"`
	Line     int    `yaml:"line,omitempty"`
	Column   int    `yaml:"column,omitempty"`
}

// Workflow returns the case's workflow file.
func (c *Case) Workflow() ([]byte, error) {
	return fs.ReadFile(FS, path.Join(c.Name, "workflow.yaml"))
}

// Cases returns the cases in an area, such as "format", in name order.
func Cases(area string) ([]Case, error) {
	entries, err := fs.ReadDir(FS, area)
	if err != nil {
		return nil, err
	}
	var cases []Case
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := path.Join(area, e.Name())
		data, err := fs.ReadFile(FS, path.Join(name, "case.yaml"))
		if err != nil {
			return nil, err
		}
		c := Case{Name: name}
		if err := decode(data, &c); err != nil {
			return nil, fmt.Errorf("%s/case.yaml: %w", name, err)
		}
		cases = append(cases, c)
	}
	return cases, nil
}

// An ExpressionFile is a file of expression cases.
type ExpressionFile struct {
	// Name is the file's path, such as expressions/operators.yaml.
	Name string
	// Description says what the file's cases pin.
	Description string `yaml:"description"`
	// Source is where the cases come from: ACTIVATE's tests, its
	// documentation, public workflows, or Meridian.
	Source string `yaml:"source"`
	// Workflow is a workflow, or part of one, that the cases' expressions
	// are in. Relative references read it, and so do inputs, which read
	// input definitions at /on/execute/inputs.
	Workflow map[string]any `yaml:"workflow,omitempty"`
	// Scope is what the cases' expressions read, unless a case says
	// otherwise.
	Scope Scope            `yaml:"scope,omitempty"`
	Cases []ExpressionCase `yaml:"cases"`
}

// A Scope is what an expression reads.
type Scope struct {
	// Contexts maps context names, such as inputs and needs, to their
	// values. A context that isn't here isn't known yet.
	Contexts map[string]any `yaml:"contexts,omitempty"`
	// Status is how the job or step got where the expression is, for the
	// status functions. Without it, they aren't known yet.
	Status *Status `yaml:"status,omitempty"`
	// Index holds the indexes of the list items an input template is for,
	// outermost first.
	Index []int `yaml:"index,omitempty"`
}

// Status is how a job or step got where an expression is.
type Status struct {
	Failed    bool `yaml:"failed,omitempty"`
	Cancelled bool `yaml:"cancelled,omitempty"`
}

// An ExpressionCase is one expression and what it must produce: Value,
// unless Error or Unknown is set.
type ExpressionCase struct {
	// Expr is an expression: the text between ${{ and }}.
	Expr string `yaml:"expr,omitempty"`
	// Template is a string with expressions in it.
	Template string `yaml:"template,omitempty"`
	// At is the JSON Pointer of a string in the file's workflow, which is
	// evaluated where it is.
	At string `yaml:"at,omitempty"`
	// Scope replaces parts of the file's scope: each context it has, and
	// its status and index if it has them.
	Scope *Scope `yaml:"scope,omitempty"`
	// Value is what the expression evaluates to.
	Value any `yaml:"value,omitempty"`
	// Error is "syntax" for an expression that doesn't parse, or "eval" for
	// one that can't be evaluated.
	Error string `yaml:"error,omitempty"`
	// Unknown is what the expression reads that isn't known yet: a context,
	// "status" or "index". It can't be evaluated until it is.
	Unknown string `yaml:"unknown,omitempty"`
	// Condition says the expression is a condition, such as an if:.
	Condition bool `yaml:"condition,omitempty"`
	// Warnings, if set, are the byte offsets in the expression of each
	// probable mistake it has. A case without it doesn't check them.
	Warnings []int `yaml:"warnings,omitempty"`
}

// ScopeFor returns the scope of a case in f: f's scope, with the parts the
// case replaces.
func (f *ExpressionFile) ScopeFor(c *ExpressionCase) Scope {
	s := Scope{Contexts: map[string]any{}, Status: f.Scope.Status, Index: f.Scope.Index}
	maps.Copy(s.Contexts, f.Scope.Contexts)
	if c.Scope == nil {
		return s
	}
	maps.Copy(s.Contexts, c.Scope.Contexts)
	if c.Scope.Status != nil {
		s.Status = c.Scope.Status
	}
	if c.Scope.Index != nil {
		s.Index = c.Scope.Index
	}
	return s
}

// ExpressionFiles returns the files of expression cases, in name order.
func ExpressionFiles() ([]ExpressionFile, error) {
	entries, err := fs.ReadDir(FS, "expressions")
	if err != nil {
		return nil, err
	}
	var files []ExpressionFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		name := path.Join("expressions", e.Name())
		data, err := fs.ReadFile(FS, name)
		if err != nil {
			return nil, err
		}
		f := ExpressionFile{Name: name}
		if err := decode(data, &f); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for i, c := range f.Cases {
			if n := count(c.Expr != "", c.Template != "", c.At != ""); n != 1 {
				return nil, fmt.Errorf("%s: case %d has %d of expr, template and at, not 1", name, i+1, n)
			}
			if c.Error != "" && c.Unknown != "" || (c.Error != "" || c.Unknown != "") && c.Value != nil {
				return nil, fmt.Errorf("%s: case %d expects more than one of value, error and unknown", name, i+1)
			}
		}
		files = append(files, f)
	}
	return files, nil
}

func count(bs ...bool) int {
	n := 0
	for _, b := range bs {
		if b {
			n++
		}
	}
	return n
}

// decode decodes YAML, failing on keys v doesn't have.
func decode(data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	return dec.Decode(v)
}
