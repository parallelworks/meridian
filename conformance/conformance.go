// Package conformance holds the workflow format's conformance suite: cases
// that pin what workflows mean, for any implementation of the format to run.
//
// In the format, inputs, compile and contexts areas, each case is a
// directory, such as format/yaml-alias, with a workflow.yaml and a
// case.yaml that says what the workflow must produce. A case's directory
// can hold other files the workflow refers to. In the expressions area,
// each file holds cases that pin what expressions evaluate to, and in the
// matrices area, cases that pin how matrices expand.
package conformance

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"strings"

	"go.yaml.in/yaml/v3"
)

// FS holds the cases.
//
//go:embed format expressions inputs compile matrices contexts
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
	// Jobs are what is known of each job, by name, where the phases after
	// the run starts evaluate it, in the contexts area.
	Jobs map[string]JobState `yaml:"jobs,omitempty"`
	// Expect is what the workflow must produce.
	Expect Expect `yaml:"expect"`
}

// Expect is what a case's workflow must produce.
type Expect struct {
	// Diagnostics are every error and warning loading the workflow
	// produces, in any order. None means it loads cleanly.
	Diagnostics []Diagnostic `yaml:"diagnostics,omitempty"`
	// Values are values the loaded workflow must have, by JSON Pointer, in
	// the format area, and in the contexts area the values the phases after
	// the run starts give at the JSON Pointers of what they evaluate.
	Values map[string]any `yaml:"values,omitempty"`
	// Inputs are the run's input values, in the inputs area, without those
	// in Deferred. A run with errors doesn't start, so in a case with
	// errors, Inputs, Deferred and Secrets hold only the values the case
	// checks, and the run can have others.
	Inputs map[string]any `yaml:"inputs,omitempty"`
	// Deferred are the values that stay expressions until the run is
	// underway, such as a default that reads a secret variable, by their
	// input's path, as in group.name. Each is its text: a template as the
	// string it is written as, and a list or mapping with each template in
	// it as its text.
	Deferred map[string]any `yaml:"deferred,omitempty"`
	// Secrets are the values of password inputs, by their path.
	Secrets map[string]any `yaml:"secrets,omitempty"`
	// Plan holds values the compiled plan must have, by JSON Pointer into
	// its JSON encoding, in the compile area.
	Plan map[string]any `yaml:"plan,omitempty"`
	// Skipped are the JSON Pointers of the jobs and steps whose if: is
	// false, in the contexts area, and of the cleanup: of each step that
	// didn't run. Every other job, step and cleanup: there runs.
	Skipped []string `yaml:"skipped,omitempty"`
	// Private are the JSON Pointers of the job outputs that are private, in
	// the contexts area: made from an organization or user variable or a
	// password, and not from a secret, so kept only with the run's secrets.
	// Every other output there isn't.
	Private []string `yaml:"private,omitempty"`
	// NeedsFailed are the JSON Pointers of the jobs skipped because a job
	// they need failed, in the contexts area, which the jobs that need them
	// read as failed. Every other job skipped there was skipped by its own
	// if:.
	NeedsFailed []string `yaml:"needs-failed,omitempty"`
}

// JobState is what is known of a job where the phases after the run
// starts evaluate it: when its dependencies have finished, and as each of
// its steps is about to run.
type JobState struct {
	// Needs are the results of the jobs it needs, by name.
	Needs map[string]NeedResult `yaml:"needs,omitempty"`
	// Cancelled says the run is being cancelled.
	Cancelled bool `yaml:"cancelled,omitempty"`
	// Index and Matrix are the matrix instance's index, from 0, and its
	// values, for a job with a matrix.
	Index  *int           `yaml:"index,omitempty"`
	Matrix map[string]any `yaml:"matrix,omitempty"`
	// Try is the try its steps are evaluated at, from 0, and Env the
	// variables the worker defines for them, such as
	// PW_WORKFLOW_STEP_CURRENT_RETRY, which env reads over the rest.
	Try int               `yaml:"try,omitempty"`
	Env map[string]string `yaml:"env,omitempty"`
	// Attempt is the attempt's number, from 1, and Group and Member where
	// it was placed: the job context's.
	Attempt int    `yaml:"attempt,omitempty"`
	Group   string `yaml:"group,omitempty"`
	Member  string `yaml:"member,omitempty"`
	// Steps are how its steps ended, in order: those before a step, as it
	// is about to run, and all of them, for its outputs.
	Steps []StepResult `yaml:"steps,omitempty"`
}

// NeedResult is how a job that another needs ended.
type NeedResult struct {
	// Result is its outcome: success, failure, cancelled or skipped.
	Result string `yaml:"result"`
	// NeedsFailed says it was skipped because a job it needs failed, was
	// cancelled or was skipped so in turn, rather than by its own if:, or
	// for a matrix job that one of its instances was. Cancelled says one of
	// a matrix job's instances was cancelled, whatever its result.
	NeedsFailed bool              `yaml:"needs-failed,omitempty"`
	Cancelled   bool              `yaml:"cancelled,omitempty"`
	Outputs     map[string]string `yaml:"outputs,omitempty"`
	// Steps are the outputs of its steps that have ids, by id.
	Steps map[string]map[string]string `yaml:"steps,omitempty"`
}

// StepResult is how a step ended: its outcome, as its job counts it, and
// what it wrote to its outputs.
type StepResult struct {
	Outcome string            `yaml:"outcome"`
	Outputs map[string]string `yaml:"outputs,omitempty"`
}

// A Diagnostic is an error or warning a case expects. Line and Column are
// each checked when they are set: column 0 says the diagnostic has a line
// but no column, and line 0 that it is about the whole file.
type Diagnostic struct {
	Severity string `yaml:"severity"`
	Code     string `yaml:"code"`
	Path     string `yaml:"path"`
	Line     *int   `yaml:"line,omitempty"`
	Column   *int   `yaml:"column,omitempty"`
	// Params are the values of some of the diagnostic's params, by name.
	// A kind's params are part of its contract, as its code is. Those
	// listed must have these values, and the others aren't checked. A
	// param is text or a number, and a boolean is the text "true" or
	// "false".
	Params map[string]any `yaml:"params,omitempty"`
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

// An ExpressionCase is one expression and what it must produce: exactly one
// of a value, an error and something unknown. A case that expects null has
// value: null.
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
	// Code, if set, is the code of the error's kind, such as
	// unclosed_string. Meridian's problems package lists them.
	Code string `yaml:"code,omitempty"`
	// Unknown is what the expression reads that isn't known yet: a context,
	// "status" or "index". It can't be evaluated until it is.
	Unknown string `yaml:"unknown,omitempty"`
	// Condition says the expression is a condition, such as an if:.
	Condition bool `yaml:"condition,omitempty"`
	// Warnings, if set, are the byte offsets in the expression of each
	// probable mistake it has. A case without it doesn't check them.
	Warnings []int `yaml:"warnings,omitempty"`
}

// A MatrixFile is a file of matrix cases.
type MatrixFile struct {
	// Name is the file's path, such as matrices/include.yaml.
	Name string
	// Description says what the file's cases pin.
	Description string `yaml:"description"`
	// Source is where the cases come from.
	Source string `yaml:"source"`
	// Contexts are what the cases' strategies read, unless a case says
	// otherwise, as an expression case's scope's are.
	Contexts map[string]any `yaml:"contexts,omitempty"`
	Cases    []MatrixCase   `yaml:"cases"`
}

// A MatrixCase is a job's strategy: and what it expands to: exactly one of
// its instances and an error.
type MatrixCase struct {
	// Description says what the case pins.
	Description string `yaml:"description,omitempty"`
	// Strategy is the job's strategy:, whose strings can hold expressions.
	Strategy map[string]any `yaml:"strategy"`
	// Contexts replace each of the file's contexts they name.
	Contexts map[string]any `yaml:"contexts,omitempty"`
	// Limit is how many instances a matrix may have, which isn't part of
	// the format: whoever runs a workflow sets it. 0 is Meridian's default,
	// 256.
	Limit int `yaml:"limit,omitempty"`
	// Instances are the matrix values of each instance, in the order of
	// their indexes.
	Instances []map[string]any `yaml:"instances,omitempty"`
	// FailFast and MaxParallel are what the strategy's fail-fast: and
	// max-parallel: are, checked when they are set: whether the remaining
	// instances are cancelled when one fails, and how many run at a time,
	// 0 for no limit.
	FailFast    *bool `yaml:"fail-fast,omitempty"`
	MaxParallel *int  `yaml:"max-parallel,omitempty"`
	// Error is the code of the problem the matrix has, and Path the JSON
	// Pointer, from the job, of where it is.
	Error string `yaml:"error,omitempty"`
	Path  string `yaml:"path,omitempty"`
}

// ContextsFor returns the contexts of a case in f: f's, with those the case
// replaces.
func (f *MatrixFile) ContextsFor(c *MatrixCase) map[string]any {
	out := maps.Clone(f.Contexts)
	if out == nil {
		out = map[string]any{}
	}
	maps.Copy(out, c.Contexts)
	return out
}

// MatrixFiles returns the files of matrix cases, in name order.
func MatrixFiles() ([]MatrixFile, error) {
	entries, err := fs.ReadDir(FS, "matrices")
	if err != nil {
		return nil, err
	}
	var files []MatrixFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		name := path.Join("matrices", e.Name())
		data, err := fs.ReadFile(FS, name)
		if err != nil {
			return nil, err
		}
		f := MatrixFile{Name: name}
		if err := decode(data, &f); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for i, c := range f.Cases {
			if (c.Error == "") == (c.Instances == nil) {
				return nil, fmt.Errorf("%s: case %d has %d of instances and error, not 1", name, i+1, count(c.Error != "", c.Instances != nil))
			}
			if (c.Error == "") != (c.Path == "") {
				return nil, fmt.Errorf("%s: case %d has an error without a path, or a path without an error", name, i+1)
			}
		}
		files = append(files, f)
	}
	return files, nil
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
		expects, err := expectations(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for i, c := range f.Cases {
			if n := count(c.Expr != "", c.Template != "", c.At != ""); n != 1 {
				return nil, fmt.Errorf("%s: case %d has %d of expr, template and at, not 1", name, i+1, n)
			}
			if expects[i] != 1 {
				return nil, fmt.Errorf("%s: case %d expects %d of value, error and unknown, not 1", name, i+1, expects[i])
			}
			if c.Code != "" && c.Error == "" {
				return nil, fmt.Errorf("%s: case %d has a code but no error", name, i+1)
			}
		}
		files = append(files, f)
	}
	return files, nil
}

// expectations returns how many of value, error and unknown each case in a
// file of expression cases has. Decoding doesn't tell value: null from no
// value, so they are counted in the file's YAML.
func expectations(data []byte) ([]int, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	var cases *yaml.Node
	if len(doc.Content) == 1 {
		cases = member(doc.Content[0], "cases")
	}
	if cases == nil || cases.Kind != yaml.SequenceNode {
		return nil, errors.New("no list of cases")
	}
	counts := make([]int, len(cases.Content))
	for i, c := range cases.Content {
		for _, key := range []string{"value", "error", "unknown"} {
			if member(c, key) != nil {
				counts[i]++
			}
		}
	}
	return counts, nil
}

// member returns the value of key in a mapping node, or nil if it has none.
func member(n *yaml.Node, key string) *yaml.Node {
	if n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
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
