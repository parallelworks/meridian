// Package conformance holds the workflow format's conformance suite: cases
// that pin what workflows mean, for any implementation of the format to run.
//
// Each case is a directory under an area, such as format/yaml-alias, with a
// workflow.yaml and a case.yaml that says what the workflow must produce.
// A case's directory can hold other files the workflow refers to.
package conformance

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"path"

	"go.yaml.in/yaml/v3"
)

// FS holds the cases.
//
//go:embed format
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
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&c); err != nil {
			return nil, fmt.Errorf("%s/case.yaml: %w", name, err)
		}
		cases = append(cases, c)
	}
	return cases, nil
}
