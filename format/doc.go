// Package format loads workflow files and validates them against the schema
// of their format.
//
// A workflow's format is its top-level `format:` key, or 1 without one.
// Format 1 is the format Parallel Works ACTIVATE runs, with the additions
// described in docs/design.md. Each format's JSON Schema (2020-12) is
// embedded from the schema directory.
//
// Format 1 reports some problems as warnings rather than errors, because
// ACTIVATE has always run workflows that have them: a key the schema doesn't
// define is ignored, a repeated key keeps its last value, and only the
// file's first YAML document is read.
package format
