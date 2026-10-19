// Package plan holds the plan: a compiled run, which a coordinator schedules
// and workers run without the workflow file, and its JSON encoding.
//
// Every part of a workflow that can hold expressions is a Value, which keeps
// each string's expressions parsed. A plan stores parsed expressions rather
// than their source, so that a run resumed by a later release means what it
// meant when it was compiled.
package plan
