// Package plan holds the plan: a compiled run, which a coordinator schedules
// and workers run without the workflow file, and its JSON encoding.
//
// Every part of a workflow that can hold expressions is a Value, which keeps
// each string's expressions parsed. A plan stores parsed expressions rather
// than their source, so that a run resumed by a later release means what it
// meant when it was compiled.
//
// The package also holds the phases after Start, each one function that
// reaches nothing: Job.Ready, in the coordinator, and ReadyJob.Step and
// ReadyJob.End, in the worker; Expand, which expands a matrix, and
// Combine, which combines its instances' results; NewScope, which builds
// the contexts an expression reads in each phase; and the values they
// share: RunValues, which Split divides between the run's secret record
// and its run state and Job.Reads cuts down to a job's part, and the
// Masker that masks secrets in what a job gives.
package plan
