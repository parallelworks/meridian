// Package compile turns a workflow and its inputs into a plan, and
// completes a run's values as it starts: the Create and Start phases,
// Compile and Start.
//
// The compiler does no I/O of its own: what it needs from a platform, such
// as the details of a cluster an input names, it asks for through resolver
// interfaces: InputResolver, Variables, Groups, Sources and Sessions. A
// resolver that is nil isn't there, and what needs it fails to compile,
// with an error that names it. Variables, Groups, Sources and Sessions get
// the caller's context, and an error from one other than ErrNotFound means
// it couldn't answer, such as a store it can't reach: it isn't a problem
// with the workflow, and the run can be created or started again.
//
// docs/design.md describes the steps the compiler takes, and the
// conformance suite's areas pin what each produces.
package compile
