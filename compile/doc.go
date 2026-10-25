// Package compile turns a workflow and its inputs into a plan, and
// completes a run's values as it starts: the Create and Start phases,
// Create and Start. Compile is Create for a caller that needs only the
// plan, such as one that checks a workflow.
//
// The compiler does no I/O of its own: what it needs from a platform, such
// as the details of a cluster an input names, it asks for through resolver
// interfaces: InputResolver, Variables, Groups, Sources and Sessions. A
// resolver that is nil isn't there, and what needs it fails to compile,
// with an error that names it.
//
// Variables, Groups, Sources and Sessions get the caller's context. An
// error from one that is, or wraps, a foundation *problem.Problem says what
// is wrong with what it was asked, such as a repository the person running
// the workflow can't read: the compiler reports it with its code and
// params, so that a platform's reason reaches each reader in their own
// language. ErrNotFound is a name the resolver doesn't have, which the
// compiler reports as a problem of its own. Any other error means the
// resolver couldn't answer, such as a store it can't reach: it isn't a
// problem with the workflow, and the run can be created or started again.
//
// docs/design.md describes the steps the compiler takes, and the
// conformance suite's areas pin what each produces.
package compile
