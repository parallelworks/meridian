// Package compile turns a workflow and its inputs into a plan.
//
// The compiler does no I/O of its own: what it needs from a platform, such
// as the details of a cluster an input names, it asks for through resolver
// interfaces. docs/design.md describes the steps it takes, and the
// conformance suite's areas pin what each produces.
package compile
