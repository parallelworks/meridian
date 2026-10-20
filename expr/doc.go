// Package expr parses and evaluates the ${{ }} expressions in workflows.
//
// The language is format 1's, the one ACTIVATE evaluates: JavaScript's
// operators, with its rules for comparing and converting values, plus word
// operators such as in and get, GitHub Actions' functions, the status
// functions, and map() and filter(), which run a lambda such as
// h => h.name for each item of a list. docs/design.md describes it, and the
// conformance suite's expressions area pins what each expression means.
//
// Parse parses one expression and ParseTemplate a string that has
// expressions in it. Both are evaluated in a Scope, which holds the
// contexts the expression can read. An expression that reads a context the
// scope doesn't have yet returns an *UnknownError, so that it can be
// evaluated later, when the context is known.
//
// Only expressions written in the workflow are evaluated. A value that an
// expression reads, such as an input's value or a job's output, is never
// evaluated, even if it holds ${{ }}.
package expr
