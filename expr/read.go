package expr

import "errors"

// errNotImplemented is what EvalRead returns until it is built.
var errNotImplemented = errors.New("expr: not implemented")

// Read is what an evaluation read of the values its scope marks: whether
// what it gives was made from, or decided by, a value Scope.Marked marks,
// and whether by one Scope.Private marks. Each is conservative, as
// EvalMarked is, and a failed evaluation reads what it read before it
// failed.
type Read struct {
	// Marked is set when it read a value Scope.Marked marks, such as a
	// secret: a runner masks what it gives.
	Marked bool
	// Private is set when it read a value Scope.Private marks, such as a
	// variable or a password: what it gives is kept only where the run's
	// secrets are, and never shown.
	Private bool
}

// EvalRead evaluates the template as Eval does, and reports what it read of
// the values scope marks, in Scope.Marked and in Scope.Private, apart: what
// EvalMarked reports of Scope.Marked alone. A phase evaluates once, and
// learns from one evaluation both whether to mask what it gives and whether
// to keep it private.
//
// It isn't built yet, and returns an error that says so.
func (t *Template) EvalRead(scope Scope) (v any, read Read, err error) {
	return nil, Read{}, errNotImplemented
}

// EvalRead evaluates what the workflow holds at location, as Eval does,
// and reports what it read of the values its scope marks, as
// Template.EvalRead does.
//
// It isn't built yet, and returns an error that says so.
func (e *Evaluator) EvalRead(location []string) (any, Read, error) {
	return nil, Read{}, errNotImplemented
}
