package expr

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

// or returns what r and s read together.
func (r Read) or(s Read) Read {
	return Read{Marked: r.Marked || s.Marked, Private: r.Private || s.Private}
}

// EvalRead evaluates the template as Eval does, and reports what it read of
// the values scope marks, in Scope.Marked and in Scope.Private, apart: what
// EvalMarked reports of Scope.Marked alone. A phase evaluates once, and
// learns from one evaluation both whether to mask what it gives and whether
// to keep it private.
func (t *Template) EvalRead(scope Scope) (v any, read Read, err error) {
	scope = scope.Budgeted()
	outer := scope.budget.noting()
	v, err = t.Eval(scope)
	return v, scope.budget.done(outer), err
}

// EvalRead evaluates what the workflow holds at location, as Eval does,
// and reports what it read of the values its scope marks, as
// Template.EvalRead does: what the expressions there read, or a string,
// list or mapping they read did, though it was made in an earlier
// evaluation.
func (e *Evaluator) EvalRead(location []string) (any, Read, error) {
	b := e.eval.scope.budget
	outer := b.noting()
	v, err := e.Eval(location)
	return v, b.done(outer), err
}
