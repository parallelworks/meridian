package expr

import "time"

// WithClock returns scope with a budget whose clock is now, so that a test
// says how long evaluating takes without waiting. Evaluations with the
// returned scope, or a copy of it, are one group, as with Scope.Budgeted.
func WithClock(scope Scope, now func() time.Time) Scope {
	scope = scope.Budgeted()
	scope.budget.clock = now
	return scope
}

// Work evaluates e in scope, as Eval does, and returns the work it counted.
func Work(e *Expr, scope Scope) (any, int, error) {
	ev := newEvaluator(scope)
	defer ev.stop()
	v, err := ev.eval(e.root)
	return v, ev.worked, err
}
