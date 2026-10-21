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

// Counted returns what the group that shares scope's budget has counted:
// what its templates built, the work they did and the steps they took.
func Counted(scope Scope) (built, work, steps int) {
	c := scope.budget.counted()
	return c.built, c.work, c.steps
}

// Work evaluates e in scope, as Eval does, and returns the work it counted.
func Work(e *Expr, scope Scope) (any, int, error) {
	ev, err := newEvaluator(scope)
	if err != nil {
		return nil, 0, err
	}
	defer ev.stop()
	v, err := ev.eval(e.root)
	return v, ev.worked, err
}

// ToJSON returns v as toJSON writes it with indent, and the length jsonLen
// counts for it.
func ToJSON(v any, indent string) (string, int) { return toJSON(v, indent), jsonLen(v, indent, 0) }
