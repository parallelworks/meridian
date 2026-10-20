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
