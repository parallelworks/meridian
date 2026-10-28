package plan

// The uses: of the built-in actions the coordinator performs besides
// checkout (CheckoutUses, CoordinatorAction): cancel-jobs, of the run's
// jobs, and update-session, of the run's sessions (docs/design.md,
// Built-in actions).
const (
	CancelJobsUses    = "parallelworks/cancel-jobs"
	UpdateSessionUses = "parallelworks/update-session"
)

// CoordinatorAction reports whether uses names a built-in action whose
// subject is the coordinator's, which it performs when a step's worker
// asks for it, through the worker API (workerapi.PathAction): cancel-jobs,
// update-session, and checkout, whose tree the coordinator then serves.
// The coordinator and the worker both read the list here, so that each
// takes the same steps for its own.
func CoordinatorAction(uses string) bool {
	switch uses {
	case CheckoutUses, CancelJobsUses, UpdateSessionUses:
		return true
	}
	return false
}
