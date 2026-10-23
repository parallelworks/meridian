package plan

import "github.com/parallelworks/meridian/expr"

// ReadyStatus returns how a job whose needs ended as needs said got to
// Ready, for the status functions, and whether its needs succeeded.
func ReadyStatus(needs map[string]JobResult) (expr.Status, bool) {
	return ReadyInput{Needs: needs}.status()
}
