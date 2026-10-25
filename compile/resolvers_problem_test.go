package compile

import (
	"fmt"
	"testing"

	"github.com/parallelworks/meridian/problems"
)

// A resolver's problem of a workflow kind is the run's to report; one of
// the meridian command's own kinds, or of the worker API's, is not, as it
// isn't what is wrong with what the resolver was asked.
func TestWorkflowProblem(t *testing.T) {
	for _, c := range []struct {
		kind *problems.Kind
		ok   bool
	}{
		{problems.SourceWithInvalid, true},
		{problems.HomeUnknown, false},
		{problems.Fenced, false},
		{problems.OutOfOrder, false},
	} {
		err := fmt.Errorf("resolving: %w", c.kind.Problem(nil))
		if k, _, ok := workflowProblem(err); ok != c.ok || ok && k != c.kind {
			t.Errorf("%s: workflowProblem gives %v, %t, want %t", c.kind.Code(), k, ok, c.ok)
		}
	}
}
