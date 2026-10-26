package backend_test

import (
	"errors"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/problems"
)

// A member of a backend this release doesn't have fits no job, and every
// call to its driver fails, with backend_not_built.
func TestNotBuilt(t *testing.T) {
	m := &backend.Member{Name: "cluster", Backend: "slurm"}
	d := backend.NotBuilt{}
	fit, err := d.Probe(t.Context(), m, backend.Demand{})
	if err != nil || fit.Problem == nil || fit.Problem.Code != problems.BackendNotBuilt.Code() ||
		fit.Problem.Params["member"] != "cluster" || fit.Problem.Params["backend"] != "slurm" {
		t.Errorf("Probe: %+v, %v", fit, err)
	}
	_, err = d.Place(t.Context(), m, backend.PlaceRequest{Attempt: "a1"})
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.BackendNotBuilt.Code() {
		t.Errorf("Place: %v", err)
	}
	for name, err := range map[string]error{
		"Connect": func() error { _, _, err := d.Connect(t.Context(), m, backend.Endpoint{}); return err }(),
		"Find":    func() error { _, _, err := d.Find(t.Context(), m, "a1"); return err }(),
		"Check":   func() error { _, err := d.Check(t.Context(), m, backend.Placement{}); return err }(),
		"Cancel":  d.Cancel(t.Context(), m, backend.Placement{}),
	} {
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.BackendNotBuilt.Code() {
			t.Errorf("%s: %v", name, err)
		}
	}
}
