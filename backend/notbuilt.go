package backend

import (
	"context"
	"io"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// NotBuilt is the Driver of a backend that a member can name but this
// release doesn't have yet: ssh, kubernetes, slurm and pbs today. Its
// Probe gives every member backend_not_built, so placement leaves it out,
// and every other call fails with the same problem.
type NotBuilt struct{}

var _ Driver = NotBuilt{}

// notBuilt is the problem of m's backend.
func notBuilt(m *Member) *problem.Problem {
	return problems.BackendNotBuilt.Problem(problems.Params{"member": m.Name, "backend": m.Backend})
}

// Probe says that m can't run a job: its backend isn't built.
func (NotBuilt) Probe(_ context.Context, m *Member, _ Demand) (Fit, error) {
	return Fit{Problem: notBuilt(m)}, nil
}

// Connect fails: m's backend isn't built.
func (NotBuilt) Connect(_ context.Context, m *Member, _ Endpoint) (Endpoint, io.Closer, error) {
	return Endpoint{}, nil, notBuilt(m)
}

// Place fails: m's backend isn't built.
func (NotBuilt) Place(_ context.Context, m *Member, _ PlaceRequest) (Placement, error) {
	return Placement{}, notBuilt(m)
}

// Find fails: m's backend isn't built.
func (NotBuilt) Find(_ context.Context, m *Member, _ string) (Placement, bool, error) {
	return Placement{}, false, notBuilt(m)
}

// Check fails: m's backend isn't built.
func (NotBuilt) Check(_ context.Context, m *Member, _ Placement) (Status, error) {
	return Status{}, notBuilt(m)
}

// Cancel fails: m's backend isn't built.
func (NotBuilt) Cancel(_ context.Context, m *Member, _ Placement) error {
	return notBuilt(m)
}
