package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// readied is what Ready gave for a job, with what it was readied with.
type readied struct {
	result *plan.ReadyResult
	diags  problems.Diagnostics
}

// readyJob readies the run's job key, as plan.Ready does, with the run's
// values and the results of the jobs it needs, each joined from run state
// and the secret record, which it opens; and with what the job was first
// readied with, which it pins the first time (store.Tx.PinReady), so that
// each time it is readied, as at its placement and at each claim of an
// attempt of it, Ready gives the same job, byte for byte.
//
// Its error is what kept it from readying the job: what the plan cache
// gives (plan.ErrVersion, plan.ErrMalformed), what opening the record
// gives, or the store's.
func (c *Coordinator) readyJob(ctx context.Context, run *store.Run, key string) (*readied, error) {
	if run.StartState == nil {
		return nil, fmt.Errorf("coordinator: run %s hasn't started", run.ID)
	}
	p, err := c.plans.get(run)
	if err != nil {
		return nil, err
	}
	var (
		stored *store.Record
		job    *store.Job
		pj     *plan.Job
		needed = map[string]*store.Job{}
	)
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if stored, err = tx.Record(run.ID); err != nil {
			return err
		}
		// Nothing stops a run yet, so a job is readied as one whose run
		// hasn't been stopped.
		if job, err = tx.PinReady(run.ID, key, false, nil); err != nil {
			return err
		}
		if pj = planJob(p, job.Name); pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
		}
		for _, need := range pj.Needs {
			if needed[need.Job], err = tx.Job(run.ID, need.Job); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	rec, err := c.sealer.Open(ctx, run.ID, stored.Generation, sealedOf(stored))
	if err != nil {
		return nil, err
	}
	in := plan.ReadyInput{Run: run.StartState.Run, Needs: map[string]plan.JobResult{}, Cancelled: job.ReadiedStopped}
	if rec.Run != nil {
		in.Run = plan.JoinRunValues(*rec.Run, run.StartState.Run)
	}
	for name, j := range needed {
		in.Needs[name] = neededResult(j, rec)
	}
	result, diags, err := p.Ready(ctx, job.Name, in)
	if err != nil {
		return nil, err
	}
	return &readied{result: result, diags: diags}, nil
}

// neededResult is the result of j, a job another needs, as the jobs that
// need it read it: run state's, joined with what the secret record holds
// of it. A job that hasn't ended, as one whose supervisor hopper gave up
// on, reads as failed. It is the coordinator's one read of a job's private
// half.
func neededResult(j *store.Job, rec *plan.Record) plan.JobResult {
	if j.Result == nil {
		return plan.JobResult{Outcome: plan.OutcomeFailure}
	}
	return plan.JoinJobResult(rec.Jobs[j.Key], *j.Result)
}

// planJob returns the job of p named name, or nil.
func planJob(p *plan.Plan, name string) *plan.Job {
	i, ok := slices.BinarySearchFunc(p.Jobs, name, func(j plan.Job, name string) int { return strings.Compare(j.Name, name) })
	if !ok {
		return nil
	}
	return &p.Jobs[i]
}

// errNotRun is what CreateRun returns for a plan with what the coordinator
// doesn't run yet.
var errNotRun = errors.New("coordinator: a matrix job, or a need on any instance of one, isn't run yet")

// checkRunnable refuses, with errNotRun, a plan with a job the coordinator
// doesn't run yet: one with a strategy:, or that needs any instance of
// one.
func checkRunnable(p *plan.Plan) error {
	for _, j := range p.Jobs {
		if j.Strategy != nil || slices.ContainsFunc(j.Needs, func(n plan.Need) bool { return n.Any }) {
			return fmt.Errorf("%w: job %q", errNotRun, j.Name)
		}
	}
	return nil
}

// jobOrder returns p's jobs in an order in which each comes after the jobs
// it needs, and in the order of their names among those it can come after.
func jobOrder(p *plan.Plan) ([]*plan.Job, error) {
	// Create refuses a cycle, so each round places at least one job.
	order := make([]*plan.Job, 0, len(p.Jobs))
	placed := map[string]bool{}
	for len(order) < len(p.Jobs) {
		n := len(order)
		for i := range p.Jobs {
			j := &p.Jobs[i]
			if !placed[j.Name] && !slices.ContainsFunc(j.Needs, func(n plan.Need) bool { return !placed[n.Job] }) {
				order = append(order, j)
			}
		}
		if len(order) == n {
			return nil, errors.New("coordinator: the plan's needs: form a cycle")
		}
		for _, j := range order[n:] {
			placed[j.Name] = true
		}
	}
	return order, nil
}
