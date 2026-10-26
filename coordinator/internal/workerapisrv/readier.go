package workerapisrv

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// Readied is what the coordinator kept of how it readied a job, which a
// claim of an attempt of the job readies it with again, so that Ready gives
// the same as it gave then (docs/design.md, A job's lifecycle).
type Readied struct {
	// Stopped is set when the run had been stopped, by a cancel or by its
	// timeout, as the job was readied (plan.ReadyInput.Cancelled).
	Stopped bool
	// Any are the keys of the instances the job's needs on any instance of
	// a matrix job (plan.Need.Any) took, by the needed job's name.
	Any map[string]string
	// Instance is the matrix instance the job is, as run state holds it
	// (plan.Expansion.Split), or nil for a job without a matrix. A private
	// instance's values are in the run's secret record, in its job's
	// expansion (plan.Record.Expansions).
	Instance *plan.Instance
}

// A PlanReadier is the coordinator's Readier: it readies an attempt's job
// from the run's plan, the run's values and its needs' results, each
// joined from what run state holds of it and what the run's secret record
// holds, and what the coordinator kept of how it readied the job.
type PlanReadier struct {
	Store  *store.Store
	Sealer Sealer
	// Plan returns the run's plan, decoded: the coordinator's cache of
	// plans, which never decodes one of a version this release doesn't
	// read (plan.ErrVersion).
	Plan func(run *store.Run) (*plan.Plan, error)
	// RunValues returns what run state holds of the run's values: what
	// Start kept there (compile.StartState.Run).
	RunValues func(run *store.Run) (plan.RunValues, error)
	// Readied returns what the coordinator kept of how it readied job, a
	// job of run, read in tx.
	Readied func(ctx context.Context, tx *store.Tx, run *store.Run, job *store.Job) (*Readied, error)
}

var _ Readier = (*PlanReadier)(nil)

// ReadyAttempt returns the job of the attempt a, as Ready gives it again,
// with the part of the run's values and of its needs' results it reads.
func (pr *PlanReadier) ReadyAttempt(ctx context.Context, run *store.Run, a *store.Attempt) (*Ready, error) {
	p, err := pr.Plan(run)
	if err != nil {
		return nil, err
	}
	var (
		job     *store.Job
		pj      *plan.Job
		readied *Readied
		stored  *store.Record
		deps    = map[string]*store.Job{}
	)
	err = pr.Store.View(ctx, func(tx *store.Tx) error {
		var err error
		if job, err = tx.Job(run.ID, a.Job); err != nil {
			return err
		}
		if pj = planJob(p, job.Name); pj == nil {
			return fmt.Errorf("workerapisrv: run %s's plan has no job %q", run.ID, job.Name)
		}
		if readied, err = pr.Readied(ctx, tx, run, job); err != nil {
			return err
		}
		if stored, err = tx.Record(run.ID); err != nil {
			return err
		}
		for _, n := range pj.Needs {
			key := n.Job
			if n.Any {
				if key = readied.Any[n.Job]; key == "" {
					return fmt.Errorf("workerapisrv: run %s's job %q was readied without an instance of %q", run.ID, a.Job, n.Job)
				}
			}
			dep, err := tx.Job(run.ID, key)
			if err != nil {
				return err
			}
			if dep.Result == nil {
				return fmt.Errorf("workerapisrv: run %s's job %q needs %q, which hasn't ended", run.ID, a.Job, key)
			}
			deps[n.Job] = dep
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	rec, err := pr.Sealer.Open(ctx, run.ID, stored.Generation, seal.Sealed{Key: stored.WrappedKey, Record: stored.Sealed})
	if err != nil {
		return nil, err
	}
	state, err := pr.RunValues(run)
	if err != nil {
		return nil, err
	}
	var private plan.RunValues
	if rec.Run != nil {
		private = *rec.Run
	}
	values := plan.JoinRunValues(private, state)
	needs := make(map[string]plan.JobResult, len(deps))
	for name, dep := range deps {
		needs[name] = plan.JoinJobResult(rec.Jobs[dep.Key], *dep.Result)
	}
	instance, err := wholeInstance(readied.Instance, rec, job.Name)
	if err != nil {
		return nil, err
	}
	res, diags, err := p.Ready(ctx, job.Name, plan.ReadyInput{Run: values, Needs: needs, Cancelled: readied.Stopped, Instance: instance})
	switch {
	case err != nil:
		return nil, err
	case diags.HasErrors():
		// Ready gave the job once with the same inputs: this is a bug, and
		// the diagnostics, which show no private value, are logged with it.
		return nil, fmt.Errorf("workerapisrv: run %s's job %q doesn't ready again as it did: %w", run.ID, a.Job, diags)
	case res.Job == nil:
		return nil, fmt.Errorf("workerapisrv: run %s's job %q readies again as no job to run", run.ID, a.Job)
	}
	return &Ready{Job: res.Job, Run: pj.Reads(values), Needs: pj.ReadsNeeds(needs)}, nil
}

// Steps returns how many steps the attempt's job has, as the plan says.
func (pr *PlanReadier) Steps(ctx context.Context, run *store.Run, a *store.Attempt) (int, error) {
	p, err := pr.Plan(run)
	if err != nil {
		return 0, err
	}
	var job *store.Job
	if err := pr.Store.View(ctx, func(tx *store.Tx) error {
		var err error
		job, err = tx.Job(run.ID, a.Job)
		return err
	}); err != nil {
		return 0, err
	}
	pj := planJob(p, job.Name)
	if pj == nil {
		return 0, fmt.Errorf("workerapisrv: run %s's plan has no job %q", run.ID, job.Name)
	}
	return len(pj.Steps), nil
}

// planJob returns the plan's job named name, or nil.
func planJob(p *plan.Plan, name string) *plan.Job {
	i, ok := slices.BinarySearchFunc(p.Jobs, name, func(j plan.Job, name string) int { return strings.Compare(j.Name, name) })
	if !ok {
		return nil
	}
	return &p.Jobs[i]
}

// wholeInstance returns the instance i, as run state holds it, whole: with
// its matrix values from the run's secret record, rec, for a private one,
// whose expansion of the job name the record holds.
func wholeInstance(i *plan.Instance, rec *plan.Record, name string) (*plan.Instance, error) {
	if i == nil || !i.Private {
		return i, nil
	}
	x := rec.Expansions[name]
	if x == nil || i.Index < 0 || i.Index >= len(x.Instances) || x.Instances[i.Index].Key != i.Key {
		return nil, errors.New("workerapisrv: the secret record has no private instance " + i.Key)
	}
	whole := x.Instances[i.Index]
	return &whole, nil
}
