package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// readied is what Ready gave for a job, with what it was readied with.
type readied struct {
	result *plan.ReadyResult
	diags  problems.Diagnostics
}

// end is how the job ends when Ready ends it without running it: failed,
// with Ready's diagnostics, when it couldn't evaluate what it evaluates;
// skipped, by its if:, which the jobs that need it read as failed when
// Ready says it was skipped because a job it needs failed (NeedsFailed);
// or cancelled, as the path its if: gives it in a run that was stopped
// before it was readied (plan.CancelPathEnd). It reports false for a job
// Ready gives to run.
func (r *readied) end() (store.JobEnd, bool) {
	switch {
	case r.diags.HasErrors():
		return store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Diagnostics: r.diags}, true
	case r.result.Skipped:
		return store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeSkipped, NeedsFailed: r.result.NeedsFailed}}, true
	case r.result.Cancelled:
		return store.JobEnd{Result: plan.JobResult{Outcome: plan.OutcomeCancelled}}, true
	}
	return store.JobEnd{}, false
}

// readyJob readies the run's job key, as plan.Ready does, with the run's
// values and the results of the jobs it needs, each joined from run state
// and the job's part of the secret record, which it opens; and with what
// the job was first readied with, which it pins the first time
// (store.Tx.PinReady), so that each time it is readied, as at its
// placement and at each claim of an attempt of it, Ready gives the same
// job, byte for byte. A job it needs that hasn't ended, as one whose
// supervisor hopper gave up on, it fails first, as the finisher does
// (failJob), in the transaction that pins it, and has the placement of its
// attempt it withdraws cancelled once that commits: so that the job, and
// the server of the worker API as an attempt of it is claimed, read it as
// ended.
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
	var unended []string
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		j, err := tx.Job(run.ID, key)
		if err != nil {
			return err
		}
		pj := planJob(p, j.Name)
		if pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, j.Name)
		}
		for _, need := range pj.Needs {
			n, err := tx.Job(run.ID, need.Job)
			if err != nil {
				return err
			}
			if n.Outcome == "" {
				unended = append(unended, n.Key)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	var ws map[string]*withdrawal
	if len(unended) > 0 {
		if ws, err = c.withdrawals(ctx, run, unended); err != nil {
			return nil, err
		}
	}
	var (
		stored    *store.Record
		job       *store.Job
		needed    = map[string]*store.Job{}
		parts     map[string]store.JobRecord
		withdrawn []string
		changed   []string
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
		pj := planJob(p, job.Name)
		if pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
		}
		withdrawn = withdrawn[:0]
		keys := make([]string, 0, len(pj.Needs))
		for _, need := range pj.Needs {
			a, err := failJob(tx, run.ID, need.Job)
			if err != nil {
				return err
			}
			if a != "" {
				withdrawn = append(withdrawn, a)
			}
			if needed[need.Job], err = tx.Job(run.ID, need.Job); err != nil {
				return err
			}
			keys = append(keys, needed[need.Job].Key)
		}
		if parts, err = tx.JobRecords(run.ID, keys); err != nil {
			return err
		}
		changed = tx.ControlsChanged()
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(unended) > 0 {
		c.log.WarnContext(ctx, "coordinator: jobs a job needs never ended, as when hopper gave up on their supervisors, so they failed as it was readied", "run", run.ID, "job", key, "needs", unended)
		c.controlsChanged(changed)
		c.cancelPlacements(ctx, run.ID, ws, withdrawn)
	}
	rec, err := c.sealer.Open(ctx, run.ID, stored.Generation, sealedOf(stored))
	if err != nil {
		return nil, err
	}
	jobs, err := c.openJobRecords(ctx, run.ID, stored.WrappedKey, parts)
	if err != nil {
		return nil, err
	}
	in := plan.ReadyInput{Run: run.StartState.Run, Needs: map[string]plan.JobResult{}, Cancelled: job.ReadiedStopped}
	if rec.Run != nil {
		in.Run = plan.JoinRunValues(*rec.Run, run.StartState.Run)
	}
	for name, j := range needed {
		in.Needs[name] = neededResult(j, jobs)
	}
	result, diags, err := p.Ready(ctx, job.Name, in)
	if err != nil {
		return nil, err
	}
	return &readied{result: result, diags: diags}, nil
}

// openJobRecords opens parts, the parts of the run runID's secret record of
// the jobs they are by key (store.Tx.JobRecords), with the run's data key,
// which Keys wrapped as wrapped.
func (c *Coordinator) openJobRecords(ctx context.Context, runID string, wrapped []byte, parts map[string]store.JobRecord) (map[string]plan.JobRecord, error) {
	sealed := make(map[string]seal.JobPart, len(parts))
	for key, part := range parts {
		sealed[key] = seal.JobPart{Generation: part.Generation, Sealed: part.Sealed}
	}
	return c.sealer.OpenJobs(ctx, runID, wrapped, sealed)
}

// neededResult is the result of j, a job another needs, which has ended,
// as the jobs that need it read it: run state's, joined with its part of
// the secret record, jobs[j.Key], which its attempt's end stored, if it
// ended with private outputs. It is the coordinator's one read of a job's
// private half.
func neededResult(j *store.Job, jobs map[string]plan.JobRecord) plan.JobResult {
	return plan.JoinJobResult(jobs[j.Key], *j.Result)
}

// planJob returns the job of p named name, or nil.
func planJob(p *plan.Plan, name string) *plan.Job {
	i, ok := slices.BinarySearchFunc(p.Jobs, name, func(j plan.Job, name string) int { return strings.Compare(j.Name, name) })
	if !ok {
		return nil
	}
	return &p.Jobs[i]
}

// checkRunnable refuses a plan with a job the coordinator doesn't run yet:
// one with a strategy:, a problem of kind matrix_not_built, or one that
// needs any instance of another, any_not_built.
func checkRunnable(p *plan.Plan) error {
	for _, j := range p.Jobs {
		if j.Strategy != nil {
			return problems.MatrixNotBuilt.Problem(problems.Params{"job": j.Name})
		}
	}
	for _, j := range p.Jobs {
		if i := slices.IndexFunc(j.Needs, func(n plan.Need) bool { return n.Any }); i >= 0 {
			return problems.AnyNotBuilt.Problem(problems.Params{"job": j.Name, "needed": j.Needs[i].Job})
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
