package coordinator

import (
	"cmp"
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
// Ready gives to run, or a matrix it expanded.
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
// and the job's part of the secret record, which it opens; for a matrix
// instance, with its matrix values; and with what the job was first
// readied with, which it pins the first time (store.Tx.PinReady), so that
// each time it is readied, as at its placement and at each claim of an
// attempt of it, Ready gives the same job, byte for byte. A need on any
// instance of a job reads the instance its marker chose, which is pinned
// too, with the outputs of the instances that had ended then. A job it
// needs, but not on any instance, that hasn't ended, as one whose
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
	unended, err := c.unendedNeeds(ctx, run, p, key)
	if err != nil {
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
		parent    *store.Job
		needed    = map[string]*store.Job{}
		parts     map[string]store.JobRecord
		withdrawn []string
		changed   []string
		// merged are, for a need on any instance, the instances that had
		// ended as it was chosen, whose outputs are read.
		merged = map[string][]*store.Job{}
	)
	err = c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if stored, err = tx.Record(run.ID); err != nil {
			return err
		}
		if job, err = tx.Job(run.ID, key); err != nil {
			return err
		}
		pj := planJob(p, job.Name)
		if pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
		}
		withdrawn = withdrawn[:0]
		var (
			choices map[string]string
			keys    []string
		)
		for _, need := range pj.Needs {
			k := need.Job
			if need.Any {
				a, err := tx.Any(run.ID, need.Job)
				switch {
				case err != nil:
					return err
				case a.ChosenAt.IsZero():
					return fmt.Errorf("coordinator: run %s's job %q is readied before any instance of %q was chosen", run.ID, key, need.Job)
				}
				if choices == nil {
					choices = map[string]string{}
				}
				choices[need.Job] = a.Instance
				k = cmp.Or(a.Instance, need.Job)
				for _, key := range a.Ended {
					j, err := tx.Job(run.ID, key)
					if err != nil {
						return err
					}
					merged[need.Job] = append(merged[need.Job], j)
					keys = append(keys, key)
				}
			} else if slices.Contains(unended, need.Job) {
				ids, err := failNeed(tx, run.ID, need.Job)
				if err != nil {
					return err
				}
				withdrawn = append(withdrawn, ids...)
			}
			if needed[need.Job], err = tx.Job(run.ID, k); err != nil {
				return err
			}
			if needed[need.Job].Result == nil {
				return fmt.Errorf("coordinator: run %s's job %q needs %q, which hasn't ended", run.ID, key, k)
			}
			keys = append(keys, k)
		}
		// A job readied once its run is stopped takes the path its if:
		// gives it (plan.CancelPath), as the run's stop record says now.
		now, err := tx.Run(run.ID)
		if err != nil {
			return err
		}
		if job, err = tx.PinReady(run.ID, key, !now.StoppedAt.IsZero(), choices); err != nil {
			return err
		}
		if job.Instance >= 0 {
			if parent, err = tx.Job(run.ID, job.Name); err != nil {
				return err
			}
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
		if ended := merged[name]; len(ended) > 0 {
			results := make([]plan.JobResult, len(ended))
			for i, e := range ended {
				results[i] = neededResult(e, jobs)
			}
			in.Needs[name] = plan.AnyResult(in.Needs[name], results)
		}
	}
	if parent != nil {
		x := expansionOf(run, parent, rec)
		if x == nil || job.Instance >= len(x.Instances) {
			return nil, fmt.Errorf("coordinator: run %s's job %q has no instance %d", run.ID, job.Name, job.Instance)
		}
		instance := x.Instances[job.Instance]
		in.Instance = &instance
	}
	result, diags, err := p.Ready(ctx, job.Name, in)
	if err != nil {
		return nil, err
	}
	return &readied{result: result, diags: diags}, nil
}

// unendedNeeds returns the keys of the jobs the run's job key needs, but
// not on any instance, that haven't ended, with the keys of their
// instances that haven't, for a matrix job: those readyJob fails.
func (c *Coordinator) unendedNeeds(ctx context.Context, run *store.Run, p *plan.Plan, key string) ([]string, error) {
	var unended []string
	err := c.store.View(ctx, func(tx *store.Tx) error {
		j, err := tx.Job(run.ID, key)
		if err != nil {
			return err
		}
		pj := planJob(p, j.Name)
		if pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, j.Name)
		}
		for _, need := range pj.Needs {
			if need.Any {
				continue
			}
			n, err := tx.Job(run.ID, need.Job)
			if err != nil {
				return err
			}
			if n.Outcome != "" {
				continue
			}
			unended = append(unended, n.Key)
			instances, err := tx.Instances(run.ID, need.Job)
			if err != nil {
				return err
			}
			for _, i := range instances {
				if i.Outcome == "" {
					unended = append(unended, i.Key)
				}
			}
		}
		return nil
	})
	return unended, err
}

// failNeed fails the run's job name, which a job needs, and its instances
// that haven't ended, for a matrix job, as failJob does each, and returns
// the attempts it withdrew.
func failNeed(tx *store.Tx, run, name string) ([]string, error) {
	instances, err := tx.Instances(run, name)
	if err != nil {
		return nil, err
	}
	var withdrawn []string
	for _, key := range append(keysOf(instances), name) {
		a, err := failJob(tx, run, key)
		if err != nil {
			return nil, err
		}
		if a != "" {
			withdrawn = append(withdrawn, a)
		}
	}
	return withdrawn, nil
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
// the secret record, jobs[j.Key], which its end stored, if it ended with
// private outputs. It is the coordinator's one read of a job's private
// half, as putJobRecord is its one write.
func neededResult(j *store.Job, jobs map[string]plan.JobRecord) plan.JobResult {
	return plan.JoinJobResult(jobs[j.Key], *j.Result)
}

// putJobRecord stores rec, the private half of the result the run's job
// key ended with, as its part of the run's secret record, in tx, sealed at
// the part's next generation, unless it has none. Sealing it asks Keys for
// the run's data key while tx holds the store's write lock, as CreateRun
// does.
func (c *Coordinator) putJobRecord(ctx context.Context, tx *store.Tx, run, key string, rec plan.JobRecord) error {
	if len(rec.Outputs) == 0 && len(rec.Steps) == 0 {
		return nil
	}
	wrapped, err := tx.WrappedKey(run)
	if err != nil {
		return err
	}
	parts, err := tx.JobRecords(run, []string{key})
	if err != nil {
		return err
	}
	gen := parts[key].Generation + 1
	sealed, err := c.sealer.SealJob(ctx, run, key, gen, wrapped, rec)
	if err != nil {
		return err
	}
	return tx.PutJobRecord(run, key, gen, sealed)
}

// keepExpansion keeps x, the instances of the run's matrix job name, which
// are private (plan.Expansion.Split), in the run's secret record, in tx,
// where the server of the worker API reads an instance's matrix values as
// its attempt is claimed. It seals the record again, which a run's matrix
// jobs do once each, and which asks Keys for its data key while tx holds
// the store's write lock, as CreateRun does.
func (c *Coordinator) keepExpansion(ctx context.Context, tx *store.Tx, run, name string, x *plan.Expansion) error {
	stored, err := tx.Record(run)
	if err != nil {
		return err
	}
	rec, err := c.sealer.Open(ctx, run, stored.Generation, sealedOf(stored))
	if err != nil {
		return err
	}
	if rec.Expansions == nil {
		rec.Expansions = map[string]*plan.Expansion{}
	}
	rec.Expansions[name] = x
	resealed, gen, err := c.sealer.Reseal(ctx, run, stored.Generation, sealedOf(stored), rec)
	if err != nil {
		return err
	}
	return tx.Reseal(run, gen, resealed.Record)
}

// expansionOf returns the instances of parent, a matrix job of run: whole,
// from the secret record rec, when they are private; else as run state
// holds them, Start's or the job's own, once its supervisor expanded its
// matrix at Ready; or nil before it has.
func expansionOf(run *store.Run, parent *store.Job, rec *plan.Record) *plan.Expansion {
	if x := rec.Expansions[parent.Name]; x != nil {
		return x
	}
	if x := run.StartState.Expansions[parent.Name]; x != nil {
		return x
	}
	return parent.Expansion
}

// planJob returns the job of p named name, or nil.
func planJob(p *plan.Plan, name string) *plan.Job {
	i, ok := slices.BinarySearchFunc(p.Jobs, name, func(j plan.Job, name string) int { return strings.Compare(j.Name, name) })
	if !ok {
		return nil
	}
	return &p.Jobs[i]
}

// anyNeeded returns the names of the jobs of jobs that another needs any
// instance of (needs: <job>:any), in order.
func anyNeeded(jobs []*plan.Job) []string {
	var names []string
	for _, j := range jobs {
		for _, n := range j.Needs {
			if n.Any && !slices.Contains(names, n.Job) {
				names = append(names, n.Job)
			}
		}
	}
	slices.Sort(names)
	return names
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
