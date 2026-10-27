package coordinator

import (
	"context"
	"fmt"
	"net"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
)

// ServeWorkers serves the worker API to the workers of the attempts the
// coordinator places, on l, a Unix socket's listener, where Endpoint says
// they reach it, until ctx is done, and then closes l: it takes
// connections only from the process's own user, or root, and stores the
// logs they upload with logs. A claim readies the attempt's job again as
// its supervisor readied it, and each change to an attempt that its
// supervisor acts on, as its claim or its end, wakes the supervisor. The
// polls it holds of attempts' controls are answered as soon as the
// coordinator changes them, rather than at the server's next read of them.
func (c *Coordinator) ServeWorkers(ctx context.Context, l net.Listener, logs Logs) error {
	if err := c.writable(); err != nil {
		return err
	}
	srv, err := workerapisrv.New(workerapisrv.Options{
		Store: c.store, Logs: logs, Sealer: c.sealer,
		Readier: claimReadier{c},
		Wake:    c.wakeWorkerJob,
		Log:     c.log,
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.controls = srv
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.controls == srv {
			c.controls = nil
		}
		c.mu.Unlock()
	}()
	return srv.Serve(ctx, l)
}

// wakeWorkerJob wakes, in tx, the supervisor of the run's job, once the
// worker API stores a change to its attempt that the supervisor acts on
// (workerapisrv.Options.Wake), in the transaction that stores it, and
// returns what wakes it again once that commits, if hopper runs it.
func (c *Coordinator) wakeWorkerJob(ctx context.Context, tx *store.Tx, run, job string) (func(), error) {
	w, err := c.wakeJobTx(ctx, tx, run, job)
	if w == nil || err != nil {
		return nil, err
	}
	return func() { c.wakeAgain(context.WithoutCancel(ctx), w) }, nil
}

// claimReadier is the coordinator's workerapisrv.Readier: it readies the
// job of an attempt a worker claims from what its supervisor pinned as it
// first readied the job, read and opened as the supervisor reads them
// (readReadyFrom, readyInput), so that every claim gives the job its
// supervisor placed, byte for byte.
type claimReadier struct{ c *Coordinator }

var _ workerapisrv.Readier = claimReadier{}

// ReadyAttempt returns the job of the attempt a, as Ready gives it again,
// with the part of the run's values and of its needs' results it reads.
func (r claimReadier) ReadyAttempt(ctx context.Context, run *store.Run, a *store.Attempt) (*workerapisrv.Ready, error) {
	c := r.c
	p, err := c.plans.get(run)
	if err != nil {
		return nil, err
	}
	var (
		from *readyFrom
		pj   *plan.Job
	)
	if err := c.store.View(ctx, func(tx *store.Tx) error {
		job, err := tx.Job(run.ID, a.Job)
		if err != nil {
			return err
		}
		if pj = p.Job(job.Name); pj == nil {
			return fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
		}
		from, err = readReadyFrom(tx, run, pj, job, false)
		return err
	}); err != nil {
		return nil, err
	}
	in, err := c.readyInput(ctx, run, from)
	if err != nil {
		return nil, err
	}
	res, diags, err := p.Ready(ctx, from.job.Name, in)
	switch {
	case err != nil:
		return nil, err
	case diags.HasErrors():
		// Ready gave the job once with the same inputs: this is a bug, and
		// the diagnostics, which show no private value, are logged with it.
		return nil, fmt.Errorf("coordinator: run %s's job %q doesn't ready again as it did: %w", run.ID, a.Job, diags)
	case res.Job == nil:
		return nil, fmt.Errorf("coordinator: run %s's job %q readies again as no job to run", run.ID, a.Job)
	}
	return &workerapisrv.Ready{Job: res.Job, Run: pj.Reads(in.Run), Needs: pj.ReadsNeeds(in.Needs)}, nil
}

// Steps returns how many steps the attempt's job has, as the plan says.
func (r claimReadier) Steps(ctx context.Context, run *store.Run, a *store.Attempt) (int, error) {
	p, err := r.c.plans.get(run)
	if err != nil {
		return 0, err
	}
	var job *store.Job
	if err := r.c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		job, err = tx.Job(run.ID, a.Job)
		return err
	}); err != nil {
		return 0, err
	}
	pj := p.Job(job.Name)
	if pj == nil {
		return 0, fmt.Errorf("coordinator: run %s's plan has no job %q", run.ID, job.Name)
	}
	return len(pj.Steps), nil
}
