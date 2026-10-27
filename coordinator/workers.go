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
// supervisor acts on, as its claim or its end, wakes the supervisor.
func (c *Coordinator) ServeWorkers(ctx context.Context, l net.Listener, logs Logs) error {
	srv, err := workerapisrv.New(workerapisrv.Options{
		Store: c.store, Logs: logs, Sealer: c.sealer,
		Readier: &workerapisrv.PlanReadier{Store: c.store, Sealer: c.sealer, Plan: c.plans.get, RunValues: runValues, Readied: readiedWith},
		Wake:    c.wakeJob,
		Log:     c.log,
	})
	if err != nil {
		return err
	}
	return srv.Serve(ctx, l)
}

// runValues is what run state holds of the run's values, which Start kept
// there.
func runValues(run *store.Run) (plan.RunValues, error) {
	if run.StartState == nil {
		return plan.RunValues{}, fmt.Errorf("coordinator: run %s hasn't started", run.ID)
	}
	return run.StartState.Run, nil
}

// readiedWith is what the job's supervisor readied it with, as it pinned
// it the first time (store.Tx.PinReady), which readyJob readies it with
// each time after.
func readiedWith(_ context.Context, _ *store.Tx, run *store.Run, job *store.Job) (*workerapisrv.Readied, error) {
	if job.ReadyAt.IsZero() {
		return nil, fmt.Errorf("coordinator: run %s's job %q hasn't been readied", run.ID, job.Key)
	}
	return &workerapisrv.Readied{Stopped: job.ReadiedStopped, Any: job.AnyChoices}, nil
}
