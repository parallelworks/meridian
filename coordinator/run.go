package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/problems"
)

// The coordinator's Keys are the seal package's.
var _ seal.Keys = Keys(nil)

// sharedQueue is the hopper queue that every coordinator of a store
// works: each run's supervisors, and its finisher.
const sharedQueue = "meridian"

// sharedWorkers is how many of the shared queue's jobs a coordinator works
// at once.
const sharedWorkers = 8

// The hopper jobs of a run, which the run's workflow holds: Start, which
// every other job waits for, and the finisher, which ends the run once
// everything else has.
type (
	startArgs struct {
		Run string `json:"run"`
	}
	finishArgs struct {
		Run string `json:"run"`
	}
)

func (startArgs) Kind() string  { return "meridian.start" }
func (finishArgs) Kind() string { return "meridian.run" }

// Options say what a coordinator works with.
type Options struct {
	// Database is the store's database: a SQLite file on a local disk,
	// which several coordinators, in this process or others, can share.
	Database string
	// Keys wraps the data keys of runs' secret records.
	Keys Keys
	// Variables, Inputs and Sessions are the resolvers Start reads the
	// run's variables, its inputs of a platform's types and its sessions
	// through (compile.StartInput). Nil means there are none.
	Variables compile.Variables
	Inputs    compile.InputResolver
	Sessions  compile.Sessions
	// Groups gives the execution groups jobs run in. CreateRun checks a
	// run's default group with it. Nil means it isn't checked.
	Groups compile.Groups
	// StartQueue names the hopper queue the Start of the run runID goes
	// in. A coordinator works the queue of each run it creates until the
	// run has started, so a queue of a run's own is worked only by the
	// coordinator that created it: its Start reads its variables from
	// where that one does, as from the environment of the meridian command
	// that ran the workflow. Nil, or "", is the queue every coordinator of
	// the store works.
	StartQueue func(runID string) string
	// Log hears of what the coordinator does that its calls don't say,
	// such as a run it couldn't start, which it tries again. Nil means
	// none.
	Log *slog.Logger
}

// RunOptions say how CreateRun stores a run.
type RunOptions struct {
	// DefaultGroup is the execution group a job runs in whose runs-on:
	// names none.
	DefaultGroup string
	// RunDir returns the run's directory, by its ID, which its jobs run in
	// (PW_JOB_DIR), and which each job's worker makes if it isn't there. It
	// is called as the run is stored, and must make nothing: a run's ID
	// can be given again to another run, after the transaction that gave
	// it first rolls back. So what is named after a run's ID, such as its
	// directory or its logs, is made only once the run is stored, and only
	// for that run: under a directory of the store's own, or named after
	// the store's ID too, so that a store made again, whose runs are
	// numbered from 1 again, makes none of another's.
	RunDir func(runID string) string
}

// startRetry is how the coordinator tries again to start a run that it
// couldn't, as when a resolver couldn't answer: after first, doubling up
// to most, tries times in all, after which the run fails. A run whose plan
// only a newer release reads it leaves for newer, each time.
type startRetry struct {
	tries       int
	first, most time.Duration
	newer       time.Duration
	// attempts bounds the hopper job's own attempts, when the coordinator
	// itself fails, as when its store does.
	attempts int
}

// defaultStartRetry tries 8 times, from 2 seconds to 2 minutes apart: about
// 4 minutes in all.
var defaultStartRetry = startRetry{tries: 8, first: 2 * time.Second, most: 2 * time.Minute, newer: 5 * time.Minute, attempts: 10}

// backoff returns how long to wait after the run's tries-th try.
func (r startRetry) backoff(tries int) time.Duration {
	d := r.first
	for range tries - 1 {
		if d >= r.most {
			return r.most
		}
		d *= 2
	}
	return min(d, r.most)
}

// A Coordinator stores runs, and works their hopper jobs as one of the
// store's replicas.
type Coordinator struct {
	opts   Options
	log    *slog.Logger
	store  *store.Store
	sealer *seal.Sealer
	plans  *planCache
	// inserter inserts runs' workflows, and served works their jobs while
	// Serve runs.
	inserter *hopper.Client[*sql.Tx]

	// What only tests change.
	timing       Timing
	retry        startRetry
	startTimeout time.Duration
	pollInterval time.Duration
	stopTimeout  time.Duration

	mu      sync.Mutex
	serving bool
	served  *hopper.Client[*sql.Tx]
	// stopping is set once Serve has begun to stop served, which then
	// works no more queues and stops working none (retireQueue).
	stopping bool
	// queues are the start queues of the runs this coordinator created,
	// which it works until their runs start.
	queues map[string]bool
	// retired counts the queues served is being made to stop working,
	// each of which waits for the queue's jobs until retiring is done, as
	// Serve stops.
	retired  sync.WaitGroup
	retiring context.Context
}

// Open opens a coordinator on the store at opts.Database. It works no
// run's jobs until Serve.
func Open(ctx context.Context, opts Options) (*Coordinator, error) {
	if opts.Keys == nil {
		return nil, errors.New("coordinator: no Keys")
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	st, err := store.Open(ctx, opts.Database, &store.Options{Log: log})
	if err != nil {
		return nil, err
	}
	sealer, err := seal.New(opts.Keys, st.ID())
	if err != nil {
		return nil, errors.Join(err, st.Close())
	}
	inserter, err := hopper.NewClient(st.Driver(), &hopper.Config{Logger: log})
	if err != nil {
		return nil, errors.Join(err, st.Close())
	}
	return &Coordinator{
		opts: opts, log: log, store: st, sealer: sealer, plans: newPlanCache(planCacheCount, planCacheBytes), inserter: inserter,
		timing: DefaultTiming(), retry: defaultStartRetry, startTimeout: 5 * time.Minute, pollInterval: time.Second, stopTimeout: 5 * time.Second,
		queues: map[string]bool{},
	}, nil
}

// Close closes the coordinator's store. Serve must have returned.
func (c *Coordinator) Close() error { return c.store.Close() }

// CreateRun stores a run of what Create gave, and returns its ID, the
// next number in the store. One transaction stores the run with its plan,
// its secret record, sealed under a data key of its own that Keys wraps,
// and its hopper workflow, so that either all of it is stored or none of
// it is: Start, which runs once a coordinator works its queue, and the
// finisher, which ends the run. A default group that Groups doesn't have
// is a problem of kind group_unknown. An error once the run is stored, as
// one that keeps the coordinator from working its start queue, comes with
// the run's ID.
func (c *Coordinator) CreateRun(ctx context.Context, created *compile.Created, opts RunOptions) (string, error) {
	switch {
	case created == nil || created.Plan == nil:
		return "", errors.New("coordinator: no plan to run")
	case opts.DefaultGroup == "":
		return "", errors.New("coordinator: a run without a default group")
	case opts.RunDir == nil:
		return "", errors.New("coordinator: a run without a directory")
	}
	if c.opts.Groups != nil {
		_, err := c.opts.Groups.Group(ctx, opts.DefaultGroup)
		if errors.Is(err, compile.ErrNotFound) {
			return "", problems.GroupUnknown.Problem(problems.Params{"group": opts.DefaultGroup})
		}
		if err != nil {
			return "", err
		}
	}
	var id, queue string
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		// Keys wraps the record's data key while the transaction holds
		// the store's write lock, so every other write waits for it,
		// workers' heartbeats among them: a local key file is quick to
		// read, and a key service slow to answer holds them all up.
		sealed, gen, err := c.sealer.Seal(ctx, id, &created.Record)
		if err != nil {
			return err
		}
		queue = c.startQueue(id)
		wf := hopper.NewWorkflow("meridian run "+id, nil)
		start := wf.Add(startArgs{Run: id}, &hopper.StepOpts{Insert: &hopper.InsertOpts{Queue: queue, MaxAttempts: c.retry.attempts}})
		wf.Add(finishArgs{Run: id}, &hopper.StepOpts{
			After: []hopper.Step{start}, OnDependencyFailure: hopper.DependencyIgnore,
			Insert: &hopper.InsertOpts{Queue: sharedQueue, Await: true},
		})
		res, err := c.inserter.InsertWorkflowTx(ctx, tx.SQL(), wf)
		if err != nil {
			return err
		}
		if err := tx.InsertRun(store.NewRun{ID: id, Plan: created.Plan, DefaultGroup: opts.DefaultGroup, RunDir: opts.RunDir(id), WorkflowID: res.ID}); err != nil {
			return err
		}
		return tx.InsertRecord(id, gen, sealed.Key, sealed.Record)
	})
	if err != nil {
		return "", err
	}
	if err := c.workQueue(ctx, queue); err != nil {
		return id, fmt.Errorf("coordinator: work run %s's start queue: %w", id, err)
	}
	return id, nil
}

// startQueue is the hopper queue the Start of the run id goes in.
func (c *Coordinator) startQueue(id string) string {
	if c.opts.StartQueue != nil {
		if q := c.opts.StartQueue(id); q != "" {
			return q
		}
	}
	return sharedQueue
}

// workQueue has the coordinator work the start queue q, from now if Serve
// runs, and once it next does otherwise, as when it is stopping.
func (c *Coordinator) workQueue(ctx context.Context, q string) error {
	if q == sharedQueue {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queues[q] {
		return nil
	}
	c.queues[q] = true
	if c.served == nil || c.stopping {
		return nil
	}
	return c.served.Queues().Add(ctx, q, hopper.QueueConfig{MaxWorkers: 1})
}

// retireQueue stops working the start queue q, once its run has started,
// when its jobs have ended. A client that is stopping stops working it
// anyway.
//
// hopper's Remove changes the client's queues without the lock its Stop
// reads them under (hopper v0.8.0), so Serve starts none once it is
// stopping, and waits for those it started before it stops the client.
func (c *Coordinator) retireQueue(ctx context.Context, q string) {
	if q == sharedQueue {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.queues[q] {
		return
	}
	delete(c.queues, q)
	if c.served == nil || c.stopping {
		return
	}
	served, retiring := c.served, c.retiring
	// Remove waits for the queue's jobs, the one retiring it among them,
	// until Serve stops.
	c.retired.Go(func() {
		if err := served.Queues().Remove(retiring, q); err != nil {
			c.log.DebugContext(ctx, "coordinator: stop working a start queue", "queue", q, "error", err)
		}
	})
}

// Serve works runs' hopper jobs, as a replica of the store's coordinators,
// until ctx is done: the shared queue, and the start queue of each run
// this coordinator created, until it has started. Once ctx is done, it
// waits a few seconds for the jobs it is working, and then cuts them
// short, to be done again. Serve can be called again once it returns.
func (c *Coordinator) Serve(ctx context.Context) error {
	c.mu.Lock()
	if c.serving {
		c.mu.Unlock()
		return errors.New("coordinator: Serve is already running")
	}
	c.serving = true
	queues := map[string]hopper.QueueConfig{sharedQueue: {MaxWorkers: sharedWorkers}}
	for _, q := range slices.Sorted(maps.Keys(c.queues)) {
		queues[q] = hopper.QueueConfig{MaxWorkers: 1}
	}
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, &startWorker{c: c})
	hopper.AddWorker(workers, &finishWorker{c: c})
	served, err := hopper.NewClient(c.store.Driver(), &hopper.Config{
		Queues: queues, Workers: workers, Logger: c.log, PollInterval: c.pollInterval,
	})
	if err == nil {
		err = served.Start(ctx)
	}
	if err != nil {
		c.serving = false
		c.mu.Unlock()
		return err
	}
	retiring, stopRetiring := context.WithCancel(context.WithoutCancel(ctx))
	c.served, c.retiring = served, retiring
	c.mu.Unlock()

	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.stopTimeout)
	defer cancel()
	// The queues being retired have stopped changing served's before it
	// stops: no more are, and those that were stop waiting for their jobs.
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()
	stopRetiring()
	if !waitGroup(stopCtx, &c.retired) {
		c.log.WarnContext(ctx, "coordinator: a start queue was still being retired as the coordinator stopped")
	}
	// Jobs still running at the deadline have their contexts cancelled:
	// each of a run's steps can be done again, and gives its attempt back
	// to hopper when it is cut short so (stopping).
	err = served.Stop(stopCtx)
	if errors.Is(err, context.DeadlineExceeded) {
		c.log.InfoContext(ctx, "coordinator: stopped the jobs it was working, to be done again")
		err = nil
	}
	c.retired.Wait()
	c.mu.Lock()
	c.served, c.serving, c.stopping, c.retiring = nil, false, false, nil
	c.mu.Unlock()
	return err
}

// waitGroup waits for wg until ctx is done, and reports whether wg was.
func waitGroup(ctx context.Context, wg *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// complete completes a run's hopper job, through done, whose work another
// attempt did. A job that this attempt no longer holds is done with.
func (c *Coordinator) complete(ctx context.Context, done func(context.Context, *store.Tx) error) error {
	err := c.store.Tx(ctx, func(tx *store.Tx) error { return done(ctx, tx) })
	if errors.Is(err, hopper.ErrJobFenced) {
		return nil
	}
	return err
}

// diagnostic is a run's problem of kind k, with params.
func diagnostic(k *problems.Kind, params problems.Params) problems.Diagnostic {
	return k.At("", problems.Position{}, "", params)
}
