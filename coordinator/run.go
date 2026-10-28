package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/fipsmode"
	"github.com/parallelworks/meridian/plan"
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
// every other job waits for; each job's supervisor, which waits for its
// needs' too; and the finisher, which ends the run once everything else
// has.
type (
	startArgs struct {
		Run string `json:"run"`
	}
	jobArgs struct {
		Run string `json:"run"`
		Key string `json:"key"`
	}
	anyArgs struct {
		Run string `json:"run"`
		Job string `json:"job"`
	}
	finishArgs struct {
		Run string `json:"run"`
	}
)

func (startArgs) Kind() string  { return "meridian.start" }
func (jobArgs) Kind() string    { return "meridian.job" }
func (anyArgs) Kind() string    { return "meridian.any" }
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
	// run's default group with it. Nil means it isn't checked, and that no
	// job can be placed.
	Groups compile.Groups
	// Sources reads, by its pin, a workflow a step calls whose text the
	// store doesn't keep with its root, as of a root that a release kept
	// none for (CreateChild). Nil means there are none: such a step's child
	// isn't created (child_not_pinned).
	Sources compile.Sources
	// Trees gives the trees of the commits parallelworks/checkout steps
	// were pinned to, which the coordinator serves their workers. Nil
	// means there are none: such a step fails as it runs, with
	// resolver_missing.
	Trees Trees
	// Drivers are the drivers of the backends jobs' workers are placed
	// with, by the backend's name (backend.Member.Backend). A member of a
	// backend without one is one that placement leaves out, with
	// backend_not_built (backend.NotBuilt).
	Drivers map[string]backend.Driver
	// Endpoint is where this coordinator serves the worker API, which each
	// member's driver makes its workers able to reach
	// (backend.Driver.Connect).
	Endpoint backend.Endpoint
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
	// ReadOnly opens the store only to read it, as a command that shows
	// runs does (store.Options.ReadOnly): it changes nothing in the store,
	// not even its schema, and refuses a store whose schema isn't this
	// release's. The coordinator reports runs, as Status, Runs, Jobs and
	// Follow do, and does nothing else: each call that would change a
	// run, or serve, returns ErrReadOnly. It needs no Keys.
	ReadOnly bool

	// now is the store's clock, as a test's that it moves on itself: nil
	// for the host's.
	now func() time.Time
}

// ErrReadOnly is what a coordinator opened only to read the store
// (Options.ReadOnly) returns for a call that would change a run, or
// serve.
var ErrReadOnly = errors.New("coordinator: opened only to read the store")

// writable returns ErrReadOnly for a coordinator opened only to read the
// store.
func (c *Coordinator) writable() error {
	if c.opts.ReadOnly {
		return ErrReadOnly
	}
	return nil
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
	supervision  supervision
	startTimeout time.Duration
	pollInterval time.Duration
	stopTimeout  time.Duration
	followEvery  time.Duration
	followHold   time.Duration
	// placed, if it is set, hears of each job a supervisor places, as it
	// readied it, before the attempt's driver places it.
	placed func(run *store.Run, attempt string, job *plan.ReadyJob)
	// rewake, if it is set, is called with the supervisors a transaction
	// that stopped children woke while hopper ran them, as it has them
	// woken again once it commits (wakeAgainAfter).
	rewake func([]*waking)
	// beforeChildStore, if it is set, is called as CreateChild stores a
	// child, between its Create and its transaction, as when a test races
	// two asks.
	beforeChildStore func()
	// checkoutWaits are how long a checkout's tree waits before each read
	// of its commit again (checkout).
	checkoutWaits []time.Duration

	mu      sync.Mutex
	serving bool
	served  *hopper.Client[*sql.Tx]
	// stopping is set once Serve has begun to stop served, which then
	// works no more queues and stops working none (retireQueue).
	stopping bool
	// queues are the start queues of the runs this coordinator created,
	// or resumed, which it works until their runs start, each with the
	// function that releases its hold on the run's Start
	// (store.HoldStart), or nil for a run that has none.
	queues map[string]func()
	// retired counts the queues served is being made to stop working,
	// each of which waits for the queue's jobs until retiring is done, as
	// Serve stops.
	retired  sync.WaitGroup
	retiring context.Context
	// connections are where members' workers reach the coordinator, as
	// their drivers' Connect gave it, by member.
	connections map[string]connection
	// server is the worker API's server while ServeWorkers or ServeMembers
	// serves it, and serverUsers how many of them do (useServer).
	server      *workerapisrv.Server
	serverUsers int
	// forwarding is what ServeMembers serves members' listeners with
	// while it runs, or nil.
	forwarding *forwarding
	// controls hears which attempts' controls changed: the worker API's
	// server while ServeWorkers serves it, or nil.
	controls controlNotifier
	// members are the members attempts were placed on, as placedMember
	// found them, by attempt, for a while.
	members map[string]memberEntry
	// seenFailed are the failures to reach a member, in all, that a job's
	// latest look saw its driver count, by the job and the member
	// (probeCounted), until the job is placed, placing it fails, or it
	// ends (forgetSeen).
	seenFailed map[string]int
	// timeouts are the jobs' timeouts by attempt, and private runs'
	// timeouts by "run" and the run's ID, as the coordinator read them
	// (jobTimeout, runTimeout).
	timeouts map[string]time.Duration
	// outlives says, by the run's ID and the job's key, whether a job of a
	// stopped run outlives the stop, as the coordinator read it
	// (jobOutlives).
	outlives map[string]bool
	// kept are where the attempts this coordinator recorded the
	// placements of were placed, by attempt, until they end, and keptOrder
	// the attempts in the order it kept them; forgetting counts the calls
	// that have their backends forget them, which run with forgetCtx,
	// while Serve runs (forgetLater).
	kept       map[string]keptPlace
	keptOrder  []string
	forgetting sync.WaitGroup
	forgetCtx  context.Context
}

// checkFIPS is fipsmode.CheckProcess, which a test replaces.
var checkFIPS = fipsmode.CheckProcess

// Open opens a coordinator on the store at opts.Database. It works no
// run's jobs until Serve. It refuses to open unless the process's
// cryptography runs in FIPS 140-3 mode, enforced, with the validated
// module (fipsmode), whatever program it runs in, as worker.Run does.
func Open(ctx context.Context, opts Options) (*Coordinator, error) {
	if err := checkFIPS(); err != nil {
		return nil, err
	}
	if opts.Keys == nil && !opts.ReadOnly {
		return nil, errors.New("coordinator: no Keys")
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	timing, err := openTiming()
	if err != nil {
		return nil, err
	}
	// The store asks c, as it stores a failed step's events, whether the
	// try was the step's last (lastTry), and whether the job's retry: lists
	// a reason (retriesFor), and has it stop the children of the attempts
	// it ends (attemptsEnded), once c is filled in below; a reset of the
	// store's clock wakes the supervisors c knows of, once it is open;
	// and a change to attempts' controls answers the polls c's server of
	// the worker API holds of them (controlsChanged).
	c := &Coordinator{}
	var opened atomic.Pointer[Coordinator]
	st, err := store.Open(ctx, opts.Database, &store.Options{
		Log: log, LastTry: c.lastTry, RetriesFor: c.retriesFor, ReadOnly: opts.ReadOnly, Now: opts.now, MaxLead: timing.Grace,
		OnAttemptsEnded: c.attemptsEnded,
		OnClockReset: func(ctx context.Context) {
			if c := opened.Load(); c != nil {
				c.wakeReadied(ctx)
			}
		},
		OnControlsChanged: func(_ context.Context, ids []string) {
			if c := opened.Load(); c != nil {
				c.controlsChanged(ids)
			}
		},
		OnPlacementsRecorded: func(_ context.Context, ids []string) {
			if c := opened.Load(); c != nil {
				c.placementsRecorded(ids)
			}
		},
		OnRunsEnded: func(_ context.Context, ids []string) {
			if c := opened.Load(); c != nil {
				c.runsEnded(ids)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	*c = Coordinator{
		opts: opts, log: log, store: st, plans: newPlanCache(planCacheCount, planCacheBytes),
		timing: timing, retry: defaultStartRetry, supervision: defaultSupervision,
		startTimeout: 5 * time.Minute, pollInterval: time.Second, stopTimeout: 5 * time.Second, followEvery: defaultFollowEvery, followHold: defaultFollowHold,
		queues: map[string]func(){}, connections: map[string]connection{}, members: map[string]memberEntry{}, seenFailed: map[string]int{}, kept: map[string]keptPlace{}, timeouts: map[string]time.Duration{},
		outlives: map[string]bool{}, checkoutWaits: checkoutWaits,
	}
	if opts.ReadOnly {
		// Neither seals nor inserts anything.
		return c, nil
	}
	if c.sealer, err = seal.New(opts.Keys, st.ID()); err != nil {
		return nil, errors.Join(err, st.Close())
	}
	if c.inserter, err = hopper.NewClient(st.Driver(), &hopper.Config{Logger: log}); err != nil {
		return nil, errors.Join(err, st.Close())
	}
	opened.Store(c)
	return c, nil
}

// retriesFor reports, in tx, whether the retry: of the attempt's job lists
// reason, as the run's plan says (plan.Job.RetriesFor): what the store
// asks before it marks the run as one a job of which failed, for an end
// of an attempt, or a step's failure, that another attempt may follow.
func (c *Coordinator) retriesFor(tx *store.Tx, attempt string, reason plan.Reason) bool {
	a, err := tx.Attempt(attempt)
	if err != nil {
		return false
	}
	run, err := tx.Run(a.Run)
	if err != nil {
		return false
	}
	job, err := tx.Job(a.Run, a.Job)
	if err != nil {
		return false
	}
	p, err := c.plans.get(run)
	if err != nil {
		return false
	}
	pj := p.Job(job.Name)
	return pj != nil && pj.RetriesFor(reason)
}

// lastTry reports, in tx, whether try of the step whose index is step is
// surely the last of the attempt's job's step, as the run's plan holds its
// retry: written out (plan.Step.LastTry): what the store asks as it stores
// a failed step's events, of an attempt claimed at version 1 of the worker
// API, whose worker doesn't say, so that the step marks the run at once.
func (c *Coordinator) lastTry(tx *store.Tx, attempt string, step, try int) bool {
	a, err := tx.Attempt(attempt)
	if err != nil {
		return false
	}
	run, err := tx.Run(a.Run)
	if err != nil {
		return false
	}
	job, err := tx.Job(a.Run, a.Job)
	if err != nil {
		return false
	}
	p, err := c.plans.get(run)
	if err != nil {
		return false
	}
	pj := p.Job(job.Name)
	if pj == nil || step < 0 || step >= len(pj.Steps) {
		return false
	}
	last, known := pj.Steps[step].LastTry(try)
	return known && last
}

// Close closes what members' drivers set up for their workers to reach the
// coordinator, and the drivers that are io.Closers, releases its holds on
// runs' Starts, and closes the coordinator's store. Serve, ServeWorkers
// and ServeMembers must have returned.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	var errs []error
	for _, key := range slices.Sorted(maps.Keys(c.connections)) {
		if closer := c.connections[key].closer; closer != nil {
			errs = append(errs, closer.Close())
		}
	}
	clear(c.connections)
	for _, name := range slices.Sorted(maps.Keys(c.opts.Drivers)) {
		if closer, ok := c.opts.Drivers[name].(io.Closer); ok {
			errs = append(errs, closer.Close())
		}
	}
	for _, release := range c.queues {
		if release != nil {
			release()
		}
	}
	clear(c.queues)
	c.mu.Unlock()
	return errors.Join(append(errs, c.store.Close())...)
}

// CreateRun stores a run of what Create gave, and returns its ID, the
// next number in the store. One transaction stores the run with its plan,
// its secret record, sealed under a data key of its own that Keys wraps,
// its jobs, and its hopper workflow, so that either all of it is stored or
// none of it is: Start, which runs once a coordinator works its queue; a
// marker for each job needed on any instance of it (needs: <job>:any),
// which waits for one; each job's supervisor, after Start and its needs'
// supervisors, or their markers; and the finisher, which ends the run. A
// default group that Groups doesn't have is a problem of kind
// group_unknown. An error once the run is stored, as one that keeps the
// coordinator from working its start queue, comes with the run's ID.
func (c *Coordinator) CreateRun(ctx context.Context, created *compile.Created, opts RunOptions) (string, error) {
	if err := c.writable(); err != nil {
		return "", err
	}
	switch {
	case created == nil || created.Plan == nil:
		return "", errors.New("coordinator: no plan to run")
	case opts.DefaultGroup == "":
		return "", errors.New("coordinator: a run without a default group")
	case opts.RunDir == nil:
		return "", errors.New("coordinator: a run without a directory")
	}
	if _, err := jobOrder(created.Plan); err != nil {
		return "", err
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
	var (
		id, queue string
		release   func()
	)
	err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		if queue = c.startQueue(id); queue != sharedQueue {
			// The run's Start is held as the run is stored, so that no
			// other coordinator that takes the run up (Resume) works
			// its start queue while this one lives. The number of a run
			// that isn't stored, which the store gives again, is let go.
			if release != nil {
				release()
			}
			if release, err = c.store.HoldStart(id); err != nil {
				return fmt.Errorf("coordinator: hold run %s's Start: %w", id, err)
			}
		}
		return c.insertRun(ctx, tx, id, created, queue, opts.DefaultGroup, opts.RunDir(id), nil)
	})
	if err != nil {
		if release != nil {
			release()
		}
		return "", err
	}
	if err := c.workQueue(ctx, queue, release); err != nil {
		return id, fmt.Errorf("coordinator: work run %s's start queue: %w", id, err)
	}
	return id, nil
}

// insertRun stores, in tx, the run id of what Create gave, as CreateRun
// says: with its plan, its secret record, sealed under a data key of its
// own that Keys wraps, the texts of the workflows its tree pins, its jobs,
// and its hopper workflow, whose Start goes in queue; with group, its
// default group, and dir, its directory; and for a child run, link, its
// link to the try of the step that called it.
func (c *Coordinator) insertRun(ctx context.Context, tx *store.Tx, id string, created *compile.Created, queue, group, dir string, link *store.Link) error {
	jobs, err := jobOrder(created.Plan)
	if err != nil {
		return err
	}
	// Keys wraps the record's data key while the transaction holds the
	// store's write lock, so every other write waits for it, workers'
	// heartbeats among them: a local key file is quick to read, and a key
	// service slow to answer holds them all up.
	sealed, gen, err := c.sealer.Seal(ctx, id, &created.Record)
	if err != nil {
		return err
	}
	wf := hopper.NewWorkflow("meridian run "+id, nil)
	start := wf.Add(startArgs{Run: id}, &hopper.StepOpts{Insert: &hopper.InsertOpts{Queue: queue, MaxAttempts: c.retry.attempts}})
	// A job needed on any instance of it has a marker, which waits for
	// one, and which the jobs that need it so wait for in its place.
	anys := anyNeeded(jobs)
	marker := map[string]hopper.Step{}
	for _, name := range anys {
		marker[name] = wf.Add(anyArgs{Run: id, Job: name}, &hopper.StepOpts{
			After: []hopper.Step{start}, OnDependencyFailure: hopper.DependencyIgnore,
			Insert: &hopper.InsertOpts{Queue: sharedQueue, MaxAttempts: c.retry.attempts},
		})
	}
	// Each job's supervisor runs after Start and its needs' supervisors,
	// whatever became of them: Ready decides what their outcomes mean.
	after := map[string]hopper.Step{}
	for _, j := range jobs {
		deps := []hopper.Step{start}
		for _, n := range j.Needs {
			if n.Any {
				deps = append(deps, marker[n.Job])
			} else {
				deps = append(deps, after[n.Job])
			}
		}
		after[j.Name] = wf.Add(jobArgs{Run: id, Key: j.Name}, &hopper.StepOpts{
			After: deps, OnDependencyFailure: hopper.DependencyIgnore,
			Insert: &hopper.InsertOpts{Queue: sharedQueue, MaxAttempts: c.retry.attempts},
		})
	}
	deps := []hopper.Step{start}
	for _, name := range anys {
		deps = append(deps, marker[name])
	}
	for _, j := range jobs {
		deps = append(deps, after[j.Name])
	}
	wf.Add(finishArgs{Run: id}, &hopper.StepOpts{
		After: deps, OnDependencyFailure: hopper.DependencyIgnore,
		Insert: &hopper.InsertOpts{Queue: sharedQueue, Await: true},
	})
	res, err := c.inserter.InsertWorkflowTx(ctx, tx.SQL(), wf)
	if err != nil {
		return err
	}
	// The finisher is the workflow's last job, which the end of a run of a
	// root's tree wakes once the root's waits for them.
	if err := tx.InsertRun(store.NewRun{
		ID: id, Plan: created.Plan, DefaultGroup: group, RunDir: dir, WorkflowID: res.ID,
		FinisherID: res.Jobs[len(res.Jobs)-1].Job.ID, Parent: link,
	}); err != nil {
		return err
	}
	// The texts of the workflows the run's tree pins are kept with it,
	// which its child runs are created from.
	if err := tx.KeepTexts(id, created.Texts); err != nil {
		return err
	}
	// The workflow's jobs are in the order of its steps: Start, the
	// markers, then the supervisors.
	for i, name := range anys {
		if err := tx.InsertAny(id, name, res.Jobs[1+i].Job.ID); err != nil {
			return err
		}
	}
	for i, j := range jobs {
		if err := tx.InsertJob(store.NewJob{Run: id, Name: j.Name, Key: j.Name, SupervisorID: res.Jobs[1+len(anys)+i].Job.ID}); err != nil {
			return err
		}
	}
	return tx.InsertRecord(id, gen, sealed.Key, sealed.Record)
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
// runs, and once it next does otherwise, as when it is stopping, holding
// the run's Start, unless release, which lets the hold go, is nil, until it
// stops working q (retireQueue), or closes.
func (c *Coordinator) workQueue(ctx context.Context, q string, release func()) error {
	if q == sharedQueue {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.queues[q]; ok {
		if release != nil {
			release()
		}
		return nil
	}
	c.queues[q] = release
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
	release, ok := c.queues[q]
	if !ok {
		return
	}
	delete(c.queues, q)
	if release != nil {
		release()
	}
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
	if err := c.writable(); err != nil {
		return err
	}
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
	hopper.AddWorker(workers, &jobWorker{c: c})
	hopper.AddWorker(workers, &anyWorker{c: c})
	hopper.AddWorker(workers, &finishWorker{c: c})
	hopper.AddWorker(workers, &timeoutWorker{c: c})
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
	forgetCtx, stopForget := context.WithCancel(context.WithoutCancel(ctx))
	c.served, c.retiring, c.forgetCtx = served, retiring, forgetCtx
	c.mu.Unlock()
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		c.watchWakes(ctx, time.Second)
	}()

	<-ctx.Done()
	<-watched
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
	// The workers of the attempts that ended last exit soon after.
	grace, cancelGrace := context.WithTimeout(context.WithoutCancel(ctx), forgetGrace)
	c.stopForgetting(grace, stopForget)
	cancelGrace()
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
