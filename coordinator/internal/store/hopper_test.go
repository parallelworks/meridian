package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/plan"
)

// The hopper jobs of a run, as a coordinator's workflow has them: Start, a
// supervisor for each of the run's jobs, and the finisher.
type (
	startArgs struct {
		Run string `json:"run"`
	}
	jobArgs struct {
		Run string `json:"run"`
		Key string `json:"key"`
	}
	finishArgs struct {
		Run string `json:"run"`
	}
)

func (startArgs) Kind() string  { return "store.test.start" }
func (jobArgs) Kind() string    { return "store.test.job" }
func (finishArgs) Kind() string { return "store.test.run" }

// newClient returns a hopper client on s's database, which inserts jobs,
// and works them with workers if workers isn't nil.
func newClient(t *testing.T, s *Store, workers *hopper.Workers) *hopper.Client[*sql.Tx] {
	t.Helper()
	cfg := &hopper.Config{Workers: workers}
	if workers != nil {
		cfg.Queues = map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}}
		cfg.PollInterval = 50 * time.Millisecond
	}
	c, err := hopper.NewClient(s.Driver(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// createRunWorkflow stores a run of p as a coordinator does, in one
// transaction: its number, the run, its hopper workflow with a supervisor
// for each job, its jobs and its secret record. Once all of it is done,
// it calls last, if it isn't nil, whose error is the transaction's.
func createRunWorkflow(ctx context.Context, s *Store, c *hopper.Client[*sql.Tx], p *plan.Plan, last func() error) (string, error) {
	var id string
	err := s.Tx(ctx, func(tx *Tx) error {
		var err error
		if id, err = tx.NewRunID(); err != nil {
			return err
		}
		wf := hopper.NewWorkflow("run "+id, nil)
		start := wf.Add(startArgs{Run: id}, nil)
		// A step comes after those it waits for, and the plan's jobs are
		// in the order of their names, so each is added once those it
		// needs are.
		steps := map[string]hopper.Step{}
		var all []hopper.Step
		var order []string
		for len(order) < len(p.Jobs) {
			for _, j := range p.Jobs {
				if _, ok := steps[j.Name]; ok {
					continue
				}
				after := []hopper.Step{start}
				for _, need := range j.Needs {
					if _, ok := steps[need.Job]; !ok {
						after = nil
						break
					}
					after = append(after, steps[need.Job])
				}
				if after == nil {
					continue
				}
				steps[j.Name] = wf.Add(jobArgs{Run: id, Key: j.Name}, &hopper.StepOpts{After: after, OnDependencyFailure: hopper.DependencyIgnore})
				all = append(all, steps[j.Name])
				order = append(order, j.Name)
			}
		}
		wf.Add(finishArgs{Run: id}, &hopper.StepOpts{After: all, OnDependencyFailure: hopper.DependencyIgnore})
		res, err := c.InsertWorkflowTx(ctx, tx.SQL(), wf)
		if err != nil {
			return err
		}
		byName := map[string]hopper.JobID{}
		for i, name := range order {
			byName[name] = res.Jobs[i+1].Job.ID
		}
		supervisors := make([]hopper.JobID, len(p.Jobs))
		for i, j := range p.Jobs {
			supervisors[i] = byName[j.Name]
		}
		if err := insertRun(tx, id, p, res.ID, supervisors); err != nil {
			return err
		}
		if last != nil {
			return last()
		}
		return nil
	})
	return id, err
}

// hopperJobs counts the hopper jobs in s's database, live and finished.
func hopperJobs(t *testing.T, c *hopper.Client[*sql.Tx]) int {
	t.Helper()
	n := 0
	for _, err := range c.Jobs(t.Context(), hopper.JobFilter{}) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	return n
}

// TestCreateRun checks that a run, its hopper workflow and its secret
// record are stored in one transaction, or none of them is.
func TestCreateRun(t *testing.T) {
	s := open(t)
	c := newClient(t, s, nil)
	p := testPlan(t)

	boom := errors.New("boom")
	if _, err := createRunWorkflow(t.Context(), s, c, p, func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("the transaction failed with %v, want boom", err)
	}
	if n := hopperJobs(t, c); n != 0 {
		t.Errorf("a run rolled back left %d hopper jobs", n)
	}
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.Run("1"); !errors.Is(err, ErrNotFound) {
			t.Errorf("a run rolled back is there: %v", err)
		}
		return nil
	})

	id, err := createRunWorkflow(t.Context(), s, c, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if id != "1" {
		t.Errorf("the run is %s, want 1, as the one rolled back gave its number back", id)
	}
	inTx(t, s, func(tx *Tx) error {
		r, err := tx.Run(id)
		if err != nil {
			return err
		}
		wf, err := c.WorkflowGet(t.Context(), r.WorkflowID)
		if err != nil {
			return err
		}
		if len(wf.Jobs) != len(p.Jobs)+2 {
			t.Errorf("the run's workflow has %d jobs, want %d", len(wf.Jobs), len(p.Jobs)+2)
		}
		jobs, err := tx.Jobs(id)
		if err != nil {
			return err
		}
		for _, j := range jobs {
			row, err := c.JobGet(t.Context(), j.SupervisorID)
			if err != nil {
				return err
			}
			if args := string(row.Args); args != `{"run":"1","key":"`+j.Key+`"}` {
				t.Errorf("job %s's supervisor has the args %s", j.Key, args)
			}
		}
		return nil
	})
	checkIntegrity(t, s)
}

// supervisor is a worker of jobArgs that ends its job in Meridian's tables
// and completes its hopper job in one transaction, as the coordinator's
// supervisors do. Its first attempt fails once both are done, so neither
// is stored, and hopper tries again.
type supervisor struct {
	hopper.WorkerDefaults[jobArgs]
	s *Store
}

func (w *supervisor) Work(ctx context.Context, job *hopper.Job[jobArgs]) error {
	err := w.s.Tx(ctx, func(tx *Tx) error {
		result := plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"attempt": string(rune('0' + job.Attempt))}}
		if err := tx.EndJob(job.Args.Run, job.Args.Key, JobEnd{Result: result}); err != nil {
			return err
		}
		if err := hopper.CompleteTx(ctx, tx.SQL(), job); err != nil {
			return err
		}
		if job.Attempt == 1 {
			return errors.New("the first attempt fails")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// NextRetry tries again at once.
func (w *supervisor) NextRetry(*hopper.Job[jobArgs]) time.Time { return time.Now() }

// TestCompleteTx checks that a hopper job's completion and what its work
// stores in Meridian's tables commit together, or neither does.
func TestCompleteTx(t *testing.T) {
	s := open(t)
	id := createRun(t, s)
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, &supervisor{s: s})
	c := newClient(t, s, workers)
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	res, err := c.Insert(t.Context(), jobArgs{Run: id, Key: "build"}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := hopper.Await[any](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	row, err := c.JobGet(t.Context(), res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != hopper.JobStateCompleted || row.Attempt != 2 {
		t.Errorf("the hopper job is %s after %d attempts, want completed after 2", row.State, row.Attempt)
	}
	inTx(t, s, func(tx *Tx) error {
		j, err := tx.Job(id, "build")
		if err != nil {
			return err
		}
		// The first attempt's end was rolled back with its completion.
		if j.Outcome != plan.OutcomeSuccess || j.Result.Outputs["attempt"] != "2" {
			t.Errorf("the job ended with %+v", j.Result)
		}
		return nil
	})
}
