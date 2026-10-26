package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// blockingVariables blocks each read until its context ends, while block
// is set, and otherwise gives vars; asked says each read began.
type blockingVariables struct {
	vars  conformancetest.Variables
	block atomic.Bool
	asked chan struct{}
}

func (v *blockingVariables) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	if v.block.Load() {
		select {
		case v.asked <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return v.vars.Read(ctx, names, secrets)
}

// serving runs c.Serve until the function it returns is called, which
// waits for it to return, and says how long that took.
func serving(t *testing.T, c *Coordinator) (stop func() time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.Serve(ctx) }()
	var once sync.Once
	stop = func() (took time.Duration) {
		once.Do(func() {
			start := time.Now()
			cancel()
			if err := <-served; err != nil {
				t.Error(err)
			}
			took = time.Since(start)
		})
		return took
	}
	t.Cleanup(func() { stop() })
	return stop
}

// TestStopDuringStart checks that a coordinator that stops while it starts
// a run, again and again, more often than hopper's attempts at Start,
// neither uses up those attempts nor counts a try at the run: the run
// waits, and starts once a coordinator serves to the end. And that it
// stops promptly.
func TestStopDuringStart(t *testing.T) {
	vars := &blockingVariables{vars: regionVars, asked: make(chan struct{}, 1)}
	vars.block.Store(true)
	c := open(t, Options{Variables: vars})
	c.stopTimeout = 100 * time.Millisecond
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	for range fastStart.attempts + 2 {
		stop := serving(t, c)
		select {
		case <-vars.asked:
		case <-time.After(10 * time.Second):
			t.Fatalf("the coordinator didn't try to start the run again: it is %s", storedRun(t, c, id).State)
		}
		if took := stop(); took > 5*time.Second {
			t.Errorf("Serve took %v to stop", took)
		}
		if r := storedRun(t, c, id); r.State != store.RunCreated || r.StartTries != 0 || len(r.Diagnostics) != 0 {
			t.Fatalf("once the coordinator stopped, the run is %s, after %d tries, with %v", r.State, r.StartTries, r.Diagnostics)
		}
	}
	vars.block.Store(false)
	serving(t, c)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeSuccess || r.StartTries != 0 {
		t.Errorf("the run ended with %s, after %d tries", r.Outcome, r.StartTries)
	}
}

// waitServed waits until c serves.
func waitServed(t *testing.T, c *Coordinator) {
	t.Helper()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.served != nil
	})
}

// TestServeAgain checks that Serve, once it has returned, serves again.
func TestServeAgain(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	stop := serving(t, c)
	waitServed(t, c)
	stop()
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	serving(t, c)
	if r := waitEnded(t, c, id); r.Outcome != plan.OutcomeSuccess {
		t.Errorf("the run ended with %s", r.Outcome)
	}
}

// errSecret is an error whose text holds a secret, as a resolver's or
// Keys' can quote what it was given.
var errSecret = errors.New("the store says tok-SECRET-VALUE-123 isn't valid")

// erringVariables fails each read with err.
type erringVariables struct{ err error }

func (v erringVariables) Read(context.Context, []string, bool) (map[string]compile.Variable, error) {
	return nil, v.err
}

// TestErrorsNotLogged checks that what a resolver's or Keys' error says,
// which can quote a secret, never reaches the coordinator's log, which
// says only which kind of try failed.
func TestErrorsNotLogged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		vars  compile.Variables
		keys  func(*testKeys)
		cause string
	}{
		{"variables", erringVariables{errSecret}, nil, causeResolver},
		{"keys", regionVars, func(k *testKeys) { k.fail(nil, errSecret) }, causeKeys},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &logRecords{}
			keys := newTestKeys(t)
			c := open(t, Options{Variables: tc.vars, Keys: keys, Log: slog.New(log)})
			id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
			if tc.keys != nil {
				tc.keys(keys)
			}
			serving(t, c)
			if r := waitEnded(t, c, id); !hasCode(r.Diagnostics, problems.RunStartUnanswered) {
				t.Errorf("the run ended with %v", r.Diagnostics)
			}
			if log.has("tok-SECRET") {
				t.Errorf("the log has the error's text: %v", log.records)
			}
			if !log.has("cause=" + tc.cause) {
				t.Errorf("the log doesn't say the cause is %s: %v", tc.cause, log.records)
			}
		})
	}
}

// hasCode reports whether diags has one of kind k.
func hasCode(diags problems.Diagnostics, k *problems.Kind) bool {
	for _, d := range diags {
		if d.Code == k.Code() {
			return true
		}
	}
	return false
}

// TestStartRacesEnd checks that a Start that stores what it gave and one
// that fails the run, at once, as a Start rescued from a replica still
// running it and its new attempt, settle the run one way: one of them
// stores, and the other finds the run settled.
func TestStartRacesEnd(t *testing.T) {
	for range 5 {
		c := open(t, Options{Variables: regionVars})
		id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
		failed := problems.Diagnostics{diagnostic(problems.RunRecordMismatch, problems.Params{"run": id})}
		var startErr, endErr error
		var wg sync.WaitGroup
		wg.Go(func() { _, startErr = c.startRun(t.Context(), id, nil) })
		wg.Go(func() { endErr = c.endStart(t.Context(), id, failed, nil) })
		wg.Wait()
		r := storedRun(t, c, id)
		switch {
		case startErr == nil && errors.Is(endErr, errSettled):
			if r.State != store.RunRunning || r.RecordGeneration != 2 {
				t.Errorf("Start stored, and the run is %s, record generation %d", r.State, r.RecordGeneration)
			}
		case endErr == nil && errors.Is(startErr, errSettled):
			if r.State != store.RunEnded || r.Outcome != plan.OutcomeFailure || storedRecord(t, c, id) != nil {
				t.Errorf("the run failed, and is %s, %s", r.State, r.Outcome)
			}
		default:
			t.Errorf("Start gave %v, and the failure %v", startErr, endErr)
		}
	}
}

// TestDiscardedStartQueue checks that the start queue of a run whose Start
// hopper gave up on stops being worked once the run's finisher ends it.
func TestDiscardedStartQueue(t *testing.T) {
	c := open(t, Options{Variables: regionVars, StartQueue: func(id string) string { return "meridian.start." + id }})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	exec(t, c, "DELETE FROM meridian_records")
	serving(t, c)
	if r := waitEnded(t, c, id); !hasCode(r.Diagnostics, problems.RunNotStarted) {
		t.Fatalf("the run ended with %v", r.Diagnostics)
	}
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.queues) == 0
	})
}

// TestFinishNewer checks that the finisher leaves a run whose plan only a
// newer release reads to one, as Start does.
func TestFinishNewer(t *testing.T) {
	log := &logRecords{}
	c := open(t, Options{Variables: regionVars, Log: slog.New(log)})
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	exec(t, c, "DROP TRIGGER meridian_runs_plan_fixed")
	exec(t, c, "UPDATE meridian_runs SET plan_version = 2")
	noop := func(context.Context, *store.Tx) error { return errors.New("the job isn't completed") }
	snooze, err := c.finishRun(t.Context(), id, noop)
	if err != nil || snooze != fastStart.newer {
		t.Errorf("finishRun of a newer run = %v, %v", snooze, err)
	}
	if !log.has(string(problems.RunPlanNewer.Code())) {
		t.Errorf("the log doesn't say so: %v", log.records)
	}
	// A start state of a newer version is a newer release's too.
	exec(t, c, `UPDATE meridian_runs SET plan_version = 1, start_state = CAST('{"version":2,"run":{"format":1}}' AS BLOB)`)
	if snooze, err := c.finishRun(t.Context(), id, noop); err != nil || snooze != fastStart.newer {
		t.Errorf("finishRun of a newer start state = %v, %v", snooze, err)
	}
	// The store doesn't read such a run, so its state is read as it is.
	var state string
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		return tx.SQL().QueryRowContext(t.Context(), "SELECT state FROM meridian_runs").Scan(&state)
	}); err != nil {
		t.Fatal(err)
	}
	if state != string(store.RunRunning) {
		t.Errorf("the run is %s", state)
	}
}
