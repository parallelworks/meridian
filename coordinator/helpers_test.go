package coordinator

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/coordtest"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// parallelTests bounds how many of the package's tests that run in
// parallel run at once, beside go test's -parallel. Each has a store of its
// own, but every write to one is synced to disk, which they all share:
// under the race detector, four at a time run the package in under two
// thirds of the time one at a time does, on 4 cores as on 30, while thirty
// at a time ran it no faster, and each test so much slower that its waits
// for what it expects ran out.
var parallelTests = make(chan struct{}, 4)

// parallel has the test t run in parallel with the package's other tests
// that do, as many at a time as parallelTests allows. A test whose subtests
// run in parallel calls t.Parallel itself, and has its subtests call this:
// a test that held a place while its subtests waited for theirs would hold
// one they need.
func parallel(t *testing.T) {
	t.Helper()
	t.Parallel()
	parallelTests <- struct{}{}
	t.Cleanup(func() { <-parallelTests })
}

// fastStart is how tests try again to start a run: soon, and few times.
var fastStart = startRetry{tries: 3, first: 10 * time.Millisecond, most: 40 * time.Millisecond, newer: 50 * time.Millisecond, attempts: 2}

// fastSupervision is how tests' supervisors wait: briefly, but for a live
// attempt, which they wait on until the attempt's worker wakes them, as
// the worker API does.
var fastSupervision = supervision{
	setup: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond},
	first: 10 * time.Millisecond, most: 40 * time.Millisecond, tries: 3,
	wait: 20 * time.Second, timeout: 30 * time.Second,
}

// testPollInterval is how often a test's idle hopper producers look for
// work that no wake told them of, as what the coordinator's inserter adds,
// or a wake that comes due: soon enough for a test not to wait long on
// one, and seldom enough that the polls of the tests that run at once,
// each a statement that the race detector makes costly, don't take much
// of the CPU they share. At 10ms they took a tenth of the package's.
const testPollInterval = 50 * time.Millisecond

// storeTemplate is the store that each test's new store is a copy of
// (coordtest.StoreTemplate). The store package's own tests have one of
// their own.
var storeTemplate = coordtest.StoreTemplate{Make: func(ctx context.Context, path string) error {
	s, err := store.Open(ctx, path, nil)
	if err != nil {
		return err
	}
	return s.Close()
}}

// TestMain runs the package's tests, and removes the store template, if a
// test made it.
func TestMain(m *testing.M) {
	code := m.Run()
	storeTemplate.Remove()
	os.Exit(code)
}

// open opens a coordinator, with opts, on the store opts.Database names, or
// on a new one in a directory of the test's: a new store is a copy of the
// store template. It has Keys of the test's own unless opts has some,
// testGroups and a testDriver of testBackend unless it has Groups and
// Drivers, and the timing of a test.
func open(t testing.TB, opts Options) *Coordinator {
	t.Helper()
	if opts.Database == "" {
		opts.Database = filepath.Join(coordtest.PrivateDir(t), "meridian.db")
	}
	if _, err := os.Lstat(opts.Database); errors.Is(err, fs.ErrNotExist) {
		storeTemplate.Copy(t, opts.Database)
	}
	if opts.Keys == nil {
		opts.Keys = coordtest.NewKeys(t)
	}
	if opts.Groups == nil {
		opts.Groups = testGroups{}
	}
	var drivers []*testDriver
	if opts.Drivers == nil {
		d := newTestDriver()
		opts.Drivers = map[string]backend.Driver{testBackend: d}
	}
	for _, d := range opts.Drivers {
		if d, ok := d.(*testDriver); ok {
			drivers = append(drivers, d)
		}
	}
	c, err := Open(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c.retry, c.supervision, c.pollInterval = fastStart, fastSupervision, testPollInterval
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, d := range drivers {
		d.c, d.ctx = c, t.Context()
		// Before the store closes, the driver's workers have finished.
		t.Cleanup(d.workers.Wait)
	}
	return c
}

// wakeJob wakes the supervisor of the run's job key in a transaction of
// its own, as a test wakes it, and again while hopper runs it, as the
// coordinator wakes one in the transaction that changes what it waits for
// (wakeJobTx).
func (c *Coordinator) wakeJob(ctx context.Context, run, key string) {
	var w *waking
	if err := c.store.Tx(ctx, func(tx *store.Tx) error {
		var err error
		w, err = c.wakeJobTx(ctx, tx, run, key)
		return err
	}); err != nil {
		c.log.DebugContext(ctx, "coordinator: wake a job's supervisor", "run", run, "job", key, "error", err)
		return
	}
	c.wakeAgain(ctx, w)
}

// driverOf returns c's testDriver.
func driverOf(c *Coordinator) *testDriver {
	return c.opts.Drivers[testBackend].(*testDriver)
}

// serve runs c.Serve until the test ends.
func serve(t testing.TB, c *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
}

// runOptions are the run options of a test's runs.
var runOptions = RunOptions{DefaultGroup: "local", RunDir: func(id string) string { return "/runs/" + id }}

// create compiles workflow with submitted, as Create does, reading vars,
// and stores the run, whose ID it returns, with what Create gave.
func create(t testing.TB, c *Coordinator, workflow string, submitted map[string]any, vars conformancetest.Variables) (string, *compile.Created) {
	t.Helper()
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(workflow), submitted,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Contexts: vars.Contexts()})
	if err != nil {
		t.Fatal(err)
	}
	if diags.HasErrors() {
		t.Fatalf("the workflow has errors: %v", diags)
	}
	id, err := c.CreateRun(t.Context(), created, runOptions)
	if err != nil {
		t.Fatal(err)
	}
	return id, created
}

// waitEnded waits for the run id's finisher to finish, as c serves, and
// returns the run.
func waitEnded(t testing.TB, c *Coordinator, id string) *store.Run {
	t.Helper()
	r := storedRun(t, c, id)
	wf, err := c.inserter.WorkflowGet(t.Context(), r.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	// The finisher, by its kind: a job of the workflow that a wake has
	// hopper run again, as a supervisor or a marker of a need on any
	// instance, comes after it in the workflow's order.
	i := slices.IndexFunc(wf.Jobs, func(j *hopper.JobRow) bool { return j.Kind == finishArgs{}.Kind() })
	if i < 0 {
		t.Fatalf("run %s's workflow has no finisher", id)
	}
	// The serving client hears the finisher finish at once, where the
	// inserter, which isn't started, polls.
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.served != nil
	})
	c.mu.Lock()
	served := c.served
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := hopper.Await[any](ctx, served, wf.Jobs[i].ID); err != nil {
		t.Fatal(err)
	}
	return storedRun(t, c, id)
}

// storedRun returns the run id, as the store holds it.
func storedRun(t testing.TB, c *Coordinator, id string) *store.Run {
	t.Helper()
	var r *store.Run
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		r, err = tx.Run(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

// storedRecord returns the run id's secret record, opened, or nil for none.
func storedRecord(t *testing.T, c *Coordinator, id string) *plan.Record {
	t.Helper()
	var stored *store.Record
	err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		stored, err = tx.Record(id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	rec, err := c.sealer.Open(t.Context(), id, stored.Generation, sealedOf(stored))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// logRecords is a slog.Handler that keeps what it hears.
type logRecords struct {
	mu      sync.Mutex
	records []string
}

func (h *logRecords) Enabled(context.Context, slog.Level) bool { return true }
func (h *logRecords) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" " + a.String())
		return true
	})
	h.records = append(h.records, b.String())
	return nil
}
func (h *logRecords) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logRecords) WithGroup(string) slog.Handler      { return h }

// has reports whether a record holds s.
func (h *logRecords) has(s string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r, s) {
			return true
		}
	}
	return false
}
