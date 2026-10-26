package coordinator

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
)

// testKeys wraps data keys under a key of its own, with AES-256-GCM, bound
// to the run's ID, as a key file does; and fails as a test says.
type testKeys struct {
	aead cipher.AEAD

	mu                 sync.Mutex
	wrapErr, unwrapErr error
}

func newTestKeys(t *testing.T) *testKeys {
	t.Helper()
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		t.Fatal(err)
	}
	return &testKeys{aead: aead}
}

func (k *testKeys) Wrap(_ context.Context, runID string, key []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.wrapErr != nil {
		return nil, k.wrapErr
	}
	return k.aead.Seal(nil, nil, key, []byte(runID)), nil
}

func (k *testKeys) Unwrap(_ context.Context, runID string, wrapped []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.unwrapErr != nil {
		return nil, k.unwrapErr
	}
	return k.aead.Open(nil, nil, wrapped, []byte(runID))
}

func (k *testKeys) fail(wrap, unwrap error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.wrapErr, k.unwrapErr = wrap, unwrap
}

// privateDir returns a directory of the test's that only its user can use,
// as the store asks of its database's.
func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// fastStart is how tests try again to start a run: soon, and few times.
var fastStart = startRetry{tries: 3, first: 10 * time.Millisecond, most: 40 * time.Millisecond, newer: 50 * time.Millisecond, attempts: 2}

// open opens a coordinator on a new store in a directory of the test's,
// with opts, Keys of the test's own unless opts has some, and the timing
// of a test.
func open(t *testing.T, opts Options) *Coordinator {
	t.Helper()
	if opts.Database == "" {
		opts.Database = filepath.Join(privateDir(t), "meridian.db")
	}
	if opts.Keys == nil {
		opts.Keys = newTestKeys(t)
	}
	c, err := Open(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c.retry, c.pollInterval = fastStart, 10*time.Millisecond
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

// serve runs c.Serve until the test ends.
func serve(t *testing.T, c *Coordinator) {
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
func create(t *testing.T, c *Coordinator, workflow string, submitted map[string]any, vars conformancetest.Variables) (string, *compile.Created) {
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
func waitEnded(t *testing.T, c *Coordinator, id string) *store.Run {
	t.Helper()
	r := storedRun(t, c, id)
	wf, err := c.inserter.WorkflowGet(t.Context(), r.WorkflowID)
	if err != nil {
		t.Fatal(err)
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
	if _, err := hopper.Await[any](ctx, served, wf.Jobs[len(wf.Jobs)-1].ID); err != nil {
		t.Fatal(err)
	}
	return storedRun(t, c, id)
}

// storedRun returns the run id, as the store holds it.
func storedRun(t *testing.T, c *Coordinator, id string) *store.Run {
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
