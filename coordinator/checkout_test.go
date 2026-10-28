package coordinator

import (
	"archive/tar"
	"cmp"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// pinnedCommit is the commit pinSources pins every checkout to.
const pinnedCommit = "0123456789abcdef0123456789abcdef01234567"

// pinSources pins every checkout to pinnedCommit, and has no workflows.
type pinSources struct{}

func (pinSources) Resolve(context.Context, plan.SourceRef) (*compile.Workflow, error) {
	return nil, compile.ErrNotFound
}

func (pinSources) Read(context.Context, plan.Source) (*compile.Workflow, error) {
	return nil, compile.ErrNotFound
}

func (pinSources) Pin(context.Context, string, string) (string, error) { return pinnedCommit, nil }

// A memRepo is a repository a memTrees holds: its files, by path, at any
// commit; whether it has submodules; the size and the entries its survey
// says, if they aren't 0, rather than its files'; and how many of its
// surveys fail before one succeeds, or -1 for every one.
type memRepo struct {
	files      map[string]string
	submodules bool
	size       int64
	entries    int
	failures   int
}

// memTrees is a Trees of repositories in memory, which counts the surveys
// and the writes it was asked for.
type memTrees struct {
	mu              sync.Mutex
	repos           map[string]*memRepo
	surveys, writes int
}

var _ Trees = (*memTrees)(nil)

// selected returns r's paths that paths name, or all of them, for none,
// in order, and the indexes of paths that name none.
func (r *memRepo) selected(paths []string) ([]string, []int) {
	var (
		names []string
		gone  []int
	)
	all := slices.Sorted(maps.Keys(r.files))
	if len(paths) == 0 {
		return all, nil
	}
	for i, p := range paths {
		p = strings.TrimSuffix(p, "/")
		found := false
		for _, name := range all {
			if name == p || strings.HasPrefix(name, p+"/") {
				found = true
				if !slices.Contains(names, name) {
					names = append(names, name)
				}
			}
		}
		if !found {
			gone = append(gone, i)
		}
	}
	slices.Sort(names)
	return names, gone
}

func (m *memTrees) SurveyTree(_ context.Context, repo, commit string, paths []string) (TreeSurvey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.surveys++
	r := m.repos[repo]
	switch {
	case r == nil || commit != pinnedCommit:
		return TreeSurvey{}, errors.New("no such commit")
	case r.failures != 0:
		if r.failures > 0 {
			r.failures--
		}
		return TreeSurvey{}, errors.New("the server didn't answer")
	}
	names, missing := r.selected(paths)
	survey := TreeSurvey{Missing: missing, Submodules: r.submodules, Size: r.size, Entries: cmp.Or(r.entries, len(names))}
	if survey.Size == 0 {
		for _, name := range names {
			survey.Size += int64(len(r.files[name]))
		}
	}
	return survey, nil
}

func (m *memTrees) WriteTree(_ context.Context, repo, _ string, paths []string, w io.Writer) error {
	m.mu.Lock()
	m.writes++
	r := m.repos[repo]
	m.mu.Unlock()
	names, _ := r.selected(paths)
	tw := tar.NewWriter(w)
	for _, name := range names {
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(r.files[name]))}); err != nil {
			return err
		}
		if _, err := io.WriteString(tw, r.files[name]); err != nil {
			return err
		}
	}
	return tw.Close()
}

// counts returns how many surveys and writes m was asked for.
func (m *memTrees) counts() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.surveys, m.writes
}

// createPinned stores a run of workflow, as Create compiles it with
// pinSources, and returns its ID.
func (r *workerRig) createPinned(t *testing.T, workflow string) string {
	t.Helper()
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(workflow), nil,
		compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: pinSources{}})
	if err != nil || created == nil || diags.HasErrors() {
		t.Fatalf("the workflow doesn't create: %v, %v", diags, err)
	}
	id, err := r.c.CreateRun(t.Context(), created, RunOptions{DefaultGroup: "local", RunDir: func(id string) string { return filepath.Join(r.runs, id) }})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// checkoutWorkflow has a job for each way a checkout ends: one that writes
// its sparse paths, and warns of one that names nothing; and one each
// whose repository has submodules, is too large, answers on its third
// try, never answers, whose sparse_checkout gives a path that isn't one,
// and whose repo is "".
const checkoutWorkflow = `
jobs:
  ok:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/app.git
          branch: main
          path: src
          sparse_checkout: [app, gone]
      - run: cat src/app/main.go
  submodules:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/sub.git
          branch: main
  large:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/big.git
          branch: main
  many:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/many.git
          branch: main
  flaky:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/flaky.git
          branch: main
  unavailable:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/down.git
          branch: main
  pattern:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/app.git
          branch: main
          sparse_checkout: ["${{ '*.go' }}"]
  nothing:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: ""
          branch: main
`

// TestCheckout checks a parallelworks/checkout step run by meridian's
// worker against the coordinator: its tree, of the commit Create pinned,
// is written into its path:, as its sparse_checkout names it, with a
// warning for a path that names nothing (checkout_path_not_found); and it
// fails for a whole checkout of a repository with submodules
// (checkout_submodules), one too large (checkout_too_large), one whose
// commit can't be read after 5 tries (checkout_unavailable), though one
// read on its third try succeeds, and an entry an expression gave that
// isn't a literal path (checkout_path_invalid). One whose repo is ""
// checks out nothing, as in format 1, and succeeds.
func TestCheckout(t *testing.T) {
	t.Parallel()
	trees := &memTrees{repos: map[string]*memRepo{
		"https://example.com/acme/app.git":   {files: map[string]string{"app/main.go": "package main\n", "README.md": "read me\n"}},
		"https://example.com/acme/sub.git":   {files: map[string]string{"README.md": "read me\n"}, submodules: true},
		"https://example.com/acme/big.git":   {files: map[string]string{"README.md": "read me\n"}, size: workerapi.MaxCheckoutBytes + 1},
		"https://example.com/acme/many.git":  {files: map[string]string{"README.md": "read me\n"}, entries: workerapi.MaxCheckoutEntries + 1},
		"https://example.com/acme/flaky.git": {files: map[string]string{"README.md": "read me\n"}, failures: 2},
		"https://example.com/acme/down.git":  {failures: -1},
	}}
	r := newWorkerRigOf(t, Options{Trees: trees})
	r.c.checkoutWaits = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond}
	id := r.createPinned(t, checkoutWorkflow)
	serve(t, r.c)
	waitEnded(t, r.c, id)

	for key, want := range map[string]struct {
		outcome plan.Outcome
		kind    *problems.Kind
		pointer string
	}{
		"ok":          {plan.OutcomeSuccess, problems.CheckoutPathNotFound, "/jobs/ok/steps/0/with/sparse_checkout/1"},
		"submodules":  {plan.OutcomeFailure, problems.CheckoutSubmodules, "/jobs/submodules/steps/0/with/repo"},
		"large":       {plan.OutcomeFailure, problems.CheckoutTooLarge, "/jobs/large/steps/0"},
		"many":        {plan.OutcomeFailure, problems.CheckoutTooMany, "/jobs/many/steps/0"},
		"flaky":       {plan.OutcomeSuccess, nil, ""},
		"unavailable": {plan.OutcomeFailure, problems.CheckoutUnavailable, "/jobs/unavailable/steps/0/with/repo"},
		"pattern":     {plan.OutcomeFailure, problems.CheckoutPathInvalid, "/jobs/pattern/steps/0/with/sparse_checkout/0"},
		"nothing":     {plan.OutcomeSuccess, nil, ""},
	} {
		e := finishedStep(t, r.c, id, key, 0, 0)
		switch {
		case e.Outcome != want.outcome:
			t.Errorf("%s's checkout ended %s, with %+v", key, e.Outcome, e.Diagnostics)
		case want.kind == nil && len(e.Diagnostics) != 0:
			t.Errorf("%s's checkout has %+v", key, e.Diagnostics)
		case want.kind != nil && (len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != want.kind.Code() || e.Diagnostics[0].Pointer != "#"+want.pointer):
			t.Errorf("%s's checkout has %+v, want %s at %s", key, e.Diagnostics, want.kind.Code(), want.pointer)
		}
	}
	if e := finishedStep(t, r.c, id, "ok", 1, 0); e.Outcome != plan.OutcomeSuccess {
		t.Errorf("the step after the checkout ended %s, with %+v", e.Outcome, e.Diagnostics)
	}
	src := filepath.Join(r.runs, id, "src")
	if data, err := os.ReadFile(filepath.Join(src, "app", "main.go")); err != nil || string(data) != "package main\n" {
		t.Errorf("the checkout wrote %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(src, "README.md")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the checkout wrote a path its sparse_checkout doesn't name: %v", err)
	}
	// ok, submodules, large, many, flaky three times, unavailable five
	// times, and pattern and nothing not at all; ok and flaky written.
	if surveys, writes := trees.counts(); surveys != 12 || writes != 2 {
		t.Errorf("Trees was asked for %d surveys and %d writes", surveys, writes)
	}
}

// TestCheckoutWithoutTrees checks that a checkout step fails as it runs
// where the coordinator has no Trees (resolver_missing).
func TestCheckoutWithoutTrees(t *testing.T) {
	t.Parallel()
	r := newWorkerRigOf(t, Options{})
	id := r.createPinned(t, `
jobs:
  main:
    steps:
      - uses: parallelworks/checkout
        with:
          repo: https://example.com/acme/app.git
          branch: main
`)
	serve(t, r.c)
	waitEnded(t, r.c, id)
	e := finishedStep(t, r.c, id, "main", 0, 0)
	if e.Outcome != plan.OutcomeFailure || len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != problems.ResolverMissing.Code() || e.Diagnostics[0].Pointer != "#/jobs/main/steps/0/uses" {
		t.Errorf("the checkout ended %s, with %+v", e.Outcome, e.Diagnostics)
	}
}
