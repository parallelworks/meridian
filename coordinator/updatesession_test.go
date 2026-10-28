package coordinator

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// testSessions is a session provider of a test's. It makes every session,
// and records each update. It authorizes one as format 1's platform does:
// a run whose permissions: hold '*' may update any session of the
// person's own, whose names here end in -session, but not one only shared
// with them, whose names here end in -shared; and any other only one its
// tree's root made. It refuses one whose with: status is refuse, the first
// time it is asked to, as a platform refuses a session it can't reach.
type testSessions struct {
	mu      sync.Mutex
	made    map[string][]string // by run
	updates []string
	refused bool
}

func (s *testSessions) Check(_ context.Context, sessions map[string]any) (map[string]string, error) {
	names := map[string]string{}
	for name := range sessions {
		names[name] = name + "-session"
	}
	return names, nil
}

func (s *testSessions) Make(_ context.Context, runID string, _ map[string]any, names map[string]string, _ expr.Read) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.made == nil {
		s.made = map[string][]string{}
	}
	for _, name := range names {
		if !slices.Contains(s.made[runID], name) {
			s.made[runID] = append(s.made[runID], name)
		}
	}
	return nil
}

func (s *testSessions) Update(_ context.Context, u compile.SessionUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	may := slices.Contains(s.made[u.Root], u.Name)
	if slices.Contains(u.Permissions, "*") {
		may = strings.HasSuffix(u.Name, "-session")
	}
	if !may {
		return problems.InputRejected.Problem(problems.Params{"input": "name", "reason": u.Name + " isn't the run's to update"})
	}
	if u.With["status"] == "refuse" && !s.refused {
		s.refused = true
		return problems.InputRejected.Problem(problems.Params{"input": "url", "reason": "not reachable"})
	}
	data, err := json.Marshal(u.With, json.Deterministic(true))
	if err != nil {
		return err
	}
	s.updates = append(s.updates, fmt.Sprintf("%s %s %v %s %s %+v", u.Run, u.Root, u.Permissions, u.Name, data, u.Read))
	return nil
}

// stepEventsOf returns the finished events of the job key's latest attempt
// of the run id, each as step.try, its outcome, and its problems' codes.
func stepEventsOf(t *testing.T, c *Coordinator, id, key string) []string {
	t.Helper()
	a := latestAttempt(t, c, id, key)
	var events []workerapi.Event
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		events, err = tx.Events(a.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range events {
		if !e.Finished || e.Cleanup {
			continue
		}
		var codes []string
		for _, d := range e.Diagnostics {
			codes = append(codes, string(d.Code))
		}
		out = append(out, fmt.Sprintf("%d.%d %s %s", e.Step, e.Try, e.Outcome, strings.Join(codes, ",")))
	}
	return out
}

// TestUpdateSession checks a step that uses parallelworks/update-session,
// run end to end by meridian's worker against the coordinator, which does
// it through its session provider: it asks it to update the session its
// with:'s name names, whichever that is, with the rest of its with:, the
// run, its root and its permissions:, and the provider decides. A run
// without '*' updates one of its own sessions, as sessions.<name> names
// it, and the provider refuses it one of the person's that its tree
// didn't make, which fails the step and ignore-errors: lets survive; one
// with '*' updates that one, and the provider refuses it one only shared
// with the person, and one that isn't theirs. A refusal
// of the provider's fails a try, which its retry: tries again; and a
// with: that read a secret is given the provider as what it must mask, and
// keep private. Without a provider, as with meridian run, the step fails
// as it runs, and only then, saying the Sessions interface is missing, as
// an ordinary step failure: each try fails, and ignore-errors: lets the
// last succeed.
func TestUpdateSession(t *testing.T) {
	parallel(t)
	t.Run("with a provider", func(t *testing.T) {
		s := &testSessions{}
		vars := conformancetest.Variables{"vars.KEY": {Value: "s3cret-api-key", Secret: true}}
		r := newWorkerRigWith(t, Options{Sessions: s, Variables: vars})
		contexts := vars.Contexts()
		contexts["sessions"] = map[string]any{"desktop": "desktop-session"}
		id := r.create(t, []byte(`
sessions:
  desktop:
    useTLS: false
jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        with:
          name: ${{ sessions.desktop }}
          url: http://localhost:8080
      - uses: parallelworks/update-session
        with:
          name: persons-other-session
        ignore-errors: true
      - uses: parallelworks/update-session
        with:
          name: ${{ sessions.desktop }}
          remotePort: 22
          status: refuse
        retry:
          max-retries: 1
          interval: 0s
      - uses: parallelworks/update-session
        with:
          name: ${{ sessions.desktop }}
          openAI: true
          apiKey: ${{ vars.KEY }}
`), nil, contexts)
		serve(t, r.c)
		run := waitEnded(t, r.c, id)
		if run.Outcome != plan.OutcomeSuccess {
			t.Errorf("the run ended %s", run.Outcome)
		}
		want := []string{
			"0.0 success ",
			"1.0 success input_rejected",
			"2.0 failure input_rejected",
			"2.1 success ",
			"3.0 success ",
		}
		if got := stepEventsOf(t, r.c, id, "main"); !slices.Equal(got, want) {
			t.Errorf("the steps' events are\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		updated := []string{
			id + " " + id + ` [] desktop-session {"url":"http://localhost:8080"} {Marked:false Private:false}`,
			id + " " + id + ` [] desktop-session {"remotePort":22,"status":"refuse"} {Marked:false Private:false}`,
			// What a secret gave, which the provider is told to mask.
			id + " " + id + ` [] desktop-session {"apiKey":"s3cret-api-key","openAI":true} {Marked:true Private:true}`,
		}
		if !slices.Equal(s.updates, updated) {
			t.Errorf("the provider updated\n%s\nwant\n%s", strings.Join(s.updates, "\n"), strings.Join(updated, "\n"))
		}
	})
	t.Run("with permissions: '*'", func(t *testing.T) {
		s := &testSessions{}
		r := newWorkerRigWith(t, Options{Sessions: s})
		id := r.create(t, []byte(`
permissions: ['*']
jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        with:
          name: persons-other-session
          url: http://localhost:8080
      - uses: parallelworks/update-session
        with:
          name: team-shared
        ignore-errors: true
      - uses: parallelworks/update-session
        with:
          name: another-persons
        ignore-errors: true
`), nil, nil)
		serve(t, r.c)
		if run := waitEnded(t, r.c, id); run.Outcome != plan.OutcomeSuccess {
			t.Errorf("the run ended %s", run.Outcome)
		}
		want := []string{"0.0 success ", "1.0 success input_rejected", "2.0 success input_rejected"}
		if got := stepEventsOf(t, r.c, id, "main"); !slices.Equal(got, want) {
			t.Errorf("the steps' events are\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		updated := []string{id + " " + id + ` [*] persons-other-session {"url":"http://localhost:8080"} {Marked:false Private:false}`}
		if !slices.Equal(s.updates, updated) {
			t.Errorf("the provider updated\n%s\nwant\n%s", strings.Join(s.updates, "\n"), strings.Join(updated, "\n"))
		}
	})
	t.Run("without a provider", func(t *testing.T) {
		r := newWorkerRig(t, nil)
		id := r.create(t, []byte(`
jobs:
  main:
    steps:
      - uses: parallelworks/update-session
        if: false
        with:
          name: never
      - uses: parallelworks/update-session
        with:
          name: desktop
        ignore-errors: true
        retry:
          max-retries: 1
          interval: 0s
      - uses: parallelworks/update-session
        with:
          name: desktop
`), nil, nil)
		serve(t, r.c)
		run := waitEnded(t, r.c, id)
		if run.Outcome != plan.OutcomeFailure {
			t.Errorf("the run ended %s", run.Outcome)
		}
		want := []string{
			"0.0 skipped ",
			"1.0 failure resolver_missing",
			"1.1 success resolver_missing",
			"2.0 failure resolver_missing",
		}
		if got := stepEventsOf(t, r.c, id, "main"); !slices.Equal(got, want) {
			t.Errorf("the steps' events are\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	})
}

// TestSessionRun checks what update-session tells the session provider of
// a run to authorize the step by: a root's own ID, as its root, and its
// workflow's permissions:; and a child's ID, its root's, and its root's
// permissions:, not its own, as format 1 gates only the top workflow's,
// so a workflow a step calls that asks for '*' under a root that doesn't
// is given none of it; and one under a root that does, all of it.
func TestSessionRun(t *testing.T) {
	parallel(t)
	const child = `permissions: ['*']
jobs:
  main:
    steps:
      - run: echo child
`
	for _, tc := range []struct {
		name, root string
		want       []string
	}{
		{"a root without '*'", "jobs:\n  build:\n    steps:\n      - uses: github/t/child@main\n        with: {$yaml: w.yaml}\n", nil},
		{"a root with '*'", "permissions: ['*']\njobs:\n  build:\n    steps:\n      - uses: github/t/child@main\n        with: {$yaml: w.yaml}\n", []string{"*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parallel(t)
			c := open(t, Options{})
			hold := make(chan struct{})
			t.Cleanup(func() { close(hold) })
			byJob(c, map[string]testEnd{"build": {wait: hold}, "main": {wait: hold}})
			sources := childSources{"github/t/child@main": child}
			created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(tc.root), nil,
				compile.Options{Source: plan.Source{Location: "workflow.yaml"}, Sources: sources})
			if err != nil || diags.HasErrors() {
				t.Fatal(diags, err)
			}
			root, err := c.CreateRun(t.Context(), created, runOptions)
			if err != nil {
				t.Fatal(err)
			}
			serve(t, c)
			var attempt *store.Attempt
			waitFor(t, func() bool {
				return c.store.View(t.Context(), func(tx *store.Tx) error {
					var err error
					attempt, err = tx.LatestAttempt(root, "build")
					return err
				}) == nil
			})
			id, diags, err := c.CreateChild(t.Context(), ChildRequest{Attempt: attempt.ID, Step: 0, Try: 0, With: map[string]any{"$yaml": "w.yaml"}})
			if err != nil || diags.HasErrors() {
				t.Fatal(id, diags, err)
			}
			for _, r := range []struct {
				run  string
				want compile.SessionUpdate
			}{
				{root, compile.SessionUpdate{Run: root, Root: root, Permissions: tc.want}},
				{id, compile.SessionUpdate{Run: id, Root: root, Permissions: tc.want}},
			} {
				got, err := c.sessionRun(t.Context(), storedRun(t, c, r.run))
				if err != nil || got.Run != r.want.Run || got.Root != r.want.Root || !slices.Equal(got.Permissions, r.want.Permissions) {
					t.Errorf("run %s is told the provider as %+v, %v; want %+v", r.run, got, err, r.want)
				}
			}
		})
	}
}
