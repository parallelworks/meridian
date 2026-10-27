package coordinator

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// testSessions is a session provider of a test's. It makes every session,
// and records each update, but refuses one whose with: status is refuse, the
// first time it is asked to, as a platform refuses a session it can't
// reach.
type testSessions struct {
	mu      sync.Mutex
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

func (s *testSessions) Make(context.Context, string, map[string]any, map[string]string, expr.Read) error {
	return nil
}

func (s *testSessions) Update(_ context.Context, runID, name string, with map[string]any, read expr.Read) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if with["status"] == "refuse" && !s.refused {
		s.refused = true
		return problems.InputRejected.Problem(problems.Params{"input": "url", "reason": "not reachable"})
	}
	data, err := json.Marshal(with, json.Deterministic(true))
	if err != nil {
		return err
	}
	s.updates = append(s.updates, fmt.Sprintf("%s %s %s %+v", runID, name, data, read))
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
// it through its session provider: it updates one of its run's own
// sessions, as sessions.<name> names it, with the rest of its with:; it
// fails for a session that isn't the run's, which ignore-errors: lets it
// survive; a refusal of the provider's fails a try, which its retry:
// tries again; and a with: that read a secret is given the provider as
// what it must mask, and keep private. Without a provider, as with meridian run, the step fails as
// it runs, and only then, saying the Sessions interface is missing, as an
// ordinary step failure: each try fails, and ignore-errors: lets the last
// succeed.
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
          name: another-runs-session
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
			"1.0 success session_not_of_run",
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
			id + ` desktop-session {"url":"http://localhost:8080"} {Marked:false Private:false}`,
			id + ` desktop-session {"remotePort":22,"status":"refuse"} {Marked:false Private:false}`,
			// What a secret gave, which the provider is told to mask.
			id + ` desktop-session {"apiKey":"s3cret-api-key","openAI":true} {Marked:true Private:true}`,
		}
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
