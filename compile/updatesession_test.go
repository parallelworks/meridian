package compile_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// TestUpdateSession checks what a step that uses
// parallelworks/update-session does through the session provider: it
// updates one of its run's own sessions, as sessions.<name> reads them,
// with the rest of its with:, a numeric name read as text; it refuses any
// other, without asking the provider; without a provider it fails, saying
// which interface is missing; a refusal of the provider's fails it, with
// its params masked where with: read a variable, a password or a secret;
// and the provider's other errors are returned, quoting nothing where
// with: read one.
func TestUpdateSession(t *testing.T) {
	p := &plan.Plan{Run: plan.Run{SessionNames: map[string]string{"desktop": "desktop-checked", "port": "8080"}}}
	const path = "/jobs/main/steps/1"
	s := &testSessions{}
	update := func(with map[string]any, read expr.Read) problems.Diagnostics {
		t.Helper()
		diags, err := compile.UpdateSession(t.Context(), s, "r1", p, path, with, read)
		if err != nil {
			t.Fatal(err)
		}
		return diags
	}
	code := func(diags problems.Diagnostics) string {
		if len(diags) != 1 {
			return ""
		}
		return string(diags[0].Code) + " at " + diags[0].Path()
	}

	if diags := update(map[string]any{"name": "desktop-checked", "url": "http://h:1", "status": "running"}, expr.Read{}); len(diags) != 0 {
		t.Errorf("updating the run's own session: %v", diags)
	}
	if diags := update(map[string]any{"name": float64(8080), "remotePort": float64(22)}, expr.Read{Marked: true}); len(diags) != 0 {
		t.Errorf("updating a session named by a number: %v", diags)
	}
	want := []string{`r1 desktop-checked {"status":"running","url":"http://h:1"}`, `r1 8080 {"remotePort":22} secret`}
	if strings.Join(s.updated, "\n") != strings.Join(want, "\n") {
		t.Errorf("updated\n%s\nwant\n%s", strings.Join(s.updated, "\n"), strings.Join(want, "\n"))
	}

	// Another run's session, a key of sessions: rather than the name it
	// reads as, and no name: none is the run's own.
	for _, name := range []any{"another-runs-session", "desktop", nil} {
		diags := update(map[string]any{"name": name, "url": "http://h:1"}, expr.Read{})
		if got := code(diags); got != "session_not_of_run at "+path+"/with/name" || diags[0].Params["session"] != expr.Text(name) {
			t.Errorf("updating the session %v: %v", name, diags)
		}
	}
	// What reads a variable, a password or a secret is private, and a
	// read that is only marked, as a secret's, is too.
	reads := []expr.Read{{}, {Private: true}, {Marked: true}, {Private: true, Marked: true}}
	for _, read := range reads[1:] {
		diags := update(map[string]any{"name": "secret-session"}, read)
		if got := code(diags); got != "session_not_of_run at "+path+"/with/name" || diags[0].Params["session"] != problems.Masked {
			t.Errorf("updating a session named by what read %+v: %v", read, diags)
		}
	}
	if len(s.updated) != 2 {
		t.Errorf("sessions not the run's were updated: %q", s.updated)
	}

	// Without a provider.
	diags, err := compile.UpdateSession(t.Context(), nil, "r1", p, path, map[string]any{"name": "desktop-checked"}, expr.Read{})
	if got := code(diags); err != nil || got != "resolver_missing at "+path+"/uses" || diags[0].Params["interface"] != "Sessions" {
		t.Errorf("without a session provider: %v, %v", diags, err)
	}

	// The provider refuses it.
	s.updateErr = problems.InputRejected.Problem(problems.Params{"input": "url", "reason": "http://inside:1 isn't reachable"})
	for _, read := range reads {
		private := read.Private || read.Marked
		diags := update(map[string]any{"name": "desktop-checked", "url": "http://inside:1"}, read)
		reason := diags[0].Params["reason"]
		if got := code(diags); got != "input_rejected at "+path || (reason == problems.Masked) != private {
			t.Errorf("a refusal, of what read %+v: %v", read, diags)
		}
	}

	// The provider can't answer.
	s.updateErr = errors.New("the provider is down, and http://inside:1 with it")
	for _, read := range reads {
		private := read.Private || read.Marked
		diags, err := compile.UpdateSession(t.Context(), s, "r1", p, path, map[string]any{"name": "desktop-checked"}, read)
		switch {
		case err == nil || len(diags) != 0:
			t.Errorf("a provider that can't answer, of what read %+v: %v, %v", read, diags, err)
		case private && strings.Contains(err.Error(), "inside"):
			t.Errorf("an error of a private with: quotes the provider's: %v", err)
		case !private && !errors.Is(err, s.updateErr):
			t.Errorf("the provider's error isn't returned: %v", err)
		}
	}
}
