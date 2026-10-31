package compile_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// TestUpdateSession checks what a step that uses
// parallelworks/update-session does through the session provider: it asks
// it to update the session its with:'s name names, a number read as text,
// whichever session that is, with the rest of its with:, the run, its
// tree's root and its permissions:, which the provider authorizes it by;
// without a provider it fails, saying which interface is missing; a
// refusal of the provider's, as of a session the run may not update,
// fails it, with its params masked where with: read a variable, a
// password or a secret; and the provider's other errors are returned,
// quoting nothing where with: read one.
func TestUpdateSession(t *testing.T) {
	const path = "/jobs/main/steps/1"
	s := &testSessions{refuses: "another-persons-session"}
	run := compile.SessionUpdate{Run: "r2", Root: "r1", Permissions: []string{"*"}}
	update := func(with map[string]any, read expr.Read) problems.Diagnostics {
		t.Helper()
		u := run
		u.Read = read
		diags, err := compile.UpdateSession(t.Context(), s, u, path, with)
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

	// The run's own session, another run's, one named by a number, and one
	// of no name: each goes to the provider, which decides.
	for _, tc := range []struct {
		with map[string]any
		read expr.Read
	}{
		{map[string]any{"name": "desktop-checked", "url": "http://h:1", "status": "running"}, expr.Read{}},
		{map[string]any{"name": "another-runs-session", "url": "http://h:2"}, expr.Read{}},
		{map[string]any{"name": float64(8080), "remotePort": float64(22)}, expr.Read{Marked: true}},
		{map[string]any{"url": "http://h:3"}, expr.Read{}},
	} {
		if diags := update(tc.with, tc.read); len(diags) != 0 {
			t.Errorf("updating %v: %v", tc.with["name"], diags)
		}
	}
	want := []string{
		`r2 r1 * desktop-checked {"status":"running","url":"http://h:1"}`,
		`r2 r1 * another-runs-session {"url":"http://h:2"}`,
		`r2 r1 * 8080 {"remotePort":22} secret`,
		`r2 r1 *  {"url":"http://h:3"}`,
	}
	if strings.Join(s.updated, "\n") != strings.Join(want, "\n") {
		t.Errorf("updated\n%s\nwant\n%s", strings.Join(s.updated, "\n"), strings.Join(want, "\n"))
	}

	// The provider refuses a session the run may not update: the step
	// fails, with the refusal masked where with: is private, as it is
	// where it read a variable, a password or a secret.
	reads := []expr.Read{{}, {Private: true}, {Marked: true}, {Private: true, Marked: true}}
	for _, read := range reads {
		private := read.Private || read.Marked
		diags := update(map[string]any{"name": "another-persons-session"}, read)
		reason := ""
		if len(diags) == 1 {
			reason, _ = diags[0].Params["reason"].(string)
		}
		if got := code(diags); got != "input_rejected at "+path || (reason == problems.Masked) != private {
			t.Errorf("a refusal, of what read %+v: %v", read, diags)
		}
	}
	if len(s.updated) != len(want) {
		t.Errorf("a refused session was updated: %q", s.updated)
	}

	// Without a provider.
	diags, err := compile.UpdateSession(t.Context(), nil, run, path, map[string]any{"name": "desktop-checked"})
	if got := code(diags); err != nil || got != "resolver_missing at "+path+"/uses" || diags[0].Params["interface"] != "Sessions" {
		t.Errorf("without a session provider: %v, %v", diags, err)
	}

	// The provider can't answer.
	s.updateErr = errors.New("the provider is down, and http://inside:1 with it")
	for _, read := range reads {
		private := read.Private || read.Marked
		u := run
		u.Read = read
		diags, err := compile.UpdateSession(t.Context(), s, u, path, map[string]any{"name": "desktop-checked"})
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

// TestUpdateSessionFoldsKeys checks that the provider gets update-session's
// with: as format 1 reads it, decoding it as encoding/json does once
// json.Marshal has ordered its keys: a key of it but for case is that key,
// and of keys that differ only in case, the last in byte order counts;
// targetInfo's keys are read so too, and other keys are kept as they are.
// The step's with: itself is left as it was.
func TestUpdateSessionFoldsKeys(t *testing.T) {
	s := &testSessions{}
	with := map[string]any{
		"Name": "desktop-checked", "REMOTEPORT": float64(8080), "LocalPort": float64(2), "localport": float64(1),
		"TargetInfo": map[string]any{"NAME": "k8s", "resourcetype": "pods"}, "local_port": float64(3),
	}
	diags, err := compile.UpdateSession(t.Context(), s, compile.SessionUpdate{Run: "r1", Root: "r1"}, "/jobs/main/steps/0", with)
	if err != nil || len(diags) != 0 {
		t.Fatal(err, diags)
	}
	want := `r1 r1  desktop-checked {"localPort":1,"local_port":3,"remotePort":8080,"targetInfo":{"name":"k8s","resourceType":"pods"}}`
	if len(s.updated) != 1 || s.updated[0] != want {
		t.Errorf("updated %q, want %q", s.updated, want)
	}
	if _, ok := with["Name"]; !ok || len(with) != 6 {
		t.Errorf("the step's with: changed: %v", with)
	}
}

// TestSessionKeyNamed checks how format 1 tells a step that names one of
// its workflow's sessions by its key: a with: whose name, under that key
// exactly, is ${{ sessions.<key> }} and nothing else, spaces in the braces
// aside.
func TestSessionKeyNamed(t *testing.T) {
	t.Parallel()
	for name, want := range map[any]string{
		"${{ sessions.desktop }}":      "desktop",
		"${{sessions.web-ui_2}}":       "web-ui_2",
		"${{  sessions.x  }}":          "x",
		"${{ sessions.desktop }}-2":    "",
		"a ${{ sessions.desktop }}":    "",
		"${{ inputs.desktop }}":        "",
		"${{ sessions['desktop'] }}":   "",
		"${{ sessions.desktop.name }}": "",
		"desktop-1":                    "",
		7:                              "",
	} {
		if got := compile.SessionKeyNamed(map[string]any{"name": name}); got != want {
			t.Errorf("SessionKeyNamed(%v) = %q, want %q", name, got, want)
		}
	}
	if got := compile.SessionKeyNamed(map[string]any{"Name": "${{ sessions.desktop }}"}); got != "" {
		t.Errorf("a name under another key = %q", got)
	}
	if got := compile.SessionKeyNamed(nil); got != "" {
		t.Errorf("no with: = %q", got)
	}
}
