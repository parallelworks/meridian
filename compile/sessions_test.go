package compile_test

import (
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/problems"
)

// TestSessionRedirectTwice checks format 1's rule that only one session
// has redirect: true: a workflow that writes it on two is refused as it
// is compiled, without a session provider too, as an editor compiles it,
// at the second's redirect:, naming both; one whose expressions give it
// to two is refused as the run starts, which makes no session; and one
// session's redirect:, with false on the others, is fine.
func TestSessionRedirectTwice(t *testing.T) {
	twice := workflow("a: {type: string, optional: true}", "sessions:\n  b:\n    redirect: true\n  a:\n    redirect: true\n  c:\n    redirect: false\n")
	for name, opts := range map[string]compile.Options{
		"created":  {Inputs: passThrough{}, Sessions: &testSessions{}},
		"compiled": {},
	} {
		var diags problems.Diagnostics
		if name == "created" {
			_, d, err := compile.Create(t.Context(), "workflow.yaml", []byte(twice), nil, opts)
			if err != nil {
				t.Fatal(err)
			}
			diags = d
		} else {
			_, diags = compile.Compile("workflow.yaml", []byte(twice), nil, opts)
		}
		var found []problems.Diagnostic
		for _, d := range diags {
			if d.Code == problems.SessionRedirectTwice.Code() {
				found = append(found, d)
			}
		}
		if len(found) != 1 || found[0].Path() != "/sessions/b/redirect" || found[0].Params["keys"] != `"a", "b"` {
			t.Errorf("%s: diagnostics %v, want session_redirect_twice at /sessions/b/redirect", name, diags)
		}
	}

	once := workflow("a: {type: string, optional: true}", "sessions:\n  a:\n    redirect: true\n  b:\n    redirect: false\n  c: {}\n")
	s := &testSessions{}
	c := createWith(t, once, nil, compile.Options{Inputs: passThrough{}, Sessions: s})
	if res, diags := startRun(t, c, nil, compile.StartInput{RunID: "r1", Sessions: s}); res == nil || len(s.made) != 1 {
		t.Fatalf("a run with one session to redirect to didn't start: %v", diags)
	}

	expressions := workflow("n: {type: number, default: 2}",
		"sessions:\n  a:\n    redirect: ${{ inputs.n == 2 }}\n  b:\n    redirect: ${{ inputs.n > 1 }}\n")
	s = &testSessions{}
	c = createWith(t, expressions, nil, compile.Options{Inputs: passThrough{}, Sessions: s})
	res, diags := startRun(t, c, nil, compile.StartInput{RunID: "r2", Sessions: s})
	errs := diags.Errors()
	if res != nil || len(errs) != 1 || errs[0].Code != problems.SessionRedirectTwice.Code() || errs[0].Path() != "/sessions" || errs[0].Params["keys"] != `"a", "b"` {
		t.Errorf("redirects that expressions give: %v", diags)
	}
	if len(s.made) != 0 {
		t.Errorf("a run that failed to start made sessions: %q", s.made)
	}
}
