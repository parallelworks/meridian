package plan_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
)

// A value's name is its path's keys joined with dots, each key's own . and
// ~ written ~1 and ~0, and SplitPath reads it back, whatever the keys hold.
// Keys that hold neither are joined as they are, as before there was a way
// to write them.
func TestPathNames(t *testing.T) {
	for _, tc := range []struct {
		path []string
		name string
	}{
		{[]string{"inputs"}, "inputs"},
		{[]string{"inputs", "creds", "0", "pass"}, "inputs.creds.0.pass"},
		{[]string{"vars", "token"}, "vars.token"},
		{[]string{"inputs", "db.pass"}, "inputs.db~1pass"},
		{[]string{"inputs", "a~b"}, "inputs.a~0b"},
		{[]string{"inputs", "~1"}, "inputs.~01"},
		{[]string{"inputs", "~0"}, "inputs.~00"},
		{[]string{"inputs", "a.~.b"}, "inputs.a~1~0~1b"},
		{[]string{"inputs", "."}, "inputs.~1"},
		{[]string{"inputs", "~"}, "inputs.~0"},
		{[]string{"inputs", ""}, "inputs."},
		{[]string{"inputs", "", ""}, "inputs.."},
		{[]string{"inputs", "", "x"}, "inputs..x"},
		{[]string{"inputs", "g", "..", "0"}, "inputs.g.~1~1.0"},
		{[]string{"vars", "клиент.пароль"}, "vars.клиент~1пароль"},
		{[]string{"inputs", "鍵", "🔑"}, "inputs.鍵.🔑"},
		{[]string{"env", "PW_JOB_DIR"}, "env.PW_JOB_DIR"},
	} {
		if got := plan.JoinPath(tc.path...); got != tc.name {
			t.Errorf("JoinPath(%q) = %q, want %q", tc.path, got, tc.name)
		}
		if got := plan.SplitPath(tc.name); !slices.Equal(got, tc.path) {
			t.Errorf("SplitPath(%q) = %q, want %q", tc.name, got, tc.path)
		}
	}
}

// A ~ that isn't followed by 0 or 1 is itself, as in a name written by
// hand.
func TestSplitPathLoneTilde(t *testing.T) {
	for name, want := range map[string][]string{
		"inputs.a~b":   {"inputs", "a~b"},
		"inputs.a~":    {"inputs", "a~"},
		"inputs.~~1":   {"inputs", "~."},
		"inputs.~2.~0": {"inputs", "~2", "~"},
	} {
		if got := plan.SplitPath(name); !slices.Equal(got, want) {
			t.Errorf("SplitPath(%q) = %q, want %q", name, got, want)
		}
	}
}

// Any path of one key or more leads back to itself through its name, and a
// path whose keys hold no . or ~ is named as strings.Join names it.
func FuzzPathNames(f *testing.F) {
	for _, s := range []string{"inputs", "inputs\x00db.pass", "vars\x00a~1", "inputs\x00\x00", "inputs\x00~01.~10", "env\x00é.ü~"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		path := strings.Split(s, "\x00")
		name := plan.JoinPath(path...)
		if got := plan.SplitPath(name); !slices.Equal(got, path) {
			t.Fatalf("SplitPath(JoinPath(%q)) = %q", path, got)
		}
		if !strings.ContainsAny(s, ".~") && name != strings.Join(path, ".") {
			t.Fatalf("JoinPath(%q) = %q, want %q", path, name, strings.Join(path, "."))
		}
	})
}

// A secret whose key holds a dot or a ~, as a submitted value's can, is
// found by its name: masked, kept in the secret record, given to the job
// that reads it, and marked in the scope, as one whose key holds neither
// is.
func TestSecretsWithDottedKeys(t *testing.T) {
	run := plan.RunValues{
		Format: 1,
		Inputs: plan.DataOf(map[string]any{
			"db.password": "s3cret-db-pass",
			"g":           map[string]any{"a~b": "tilde-secret", "plain": "shown"},
			"name":        "Alice",
		}),
		Vars:    map[string]string{"my.token": "var-token-value", "region": "eu"},
		Secrets: []string{"inputs.db~1password", "inputs.g.a~0b", "vars.my~1token"},
	}
	masked := run.Masked()
	for _, s := range []string{"s3cret-db-pass", "tilde-secret", "var-token-value"} {
		if !slices.Contains(masked, s) {
			t.Errorf("Masked() = %q, without %q", masked, s)
		}
	}
	record, state := run.Split()
	tree := func(r plan.RunValues) string { return fmt.Sprint(r.Inputs.Get(), r.Vars) }
	for _, s := range []string{"s3cret-db-pass", "tilde-secret", "var-token-value"} {
		if strings.Contains(tree(state), s) {
			t.Errorf("the run state holds %q: %s", s, tree(state))
		}
		if !strings.Contains(tree(record), s) {
			t.Errorf("the secret record doesn't hold %q: %s", s, tree(record))
		}
	}
	if !strings.Contains(tree(state), "shown") || !strings.Contains(tree(state), "Alice") {
		t.Errorf("the run state lost what isn't private: %s", tree(state))
	}
	if joined := plan.JoinRunValues(record, state); tree(joined) != tree(run) {
		t.Errorf("joined %s, want %s", tree(joined), tree(run))
	}

	scope := plan.NewScope(plan.PhaseStep, plan.Contexts{Run: run})
	for src, want := range map[string]expr.Read{
		"${{ inputs['db.password'] }}": {Marked: true, Private: true},
		"${{ inputs.g['a~b'] }}":       {Marked: true, Private: true},
		"${{ vars['my.token'] }}":      {Marked: true, Private: true},
		"${{ inputs.g.plain }}":        {},
	} {
		tmpl, err := expr.ParseTemplate(src)
		if err != nil {
			t.Fatal(err)
		}
		if _, read, err := tmpl.EvalRead(scope); err != nil || read != want {
			t.Errorf("%s read %+v, %v, want %+v", src, read, err, want)
		}
	}

	for src, want := range map[string]string{
		"${{ inputs['db.password'] }}": "map[db.password:s3cret-db-pass] map[] [inputs.db~1password]",
		"${{ inputs.g }}":              "map[g:map[a~b:tilde-secret plain:shown]] map[] [inputs.g.a~0b]",
		"${{ vars['my.token'] }}":      "map[] map[my.token:var-token-value] [vars.my~1token]",
		"${{ inputs.name }}":           "map[name:Alice] map[] []",
	} {
		j := &plan.Job{Name: "j", Steps: []plan.Step{{Run: mustValue(t, src)}}}
		got := j.Reads(run)
		if s := fmt.Sprint(got.Inputs.Get(), got.Vars, got.Secrets); s != want {
			t.Errorf("%s reads %s, want %s", src, s, want)
		}
	}
}
