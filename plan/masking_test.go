package plan_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A secret that a step wrote to an output shows in no problem with what
// reads the output, though what shows a value quotes it, escapes it and
// cuts it short: what reads one is masked whole.
func TestDiagnosticsHideStepOutputSecrets(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - id: a
        run: "true"
    outputs:
      fails: ${{ fromJSON(steps.a.outputs.y) }}
      merged: ${{ fromJSON(needs.main.outputs.y) }}
      wrapped: ${{ fromJSON(format('{{"x":{0}}}', steps.a.outputs.y)) }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	for _, secret := range []string{
		"ghp_" + strings.Repeat("Z", 36), // 40 characters, as a GitHub token is
		"pa\"ss\"wordQQQ",                // a quote, which JSON escapes
		"line1QQQ\nline2QQQ",             // a line break
		"tab\tQQQQ",                      // a tab
		"uni QQQQ",                       // a space
	} {
		run := runValues(t, p, map[string]string{"token": secret})
		run.Secrets = []string{"vars.token"}
		ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
		if err != nil {
			t.Fatal(err)
		}
		state := plan.JobState{
			Run: run, Masked: run.Masked(),
			Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"y": secret}}},
		}
		_, diags, err := ready.Job.End(t.Context(), state)
		if err != nil {
			t.Fatal(err)
		}
		if len(diags) != 3 {
			t.Fatalf("%q: %v", secret, diags)
		}
		for _, d := range diags {
			if shown := fmt.Sprint(d.Params, d.Detail); strings.Contains(shown, "ZZZZ") || strings.Contains(shown, "QQQ") {
				t.Errorf("secret %q shows in %s", secret, shown)
			}
		}
	}
}

// Only strings are masked, and only those that a template that read a
// secret gave: a boolean a secret decided, such as whether it is set, masks
// no true in the job's logs and outputs, and a with: that holds a secret
// masks only it.
func TestMaskedStrings(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    env:
      HAS_TOKEN: ${{ vars.token != '' }}
    steps:
      - id: a
        uses: workflow/mine
        with:
          user: admin
          region: us-east-1
          password: ${{ vars.token }}
          both: ${{ vars.token }}-${{ 1 }}
    outputs:
      ok: ${{ steps.a.outputs.ok }}
      json: ${{ steps.a.outputs.json }}
      has: ${{ env.HAS_TOKEN }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	run := runValues(t, p, map[string]string{"token": "s3cret-value"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Masked: run.Masked()}
	s, _, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.Masked) != "[s3cret-value s3cret-value-1]" {
		t.Errorf("Masked = %q", s.Masked)
	}
	state.Masked = append(state.Masked, s.Masked...)
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"ok": "true", "json": `{"deployed":true}`}}}
	end, diags, err := ready.Job.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if end.Outputs["ok"] != "true" || end.Outputs["json"] != `{"deployed":true}` || end.Outputs["has"] != "true" || fmt.Sprint(end.Secrets) != "[has]" {
		t.Errorf("outputs %v, %q", end.Outputs, codes(diags))
	}
	var out bytes.Buffer
	w := plan.NewMasker(state.Masked...).Writer(&out)
	_, _ = w.Write([]byte("tests passed: true\n"))
	_ = w.Close()
	if out.String() != "tests passed: true\n" {
		t.Errorf("log line %q", out.String())
	}
}

// What one output: is made of makes none of the others private or a
// secret, so they don't depend on the order of their names.
func TestEndOutputsIndependent(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - run: "true"
    outputs:
      a_zone: ${{ vars.zone }}
      length: ${{ format('{0}', length(vars.token)) }}
      z_zone: ${{ vars.zone }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}, "zone": "us-6"}})
	run := runValues(t, p, map[string]string{"token": "s3cret", "zone": "us-6"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	end, diags, err := ready.Job.End(t.Context(), plan.JobState{Run: run, Masked: run.Masked(), Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess}}})
	if err != nil {
		t.Fatal(err)
	}
	if end.Outputs["a_zone"] != "us-6" || end.Outputs["z_zone"] != "us-6" || end.Outputs["length"] != "6" {
		t.Errorf("outputs %v, %q", end.Outputs, codes(diags))
	}
	if fmt.Sprint(end.Private, end.Secrets) != "[a_zone length z_zone] [length]" {
		t.Errorf("private outputs %q, secrets %q", end.Private, end.Secrets)
	}
}

// A matrix made from a secret variable shows none of its keys in a
// problem, nor in the problem's JSON Pointer, but those the workflow
// writes.
func TestExpandHidesPrivateKeys(t *testing.T) {
	for matrix, want := range map[string]string{
		`{"hunter2-s3cret": 1}`:                         "/jobs/j/strategy/matrix",
		`{"include": {"a": 1}}`:                         "/jobs/j/strategy/matrix/include",
		`{"include": [1], "hunter2-s3cret": [1]}`:       "/jobs/j/strategy/matrix/include/0",
		`{"a": [1], "exclude": 5, "hunter2-s3cret": 7}`: "/jobs/j/strategy/matrix/exclude",
	} {
		job := &plan.Job{Name: "j", Path: "/jobs/j", Strategy: &plan.Strategy{Matrix: mustValue(t, "${{ fromJSON(vars.m) }}")}}
		scope := plan.NewScope(plan.PhaseStrategy, plan.Contexts{Run: plan.RunValues{
			Vars: map[string]string{"m": matrix}, Secrets: []string{"vars.m"},
		}})
		_, err := plan.Expand(t.Context(), job, scope, plan.Limits{})
		xe, ok := errors.AsType[*plan.ExpandError](err)
		if !ok || xe.Path != want || strings.Contains(fmt.Sprint(err, xe.Params, xe.Path), "hunter2") {
			t.Errorf("%s: %v, want path %q", matrix, err, want)
		}
	}
	job := &plan.Job{Name: "j", Path: "/jobs/j", Strategy: &plan.Strategy{Matrix: mustValue(t, map[string]any{"os": "${{ vars.m }}"})}}
	scope := plan.NewScope(plan.PhaseStrategy, plan.Contexts{Run: plan.RunValues{Vars: map[string]string{"m": "linux"}}})
	_, err := plan.Expand(t.Context(), job, scope, plan.Limits{})
	if xe, ok := errors.AsType[*plan.ExpandError](err); !ok || xe.Params["key"] != "os" || xe.Params["value"] != problems.Masked || xe.Path != "/jobs/j/strategy/matrix/os" {
		t.Errorf("a written key: %v", err)
	}
}

// Expand counts what it compares toward its limit and stops when its
// context is done, however many exclude entries a matrix has.
func TestExpandExcludeCost(t *testing.T) {
	var excludes []any
	for i := range 4000 {
		excludes = append(excludes, map[string]any{"a": int64(100000 + i), "b": int64(100000 + i)})
	}
	job := &plan.Job{Name: "j", Path: "/jobs/j", Strategy: &plan.Strategy{Matrix: mustValue(t, map[string]any{
		"a": "${{ range 10000 }}", "b": "${{ range 10000 }}", "exclude": excludes,
	})}}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := plan.Expand(ctx, job, plan.NewScope(plan.PhaseStrategy, plan.Contexts{}), plan.Limits{})
	if xe, ok := errors.AsType[*plan.ExpandError](err); !ok || xe.Kind != problems.MatrixTooLarge || time.Since(start) > 5*time.Second {
		t.Errorf("4000 two-key excludes over 10000x10000: %v after %v", err, time.Since(start))
	}
	// Excludes that remove all but a few combinations: what Expand looks
	// at is bounded, and it stops when its context is done.
	var values []any
	excludes = nil
	for i := range 2048 {
		values = append(values, int64(i))
	}
	for i := range 2048 {
		excludes = append(excludes, map[string]any{"a": int64(i), "b": int64(i % 7)})
	}
	job.Strategy.Matrix = mustValue(t, map[string]any{"a": values, "b": values, "exclude": excludes})
	done, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start = time.Now()
	_, err = plan.Expand(done, job, plan.NewScope(plan.PhaseStrategy, plan.Contexts{}), plan.Limits{MaxInstances: 1 << 30})
	if !errors.Is(err, context.DeadlineExceeded) && !isTooLarge(err) || time.Since(start) > 5*time.Second {
		t.Errorf("a matrix Expand stops for: %v after %v", err, time.Since(start))
	}
}

// One-key excludes that match many equal values cost Expand what they
// remove, once each: a run of them is quick, and stops when its context is
// done.
func TestExpandOneKeyExcludeCost(t *testing.T) {
	for name, matrix := range map[string]map[string]any{
		"10000 one-key excludes of a value 10000 share": {
			"a": "${{ map(range 10000, i => 'x') }}", "b": []any{"1"},
			"exclude": `${{ map(range 10000, i => fromJSON('{"a":"x","b":"2"}')) }}`,
		},
		"10000 one-key excludes of the only key": {
			"a":       "${{ map(range 10000, i => 'x') }}",
			"exclude": `${{ map(range 10000, i => fromJSON('{"a":"x"}')) }}`,
		},
		"40001 equal values": {
			"a":       `${{ fromJSON(format('[{0}"x"]', join(map(range 10000, i => '"x","x","x","x",'), ''))) }}`,
			"exclude": `${{ map(range 10000, i => fromJSON('{"a":"x"}')) }}`,
		},
	} {
		job := &plan.Job{Name: "j", Path: "/jobs/j", Strategy: &plan.Strategy{Matrix: mustValue(t, matrix)}}
		start := time.Now()
		_, err := plan.Expand(t.Context(), job, plan.NewScope(plan.PhaseStrategy, plan.Contexts{}), plan.Limits{})
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("%s: %v after %v", name, err, elapsed)
		}
		done, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := plan.Expand(done, job, plan.NewScope(plan.PhaseStrategy, plan.Contexts{}), plan.Limits{}); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a done context: %v", name, err)
		}
	}
}

func isTooLarge(err error) bool {
	xe, ok := errors.AsType[*plan.ExpandError](err)
	return ok && xe.Kind == problems.MatrixTooLarge
}

// A Masker takes many values one at a time, and masks text with them in
// one pass.
func TestMaskerManyValues(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	const n = 50000
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("value-%08d", r.IntN(1<<30))
	}
	m := plan.NewMasker()
	start := time.Now()
	for _, v := range values {
		m.Add(v)
	}
	text := strings.Repeat("x", 1<<16) + values[n-1] + strings.Repeat("y", 10)
	if got := m.Mask(text); !strings.HasSuffix(got, "x***yyyyyyyyyy") {
		t.Errorf("not masked: ...%s", got[len(got)-30:])
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("%d Adds and a Mask took %v", n, elapsed)
	}
	// Adds between masks rebuild what matches only now and then.
	m = plan.NewMasker()
	start = time.Now()
	for i, v := range values {
		m.Add(v)
		if i%100 == 0 && m.Mask(v) != "***" {
			t.Fatalf("%q isn't masked", v)
		}
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("%d Adds with a Mask each 100 took %v", n, elapsed)
	}
}

// A Writer writes what Mask gives for any values, however its text is
// split.
func TestMaskWriterRandomValues(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 9))
	alpha := "ab\nc"
	word := func(max int) string {
		var b strings.Builder
		for range 1 + r.IntN(max) {
			b.WriteByte(alpha[r.IntN(len(alpha))])
		}
		return b.String()
	}
	for range 20000 {
		var values []string
		for range 1 + r.IntN(4) {
			values = append(values, word(5))
		}
		m := plan.NewMasker(values...)
		s := word(40)
		var out bytes.Buffer
		w := m.Writer(&out)
		for rest := s; rest != ""; {
			n := min(len(rest), 1+r.IntN(6))
			if _, err := w.Write([]byte(rest[:n])); err != nil {
				t.Fatal(err)
			}
			rest = rest[n:]
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if want := m.Mask(s); out.String() != want {
			t.Fatalf("values %q, %q in parts gave %q, want %q", values, s, out.String(), want)
		}
	}
}

// A secret is masked as a log or an output shows it too: without its last
// line break, a line at a time, with CRLF line breaks, and escaped in JSON
// as Go's encoding/json/v2 and encoding/json and Python's json.dumps write
// it. Its short lines, and those of punctuation or JSON words, aren't
// masked on their own.
func TestMaskShownForms(t *testing.T) {
	m := plan.NewMasker(
		"tok3n-value\n",
		"-----BEGIN KEY-----\nMIIEabcdef\nxyz12345\n  },\n    true\nxyz\n-----END KEY-----",
		"pa\"ss\\wd<é😀>\u2028",
	)
	for s, want := range map[string]string{
		`Authorization: tok3n-value"`: `Authorization: ***"`,
		"-----BEGIN KEY-----\r\nMIIEabcdef\r\nxyz12345\r\n  },\r\n    true\r\nxyz\r\n-----END KEY-----": "***\r\n***\r\n***\r\n  },\r\n    true\r\nxyz\r\n***",
		"+ echo MIIEabcdef":                              "+ echo ***",
		"tests passed: true },":                          "tests passed: true },",
		`{"pw":"pa\"ss\\wd<é😀>` + "\u2028" + `"}`:        `{"pw":"***"}`,
		`{"pw":"pa\"ss\\wd<é😀>\u2028"}`:                  `{"pw":"***"}`,
		`{"pw":"pa\"ss\\wd\u003cé😀\u003e\u2028"}`:        `{"pw":"***"}`,
		`{"pw": "pa\"ss\\wd<\u00e9\ud83d\ude00>\u2028"}`: `{"pw": "***"}`,
		"xyz is short, so it shows":                      "xyz is short, so it shows",
	} {
		if got := m.Mask(s); got != want {
			t.Errorf("Mask(%q) = %q, want %q", s, got, want)
		}
	}
}

// A job reads the inputs its expressions' paths lead to, and no more: not
// the password beside a field it reads.
func TestReadsByPath(t *testing.T) {
	run := plan.RunValues{
		Inputs: plan.DataOf(map[string]any{
			"hosts":  []any{map[string]any{"name": "a", "pass": "pw-a"}, map[string]any{"name": "b", "pass": "pw-b"}},
			"config": `{"x": 1}`, "list": []any{"p", "q"}, "other": "o",
		}),
		Secrets: []string{"inputs.hosts.0.pass", "inputs.hosts.1.pass"},
	}
	for src, want := range map[string]string{
		"${{ inputs.hosts[1].name }}":         `map[hosts:[<nil> map[name:b]]] []`,
		"${{ inputs.hosts[0] }}":              `map[hosts:[map[name:a pass:pw-a]]] [inputs.hosts.0.pass]`,
		"${{ inputs.hosts.*.name }}":          `map[hosts:[map[name:a pass:pw-a] map[name:b pass:pw-b]]] [inputs.hosts.0.pass inputs.hosts.1.pass]`,
		"${{ inputs.hosts[0:1] }}":            `map[hosts:[map[name:a pass:pw-a] map[name:b pass:pw-b]]] [inputs.hosts.0.pass inputs.hosts.1.pass]`,
		"${{ inputs.config.x }}":              `map[config:{"x": 1}] []`,
		"${{ inputs.missing.x }} ${{ 1 }}":    `map[] []`,
		"${{ inputs.list[5] }}":               `map[] []`,
		"${{ inputs.hosts[00].name }}":        `map[hosts:[map[name:a pass:pw-a] map[name:b pass:pw-b]]] [inputs.hosts.0.pass inputs.hosts.1.pass]`,
		"${{ inputs.hosts['1'].name }}":       `map[hosts:[<nil> map[name:b]]] []`,
		"${{ inputs.hosts.length }}":          `map[hosts:[map[name:a pass:pw-a] map[name:b pass:pw-b]]] [inputs.hosts.0.pass inputs.hosts.1.pass]`,
		"${{ inputs.other }} ${{ inputs.* }}": `map[config:{"x": 1} hosts:[map[name:a pass:pw-a] map[name:b pass:pw-b]] list:[p q] other:o] [inputs.hosts.0.pass inputs.hosts.1.pass]`,
	} {
		v := mustValue(t, src)
		j := &plan.Job{Name: "j", Steps: []plan.Step{{Run: v}}}
		got := j.Reads(run)
		if s := fmt.Sprint(got.Inputs.Get(), got.Secrets); s != want {
			t.Errorf("%s reads %s, want %s", src, s, want)
		}
		// What the job reads gives what all the run's values do.
		all, _ := v.Eval(plan.NewScope(plan.PhaseStep, plan.Contexts{Run: run}))
		part, _ := v.Eval(plan.NewScope(plan.PhaseStep, plan.Contexts{Run: got}))
		if fmt.Sprint(all) != fmt.Sprint(part) {
			t.Errorf("%s is %v with the run's values, %v with what it reads", src, all, part)
		}
	}
}

// A short string made from a secret, such as an answer it decided, masks
// nothing, unless a secret's own text holds it: a secret's own text, and
// its shown forms, are masked whatever their length, so a plain output that
// holds the short string isn't masked.
func TestMaskDerivedShortStrings(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    env:
      HAS: ${{ vars.token != '' && 'yes' || 'no' }}
      USER: ${{ fromJSON(vars.creds).user }}
      LONG: ${{ vars.token != '' && 'configured-yes' || 'no' }}
    steps:
      - id: a
        run: "true"
      - run: "true"
    outputs:
      answer: ${{ steps.a.outputs.answer }}
      host: ${{ steps.a.outputs.host }}
      user: ${{ steps.a.outputs.user }}
      note: ${{ needs.main.outputs.note }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}, "creds": expr.Unknown{Name: "vars"}}})
	run := runValues(t, p, map[string]string{"token": "s3cret-value", "creds": `{"user":"svc","pass":"hunter2-pass"}`})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Masked: run.Masked()}
	s, _, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.Masked) != "[configured-yes]" {
		t.Errorf("Masked = %q", s.Masked)
	}
	state.Masked = append(state.Masked, s.Masked...)
	state.Steps = []plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"answer": "yes", "host": "eyes-01", "user": "svc"}},
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"note": "the eyes have it"}},
	}
	end, diags, err := ready.Job.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"answer": "yes", "host": "eyes-01", "user": "svc", "note": "the eyes have it"}
	if !maps.Equal(end.Outputs, want) || len(diags) > 0 {
		t.Errorf("outputs %q, %q", end.Outputs, codes(diags))
	}
	if got := plan.NewMasker(state.Masked...).Mask("the eyes have it: yes, svc"); got != "the eyes have it: yes, svc" {
		t.Errorf("a log line: %q", got)
	}
}

// A secret of more than one line masks each of its lines in a log, but
// for those that are short, only punctuation, or a JSON word, and makes no
// step's output that holds a line of it a secret.
func TestMaskMultiLineSecret(t *testing.T) {
	secret := "{\n  \"enabled\":\ntrue\n    },\napiVersion: v1\n  \"key\": \"abc123456\"\n}"
	p := compiled(t, `
jobs:
  main:
    env:
      T: ${{ vars.token }}
    steps:
      - id: a
        run: "true"
      - run: "true"
    outputs:
      ok: ${{ steps.a.outputs.ok }}
      kind: ${{ steps.a.outputs.kind }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	run := runValues(t, p, map[string]string{"token": secret})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Masked: run.Masked(), Steps: []plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"ok": "true", "kind": "apiVersion: v1"}},
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"json": "{\n    },\n  \"done\": true\n}"}},
	}}
	end, diags, err := ready.Job.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if end.Outputs["ok"] != "true" || end.Outputs["json"] != "{\n    },\n  \"done\": true\n}" || end.Outputs["kind"] != "apiVersion: v1" || end.Secrets != nil {
		t.Errorf("outputs %q, secrets %q, %q", end.Outputs, end.Secrets, codes(diags))
	}
	m := plan.NewMasker(secret)
	for s, want := range map[string]string{
		"tests passed: true\n":      "tests passed: true\n",
		"  },\n":                    "  },\n",
		"kind: apiVersion: v1\n":    "kind: ***\n",
		`echo "key": "abc123456"`:   "echo ***",
		"the whole " + secret + ".": "the whole ***.",
	} {
		if got := m.Mask(s); got != want {
			t.Errorf("Mask(%q) = %q, want %q", s, got, want)
		}
	}
}

// Only a secret's string is masked: a password input left empty, or a
// boolean one, masks nothing, and a plain output that is a word such as
// true is never masked.
func TestMaskScalars(t *testing.T) {
	r := plan.RunValues{Inputs: plan.DataOf(map[string]any{"pw": nil, "flag": true, "n": int64(5)}), Secrets: []string{"inputs.pw", "inputs.flag", "inputs.n"}}
	if got := r.Masked(); len(got) != 0 {
		t.Errorf("Masked() = %q", got)
	}
	p := compiled(t, `
jobs:
  main:
    env:
      HAS: ${{ vars.token != '' }}
      N: ${{ length(vars.token) }}
    steps:
      - run: "true"
    outputs:
      ok: ${{ 'true' }}
      six: ${{ 'us-6' }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	run := runValues(t, p, map[string]string{"token": "s3cret"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	end, diags, err := ready.Job.End(t.Context(), plan.JobState{
		Run: run, Masked: run.Masked(), Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"w": "true"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"ok": "true", "six": "us-6", "w": "true"}; !maps.Equal(end.Outputs, want) || len(diags) > 0 {
		t.Errorf("outputs %v, %q", end.Outputs, codes(diags))
	}
}

// A Writer can be written from more than one goroutine, as a step's
// standard output and error are, and fails a Write after Close.
func TestMaskWriterConcurrent(t *testing.T) {
	var out bytes.Buffer
	w := plan.NewMasker("s3cret-value").Writer(&out)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 500 {
				if _, err := w.Write([]byte("s3cret-value\n")); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if out.String() != strings.Repeat("***\n", 2000) {
		t.Errorf("wrote %d bytes: %.40q", out.Len(), out.String())
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Error("a Write after Close")
	}
}

// The plan reads only the worker's variables that format 1 defines, so a
// credential the worker passes, such as PW_API_KEY, reaches no output.
func TestWorkerCredentialUnread(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - run: echo [${{ env.PW_API_KEY }}] ${{ env.PW_JOB_DIR }}
    outputs:
      k: ${{ env.PW_API_KEY }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Env: map[string]string{"PW_API_KEY": "api-key-123", "PW_JOB_DIR": "/j"}}
	s, _, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil || s.Run != "echo [] /j" || s.Env["PW_API_KEY"] != "" {
		t.Errorf("step: %+v, %v", s, err)
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess}}
	end, _, err := ready.Job.End(t.Context(), state)
	if err != nil || end.Outputs["k"] != "" {
		t.Errorf("outputs %v, %v", end, err)
	}
}

// BenchmarkMask masks a log with a few secrets.
func BenchmarkMask(b *testing.B) {
	text := strings.Repeat("2026-10-22T10:00:00Z INFO building target //pkg/foo:bar in 12.3s, cache hit ratio 0.93\n", 1<<14)
	for _, n := range []int{0, 3, 30} {
		var values []string
		for i := range n {
			values = append(values, fmt.Sprintf("ghp_%036d", i))
		}
		m := plan.NewMasker(values...)
		b.Run(fmt.Sprintf("%d values", n), func(b *testing.B) {
			b.SetBytes(int64(len(text)))
			for b.Loop() {
				_ = m.Mask(text)
			}
		})
		b.Run(fmt.Sprintf("%d values, Writer", n), func(b *testing.B) {
			b.SetBytes(int64(len(text)))
			for b.Loop() {
				w := m.Writer(io.Discard)
				for i := 0; i < len(text); i += 4096 {
					_, _ = w.Write([]byte(text[i:min(i+4096, len(text))]))
				}
				_ = w.Close()
			}
		})
	}
}

// readyMain compiles workflow with vars, those named in secret unknown as
// secret variables are, and readies its job main.
func readyMain(t testing.TB, workflow string, vars map[string]string, secret ...string) (*plan.ReadyJob, plan.RunValues) {
	t.Helper()
	ctxVars := map[string]any{}
	for k, v := range vars {
		ctxVars[k] = v
	}
	for _, k := range secret {
		ctxVars[k] = expr.Unknown{Name: "vars"}
	}
	p := compiled(t, workflow, map[string]any{"vars": ctxVars})
	run := runValues(t, p, vars)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	return ready.Job, run
}

// A field of a structured secret is masked wherever it shows, though it is
// short, as a secret's own text holds it, and though it is read inline, as
// each expression that read a secret is masked: not one under 4 bytes, nor
// a JSON word.
func TestMaskSecretFields(t *testing.T) {
	token := "tok_" + strings.Repeat("Z", 36)
	rj, run := readyMain(t, `
jobs:
  main:
    steps:
      - run: mysql -u${{ fromJSON(vars.db).user }} -p${{ fromJSON(vars.db).password }} --ssl=${{ fromJSON(vars.db).ssl }}
      - run: deploy --token ${{ fromJSON(vars.creds).token }}
`, map[string]string{"db": `{"user":"svc","password":"hunt3r2","ssl":"true"}`, "creds": `{"user":"svc","token":"` + token + `"}`}, "db", "creds")
	state := plan.JobState{Run: run, Masked: run.Masked()}
	for i := range 2 {
		s, _, err := rj.Step(t.Context(), i, state)
		if err != nil || s == nil {
			t.Fatal(err)
		}
		state.Masked = append(state.Masked, s.Masked...)
		state.Steps = append(state.Steps, plan.StepResult{Outcome: plan.OutcomeSuccess})
	}
	state.Steps[0].Outputs = map[string]string{"pw": "hunt3r2", "user": "svc", "ssl": "true"}
	end, _, err := rj.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"pw": "hunt3r2", "user": "svc", "ssl": "true"}; !maps.Equal(end.Outputs, want) || fmt.Sprint(end.Secrets) != "[pw]" {
		t.Errorf("outputs %q, secrets %q, want %q", end.Outputs, end.Secrets, want)
	}
	m := plan.NewMasker(state.Masked...)
	for log, want := range map[string]string{
		"ERROR 1045: Access denied for svc (using password: hunt3r2), ssl true": "ERROR 1045: Access denied for svc (using password: ***), ssl true",
		"+ deploy --token " + token + "\nerror: token " + token + " expired":    "+ ***\nerror: token *** expired",
	} {
		if got := m.Mask(log); got != want {
			t.Errorf("Mask(%q) = %q, want %q", log, got, want)
		}
	}
}

// A step's output that holds a string made from a secret holds a secret,
// so a problem with what reads it shows none of it.
func TestMaskLeakCheckDerived(t *testing.T) {
	token := "tok_" + strings.Repeat("Z", 36)
	rj, run := readyMain(t, `
jobs:
  main:
    steps:
      - id: a
        run: echo $T
        env:
          T: ${{ fromJSON(vars.creds).token }}
      - run: echo ${{ fromJSON(steps.a.outputs.y) }}
`, map[string]string{"creds": `{"user":"svc","token":"` + token + `"}`}, "creds")
	state := plan.JobState{Run: run, Masked: run.Masked()}
	s, _, err := rj.Step(t.Context(), 0, state)
	if err != nil {
		t.Fatal(err)
	}
	state.Masked = append(state.Masked, s.Masked...)
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"y": "got " + token}}}
	_, diags, err := rj.Step(t.Context(), 1, state)
	if err != nil || len(diags) != 1 {
		t.Fatal(diags, err)
	}
	if shown := fmt.Sprint(diags, diags[0].Params); strings.Contains(shown, "ZZZZ") || diags[0].Params["text"] != problems.Masked {
		t.Errorf("the token shows: %s", shown)
	}
}

// Values of an env: that read each other keep what they read of a secret,
// in a chain or a cycle.
func TestEnvOrderKeepsMarks(t *testing.T) {
	rj, run := readyMain(t, `
env:
  A: placeholder
jobs:
  main:
    env:
      A: ${{ env.A }}-x
      B: ${{ env.C }}
      C: ${{ env.B }}${{ env.A }}
    steps:
      - run: "true"
        env:
          D: ${{ env.E }}
          E: ${{ env.D }}${{ env.C }}
    outputs:
      a: ${{ env.A }}
      b: ${{ env.B }}
      c: ${{ env.C }}
`, nil)
	run.Env["A"] = "s3cret-tok"
	run.Secrets = append(run.Secrets, "env.A")
	state := plan.JobState{Run: run, Masked: run.Masked()}
	s, diags, err := rj.Step(t.Context(), 0, state)
	if err != nil || s == nil {
		t.Fatal(diags, err)
	}
	if s.Env["E"] != "s3cret-tok-x" || !slices.Contains(s.Masked, "s3cret-tok-x") {
		t.Errorf("env %q, masked %q", s.Env, s.Masked)
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess}}
	end, diags, err := rj.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"a": "s3cret-tok-x", "b": "", "c": "s3cret-tok-x"}; !maps.Equal(end.Outputs, want) || fmt.Sprint(end.Secrets) != "[a c]" {
		t.Errorf("outputs %q, secrets %q, %q", end.Outputs, end.Secrets, codes(diags))
	}
}

// A cleanup's problems show no secret a step's output held, and End masks
// a cancelled step's outputs as any other's.
func TestCleanupMasks(t *testing.T) {
	token := "tok_" + strings.Repeat("Q", 36)
	rj, run := readyMain(t, `
jobs:
  main:
    env:
      T: ${{ vars.token }}
    steps:
      - id: a
        run: "true"
      - run: "true"
        cleanup: rm ${{ fromJSON(steps.a.outputs.y) }} ${{ steps.a.outputs.missing }}
`, map[string]string{"token": token}, "token")
	steps := []plan.StepResult{{Outcome: plan.OutcomeCancelled, Outputs: map[string]string{"y": "x " + token}}, {Outcome: plan.OutcomeCancelled}}
	s, diags, err := rj.Cleanup(t.Context(), 1, plan.JobState{Run: run, Masked: run.Masked(), Steps: steps})
	if err != nil || s != nil || !slices.Equal(codes(diags), []string{"from_json_invalid /jobs/main/steps/1/cleanup"}) || strings.Contains(fmt.Sprint(diags), "QQQQ") {
		t.Errorf("cleanup %+v, %v, %v", s, diags, err)
	}
	steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"y": "x " + token}}, {Outcome: plan.OutcomeCancelled, Outputs: map[string]string{"w": "w " + token}}}
	end, _, err := rj.End(t.Context(), plan.JobState{Run: run, Masked: run.Masked(), Steps: steps})
	if err != nil || end.Outputs["w"] != "w "+token || fmt.Sprint(end.Secrets, end.SecretSteps) != "[w] map[a:[y]]" {
		t.Errorf("outputs %q, %v", end, err)
	}
}

// BenchmarkMaskRandomTokens masks a log with tokens whose first bytes are
// common, and with a value that matches far into each line, which are
// harder cases than BenchmarkMask's.
func BenchmarkMaskRandomTokens(b *testing.B) {
	text := strings.Repeat("2026-10-22T10:00:00Z INFO building target //pkg/foo:bar in 12.3s, cache hit ratio 0.93\n", 1<<14)
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	r := rand.New(rand.NewPCG(5, 8))
	for _, n := range []int{3, 30} {
		var values []string
		for range n {
			var v strings.Builder
			for range 40 {
				v.WriteByte(chars[r.IntN(len(chars))])
			}
			values = append(values, v.String())
		}
		m := plan.NewMasker(values...)
		b.Run(fmt.Sprintf("%d tokens", n), func(b *testing.B) {
			b.SetBytes(int64(len(text)))
			for b.Loop() {
				_ = m.Mask(text)
			}
		})
	}
	m := plan.NewMasker(" INFO building target //pkg/foo:baz")
	b.Run("a value that matches far into each line", func(b *testing.B) {
		b.SetBytes(int64(len(text)))
		for b.Loop() {
			_ = m.Mask(text)
		}
	})
}

// Of a structured secret's short fields, only those kept under a key a
// credential is kept under are masked, such as a password, and not the
// rest, such as a database's name or a region, which common text holds.
func TestMaskCredentialFieldsOnly(t *testing.T) {
	dsn := `{"host":"db.internal","port":"5432","user":"app","password":"hunt3r2","dbname":"orders","sslmode":"require"}`
	aws := `{"aws_access_key_id":"AKIAIOSFODNN7EXAMPLE","aws_secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY","region":"us-east-1","output":"json"}`
	kube := `{"apiVersion":"v1","kind":"Config","clusters":[{"name":"prod","cluster":{"server":"https://10.0.0.1:6443"}}],"users":[{"name":"admin","user":{"token":"eyJhbGciOi.payload.sig","pin":"4821"}}],"contexts":[{"name":"prod","context":{"cluster":"prod","user":"admin","namespace":"default"}}],"current-context":"prod"}`
	for _, tc := range []struct {
		name, secret, workflow string
		masked                 []string
		logs                   map[string]string
	}{
		{"dsn", dsn, `
jobs:
  main:
    steps:
      - run: psql "port=${{ fromJSON(vars.s).port }} password=${{ fromJSON(vars.s).password }} dbname=${{ fromJSON(vars.s).dbname }} sslmode=${{ fromJSON(vars.s).sslmode }}"
`, []string{"hunt3r2", `psql "port=5432 password=hunt3r2 dbname=orders sslmode=require"`}, map[string]string{
			"processing 5432 orders": "processing 5432 orders", "Requirement already satisfied": "Requirement already satisfied",
			"auth failed for hunt3r2": "auth failed for ***",
		}},
		{"aws", aws, `
jobs:
  main:
    steps:
      - run: aws configure set output ${{ fromJSON(vars.s).output }}
`, []string{"aws configure set output json"}, map[string]string{"writing json report": "writing json report"}},
		{"kube", kube, `
jobs:
  main:
    steps:
      - uses: workflow/deploy
        with:
          config: ${{ fromJSON(vars.s) }}
`, []string{"https://10.0.0.1:6443", "4821", "eyJhbGciOi.payload.sig"}, map[string]string{
			"namespace: default": "namespace: default", "Running on prod": "Running on prod", "user admin logged in": "user admin logged in",
			"pin 4821 accepted": "pin *** accepted",
		}},
	} {
		rj, run := readyMain(t, tc.workflow, map[string]string{"s": tc.secret}, "s")
		s, diags, err := rj.Step(t.Context(), 0, plan.JobState{Run: run, Masked: run.Masked()})
		if err != nil || s == nil {
			t.Fatal(diags, err)
		}
		if !slices.Equal(s.Masked, tc.masked) {
			t.Errorf("%s: Masked = %q, want %q", tc.name, s.Masked, tc.masked)
		}
		m := plan.NewMasker(append(run.Masked(), s.Masked...)...)
		for log, want := range tc.logs {
			if got := m.Mask(log); got != want {
				t.Errorf("%s: Mask(%q) = %q, want %q", tc.name, log, got, want)
			}
		}
	}
}

// A step's output that holds a structured secret's field that isn't a
// credential, such as a database's name, holds no secret.
func TestOutputHoldsNoField(t *testing.T) {
	rj, run := readyMain(t, `
jobs:
  main:
    env:
      DB: ${{ fromJSON(vars.s).dbname }}
    steps:
      - id: a
        run: "true"
      - run: "true"
    outputs:
      summary: ${{ steps.a.outputs.summary }}
`, map[string]string{"s": `{"dbname":"orders","password":"hunt3r2"}`}, "s")
	state := plan.JobState{Run: run, Masked: run.Masked(), Steps: []plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"summary": "12 orders shipped"}}, {Outcome: plan.OutcomeSuccess},
	}}
	end, diags, err := rj.End(t.Context(), state)
	if err != nil || end.Outputs["summary"] != "12 orders shipped" || end.Private != nil || len(diags) > 0 {
		t.Errorf("End %+v, %v, %v", end, diags, err)
	}
}

// A template of many expressions notes each string once.
func TestMaskManyParts(t *testing.T) {
	var b strings.Builder
	for range 2000 {
		b.WriteString("${{ fromJSON(vars.s).password }} ${{ vars.s }} ")
	}
	rj, run := readyMain(t, "jobs:\n  main:\n    steps:\n      - run: "+b.String()+"\n", map[string]string{"s": `{"password":"hunt3r2"}`}, "s")
	s, diags, err := rj.Step(t.Context(), 0, plan.JobState{Run: run, Masked: run.Masked()})
	if err != nil || s == nil || len(s.Masked) != 3 {
		t.Errorf("%v, %v, masked %d", diags, err, len(s.Masked))
	}
}

// BenchmarkStepStructuredSecret evaluates a step whose job's env: reads a
// structured secret of many fields whole, with and without the job's env:
// as JobEnv gave it.
func BenchmarkStepStructuredSecret(b *testing.B) {
	for _, n := range []int{5000, 20000} {
		m := map[string]string{}
		for i := range n {
			m[fmt.Sprintf("k%06d", i)] = fmt.Sprintf("v%04d", i%10000)
		}
		data, err := json.Marshal(m)
		if err != nil {
			b.Fatal(err)
		}
		rj, run := readyMain(b, "jobs:\n  main:\n    env:\n      CFG: ${{ fromJSON(vars.s) }}\n    steps:\n      - run: \"true\"\n", map[string]string{"s": string(data)}, "s")
		state := plan.JobState{Run: run, Masked: run.Masked()}
		b.Run(fmt.Sprintf("%d fields", n), func(b *testing.B) {
			for b.Loop() {
				_, _, _ = rj.Step(b.Context(), 0, state)
			}
		})
		env, _, err := rj.JobEnv(b.Context(), state)
		if err != nil {
			b.Fatal(err)
		}
		with := state
		with.JobEnv = env
		b.Run(fmt.Sprintf("%d fields, JobEnv given", n), func(b *testing.B) {
			for b.Loop() {
				_, _, _ = rj.Step(b.Context(), 0, with)
			}
		})
	}
}

// A value the run was given in the place of what is made from a secret
// (RunValues.Derived) is masked as what is made from a secret is: from 8
// bytes, or as a credential field of a secret's own text, which is masked
// however short. One given in the place of what is made from a variable
// (RunValues.Private) is private, and never masked. Split, JoinRunValues
// and Reads keep both.
func TestRunValuesDerivedAndPrivate(t *testing.T) {
	run := plan.RunValues{
		Inputs: plan.DataOf(map[string]any{
			"pw": "hunter2-secret", "short": "abc1", "typed": "typed-over-1", "typed_short": "pin1", "field": "hunt3r2", "region": "eu-west",
		}),
		Vars:    map[string]string{"db": `{"user":"svc","password":"hunt3r2"}`},
		Secrets: []string{"vars.db", "inputs.pw", "inputs.short", "inputs.typed", "inputs.typed_short", "inputs.field"},
		Derived: []string{"inputs.typed", "inputs.typed_short", "inputs.field"},
		Private: []string{"inputs.region"},
	}
	want := []string{`{"user":"svc","password":"hunt3r2"}`, "hunter2-secret", "abc1", "hunt3r2", "typed-over-1"}
	if got := run.Masked(); !slices.Equal(got, want) {
		t.Errorf("Masked() = %q, want %q", got, want)
	}
	record, state := run.Split()
	if fmt.Sprint(state.Derived, state.Private) != "[inputs.typed inputs.typed_short inputs.field] [inputs.region]" {
		t.Errorf("state names %q and %q", state.Derived, state.Private)
	}
	if strings.Contains(fmt.Sprint(state.Inputs.Get()), "typed") || strings.Contains(fmt.Sprint(state.Inputs.Get()), "eu-west") {
		t.Errorf("the state holds %v", state.Inputs.Get())
	}
	if !slices.Equal(record.Derived, nil) || fmt.Sprint(record.Inputs.Get()) == "map[]" {
		t.Errorf("the record %+v", record)
	}
	back := plan.JoinRunValues(record, state)
	a, _ := json.Marshal(run, json.Deterministic(true))
	b, _ := json.Marshal(back, json.Deterministic(true))
	if string(a) != string(b) {
		t.Errorf("joined\n%s\nwant\n%s", b, a)
	}
	j := &plan.Job{Name: "j", Steps: []plan.Step{{Run: mustValue(t, "${{ inputs.typed_short }} ${{ inputs.region }}")}}}
	if got := j.Reads(run); fmt.Sprint(got.Secrets, got.Derived, got.Private) != "[inputs.typed_short] [inputs.typed_short] [inputs.region]" {
		t.Errorf("Reads gives secrets %q, derived %q, private %q", got.Secrets, got.Derived, got.Private)
	}
	p := compiled(t, `
on:
  execute:
    inputs:
      typed_short: {type: string, default: x}
      region: {type: string, default: x}
jobs:
  main:
    steps:
      - id: a
        run: echo ${{ inputs.typed_short }} ${{ inputs.region }}
    outputs:
      typed: ${{ inputs.typed_short }}
      region: ${{ inputs.region }}
`, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state2 := plan.JobState{Run: run, Masked: run.Masked()}
	s, _, err := ready.Job.Step(t.Context(), 0, state2)
	if err != nil || !slices.Equal(s.Masked, []string{"echo pin1 eu-west"}) {
		t.Errorf("step %+v, %v", s, err)
	}
	if got := plan.NewMasker(append(state2.Masked, s.Masked...)...).Mask("pin1 eu-west"); got != "pin1 eu-west" {
		t.Errorf("a log line %q", got)
	}
	state2.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess}}
	end, _, err := ready.Job.End(t.Context(), state2)
	if err != nil || fmt.Sprint(end.Outputs, end.Private, end.Secrets) != "map[region:eu-west typed:pin1] [region typed] [typed]" {
		t.Errorf("End %+v, %v", end, err)
	}
}

// An output that holds a secret's own text, however short, reaches the
// jobs that need the job as one: they mask it however short, and an
// output of theirs that holds it holds a secret. One that holds only what
// was made from a secret (EndResult.Derived) is masked as what is made
// from one.
func TestNeedsShortSecret(t *testing.T) {
	rjA, runA := readyMain(t, `
jobs:
  main:
    steps:
      - run: "true"
    outputs:
      pin: ${{ vars.pin }}
      has: ${{ vars.pin != '' && 'yes' || 'no' }}
`, map[string]string{"pin": "4821"}, "pin")
	endA, _, err := rjA.End(t.Context(), plan.JobState{Run: runA, Masked: runA.Masked(), Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess}}})
	if err != nil || fmt.Sprint(endA.Secrets, endA.Derived) != "[has pin] [has]" {
		t.Fatalf("a: %+v, %v", endA, err)
	}
	resA := plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: endA.Outputs, Private: endA.Private, Secrets: endA.Secrets, Derived: endA.Derived}
	p := compiled(t, `
jobs:
  a:
    steps:
      - run: "true"
    outputs:
      pin: ${{ 'x' }}
      has: ${{ 'x' }}
  main:
    needs: [a]
    steps:
      - run: ./unlock.sh
        env:
          PIN: ${{ needs.a.outputs.pin }}
          HAS: ${{ needs.a.outputs.has }}
      - run: "true"
`, nil)
	run := runValues(t, p, nil)
	needs := map[string]plan.JobResult{"a": resA}
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run, Needs: needs})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: job(t, p, "main").Reads(run), Masked: run.Masked(), Needs: needs}
	s, _, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil {
		t.Fatal(err)
	}
	state.Masked = append(state.Masked, s.Masked...)
	if log := plan.NewMasker(state.Masked...).Mask("unlocking with PIN 4821: yes"); log != "unlocking with PIN ***: yes" {
		t.Errorf("b's log %q", log)
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"used": "pin=4821", "said": "yes"}}, {Outcome: plan.OutcomeSuccess}}
	end, _, err := ready.Job.End(t.Context(), state)
	if err != nil || fmt.Sprint(end.Private, end.Secrets, end.Derived) != "[used] [used] []" {
		t.Errorf("b: %+v, %v", end, err)
	}
}

// End names each output that holds a secret, the cleanups' among them, and
// those of them that hold only what was made from one, and keeps the rest
// as they are; Combine keeps both.
func TestEndNamesSecrets(t *testing.T) {
	token := "tok_" + strings.Repeat("Z", 36)
	rj, run := readyMain(t, `
jobs:
  main:
    env:
      HAS: ${{ vars.token != '' && 'yes' || 'no' }}
      DB: ${{ fromJSON(vars.db).dbname }}
    steps:
      - id: a
        run: "true"
      - run: "true"
        cleanup: "true"
    outputs:
      ok: ${{ steps.a.outputs.ok }}
      summary: ${{ steps.a.outputs.summary }}
      held: ${{ steps.a.outputs.held }}
      made: ${{ format('x{0}', vars.token) }}
      has: ${{ env.HAS }}
      region: ${{ vars.region }}
`, map[string]string{"token": token, "db": `{"dbname":"orders","password":"hunt3r2"}`, "region": "eu"}, "token", "db")
	state := plan.JobState{Run: run, Masked: run.Masked(), Steps: []plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"ok": "true", "summary": "12 orders shipped yes", "held": "got " + token}},
		{Outcome: plan.OutcomeCancelled, Outputs: map[string]string{"w": "w " + token, "plain": "fine"}, CleanupOutcome: plan.OutcomeSuccess, CleanupOutputs: map[string]string{"c": "pw hunt3r2"}},
	}}
	end, _, err := rj.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(end.Private, end.Secrets, end.Derived); got != "[c has held made region w] [c has held made w] [has]" {
		t.Errorf("private, secrets and derived %s", got)
	}
	if end.Outputs["held"] != "got "+token || end.Outputs["c"] != "pw hunt3r2" {
		t.Errorf("outputs %q", end.Outputs)
	}
	r := plan.Combine([]plan.JobResult{
		{Outcome: plan.OutcomeSuccess, Outputs: end.Outputs, Private: end.Private, Secrets: end.Secrets, Derived: end.Derived},
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"made": "plain"}},
	})
	if got := fmt.Sprintf("%s %v %v", r.Outputs["made"], r.Secrets, r.Derived); got != "plain [c has held w] [has]" {
		t.Errorf("combined %s", got)
	}
}

// resultOf returns the result of a job that ended as e gave.
func resultOf(e *plan.EndResult) plan.JobResult {
	return plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: e.Outputs, Private: e.Private, Secrets: e.Secrets, Derived: e.Derived}
}

// A secret's own text a job gives another as an output stays one through a
// chain of jobs: b forwards a's PIN as an output, and writes it from a
// step, and c, which needs b, masks it in its log and names its own output
// that holds it a secret, not derived; b's output made from the PIN's
// length is derived.
func TestNeedsChain(t *testing.T) {
	rjA, runA := readyMain(t, `
jobs:
  main:
    steps:
      - run: "true"
    outputs:
      pin: ${{ vars.pin }}
      len: ${{ length(vars.pin) }}
`, map[string]string{"pin": "4821"}, "pin")
	endA, _, err := rjA.End(t.Context(), plan.JobState{Run: runA, Masked: runA.Masked(), Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess}}})
	if err != nil {
		t.Fatal(err)
	}
	p := compiled(t, `
jobs:
  a:
    steps:
      - run: "true"
    outputs:
      pin: ${{ 'x' }}
      len: ${{ 'x' }}
  b:
    needs: [a]
    steps:
      - run: ./forward.sh
        env:
          PIN: ${{ needs.a.outputs.pin }}
    outputs:
      fwd: ${{ needs.a.outputs.pin }}
      n: ${{ needs.a.outputs.len }}
  c:
    needs: [b]
    steps:
      - run: ./use.sh
        env:
          A: ${{ needs.b.outputs.fwd }}
          B: ${{ needs.b.outputs.written }}
`, nil)
	run := runValues(t, p, nil)
	runJob := func(name string, needs map[string]plan.JobResult, outputs map[string]string) (*plan.EndResult, []string) {
		ready, _, err := job(t, p, name).Ready(t.Context(), plan.ReadyInput{Run: run, Needs: needs})
		if err != nil || ready.Job == nil {
			t.Fatal(err)
		}
		state := plan.JobState{Run: job(t, p, name).Reads(run), Masked: run.Masked(), Needs: needs}
		je, _, err := ready.Job.JobEnv(t.Context(), state)
		if err != nil {
			t.Fatal(err)
		}
		state.JobEnv = je
		s, _, err := ready.Job.Step(t.Context(), 0, state)
		if err != nil || s == nil {
			t.Fatal(err)
		}
		state.Masked = append(append(state.Masked, je.Masked...), s.Masked...)
		state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: outputs}}
		end, _, err := ready.Job.End(t.Context(), state)
		if err != nil {
			t.Fatal(err)
		}
		return end, state.Masked
	}
	endB, _ := runJob("b", map[string]plan.JobResult{"a": resultOf(endA)}, map[string]string{"written": "pin=4821"})
	endC, maskedC := runJob("c", map[string]plan.JobResult{"b": resultOf(endB)}, map[string]string{"x": "used 4821"})
	log := plan.NewMasker(maskedC...).Mask("unlocking with 4821")
	if strings.Contains(log, "4821") {
		t.Errorf("c's log: %q", log)
	}
	if !slices.Contains(endC.Secrets, "x") || slices.Contains(endC.Derived, "x") {
		t.Errorf("c's output that holds the PIN: secrets %v derived %v", endC.Secrets, endC.Derived)
	}
	if slices.Contains(endB.Derived, "fwd") || !slices.Contains(endB.Derived, "n") {
		t.Errorf("b derived %v: fwd is own text, n is made from it", endB.Derived)
	}
}

// BenchmarkAttemptLargeSecret evaluates the steps and End of an attempt
// whose run has a large structured secret, with the job's env: as JobEnv
// gave it, which keeps what masks the secret for the attempt.
func BenchmarkAttemptLargeSecret(b *testing.B) {
	m := map[string]any{}
	for i := range 5000 {
		m[fmt.Sprintf("svc%05d", i)] = map[string]any{"user": "u", "password": fmt.Sprintf("pw%06d", i), "host": "h"}
	}
	data, err := json.Marshal(m)
	if err != nil {
		b.Fatal(err)
	}
	rj, run := readyMain(b, "jobs:\n  main:\n    steps:\n      - run: echo ${{ fromJSON(vars.big).svc00001.password }}\n      - run: \"true\"\n", map[string]string{"big": string(data)}, "big")
	for b.Loop() {
		state := plan.JobState{Run: run, Masked: run.Masked()}
		env, _, err := rj.JobEnv(b.Context(), state)
		if err != nil {
			b.Fatal(err)
		}
		state.JobEnv = env
		for i := range 2 {
			state.Steps = make([]plan.StepResult, i)
			for k := range state.Steps {
				state.Steps[k] = plan.StepResult{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "plain"}}
			}
			if _, _, err := rj.Step(b.Context(), i, state); err != nil {
				b.Fatal(err)
			}
		}
		state.Steps = append(state.Steps, plan.StepResult{Outcome: plan.OutcomeSuccess})
		if _, _, err := rj.End(b.Context(), state); err != nil {
			b.Fatal(err)
		}
	}
}
