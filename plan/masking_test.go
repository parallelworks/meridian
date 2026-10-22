package plan_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
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
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	for _, secret := range []string{
		"ghp_" + strings.Repeat("Z", 36), // 40 characters, as a GitHub token is
		"pa\"ss\"wordQQQ",                // a quote, which JSON escapes
		"line1QQQ\nline2QQQ",             // a line break
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
		if len(diags) != 2 {
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
	if fmt.Sprint(s.Masked) != "[s3cret-value-1 s3cret-value]" {
		t.Errorf("Masked = %q", s.Masked)
	}
	state.Masked = append(state.Masked, s.Masked...)
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"ok": "true", "json": `{"deployed":true}`}}}
	end, diags, err := ready.Job.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if end.Outputs["ok"] != "true" || end.Outputs["json"] != `{"deployed":true}` || end.Outputs["has"] != "***" {
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

// What one output: is made of masks none of the others, so they don't
// depend on the order of their names.
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
	if end.Outputs["a_zone"] != "us-6" || end.Outputs["z_zone"] != "us-6" || end.Outputs["length"] != "***" {
		t.Errorf("outputs %v, %q", end.Outputs, codes(diags))
	}
	if fmt.Sprint(end.Private) != "[a_zone z_zone]" {
		t.Errorf("private outputs %q", end.Private)
	}
}

// A matrix made from a secret variable shows none of its keys in a
// problem, nor in the problem's JSON Pointer, but those the workflow
// writes.
func TestExpandHidesPrivateKeys(t *testing.T) {
	for matrix, want := range map[string]string{
		`{"hunter2-s3cret": 1}`: "/jobs/j/strategy/matrix",
		`{"include": {"a": 1}}`: "/jobs/j/strategy/matrix/include",
	} {
		job := &plan.Job{Name: "j", Path: "/jobs/j", Strategy: &plan.Strategy{Matrix: mustValue(t, "${{ fromJSON(vars.m) }}")}}
		scope := plan.NewScope(plan.PhaseStrategy, plan.Contexts{Run: plan.RunValues{
			Vars: map[string]string{"m": matrix}, Secrets: []string{"vars.m"},
		}})
		_, err := plan.Expand(t.Context(), job, scope, plan.Limits{})
		xe, ok := errors.AsType[*plan.ExpandError](err)
		if !ok || xe.Path != want || strings.Contains(fmt.Sprint(err, xe.Params, xe.Path), "hunter2") {
			t.Errorf("%s: %v, %v, path %q", matrix, err, xe, want)
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
// line break, a line at a time, with CRLF line breaks, and escaped in JSON.
// Its short lines aren't masked on their own.
func TestMaskShownForms(t *testing.T) {
	m := plan.NewMasker("tok3n-value\n", "-----BEGIN KEY-----\nMIIEabc\nxyz\n-----END KEY-----", "pa\"ss\\wd")
	for s, want := range map[string]string{
		`Authorization: tok3n-value"`:                                `Authorization: ***"`,
		"-----BEGIN KEY-----\r\nMIIEabc\r\nxyz\r\n-----END KEY-----": "***\r\n***\r\nxyz\r\n***",
		"+ echo MIIEabc":            "+ echo ***",
		`{"pw":"pa\"ss\\wd"}`:       `{"pw":"***"}`,
		"xyz is short, so it shows": "xyz is short, so it shows",
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
		"${{ inputs.other }} ${{ inputs.* }}": `map[config:{"x": 1} hosts:[map[name:a pass:pw-a] map[name:b pass:pw-b]] list:[p q] other:o] [inputs.hosts.0.pass inputs.hosts.1.pass]`,
	} {
		j := &plan.Job{Name: "j", Steps: []plan.Step{{Run: mustValue(t, src)}}}
		got := j.Reads(run)
		if s := fmt.Sprint(got.Inputs.Get(), got.Secrets); s != want {
			t.Errorf("%s reads %s, want %s", src, s, want)
		}
	}
}
