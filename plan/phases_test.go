package plan_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// compiled compiles a workflow, which must compile without errors.
func compiled(t testing.TB, workflow string, contexts map[string]any) *plan.Plan {
	t.Helper()
	p, diags := compile.Compile("workflow.yaml", []byte(workflow), nil, compile.Options{Contexts: contexts})
	if diags.HasErrors() {
		t.Fatalf("compiling: %v", diags)
	}
	return p
}

// job returns the plan's job of name.
func job(t testing.TB, p *plan.Plan, name string) *plan.Job {
	t.Helper()
	for i := range p.Jobs {
		if p.Jobs[i].Name == name {
			return &p.Jobs[i]
		}
	}
	t.Fatalf("no job %s", name)
	return nil
}

// runValues returns the run's values of a plan whose env: holds no
// expressions, as Start would complete them, with its user variables and
// secrets.
func runValues(t testing.TB, p *plan.Plan, vars map[string]string) plan.RunValues {
	t.Helper()
	r := plan.RunValues{
		Format: p.Run.Format, Inputs: p.Run.Inputs, Vars: vars, Secrets: p.Run.Secrets, Derived: p.Run.Derived, Private: p.Run.Private,
	}
	for name, v := range p.Run.Env {
		lit, ok := v.Literal()
		if !ok {
			t.Fatalf("env %s holds an expression", name)
		}
		if r.Env == nil {
			r.Env = map[string]string{}
		}
		r.Env[name], _ = lit.(string)
	}
	return r
}

// codes returns the codes of diagnostics and their paths, and their params
// that show a value.
func codes(ds problems.Diagnostics) []string {
	var out []string
	for _, d := range ds {
		s := string(d.Code) + " " + d.Path()
		if v, ok := d.Params["value"]; ok {
			s += fmt.Sprintf(" %v", v)
		}
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

func TestCombine(t *testing.T) {
	r := func(o plan.Outcome, outputs ...string) plan.JobResult {
		res := plan.JobResult{Outcome: o}
		for i := 0; i+1 < len(outputs); i += 2 {
			if res.Outputs == nil {
				res.Outputs = map[string]string{}
			}
			res.Outputs[outputs[i]] = outputs[i+1]
		}
		return res
	}
	for _, tc := range []struct {
		in   []plan.JobResult
		want plan.Outcome
	}{
		{[]plan.JobResult{r(plan.OutcomeSuccess), r(plan.OutcomeSuccess)}, plan.OutcomeSuccess},
		{[]plan.JobResult{r(plan.OutcomeSuccess), r(plan.OutcomeSkipped)}, plan.OutcomeSuccess},
		{[]plan.JobResult{r(plan.OutcomeCancelled), r(plan.OutcomeFailure), r(plan.OutcomeSuccess)}, plan.OutcomeFailure},
		{[]plan.JobResult{r(plan.OutcomeCancelled), r(plan.OutcomeSkipped), r(plan.OutcomeSuccess)}, plan.OutcomeCancelled},
		{[]plan.JobResult{r(plan.OutcomeSkipped), r(plan.OutcomeSkipped)}, plan.OutcomeSkipped},
	} {
		if got := plan.Combine(tc.in); got.Outcome != tc.want || got.NeedsFailed {
			t.Errorf("Combine(%v) = %v, want %s", tc.in, got, tc.want)
		}
	}
	got := plan.Combine([]plan.JobResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "0", "b": "0"}, Steps: map[string]map[string]string{"s": {"x": "0", "y": "0"}}},
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "1"}, Steps: map[string]map[string]string{"s": {"x": "1"}}},
		{Outcome: plan.OutcomeSkipped},
	})
	if fmt.Sprint(got.Outputs, got.Steps) != "map[a:1 b:0] map[s:map[x:1 y:0]]" {
		t.Errorf("outputs %v, steps %v: the highest index's win", got.Outputs, got.Steps)
	}
	blocked := plan.Combine([]plan.JobResult{{Outcome: plan.OutcomeSkipped}, {Outcome: plan.OutcomeSkipped, NeedsFailed: true}})
	if blocked.Outcome != plan.OutcomeSkipped || !blocked.NeedsFailed {
		t.Errorf("all skipped, one for its needs: %+v", blocked)
	}
	// As format 1 reads each instance of a job its dependents need: one
	// skipped for its needs fails the job for them, whatever the rest did,
	// and one cancelled cancels it for them, though another failed.
	blocked = plan.Combine([]plan.JobResult{{Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSkipped, NeedsFailed: true}})
	if blocked.Outcome != plan.OutcomeSuccess || !blocked.NeedsFailed || blocked.Cancelled {
		t.Errorf("one skipped for its needs, one succeeded: %+v", blocked)
	}
	both := plan.Combine([]plan.JobResult{{Outcome: plan.OutcomeFailure}, {Outcome: plan.OutcomeCancelled}})
	if both.Outcome != plan.OutcomeFailure || !both.Cancelled || both.NeedsFailed {
		t.Errorf("one failed, one cancelled: %+v", both)
	}
	for _, needs := range []plan.JobResult{blocked, both} {
		status, ok := plan.ReadyStatus(map[string]plan.JobResult{"m": needs})
		if ok || !status.Failed || status.Cancelled != needs.Cancelled {
			t.Errorf("after %+v: %+v, needs succeeded %v", needs, status, ok)
		}
	}
}

// A job reads the inputs and variables its worker's expressions name, each
// of a context it reads whole, and the workflow's env, remote and sessions,
// with the names of what of them is private or secret.
func TestReads(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      a: {type: string, default: x}
      b: {type: string, default: y}
      c: {type: string, default: z}
env:
  E: e
jobs:
  one:
    if: ${{ inputs.c == 'z' && vars.unread != '' }}
    env:
      X: ${{ inputs.a }}
    steps:
      - run: echo ${{ vars.token }} ${{ org.* }}
  two:
    steps:
      - run: echo ${{ inputs.* }}
    outputs:
      v: ${{ vars.region }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}, "region": "eu", "unread": "u"}, "org": map[string]any{"a": "1"}})
	run := runValues(t, p, map[string]string{"token": "t", "region": "eu", "unread": "u"})
	run.Org = map[string]string{"a": "1", "b": "2"}
	run.Remote = map[string]string{"repo": "r"}
	run.Private = []string{"inputs.b", "inputs.a", "org", "vars.region"}
	for name, want := range map[string]string{
		"one": `{"format":1,"inputs":{"a":"x"},"env":{"E":"e"},"org":{"a":"1","b":"2"},"vars":{"token":"t"},"remote":{"repo":"r"},"private":["inputs.a","org"],"secrets":["vars.token"]}`,
		"two": `{"format":1,"inputs":{"a":"x","b":"y","c":"z"},"env":{"E":"e"},"vars":{"region":"eu"},"remote":{"repo":"r"},"private":["inputs.b","inputs.a","vars.region"]}`,
	} {
		got, err := json.Marshal(job(t, p, name).Reads(run), json.Deterministic(true))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("%s reads\n%s\nwant\n%s", name, got, want)
		}
	}
}

// Ready decides a job's if: by its needs' outcomes: one it needs that
// failed, or was skipped because its own needs failed, fails it, and one
// skipped by its own if: doesn't. A job it skips because of them is
// skipped because its needs failed.
func TestReadyStatus(t *testing.T) {
	p := compiled(t, `
jobs:
  dep:
    steps: [{run: "true"}]
  plain:
    needs: [dep]
    steps: [{run: "true"}]
  cond:
    needs: [dep]
    if: ${{ needs.dep.result != 'cancelled' }}
    steps: [{run: "true"}]
  on-failure:
    needs: [dep]
    if: failure()
    steps: [{run: "true"}]
  on-cancel:
    needs: [dep]
    if: cancelled()
    steps: [{run: "true"}]
  word:
    needs: [dep]
    if: always
    steps: [{run: "true"}]
  value-always:
    needs: [dep]
    if: ${{ 'always' }}
    steps: [{run: "true"}]
  falsy:
    needs: [dep]
    if: ${{ null }}
    steps: [{run: "true"}]
`, nil)
	run := runValues(t, p, nil)
	type result struct{ skipped, needsFailed bool }
	outcomes := []plan.JobResult{
		{Outcome: plan.OutcomeSuccess},
		{Outcome: plan.OutcomeSkipped},
		{Outcome: plan.OutcomeFailure},
		{Outcome: plan.OutcomeSkipped, NeedsFailed: true},
		{Outcome: plan.OutcomeCancelled},
	}
	for name, want := range map[string][]result{
		"plain":        {{}, {}, {true, true}, {true, true}, {true, true}},
		"cond":         {{}, {}, {true, true}, {true, true}, {true, true}},
		"on-failure":   {{true, false}, {true, false}, {}, {}, {true, true}},
		"on-cancel":    {{true, false}, {true, false}, {true, true}, {true, true}, {}},
		"word":         {{}, {}, {}, {}, {}},
		"value-always": {{}, {}, {}, {}, {}},
		"falsy":        {{true, false}, {true, false}, {true, true}, {true, true}, {true, true}},
	} {
		for i, dep := range outcomes {
			r, diags, err := job(t, p, name).Ready(t.Context(), plan.ReadyInput{Run: run, Needs: map[string]plan.JobResult{"dep": dep}})
			if err != nil || len(diags) > 0 {
				t.Fatalf("%s: %v, %v", name, diags, err)
			}
			if got := (result{r.Skipped, r.NeedsFailed}); got != want[i] || (r.Job == nil) != r.Skipped {
				t.Errorf("%s after %+v: skipped %v, needs failed %v; want %+v", name, dep, r.Skipped, r.NeedsFailed, want[i])
			}
		}
	}
	// A run being cancelled skips a job whose if: is success(), and runs
	// one whose is cancelled().
	for name, skipped := range map[string]bool{"plain": true, "on-cancel": false, "word": false} {
		r, _, _ := job(t, p, name).Ready(t.Context(), plan.ReadyInput{
			Run: run, Cancelled: true, Needs: map[string]plan.JobResult{"dep": {Outcome: plan.OutcomeSuccess}},
		})
		if r.Skipped != skipped || r.NeedsFailed {
			t.Errorf("%s while cancelling: %+v", name, r)
		}
	}
}

// A matrix job whose if: doesn't read matrix is skipped before its matrix
// is expanded; one whose if: does is expanded, and each instance decides.
// An instance gives the job with its key and matrix values.
func TestReadyMatrix(t *testing.T) {
	p := compiled(t, `
jobs:
  dep:
    steps: [{run: "true"}]
  plain:
    needs: [dep]
    strategy:
      matrix:
        os: ${{ fromJSON(needs.dep.outputs.oses) }}
    runs-on: ${{ matrix.os }}
    steps: [{run: "true"}]
  reads:
    needs: [dep]
    if: ${{ matrix.os != 'mac' }}
    strategy:
      matrix:
        os: [linux, mac]
    steps: [{run: "true"}]
`, nil)
	run := runValues(t, p, nil)
	ctx := t.Context()
	failed := map[string]plan.JobResult{"dep": {Outcome: plan.OutcomeFailure}}
	if r, _, _ := job(t, p, "plain").Ready(ctx, plan.ReadyInput{Run: run, Needs: failed}); !r.Skipped || !r.NeedsFailed {
		t.Errorf("plain after a failure: %+v", r)
	}
	ok := map[string]plan.JobResult{"dep": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"oses": `["linux","mac"]`}}}
	r, diags, err := job(t, p, "plain").Ready(ctx, plan.ReadyInput{Run: run, Needs: ok})
	if err != nil || len(diags) > 0 || r.Expansion == nil || len(r.Expansion.Instances) != 2 {
		t.Fatalf("plain: %+v, %v, %v", r, diags, err)
	}
	in := r.Expansion.Instances[1]
	in.Private, in.Secret = true, true
	r, diags, err = job(t, p, "plain").Ready(ctx, plan.ReadyInput{Run: run, Needs: ok, Instance: &in})
	if err != nil || len(diags) > 0 || r.Job == nil {
		t.Fatalf("plain#1: %+v, %v, %v", r, diags, err)
	}
	if rj := r.Job; rj.Key != "plain#1" || rj.Target.Group != "mac" || fmt.Sprint(rj.Matrix.Get()) != "map[os:mac]" ||
		!slices.Equal(rj.Private, []string{"matrix"}) || !slices.Equal(rj.Secrets, []string{"matrix"}) || rj.Masked != nil {
		t.Errorf("plain#1 is %+v", rj)
	}
	r, _, _ = job(t, p, "reads").Ready(ctx, plan.ReadyInput{Run: run, Needs: failed})
	if r.Expansion == nil {
		t.Fatalf("reads after a failure isn't expanded: %+v", r)
	}
	for _, in := range r.Expansion.Instances {
		r, _, _ := job(t, p, "reads").Ready(ctx, plan.ReadyInput{Run: run, Needs: failed, Instance: &in})
		if !r.Skipped || !r.NeedsFailed {
			t.Errorf("%s after a failure: %+v", in.Key, r)
		}
		r, _, _ = job(t, p, "reads").Ready(ctx, plan.ReadyInput{Run: run, Needs: ok, Instance: &in})
		if r.Skipped != (in.Matrix.Get().(map[string]any)["os"] == "mac") {
			t.Errorf("%s: %+v", in.Key, r)
		}
	}
}

// Ready reads a job's runs-on:, resources:, retry:, timeout: and
// working-directory: as the format writes them, with the design's defaults
// for a retry:.
func TestReadyValues(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      target:
        type: string
        default: ""
jobs:
  group:
    runs-on: ${{ 'gpus' }}
    resources: {cpu: 0.25, memory: 1.5Gi, gpu: "2"}
    retry: {reasons: [worker-lost]}
    timeout: 2h
    working-directory: /scratch
    steps: [{run: "true"}]
  env:
    runs-on:
      environment: {cluster: owner/hpc, name: large}
      params: {walltime: "01:00:00"}
    resources: {cpu: 4, memory: "1000"}
    retry: {reasons: [worker-lost], max-retries: 5, interval: 5m}
    steps: [{run: "true"}]
  target:
    runs-on:
      mode: worker
      targetId: abc
      newWorker: true
    resources: {cpu: 1500m, memory: 2k}
    timeout: ${{ 300 }}
    steps: [{run: "true"}]
  ssh:
    ssh:
      remoteHost: ${{ 'host' }}
      disconnect-timeout: 10
    steps: [{run: "true"}]
  none:
    runs-on: ${{ inputs.target }}
    steps: [{run: "true"}]
`, nil)
	run := runValues(t, p, nil)
	for name, want := range map[string]string{
		"group":  `{"target":{"group":"gpus"},"resources":{"milliCPU":250,"memory":1610612736,"gpu":2},"retry":{"reasons":["worker-lost"],"maxRetries":3,"intervalSeconds":60},"timeoutSeconds":7200}`,
		"env":    `{"target":{"environment":{"cluster":"owner/hpc","name":"large","params":{"walltime":"01:00:00"}}},"resources":{"milliCPU":4000,"memory":1000},"retry":{"reasons":["worker-lost"],"maxRetries":5,"intervalSeconds":300}}`,
		"target": `{"target":{"computeTarget":{"mode":"worker","targetId":"abc","newWorker":true}},"resources":{"milliCPU":1500,"memory":2000},"timeoutSeconds":300}`,
		"ssh":    `{"target":{"inline":{"name":"ssh","backend":"ssh","isolation":"host","ssh":{"disconnect-timeout":10,"remoteHost":"host"}}}}`,
		"none":   `{}`,
	} {
		r, diags, err := job(t, p, name).Ready(t.Context(), plan.ReadyInput{Run: run})
		if err != nil || len(diags) > 0 || r.Job == nil {
			t.Fatalf("%s: %+v, %v, %v", name, r, diags, err)
		}
		rj := *r.Job
		data, err := json.Marshal(struct {
			Target         plan.Target         `json:"target,omitzero"`
			Resources      plan.Resources      `json:"resources,omitzero"`
			Retry          *plan.ReadyJobRetry `json:"retry,omitzero"`
			TimeoutSeconds int64               `json:"timeoutSeconds,omitzero"`
		}{rj.Target, rj.Resources, rj.Retry, rj.TimeoutSeconds}, json.Deterministic(true))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != want {
			t.Errorf("%s:\n%s\nwant\n%s", name, data, want)
		}
	}
}

// A value that doesn't fit its key is a problem with the job, which shows
// the value unless it was made from a variable or a password.
func TestReadyProblems(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      text: {type: string, default: lots}
jobs:
  bad:
    runs-on: ${{ inputs.text == 'lots' && 5 }}
    resources:
      cpu: ${{ inputs.text }}
      memory: 8GB
      gpu: ${{ 1.5 }}
    retry:
      reasons: [worker-lost]
      max-retries: ${{ inputs.text }}
      interval: ${{ inputs.text }}
    timeout: ${{ -1 }}
    steps: [{run: "true"}]
  private:
    resources:
      memory: ${{ vars.mem }}
    timeout: ${{ vars.mem }}
    steps: [{run: "true"}]
  failing:
    if: ${{ fromJSON(vars.mem) }}
    steps: [{run: "true"}]
`, map[string]any{"vars": map[string]any{"mem": "lots"}})
	run := runValues(t, p, map[string]string{"mem": "lots"})
	for name, want := range map[string][]string{
		"bad": {
			"count_invalid /jobs/bad/retry/max-retries \"lots\"",
			"duration_invalid /jobs/bad/retry/interval \"lots\"",
			"duration_invalid /jobs/bad/timeout -1",
			"resources_invalid /jobs/bad/resources/cpu \"lots\"",
			"resources_invalid /jobs/bad/resources/gpu 1.5",
			"resources_invalid /jobs/bad/resources/memory \"8GB\"",
			"runs_on_invalid /jobs/bad/runs-on 5",
		},
		"private": {
			"duration_invalid /jobs/private/timeout ***",
			"resources_invalid /jobs/private/resources/memory ***",
		},
		"failing": {"from_json_invalid /jobs/failing/if"},
	} {
		r, diags, err := job(t, p, name).Ready(t.Context(), plan.ReadyInput{Run: run})
		if err != nil || r != nil {
			t.Errorf("%s: %+v, %v", name, r, err)
		}
		if got := codes(diags); !slices.Equal(got, want) {
			t.Errorf("%s: %q, want %q", name, got, want)
		}
		if name == "failing" && diags[0].Params["text"] != problems.Masked {
			t.Errorf("failing shows %v", diags[0].Params)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := job(t, p, "bad").Ready(ctx, plan.ReadyInput{Run: run}); !errors.Is(err, context.Canceled) {
		t.Errorf("a done context: %v", err)
	}
}

// Step evaluates the job's env:, then the step's if:, its env: and the
// rest of it, with format 1's defaults, and gives the values it made from a
// secret to mask.
func TestStep(t *testing.T) {
	p := compiled(t, `
env:
  W: workflow
jobs:
  main:
    working-directory: /job
    env:
      TOKEN: ${{ vars.token }}
      REGION: ${{ vars.region }}
    steps:
      - id: first
        name: first ${{ matrix.n }}
        run: echo ${{ env.TOKEN }}
        env:
          TOKEN: plain
          X: ${{ env.TOKEN }}
          Y: ${{ vars.token }}-y
        retry: {}
        ignore-errors: ${{ 'true' }}
        cleanup: rm -rf ${{ env.W }}
        working-directory: null
        timeout: 1m
        shell: sh {0}
      - run: echo ${{ steps.first.outputs.o }} ${{ env.TOKEN }}
        if: failure()
        working-directory: ""
        ssh:
          remoteHost: h
      - run: ${{ fromJSON(steps.first.outputs.o) }}
      - uses: workflow/mine
        with:
          a: ${{ env.REGION }}
          n: [1, "${{ job.attempt }}"]
        if: always()
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}, "region": "eu"}})
	run := runValues(t, p, map[string]string{"token": "s3cret", "region": "eu"})
	run.Private = []string{}
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil || ready.Job == nil {
		t.Fatal(err)
	}
	rj := ready.Job
	state := plan.JobState{Run: job(t, p, "main").Reads(run), Job: plan.JobContext{Name: "main", Key: "main", Attempt: 1}}
	s, diags, err := rj.Step(t.Context(), 0, state)
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	data, err := json.Marshal(s, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"index":0,"id":"first","name":"first ","run":"echo plain","shell":"sh {0}","env":{"REGION":"eu","TOKEN":"plain","W":"workflow","X":"plain","Y":"s3cret-y"},"timeoutSeconds":60,"retry":{"maxRetries":10,"intervalSeconds":5,"timeoutSeconds":30,"cleanOnRetry":true},"ignoreErrors":true,"masked":["s3cret-y"]}`; string(data) != want {
		t.Errorf("step 0:\n%s\nwant\n%s", data, want)
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"o": "1"}}}
	state.Masked = []string{"secret-ish"}
	if s, _, _ := rj.Step(t.Context(), 1, state); s == nil || !s.Skipped {
		t.Errorf("step 1 after a success: %+v", s)
	}
	state.Steps[0].Outcome = plan.OutcomeFailure
	s, diags, err = rj.Step(t.Context(), 1, state)
	if err != nil || len(diags) > 0 || s.Skipped {
		t.Fatal(s, diags, err)
	}
	if s.Run != "echo 1 s3cret" || s.WorkingDirectory != "/job" || s.SSH == nil || s.SSH.SSH.Get().(map[string]any)["remoteHost"] != "h" ||
		!slices.Equal(s.Masked, []string{"echo 1 s3cret"}) || s.OutlivesCancel {
		t.Errorf("step 1: %+v", s)
	}
	state.Steps = append(state.Steps, plan.StepResult{Outcome: plan.OutcomeSuccess})
	// A step's outputs are read as written, so a problem with one that
	// holds a secret shows none of it.
	state.Steps[0].Outputs["o"] = "a s3cret value"
	state.Steps[0].Outcome = plan.OutcomeSuccess
	s, diags, _ = rj.Step(t.Context(), 2, state)
	if s != nil || !slices.Equal(codes(diags), []string{"from_json_invalid /jobs/main/steps/2/run"}) || diags[0].Params["text"] != problems.Masked || strings.Contains(fmt.Sprint(diags), "s3cret") {
		t.Errorf("step 2: %+v, %v", s, diags)
	}
	state.Steps = append(state.Steps, plan.StepResult{Outcome: plan.OutcomeFailure})
	s, diags, err = rj.Step(t.Context(), 3, state)
	if err != nil || len(diags) > 0 || s.Uses != "workflow/mine" || fmt.Sprint(s.With.Get()) != "map[a:eu n:[1 1]]" {
		t.Errorf("step 3: %+v, %v, %v", s, diags, err)
	}
	for i, n := range map[int]int{-1: 0, 4: 4, 2: 1} {
		state.Steps = make([]plan.StepResult, n)
		if _, _, err := rj.Step(t.Context(), i, state); err == nil {
			t.Errorf("step %d with %d results before it", i, n)
		}
	}
	state.Steps = make([]plan.StepResult, 1)
	state.Try = 1
	if _, _, err := rj.Step(t.Context(), 1, state); err == nil {
		t.Error("a later try without its first")
	}
}

// Cleanup evaluates a step's cleanup: once the job's steps have run, as a
// step of its own named POST and the step's name, or its index when the
// name isn't text, which outlives a cancel, reads every step's outputs and
// those of the cleanups that ran before it, and reads one that nothing
// wrote as "". It skips the cleanup of a step that didn't run, and of one
// whose if:, evaluated with the job's status as it ends, is false.
func TestCleanup(t *testing.T) {
	p := compiled(t, `
env:
  W: workflow
jobs:
  main:
    steps:
      - id: first
        name: first
        if: ${{ vars.token != '' }}
        run: echo jobid=1 >> $OUTPUTS
        cleanup: rm -rf ${{ env.W }} ${{ needs.main.outputs.late }} [${{ needs.main.outputs.none }}] ${{ vars.token }} ${{ needs.main.outputs.cleaned }} ${{ steps.first.outputs.jobid }}
        timeout: 1m
        retry: {}
        early-cancel: any-job-failed
      - if: success()
        run: "true"
        cleanup: echo gated
      - run: exit 1
        cleanup: echo ${{ needs.main.outputs.late }} ${{ steps.first.outputs.jobid }} ${{ failure() }}
      - run: "true"
        cleanup: never
      - name: ${{ '' }}
        if: always()
        run: "true"
        cleanup: echo ${{ cancelled() }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	run := runValues(t, p, map[string]string{"token": "s3cret"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	rj := ready.Job
	state := plan.JobState{
		Run: run, Cancelled: true,
		Steps: []plan.StepResult{
			{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"jobid": "1"}},
			{Outcome: plan.OutcomeSuccess},
			{Outcome: plan.OutcomeFailure, Outputs: map[string]string{"late": "2"}, CleanupOutputs: map[string]string{"cleaned": "yes"}},
			{Outcome: plan.OutcomeSkipped},
			{Outcome: plan.OutcomeSuccess},
		},
	}
	s, diags, err := rj.Cleanup(t.Context(), 0, state)
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	data, err := json.Marshal(s, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"index":0,"name":"POST first","run":"rm -rf workflow 2 [] s3cret yes 1","env":{"W":"workflow"},"masked":["rm -rf workflow 2 [] s3cret yes 1"],"outlivesCancel":true}`; string(data) != want {
		t.Errorf("cleanup 0:\n%s\nwant\n%s", data, want)
	}
	if s, _, err := rj.Cleanup(t.Context(), 2, state); err != nil || s.Name != "POST Step 2" || s.Run != "echo 2 1 true" || !s.OutlivesCancel {
		t.Errorf("cleanup 2: %+v, %v", s, err)
	}
	if s, _, err := rj.Cleanup(t.Context(), 4, state); err != nil || s.Name != "POST " || s.Run != "echo true" {
		t.Errorf("cleanup 4: %+v, %v", s, err)
	}
	for _, i := range []int{1, 3} {
		if s, diags, err := rj.Cleanup(t.Context(), i, state); err != nil || len(diags) > 0 || !s.Skipped || s.Run != "" {
			t.Errorf("cleanup %d: %+v, %v, %v", i, s, diags, err)
		}
	}
	state.Steps[2].Outcome, state.Cancelled = plan.OutcomeSuccess, false
	if s, _, err := rj.Cleanup(t.Context(), 1, state); err != nil || s.Skipped || s.Run != "echo gated" {
		t.Errorf("cleanup 1 of a job that succeeded: %+v, %v", s, err)
	}
	end, _, err := rj.End(t.Context(), state)
	if err != nil || fmt.Sprint(end.Outputs) != "map[cleaned:yes late:2]" {
		t.Errorf("outputs %+v, %v", end, err)
	}
	for i, n := range map[int]int{-1: 5, 5: 5, 4: 4, 0: 4} {
		state.Steps = make([]plan.StepResult, n)
		if _, _, err := rj.Cleanup(t.Context(), i, state); err == nil {
			t.Errorf("cleanup %d with %d results", i, n)
		}
	}
}

// End gives a job's outputs, its steps' merged in their order with its
// outputs: over them, as they were made: one made from a secret, or that
// holds one, is private and a secret, as is a step's output that holds
// one, and one made from a variable is private. A problem with one shows no part of the secret.
func TestEnd(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - id: a
        run: "true"
    outputs:
      plain: ${{ steps.a.outputs.x }}
      holds: ${{ steps.a.outputs.y }}
      made: ${{ length(vars.token) }}
      empty: ${{ vars.token == 'x' && 'yes' || '' }}
      fails: ${{ fromJSON(steps.a.outputs.y) }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}}})
	run := runValues(t, p, map[string]string{"token": "s3cret"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{
		Run: run, Masked: run.Masked(),
		Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1", "y": "a s3cret"}}},
	}
	end, diags, err := ready.Job.End(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(end.Outputs) != "map[empty: holds:a s3cret made:6 plain:1]" || fmt.Sprint(end.Private) != "[empty holds made]" ||
		fmt.Sprint(end.Secrets) != "[holds made]" || fmt.Sprint(end.SecretSteps) != "map[a:[y]]" {
		t.Errorf("outputs %v, private %q, secrets %q, %q", end.Outputs, end.Private, end.Secrets, end.SecretSteps)
	}
	want := []string{"from_json_invalid /jobs/main/outputs/fails"}
	if got := codes(diags); !slices.Equal(got, want) {
		t.Errorf("diagnostics %q, want %q", got, want)
	}
	if strings.Contains(fmt.Sprint(diags), "s3cret") || diags[0].Params["text"] != problems.Masked {
		t.Errorf("a diagnostic shows the secret: %v", diags)
	}
	state.Steps = nil
	if _, _, err := ready.Job.End(t.Context(), state); err == nil {
		t.Error("End without each step's result")
	}
}

// Expand marks its instances private and secret as the strategy read, and
// keeps what it read out of its problems. It looks at a bounded number of
// combinations, and stops when its context is done.
func TestExpand(t *testing.T) {
	strategy := func(matrix any) *plan.Job {
		return &plan.Job{Name: "j", Path: "/jobs/j", Strategy: &plan.Strategy{Matrix: mustValue(t, matrix)}}
	}
	scope := plan.NewScope(plan.PhaseStrategy, plan.Contexts{Run: plan.RunValues{
		Vars: map[string]string{"list": "[1,2]", "text": "abc"}, Inputs: plan.DataOf(map[string]any{"n": int64(2)}), Secrets: []string{"vars.list"},
	}})
	ctx := t.Context()
	x, err := plan.Expand(ctx, strategy(map[string]any{"n": "${{ fromJSON(vars.list) }}"}), scope, plan.Limits{})
	if err != nil || !x.Instances[0].Private || !x.Instances[1].Secret {
		t.Errorf("from a secret: %+v, %v", x, err)
	}
	x, err = plan.Expand(ctx, strategy(map[string]any{"n": "${{ 1 range inputs.n + 1 }}"}), scope, plan.Limits{})
	if err != nil || x.Instances[0].Private || x.Instances[1].Secret {
		t.Errorf("from an input: %+v, %v", x, err)
	}
	_, err = plan.Expand(ctx, strategy(map[string]any{"n": "${{ vars.text }}"}), scope, plan.Limits{})
	if xe, ok := errors.AsType[*plan.ExpandError](err); !ok || xe.Params["value"] != problems.Masked || xe.Params["key"] != "n" {
		t.Errorf("a variable's text: %v", err)
	}
	_, err = plan.Expand(ctx, strategy(map[string]any{"n": "${{ fromJSON(vars.text) }}"}), scope, plan.Limits{})
	if ee, ok := errors.AsType[*expr.EvalError](err); !ok || ee.Params["text"] != problems.Masked {
		t.Errorf("a variable's text that isn't JSON: %v", err)
	}
	_, err = plan.Expand(ctx, strategy(map[string]any{"n": "${{ needs.a.outputs.b }}"}), plan.NewScope(plan.PhaseStart, plan.Contexts{}), plan.Limits{})
	if ue, ok := errors.AsType[*expr.UnknownError](err); !ok || ue.Name != "needs" {
		t.Errorf("needs at Start: %v", err)
	}
	// Combinations that entries of two keys exclude, more of them than
	// Expand looks at.
	var values, excludes []any
	for i := range 128 {
		values = append(values, int64(i))
		for j := range 128 {
			excludes = append(excludes, map[string]any{"a": int64(i), "b": int64(j)})
		}
	}
	big := map[string]any{"a": values, "b": values, "c": values, "exclude": excludes}
	_, err = plan.Expand(ctx, strategy(big), scope, plan.Limits{})
	if xe, ok := errors.AsType[*plan.ExpandError](err); !ok || xe.Kind != problems.MatrixTooLarge {
		t.Errorf("a matrix of more combinations than Expand looks at: %v", err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := plan.Expand(done, strategy(map[string]any{"n": []any{1}}), scope, plan.Limits{}); !errors.Is(err, context.Canceled) {
		t.Errorf("a done context: %v", err)
	}
	if _, err := plan.Expand(ctx, &plan.Job{Name: "j"}, scope, plan.Limits{}); err == nil {
		t.Error("a job without a strategy")
	}
}

// The quantities a job asks for read as Kubernetes writes them, and
// durations as format 1 does; what isn't one, or doesn't fit, isn't read.
func TestParseQuantities(t *testing.T) {
	type result struct {
		n  int64
		ok bool
	}
	check := func(name string, got result, in any, want result) {
		t.Helper()
		if got != want {
			t.Errorf("%s(%#v) = %v, want %v", name, in, got, want)
		}
	}
	for in, want := range map[any]result{
		int64(4): {4000, true}, 0.5: {500, true}, "1.5": {1500, true}, "500m": {500, true}, "": {0, true},
		0.0001: {0, false}, "1.5m": {0, false}, "-1": {0, false}, int64(-1): {0, false}, "4 cores": {0, false}, true: {0, false},
	} {
		n, ok := plan.ParseCPU(in)
		check("ParseCPU", result{n, ok}, in, want)
	}
	for in, want := range map[any]result{
		"1.5Gi": {1610612736, true}, "8G": {8e9, true}, "512Mi": {512 << 20, true}, int64(1000): {1000, true}, "2k": {2000, true},
		"8E": {8e18, true}, "9Ei": {0, false}, "0.5": {0, false}, "8GB": {0, false}, "8gi": {0, false}, nil: {0, true},
	} {
		n, ok := plan.ParseMemory(in)
		check("ParseMemory", result{n, ok}, in, want)
	}
	for in, want := range map[any]result{
		"2": {2, true}, 2.0: {2, true}, int64(0): {0, true}, 2.5: {0, false}, int64(-1): {0, false}, "two": {0, false},
	} {
		n, ok := plan.ParseGPU(in)
		check("ParseGPU", result{int64(n), ok}, in, want)
	}
	for in, want := range map[any]result{
		"10m": {600, true}, "2d": {172800, true}, "300": {300, true}, "45s": {45, true}, int64(300): {300, true}, 60.0: {60, true},
		"1.5h": {0, false}, int64(-1): {0, false}, "99999999999999999999": {0, false}, "9999999999999999d": {0, false}, "10 m": {0, false},
	} {
		n, ok := plan.ParseDuration(in)
		check("ParseDuration", result{n, ok}, in, want)
	}
}

// A retry's interval, timeout and max-retries that an expression gives as
// null or "" are its defaults, as format 1 reads them, and a timeout: is
// none.
func TestStepRetryEmpty(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      empty: {type: string, default: ""}
jobs:
  main:
    steps:
      - run: "true"
        timeout: ${{ inputs.empty }}
        retry:
          max-retries: ${{ inputs.empty }}
          interval: ${{ inputs.empty }}
          timeout: ${{ inputs.missing }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	s, diags, err := ready.Job.Step(t.Context(), 0, plan.JobState{Run: run})
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	if s.TimeoutSeconds != 0 || *s.Retry != (plan.ReadyStepRetry{MaxRetries: 10, IntervalSeconds: 5, TimeoutSeconds: 30, CleanOnRetry: true}) {
		t.Errorf("timeout %d, retry %+v", s.TimeoutSeconds, s.Retry)
	}
}

// A step outlives its job's cancelling when its if: can be true once the
// job is stopping, as format 1 reads one, whether or not the job is.
func TestStepOutlivesCancel(t *testing.T) {
	forms := map[string]bool{
		"always": true, "${{ always }}": true, "${{ 'always' }}": true,
		"always()": true, "cancelled()": true, "canceled()": true, "${{ !success() }}": true, "${{ !failure() }}": true,
		"${{ always() && inputs.x == 'y' }}": true,
		"success()":                          false, "failure()": false, "${{ !cancelled() }}": false,
		"${{ inputs.x == 'y' }}": false, "": false,
	}
	var b strings.Builder
	b.WriteString("on:\n  execute:\n    inputs:\n      x: {type: string, default: y}\njobs:\n  main:\n    steps:\n      - run: \"true\"\n")
	var order []string
	for form := range forms {
		order = append(order, form)
		fmt.Fprintf(&b, "      - run: \"true\"\n")
		if form != "" {
			fmt.Fprintf(&b, "        if: %q\n", form)
		}
	}
	p := compiled(t, b.String(), nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	for i, form := range order {
		ran := false
		for _, before := range []struct {
			outcome   plan.Outcome
			cancelled bool
		}{{plan.OutcomeSuccess, false}, {plan.OutcomeSuccess, true}, {plan.OutcomeFailure, false}} {
			cancelled := before.cancelled
			state := plan.JobState{Run: run, Steps: make([]plan.StepResult, i+1), Cancelled: cancelled}
			for j := range state.Steps {
				state.Steps[j].Outcome = before.outcome
			}
			s, diags, err := ready.Job.Step(t.Context(), i+1, state)
			if err != nil || diags.HasErrors() {
				t.Fatalf("%q: %v, %v", form, diags, err)
			}
			if s.Skipped {
				continue
			}
			ran = true
			if s.OutlivesCancel != forms[form] {
				t.Errorf("if: %q, cancelled %v, outlives a cancel: %v, want %v", form, cancelled, s.OutlivesCancel, forms[form])
			}
		}
		if !ran {
			t.Errorf("if: %q never runs", form)
		}
	}
}

// Step evaluates a step again for each try, with the worker's variables
// over the env, and reads its if: and retry: at the first: a later try runs
// whatever its if: gives, and has no Retry.
func TestStepTries(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    working-directory: ${{ env.PW_PARENT_JOB_DIR }}/work
    env:
      PW_JOB_DIR: overridden
    steps:
      - run: echo try ${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }} in ${{ env.PW_JOB_DIR }}
        if: ${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY == '0' }}
        retry:
          max-retries: 2
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	var first *plan.ReadyStep
	for try, want := range []string{"echo try 0 in /runs/r/j", "echo try 1 in /runs/r/j"} {
		state := plan.JobState{Run: run, Try: try, FirstTry: first, Env: map[string]string{
			"PW_WORKFLOW_STEP_CURRENT_RETRY": strconv.Itoa(try), "PW_JOB_DIR": "/runs/r/j", "PW_PARENT_JOB_DIR": "/runs/r",
		}}
		s, diags, err := ready.Job.Step(t.Context(), 0, state)
		if err != nil || len(diags) > 0 {
			t.Fatal(diags, err)
		}
		if s.Skipped || s.Run != want || s.WorkingDirectory != "/runs/r/work" || (s.Retry != nil) != (try == 0) {
			t.Errorf("try %d: %+v", try, s)
		}
		first = s
	}
}

// A later try carries over what its first try's if: decided: that it is
// the word always, so that it reads an output that nothing wrote as "", and
// that it outlives a cancel.
func TestStepTryCarriesIf(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - run: echo [${{ needs.main.outputs.none }}]
        if: always
      - run: echo [${{ needs.main.outputs.none }}]
        if: ${{ cancelled() || env.PW_WORKFLOW_STEP_CURRENT_RETRY == '0' }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	for i, always := range []bool{true, false} {
		state := plan.JobState{Run: run, Steps: make([]plan.StepResult, i), Env: map[string]string{"PW_WORKFLOW_STEP_CURRENT_RETRY": "0"}}
		first, diags, err := ready.Job.Step(t.Context(), i, state)
		if err != nil || (len(diags) > 0) != !always {
			t.Fatalf("step %d: %v, %v", i, diags, err)
		}
		if !always {
			// The output read strictly fails the first try; carry over
			// what a first try that ran would give.
			first = &plan.ReadyStep{Index: i, OutlivesCancel: true}
		} else if !first.IfAlways || !first.OutlivesCancel {
			t.Errorf("step %d's first try: %+v", i, first)
		}
		state.Try, state.FirstTry, state.Env = 1, first, map[string]string{"PW_WORKFLOW_STEP_CURRENT_RETRY": "1"}
		s, diags, err := ready.Job.Step(t.Context(), i, state)
		switch {
		case err != nil:
			t.Fatal(err)
		case always && (len(diags) > 0 || s.Skipped || s.Run != "echo []" || !s.IfAlways || !s.OutlivesCancel):
			t.Errorf("step %d's later try: %+v, %v", i, s, diags)
		case !always && (s != nil || len(diags) != 1 || diags[0].Code != "output_not_written"):
			t.Errorf("step %d's later try: %+v, %v", i, s, diags)
		}
	}
}

// The job's env: and working-directory: read only the variables format 1
// defines as a job starts, the same for each step, and a step reads each
// that format 1 defines, but none that it doesn't, such as a credential.
func TestStepWorkerVariables(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    working-directory: /w/${{ env.OUTPUTS }}
    env:
      J: ${{ env.PW_JOB_DIR }}:${{ env.OUTPUTS }}:${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}
    steps:
      - run: echo ${{ env.J }} ${{ env.OUTPUTS }} ${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }} [${{ env.PW_API_KEY }}]
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Env: map[string]string{
		"PW_JOB_DIR": "/j", "OUTPUTS": "/j/out", "PW_WORKFLOW_STEP_CURRENT_RETRY": "0", "PW_API_KEY": "k3y",
	}}
	s, diags, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	if s.Run != "echo /j:: /j/out 0 []" || s.WorkingDirectory != "/w/" || s.Env["J"] != "/j::" {
		t.Errorf("%+v", s)
	}
}

// A working directory that keeps a shell variable fails the step, as no
// shell expands it, and one made from a variable doesn't show it.
func TestStepWorkingDirectoryVariable(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    working-directory: $HOME/work
    steps:
      - run: "true"
      - run: "true"
        working-directory: ${{ vars.dir }}
      - run: "true"
        working-directory: /tmp/${{ 'x' }}
`, map[string]any{"vars": map[string]any{"dir": "${PRIVATE}"}})
	run := runValues(t, p, map[string]string{"dir": "${PRIVATE}"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"working_directory_variable /jobs/main/working-directory $HOME", "working_directory_variable /jobs/main/steps/1/working-directory ***", ""} {
		s, diags, err := ready.Job.Step(t.Context(), i, plan.JobState{Run: run, Steps: make([]plan.StepResult, i)})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, d := range diags {
			got = append(got, fmt.Sprintf("%s %s %v", d.Code, d.Path(), d.Params["variable"]))
		}
		if fmt.Sprint(got) != fmt.Sprint(slices.DeleteFunc([]string{want}, func(s string) bool { return s == "" })) || (want == "") != (s != nil) {
			t.Errorf("step %d: %q, %+v", i, got, s)
		}
	}
}

// Durations and counts read as format 1 writes them, and what it reads as
// left out is the default: 0 without a unit. What isn't one is a problem,
// as a bug fix to format 1 makes it.
func TestDurationsAndCounts(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      v: {type: string, default: ""}
      n: {type: string, default: ""}
jobs:
  main:
    steps:
      - run: "true"
        timeout: ${{ inputs.v }}
        retry:
          interval: ${{ inputs.v }}
          timeout: ${{ inputs.v }}
          max-retries: ${{ inputs.n }}
`, nil)
	for _, tc := range []struct{ v, n, want string }{
		{"+5m", "2", "timeout 300, retry {MaxRetries:2 IntervalSeconds:300 TimeoutSeconds:300 CleanOnRetry:true}"},
		{"0", "+3", "timeout 0, retry {MaxRetries:3 IntervalSeconds:5 TimeoutSeconds:30 CleanOnRetry:true}"},
		{"+0", "-1", "timeout 0, retry {MaxRetries:0 IntervalSeconds:5 TimeoutSeconds:30 CleanOnRetry:true}"},
		{"0s", "0", "timeout 0, retry {MaxRetries:0 IntervalSeconds:0 TimeoutSeconds:0 CleanOnRetry:true}"},
		{"3", "7", "timeout 3, retry {MaxRetries:7 IntervalSeconds:3 TimeoutSeconds:3 CleanOnRetry:true}"},
		{"3", " 7\n", "timeout 3, retry {MaxRetries:7 IntervalSeconds:3 TimeoutSeconds:3 CleanOnRetry:true}"},
		{"3", "7 tries", "[count_invalid]"},
		{"", "", "timeout 0, retry {MaxRetries:10 IntervalSeconds:5 TimeoutSeconds:30 CleanOnRetry:true}"},
		{"-5m", "3.5", "[count_invalid duration_invalid duration_invalid duration_invalid]"},
		{"3.5", "1e3", "[count_invalid duration_invalid duration_invalid duration_invalid]"},
		{" 7 ", "0x10", "[count_invalid duration_invalid duration_invalid duration_invalid]"},
		{"1e3", "2d", "[count_invalid duration_invalid duration_invalid duration_invalid]"},
	} {
		run := runValues(t, p, nil)
		run.Inputs = plan.DataOf(map[string]any{"v": tc.v, "n": tc.n})
		ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
		if err != nil {
			t.Fatal(err)
		}
		s, diags, err := ready.Job.Step(t.Context(), 0, plan.JobState{Run: run})
		if err != nil {
			t.Fatal(err)
		}
		got := fmt.Sprintf("[%s]", strings.Join(slices.Sorted(slices.Values(codeList(diags))), " "))
		if s != nil {
			got = fmt.Sprintf("timeout %d, retry %+v", s.TimeoutSeconds, *s.Retry)
		}
		if got != tc.want {
			t.Errorf("%q, %q: %s, want %s", tc.v, tc.n, got, tc.want)
		}
	}
}

// A step's retry: interval: or timeout: that is a number with a fraction
// is rounded down, as format 1 reads it, and one under a second is the
// default; a timeout: that is one is a problem, as a duration is a whole
// number of seconds.
func TestStepRetryFractions(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - run: "true"
        retry:
          interval: ${{ 1.9 }}
          timeout: ${{ 0.5 }}
      - run: "true"
        timeout: ${{ 1.5 }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	s, diags, err := ready.Job.Step(t.Context(), 0, plan.JobState{Run: run})
	if err != nil || len(diags) > 0 || *s.Retry != (plan.ReadyStepRetry{MaxRetries: 10, IntervalSeconds: 1, TimeoutSeconds: 30, CleanOnRetry: true}) {
		t.Errorf("retry %+v, %v, %v", s, diags, err)
	}
	s, diags, err = ready.Job.Step(t.Context(), 1, plan.JobState{Run: run, Steps: make([]plan.StepResult, 1)})
	if err != nil || s != nil || !slices.Equal(codeList(diags), []string{"duration_invalid"}) {
		t.Errorf("timeout %+v, %v, %v", s, diags, err)
	}
}

func codeList(ds problems.Diagnostics) []string {
	var out []string
	for _, d := range ds {
		out = append(out, string(d.Code))
	}
	return out
}

// A job's outputs are those its steps without an id wrote, merged in
// their order, the later step's where two wrote the same name, and its
// outputs: over them, as format 1's one $OUTPUTS file has them.
func TestEndMergesOutputsInOrder(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - run: "true"
      - id: own
        run: "true"
      - run: "true"
    outputs:
      declared: ${{ needs.main.outputs.a }}
      b: ${{ 'declared' }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	end, diags, err := ready.Job.End(t.Context(), plan.JobState{Run: run, Steps: []plan.StepResult{
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "1", "b": "1"}},
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "own", "c": "own"}},
		{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "3"}},
	}})
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	if fmt.Sprint(end.Outputs) != "map[a:3 b:declared declared:3]" {
		t.Errorf("outputs %v", end.Outputs)
	}
}

// A value of an env: that reads another of it reads that one's, in the
// order they read each other, however long the chain; one that reads
// itself, and those that read each other, read what is under the env:.
func TestStepEnvOrder(t *testing.T) {
	p := compiled(t, `
env:
  C: wc
  D: wd
  S: ws
jobs:
  main:
    env:
      X: ${{ env.Y }}1
      Y: ${{ env.Z }}2
      Z: z
      C: ${{ env.D }}c
      D: ${{ env.C }}d
      S: ${{ env.S }}s
    steps:
      - run: echo ${{ toJSON(env) }}
        env:
          A: ${{ env.B }}a
          B: ${{ env.X }}b
          X: ${{ env.X }}x
          ALL: ${{ env.S }}-${{ join(env.*, ',') }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	s, diags, err := ready.Job.Step(t.Context(), 0, plan.JobState{Run: run})
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	want := map[string]string{
		"X": "z21x", "Y": "z2", "Z": "z", "C": "wdc", "D": "wcd", "S": "wss",
		"B": "z21xb", "A": "z21xba", "ALL": "wss-z21xba,z21xb,wdc,wcd,wss,z21x,z2,z",
	}
	for name, v := range want {
		if s.Env[name] != v {
			t.Errorf("env %s is %q, want %q", name, s.Env[name], v)
		}
	}
}

// A job's ssh: that gives null is none, as format 1 runs such a job
// without ssh.
func TestJobSSHNull(t *testing.T) {
	j := &plan.Job{Name: "j", Path: "/jobs/j", SSH: mustValue(t, "${{ null }}"), Steps: []plan.Step{{Path: "/jobs/j/steps/0", Run: mustValue(t, "true")}}}
	r, diags, err := j.Ready(t.Context(), plan.ReadyInput{})
	if err != nil || len(diags) > 0 || r.Job == nil || r.Job.Target != (plan.Target{}) {
		t.Errorf("%+v, %v, %v", r, diags, err)
	}
}

// A strategy reads the outputs of the jobs its job needs as the rest of
// the job does: strictly, unless the job's if: gives the word always.
func TestStrategyStrictOutputs(t *testing.T) {
	for cond, want := range map[string]string{
		"${{ 'always' }}": "",
		"always":          "",
		"success()":       "[output_not_written]",
	} {
		p := compiled(t, `
jobs:
  a:
    steps:
      - run: "true"
  b:
    needs: [a]
    if: `+cond+`
    strategy:
      matrix:
        x: ${{ fromJSON(needs.a.outputs.list || '[1]') }}
    steps:
      - run: "true"
`, nil)
		run := runValues(t, p, nil)
		r, diags, err := job(t, p, "b").Ready(t.Context(), plan.ReadyInput{Run: run, Needs: map[string]plan.JobResult{"a": {Outcome: plan.OutcomeSuccess}}})
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		if len(diags) > 0 {
			got = fmt.Sprint(codeList(diags))
		} else if r.Expansion == nil {
			got = "no expansion"
		}
		if got != want {
			t.Errorf("if: %s: %s, want %s", cond, got, want)
		}
	}
}

// A cleanup's step is named POST and its step's name, or Step and its
// index when the name is empty or unset, as format 1 names it.
func TestCleanupName(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - name: ""
        run: "true"
        cleanup: "true"
      - run: "true"
        cleanup: "true"
      - name: build ${{ 1 }}
        run: "true"
        cleanup: "true"
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Steps: make([]plan.StepResult, 3)}
	for i, want := range []string{"POST Step 0", "POST Step 1", "POST build 1"} {
		state.Steps[i].Outcome = plan.OutcomeSuccess
		if s, _, err := ready.Job.Cleanup(t.Context(), i, state); err != nil || s.Name != want {
			t.Errorf("cleanup %d: %+v, %v, want %q", i, s, err, want)
		}
	}
}

// JobEnv evaluates the job's env: once, as the job starts, and Step,
// Cleanup and End read what it gave rather than evaluate it again, so it
// is the same for each step, and its problems are its own.
func TestJobEnv(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    env:
      STOPPING: ${{ cancelled() }}
      TOKEN: ${{ vars.token }}-x
      REGION: ${{ vars.region }}
      DIR: ${{ env.PW_JOB_DIR }}
    steps:
      - run: echo $STOPPING ${{ env.STOPPING }} ${{ env.TOKEN }} ${{ env.REGION }} ${{ env.DIR }}
        if: always()
        cleanup: echo ${{ env.STOPPING }}
    outputs:
      token: ${{ env.TOKEN }}
      region: ${{ env.REGION }}
`, map[string]any{"vars": map[string]any{"token": expr.Unknown{Name: "vars"}, "region": "eu"}})
	run := runValues(t, p, map[string]string{"token": "s3cret-value", "region": "eu"})
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	rj := ready.Job
	state := plan.JobState{Run: run, Masked: run.Masked(), Env: map[string]string{"PW_JOB_DIR": "/j"}}
	env, diags, err := rj.JobEnv(t.Context(), state)
	if err != nil || len(diags) > 0 {
		t.Fatal(diags, err)
	}
	data, err := json.Marshal(env, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"env":{"DIR":"/j","REGION":"eu","STOPPING":"false","TOKEN":"s3cret-value-x"},"private":["REGION","TOKEN"],"secrets":["TOKEN"],"masked":["s3cret-value","s3cret-value-x"]}`; string(data) != want {
		t.Errorf("JobEnv:\n%s\nwant\n%s", data, want)
	}
	// The run is cancelled once the job has started: the job's env: is
	// what it was, and a step that reads it again reads the same.
	state.Cancelled, state.Env = true, nil
	without, _, _ := rj.Step(t.Context(), 0, state)
	state.JobEnv = env
	with, _, err := rj.Step(t.Context(), 0, state)
	if err != nil || with.Run != "echo $STOPPING false s3cret-value-x eu /j" || with.Env["STOPPING"] != "false" || without.Run != "echo $STOPPING true s3cret-value-x eu " {
		t.Errorf("Step with JobEnv %+v; without %+v; %v", with, without, err)
	}
	if !slices.Equal(with.Masked, []string{"s3cret-value", "s3cret-value-x", "echo $STOPPING false s3cret-value-x eu /j"}) {
		t.Errorf("Masked = %q", with.Masked)
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess}}
	if s, _, err := rj.Cleanup(t.Context(), 0, state); err != nil || s.Run != "echo false" {
		t.Errorf("Cleanup with JobEnv: %+v, %v", s, err)
	}
	end, _, err := rj.End(t.Context(), state)
	if err != nil || fmt.Sprint(end.Outputs, end.Private, end.Secrets) != "map[region:eu token:s3cret-value-x] [region token] [token]" {
		t.Errorf("End with JobEnv: %+v, %v", end, err)
	}
	if _, diags, err := rj.JobEnv(t.Context(), plan.JobState{Run: run, Needs: map[string]plan.JobResult{}}); err != nil || len(diags) > 0 {
		t.Errorf("JobEnv without the worker's variables: %v, %v", diags, err)
	}
}

// A cleanup that failed fails the job, for the cleanups after it, and End
// reads outputs that nothing wrote leniently after it, as format 1 has it.
func TestCleanupFailureFailsJob(t *testing.T) {
	p := compiled(t, `
jobs:
  main:
    steps:
      - if: success()
        run: "true"
        cleanup: echo first
      - run: "true"
        cleanup: exit 1
    outputs:
      none: ${{ needs.main.outputs.none }}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Steps: []plan.StepResult{{Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSuccess}}}
	if s, _, err := ready.Job.Cleanup(t.Context(), 0, state); err != nil || s.Skipped {
		t.Errorf("cleanup 0 before the other failed: %+v, %v", s, err)
	}
	if _, diags, _ := ready.Job.End(t.Context(), state); len(diags) != 1 || diags[0].Code != "output_not_written" {
		t.Errorf("End after the cleanups succeeded: %v", diags)
	}
	state.Steps[1].CleanupOutcome = plan.OutcomeFailure
	if s, _, err := ready.Job.Cleanup(t.Context(), 0, state); err != nil || !s.Skipped {
		t.Errorf("cleanup 0 after the other failed: %+v, %v", s, err)
	}
	if end, diags, err := ready.Job.End(t.Context(), state); err != nil || len(diags) > 0 || end.Outputs["none"] != "" {
		t.Errorf("End after a cleanup failed: %+v, %v, %v", end, diags, err)
	}
}

// A job reads the outputs of a job it needs, and of its steps, that hold a
// secret as secrets: what it makes of one is masked, and an output it
// makes of one holds a secret too.
func TestNeedsSecretOutputs(t *testing.T) {
	p := compiled(t, `
jobs:
  a:
    steps:
      - id: login
        run: "true"
  b:
    needs: [a]
    steps:
      - run: echo ${{ needs.a.outputs.dsn }} ${{ needs.a.steps.login.outputs.key }} ${{ needs.a.outputs.plain }}
    outputs:
      dsn: ${{ needs.a.outputs.dsn }}
      key: ${{ needs.a.steps.login.outputs.key }}
      plain: ${{ needs.a.outputs.plain }}
`, nil)
	run := runValues(t, p, nil)
	needs := map[string]plan.JobResult{"a": {
		Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"dsn": "pg://u:hunt3r2@db", "plain": "p"},
		Private: []string{"dsn"}, Secrets: []string{"dsn"},
		Steps: map[string]map[string]string{"login": {"key": "k3y-value"}}, SecretSteps: map[string][]string{"login": {"key"}},
	}}
	ready, _, err := job(t, p, "b").Ready(t.Context(), plan.ReadyInput{Run: run, Needs: needs})
	if err != nil {
		t.Fatal(err)
	}
	state := plan.JobState{Run: run, Needs: needs}
	s, _, err := ready.Job.Step(t.Context(), 0, state)
	if err != nil || !slices.Equal(s.Masked, []string{"pg://u:hunt3r2@db", "k3y-value", "echo pg://u:hunt3r2@db k3y-value p"}) {
		t.Errorf("step %+v, %v", s, err)
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess}}
	end, _, err := ready.Job.End(t.Context(), state)
	if err != nil || fmt.Sprint(end.Outputs, end.Private, end.Secrets) != "map[dsn:pg://u:hunt3r2@db key:k3y-value plain:p] [dsn key] [dsn key]" {
		t.Errorf("End %+v, %v", end, err)
	}
}

// Combine keeps which outputs, and steps' outputs, are private or secrets
// as the instance with the highest index gave them.
func TestCombineSecrets(t *testing.T) {
	got := plan.Combine([]plan.JobResult{
		{
			Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "1", "b": "s"}, Private: []string{"b"}, Secrets: []string{"b"},
			Steps: map[string]map[string]string{"x": {"k": "s", "l": "p"}}, SecretSteps: map[string][]string{"x": {"k"}},
		},
		{
			Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"a": "t"}, Private: []string{"a"}, Secrets: []string{"a"},
			Steps: map[string]map[string]string{"x": {"k": "plain"}},
		},
	})
	if fmt.Sprint(got.Outputs, got.Private, got.Secrets, got.Steps, got.SecretSteps) != "map[a:t b:s] [a b] [a b] map[x:map[k:plain l:p]] map[]" {
		t.Errorf("%+v", got)
	}
}

// A step's clean-on-retry: is true unless the workflow says otherwise, and
// an expression's value reads as true or false as format 1 reads it, or
// as true. A plan written before it was a Value, which leaves it unset, is
// false.
func TestStepCleanOnRetry(t *testing.T) {
	p := compiled(t, `
on:
  execute:
    inputs:
      clean: {type: boolean, default: false}
      text: {type: string, default: "false"}
jobs:
  main:
    steps:
      - run: "true"
        retry: {max-retries: 1}
      - run: "true"
        retry: {max-retries: 1, clean-on-retry: false}
      - run: "true"
        retry: {max-retries: 1, clean-on-retry: true}
      - run: "true"
        retry: {max-retries: 1, clean-on-retry: "${{ inputs.clean }}"}
      - run: "true"
        retry: {max-retries: 1, clean-on-retry: "${{ inputs.text }}"}
      - run: "true"
        retry: {max-retries: 1, clean-on-retry: "${{ null }}"}
`, nil)
	run := runValues(t, p, nil)
	ready, _, err := job(t, p, "main").Ready(t.Context(), plan.ReadyInput{Run: run})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []bool{true, false, true, false, false, true} {
		s, diags, err := ready.Job.Step(t.Context(), i, plan.JobState{Run: run, Steps: make([]plan.StepResult, i)})
		if err != nil || len(diags) > 0 || s.Retry == nil || s.Retry.CleanOnRetry != want {
			t.Errorf("step %d: %+v, %v, %v, want clean-on-retry %v", i, s, diags, err, want)
		}
	}
	old := &plan.ReadyJob{Key: "j", Steps: []plan.Step{{Path: "/jobs/j/steps/0", Run: mustValue(t, "true"), Retry: &plan.StepRetry{}}}}
	if s, _, err := old.Step(t.Context(), 0, plan.JobState{}); err != nil || s.Retry == nil || s.Retry.CleanOnRetry {
		t.Errorf("a plan without clean-on-retry: %+v, %v", s, err)
	}
}
