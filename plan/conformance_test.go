package plan_test

import (
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/conformancetest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

func TestMatricesConformance(t *testing.T) {
	files, err := conformance.MatrixFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		t.Run(strings.TrimPrefix(f.Name, "matrices/"), func(t *testing.T) {
			for i, c := range f.Cases {
				t.Run(strconv.Itoa(i+1), func(t *testing.T) {
					s := c.Strategy
					job := &plan.Job{Key: "job", Name: "job", Path: "/jobs/job", Strategy: &plan.Strategy{
						Matrix: mustValue(t, s["matrix"]),
					}}
					if v, ok := s["fail-fast"]; ok {
						job.Strategy.FailFast = mustValue(t, v)
					}
					if v, ok := s["max-parallel"]; ok {
						job.Strategy.MaxParallel = mustValue(t, v)
					}
					scope := expr.Scope{Contexts: f.ContextsFor(&c)}
					x, err := plan.Expand(t.Context(), job, scope, plan.Limits{MaxInstances: c.Limit})
					if c.Error != "" {
						xe, ok := errors.AsType[*plan.ExpandError](err)
						if !ok || string(xe.Kind.Code()) != c.Error || xe.Path != "/jobs/job"+c.Path {
							t.Fatalf("expanded to %v, %v; want %s at %s", x, err, c.Error, c.Path)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					var got []any
					for i, in := range x.Instances {
						if in.Index != i || in.Key != "job#"+strconv.Itoa(i) {
							t.Errorf("instance %d is %s, index %d", i, in.Key, in.Index)
						}
						got = append(got, in.Matrix.Get())
					}
					if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, c.Instances); g != w {
						t.Errorf("instances\n%s\nwant\n%s", g, w)
					}
					if c.FailFast != nil && x.FailFast != *c.FailFast {
						t.Errorf("fail-fast is %v, want %v", x.FailFast, *c.FailFast)
					}
					if c.MaxParallel != nil && x.MaxParallel != *c.MaxParallel {
						t.Errorf("max-parallel is %d, want %d", x.MaxParallel, *c.MaxParallel)
					}
				})
			}
		})
	}
}

func TestContextsConformance(t *testing.T) {
	cases, err := conformance.Cases("contexts")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(strings.TrimPrefix(c.Name, "contexts/"), func(t *testing.T) {
			t.Parallel()
			data, err := c.Workflow()
			if err != nil {
				t.Fatal(err)
			}
			p, diags := compile.Compile("workflow.yaml", data, c.Inputs, compile.Options{Contexts: createContexts(c)})
			if diags.HasErrors() {
				t.Fatalf("the workflow doesn't compile: %v", diags)
			}
			r := &contextsRun{t: t, p: p, run: startedRun(t, c, p), got: map[string]any{}, diags: diags}
			for i := range p.Jobs {
				r.job(&p.Jobs[i], c.Jobs[p.Jobs[i].Name])
			}
			conformancetest.CheckDiagnostics(t, c.Expect.Diagnostics, r.diags)
			if g, w := slices.Sorted(slices.Values(r.skipped)), slices.Sorted(slices.Values(c.Expect.Skipped)); !slices.Equal(g, w) {
				t.Errorf("skipped %q, want %q", g, w)
			}
			if g, w := slices.Sorted(slices.Values(r.private)), slices.Sorted(slices.Values(c.Expect.PrivateOutputs)); !slices.Equal(g, w) {
				t.Errorf("private outputs %q, want %q", g, w)
			}
			if g, w := slices.Sorted(slices.Values(r.secrets)), slices.Sorted(slices.Values(c.Expect.SecretOutputs)); !slices.Equal(g, w) {
				t.Errorf("secret outputs %q, want %q", g, w)
			}
			if g, w := slices.Sorted(slices.Values(r.needsFailed)), slices.Sorted(slices.Values(c.Expect.NeedsFailed)); !slices.Equal(g, w) {
				t.Errorf("skipped because their needs failed %q, want %q", g, w)
			}
			if g, w := slices.Sorted(slices.Values(r.cancelled)), slices.Sorted(slices.Values(c.Expect.Cancelled)); !slices.Equal(g, w) {
				t.Errorf("cancelled %q, want %q", g, w)
			}
			for pointer, want := range c.Expect.Values {
				got, ok := r.got[pointer]
				if !ok {
					t.Errorf("%s has no value; the values are at %q", pointer, slices.Sorted(maps.Keys(r.got)))
				} else if g, w := conformancetest.Canonical(t, got), conformancetest.Canonical(t, want); g != w {
					t.Errorf("%s = %s, want %s", pointer, g, w)
				}
			}
		})
	}
}

// createContexts gives the case's contexts as the run is created, with its
// secret variables unknown.
func createContexts(c conformance.Case) map[string]any {
	out := maps.Clone(c.Contexts)
	if out == nil {
		out = map[string]any{}
	}
	if len(c.SecretVariables) > 0 {
		vars, _ := out["vars"].(map[string]any)
		vars = maps.Clone(vars)
		if vars == nil {
			vars = map[string]any{}
		}
		for _, name := range c.SecretVariables {
			vars[name] = expr.Unknown{Name: "vars"}
		}
		out["vars"] = vars
	}
	return out
}

// startedRun returns the run's values of a contexts case's plan, as Start
// completes them: the plan's inputs, its env, which holds no expressions,
// and the case's contexts.
func startedRun(t *testing.T, c conformance.Case, p *plan.Plan) plan.RunValues {
	t.Helper()
	strs := func(name string) map[string]string {
		m, _ := c.Contexts[name].(map[string]any)
		if m == nil {
			return nil
		}
		out := map[string]string{}
		for k, v := range m {
			out[k], _ = v.(string)
		}
		return out
	}
	run := runValues(t, p, strs("vars"))
	run.Org, run.Remote, run.Sessions = strs("org"), strs("remote"), strs("sessions")
	return run
}

// contextsRun evaluates a contexts case's jobs, and gathers what they
// give.
type contextsRun struct {
	t   *testing.T
	p   *plan.Plan
	run plan.RunValues
	// got holds the values evaluated, by their JSON Pointers, and skipped
	// the JSON Pointers of the jobs and steps skipped.
	got                                               map[string]any
	skipped, needsFailed, cancelled, private, secrets []string
	diags                                             problems.Diagnostics
}

// job evaluates job, with what state says is known of it.
func (r *contextsRun) job(job *plan.Job, state conformance.JobState) {
	t := r.t
	ctx := t.Context()
	in := plan.ReadyInput{Run: r.run, Cancelled: state.Cancelled, Needs: map[string]plan.JobResult{}}
	for name, n := range state.Needs {
		in.Needs[name] = plan.JobResult{
			Outcome: plan.Outcome(n.Result), Outputs: n.Outputs, Steps: n.Steps, NeedsFailed: n.NeedsFailed, Cancelled: n.Cancelled,
		}
	}
	if state.Index != nil {
		in.Instance = &plan.Instance{
			Key: job.Name + "#" + strconv.Itoa(*state.Index), Index: *state.Index, Matrix: plan.DataOf(state.Matrix),
		}
	}
	ready, diags, err := r.p.Ready(ctx, job.Name, in)
	r.diags = append(r.diags, diags...)
	switch {
	case err != nil:
		t.Fatalf("readying %s: %v", job.Name, err)
	case ready == nil:
		return
	case ready.Skipped:
		r.skipped = append(r.skipped, job.Path)
		if ready.NeedsFailed {
			r.needsFailed = append(r.needsFailed, job.Path)
		}
		return
	case ready.Cancelled:
		r.cancelled = append(r.cancelled, job.Path)
		return
	case ready.Job == nil:
		return // a matrix the case gives no instance of
	}
	rj := ready.Job
	if g := rj.Target.Group; g != "" {
		r.got[job.Path+"/runs-on"], r.got[job.Path+"/runs-on/execution-group"] = g, g
	}
	switch {
	case rj.Target.Inline != nil:
		r.got[job.Path+"/ssh"] = rj.Target.Inline.SSH.Get()
	case !job.SSH.IsZero():
		r.got[job.Path+"/ssh"] = nil
	}
	attempt := state.Attempt
	if attempt == 0 {
		attempt = 1
	}
	js := plan.JobState{
		Run: job.Reads(r.run), Needs: job.ReadsNeeds(in.Needs), Cancelled: state.Cancelled, Try: state.Try, Env: state.Env,
		Job: plan.JobContext{Name: job.Name, Key: rj.Key, Attempt: attempt, Group: state.Group, Member: state.Member},
	}
	js.Masked = append(js.Run.Masked(), rj.Masked...)
	env, diags, err := rj.JobEnv(ctx, js)
	r.diags = append(r.diags, diags...)
	switch {
	case err != nil:
		t.Fatalf("the env: of %s: %v", job.Name, err)
	case env == nil:
		return
	}
	js.JobEnv = env
	for i := range rj.Steps {
		if i > len(state.Steps) {
			return
		}
		js.Steps = toResults(state.Steps[:i])
		s, diags, err := r.step(rj, i, js)
		r.diags = append(r.diags, diags...)
		if err != nil {
			t.Fatalf("step %d of %s: %v", i, job.Name, err)
		}
		if s == nil {
			continue
		}
		path := rj.Steps[i].Path
		if s.Skipped {
			r.skipped = append(r.skipped, path)
			continue
		}
		js.Masked = append(js.Masked, s.Masked...)
		r.got[path+"/run"], r.got[path+"/name"] = s.Run, s.Name
		r.got[path+"/working-directory"], r.got[path+"/with"] = s.WorkingDirectory, s.With.Get()
		for name, v := range s.Env {
			r.got[path+"/env/"+name] = v
		}
		switch {
		case s.NoSSH:
			r.got[path+"/ssh"] = nil
		case s.SSH != nil:
			r.got[path+"/ssh"] = s.SSH.SSH.Get()
		}
	}
	if len(state.Steps) < len(rj.Steps) {
		return
	}
	js.Steps = toResults(state.Steps)
	for i, step := range slices.Backward(rj.Steps) {
		if step.Cleanup.IsZero() {
			continue
		}
		s, diags, err := rj.Cleanup(ctx, i, js)
		r.diags = append(r.diags, diags...)
		if err != nil {
			t.Fatalf("cleanup of step %d of %s: %v", i, job.Name, err)
		}
		path := step.Path + "/cleanup"
		switch {
		case s == nil:
		case s.Skipped:
			r.skipped = append(r.skipped, path)
		default:
			r.got[path] = s.Run
			js.Masked = append(js.Masked, s.Masked...)
		}
	}
	end, diags, err := rj.End(ctx, js)
	r.diags = append(r.diags, diags...)
	if err != nil {
		t.Fatalf("ending %s: %v", job.Name, err)
	}
	if end == nil {
		return
	}
	for name, v := range end.Outputs {
		r.got[job.Path+"/outputs/"+name] = v
	}
	for _, name := range end.Private {
		r.private = append(r.private, job.Path+"/outputs/"+name)
	}
	for _, name := range end.Secrets {
		r.secrets = append(r.secrets, job.Path+"/outputs/"+name)
	}
	for _, step := range rj.Steps {
		for _, name := range end.SecretSteps[step.ID] {
			r.secrets = append(r.secrets, step.Path+"/outputs/"+name)
		}
	}
}

// step evaluates the step at index in rj, at the try state says: a later
// try after the first, as the worker does.
func (r *contextsRun) step(rj *plan.ReadyJob, index int, state plan.JobState) (*plan.ReadyStep, problems.Diagnostics, error) {
	if state.Try == 0 {
		return rj.Step(r.t.Context(), index, state)
	}
	first := state
	first.Try = 0
	s, diags, err := rj.Step(r.t.Context(), index, first)
	if err != nil || s == nil || s.Skipped {
		return s, diags, err
	}
	state.FirstTry = s
	return rj.Step(r.t.Context(), index, state)
}

func toResults(steps []conformance.StepResult) []plan.StepResult {
	out := make([]plan.StepResult, len(steps))
	for i, s := range steps {
		out[i] = plan.StepResult{Outcome: plan.Outcome(s.Outcome), Outputs: s.Outputs}
	}
	return out
}
