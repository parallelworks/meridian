package plan_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// A child run's plan, with a Parent, is of version 2, which a plan of
// version 1 can't hold; and Decode refuses what it holds of its step that
// isn't well formed: private names out of order, twice, of neither the
// step's with: nor its environment, or of a variable Env holds.
func TestParentRun(t *testing.T) {
	p := samplePlan(t)
	parent := plan.ParentRun{
		Run: "7", Job: "deploy#1", Step: 2, Try: 1, Root: "3", Depth: 2,
		Env:     map[string]string{"STAGE": "prod"},
		Private: []string{"env.REGION", "inputs.region"},
		Secrets: []string{"env.TOKEN", "inputs.creds.key"},
	}
	p.Run.Parent = &parent
	if p.MinVersion() != 2 {
		t.Errorf("a child run's plan is of version %d", p.MinVersion())
	}
	for version, ok := range map[int]bool{1: false, 2: true} {
		p.Version = version
		data, err := plan.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.Decode(data); (err == nil) != ok || !ok && !errors.Is(err, plan.ErrMalformed) {
			t.Errorf("a child run's plan of version %d: %v", version, err)
		}
	}
	for name, change := range map[string]func(r *plan.ParentRun){
		"out of order":       func(r *plan.ParentRun) { r.Private = []string{"inputs.region", "env.REGION"} },
		"twice":              func(r *plan.ParentRun) { r.Secrets = []string{"env.REGION"} },
		"of neither":         func(r *plan.ParentRun) { r.Private = []string{"vars.region"} },
		"a whole with:":      func(r *plan.ParentRun) { r.Private = []string{"inputs"} },
		"a part of env":      func(r *plan.ParentRun) { r.Private = []string{"env.REGION.x"} },
		"held by the plan":   func(r *plan.ParentRun) { r.Env["REGION"] = "east" },
		"secret and private": func(r *plan.ParentRun) { r.Secrets = []string{"inputs.region"} },
	} {
		r := parent
		r.Env = map[string]string{"STAGE": "prod"}
		change(&r)
		p.Run.Parent = &r
		data, err := plan.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.Decode(data); !errors.Is(err, plan.ErrMalformed) {
			t.Errorf("a parent %s: %v", name, err)
		}
	}
}

// A secret record of version 1 can't hold what a child run has of its step,
// which version 2 adds.
func TestRecordInherited(t *testing.T) {
	r := sampleRecord()
	r.Inherited = &plan.Inherited{Env: map[string]string{"TOKEN": "t0k3n"}}
	for version, ok := range map[int]bool{1: false, 2: true} {
		r.Version = version
		data, err := plan.EncodeRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.DecodeRecord(data); (err == nil) != ok || !ok && !errors.Is(err, plan.ErrMalformed) {
			t.Errorf("a record of version %d with what a child has of its step: %v", version, err)
		}
	}
}

// CallName is format 1's name for a run of the workflow a reference
// names, as PW_WORKFLOW_NAME.
func TestCallName(t *testing.T) {
	for _, tc := range []struct {
		ref  plan.SourceRef
		want string
	}{
		{plan.SourceRef{Uses: "github/acme/tools@main", Path: "deploy.yaml"}, "github.acme.tools"},
		{plan.SourceRef{Uses: "github/acme/tools.git/@v1"}, "github.acme.tools"},
		{plan.SourceRef{Uses: "gitlab/group/sub/project@main"}, "gitlab.gitlab.com.group.sub.project"},
		{plan.SourceRef{Uses: "gitlab/group/project", Host: " Git.Example.COM "}, "gitlab.git.example.com.group.project"},
		{plan.SourceRef{Uses: "marketplace/slurm-setup"}, "slurm-setup"},
		{plan.SourceRef{Uses: "marketplace/slurm-setup/v2"}, "slurm-setup"},
		{plan.SourceRef{Uses: "marketplace/slurm-setup@v2"}, "slurm-setup"},
		{plan.SourceRef{Uses: "workflow/nightly"}, "nightly"},
		{plan.SourceRef{Uses: "workflow/nightly@3"}, "nightly"},
	} {
		if got := tc.ref.CallName(); got != tc.want {
			t.Errorf("%+v: %q, want %q", tc.ref, got, tc.want)
		}
	}
}

// Step names, for a step that calls a workflow, the parts of its with:,
// and the variables of its environment, made from a variable or a password
// and from a secret, which the run it calls keeps out of its plan: the
// workflow's env: as the run's values name it, the job's over it and the
// step's over that; and for a step that calls none, nothing.
func TestStepNamesWhatItGivesThatIsPrivate(t *testing.T) {
	steps := []plan.Step{{
		Uses: "github/acme/tools@main",
		Source: &plan.Source{
			Location: "github/acme/tools@main:w.yaml", Revision: "0123456789abcdef0123456789abcdef01234567",
			Ref: plan.SourceRef{Uses: "github/acme/tools@main"},
		},
		With: mustValue(t, map[string]any{
			"plain":  "x",
			"region": "${{ vars.REGION }}",
			"rows":   []any{"a", "${{ vars.TOKEN }}"},
			"nested": map[string]any{"key": "${{ vars.TOKEN }}-1", "n": "${{ inputs.n }}"},
			"stage":  "${{ env.STAGE }}",
		}),
		Env: map[string]plan.Value{
			"STEP_SECRET": mustValue(t, "${{ vars.TOKEN }}"),
			"JOB_REGION":  mustValue(t, "plain"),
		},
		Path: "/jobs/build/steps/0",
	}, {
		Run:  mustValue(t, "echo ${{ vars.REGION }}"),
		With: mustValue(t, map[string]any{"region": "${{ vars.REGION }}"}),
		Path: "/jobs/build/steps/1",
	}}
	job := &plan.ReadyJob{
		Key: "build", Name: "build", Path: "/jobs/build", Steps: steps,
		Env: map[string]plan.Value{
			"JOB_REGION": mustValue(t, "${{ vars.REGION }}"),
			"JOB_PLAIN":  mustValue(t, "plain"),
		},
	}
	run := plan.RunValues{
		Format:  1,
		Inputs:  plan.DataOf(map[string]any{"n": int64(3)}),
		Env:     map[string]string{"STAGE": "prod", "WORKFLOW_TOKEN": "t0k3n-0123456789", "WORKFLOW_REGION": "east"},
		Vars:    map[string]string{"REGION": "east", "TOKEN": "t0k3n-0123456789", "JSON": `{"a": 1, "b": 2}`},
		Private: []string{"env.WORKFLOW_REGION"},
		Secrets: []string{"env.WORKFLOW_TOKEN", "vars.TOKEN"},
		Derived: []string{"env.WORKFLOW_TOKEN"},
	}
	state := plan.JobState{Run: run, Masked: run.Masked()}
	rs, diags, err := job.Step(t.Context(), 0, state)
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	for name, got := range map[string][]string{
		"WithPrivate": rs.WithPrivate, "WithSecrets": rs.WithSecrets, "EnvPrivate": rs.EnvPrivate, "EnvSecrets": rs.EnvSecrets,
	} {
		want := map[string][]string{
			"WithPrivate": {"region"},
			"WithSecrets": {"nested.key", "rows.1"},
			"EnvPrivate":  {"WORKFLOW_REGION"},
			"EnvSecrets":  {"STEP_SECRET", "WORKFLOW_TOKEN"},
		}[name]
		if !slices.Equal(got, want) {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	state.Steps = []plan.StepResult{{Outcome: plan.OutcomeSuccess}}
	rs, _, err = job.Step(t.Context(), 1, state)
	if err != nil || rs.WithPrivate != nil || rs.WithSecrets != nil || rs.EnvPrivate != nil || rs.EnvSecrets != nil {
		t.Errorf("a step that calls no workflow names what is private: %+v, %v", rs, err)
	}

	// The workflow's env named private whole, as the run's values can name
	// a context, makes each of its variables private, but those made from
	// a secret, which stay secrets.
	whole := state
	whole.Steps, whole.Run.Private = nil, append(slices.Clone(run.Private), "env")
	rs, _, err = job.Step(t.Context(), 0, whole)
	if want := []string{"STAGE", "WORKFLOW_REGION"}; err != nil || !slices.Equal(rs.EnvPrivate, want) {
		t.Errorf("with the workflow's env private whole, EnvPrivate = %q, %v, want %q", rs.EnvPrivate, err, want)
	}

	// A with: that is one template is private whole: each of its keys.
	steps[0].With = mustValue(t, "${{ fromJSON(vars.JSON) }}")
	state.Steps = nil
	rs, diags, err = job.Step(t.Context(), 0, state)
	if err != nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	if want := []string{"a", "b"}; !slices.Equal(rs.WithPrivate, want) {
		t.Errorf("a with: made whole from a variable: %q, want %q", rs.WithPrivate, want)
	}
}
