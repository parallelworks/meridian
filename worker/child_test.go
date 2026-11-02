package worker

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// TestChildInheritedSecret checks that a child run's job masks, in every
// phase, the strings the worker of the step that called it masked
// (workerapi.Tree.Masked), however short, as secrets' own texts: in each of
// JobEnv, Step, Cleanup and End, a step's log and its cleanup's, their
// problems, the job's env: and outputs:, and an output that holds one,
// whichever wrote it, is private and a secret, not only derived; and that
// the job's steps read its root's identity, and the call's name.
func TestChildInheritedSecret(t *testing.T) {
	t.Parallel()
	const short = "s3cr" // under the 8 bytes what is made from a secret is masked at
	step := command(`echo "got=s3cr"; echo "o=s3cr" >> $OUTPUTS; echo "id=$PW_JOB_ID root=$PW_PARENT_JOB_DIR name=$PW_WORKFLOW_NAME dir=$PW_JOB_DIR"`)
	step.Cleanup = plan.Literal(`echo "cleaned=s3cr"; echo "c=s3cr" >> $OUTPUTS`)
	job := newJob(step)
	job.Env = map[string]plan.Value{"FROM_JOB": plan.Literal(short)}
	job.Outputs = map[string]plan.Value{"out": cond("env.FROM_JOB")}
	f := workerapitest.NewFake(t, job)
	f.Claim.Tree = &workerapi.Tree{JobID: "3", RootDir: "/runs/3", WorkflowName: "github.acme.tools", Masked: []string{short, ""}}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "got=***\nid=3 root=/runs/3 name=github.acme.tools dir="+f.Claim.RunDir+"\n")
	checkLog(t, f, logKey{workerapi.LogCleanup, 0, 0}, "cleaned=***\n")
	for _, e := range f.Events() {
		if shown := fmt.Sprint(e.Outputs, e.Diagnostics); strings.Contains(shown, short) && len(e.Outputs) == 0 {
			t.Errorf("an event quotes the inherited secret: %v %v", e.Outputs, e.Diagnostics)
		}
	}
	end := f.End()
	if end == nil || end.Result.Outputs["o"] != short || end.Result.Outputs["out"] != short || end.Result.Outputs["c"] != short ||
		!slices.Equal(slices.Sorted(slices.Values(end.Result.Secrets)), []string{"c", "o", "out"}) || len(end.Result.Derived) > 0 {
		t.Errorf("the job's end doesn't keep the outputs that hold the inherited secret secrets of their own: %+v", end)
	}
}

// TestChildRequest checks what the worker asks for a step's child: Step's
// with: and env, with the worker's variables that format 1 defines over
// the env, never the platform's credential, nor a variable of the
// worker's own environment; what of them is private, as Step named it; and
// the strings the worker masks, each once.
func TestChildRequest(t *testing.T) {
	rs := &plan.ReadyStep{
		Index: 2, With: plan.DataOf(map[string]any{"region": "east"}), WithPrivate: []string{"region"}, WithSecrets: []string{"key"},
		Env: map[string]string{"STAGE": "prod", "TOKEN": "t0k3n-123", "PW_JOB_DIR": "wf"}, EnvSecrets: []string{"TOKEN"}, EnvPrivate: []string{"STAGE"},
		WorkingDirectory: "work",
	}
	vars := map[string]string{
		"PW_JOB_DIR": "/runs/7", "PW_MATRIX_INDEX": "1", "PW_API_KEY": "k3y", "HOME": "/home/u", "OUTPUTS": "/tmp/o",
		"PW_WORKFLOW_STEP_CURRENT_RETRY": "1", "PW_WORKFLOW_STEP_MAX_RETRIES": "2",
	}
	got := childRequest(rs, 1, vars, []string{"t0k3n-123", "", "s3cr", "t0k3n-123"})
	// As in format 1, the step's own variables are the child's too.
	want := map[string]string{
		"STAGE": "prod", "TOKEN": "t0k3n-123", "PW_JOB_DIR": "/runs/7", "PW_MATRIX_INDEX": "1", "OUTPUTS": "/tmp/o",
		"PW_WORKFLOW_STEP_CURRENT_RETRY": "1", "PW_WORKFLOW_STEP_MAX_RETRIES": "2",
	}
	switch {
	case got.Step != 2 || got.Try != 1 || got.WorkingDirectory != "work":
		t.Errorf("the request is of %+v", got)
	case fmt.Sprint(got.Env) != fmt.Sprint(want):
		t.Errorf("the child's environment is %v, want %v", got.Env, want)
	case !slices.Equal(got.WithPrivate, rs.WithPrivate) || !slices.Equal(got.WithSecrets, rs.WithSecrets) ||
		!slices.Equal(got.EnvPrivate, rs.EnvPrivate) || !slices.Equal(got.EnvSecrets, rs.EnvSecrets):
		t.Errorf("what is private of the request is %+v", got)
	case !slices.Equal(got.Masked, []string{"s3cr", "t0k3n-123"}):
		t.Errorf("the request masks %q", got.Masked)
	}
}

// TestMaskedBeyond checks that a step whose run of its own, its child or
// its step run, would mask more than the coordinator takes, more strings
// than workerapi.MaxMasks or more bytes of them than
// workerapi.MaxMaskedBytes, fails, with masks_too_many or masks_too_large,
// and asks for no run, rather than ask with fewer; and that one that masks
// as much as the coordinator takes, and no more, asks for its run.
func TestMaskedBeyond(t *testing.T) {
	t.Parallel()
	strs := func(n int) []string {
		s := make([]string, n)
		for i := range s {
			s[i] = fmt.Sprintf("mask-%06d", i)
		}
		return s
	}
	for _, c := range []struct {
		name   string
		masked []string
		code   string
	}{
		{"too many", strs(workerapi.MaxMasks + 1), "masks_too_many"},
		{"too large", []string{strings.Repeat("m", workerapi.MaxMaskedBytes+1)}, "masks_too_large"},
		{"as many as it may", strs(workerapi.MaxMasks), ""},
		{"as large as it may", []string{strings.Repeat("m", workerapi.MaxMaskedBytes)}, ""},
	} {
		for _, kind := range []string{"child", "step run"} {
			t.Run(kind+", "+c.name, func(t *testing.T) {
				t.Parallel()
				// A child has no exit status of its own.
				step, succeeded := calling(nil), "finished 0.0 success -1"
				if kind == "step run" {
					step, succeeded = remote("true"), "finished 0.0 success 0"
				}
				job := newJob(step)
				job.Masked = c.masked
				f := workerapitest.NewFake(t, job)
				f.Child = ends(f, map[string]plan.Outcome{"0.0": plan.OutcomeSuccess})
				f.StepRun = stepRunEnds(f, map[string]workerapi.StepRun{"0.0": {Outcome: plan.OutcomeSuccess}})
				if err := runWorker(t, f, t.Context(), Config{}); err != nil {
					t.Fatal(err)
				}
				asked := len(f.ChildAsks()) + len(f.StepRunAsks())
				if c.code == "" {
					checkEvents(t, f, "started 0.0", succeeded)
					if asked != 1 {
						t.Errorf("the worker asked for %d runs", asked)
					}
					return
				}
				checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
				if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{c.code}) {
					t.Errorf("the step's diagnostics are %v", got)
				}
				if asked != 0 {
					t.Errorf("the worker asked for %d runs, with too much to mask", asked)
				}
			})
		}
	}
}

// TestRunAskFenced checks that a step whose ask for a run of its own, its
// child or its step run, is answered that the attempt was superseded, or
// that its credential isn't accepted, halts the attempt, as any request so
// answered does: the worker exits without completing it, rather than fail
// the step with the request's problem and go on.
func TestRunAskFenced(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"child", "step run"} {
		for _, tc := range []struct {
			name string
			flt  workerapitest.Fault
			want error
		}{
			{"fenced", workerapitest.Fence, errFenced},
			{"unauthorized", workerapitest.Unauthorized, errUnauthorized},
		} {
			t.Run(kind+", "+tc.name, func(t *testing.T) {
				t.Parallel()
				step, path := calling(nil), workerapi.PathChild
				if kind == "step run" {
					step, path = remote("true"), workerapi.PathStepRun
				}
				f := workerapitest.NewFake(t, newJob(step))
				f.Fault = func(r *http.Request) workerapitest.Fault {
					if r.URL.Path == path {
						return tc.flt
					}
					return workerapitest.Answer
				}
				if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
				if f.End() != nil {
					t.Error("a worker whose ask for a run was refused so completed its attempt")
				}
			})
		}
	}
}

// TestChildClient checks the worker's requests of a step's child: the ask,
// with what childRequest gives, the poll for its end, and its stop.
func TestChildClient(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	c := claimedClient(t, f)
	rs := &plan.ReadyStep{Index: 0, With: plan.DataOf(map[string]any{"$yaml": "w.yaml", "region": "east"})}
	child, err := c.child(t.Context(), childRequest(rs, 0, nil, []string{"s3cr"}))
	if err != nil || child.Run == "" || child.Ended {
		t.Fatalf("the ask for the step's child: %+v, %v", child, err)
	}
	if asks := f.ChildAsks(); len(asks) != 1 || !slices.Equal(asks[0].Masked, []string{"s3cr"}) {
		t.Errorf("the coordinator was asked %+v", asks)
	}
	f.EndChild(0, 0, plan.OutcomeFailure)
	if end, err := c.childEnd(t.Context(), 0, 0); err != nil || !end.Ended || end.Outcome != plan.OutcomeFailure || end.Run != child.Run {
		t.Errorf("the poll for the child's end: %+v, %v", end, err)
	}
	stop := workerapi.ChildStop{Reason: plan.ReasonCancelled}
	if err := c.stopChild(t.Context(), &stop); err != nil {
		t.Fatal(err)
	}
	if stops := f.Stored().ChildStops; len(stops) != 1 || stops[0] != stop {
		t.Errorf("the child's stops are %+v", stops)
	}
}
