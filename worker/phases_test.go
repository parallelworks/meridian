package worker

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// readied compiles a workflow whose run reads the variables vars, of which
// those secrets names are secret, and readies its job name, as the
// coordinator would, and returns the job and the run's values, as Start
// would complete them.
func readied(t *testing.T, workflow, name string, vars map[string]string, secrets ...string) (*plan.ReadyJob, plan.RunValues) {
	t.Helper()
	known := map[string]any{}
	for v := range vars {
		known[v] = expr.Unknown{Name: "vars"}
	}
	p, diags := compile.Compile("workflow.yaml", []byte(workflow), nil, compile.Options{Contexts: map[string]any{"vars": known}})
	if diags.HasErrors() {
		t.Fatalf("compiling: %v", diags)
	}
	run := plan.RunValues{Format: p.Run.Format, Inputs: p.Run.Inputs, Vars: vars, Secrets: p.Run.Secrets}
	for _, s := range secrets {
		run.Secrets = append(run.Secrets, "vars."+s)
	}
	for env, v := range p.Run.Env {
		lit, ok := v.Literal()
		if !ok {
			t.Fatalf("env %s holds an expression", env)
		}
		if run.Env == nil {
			run.Env = map[string]string{}
		}
		run.Env[env], _ = lit.(string)
	}
	for i := range p.Jobs {
		if p.Jobs[i].Name != name {
			continue
		}
		r, diags, err := p.Jobs[i].Ready(t.Context(), plan.ReadyInput{Run: run})
		if err != nil || diags.HasErrors() || r.Job == nil {
			t.Fatalf("readying %s: %v, %v", name, diags, err)
		}
		return r.Job, run
	}
	t.Fatalf("no job %s", name)
	return nil, run
}

// TestCompiledWorkflow runs a job of a compiled workflow through the
// phases a worker evaluates it with: JobEnv, once, as the job starts, with
// the run's directory in its working-directory:; Step, for each try of a
// step, with the try's variables; Cleanup, for each step with a cleanup:,
// in reverse order, writing the job's outputs; and End, whose outputs keep
// their values, with which of them are private and hold a secret.
func TestCompiledWorkflow(t *testing.T) {
	t.Parallel()
	job, run := readied(t, `
jobs:
  build:
    working-directory: ${{ env.PW_PARENT_JOB_DIR }}/work
    env:
      TOKEN: ${{ vars.token }}
      WHERE: ${{ env.PW_JOB_DIR }}
    steps:
      - id: flaky
        run: |
          echo "try ${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }} of ${{ env.PW_WORKFLOW_STEP_MAX_RETRIES }} in $(pwd)"
          echo "try=${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }}" >> $OUTPUTS
          [ ${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY }} -ge 1 ]
        retry:
          max-retries: 2
          interval: 0s
        cleanup: echo "cleaned=${{ steps.flaky.outputs.try }}" >> $OUTPUTS
      - run: |
          echo "token=$TOKEN where=$WHERE"
          echo "token=$TOKEN" >> $OUTPUTS
          echo "plain=yes" >> $OUTPUTS
      - if: ${{ failure() }}
        run: echo never
        cleanup: echo never cleaned
    outputs:
      try: ${{ steps.flaky.outputs.try }}
`, "build", map[string]string{"token": "s3cr3t-token"}, "token")
	f := newFakeCoordinator(t, job)
	f.claim.Run = run
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 1",
		"started 0.1", "finished 0.1 success 0",
		"started 1.0", "finished 1.0 success 0",
		"finished 2.0 skipped -1",
		"cleanup finished 2.0 skipped -1",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	work := filepath.Join(f.claim.RunDir, "work")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "try 0 of 2 in "+work+"\n")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 1}, "try 1 of 2 in "+work+"\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "token=*** where="+f.claim.RunDir+"\n")
	if got, want := f.finishedEvent(1, 0, false).Outputs, map[string]string{"token": "***", "plain": "yes"}; !maps.Equal(got, want) {
		t.Errorf("step 1's event shows its outputs as %v, want %v", got, want)
	}
	if got, want := f.finishedEvent(0, 0, true).Outputs, map[string]string{"cleaned": "1"}; !maps.Equal(got, want) {
		t.Errorf("the cleanup's event shows its outputs as %v, want %v", got, want)
	}
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"token": "s3cr3t-token", "plain": "yes", "cleaned": "1", "try": "1"}; !maps.Equal(end.Outputs, want) {
		t.Errorf("the job's outputs are %v, want %v", end.Outputs, want)
	}
	if want := map[string]map[string]string{"flaky": {"try": "1"}}; !reflect.DeepEqual(end.StepOutputs, want) {
		t.Errorf("the steps' outputs are %v, want %v", end.StepOutputs, want)
	}
	if !slices.Equal(end.PrivateOutputs, []string{"token"}) || !slices.Equal(end.SecretOutputs, []string{"token"}) || len(end.DerivedOutputs) > 0 {
		t.Errorf("the job's outputs are private %v, secret %v and derived %v", end.PrivateOutputs, end.SecretOutputs, end.DerivedOutputs)
	}
	if len(end.Diagnostics) > 0 {
		t.Errorf("the job's end has problems: %v", end.Diagnostics)
	}
}

// TestJobEnvFails checks that a job whose env: fails to evaluate fails
// before any step runs, with the problem.
func TestJobEnvFails(t *testing.T) {
	t.Parallel()
	job, run := readied(t, `
jobs:
  build:
    env:
      BAD: ${{ fromJSON('{') }}
    steps:
      - run: echo never
`, "build", nil)
	f := newFakeCoordinator(t, job)
	f.claim.Run = run
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f)
	end := checkEnd(t, f, plan.OutcomeFailure, "")
	if !end.Diagnostics.HasErrors() || !strings.Contains(end.Diagnostics[0].Path(), "/env/BAD") {
		t.Errorf("the job's end has the problems %v", end.Diagnostics)
	}
}

// TestCancelledStep checks that, as in format 1, a step that early-cancel:
// stops ends cancelled, which cancels the rest of its job: a later step
// runs only if it outlives a cancel, and the job ends cancelled.
func TestCancelledStep(t *testing.T) {
	t.Parallel()
	waits := command("sleep 30")
	waits.EarlyCancel = "any-job-failed"
	onCancel := command("echo cancelled")
	onCancel.If = cond("cancelled()")
	f := newFakeCoordinator(t, newJob(waits, command("echo skipped"), onCancel))
	f.onEvent = func(e workerapi.Event) {
		if !e.Finished && e.Step == 0 {
			f.setControlLocked(workerapi.Control{AnyJobFailed: true})
		}
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 cancelled -1",
		"finished 1.0 skipped -1",
		"started 2.0", "finished 2.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, "")
	checkLog(t, f, logKey{workerapi.LogStep, 2, 0}, "cancelled\n")
}

// TestStepsSkipped checks that, as in format 1, a job whose steps were all
// skipped is skipped.
func TestStepsSkipped(t *testing.T) {
	t.Parallel()
	skipped := command("echo never")
	skipped.If = cond("false")
	f := newFakeCoordinator(t, newJob(skipped, skipped))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "finished 0.0 skipped -1", "finished 1.0 skipped -1")
	checkEnd(t, f, plan.OutcomeSkipped, "")
}

// TestStepHosts checks where a step whose ssh: is null, or gives no
// remoteHost, runs, as format 1 runs it: where its job would run without
// its ssh:, which is the worker's host, with a note in the log of one that
// gives no remoteHost; and that one whose job's ssh: placed the worker on
// a host of its own fails, rather than run on that host.
func TestStepHosts(t *testing.T) {
	t.Parallel()
	steps := func() []plan.Step {
		null := command("echo null")
		null.SSH = plan.Literal(nil)
		empty := command("echo empty")
		empty.SSH = plan.Literal(map[string]any{"remoteHost": ""})
		empty.If = cond("always()")
		return []plan.Step{null, empty}
	}
	t.Run("the job's host", func(t *testing.T) {
		t.Parallel()
		f := newFakeCoordinator(t, newJob(steps()...))
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
		checkEnd(t, f, plan.OutcomeSuccess, "")
		checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "null\n")
		note := f.finishedEvent(1, 0, false).Diagnostics
		if got := codes(note); !slices.Equal(got, []string{"ssh_host_empty"}) {
			t.Fatalf("the step's diagnostics are %v", got)
		}
		checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, note[0].Message()+"\nempty\n")
	})
	t.Run("another host", func(t *testing.T) {
		t.Parallel()
		j := newJob(steps()...)
		j.Target = plan.Target{Inline: &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "elsewhere"})}}
		f := newFakeCoordinator(t, j)
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "finished 0.0 failure -1", "finished 1.0 failure -1")
		checkEnd(t, f, plan.OutcomeFailure, "")
		for step := range 2 {
			if got := codes(f.finishedEvent(step, 0, false).Diagnostics); !slices.Equal(got, []string{"step_off_job_host"}) {
				t.Errorf("step %d's diagnostics are %v", step, got)
			}
		}
	})
}

// TestCleanupAfterFailedIf checks that, as in format 1, the cleanup of a
// step whose if: failed to evaluate is skipped, since the step didn't run,
// while the step fails its job; and that the cleanup of one whose run:
// failed to evaluate, after its if: passed, runs.
func TestCleanupAfterFailedIf(t *testing.T) {
	t.Parallel()
	badIf := command("echo never")
	badIf.If = cond("fromJSON('{')")
	badIf.Cleanup = plan.Literal("echo if >> $PW_JOB_DIR/cleaned")
	badRun := plan.Step{Run: cond("fromJSON('{')"), If: cond("always()"), Cleanup: plan.Literal("echo run >> $PW_JOB_DIR/cleaned")}
	f := newFakeCoordinator(t, newJob(badIf, badRun))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"finished 0.0 failure -1",
		"finished 1.0 failure -1",
		"cleanup started 1.0", "cleanup finished 1.0 success 0",
		"cleanup finished 0.0 skipped -1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	if cleaned, err := os.ReadFile(filepath.Join(f.claim.RunDir, "cleaned")); err != nil || string(cleaned) != "run\n" {
		t.Errorf("the cleanups that ran wrote %q (%v), want run", cleaned, err)
	}
	if got := f.finishedEvent(0, 0, true).Diagnostics; len(got) > 0 {
		t.Errorf("the skipped cleanup has the problems %v", got)
	}
}

// TestPlatformSecretFields checks that a credential field of a platform's
// secret variable that holds JSON, such as a password, is masked in the
// job's logs and events, as the secret is, and that an output that holds
// it is a secret of its own.
func TestPlatformSecretFields(t *testing.T) {
	t.Parallel()
	step := command(`pw=$(echo "$PW_CREDS" | sed 's/.*"password":"\([^"]*\)".*/\1/'); echo "password is $pw"; echo "pw=$pw" >> $OUTPUTS`)
	f := newFakeCoordinator(t, newJob(step))
	f.claim.Env = map[string]workerapi.EnvVar{"PW_CREDS": {Value: `{"user":"svc","password":"hunt3r2"}`, Secret: true}}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "password is ***\n")
	if got := f.finishedEvent(0, 0, false).Outputs; got["pw"] != "***" {
		t.Errorf("the step's event shows its outputs as %v", got)
	}
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if end.Outputs["pw"] != "hunt3r2" || !slices.Equal(end.SecretOutputs, []string{"pw"}) || len(end.DerivedOutputs) > 0 {
		t.Errorf("the job's outputs are %v, secret %v and derived %v", end.Outputs, end.SecretOutputs, end.DerivedOutputs)
	}
}
