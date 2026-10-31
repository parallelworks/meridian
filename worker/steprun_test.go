package worker

import (
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// farm is the ssh: of a step on the host farm, another than its job's.
var farm = plan.Literal(map[string]any{"remoteHost": "farm"})

// remote returns a step that runs script on farm.
func remote(script string) plan.Step {
	s := command(script)
	s.SSH = farm
	return s
}

// stepRunKey names the step run of a step's try, or of its cleanup, as
// "1.0" or "1.c".
func stepRunKey(step, try int, cleanup bool) string {
	if cleanup {
		return strconv.Itoa(step) + ".c"
	}
	return strconv.Itoa(step) + "." + strconv.Itoa(try)
}

// stepRunEnds answers each ask for a step run, made, as ending as ends
// gives it, by stepRunKey: it ends it once the ask has been answered, so
// that the worker waits for its end. One ends doesn't name runs until
// something else ends it.
func stepRunEnds(f *workerapitest.Fake, ends map[string]workerapi.StepRun) func(req workerapi.StepRunRequest) *workerapi.StepRun {
	return func(req workerapi.StepRunRequest) *workerapi.StepRun {
		if end, ok := ends[stepRunKey(req.Step, req.Try, req.Cleanup)]; ok {
			go f.EndStepRun(req.Step, req.Try, req.Cleanup, end)
		}
		return nil
	}
}

// TestStepRunCaller checks a job's steps whose ssh: names another host
// than the job's: each try asks the coordinator for a step run, with the
// try's ssh:, the job's state, of the outputs only those the step reads,
// and what the worker masks, never a variable of its environment or a
// path, and ends as it ended. What a step run's
// step wrote to $OUTPUTS, once it succeeded, is appended to the file the
// step would have written, read as any step's: the job's, for a step
// without an id, of which the step's outputs are what it changed, or the
// step's own; and one its step run says holds a secret is masked, and the
// job's outputs that hold it are secrets. A try that failed gives none;
// ignore-errors: lets the last succeed; and retry: tries again, with the
// ssh: the next try gives, as a step that fails over to another host does.
func TestStepRunCaller(t *testing.T) {
	t.Parallel()
	own := remote("")
	own.Run, own.ID = mustValue(t, "echo own ${{ needs.build.outputs.y }}"), "own"
	ignored := remote("exit 2")
	ignored.IgnoreErrors = plan.Literal(true)
	failover := command("echo failover")
	failover.SSH = mustValue(t, map[string]any{"remoteHost": "${{ env.PW_WORKFLOW_STEP_CURRENT_RETRY == '0' && 'farm' || 'backup' }}"})
	failover.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	reads := remote("")
	reads.Run = mustValue(t, "echo remote ${{ needs.build.outputs.x }}")
	job := newJob(command("echo 'x=here' >> $OUTPUTS"), reads, own, ignored, failover)
	job.Masked = []string{"masked-by-the-job"}
	job.Outputs = map[string]plan.Value{"y": cond("needs.build.outputs.y"), "z": cond("steps.own.outputs.z"), "s": cond("needs.build.outputs.s")}
	f := workerapitest.NewFake(t, job)
	f.Claim.Retry, f.Claim.Attempt = 1, 2
	written := map[string]string{"x": "here", "y": "two\nlines\tand a\rreturn", "s": "s3cr3t-of-the-host", "e": "e"}
	f.StepRun = stepRunEnds(f, map[string]workerapi.StepRun{
		"1.0": {Outcome: plan.OutcomeSuccess, Outputs: written, Private: []string{"e", "s"}, Secrets: []string{"e", "s"}},
		"2.0": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"z": "3"}},
		"3.0": {Outcome: plan.OutcomeFailure, ExitCode: 2},
		"4.0": {Outcome: plan.OutcomeFailure, ExitCode: 1},
		"4.1": {Outcome: plan.OutcomeSuccess},
	})
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
		"started 3.0", "finished 3.0 success 2",
		"started 4.0", "finished 4.0 failure 1", "started 4.1", "finished 4.1 success 0",
	)
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"x": "here", "y": written["y"], "z": "3", "s": written["s"], "e": "e"}; !maps.Equal(end.Result.Outputs, want) {
		t.Errorf("the job's outputs are %q, want %q", end.Result.Outputs, want)
	}
	if !slices.Contains(end.Result.Secrets, "s") {
		t.Errorf("the job's output that holds the step run's secret isn't one: %v", end.Result.Secrets)
	}
	// The step's outputs are what it changed of the job's file, its secret
	// masked, but one too short to mask, which would mask common text.
	if got := finishedEvent(t, f, 1, 0, false).Outputs; len(got) != 3 || got["y"] != written["y"] || got["s"] == written["s"] || got["e"] != "e" {
		t.Errorf("the step run's step's outputs are %q", got)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "")

	asks := f.StepRunAsks()
	var tries []string
	for _, a := range asks {
		tries = append(tries, stepRunKey(a.Step, a.Try, a.Cleanup))
	}
	if want := []string{"1.0", "2.0", "3.0", "4.0", "4.1"}; !slices.Equal(tries, want) {
		t.Fatalf("the worker asked for the step runs of %v, want %v", tries, want)
	}
	first := asks[0]
	host := func(r workerapi.StepRunRequest) any {
		if r.SSH == nil {
			return nil
		}
		return r.SSH.SSH.Get().(map[string]any)["remoteHost"]
	}
	switch st := first.State; {
	case host(first) != "farm":
		t.Errorf("the first ask's ssh: is %+v", first.SSH)
	case len(st.Steps) != 1 || st.Steps[0].Outcome != plan.OutcomeSuccess || !maps.Equal(st.Steps[0].Outputs, map[string]string{"x": "here"}):
		t.Errorf("the first ask's state has of the steps before it %+v", st.Steps)
	case st.JobRetry != 1 || st.JobAttempt != 2 || st.Cancelled || st.MaxRetries != 0:
		t.Errorf("the first ask's state is %+v", st)
	case !slices.Contains(first.Masked, "masked-by-the-job"):
		t.Errorf("the first ask masks %v", first.Masked)
	}
	if second := asks[1]; len(second.State.Steps) != 2 || second.State.Steps[1].Outputs["y"] != written["y"] || !slices.Contains(second.Masked, written["s"]) {
		t.Errorf("the second ask's state is %+v, masking %v", second.State, second.Masked)
	}
	for i, want := range map[int]string{3: "farm", 4: "backup"} {
		if a := asks[i]; host(a) != want || a.State.MaxRetries != 1 {
			t.Errorf("try %d's ask names %v, with %d retries", a.Try, host(a), a.State.MaxRetries)
		}
	}
}

// mustValue returns the Value of v, whose strings may hold expressions.
func mustValue(t *testing.T, v any) plan.Value {
	t.Helper()
	out, err := plan.ValueOf(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestStepRunCallerStopped checks that a stop of a try that waits for its
// step run stops the step run, once, for the stop's reason, and ends the
// try as that stop ends a command's.
func TestStepRunCallerStopped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		step    func(s *plan.Step)
		control *workerapi.Control
		stops   []workerapi.StepRunStop
		events  []string
		outcome plan.Outcome
		reason  plan.Reason
	}{
		{
			name:    "the job's cancel",
			control: &workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled},
			stops:   []workerapi.StepRunStop{{Reason: plan.ReasonCancelled}},
			events:  []string{"started 0.0", "finished 0.0 cancelled -1", "finished 1.0 skipped -1"},
			outcome: plan.OutcomeCancelled, reason: plan.ReasonCancelled,
		},
		{
			name:    "its timeout:",
			step:    func(s *plan.Step) { s.Timeout = plan.Literal("1s") },
			stops:   []workerapi.StepRunStop{{Reason: plan.ReasonTimeout}},
			events:  []string{"started 0.0", "finished 0.0 failure -1", "finished 1.0 skipped -1"},
			outcome: plan.OutcomeFailure,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			step := remote("sleep 60")
			if tc.step != nil {
				tc.step(&step)
			}
			f := workerapitest.NewFake(t, newJob(step, command("echo after")))
			f.StepRun = func(workerapi.StepRunRequest) *workerapi.StepRun {
				if tc.control != nil {
					go f.SetControl(*tc.control)
				}
				return nil
			}
			f.OnStepRunStop = func(workerapi.StepRunStop) plan.Outcome { return plan.OutcomeCancelled }
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f, tc.events...)
			checkEnd(t, f, tc.outcome, tc.reason)
			if got := f.Stored().StepRunStops; !slices.Equal(got, tc.stops) {
				t.Errorf("the step run's stops are %+v, want %+v", got, tc.stops)
			}
		})
	}
}

// TestStepRunCallerNotMade checks that a step run the coordinator didn't
// make fails the try with its problems, in the step's log too, which its
// retry: and ignore-errors: apply to, as a command's exit; and that one
// the coordinator refused, as for its job's cancel, which the worker
// hadn't read, ends the try as that cancel does.
func TestStepRunCallerNotMade(t *testing.T) {
	t.Parallel()
	failed := remote("true")
	cancelled := remote("true")
	cancelled.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(failed, cancelled))
	readsWorker := workerapi.StepRun{Ended: true, Outcome: plan.OutcomeFailure, ExitCode: -1, Diagnostics: problems.Diagnostics{
		problems.StepSSHReadsWorker.At("", problems.Position{}, "/jobs/build/steps/0/ssh", problems.Params{"ref": "env.HOST"}),
	}}
	f.StepRun = func(req workerapi.StepRunRequest) *workerapi.StepRun {
		if req.Step == 0 {
			return &readsWorker
		}
		return &workerapi.StepRun{Ended: true, Outcome: plan.OutcomeCancelled, Reason: plan.ReasonCancelled, ExitCode: -1}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "started 1.0", "finished 1.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeFailure, "")
	if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"step_ssh_reads_worker"}) {
		t.Errorf("the step whose step run wasn't made has the diagnostics %v", got)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, readsWorker.Diagnostics[0].Message()+"\n")
}

// TestStepRunCallerWhich checks which steps run elsewhere, as step runs:
// one whose ssh: names another host, and one whose ssh: is null in a job
// on a host, which runs where the job would without its ssh:, asked for
// with no ssh:; but not one that calls a workflow, which fails, nor an
// action the coordinator performs, whose ssh: places nothing, which runs
// in its job; and, in an attempt of version 2 of the worker API, which has
// no step runs, none of them, which fail as before it.
func TestStepRunCallerWhich(t *testing.T) {
	t.Parallel()
	null := command("true")
	null.SSH = plan.Literal(nil)
	calls := calling(nil)
	calls.SSH = farm
	action := plan.Step{Uses: plan.CancelJobsUses, With: plan.Literal(map[string]any{"jobs": []any{"other"}}), SSH: farm}
	steps := func() []plan.Step { return []plan.Step{remote("true"), null, calls, action} }
	for _, tc := range []struct {
		version int
		asks    []string
		codes   []string
	}{
		{workerapi.Version, []string{"0.0", "1.0"}, []string{"", "", "step_ssh_not_supported", ""}},
		{workerapi.Version2, nil, []string{"step_ssh_not_supported", "step_off_job_host", "uses_not_supported", "uses_not_supported"}},
	} {
		t.Run(strconv.Itoa(tc.version), func(t *testing.T) {
			t.Parallel()
			s := steps()
			for i := range s {
				s[i].If = cond("always()")
			}
			job := newJob(s...)
			job.Target = plan.Target{Inline: &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "home"})}}
			f := workerapitest.NewFake(t, job)
			f.Speaks = tc.version
			// Every step run, and every child, ends at once, so that one asked
			// for that shouldn't be fails the test, rather than wait for its
			// end.
			f.StepRun = func(req workerapi.StepRunRequest) *workerapi.StepRun {
				return &workerapi.StepRun{Run: "steprun-" + stepRunKey(req.Step, req.Try, req.Cleanup), Ended: true, Outcome: plan.OutcomeSuccess}
			}
			f.Child = func(req workerapi.ChildRequest) *workerapi.Child {
				return &workerapi.Child{Run: "child-" + stepRunKey(req.Step, req.Try, false), Ended: true, Outcome: plan.OutcomeSuccess}
			}
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			var asks []string
			for _, a := range f.StepRunAsks() {
				asks = append(asks, stepRunKey(a.Step, a.Try, a.Cleanup))
				if a.Step == 1 && a.SSH != nil {
					t.Errorf("the null ssh:'s step run was asked for at %+v", a.SSH)
				}
			}
			if !slices.Equal(asks, tc.asks) {
				t.Errorf("the worker asked for the step runs of %v, want %v", asks, tc.asks)
			}
			for step, want := range tc.codes {
				if got := codes(finishedEvent(t, f, step, 0, false).Diagnostics); want == "" && len(got) > 0 || want != "" && !slices.Equal(got, []string{want}) {
					t.Errorf("step %d's diagnostics are %v, want %q", step, got, want)
				}
			}
		})
	}
}

// TestStepRunCallerCleanup checks that a step's cleanup: whose ssh: puts it
// on another host runs as a step run of its own, once its job's steps
// have, as the cleanups run, with every step's result: what its step wrote
// to $OUTPUTS is the job's; one that failed fails the job, unless its
// step's ignore-errors: lets it succeed, as a cleanup here.
func TestStepRunCallerCleanup(t *testing.T) {
	t.Parallel()
	failing := remote("true")
	failing.Cleanup = plan.Literal("exit 3")
	ignored := remote("true")
	ignored.Cleanup = plan.Literal("exit 4")
	ignored.IgnoreErrors = plan.Literal(true)
	tidy := remote("true")
	tidy.Cleanup = plan.Literal("echo tidy")
	job := newJob(failing, ignored, tidy)
	job.Outputs = map[string]plan.Value{"c": cond("needs.build.outputs.c")}
	f := workerapitest.NewFake(t, job)
	f.StepRun = stepRunEnds(f, map[string]workerapi.StepRun{
		"0.0": {Outcome: plan.OutcomeSuccess}, "1.0": {Outcome: plan.OutcomeSuccess}, "2.0": {Outcome: plan.OutcomeSuccess},
		"0.c": {Outcome: plan.OutcomeFailure, ExitCode: 3},
		"1.c": {Outcome: plan.OutcomeFailure, ExitCode: 4},
		"2.c": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"c": "1"}},
	})
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0", "started 2.0", "finished 2.0 success 0",
		"cleanup started 2.0", "cleanup finished 2.0 success 0",
		"cleanup started 1.0", "cleanup finished 1.0 success 4",
		"cleanup started 0.0", "cleanup finished 0.0 failure 3",
	)
	end := checkEnd(t, f, plan.OutcomeFailure, "")
	if end.Result.Outputs["c"] != "1" {
		t.Errorf("the job's outputs are %v", end.Result.Outputs)
	}
	asks := f.StepRunAsks()
	if len(asks) != 6 || !asks[3].Cleanup || asks[3].Step != 2 || asks[3].Try != 0 || len(asks[3].State.Steps) != 3 || asks[3].State.Steps[0].Outcome != plan.OutcomeSuccess {
		t.Errorf("the worker asked for %+v", asks)
	}
}

// stepRunJob returns the job of a step run of the step at index 2 of a
// job, as its claim has it: the steps before it by their id alone.
func stepRunJob(step plan.Step) *plan.ReadyJob {
	return newJob(plan.Step{ID: "here"}, plan.Step{}, step)
}

// TestStepRunWorker checks the worker of a step run (workerapi.Claim.StepRun):
// it runs the one step its claim names, at its index, and none of the
// others, with its own variables, over the state its caller's worker gave,
// without its if:, which its caller decided, false as it is here, and with
// the job's retry counter and attempt its caller's; and it reports what the
// step wrote to an $OUTPUTS of its own, empty, never the state's outputs
// nor the job's, with those that hold what it masks secrets. It evaluates
// the job's env: again, with its own paths. The state's cancelled() is
// the step's, but no stop of it.
func TestStepRunWorker(t *testing.T) {
	t.Parallel()
	step := plan.Step{ID: "there", If: cond("false"), Run: mustValue(t, `echo "dir=${{ env.PW_JOB_DIR }}" >> $OUTPUTS
echo "x=${{ steps.here.outputs.x }}" >> $OUTPUTS
echo "y=${{ needs.build.outputs.y }}" >> $OUTPUTS
echo "status=${{ failure() }} ${{ cancelled() }}" >> $OUTPUTS
echo "retry=$PW_WORKFLOW_STEP_CURRENT_RETRY of $PW_WORKFLOW_STEP_MAX_RETRIES" >> $OUTPUTS
echo "job=$PW_WORKFLOW_JOB_CURRENT_RETRY ${{ job.attempt }}" >> $OUTPUTS
echo "token=t0k3n-of-the-caller" >> $OUTPUTS
echo "jobdir=$JOBDIR" >> $OUTPUTS
cat $OUTPUTS`), Retry: &plan.StepRetry{}}
	job := stepRunJob(step)
	job.Retry = &plan.ReadyJobRetry{MaxRetries: 9}
	job.Env = map[string]plan.Value{"JOBDIR": mustValue(t, "${{ env.PW_JOB_DIR }}/x")}
	job.Outputs = map[string]plan.Value{"never": plan.Literal("given")}
	f := workerapitest.NewFake(t, job)
	f.Claim.Tree = &workerapi.Tree{JobID: "root1", RootDir: filepath.Dir(f.Claim.RunDir), Masked: []string{"t0k3n-of-the-caller"}}
	f.Claim.StepRun = &workerapi.StepRunClaim{Step: 2, Try: 1, State: workerapi.StepRunState{
		Steps: []workerapi.StepRunResult{
			{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "1", "fromstate": "no"}},
			{Outcome: plan.OutcomeFailure, Outputs: map[string]string{"y": "2"}},
		},
		Cancelled: true, MaxRetries: 3, JobRetry: 4, JobAttempt: 5,
	}}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 2.1", "finished 2.1 success 0")
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	want := map[string]string{
		"dir": f.Claim.RunDir, "x": "1", "y": "2", "status": "true true", "retry": "1 of 3", "job": "4 5", "token": "t0k3n-of-the-caller",
		"jobdir": f.Claim.RunDir + "/x",
	}
	if !maps.Equal(end.Result.Outputs, want) {
		t.Errorf("the step run's outputs are %q, want %q", end.Result.Outputs, want)
	}
	if !slices.Equal(end.Result.Secrets, []string{"token"}) || !slices.Equal(end.Result.Private, []string{"token"}) {
		t.Errorf("the step run's outputs' secrets are %v, private %v", end.Result.Secrets, end.Result.Private)
	}
	if log, _ := logOf(f, logKey{workerapi.LogStep, 2, 1}); strings.Contains(log, "t0k3n") || !strings.Contains(log, "dir="+f.Claim.RunDir) {
		t.Errorf("the step's log is %q", log)
	}
}

// TestStepRunWorkerWaits checks that the worker of a step run doesn't start
// its step until the coordinator has stored that it is starting, as a
// job's first step waits (TestFirstStepWaits).
func TestStepRunWorkerWaits(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, stepRunJob(command("touch $PW_JOB_DIR/ran")))
	f.Claim.StepRun = &workerapi.StepRunClaim{Step: 2, State: workerapi.StepRunState{Steps: []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSuccess}}}}
	var requests counter
	var early atomic.Bool
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathEvents && requests.next(r.URL.Path) <= 5 {
			if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "ran")); err == nil {
				early.Store(true)
			}
			return workerapitest.Unavailable
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	if early.Load() {
		t.Error("the step ran before the coordinator stored that it was starting")
	}
}

// TestStepRunWorkerStopped checks that a step run told to stop runs no
// step, whatever its step's if: says, and whatever its caller's state
// says of it, a cleanup's too: one told before its step starts runs none,
// and ends cancelled; and one told as it runs stops it.
func TestStepRunWorkerStopped(t *testing.T) {
	t.Parallel()
	always := remote("sleep 60")
	always.If = cond("always()")
	tidy := command("true")
	tidy.Cleanup = plan.Literal("sleep 60")
	for _, tc := range []struct {
		name    string
		step    plan.Step
		claim   workerapi.StepRunClaim
		started bool
		events  []string
	}{
		{"always()", always, workerapi.StepRunClaim{Step: 2, State: workerapi.StepRunState{Steps: make([]workerapi.StepRunResult, 2), IfAlways: true}}, false, []string{"finished 2.0 cancelled -1"}},
		{"a cleanup", tidy, workerapi.StepRunClaim{Step: 2, Cleanup: true, State: workerapi.StepRunState{Steps: make([]workerapi.StepRunResult, 3)}}, false, []string{"cleanup finished 2.0 cancelled -1"}},
		{"as it runs", always, workerapi.StepRunClaim{Step: 2, State: workerapi.StepRunState{Steps: make([]workerapi.StepRunResult, 2)}}, true, []string{"started 2.0", "finished 2.0 cancelled -1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, stepRunJob(tc.step))
			for i := range tc.claim.State.Steps {
				tc.claim.State.Steps[i].Outcome = plan.OutcomeSuccess
			}
			f.Claim.StepRun = &tc.claim
			stop := workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled}
			if tc.started {
				f.OnEvent = func(e workerapi.Event) {
					if !e.Finished {
						go f.SetControl(stop)
					}
				}
			} else {
				f.SetControl(stop)
			}
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f, tc.events...)
			if end := f.End(); end == nil || end.Result.Outcome != plan.OutcomeCancelled {
				t.Errorf("the step run ended %+v", end)
			}
		})
	}
}

// TestAppendOutputs checks that what a step run's step wrote, appended to
// a file of $OUTPUTS, reads back as it was: each value's newlines, tabs
// and carriage returns, which it escapes, and the file's last line before
// it, which it ends, though its step wrote no newline after it.
func TestAppendOutputs(t *testing.T) {
	t.Parallel()
	d, err := makePrivateDir([]parentDir{{path: t.TempDir()}}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.removeAll()
	f, _, _, err := d.create(jobOutputs, os.O_WRONLY|os.O_EXCL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("before=1"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	outputs := map[string]string{"lines": "a\nb", "tab": "\tc\t", "return": "d\r", "back": `e\` + "\n", "empty": "", "eq": "x=y"}
	if err := appendOutputs(d, jobOutputs, outputs); err != nil {
		t.Fatal(err)
	}
	got, _, _, err := readOutputs(d, jobOutputs)
	want := maps.Clone(outputs)
	want["before"] = "1"
	if err != nil || !maps.Equal(got, want) {
		t.Errorf("the outputs read back are %q, %v; want %q", got, err, want)
	}
	// One whose line would be too long to read, escaped, has none of them
	// appended.
	long := map[string]string{"fits": "1", "long": strings.Repeat("\r", maxOutputsLine/2+1)}
	if err := appendOutputs(d, jobOutputs, long); err == nil {
		t.Error("an output whose line would be too long to read was appended")
	}
	if again, _, _, err := readOutputs(d, jobOutputs); err != nil || !maps.Equal(again, want) {
		t.Errorf("after an output too long, the outputs read back are %q, %v", again, err)
	}
}

// TestStepRunAnswered checks the answers of the step run paths that no
// step run can be, which the worker can't read: an end with outputs but
// for a success, or one whose name no line of $OUTPUTS can hold; one not
// made that succeeded; and one that hasn't ended that is no run.
func TestStepRunAnswered(t *testing.T) {
	t.Parallel()
	for name, sr := range map[string]workerapi.StepRun{
		"outputs of a failure":  {Run: "r", Ended: true, Outcome: plan.OutcomeFailure, Outputs: map[string]string{"x": "1"}},
		"a name with an =":      {Run: "r", Ended: true, Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x=y": "1"}},
		"a name with a newline": {Run: "r", Ended: true, Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x\ny": "1"}},
		"not made, a success":   {Ended: true, Outcome: plan.OutcomeSuccess},
		"no outcome a run has":  {Run: "r", Ended: true, Outcome: plan.OutcomeSkipped},
		"not ended, and no run": {},
	} {
		if err := stepRunAnswered(workerapi.PathStepRun, &sr); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if err := stepRunAnswered(workerapi.PathStepRun, &workerapi.StepRun{Run: "r", Ended: true, Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"x": "a\nb"}}); err != nil {
		t.Errorf("a success with outputs: %v", err)
	}
}

// TestStepRunCallerLargeOutputs checks that a step run's ask carries, of
// what the job's earlier steps wrote, only what its step reads: earlier
// steps that wrote far more, together, than the coordinator takes in an
// ask (workerapi.MaxStepRunRequest), but that the step doesn't read,
// leave the ask small, and the step reads what it does.
func TestStepRunCallerLargeOutputs(t *testing.T) {
	t.Parallel()
	const steps = 5
	var job []plan.Step
	for i := range steps {
		// About 1 MiB each, in lines short enough to read.
		s := command(`line=$(head -c 60000 /dev/zero | tr '\0' a)
for i in $(seq 16); do echo "big$i=$line"; done >> $OUTPUTS
echo small=ok >> $OUTPUTS`)
		s.ID = "s" + strconv.Itoa(i)
		job = append(job, s)
	}
	reads := remote("")
	reads.Run = mustValue(t, "echo ${{ steps.s0.outputs.small }}")
	f := workerapitest.NewFake(t, newJob(append(job, reads)...))
	f.StepRun = stepRunEnds(f, map[string]workerapi.StepRun{stepRunKey(steps, 0, false): {Outcome: plan.OutcomeSuccess}})
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	asks := f.StepRunAsks()
	if len(asks) != 1 {
		t.Fatalf("the worker asked for %d step runs", len(asks))
	}
	st := asks[0].State
	if len(st.Steps) != steps || !maps.Equal(st.Steps[0].Outputs, map[string]string{"small": "ok"}) {
		t.Errorf("the ask's state has of the steps before it %d, the first with %d outputs", len(st.Steps), len(st.Steps[0].Outputs))
	}
	for i, r := range st.Steps[1:] {
		if len(r.Outputs) != 0 || r.Outcome != plan.OutcomeSuccess {
			t.Errorf("the ask's state has of step %d %d outputs, %s", i+1, len(r.Outputs), r.Outcome)
		}
	}
	encoded, err := workerapi.Marshal(asks[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > workerapi.MaxStepRunRequest/64 {
		t.Errorf("the ask is %d bytes", len(encoded))
	}
}

// TestStepRunCallerUnread checks that an end of a step run the worker
// can't read, as one with outputs of a try that failed, fails the step,
// with step_run_request_failed, and none of what it says is the step's.
func TestStepRunCallerUnread(t *testing.T) {
	t.Parallel()
	job := newJob(remote("true"))
	f := workerapitest.NewFake(t, job)
	f.StepRun = stepRunEnds(f, map[string]workerapi.StepRun{"0.0": {Outcome: plan.OutcomeFailure, Outputs: map[string]string{"leak": "x"}}})
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
	end := checkEnd(t, f, plan.OutcomeFailure, "")
	if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"step_run_request_failed"}) {
		t.Errorf("the step's diagnostics are %v", got)
	}
	if _, ok := end.Result.Outputs["leak"]; ok {
		t.Errorf("the job's outputs are %v", end.Result.Outputs)
	}
}

// TestStepRunCallerStoppedSucceeds checks that a step run that its try's
// stop reached, but that ended a success all the same, gives the try none
// of what its step wrote: the try ends as its stop does.
func TestStepRunCallerStoppedSucceeds(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(remote("true")))
	f.StepRun = func(workerapi.StepRunRequest) *workerapi.StepRun {
		go f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
		return nil
	}
	f.OnStepRunStop = func(stop workerapi.StepRunStop) plan.Outcome {
		go f.EndStepRun(stop.Step, stop.Try, stop.Cleanup, workerapi.StepRun{Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"late": "1"}})
		return ""
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 cancelled 0")
	end := checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	if e := finishedEvent(t, f, 0, 0, false); len(e.Outputs) > 0 || len(end.Result.Outputs) > 0 {
		t.Errorf("the stopped try's outputs are %v, and its job's %v", e.Outputs, end.Result.Outputs)
	}
}

// TestStepRunWorkerCheckout checks that a step run's step that checks out
// a repository asks the coordinator for its action, and its tree, as the
// step at its index, and writes the tree in its run's directory, the
// host's copy of its caller's.
func TestStepRunWorkerCheckout(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, stepRunJob(checkoutStep("src")))
	f.Claim.StepRun = &workerapi.StepRunClaim{Step: 2, State: workerapi.StepRunState{Steps: []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSuccess}}}}
	f.Tree = func(workerapi.ActionRequest) []byte { return testTree(t) }
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 2.0", "finished 2.0 success -1")
	checkEnd(t, f, plan.OutcomeSuccess, "")
	if actions := f.Actions(); len(actions) != 1 || actions[0].Request.Step != 2 {
		t.Errorf("the actions asked for are %+v", actions)
	}
	if data, err := os.ReadFile(filepath.Join(f.Claim.RunDir, "src", "README")); err != nil || string(data) != "hello\n" {
		t.Errorf("the tree's README holds %q, %v", data, err)
	}
}

// TestStepRunCallerLineTooLong checks that what a step run's step wrote,
// one of whose lines, escaped, would be longer than $OUTPUTS is read
// with, fails the step, and none of it is appended, which would keep the
// whole file from being read: the job's earlier step's outputs are read.
func TestStepRunCallerLineTooLong(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo 'x=here' >> $OUTPUTS"), remote("true")))
	long := strings.Repeat("\t", maxOutputsLine/2+1)
	f.StepRun = stepRunEnds(f, map[string]workerapi.StepRun{"1.0": {Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"long": long, "short": "1"}}})
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 failure 0")
	end := checkEnd(t, f, plan.OutcomeFailure, "")
	if got := codes(finishedEvent(t, f, 1, 0, false).Diagnostics); !slices.Equal(got, []string{"step_run_output_line_too_long"}) {
		t.Errorf("the step's diagnostics are %v", got)
	}
	if !maps.Equal(end.Result.Outputs, map[string]string{"x": "here"}) {
		t.Errorf("the job's outputs are %q", end.Result.Outputs)
	}
}

// TestStepRunWorkerCleanupIf checks that a step run of a step's cleanup:
// runs it, without evaluating its step's if: again, which its caller
// decided: false as the state has it here.
func TestStepRunWorkerCleanupIf(t *testing.T) {
	t.Parallel()
	step := command("true")
	step.If = cond("failure()")
	step.Cleanup = plan.Literal("echo tidied=yes >> $OUTPUTS")
	f := workerapitest.NewFake(t, stepRunJob(step))
	steps := []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSuccess}}
	f.Claim.StepRun = &workerapi.StepRunClaim{Step: 2, Cleanup: true, State: workerapi.StepRunState{Steps: steps}}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "cleanup started 2.0", "cleanup finished 2.0 success 0")
	if end := checkEnd(t, f, plan.OutcomeSuccess, ""); end.Result.Outputs["tidied"] != "yes" {
		t.Errorf("the cleanup's outputs are %v", end.Result.Outputs)
	}
}

// TestStepRunWorkerCleanupSkipped checks that a step run of the cleanup:
// of a step its state says was skipped, which its caller would have asked
// none for, fails, rather than say that a cleanup that never ran
// succeeded.
func TestStepRunWorkerCleanupSkipped(t *testing.T) {
	t.Parallel()
	step := command("true")
	step.Cleanup = plan.Literal("echo tidy")
	f := workerapitest.NewFake(t, stepRunJob(step))
	steps := []workerapi.StepRunResult{{Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSuccess}, {Outcome: plan.OutcomeSkipped}}
	f.Claim.StepRun = &workerapi.StepRunClaim{Step: 2, Cleanup: true, State: workerapi.StepRunState{Steps: steps}}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "cleanup finished 2.0 failure -1")
	checkEnd(t, f, plan.OutcomeFailure, "")
}
