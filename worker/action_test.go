package worker

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// action returns a step that uses action, a built-in action the
// coordinator performs, with with.
func action(uses string, with map[string]any) plan.Step {
	return plan.Step{Uses: uses, With: plan.Literal(with)}
}

// TestActions checks that a step that uses an action the coordinator
// performs asks the coordinator for it, once for each try, with its with:
// as Step evaluated it, and whether it outlives its job's cancellation;
// and ends as the coordinator answers, as a command that exits 0 or not:
// a failure leaves its retry: to try it again, and its ignore-errors: to
// let its last try succeed. The answer's problems are the try's, and its
// log says them.
func TestActions(t *testing.T) {
	t.Parallel()
	warned := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})
	failing := action("parallelworks/update-session", map[string]any{"name": "s"})
	failing.If = cond("always()")
	failing.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	ignored := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"ignored"}})
	ignored.If = cond("always()")
	ignored.IgnoreErrors = plan.Literal(true)
	with, err := plan.ValueOf(map[string]any{"jobs": []any{"${{ job.name }}-x"}})
	if err != nil {
		t.Fatal(err)
	}
	evaluated := plan.Step{Uses: "parallelworks/cancel-jobs", With: with, If: cond("always()")}
	steps := []plan.Step{warned, failing, ignored, evaluated}
	f := workerapitest.NewFake(t, newJob(steps...))
	warning := problems.SSHHostEmpty.At("", problems.Position{}, "/jobs/build/steps/0", nil)
	refused := func(step int) problems.Diagnostic {
		return problems.UsesNotSupported.At("", problems.Position{}, "/jobs/build/steps/"+strconv.Itoa(step), problems.Params{"uses": steps[step].Uses})
	}
	f.Act = func(req workerapi.ActionRequest) workerapi.ActionAnswer {
		switch req.Step {
		case 0:
			return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess, Diagnostics: problems.Diagnostics{warning}}
		case 1, 2:
			return workerapi.ActionAnswer{Outcome: plan.OutcomeFailure, Diagnostics: problems.Diagnostics{refused(req.Step)}}
		}
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success -1",
		"started 1.0", "finished 1.0 failure -1", "started 1.1", "finished 1.1 failure -1",
		"started 2.0", "finished 2.0 success -1",
		"started 3.0", "finished 3.0 success -1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")

	type asked struct {
		step, try int
		outlives  bool
		with      string
	}
	var got []asked
	for _, a := range f.Actions() {
		with, err := workerapi.Marshal(a.Request.With)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, asked{a.Request.Step, a.Request.Try, a.Request.OutlivesCancel, string(with)})
	}
	want := []asked{
		{0, 0, false, `{"jobs":["other"]}`},
		{1, 0, true, `{"name":"s"}`},
		{1, 1, true, `{"name":"s"}`},
		{2, 0, true, `{"jobs":["ignored"]}`},
		{3, 0, true, `{"jobs":["build-x"]}`},
	}
	if !slices.Equal(got, want) {
		t.Errorf("the worker asked for\n%v\nwant\n%v", got, want)
	}

	for _, tc := range []struct {
		step, try int
		want      []string
		lastTry   bool
	}{
		{0, 0, []string{"ssh_host_empty"}, false},
		{1, 0, []string{"uses_not_supported"}, false},
		{1, 1, []string{"uses_not_supported"}, true},
		{2, 0, []string{"uses_not_supported"}, false},
		{3, 0, nil, false},
	} {
		e := finishedEvent(t, f, tc.step, tc.try, false)
		if got := codes(e.Diagnostics); !slices.Equal(got, tc.want) || e.LastTry != tc.lastTry {
			t.Errorf("step %d's try %d has the diagnostics %v and lastTry %v, want %v and %v", tc.step, tc.try, got, e.LastTry, tc.want, tc.lastTry)
		}
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, warning.Message()+"\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 1}, refused(1).Message()+"\n")
	checkLog(t, f, logKey{workerapi.LogStep, 3, 0}, "")
}

// TestActionRepeated checks that a request for an action that fails
// without an answer, or with one that asks the worker to try again, is
// sent again, and that the action is performed once: the step ends as the
// coordinator answered the first time it was asked.
func TestActionRepeated(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})))
	var (
		asked   atomic.Int32
		actions atomic.Int32
	)
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path != workerapi.PathAction {
			return workerapitest.Answer
		}
		switch asked.Add(1) {
		case 1:
			return workerapitest.Unavailable
		case 2:
			// Performed, but the answer is lost.
			return workerapitest.DropAnswer
		}
		return workerapitest.Answer
	}
	f.Act = func(workerapi.ActionRequest) workerapi.ActionAnswer {
		actions.Add(1)
		return workerapi.ActionAnswer{Outcome: plan.OutcomeFailure}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
	if n, m := asked.Load(), actions.Load(); n != 3 || m != 1 {
		t.Errorf("the worker asked %d times, and the action was performed %d times; want 3 and 1", n, m)
	}
}

// TestActionAnsweredAfterCancel checks that a step whose action the
// coordinator performed before the job was cancelled, but whose answer the
// worker has only after, inside the cancel's grace period, ends as the
// answer says. The cancel applies from the step after.
func TestActionAnsweredAfterCancel(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}}), command("echo after")))
	asked, release := make(chan struct{}), make(chan struct{})
	var tries atomic.Int32
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path != workerapi.PathAction {
			return workerapitest.Answer
		}
		if tries.Add(1) == 1 {
			// Performed, and its answer lost.
			return workerapitest.DropAnswer
		}
		// The worker asks again, and the answer comes once the job is
		// cancelled.
		if tries.Load() == 2 {
			close(asked)
			<-release
		}
		return workerapitest.Answer
	}
	// Released as the test ends too, before its cleanups, so that one that
	// fails first doesn't hang closing the Fake's server.
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	done := make(chan error, 1)
	go func() { done <- runWorker(t, f, t.Context(), Config{}) }()
	<-asked
	polls := f.Requests()[workerapi.PathControl]
	f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 60})
	// A poll that starts once the control is set is answered with it at
	// once, and the worker polls again only once it has done as it asks.
	waitFor(t, "the worker to read the cancel", func() bool { return f.Requests()[workerapi.PathControl] >= polls+2 })
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success -1", "finished 1.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	if n := len(f.Actions()); n != 1 {
		t.Errorf("the action was performed %d times", n)
	}
}

// TestActionStopped checks that a step whose action the coordinator
// doesn't answer ends as a command does that its stop ends once its time
// is up: cancelled, once the cancel's grace period has passed; and failed,
// past its timeout:, once the worker's grace period has.
func TestActionStopped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		timeout int64
		cancel  bool
		event   string
		end     plan.Outcome
		reason  plan.Reason
		codes   []string
	}{
		{"cancelled", 0, true, "finished 0.0 cancelled -1", plan.OutcomeCancelled, plan.ReasonCancelled, nil},
		{"timed out", 1, false, "finished 0.0 failure -1", plan.OutcomeFailure, "", []string{"step_timed_out"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			step := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})
			if tc.timeout > 0 {
				step.Timeout = number(tc.timeout)
			}
			f := workerapitest.NewFake(t, newJob(step))
			var asked atomic.Int32
			f.Fault = func(r *http.Request) workerapitest.Fault {
				if r.URL.Path != workerapi.PathAction {
					return workerapitest.Answer
				}
				if asked.Add(1) == 1 && tc.cancel {
					f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1})
				}
				return workerapitest.Hang
			}
			start := time.Now()
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(start); elapsed < time.Second || elapsed > 15*time.Second {
				t.Errorf("the step took %v to stop", elapsed)
			}
			checkEvents(t, f, "started 0.0", tc.event)
			checkEnd(t, f, tc.end, tc.reason)
			if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, tc.codes) {
				t.Errorf("the step's diagnostics are %v, want %v", got, tc.codes)
			}
			if asked.Load() == 0 || len(f.Actions()) != 0 {
				t.Errorf("the worker asked %d times, and %d actions were performed", asked.Load(), len(f.Actions()))
			}
		})
	}
}

// TestActionAnsweredCancelled checks that a step the coordinator answers
// cancelled, as one of a job it is cancelling that the worker hasn't heard
// of yet, ends cancelled, and the job with it, for the answer's reason, as
// it would had the worker read the control first: the steps after it run
// only if they outlive the cancel.
func TestActionAnsweredCancelled(t *testing.T) {
	t.Parallel()
	always := command("echo always")
	always.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}}), command("echo skipped"), always))
	f.Act = func(workerapi.ActionRequest) workerapi.ActionAnswer {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled, Reason: plan.ReasonFailFast}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 cancelled -1",
		"finished 1.0 skipped -1",
		"started 2.0", "finished 2.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonFailFast)
}

// TestActionCancelsItsJob checks that a step whose action cancelled its
// own job, as the answer's reason says, succeeds, and that the job is
// cancelled before its next step, for that reason, as though the worker
// had read its control: the steps after run only if they outlive the
// cancel, and the job ends cancelled.
func TestActionCancelsItsJob(t *testing.T) {
	t.Parallel()
	always := command("echo always")
	always.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"build"}}), command("echo cancelled"), always))
	f.Act = func(workerapi.ActionRequest) workerapi.ActionAnswer {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess, Reason: plan.ReasonCancelled}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success -1",
		"finished 1.0 cancelled -1",
		"started 2.0", "finished 2.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
}

// TestActionEarlyCancelled checks that a step whose early-cancel: is
// any-job-failed, whose action the coordinator answers cancelled, with no
// reason, as a job of the run failed while the worker asked, ends as that
// early-cancel stops a command: cancelled, so that a plain step after it
// is skipped, and an always() step runs.
func TestActionEarlyCancelled(t *testing.T) {
	t.Parallel()
	step := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})
	step.EarlyCancel = plan.EarlyCancelAnyJobFailed
	always := command("echo always")
	always.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(step, command("echo after"), always))
	var once sync.Once
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathAction {
			once.Do(func() { f.SetControl(workerapi.Control{AnyJobFailed: true}) })
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 cancelled -1",
		"finished 1.0 skipped -1",
		"started 2.0", "finished 2.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, "")
	if n := len(f.Actions()); n != 1 || f.Actions()[0].Answer.Outcome != plan.OutcomeCancelled {
		t.Errorf("the actions are %+v", f.Actions())
	}
}

// TestActionEarlyCancelTold checks that an early-cancel answer tells the
// rest of the job that a job of the run failed at once, as its control
// would: a later step whose early-cancel: is any-job-failed doesn't start,
// though no control poll has brought the failure, as none does here.
func TestActionEarlyCancelTold(t *testing.T) {
	t.Parallel()
	step := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})
	step.EarlyCancel = plan.EarlyCancelAnyJobFailed
	later := command("touch ran")
	later.If = cond("always()")
	later.EarlyCancel = plan.EarlyCancelAnyJobFailed
	f := workerapitest.NewFake(t, newJob(step, later))
	f.Act = func(workerapi.ActionRequest) workerapi.ActionAnswer {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 cancelled -1", "finished 1.0 cancelled -1")
	if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "ran")); err == nil {
		t.Error("the later step that early-cancels ran")
	}
}

// TestActionRefused checks that a step whose request the coordinator
// refuses, or answers with what an action can't have, fails, with a
// problem that says why, and isn't tried again; and that one the
// coordinator answers that the attempt no longer holds its job halts the
// worker, which completes nothing.
func TestActionRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		flt    workerapitest.Fault
		answer workerapi.ActionAnswer
		reason string
	}{
		{"refused", workerapitest.Refuse, workerapi.ActionAnswer{}, string(workerapitest.Refusal)},
		{"no outcome", workerapitest.Answer, workerapi.ActionAnswer{Outcome: plan.OutcomeSkipped}, string(problems.CoordinatorAnswerInvalid.Code())},
		{"cancelled for the run's timeout", workerapitest.Answer, workerapi.ActionAnswer{Outcome: plan.OutcomeCancelled, Reason: plan.ReasonTimeout}, string(problems.CoordinatorAnswerInvalid.Code())},
		{"succeeded for the run's timeout", workerapitest.Answer, workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess, Reason: plan.ReasonTimeout}, string(problems.CoordinatorAnswerInvalid.Code())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			step := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})
			step.Retry = &plan.StepRetry{MaxRetries: number(2), Interval: plan.Literal("0s")}
			f := workerapitest.NewFake(t, newJob(step))
			f.Fault = func(r *http.Request) workerapitest.Fault {
				if r.URL.Path == workerapi.PathAction {
					return tc.flt
				}
				return workerapitest.Answer
			}
			f.Act = func(workerapi.ActionRequest) workerapi.ActionAnswer { return tc.answer }
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
			checkEnd(t, f, plan.OutcomeFailure, "")
			e := finishedEvent(t, f, 0, 0, false)
			if got := codes(e.Diagnostics); !slices.Equal(got, []string{"action_request_failed"}) || !e.LastTry {
				t.Fatalf("the step's diagnostics are %v, and lastTry %v", got, e.LastTry)
			}
			if p := e.Diagnostics[0].Params; p["uses"] != "parallelworks/cancel-jobs" || p["reason"] != tc.reason {
				t.Errorf("the problem's params are %v, want the reason %s", p, tc.reason)
			}
		})
	}

	t.Run("fenced", func(t *testing.T) {
		t.Parallel()
		step := action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})
		step.Cleanup = plan.Literal("touch $PW_JOB_DIR/cleaned")
		f := workerapitest.NewFake(t, newJob(step))
		f.Fault = func(r *http.Request) workerapitest.Fault {
			if r.URL.Path == workerapi.PathAction {
				return workerapitest.Fence
			}
			return workerapitest.Answer
		}
		if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, errFenced) {
			t.Fatalf("got %v, want %v", err, errFenced)
		}
		if f.End() != nil {
			t.Error("a fenced worker completed its attempt")
		}
	})
}

// TestActionOfAnOlderCoordinator checks that a step that uses an action
// the coordinator performs, of an attempt of a version of the worker API
// before the coordinator had the path, fails as it did then, without
// asking.
func TestActionOfAnOlderCoordinator(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(action("parallelworks/cancel-jobs", map[string]any{"jobs": []any{"other"}})))
	f.Speaks = workerapi.Version2
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "finished 0.0 failure -1")
	if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"uses_not_supported"}) {
		t.Errorf("the step's diagnostics are %v", got)
	}
	if n := f.Requests()[workerapi.PathAction]; n != 0 {
		t.Errorf("the worker asked for the action %d times", n)
	}
}
