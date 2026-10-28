package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// newJob returns a job of steps, at their paths.
func newJob(steps ...plan.Step) *plan.ReadyJob {
	for i := range steps {
		steps[i].Path = "/jobs/build/steps/" + strconv.Itoa(i)
	}
	return &plan.ReadyJob{Key: "build", Name: "build", Steps: steps, Path: "/jobs/build"}
}

func command(script string) plan.Step { return plan.Step{Run: plan.Literal(script)} }

func number(n int64) plan.Value { return plan.Literal(n) }

// cond returns the Value of an expression, as a workflow's ${{ }} holds it.
func cond(expression string) plan.Value {
	v, err := plan.ValueOf("${{ " + expression + " }}")
	if err != nil {
		panic(err) //nolint:forbidigo // a test's own expression, which must parse
	}
	return v
}

func checkEvents(t *testing.T, f *workerapitest.Fake, want ...string) {
	t.Helper()
	if got := stepEvents(t, f); !slices.Equal(got, want) {
		t.Errorf("events:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func checkEnd(t *testing.T, f *workerapitest.Fake, outcome plan.Outcome, reason plan.Reason) *workerapi.Complete {
	t.Helper()
	end := f.End()
	if end == nil {
		t.Fatal("the attempt didn't complete")
	}
	if end.Result.Outcome != outcome || end.Reason != reason {
		t.Errorf("the attempt ended %s (%s), want %s (%s)", end.Result.Outcome, end.Reason, outcome, reason)
	}
	if want := int64(len(stepEvents(t, f))); end.Seq != want {
		t.Errorf("Complete's seq is %d, want %d", end.Seq, want)
	}
	return end
}

func checkLog(t *testing.T, f *workerapitest.Fake, key logKey, want string) {
	t.Helper()
	got, ended := logOf(f, key)
	if got != want || !ended {
		t.Errorf("log %v is %q (ended %t), want %q, ended", key, got, ended, want)
	}
}

// TestJobRuns checks a job whose steps succeed: their events in order,
// their logs, the environment format 1 gives them, where they run, and
// their outputs, with format 1's $OUTPUTS shared by the steps without an
// id.
func TestJobRuns(t *testing.T) {
	t.Parallel()
	j := newJob(
		command("echo hello\necho ' a = 1 ' >> $OUTPUTS\necho 'multi=x\\ny' >> $OUTPUTS\necho 'no equals' >> $OUTPUTS"),
		plan.Step{ID: "s", Run: plan.Literal(`echo "b=2" >> "$OUTPUTS"
echo "dir=$(pwd) job_dir=$PW_JOB_DIR ci=$CI job=$PW_JOB_ID inherited=$FROM_WORKER step=$STEP"
env | grep -c '^MERIDIAN_' || true`), Env: map[string]plan.Value{"STEP": plan.Literal("env")}},
		plan.Step{Run: plan.Literal(`echo "a=3" >> $OUTPUTS; pwd`), WorkingDirectory: plan.Literal("sub/dir")},
	)
	j.Env = map[string]plan.Value{"CI": plan.Literal("overridden"), "STEP": plan.Literal("job")}
	j.Outputs = map[string]plan.Value{"b": cond("steps.s.outputs.b")}
	f := workerapitest.NewFake(t, j)
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
	)
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"a": "3", "multi": "x\ny", "b": "2"}; !reflect.DeepEqual(end.Result.Outputs, want) {
		t.Errorf("the job's outputs are %v, want %v", end.Result.Outputs, want)
	}
	for _, tc := range []struct {
		step int
		want map[string]string
	}{
		{0, map[string]string{"a": "1", "multi": "x\ny"}},
		{1, map[string]string{"b": "2"}},
		{2, map[string]string{"a": "3"}},
	} {
		if got := finishedEvent(t, f, tc.step, 0, false).Outputs; !reflect.DeepEqual(got, tc.want) {
			t.Errorf("step %d's outputs are %v, want %v", tc.step, got, tc.want)
		}
	}
	dir := f.Claim.RunDir
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "hello\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0},
		"dir="+dir+" job_dir="+dir+" ci=overridden job=run1 inherited=inherited step=env\n0\n")
	checkLog(t, f, logKey{workerapi.LogStep, 2, 0}, filepath.Join(dir, "sub/dir")+"\n")
	if setup, ended := logOf(f, logKey{part: workerapi.LogSetup}); !ended || !strings.Contains(setup, "claimed the attempt") {
		t.Errorf("the setup log is %q (ended %t)", setup, ended)
	}
}

// TestStepFails checks that a failed step fails its job, that the steps
// after it run only as their if: says, and that ignore-errors: makes a
// failed step succeed.
func TestStepFails(t *testing.T) {
	t.Parallel()
	ignore := command("exit 4")
	ignore.If, ignore.IgnoreErrors = cond("failure()"), plan.Literal(true)
	always := command("echo always")
	always.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(command("echo before; exit 3"), command("echo skipped"), always, ignore))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 3",
		"finished 1.0 skipped -1",
		"started 2.0", "finished 2.0 success 0",
		"started 3.0", "finished 3.0 success 4",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "before\n")
	if _, ok := logOf(f, logKey{workerapi.LogStep, 1, 0}); ok {
		t.Error("a skipped step has a log")
	}
}

// TestStepRetries checks a step's retry:, as format 1 has it: each try has
// its events and log, and is told which it is.
func TestStepRetries(t *testing.T) {
	t.Parallel()
	succeeds := command(`n=$(cat count 2>/dev/null || echo 0); echo $((n+1)) > count
echo "try $PW_WORKFLOW_STEP_CURRENT_RETRY of $PW_WORKFLOW_STEP_MAX_RETRIES"; [ "$n" -ge 2 ]`)
	succeeds.Retry = &plan.StepRetry{MaxRetries: number(3), Interval: plan.Literal("0s")}
	fails := command("exit 1")
	fails.If = cond("always()")
	fails.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	f := workerapitest.NewFake(t, newJob(succeeds, fails))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 1",
		"started 0.1", "finished 0.1 failure 1",
		"started 0.2", "finished 0.2 success 0",
		"started 1.0", "finished 1.0 failure 1",
		"started 1.1", "finished 1.1 failure 1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	for try := range 3 {
		checkLog(t, f, logKey{workerapi.LogStep, 0, try}, "try "+strconv.Itoa(try)+" of 3\n")
	}
}

// TestTimeouts checks a step's timeout:, its retry:'s timeout for each
// try, and the job's timeout:, which the worker enforces.
func TestTimeouts(t *testing.T) {
	t.Parallel()
	t.Run("step", func(t *testing.T) {
		t.Parallel()
		slow := command("sleep 30")
		slow.Timeout = number(1)
		// A step past its own timeout: isn't tried again.
		slow.Retry = &plan.StepRetry{MaxRetries: number(2), Interval: plan.Literal("0s"), Timeout: number(20)}
		after := command("echo after")
		after.If = cond("always()")
		f := workerapitest.NewFake(t, newJob(slow, after))
		start := time.Now()
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed > 15*time.Second {
			t.Errorf("the step ran for %v", elapsed)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "started 1.0", "finished 1.0 success 0")
		checkEnd(t, f, plan.OutcomeFailure, "")
		if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"step_timed_out"}) {
			t.Errorf("the step's diagnostics are %v", got)
		}
	})
	t.Run("try", func(t *testing.T) {
		t.Parallel()
		slow := command("sleep 30")
		slow.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s"), Timeout: number(1)}
		f := workerapitest.NewFake(t, newJob(slow))
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "started 0.1", "finished 0.1 failure -1")
		checkEnd(t, f, plan.OutcomeFailure, "")
		for try := range 2 {
			if got := codes(finishedEvent(t, f, 0, try, false).Diagnostics); !slices.Equal(got, []string{"step_timed_out"}) {
				t.Errorf("try %d's diagnostics are %v", try, got)
			}
		}
	})
	t.Run("job", func(t *testing.T) {
		t.Parallel()
		// As in format 1, a step that outlives the job's timeout runs
		// after it, and a job past its timeout isn't cancelled: its steps'
		// cancelled() is false.
		after := command("echo after")
		after.If = cond("always()")
		onCancel := command("echo cancelled")
		onCancel.If = cond("cancelled()")
		j := newJob(command("sleep 30"), after, onCancel)
		j.TimeoutSeconds = 1
		f := workerapitest.NewFake(t, j)
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "started 1.0", "finished 1.0 success 0", "finished 2.0 skipped -1")
		checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
	})
}

// TestCancel checks that a job the coordinator cancels stops: its running
// step gets SIGTERM, and SIGKILL after the cancellation's grace period if
// it ignores that; its later steps don't run, but, as in format 1, those
// whose if: is always() or cancelled(); its cleanup: commands run; and it
// ends cancelled.
func TestCancel(t *testing.T) {
	t.Parallel()
	stubborn := command("trap '' TERM; echo waiting; sleep 30")
	stubborn.Cleanup = plan.Literal("echo cleaned > $PW_JOB_DIR/cleaned")
	always := command("echo always")
	always.If = cond("always()")
	onCancel := command("echo cancelled")
	onCancel.If = cond("cancelled()")
	f := workerapitest.NewFake(t, newJob(stubborn, always, command("echo skipped"), onCancel))
	// Cancel once the step has printed, and so ignores SIGTERM.
	var once sync.Once
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(func() { f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1}) })
		}
		return workerapitest.Answer
	}
	start := time.Now()
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("the job took %v to stop", elapsed)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 cancelled -1",
		"started 1.0", "finished 1.0 success 0",
		"finished 2.0 skipped -1",
		"started 3.0", "finished 3.0 success 0",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "waiting\n")
	checkLog(t, f, logKey{workerapi.LogStep, 3, 0}, "cancelled\n")
	if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "cleaned")); err != nil {
		t.Errorf("the cleanup: command didn't run: %v", err)
	}
}

// cancelDuringFirst has f cancel the job as its first step starts, which
// outlives the cancel, and runs for a second, so that the cancel comes
// between it and the next.
func cancelDuringFirst(f *workerapitest.Fake) {
	f.OnEvent = func(e workerapi.Event) {
		if e.Step == 0 && !e.Finished {
			f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
		}
	}
}

// TestCancelNextStep checks a cancel between steps, as format 1 has it:
// the next step that comes to run whose if: passes, as cancelled() is
// still false, ends cancelled without starting, and its cleanup runs; a
// step whose if: is cancelled() right after the cancel is skipped, since
// no step has been cancelled yet; and once one has, the plain steps after
// it are skipped, with their cleanups, and a cancelled() step runs.
func TestCancelNextStep(t *testing.T) {
	t.Parallel()
	first := command("echo first; sleep 1")
	first.If = cond("always()")
	onCancel := command("echo cancelled")
	onCancel.If = cond("cancelled()")
	next := command("touch $PW_JOB_DIR/ran")
	next.Cleanup = plan.Literal("touch $PW_JOB_DIR/next-cleaned")
	later := command("touch $PW_JOB_DIR/ran")
	later.Cleanup = plan.Literal("touch $PW_JOB_DIR/later-cleaned")
	f := workerapitest.NewFake(t, newJob(first, onCancel, next, later, onCancel))
	cancelDuringFirst(f)
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"finished 1.0 skipped -1",
		"finished 2.0 cancelled -1",
		"finished 3.0 skipped -1",
		"started 4.0", "finished 4.0 success 0",
		"cleanup finished 3.0 skipped -1",
		"cleanup started 2.0", "cleanup finished 2.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	checkLog(t, f, logKey{workerapi.LogStep, 4, 0}, "cancelled\n")
	for name, want := range map[string]bool{"ran": false, "next-cleaned": true, "later-cleaned": false} {
		if _, err := os.Stat(filepath.Join(f.Claim.RunDir, name)); (err == nil) != want {
			t.Errorf("%s is there: %t, want %t", name, err == nil, want)
		}
	}
}

// TestCancelOutlived checks that a job whose steps after a cancel all
// outlive it ends as they did, as in format 1, where only a step that
// ended cancelled cancels its job.
func TestCancelOutlived(t *testing.T) {
	t.Parallel()
	first := command("echo first; sleep 1")
	first.If = cond("always()")
	after := command("echo after")
	after.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(first, after))
	cancelDuringFirst(f)
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	checkEnd(t, f, plan.OutcomeSuccess, "")
}

// TestRunTimeout checks that a job whose run is past its timeout, which
// the coordinator stops with a Cancel whose reason is timeout, isn't
// cancelled, as in format 1, whose run's deadline fails the step it stops:
// the step fails, failure() is true after it and cancelled() false, so a
// step whose if: is failure() comes to run, and fails without starting, as
// a step past the deadline does, and one whose if: is cancelled() is
// skipped; a step that outlives a cancel runs, as one does after the job's
// own timeout; its cleanup runs; and the job ends failure, timeout.
func TestRunTimeout(t *testing.T) {
	t.Parallel()
	stubborn := command("trap '' TERM; echo waiting; sleep 30")
	stubborn.Cleanup = plan.Literal("echo cleaned > $PW_JOB_DIR/cleaned")
	always := command("echo always")
	always.If = cond("always()")
	onFailure := command("echo failed")
	onFailure.If = cond("failure()")
	onCancel := command("echo cancelled")
	onCancel.If = cond("cancelled()")
	f := workerapitest.NewFake(t, newJob(stubborn, always, onFailure, command("echo skipped"), onCancel))
	// The run's timeout, once the step has printed, and so ignores SIGTERM,
	// with a grace of its own, shorter than the worker's.
	sent, ended := timeoutOnFirstLog(f, 1)
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if took := ended().Sub(sent()); took < time.Second || took >= testTiming.grace {
		t.Errorf("the step was killed %v after the run's timeout, not after its grace of 1 s", took)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure -1",
		"started 1.0", "finished 1.0 success 0",
		"finished 2.0 failure -1",
		"finished 3.0 skipped -1",
		"finished 4.0 skipped -1",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "waiting\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "always\n")
	if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "cleaned")); err != nil {
		t.Errorf("the cleanup: command didn't run: %v", err)
	}
}

// timeoutOnFirstLog has f stop the job for the run's timeout, with grace
// seconds of grace, once its first step has printed, and returns when it
// did, and when the first step ended, which the worker says.
func timeoutOnFirstLog(f *workerapitest.Fake, grace int64) (sent, ended func() time.Time) {
	var mu sync.Mutex
	var at, end time.Time
	var once sync.Once
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(func() {
				mu.Lock()
				at = time.Now()
				mu.Unlock()
				f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonTimeout, GraceSeconds: grace})
			})
		}
		return workerapitest.Answer
	}
	f.OnEvent = func(e workerapi.Event) {
		if e.Finished && e.Step == 0 && !e.Cleanup {
			mu.Lock()
			end = e.Time
			mu.Unlock()
		}
	}
	return func() time.Time { mu.Lock(); defer mu.Unlock(); return at },
		func() time.Time { mu.Lock(); defer mu.Unlock(); return end }
}

// TestRunTimeoutKills checks that a step a deadline stops is killed at the
// soonest any deadline gives: a later run's timeout that gives less grace
// makes SIGKILL sooner, and so does the job's own timeout: after a run's
// timeout with a long grace, as before the run's timeout was taken as
// the job's.
func TestRunTimeoutKills(t *testing.T) {
	t.Parallel()
	t.Run("a shorter grace", func(t *testing.T) {
		t.Parallel()
		f := workerapitest.NewFake(t, newJob(command("trap '' TERM; echo waiting; sleep 30")))
		sent, ended := timeoutOnFirstLog(f, 20)
		go func() {
			// A second, with less grace, once the first has come.
			for sent().IsZero() {
				select {
				case <-t.Context().Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
			f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonTimeout, GraceSeconds: 1})
		}()
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		if took := ended().Sub(sent()); took < time.Second || took > 5*time.Second {
			t.Errorf("the step was killed %v after the run's timeout, not after the second's grace of 1 s", took)
		}
		checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
	})
	t.Run("the job's own timeout", func(t *testing.T) {
		t.Parallel()
		job := newJob(command("trap '' TERM; echo waiting; sleep 60"))
		job.TimeoutSeconds = 2
		f := workerapitest.NewFake(t, job)
		sent, ended := timeoutOnFirstLog(f, 25)
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		// The job's timeout, 2 s after its claim, and the worker's grace
		// after that, not the run's timeout's grace of 25 s.
		if took := ended().Sub(sent()); took > 2*time.Second+testTiming.grace+3*time.Second {
			t.Errorf("the step was killed %v after the run's timeout, not at the job's", took)
		}
		checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
	})
}

// TestTimeoutThenCancel checks that, as in format 1, whose context keeps
// the reason it ended with first, a job past its timeout, or whose run is,
// that the coordinator then cancels, as an always() step runs, isn't
// cancelled: cancelled() stays false, and a step whose if: is cancelled()
// is skipped.
func TestTimeoutThenCancel(t *testing.T) {
	t.Parallel()
	for _, deadline := range []string{"run", "job"} {
		t.Run(deadline, func(t *testing.T) {
			t.Parallel()
			slowAlways := command("echo always; sleep 2")
			slowAlways.If = cond("always()")
			onCancel := command("echo cancelled")
			onCancel.If = cond("cancelled()")
			job := newJob(command("trap '' TERM; echo waiting; sleep 30"), slowAlways, onCancel)
			if deadline == "job" {
				job.TimeoutSeconds = 1
			}
			f := workerapitest.NewFake(t, job)
			var first, second sync.Once
			f.Fault = func(r *http.Request) workerapitest.Fault {
				q := r.URL.Query()
				if q.Get(workerapi.QueryPart) == string(workerapi.LogStep) {
					switch q.Get(workerapi.QueryStep) {
					case "0":
						if deadline == "run" {
							first.Do(func() { f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonTimeout, GraceSeconds: 1}) })
						}
					case "1":
						second.Do(func() { f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1}) })
					}
				}
				return workerapitest.Answer
			}
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f,
				"started 0.0", "finished 0.0 failure -1",
				"started 1.0", "finished 1.0 success 0",
				"finished 2.0 skipped -1",
			)
			checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
		})
	}
}

// TestIgnoreErrorsDeadline checks that, as in format 1, a step with
// ignore-errors: whose last try a deadline stops, the job's timeout: or the
// run's, succeeds, and the job goes on as format 1's does: a step that
// comes to run after the deadline fails without starting, but one that
// ignores its errors too, which succeeds; a step that outlives a cancel
// runs, and so do cleanups; and the job ends failure, timeout, if a step
// failed, and as its steps did otherwise, as success, with no reason.
func TestIgnoreErrorsDeadline(t *testing.T) {
	t.Parallel()
	for _, deadline := range []string{"job", "run"} {
		for _, place := range []string{"middle", "last"} {
			t.Run(deadline+" "+place, func(t *testing.T) {
				t.Parallel()
				ignoring := command("echo waiting; sleep 30")
				ignoring.IgnoreErrors = plan.Literal(true)
				ignoring.Cleanup = plan.Literal("echo cleaned > $PW_JOB_DIR/cleaned")
				steps := []plan.Step{ignoring}
				if place == "middle" {
					later := command("echo later")
					later.IgnoreErrors = plan.Literal(true)
					always := command("echo always")
					always.If = cond("always()")
					steps = append(steps, later, command("echo after"), always)
				}
				job := newJob(steps...)
				if deadline == "job" {
					job.TimeoutSeconds = 1
				}
				f := workerapitest.NewFake(t, job)
				if deadline == "run" {
					timeoutOnFirstLog(f, 1)
				}
				if err := runWorker(t, f, t.Context(), Config{}); err != nil {
					t.Fatal(err)
				}
				if place == "last" {
					checkEvents(t, f,
						"started 0.0", "finished 0.0 success -1",
						"cleanup started 0.0", "cleanup finished 0.0 success 0",
					)
					checkEnd(t, f, plan.OutcomeSuccess, "")
				} else {
					checkEvents(t, f,
						"started 0.0", "finished 0.0 success -1",
						"finished 1.0 success -1",
						"finished 2.0 failure -1",
						"started 3.0", "finished 3.0 success 0",
						"cleanup started 0.0", "cleanup finished 0.0 success 0",
					)
					checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
				}
				if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "cleaned")); err != nil {
					t.Errorf("the cleanup: command didn't run: %v", err)
				}
			})
		}
	}
}

// TestDeadlineReason checks that, as in format 1, the job's timeout: or
// the run's fails a step with ignore-errors: on a try that isn't its last:
// one whose earlier try it stops, which isn't tried again, and one waiting
// to try again; and that the job ends with the reason timeout only when
// its deadline failed a step: one whose deadline stopped only a step that
// ignores its errors, and whose step after fails of its own, ends failure
// as its steps did, with no reason.
func TestDeadlineReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		steps func() []plan.Step
		// betweenTries has the run's timeout come once the first try has
		// ended, rather than once it has printed.
		betweenTries bool
		events       []string
		reason       plan.Reason
	}{{
		name: "an earlier try",
		steps: func() []plan.Step {
			s := command("echo waiting; sleep 30")
			s.IgnoreErrors = plan.Literal(true)
			s.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
			return []plan.Step{s}
		},
		events: []string{"started 0.0", "finished 0.0 failure -1"},
		reason: plan.ReasonTimeout,
	}, {
		name: "between tries",
		steps: func() []plan.Step {
			s := command("echo failing; exit 1")
			s.IgnoreErrors = plan.Literal(true)
			s.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: number(30)}
			return []plan.Step{s}
		},
		betweenTries: true,
		events:       []string{"started 0.0", "finished 0.0 failure 1", "finished 0.1 failure -1"},
		reason:       plan.ReasonTimeout,
	}, {
		name: "a later step fails",
		steps: func() []plan.Step {
			s := command("echo waiting; sleep 30")
			s.IgnoreErrors = plan.Literal(true)
			after := command("exit 3")
			after.If = cond("always()")
			return []plan.Step{s, after}
		},
		events: []string{
			"started 0.0", "finished 0.0 success -1",
			"started 1.0", "finished 1.0 failure 3",
		},
	}}
	for _, deadline := range []string{"job", "run"} {
		for _, c := range cases {
			t.Run(deadline+" "+c.name, func(t *testing.T) {
				t.Parallel()
				job := newJob(c.steps()...)
				if deadline == "job" {
					job.TimeoutSeconds = 1
				}
				f := workerapitest.NewFake(t, job)
				switch {
				case deadline == "run" && c.betweenTries:
					var once sync.Once
					f.OnEvent = func(e workerapi.Event) {
						if e.Finished && e.Step == 0 && e.Try == 0 {
							once.Do(func() { f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonTimeout, GraceSeconds: 1}) })
						}
					}
				case deadline == "run":
					timeoutOnFirstLog(f, 1)
				}
				if err := runWorker(t, f, t.Context(), Config{}); err != nil {
					t.Fatal(err)
				}
				checkEvents(t, f, c.events...)
				checkEnd(t, f, plan.OutcomeFailure, c.reason)
			})
		}
	}
}

// TestOutlivesCancel checks that, as in format 1, a step whose if: is
// cancel-aware runs to its end whatever happens to its job: neither the
// job's cancellation nor the step's own timeout: stops it, though its
// retry:'s timeout does; and a job whose steps all ran to their ends ends
// as they did.
func TestOutlivesCancel(t *testing.T) {
	t.Parallel()
	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()
		step := command("echo started; sleep 2; echo ended")
		step.If, step.Timeout = cond("always()"), number(1)
		f := workerapitest.NewFake(t, newJob(step))
		var once sync.Once
		f.Fault = func(r *http.Request) workerapitest.Fault {
			if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
				once.Do(func() { f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1}) })
			}
			return workerapitest.Answer
		}
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 success 0")
		checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "started\nended\n")
		// As in format 1, the job ends as its steps did: none was
		// cancelled.
		checkEnd(t, f, plan.OutcomeSuccess, "")
	})
	t.Run("retry timeout", func(t *testing.T) {
		t.Parallel()
		step := command("sleep 30")
		step.If = cond("always()")
		step.Retry = &plan.StepRetry{MaxRetries: number(0), Timeout: number(1)}
		f := workerapitest.NewFake(t, newJob(step))
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
		if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"step_timed_out"}) {
			t.Errorf("the step's diagnostics are %v", got)
		}
	})
}

// TestCancelEscalates checks that a later cancellation that gives less
// time, or the attempt ending without the worker, makes SIGKILL sooner,
// for a step and a cleanup: command alike.
func TestCancelEscalates(t *testing.T) {
	t.Parallel()
	t.Run("cancel again", func(t *testing.T) {
		t.Parallel()
		f := workerapitest.NewFake(t, newJob(command("trap '' TERM; echo waiting; sleep 30")))
		var once sync.Once
		f.Fault = func(r *http.Request) workerapitest.Fault {
			if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
				once.Do(func() {
					f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 60})
					f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1})
				})
			}
			return workerapitest.Answer
		}
		start := time.Now()
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed > 15*time.Second {
			t.Errorf("the step ran for %v, past the second cancellation's grace period", elapsed)
		}
		checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	})
	t.Run("halted cleanup", func(t *testing.T) {
		t.Parallel()
		step := command("true")
		step.Cleanup = plan.Literal("trap '' TERM; echo cleaning; sleep 30")
		f := workerapitest.NewFake(t, newJob(step))
		var cleaning atomic.Bool
		f.Fault = func(r *http.Request) workerapitest.Fault {
			if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogCleanup) {
				cleaning.Store(true)
			}
			if cleaning.Load() && r.URL.Path == workerapi.PathHeartbeat {
				return workerapitest.Fence
			}
			return workerapitest.Answer
		}
		start := time.Now()
		if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, errFenced) {
			t.Fatalf("got %v, want %v", err, errFenced)
		}
		if elapsed := time.Since(start); elapsed > 15*time.Second {
			t.Errorf("the cleanup ran for %v after the attempt was superseded", elapsed)
		}
	})
}

// TestEarlyCancel checks that a step whose early-cancel: is
// any-job-failed stops when the coordinator says another job failed, and
// that its job ends cancelled.
func TestEarlyCancel(t *testing.T) {
	t.Parallel()
	waits := command("sleep 30")
	waits.EarlyCancel = "any-job-failed"
	f := workerapitest.NewFake(t, newJob(waits))
	f.OnEvent = func(e workerapi.Event) {
		if !e.Finished {
			f.SetControl(workerapi.Control{AnyJobFailed: true})
		}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, "")
}

// TestFirstControl checks that what the coordinator asked before the
// claim applies from the job's first step, as format 1 starts a job whose
// context has already ended: after a cancel, the first step, which doesn't
// outlive it, ends cancelled without starting, and its cleanup runs, as
// does one that outlives it, with its own, and the job ends cancelled;
// after the run's timeout, the first step fails without starting, as a
// step that comes to run past a deadline does, so its cleanup runs, and
// the job ends failure, with the reason timeout; and after a job of the
// run failed, a step whose early-cancel: is any-job-failed doesn't run.
// It is so for a coordinator of each version of the API: of version 2,
// which gives the control in the claim, and of version 1, whose first
// control poll the worker waits for.
func TestFirstControl(t *testing.T) {
	t.Parallel()
	plain := command("touch $PW_JOB_DIR/ran")
	plain.Cleanup = plan.Literal("echo cleaned")
	always := command("echo always")
	always.If, always.Cleanup = cond("always()"), plan.Literal("echo cleaned")
	early := command("touch $PW_JOB_DIR/ran")
	early.EarlyCancel = "any-job-failed"
	for _, tc := range []struct {
		name    string
		job     *plan.ReadyJob
		control workerapi.Control
		events  []string
		outcome plan.Outcome
		reason  plan.Reason
	}{
		{
			"cancelled", newJob(plain, always),
			workerapi.Control{Cancel: true, Reason: plan.ReasonFailFast},
			[]string{
				"finished 0.0 cancelled -1",
				"started 1.0", "finished 1.0 success 0",
				"cleanup started 1.0", "cleanup finished 1.0 success 0",
				"cleanup started 0.0", "cleanup finished 0.0 success 0",
			},
			plan.OutcomeCancelled, plan.ReasonFailFast,
		},
		{
			"run timeout", newJob(plain, always),
			workerapi.Control{Cancel: true, Reason: plan.ReasonTimeout},
			[]string{
				"finished 0.0 failure -1",
				"started 1.0", "finished 1.0 success 0",
				"cleanup started 1.0", "cleanup finished 1.0 success 0",
				"cleanup started 0.0", "cleanup finished 0.0 success 0",
			},
			plan.OutcomeFailure, plan.ReasonTimeout,
		},
		{
			"a job failed", newJob(early),
			workerapi.Control{AnyJobFailed: true},
			[]string{"finished 0.0 cancelled -1"},
			plan.OutcomeCancelled, "",
		},
	} {
		for _, version := range []int{1, workerapi.Version2} {
			t.Run(tc.name+", version "+strconv.Itoa(version), func(t *testing.T) {
				t.Parallel()
				f := workerapitest.NewFake(t, tc.job)
				f.Speaks = version
				f.SetControl(tc.control)
				// The first poll is answered after the first step would
				// have started, were it not waiting for it.
				var requests counter
				f.Fault = func(r *http.Request) workerapitest.Fault {
					if r.URL.Path == workerapi.PathControl && requests.next(r.URL.Path) == 1 {
						// The delay is the test: a coordinator that answers
						// a little late, well within the worker's wait.
						time.Sleep(testTiming.firstControl / 5) //nolint:forbidigo // the delay is the test
					}
					return workerapitest.Answer
				}
				if err := runWorker(t, f, t.Context(), Config{}); err != nil {
					t.Fatal(err)
				}
				checkEvents(t, f, tc.events...)
				checkEnd(t, f, tc.outcome, tc.reason)
				if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "ran")); err == nil {
					t.Error("the first step ran")
				}
			})
		}
	}
}

// TestClaimControl checks that what the coordinator asked before the
// claim, given in the claim in an attempt of version 2 of the API, applies
// from the job's first step however slowly the coordinator answers its
// control polls: a stopped job's first step never starts, as a step of a
// job whose context has already ended doesn't in format 1. A worker of
// version 1, which waits for its first poll only a while, starts it, as
// TestFirstControlHangs shows.
func TestClaimControl(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("touch $PW_JOB_DIR/ran")))
	f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathControl {
			return workerapitest.Hang
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "finished 0.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "ran")); err == nil {
		t.Error("the first step of a stopped job ran")
	}
}

// TestFirstControlHangs checks that a first control poll that a
// coordinator of version 1 of the API doesn't answer, as a connection that
// is half open would hold it, doesn't hold the job: its first step starts
// once the worker has waited a while for it.
func TestFirstControlHangs(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo ran")))
	f.Speaks = 1
	var requests counter
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathControl && requests.next(r.URL.Path) == 1 {
			return workerapitest.Hang
		}
		return workerapitest.Answer
	}
	start := time.Now()
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < testTiming.firstControl || took > 15*time.Second {
		t.Errorf("the job took %v, with a first control poll that hung, and a wait for it of %v", took, testTiming.firstControl)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0")
	checkEnd(t, f, plan.OutcomeSuccess, "")
}

// TestCleanupOrder checks that the cleanup: commands of the steps that ran
// run at the job's end, whatever its outcome, in reverse order, each with
// its events and log, and that those of the steps that didn't run are
// skipped.
func TestCleanupOrder(t *testing.T) {
	t.Parallel()
	steps := []plan.Step{command("true"), command("true"), command("exit 1"), command("true")}
	steps[1].If = cond("false")
	for i := range steps {
		cleanup := "echo " + strconv.Itoa(i) + " >> $PW_JOB_DIR/order; echo cleaning " + strconv.Itoa(i)
		if i == 2 {
			cleanup += "; exit 5"
		}
		steps[i].Cleanup = plan.Literal(cleanup)
	}
	f := workerapitest.NewFake(t, newJob(steps...))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"finished 1.0 skipped -1",
		"started 2.0", "finished 2.0 failure 1",
		"finished 3.0 skipped -1",
		"cleanup finished 3.0 skipped -1",
		"cleanup started 2.0", "cleanup finished 2.0 failure 5",
		"cleanup finished 1.0 skipped -1",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	order, err := os.ReadFile(filepath.Join(f.Claim.RunDir, "order"))
	if err != nil || string(order) != "2\n0\n" {
		t.Errorf("the cleanup: commands ran in the order %q (%v), want 2, 0", order, err)
	}
	checkLog(t, f, logKey{workerapi.LogCleanup, 2, 0}, "cleaning 2\n")
}

// TestCleanupFails checks that a cleanup: command that fails fails its
// job, as in format 1, unless its step has ignore-errors:, and that it
// runs as a try of its own, with no retries.
func TestCleanupFails(t *testing.T) {
	t.Parallel()
	for _, ignore := range []bool{false, true} {
		t.Run("ignore-errors "+strconv.FormatBool(ignore), func(t *testing.T) {
			t.Parallel()
			step := command("true")
			step.Cleanup = plan.Literal(`echo "try $PW_WORKFLOW_STEP_CURRENT_RETRY of $PW_WORKFLOW_STEP_MAX_RETRIES"; exit 2`)
			step.Retry = &plan.StepRetry{MaxRetries: number(3), Interval: plan.Literal("0s")}
			step.IgnoreErrors = plan.Literal(ignore)
			f := workerapitest.NewFake(t, newJob(step))
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			outcome, want := plan.OutcomeFailure, "cleanup finished 0.0 failure 2"
			if ignore {
				outcome, want = plan.OutcomeSuccess, "cleanup finished 0.0 success 2"
			}
			checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "cleanup started 0.0", want)
			checkEnd(t, f, outcome, "")
			checkLog(t, f, logKey{workerapi.LogCleanup, 0, 0}, "try 0 of 0\n")
		})
	}
}

// TestCleanupNotStarted checks that a cleanup: that can't run fails its
// job, with what kept it from running and no exit code, as a step that
// can't run does: one whose expression fails, one of a step whose ssh:
// names another host, and one whose working directory can't be made.
func TestCleanupNotStarted(t *testing.T) {
	t.Parallel()
	failing := command("true")
	failing.Cleanup = cond("fromJSON('{')")
	elsewhere := command("true")
	elsewhere.If = cond("always()")
	elsewhere.SSH = plan.Literal(map[string]any{"remoteHost": "elsewhere"})
	elsewhere.Cleanup = plan.Literal("true")
	noDir := command("true")
	noDir.If = cond("always()")
	noDir.WorkingDirectory = plan.Literal("/dev/null/x")
	noDir.Cleanup = plan.Literal("true")
	f := workerapitest.NewFake(t, newJob(failing, elsewhere, noDir))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"finished 1.0 failure -1",
		"finished 2.0 failure -1",
		"cleanup finished 2.0 failure -1",
		"cleanup finished 1.0 failure -1",
		"cleanup finished 0.0 failure -1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	for step, want := range []string{"from_json_invalid", "step_ssh_not_supported", "step_not_started"} {
		if got := codes(finishedEvent(t, f, step, 0, true).Diagnostics); !slices.Equal(got, []string{want}) {
			t.Errorf("step %d's cleanup's diagnostics are %v, want %s", step, got, want)
		}
	}
}

// TestSecretsMasked checks that the run's secrets, and the values the job
// and its steps made from them, are masked in logs and events before they
// leave the worker, and that the job's outputs keep their values, as
// format 1 passes them to the jobs that need the job, named as End names
// them: private, and holding a secret, or only what was made from one.
func TestSecretsMasked(t *testing.T) {
	t.Parallel()
	step := command(`echo "token=$TOKEN made=$MADE matrix=matrix-secret"
echo "t=$TOKEN" >> $OUTPUTS; echo "m=$MADE" >> $OUTPUTS; echo "p=plain" >> $OUTPUTS`)
	step.Env = map[string]plan.Value{"TOKEN": cond("vars.token"), "MADE": cond("vars.token == 'hunter2' && 'swordfish-made' || 'no'")}
	j := newJob(step)
	j.Masked = []string{"matrix-secret"}
	f := workerapitest.NewFake(t, j)
	f.Claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hunter2"}, Secrets: []string{"vars.token"}}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "token=*** made=*** matrix=***\n")
	if got, want := finishedEvent(t, f, 0, 0, false).Outputs, map[string]string{"t": "***", "m": "***", "p": "plain"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the step's event shows its outputs as %v, want %v", got, want)
	}
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"t": "hunter2", "m": "swordfish-made", "p": "plain"}; !reflect.DeepEqual(end.Result.Outputs, want) {
		t.Errorf("the job's outputs are %v, want %v", end.Result.Outputs, want)
	}
	if !slices.Equal(end.Result.Private, []string{"m", "t"}) || !slices.Equal(end.Result.Secrets, []string{"m", "t"}) || !slices.Equal(end.Result.Derived, []string{"m"}) {
		t.Errorf("the job's outputs are private %v, secret %v and derived %v", end.Result.Private, end.Result.Secrets, end.Result.Derived)
	}
}

// counter counts requests to a path, safely for a fault function.
type counter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *counter) next(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[path]++
	return c.n[path]
}

// TestResume checks that requests whose connections drop, before or after
// the coordinator stored what they sent, and those it asks to try again,
// are sent again: a log of several uploads is stored whole, once, and
// events once each, in order.
func TestResume(t *testing.T) {
	t.Parallel()
	const size = 2*workerapi.MaxLogUpload + 1000
	f := workerapitest.NewFake(t, newJob(command("head -c "+strconv.Itoa(size)+" /dev/zero | tr '\\0' a; echo"), command("echo two")))
	var requests counter
	f.Fault = func(r *http.Request) workerapitest.Fault {
		n := requests.next(r.URL.Path)
		switch r.URL.Path {
		case workerapi.PathLogs:
			return []workerapitest.Fault{workerapitest.Answer, workerapitest.DropAnswer, workerapitest.Drop, workerapitest.Unavailable, workerapitest.DropAnswer}[n%5]
		case workerapi.PathEvents, workerapi.PathComplete, workerapi.PathClaim:
			return []workerapitest.Fault{workerapitest.Answer, workerapitest.DropAnswer, workerapitest.Unavailable}[n%3]
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, strings.Repeat("a", size)+"\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "two\n")
}

// TestLostLog checks that a worker sends a log again from where the
// coordinator says it has it to, when it has lost what it had stored that
// the worker still has: the worker keeps a log's bytes the coordinator has
// until it has all of them, or enough to be worth dropping (spool).
func TestLostLog(t *testing.T) {
	t.Parallel()
	const size = 2*workerapi.MaxLogUpload + 1000
	f := workerapitest.NewFake(t, newJob(command("head -c "+strconv.Itoa(size)+" /dev/zero | tr '\\0' a; echo")))
	// The first upload is held until the step has printed everything, so
	// that the worker has more to send once it is stored, and the second
	// is lost.
	printed := make(chan struct{})
	var once sync.Once
	f.OnEvent = func(e workerapi.Event) {
		if e.Finished {
			once.Do(func() { close(printed) })
		}
	}
	var requests counter
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path != workerapi.PathLogs || r.URL.Query().Get(workerapi.QueryPart) != string(workerapi.LogStep) {
			return workerapitest.Answer
		}
		switch requests.next(r.URL.Path) {
		case 1:
			<-printed
		case 2:
			return workerapitest.Forget
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, strings.Repeat("a", size)+"\n")
}

// TestSpool checks that a worker cut off from the coordinator keeps
// running its steps, and delivers their events and logs in order once it
// reaches it again.
func TestSpool(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo one; sleep 0.2; echo two"), command("echo three")))
	var down, up atomic.Int64
	f.OnEvent = func(e workerapi.Event) {
		if !e.Finished && e.Step == 0 {
			now := time.Now()
			down.Store(now.UnixNano())
			up.Store(now.Add(3 * time.Second).UnixNano())
		}
	}
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if now := time.Now().UnixNano(); now > down.Load() && now < up.Load() {
			return workerapitest.Drop
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "one\ntwo\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "three\n")
	if last := finishedEvent(t, f, 1, 0, false).Time; !last.Before(time.Unix(0, up.Load())) {
		t.Errorf("the last step ended at %v, after the coordinator was back at %v: the worker waited for it", last, time.Unix(0, up.Load()))
	}
}

// TestSpoolLimit checks that a worker cut off from the coordinator keeps
// no more of its logs than its limit, and that the step's event warns of
// what it didn't keep.
func TestSpoolLimit(t *testing.T) {
	t.Parallel()
	const limit, printed = 4096, 10000
	f := workerapitest.NewFake(t, newJob(command("head -c "+strconv.Itoa(printed)+" /dev/zero | tr '\\0' a; echo")))
	var finished atomic.Bool
	f.OnEvent = func(e workerapi.Event) {
		if e.Finished {
			finished.Store(true)
		}
	}
	// Every log is held until the step has finished: what the coordinator
	// stores of one, the setup log's too, frees room in the spool, and
	// what the step printed after would be kept again.
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathLogs && !finished.Load() {
			return workerapitest.Unavailable
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{SpoolLimit: limit}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	log, ended := logOf(f, logKey{workerapi.LogStep, 0, 0})
	if !ended || len(log) == 0 || len(log) > limit || strings.Trim(log, "a") != "" {
		t.Fatalf("the log kept is %d bytes (ended %t), want at most %d of a", len(log), ended, limit)
	}
	diags := finishedEvent(t, f, 0, 0, false).Diagnostics
	if len(diags) != 1 || diags[0].Code != problems.LogNotKept.Code() {
		t.Fatalf("the step's diagnostics are %v", codes(diags))
	}
	if got, want := diags[0].Params["size"], printed+1-len(log); got != float64(want) && got != want {
		t.Errorf("the step's event says %v bytes weren't kept, want %d", got, want)
	}
}

// TestFirstStepWaits checks that the worker doesn't start the first step
// until the coordinator has stored that it is starting.
func TestFirstStepWaits(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("touch $PW_JOB_DIR/ran")))
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

// TestFenced checks that a worker whose attempt was superseded, or whose
// credential the coordinator no longer accepts, stops its job's steps,
// without their cleanup: commands, and exits without completing it.
func TestFenced(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		path string
		flt  workerapitest.Fault
		want error
	}{
		{"heartbeat fenced", workerapi.PathHeartbeat, workerapitest.Fence, errFenced},
		{"control unauthorized", workerapi.PathControl, workerapitest.Unauthorized, errUnauthorized},
		{"logs fenced", workerapi.PathLogs, workerapitest.Fence, errFenced},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			step := command("echo started; sleep 30")
			step.Cleanup = plan.Literal("touch $PW_JOB_DIR/cleaned")
			f := workerapitest.NewFake(t, newJob(step))
			var started atomic.Bool
			f.OnEvent = func(e workerapi.Event) { started.Store(true) }
			f.Fault = func(r *http.Request) workerapitest.Fault {
				if started.Load() && r.URL.Path == tc.path {
					return tc.flt
				}
				return workerapitest.Answer
			}
			start := time.Now()
			if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if elapsed := time.Since(start); elapsed > 15*time.Second {
				t.Errorf("the worker took %v to stop", elapsed)
			}
			if f.End() != nil {
				t.Error("a fenced worker completed its attempt")
			}
			if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "cleaned")); err == nil {
				t.Error("a fenced worker ran a cleanup: command")
			}
		})
	}
}

// TestSetupFails checks the attempts that fail before their first step,
// with a problem whose code placement compares: a job whose steps are to
// run in a container, which this worker refuses rather than run them on
// the host; a job it can't read; and a run directory it can't make.
func TestSetupFails(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*workerapi.Claim)
		code   string
		params []string
	}{
		{"container", func(c *workerapi.Claim) {
			c.Container = &workerapi.Container{Runtime: "podman", Image: "ghcr.io/parallelworks/meridian-base@sha256:" + strings.Repeat("0", 64)}
		}, "container_not_supported", []string{"image", "runtime"}},
		// What the decoder says can quote the job, and a value made from a
		// secret in it, which isn't known yet to mask.
		{"unreadable job", func(c *workerapi.Claim) {
			c.Job = []byte(`{"key":"build","steps":[],"path":"/jobs/build","s3cr3t-key":"vm"}`)
		}, "job_unreadable", nil},
		{"job's version", func(c *workerapi.Claim) { c.PlanVersion = plan.Version + 1 }, "job_version_unsupported", []string{"supported", "version"}},
		// A job of the worker's version that a newer release wrote, with a
		// function the worker doesn't know, is one it can't read.
		{"a newer release's job", func(c *workerapi.Claim) {
			c.Job = []byte(`{"key":"build","name":"build","steps":[{"run":{"${{":[{"src":" s3cr3t() ","tree":{"args":[],"call":"s3cr3t"}}]},"path":"/jobs/build/steps/0"}],"path":"/jobs/build"}`)
		}, "job_unreadable", nil},
		{"run directory", func(c *workerapi.Claim) { c.RunDir = filepath.Join(file, "run") }, "run_dir_unusable", []string{"path", "reason"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, newJob(command("touch $PW_JOB_DIR/ran")))
			tc.change(&f.Claim)
			// A coordinator refuses a worker that doesn't read the run's
			// version; this one doesn't, to see what the worker makes of
			// a job it can't read.
			f.AnyVersion = true
			if err := runWorker(t, f, t.Context(), Config{}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f)
			end := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed)
			if end.Problem == nil || string(end.Problem.Code) != tc.code || end.Problem.Detail == "" ||
				!slices.Equal(slices.Sorted(maps.Keys(end.Problem.Params)), tc.params) {
				t.Fatalf("the attempt's problem is %+v, want %s with %v", end.Problem, tc.code, tc.params)
			}
			if end.Problem.Type != "/problems/workflow/"+tc.code {
				t.Errorf("the problem's type is %s", end.Problem.Type)
			}
			if shown := fmt.Sprint(end.Problem.Detail, end.Problem.Params); strings.Contains(shown, "s3cr3t") {
				t.Errorf("the problem quotes the job: %s", shown)
			}
			if setup, ended := logOf(f, logKey{part: workerapi.LogSetup}); !ended || !strings.Contains(setup, "the job failed before its first step") {
				t.Errorf("the setup log is %q (ended %t)", setup, ended)
			}
			if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "ran")); err == nil {
				t.Error("the step ran")
			}
		})
	}
}

// TestStepsRefused checks the steps this worker can't run, which fail
// with a problem that says why: a uses: step, a step on another host, a
// shell that isn't there, outputs too large to keep, and a shell format 1
// doesn't run.
func TestStepsRefused(t *testing.T) {
	t.Parallel()
	uses := plan.Step{Uses: "github/acme/flows@main", With: plan.Literal(map[string]any{"$yaml": "w.yaml"})}
	onHost := command("hostname")
	onHost.SSH = plan.Literal(map[string]any{"remoteHost": "elsewhere"})
	noShell := command("true")
	noShell.Shell = "/nonexistent/bash {0}"
	large := command("head -c 1100000 /dev/zero | tr '\\0' a > $OUTPUTS")
	zsh := command("true")
	zsh.Shell = "zsh {0}"
	steps := []plan.Step{uses, onHost, noShell, large, zsh}
	for i := range steps {
		steps[i].If = cond("always()")
	}
	f := workerapitest.NewFake(t, newJob(steps...))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"finished 0.0 failure -1",
		"finished 1.0 failure -1",
		"started 2.0", "finished 2.0 failure -1",
		"started 3.0", "finished 3.0 failure 0",
		"finished 4.0 failure -1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	for step, want := range []string{"uses_not_supported", "step_ssh_not_supported", "step_not_started", "outputs_too_large", "shell_not_supported"} {
		if got := codes(finishedEvent(t, f, step, 0, false).Diagnostics); !slices.Equal(got, []string{want}) {
			t.Errorf("step %d's diagnostics are %v, want %s", step, got, want)
		}
	}
}

// TestPlatformActions checks that a step that uses a built-in action only a
// platform provides fails each try as it runs, with a problem that says
// so, as a step whose command fails does: its retry: tries it again, and
// its ignore-errors: lets its last try succeed; and a skipped step that
// uses one is skipped, with no problem.
func TestPlatformActions(t *testing.T) {
	t.Parallel()
	use := func(action string, with map[string]any) plan.Step {
		return plan.Step{Uses: action, With: plan.Literal(with), If: cond("always()")}
	}
	skipped := use("parallelworks/scheduler-agent", nil)
	skipped.If = cond("false")
	retried := use("parallelworks/wait-for-agent", map[string]any{"agentId": "a"})
	retried.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	ignored := use("parallelworks/wait-for-agent", map[string]any{"agentId": "b"})
	ignored.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	ignored.IgnoreErrors = plan.Literal(true)
	steps := []plan.Step{
		skipped,
		use("parallelworks/scheduler-agent", map[string]any{"scheduler": "slurm"}),
		retried,
		ignored,
	}
	f := workerapitest.NewFake(t, newJob(steps...))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"finished 0.0 skipped -1",
		"started 1.0", "finished 1.0 failure -1",
		"started 2.0", "finished 2.0 failure -1", "started 2.1", "finished 2.1 failure -1",
		"started 3.0", "finished 3.0 failure -1", "started 3.1", "finished 3.1 success -1",
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	for _, tc := range []struct {
		step, try int
		want      string
	}{
		{1, 0, "action_needs_platform"},
		{2, 0, "action_needs_platform"},
		{2, 1, "action_needs_platform"},
		{3, 0, "action_needs_platform"},
		{3, 1, "action_needs_platform"},
	} {
		d := finishedEvent(t, f, tc.step, tc.try, false).Diagnostics
		if got := codes(d); !slices.Equal(got, []string{tc.want}) {
			t.Errorf("step %d's try %d's diagnostics are %v, want %s", tc.step, tc.try, got, tc.want)
		} else if d[0].Params["uses"] != steps[tc.step].Uses {
			t.Errorf("step %d's problem names %v, want %s", tc.step, d[0].Params["uses"], steps[tc.step].Uses)
		}
	}
	if d := finishedEvent(t, f, 0, 0, false).Diagnostics; len(d) != 0 {
		t.Errorf("the skipped step's diagnostics are %v", codes(d))
	}
	// Each try's log says why it failed, and ends.
	want := problems.ActionNeedsPlatform.Message(problems.Params{"uses": "parallelworks/wait-for-agent"}) + "\n"
	for try := range 2 {
		if log, ended := logOf(f, logKey{workerapi.LogStep, 2, try}); log != want || !ended {
			t.Errorf("the log of step 2's try %d is %q, ended %v; want %q", try, log, ended, want)
		}
	}
}

// TestPlatformActionIgnored checks that a job whose step uses an action
// only a platform provides, with ignore-errors:, goes on as after a step
// that succeeded, as after a command that fails with it: the steps after
// it run, success() is true for them, its cleanup: runs, and the job
// succeeds.
func TestPlatformActionIgnored(t *testing.T) {
	t.Parallel()
	agent := plan.Step{Uses: "parallelworks/scheduler-agent", With: plan.Literal(map[string]any{}), IgnoreErrors: plan.Literal(true)}
	agent.Cleanup = plan.Literal("echo cleaned")
	after := command("echo after")
	after.If = cond("success()")
	f := workerapitest.NewFake(t, newJob(agent, after))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success -1",
		"started 1.0", "finished 1.0 success 0",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeSuccess, "")
	if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"action_needs_platform"}) {
		t.Errorf("the step's diagnostics are %v", got)
	}
}

// TestOutputsLineTooLong checks that, as in format 1, a $OUTPUTS file with
// a line of 64 KiB or longer is read as having no outputs, and that the
// step's event warns of it.
func TestOutputsLineTooLong(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo a=1 >> $OUTPUTS; head -c 70000 /dev/zero | tr '\\0' b >> $OUTPUTS; echo >> $OUTPUTS; echo c=3 >> $OUTPUTS")))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0")
	e := finishedEvent(t, f, 0, 0, false)
	if len(e.Outputs) != 0 || !slices.Equal(codes(e.Diagnostics), []string{"outputs_line_too_long"}) {
		t.Errorf("the step's outputs are %v, and its diagnostics %v", e.Outputs, codes(e.Diagnostics))
	}
	if end := checkEnd(t, f, plan.OutcomeSuccess, ""); len(end.Result.Outputs) != 0 {
		t.Errorf("the job's outputs are %v", end.Result.Outputs)
	}
}

// TestOutputsNotUTF8 checks that bytes that aren't UTF-8 in $OUTPUTS, in a
// name or a value, are each replaced with U+FFFD, as format 1 stores
// outputs, and that the step's event warns of it, rather than end the
// attempt, which the API's JSON can't report: the job goes on, its cleanups
// run, and it reports its end. Its logs keep the bytes its steps printed,
// as they are.
func TestOutputsNotUTF8(t *testing.T) {
	t.Parallel()
	step := plan.Step{ID: "s", Run: plan.Literal(`printf 'out \377\376\n'; printf 'err \303(\n' >&2
printf 'bin=a\377b\376\376c\nna\351me=ok\nplain=1\n' >> "$OUTPUTS"`)}
	step.Cleanup = plan.Literal(`printf 'cleaning \377\n'; printf 'clean=\377\n' >> "$OUTPUTS"`)
	j := newJob(step, command(`printf 'before=\377\n' >> "$OUTPUTS"`), command(`printf 'shared=x\342\202\n' >> "$OUTPUTS"`))
	j.Outputs = map[string]plan.Value{"bin": cond("steps.s.outputs.bin")}
	f := workerapitest.NewFake(t, j)
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	bin := "a\uFFFDb\uFFFD\uFFFDc"
	for _, tc := range []struct {
		step    int
		cleanup bool
		outputs map[string]string
		name    string
		count   int
	}{
		{0, false, map[string]string{"bin": bin, "na\uFFFDme": "ok", "plain": "1"}, "bin", 2},
		{1, false, map[string]string{"before": "\uFFFD"}, "before", 1},
		// What an earlier step wrote to the job's file isn't this one's.
		{2, false, map[string]string{"shared": "x\uFFFD\uFFFD"}, "shared", 1},
		{0, true, map[string]string{"clean": "\uFFFD"}, "clean", 1},
	} {
		e := finishedEvent(t, f, tc.step, 0, tc.cleanup)
		if !reflect.DeepEqual(e.Outputs, tc.outputs) {
			t.Errorf("step %d (cleanup %t) shows its outputs as %q, want %q", tc.step, tc.cleanup, e.Outputs, tc.outputs)
		}
		if got := codes(e.Diagnostics); !slices.Equal(got, []string{"outputs_not_utf8"}) {
			t.Errorf("step %d (cleanup %t) has the diagnostics %v", tc.step, tc.cleanup, got)
			continue
		}
		if d := e.Diagnostics[0]; d.Params["name"] != tc.name || d.Params["count"] != float64(tc.count) {
			t.Errorf("step %d (cleanup %t) warns of %v", tc.step, tc.cleanup, d.Params)
		}
	}
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"before": "\uFFFD", "shared": "x\uFFFD\uFFFD", "clean": "\uFFFD", "bin": bin}; !reflect.DeepEqual(end.Result.Outputs, want) {
		t.Errorf("the job's outputs are %q, want %q", end.Result.Outputs, want)
	}
	if want := map[string]map[string]string{"s": {"bin": bin, "na\uFFFDme": "ok", "plain": "1"}}; !reflect.DeepEqual(end.Result.Steps, want) {
		t.Errorf("the job's step outputs are %q, want %q", end.Result.Steps, want)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "out \xff\xfe\nerr \xc3(\n")
	checkLog(t, f, logKey{workerapi.LogCleanup, 0, 0}, "cleaning \xff\n")
}

// TestProblemsNotUTF8 checks that a problem the worker makes from an
// error's text, which quotes a path as the system gives it, reaches the
// coordinator with each byte of it that isn't UTF-8 replaced with U+FFFD,
// as foundation's problems encode themselves with encoding/json, rather
// than end the attempt: here the path of the worker's directory, in a
// directory whose name isn't UTF-8, as one in a Latin-1 home is.
func TestProblemsNotUTF8(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "caf\xe9")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	step := command("true")
	step.WorkingDirectory = cond("env.OUTPUTS")
	next := command("true")
	next.If = cond("always()")
	f := workerapitest.NewFake(t, newJob(step, next))
	if err := runWorker(t, f, t.Context(), Config{Dir: dir}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "finished 0.0 failure -1", "started 1.0", "finished 1.0 success 0")
	checkEnd(t, f, plan.OutcomeFailure, "")
	ds := finishedEvent(t, f, 0, 0, false).Diagnostics
	if len(ds) != 1 || ds[0].Code != problems.StepNotStarted.Code() {
		t.Fatalf("step 0's diagnostics are %v", codes(ds))
	}
	if reason, _ := ds[0].Params["reason"].(string); !strings.Contains(reason, "/caf\uFFFD/") || !strings.Contains(ds[0].Detail, `/caf\xe9/`) {
		t.Errorf("the problem's reason is %q, and its message %q", reason, ds[0].Detail)
	}
}

// TestOutputsTooLargeNotUTF8 checks that the limit on a step's $OUTPUTS
// counts its outputs as they are kept, each byte that isn't UTF-8 as the 3
// of the U+FFFD that replaces it, though the file itself is within it, so
// that what the worker reports keeps to the limit.
func TestOutputsTooLargeNotUTF8(t *testing.T) {
	t.Parallel()
	// Lines of 60,004 bytes, each 180,004 once its bytes are replaced.
	lines := func(n int) plan.Value {
		return plan.Literal(`for i in $(seq ` + strconv.Itoa(n) + `); do printf 'v%d=' "$i"; head -c 60000 /dev/zero | LC_ALL=C tr '\0' '\377'; echo; done >> "$OUTPUTS"`)
	}
	over := plan.Step{ID: "over", Run: lines(6)}
	under := plan.Step{ID: "under", Run: lines(5), If: cond("always()")}
	f := workerapitest.NewFake(t, newJob(over, under))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure 0", "started 1.0", "finished 1.0 success 0")
	e := finishedEvent(t, f, 0, 0, false)
	if len(e.Outputs) != 0 || !slices.Equal(codes(e.Diagnostics), []string{"outputs_too_large"}) || e.Diagnostics[0].Params["size"] != float64(6*180004) {
		t.Errorf("step 0 has %d outputs, and the diagnostics %v", len(e.Outputs), e.Diagnostics)
	}
	if e := finishedEvent(t, f, 1, 0, false); len(e.Outputs) != 5 || !slices.Equal(codes(e.Diagnostics), []string{"outputs_not_utf8"}) {
		t.Errorf("step 1 has %d outputs, and the diagnostics %v", len(e.Outputs), codes(e.Diagnostics))
	}
	checkEnd(t, f, plan.OutcomeFailure, "")
}

// TestReportNotEncoded checks that a report the worker can't encode, its
// own bug, fails the job, rather than stop the attempt as if it had ended
// without the worker: an event is sent without its outputs and problems,
// with one that says so, and the attempt's end as a failure, without what
// it couldn't encode, if it couldn't encode that.
func TestReportNotEncoded(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// event and end are the outputs of the step's finished event and
		// of the attempt's end.
		event, end map[string]string
	}{
		{"event", map[string]string{"a": "\xff"}, map[string]string{"a": "ok"}},
		{"end", map[string]string{"a": "ok"}, map[string]string{"a": "\xff"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			job := newJob(command("true"))
			f := workerapitest.NewFake(t, job)
			a, ctx, start := sendingAttempt(t, f, job)
			start()
			a.events.add(workerapi.Event{Time: now(), Step: 0})
			a.events.add(workerapi.Event{Time: now(), Finished: true, Step: 0, Outcome: plan.OutcomeSuccess, Outputs: tc.event})
			end := &workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeSuccess, Outputs: tc.end, Steps: map[string]map[string]string{"s": tc.end}}}
			if err := a.deliver(ctx, end); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f, "started 0.0", "finished 0.0 success 0")
			e := finishedEvent(t, f, 0, 0, false)
			got := checkEnd(t, f, plan.OutcomeFailure, "")
			if !slices.Equal(codes(got.Diagnostics), []string{"report_not_encoded"}) || got.Diagnostics[0].Pointer != "#/jobs/build" {
				t.Errorf("the end has the diagnostics %v", got.Diagnostics)
			}
			if tc.name == "event" {
				if e.Outputs != nil || !slices.Equal(codes(e.Diagnostics), []string{"report_not_encoded"}) || e.Diagnostics[0].Pointer != "#/jobs/build/steps/0" {
					t.Errorf("the event has the outputs %q and the diagnostics %v", e.Outputs, e.Diagnostics)
				}
				if !reflect.DeepEqual(got.Result.Outputs, tc.end) || !reflect.DeepEqual(got.Result.Steps, end.Result.Steps) {
					t.Errorf("the end has the outputs %q and %q", got.Result.Outputs, got.Result.Steps)
				}
			} else {
				if !reflect.DeepEqual(e.Outputs, tc.event) || len(e.Diagnostics) != 0 {
					t.Errorf("the event has the outputs %q and the diagnostics %v", e.Outputs, e.Diagnostics)
				}
				if got.Result.Outputs != nil || got.Result.Steps != nil {
					t.Errorf("the end has the outputs %q and %q", got.Result.Outputs, got.Result.Steps)
				}
			}
		})
	}
}

// TestReportNotEncodedBatch checks that of a batch of events, only the one
// the worker can't encode is cut, and the rest keep their outputs, in
// order; that an end it can't encode loses its outputs and the lists that
// name them together; and that a cancelled job stays cancelled.
func TestReportNotEncodedBatch(t *testing.T) {
	t.Parallel()
	job := newJob(command("true"), command("true"))
	f := workerapitest.NewFake(t, job)
	a, ctx, start := sendingAttempt(t, f, job)
	// Every event is queued before the sender starts, so they go in one
	// batch.
	a.events.add(workerapi.Event{Time: now(), Step: 0})
	a.events.add(workerapi.Event{Time: now(), Finished: true, Step: 0, Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"bad": "\xff"}})
	a.events.add(workerapi.Event{Time: now(), Step: 1})
	a.events.add(workerapi.Event{Time: now(), Finished: true, Step: 1, Outcome: plan.OutcomeSuccess, Outputs: map[string]string{"good": "ok"}})
	start()
	end := &workerapi.Complete{
		Reason: plan.ReasonCancelled,
		Result: plan.JobResult{Outcome: plan.OutcomeCancelled, Outputs: map[string]string{"x": "\xff", "s": "secret"}, Private: []string{"s"}, Secrets: []string{"s"}},
	}
	if err := a.deliver(ctx, end); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	if e := finishedEvent(t, f, 0, 0, false); e.Outputs != nil || !slices.Equal(codes(e.Diagnostics), []string{"report_not_encoded"}) {
		t.Errorf("the event the worker couldn't encode has the outputs %q and the diagnostics %v", e.Outputs, codes(e.Diagnostics))
	}
	if e := finishedEvent(t, f, 1, 0, false); !reflect.DeepEqual(e.Outputs, map[string]string{"good": "ok"}) || len(e.Diagnostics) != 0 {
		t.Errorf("the event after it has the outputs %q and the diagnostics %v", e.Outputs, codes(e.Diagnostics))
	}
	got := checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	if got.Result.Outputs != nil || got.Result.Private != nil || got.Result.Secrets != nil {
		t.Errorf("the end has the outputs %q, private %v and secret %v", got.Result.Outputs, got.Result.Private, got.Result.Secrets)
	}
	if !slices.Equal(codes(got.Diagnostics), []string{"report_not_encoded"}) {
		t.Errorf("the end has the diagnostics %v", codes(got.Diagnostics))
	}
}

// TestReportNotEncodedProblem checks that an end whose problem the worker
// can't encode, as one with a param that is NaN, is sent with the problem's
// code alone, which placement compares, and its title.
func TestReportNotEncodedProblem(t *testing.T) {
	t.Parallel()
	job := newJob(command("true"))
	f := workerapitest.NewFake(t, job)
	a, ctx, start := sendingAttempt(t, f, job)
	start()
	p := problems.RunDirUnusable.Problem(problems.Params{"path": "/run", "reason": math.NaN()})
	if err := a.deliver(ctx, &workerapi.Complete{Result: plan.JobResult{Outcome: plan.OutcomeFailure}, Reason: plan.ReasonSetupFailed, Problem: p}); err != nil {
		t.Fatal(err)
	}
	got := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed)
	if got.Problem == nil || got.Problem.Code != problems.RunDirUnusable.Code() || got.Problem.Params != nil || got.Problem.Title != p.Title {
		t.Errorf("the end's problem is %+v", got.Problem)
	}
	if !slices.Equal(codes(got.Diagnostics), []string{"report_not_encoded"}) {
		t.Errorf("the end has the diagnostics %v", codes(got.Diagnostics))
	}
}

// TestAPIVersion checks the worker's version of the worker API: it claims
// as one that speaks workerapi.Version, which the Fake checks; it exits
// without running anything when the coordinator refuses it as older than
// the attempt needs, with the coordinator's problem, which says both
// versions; and when the coordinator serves the attempt at a version the
// worker doesn't speak, as an answer it can't read.
func TestAPIVersion(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if end := f.End(); end == nil || end.Result.Outcome != plan.OutcomeSuccess {
		t.Errorf("the attempt ended %+v", end)
	}

	f = workerapitest.NewFake(t, newJob(command("true")))
	f.Speaks, f.Needs = workerapi.Version+1, workerapi.Version+1
	err := runWorker(t, f, t.Context(), Config{})
	p, ok := errors.AsType[*problem.Problem](err)
	if !ok || p.Code != workerapi.WorkerAPITooOld || p.Params[workerapi.ParamVersion] != float64(workerapi.Version) || p.Params[workerapi.ParamNeeded] != float64(workerapi.Version+1) {
		t.Fatalf("a worker older than the attempt needs got %v", err)
	}
	if requests := f.Requests(); len(requests) != 1 {
		t.Errorf("the worker made the requests %v", requests)
	}

	for _, served := range []int{-1, workerapi.Version + 1} {
		f = workerapitest.NewFake(t, newJob(command("true")))
		f.AnyVersion, f.Claim.APIVersion = true, served
		err := runWorker(t, f, t.Context(), Config{})
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.CoordinatorAnswerInvalid.Code() || p.Params["at"] != "/apiVersion" {
			t.Errorf("a claim served at version %d got %v", served, err)
		}
		if requests := f.Requests(); len(requests) > 1 {
			t.Errorf("a claim served at version %d: the worker made the requests %v", served, requests)
		}
	}

	// A claim of version 1 with a control, which version 1 doesn't have.
	f = workerapitest.NewFake(t, newJob(command("true")))
	f.Claim.APIVersion, f.Claim.Control = 1, &workerapi.Control{Version: 1}
	err = runWorker(t, f, t.Context(), Config{})
	if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.CoordinatorAnswerInvalid.Code() {
		t.Errorf("a claim of version 1 with a control got %v", err)
	}
	if requests := f.Requests(); len(requests) > 1 {
		t.Errorf("a claim of version 1 with a control: the worker made the requests %v", requests)
	}
}

// TestLastTry checks that a worker of version 2 of the API says, of a
// failed try, whether it was its step's last, wherever the try fails: so
// of a step without a retry:, of a retry:'s last try but not the ones
// before it, of a step past its timeout:, which isn't tried again, of a
// step whose if: fails to evaluate, which can't start, and of steps the
// job's deadline fails, the one it stops and one that comes to run after
// it; not of a step that fails and succeeds, with ignore-errors:, or of a
// cleanup. A worker served at version 1 says it of none, which version 1
// doesn't have. A try that failed and doesn't say so would mark its run
// for early-cancel: any-job-failed only as its job ends.
func TestLastTry(t *testing.T) {
	t.Parallel()
	once := command("exit 1")
	retried := command("exit 1")
	retried.If = cond("failure()")
	retried.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s")}
	ignored := command("exit 1")
	ignored.If, ignored.IgnoreErrors = cond("failure()"), plan.Literal(true)
	timedOut := command("sleep 10")
	timedOut.If, timedOut.Timeout = cond("failure()"), number(1)
	cleanup := command("true")
	cleanup.If, cleanup.Cleanup = cond("failure()"), plan.Literal("exit 1")
	badIf := command("true")
	badIf.If = cond("fromJSON('not json')")
	// Past the job's deadline: the step it stops, and one after it whose
	// if: is true, but that doesn't outlive the deadline, so never starts.
	stopped := command("sleep 30")
	afterDeadline := command("true")
	afterDeadline.If = cond("failure()")
	deadline := newJob(stopped, afterDeadline)
	deadline.TimeoutSeconds = 1
	for _, tc := range []struct {
		name string
		job  *plan.ReadyJob
		want []string
	}{
		{"steps", newJob(once, retried, ignored, timedOut, cleanup, badIf), []string{"0.0", "1.1", "3.0", "5.0"}},
		{"the job's deadline", deadline, []string{"0.0", "1.0"}},
	} {
		for _, version := range []int{1, workerapi.Version2} {
			t.Run(tc.name+", version "+strconv.Itoa(version), func(t *testing.T) {
				t.Parallel()
				f := workerapitest.NewFake(t, tc.job)
				f.Speaks = version
				if err := runWorker(t, f, t.Context(), Config{}); err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, e := range f.Events() {
					if e.LastTry {
						got = append(got, fmt.Sprintf("%d.%d", e.Step, e.Try))
					}
				}
				want := tc.want
				if version < workerapi.Version2 {
					want = nil
				}
				if !slices.Equal(got, want) {
					t.Errorf("the events of the tries %v say they were their steps' last, not %v:\n%s", got, want, strings.Join(stepEvents(t, f), "\n"))
				}
			})
		}
	}
}

// TestPlanTooNew checks that a worker that doesn't read the run's plan
// version exits without running anything.
func TestPlanTooNew(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	// The run's plan is of a version newer than the worker reads.
	f.Reads, f.Claim.PlanVersion = plan.Version+1, plan.Version+1
	if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, errPlanTooNew) {
		t.Fatalf("got %v, want %v", err, errPlanTooNew)
	}
	if requests := f.Requests(); len(requests) != 1 {
		t.Errorf("the worker made the requests %v", requests)
	}
}

// TestTLS checks that a worker reaches a coordinator over TLS 1.3 when its
// key has the hash the worker was given, and refuses one whose key
// doesn't, without trying it again.
func TestTLS(t *testing.T) {
	t.Parallel()
	for _, pinned := range []bool{true, false} {
		t.Run("pinned "+strconv.FormatBool(pinned), func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, newJob(command("echo over tls")))
			// HTTP/1.1, though the server offers HTTP/2, so that a
			// replica's 503 can send the next request to another.
			var protos sync.Map
			f.Fault = func(r *http.Request) workerapitest.Fault {
				protos.Store(r.Proto, true)
				return workerapitest.Answer
			}
			srv := httptest.NewUnstartedServer(f)
			srv.EnableHTTP2 = true
			srv.Config.ErrorLog = log.New(io.Discard, "", 0)
			srv.StartTLS()
			t.Cleanup(srv.Close)
			hash := keyHash(t, srv)
			if !pinned {
				hash = "sha256:" + strings.Repeat("0", 64)
			}
			err := runWorker(t, f, t.Context(), Config{Environ: []string{
				workerapi.EnvCoordinator + "=" + srv.Listener.Addr().String(),
				workerapi.EnvKeySHA256 + "=" + hash,
			}})
			if !pinned {
				if problemOf(err, problems.CoordinatorKeyMismatch) == nil {
					t.Fatalf("got %v, want coordinator_key_mismatch", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			checkEnd(t, f, plan.OutcomeSuccess, "")
			checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "over tls\n")
			protos.Range(func(proto, _ any) bool {
				if proto != "HTTP/1.1" {
					t.Errorf("the worker spoke %v", proto)
				}
				return true
			})
		})
	}
}

// TestOutputsNotFile checks that a step that puts something other than a
// file at $OUTPUTS, such as a FIFO, which reading would wait on forever,
// fails, and that the steps after it have a file again; and that a
// symbolic link a step puts there is followed, as format 1 follows it,
// unless it names something other than a file.
func TestOutputsNotFile(t *testing.T) {
	t.Parallel()
	fifo := command(`rm -f "$OUTPUTS"; mkfifo "$OUTPUTS"`)
	link := command(`printf 'l=1\n' > "$PW_JOB_DIR/target"; ln -sf "$PW_JOB_DIR/target" "$OUTPUTS"`)
	through := command("echo a=2 >> $OUTPUTS")
	device := command(`ln -sf /dev/null "$OUTPUTS"`)
	for _, s := range []*plan.Step{&link, &through, &device} {
		s.If = cond("always()")
	}
	f := workerapitest.NewFake(t, newJob(fifo, link, through, device))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := runWorker(t, f, ctx, Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
		"started 3.0", "finished 3.0 failure 0",
	)
	for _, step := range []int{0, 3} {
		if got := codes(finishedEvent(t, f, step, 0, false).Diagnostics); !slices.Equal(got, []string{"outputs_not_file"}) {
			t.Errorf("step %d's diagnostics are %v", step, got)
		}
	}
	for step, want := range map[int]map[string]string{1: {"l": "1"}, 2: {"a": "2"}} {
		if got := finishedEvent(t, f, step, 0, false).Outputs; !reflect.DeepEqual(got, want) {
			t.Errorf("step %d's outputs are %v, want %v", step, got, want)
		}
	}
	if data, err := os.ReadFile(filepath.Join(f.Claim.RunDir, "target")); err != nil || string(data) != "l=1\na=2\n" {
		t.Errorf("the file the link names holds %q (%v): the step after it didn't write through it", data, err)
	}
}

// TestSharedOutputsTooLarge checks that only the step that takes its
// job's shared $OUTPUTS past the limit fails, and not the steps without
// an id after it, which can't add to it.
func TestSharedOutputsTooLarge(t *testing.T) {
	t.Parallel()
	shared := command("echo b=2 >> $OUTPUTS")
	own := plan.Step{ID: "own", Run: plan.Literal("echo c=3 >> $OUTPUTS")}
	for _, s := range []*plan.Step{&shared, &own} {
		s.If = cond("always()")
	}
	f := workerapitest.NewFake(t, newJob(command("head -c 1100000 /dev/zero | tr '\\0' a >> $OUTPUTS"), shared, own))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
	)
	if got := codes(finishedEvent(t, f, 0, 0, false).Diagnostics); !slices.Equal(got, []string{"outputs_too_large"}) {
		t.Errorf("step 0's diagnostics are %v", got)
	}
	if e := finishedEvent(t, f, 1, 0, false); len(e.Diagnostics) != 0 || len(e.Outputs) != 0 {
		t.Errorf("step 1 has the diagnostics %v and the outputs %v", codes(e.Diagnostics), e.Outputs)
	}
	if got := finishedEvent(t, f, 2, 0, false).Outputs; !reflect.DeepEqual(got, map[string]string{"c": "3"}) {
		t.Errorf("step 2's outputs are %v", got)
	}
}

// TestDirRemoved checks that a step that removes the worker's directory,
// as rm -rf "$TMPDIR"/* does, doesn't keep the job from going on: the
// worker makes it again.
func TestDirRemoved(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command(`rm -rf "$(dirname "$OUTPUTS")"`), command("echo next; echo a=1 >> $OUTPUTS")))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "next\n")
}

// TestProblemsMasked checks that a problem the worker makes from an error,
// which can quote a value made from a secret, is masked, in a step's event
// and in the attempt's end.
func TestProblemsMasked(t *testing.T) {
	t.Parallel()
	secret := func(f *workerapitest.Fake) {
		f.Claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hunter2"}, Secrets: []string{"vars.token"}}
	}
	t.Run("step", func(t *testing.T) {
		t.Parallel()
		step := command("true")
		step.WorkingDirectory = plan.Literal("/proc/hunter2/x")
		f := workerapitest.NewFake(t, newJob(step))
		secret(f)
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		for _, d := range finishedEvent(t, f, 0, 0, false).Diagnostics {
			if strings.Contains(fmt.Sprint(d.Params, d.Detail), "hunter2") {
				t.Errorf("diagnostic %s quotes the secret: %v, %q", d.Code, d.Params, d.Detail)
			}
		}
	})
	t.Run("setup", func(t *testing.T) {
		t.Parallel()
		file := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(file, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		f := workerapitest.NewFake(t, newJob(command("true")))
		secret(f)
		f.Claim.RunDir = filepath.Join(file, "hunter2")
		if err := runWorker(t, f, t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		end := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed)
		if p := end.Problem; p == nil || strings.Contains(fmt.Sprint(p.Params, p.Detail), "hunter2") {
			t.Errorf("the attempt's problem quotes the secret: %+v", p)
		}
		if setup, _ := logOf(f, logKey{part: workerapi.LogSetup}); strings.Contains(setup, "hunter2") {
			t.Errorf("the setup log quotes the secret: %q", setup)
		}
	})
}

// TestPlatformEnv checks the variables a step gets from what the worker
// knows, and from what a platform gives the job, as format 1 lays them:
// over the workflow's environment, with the worker's own over them, and a
// secret one masked.
func TestPlatformEnv(t *testing.T) {
	t.Parallel()
	step := command(`echo "$PW_USER $PW_JOB_ID $PW_API_KEY $PW_PARENT_JOB_DIR $PW_WORKFLOW_JOB_CURRENT_RETRY $PW_WORKFLOW_JOB_MAX_RETRIES $PW_MATRIX_INDEX"; [ "$OUTPUTS" != /x ]`)
	step.Env = map[string]plan.Value{"PW_USER": plan.Literal("workflow"), "OUTPUTS": plan.Literal("/x")}
	j := newJob(step)
	j.Key, j.Matrix = "a#b#7", plan.DataOf(map[string]any{"n": int64(7)})
	j.Retry = &plan.ReadyJobRetry{Reasons: []plan.Reason{plan.ReasonWorkerLost}, MaxRetries: 3, IntervalSeconds: 60}
	f := workerapitest.NewFake(t, j)
	f.Claim.Retry = 2
	f.Claim.Env = map[string]workerapi.EnvVar{
		"PW_USER": {Value: "alice"}, "PW_JOB_ID": {Value: "wf-00042"}, "PW_API_KEY": {Value: "k3y-s3cret", Secret: true},
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "alice wf-00042 *** "+f.Claim.RunDir+" 2 3 7\n")
}

// TestEventsLost checks that a worker whose coordinator has lost events it
// said it had stored, which the worker no longer has, stops its job.
func TestEventsLost(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true"), command("sleep 30")))
	var forgot atomic.Bool
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path != workerapi.PathEvents {
			return workerapitest.Answer
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		// Forget them as the second step starts.
		if strings.Contains(string(body), `"step":1`) && !forgot.Swap(true) {
			return workerapitest.Forget
		}
		return workerapitest.Answer
	}
	start := time.Now()
	err := runWorker(t, f, t.Context(), Config{})
	if p := problemOf(err, problems.CoordinatorLostData); p == nil || p.Params["data"] != "events" {
		t.Fatalf("got %v, want the events lost", err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("the worker took %v to stop", elapsed)
	}
}

// TestRedirectRefused checks that a worker follows no redirect: it talks
// only to the coordinator it was given.
func TestRedirectRefused(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	var claims atomic.Int32
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathClaim {
			claims.Add(1)
			return workerapitest.Redirect
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err == nil {
		t.Fatal("claimed through a redirect")
	}
	if n := claims.Load(); n != 1 {
		t.Errorf("the worker made %d claims", n)
	}
}

// TestCleanupRemovesRunDir checks that a cleanup: that removes the run's
// directory, as rm -r ${PW_JOB_DIR} does in workflows we know of, leaves
// the cleanups after it, and the worker's files, as they were: the
// worker's directory is outside the run's.
func TestCleanupRemovesRunDir(t *testing.T) {
	t.Parallel()
	first := command("true")
	first.Cleanup = plan.Literal("echo cleaned first")
	second := command("echo two=2 >> $OUTPUTS")
	second.Cleanup = plan.Literal(`rm -r "$PW_JOB_DIR"`)
	f := workerapitest.NewFake(t, newJob(first, second))
	home := t.TempDir()
	if err := runWorker(t, f, t.Context(), Config{Environ: []string{"HOME=" + home}}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"started 1.0", "finished 1.0 success 0",
		"cleanup started 1.0", "cleanup finished 1.0 success 0",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if end.Result.Outputs["two"] != "2" {
		t.Errorf("the job's outputs are %v", end.Result.Outputs)
	}
}

// TestProblemsMaskedEscaped checks that a problem the worker makes is
// masked before its message is made, so that a secret with a character
// the message escapes, such as a tab, isn't quoted escaped.
func TestProblemsMaskedEscaped(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f := workerapitest.NewFake(t, newJob(command("true")))
	f.Claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hun\tter2"}, Secrets: []string{"vars.token"}}
	f.Claim.RunDir = filepath.Join(file, "hun\tter2")
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	end := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed)
	if end.Problem == nil || strings.Contains(end.Problem.Detail, "ter2") {
		t.Errorf("the problem quotes the secret: %+v", end.Problem)
	}
	if setup, _ := logOf(f, logKey{part: workerapi.LogSetup}); strings.Contains(setup, "ter2") {
		t.Errorf("the setup log quotes the secret: %q", setup)
	}
}
