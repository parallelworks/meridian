package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
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

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
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

func checkEvents(t *testing.T, f *fakeCoordinator, want ...string) {
	t.Helper()
	if got := f.stepEvents(); !slices.Equal(got, want) {
		t.Errorf("events:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func checkEnd(t *testing.T, f *fakeCoordinator, outcome plan.Outcome, reason plan.Reason) *workerapi.Complete {
	t.Helper()
	f.mu.Lock()
	end := f.end
	f.mu.Unlock()
	if end == nil {
		t.Fatal("the attempt didn't complete")
	}
	if end.Outcome != outcome || end.Reason != reason {
		t.Errorf("the attempt ended %s (%s), want %s (%s)", end.Outcome, end.Reason, outcome, reason)
	}
	if want := int64(len(f.stepEvents())); end.Seq != want {
		t.Errorf("Complete's seq is %d, want %d", end.Seq, want)
	}
	return end
}

func checkLog(t *testing.T, f *fakeCoordinator, key logKey, want string) {
	t.Helper()
	got, ended := f.log(key)
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
	f := newFakeCoordinator(t, j)
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
	)
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"a": "3", "multi": "x\ny", "b": "2"}; !reflect.DeepEqual(end.Outputs, want) {
		t.Errorf("the job's outputs are %v, want %v", end.Outputs, want)
	}
	for _, tc := range []struct {
		step int
		want map[string]string
	}{
		{0, map[string]string{"a": "1", "multi": "x\ny"}},
		{1, map[string]string{"b": "2"}},
		{2, map[string]string{"a": "3"}},
	} {
		if got := f.finishedEvent(tc.step, 0, false).Outputs; !reflect.DeepEqual(got, tc.want) {
			t.Errorf("step %d's outputs are %v, want %v", tc.step, got, tc.want)
		}
	}
	dir := f.claim.RunDir
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "hello\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0},
		"dir="+dir+" job_dir="+dir+" ci=overridden job=run1 inherited=inherited step=env\n0\n")
	checkLog(t, f, logKey{workerapi.LogStep, 2, 0}, filepath.Join(dir, "sub/dir")+"\n")
	if setup, ended := f.log(logKey{part: workerapi.LogSetup}); !ended || !strings.Contains(setup, "claimed the attempt") {
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
	f := newFakeCoordinator(t, newJob(command("echo before; exit 3"), command("echo skipped"), always, ignore))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	if _, ok := f.log(logKey{workerapi.LogStep, 1, 0}); ok {
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
	f := newFakeCoordinator(t, newJob(succeeds, fails))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
		f := newFakeCoordinator(t, newJob(slow, after))
		start := time.Now()
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed > 15*time.Second {
			t.Errorf("the step ran for %v", elapsed)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "started 1.0", "finished 1.0 success 0")
		checkEnd(t, f, plan.OutcomeFailure, "")
		if got := codes(f.finishedEvent(0, 0, false).Diagnostics); !slices.Equal(got, []string{"step_timed_out"}) {
			t.Errorf("the step's diagnostics are %v", got)
		}
	})
	t.Run("try", func(t *testing.T) {
		t.Parallel()
		slow := command("sleep 30")
		slow.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: plan.Literal("0s"), Timeout: number(1)}
		f := newFakeCoordinator(t, newJob(slow))
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "started 0.1", "finished 0.1 failure -1")
		checkEnd(t, f, plan.OutcomeFailure, "")
		for try := range 2 {
			if got := codes(f.finishedEvent(0, try, false).Diagnostics); !slices.Equal(got, []string{"step_timed_out"}) {
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
		f := newFakeCoordinator(t, j)
		if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	f := newFakeCoordinator(t, newJob(stubborn, always, command("echo skipped"), onCancel))
	// Cancel once the step has printed, and so ignores SIGTERM.
	var once sync.Once
	f.fault = func(r *http.Request) fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(func() { f.setControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1}) })
		}
		return answer
	}
	start := time.Now()
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	if _, err := os.Stat(filepath.Join(f.claim.RunDir, "cleaned")); err != nil {
		t.Errorf("the cleanup: command didn't run: %v", err)
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
		f := newFakeCoordinator(t, newJob(step))
		var once sync.Once
		f.fault = func(r *http.Request) fault {
			if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
				once.Do(func() { f.setControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1}) })
			}
			return answer
		}
		if err := f.runWorker(t.Context(), Config{}); err != nil {
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
		f := newFakeCoordinator(t, newJob(step))
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1")
		if got := codes(f.finishedEvent(0, 0, false).Diagnostics); !slices.Equal(got, []string{"step_timed_out"}) {
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
		f := newFakeCoordinator(t, newJob(command("trap '' TERM; echo waiting; sleep 30")))
		var once sync.Once
		f.fault = func(r *http.Request) fault {
			if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
				once.Do(func() {
					f.setControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 60})
					f.setControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1})
				})
			}
			return answer
		}
		start := time.Now()
		if err := f.runWorker(t.Context(), Config{}); err != nil {
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
		f := newFakeCoordinator(t, newJob(step))
		var cleaning atomic.Bool
		f.fault = func(r *http.Request) fault {
			if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogCleanup) {
				cleaning.Store(true)
			}
			if cleaning.Load() && r.URL.Path == workerapi.PathHeartbeat {
				return fence
			}
			return answer
		}
		start := time.Now()
		if err := f.runWorker(t.Context(), Config{}); !errors.Is(err, errFenced) {
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
	f := newFakeCoordinator(t, newJob(waits))
	f.onEvent = func(e workerapi.Event) {
		if !e.Finished {
			f.setControlLocked(workerapi.Control{AnyJobFailed: true})
		}
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, "")
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
	f := newFakeCoordinator(t, newJob(steps...))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	order, err := os.ReadFile(filepath.Join(f.claim.RunDir, "order"))
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
			f := newFakeCoordinator(t, newJob(step))
			if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	f := newFakeCoordinator(t, j)
	f.claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hunter2"}, Secrets: []string{"vars.token"}}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "token=*** made=*** matrix=***\n")
	if got, want := f.finishedEvent(0, 0, false).Outputs, map[string]string{"t": "***", "m": "***", "p": "plain"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the step's event shows its outputs as %v, want %v", got, want)
	}
	end := checkEnd(t, f, plan.OutcomeSuccess, "")
	if want := map[string]string{"t": "hunter2", "m": "swordfish-made", "p": "plain"}; !reflect.DeepEqual(end.Outputs, want) {
		t.Errorf("the job's outputs are %v, want %v", end.Outputs, want)
	}
	if !slices.Equal(end.PrivateOutputs, []string{"m", "t"}) || !slices.Equal(end.SecretOutputs, []string{"m", "t"}) || !slices.Equal(end.DerivedOutputs, []string{"m"}) {
		t.Errorf("the job's outputs are private %v, secret %v and derived %v", end.PrivateOutputs, end.SecretOutputs, end.DerivedOutputs)
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
	f := newFakeCoordinator(t, newJob(command("head -c "+strconv.Itoa(size)+" /dev/zero | tr '\\0' a; echo"), command("echo two")))
	var requests counter
	f.fault = func(r *http.Request) fault {
		n := requests.next(r.URL.Path)
		switch r.URL.Path {
		case workerapi.PathLogs:
			return []fault{answer, dropAnswer, drop, unavailable, dropAnswer}[n%5]
		case workerapi.PathEvents, workerapi.PathComplete, workerapi.PathClaim:
			return []fault{answer, dropAnswer, unavailable}[n%3]
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	f := newFakeCoordinator(t, newJob(command("head -c "+strconv.Itoa(size)+" /dev/zero | tr '\\0' a; echo")))
	// The first upload is held until the step has printed everything, so
	// that the worker has more to send once it is stored, and the second
	// is lost.
	printed := make(chan struct{})
	var once sync.Once
	f.onEvent = func(e workerapi.Event) {
		if e.Finished {
			once.Do(func() { close(printed) })
		}
	}
	var requests counter
	f.fault = func(r *http.Request) fault {
		if r.URL.Path != workerapi.PathLogs || r.URL.Query().Get(workerapi.QueryPart) != string(workerapi.LogStep) {
			return answer
		}
		switch requests.next(r.URL.Path) {
		case 1:
			<-printed
		case 2:
			return forget
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	f := newFakeCoordinator(t, newJob(command("echo one; sleep 0.2; echo two"), command("echo three")))
	var down, up atomic.Int64
	f.onEvent = func(e workerapi.Event) {
		if !e.Finished && e.Step == 0 {
			now := time.Now()
			down.Store(now.UnixNano())
			up.Store(now.Add(3 * time.Second).UnixNano())
		}
	}
	f.fault = func(r *http.Request) fault {
		if now := time.Now().UnixNano(); now > down.Load() && now < up.Load() {
			return drop
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0", "started 1.0", "finished 1.0 success 0")
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "one\ntwo\n")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "three\n")
	if last := f.finishedEvent(1, 0, false).Time; !last.Before(time.Unix(0, up.Load())) {
		t.Errorf("the last step ended at %v, after the coordinator was back at %v: the worker waited for it", last, time.Unix(0, up.Load()))
	}
}

// TestSpoolLimit checks that a worker cut off from the coordinator keeps
// no more of its logs than its limit, and that the step's event warns of
// what it didn't keep.
func TestSpoolLimit(t *testing.T) {
	t.Parallel()
	const limit, printed = 4096, 10000
	f := newFakeCoordinator(t, newJob(command("head -c "+strconv.Itoa(printed)+" /dev/zero | tr '\\0' a; echo")))
	var finished atomic.Bool
	f.onEvent = func(e workerapi.Event) {
		if e.Finished {
			finished.Store(true)
		}
	}
	// Every log is held until the step has finished: what the coordinator
	// stores of one, the setup log's too, frees room in the spool, and
	// what the step printed after would be kept again.
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathLogs && !finished.Load() {
			return unavailable
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{SpoolLimit: limit}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	log, ended := f.log(logKey{workerapi.LogStep, 0, 0})
	if !ended || len(log) == 0 || len(log) > limit || strings.Trim(log, "a") != "" {
		t.Fatalf("the log kept is %d bytes (ended %t), want at most %d of a", len(log), ended, limit)
	}
	diags := f.finishedEvent(0, 0, false).Diagnostics
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
	f := newFakeCoordinator(t, newJob(command("touch $PW_JOB_DIR/ran")))
	var requests counter
	var early atomic.Bool
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathEvents && requests.next(r.URL.Path) <= 5 {
			if _, err := os.Stat(filepath.Join(f.claim.RunDir, "ran")); err == nil {
				early.Store(true)
			}
			return unavailable
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
		flt  fault
		want error
	}{
		{"heartbeat fenced", workerapi.PathHeartbeat, fence, errFenced},
		{"control unauthorized", workerapi.PathControl, unauthorized, errUnauthorized},
		{"logs fenced", workerapi.PathLogs, fence, errFenced},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			step := command("echo started; sleep 30")
			step.Cleanup = plan.Literal("touch $PW_JOB_DIR/cleaned")
			f := newFakeCoordinator(t, newJob(step))
			var started atomic.Bool
			f.onEvent = func(e workerapi.Event) { started.Store(true) }
			f.fault = func(r *http.Request) fault {
				if started.Load() && r.URL.Path == tc.path {
					return tc.flt
				}
				return answer
			}
			start := time.Now()
			if err := f.runWorker(t.Context(), Config{}); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if elapsed := time.Since(start); elapsed > 15*time.Second {
				t.Errorf("the worker took %v to stop", elapsed)
			}
			if f.end != nil {
				t.Error("a fenced worker completed its attempt")
			}
			if _, err := os.Stat(filepath.Join(f.claim.RunDir, "cleaned")); err == nil {
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
		{"run directory", func(c *workerapi.Claim) { c.RunDir = filepath.Join(file, "run") }, "run_dir_unusable", []string{"path", "reason"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFakeCoordinator(t, newJob(command("touch $PW_JOB_DIR/ran")))
			tc.change(&f.claim)
			if err := f.runWorker(t.Context(), Config{}); err != nil {
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
			if setup, ended := f.log(logKey{part: workerapi.LogSetup}); !ended || !strings.Contains(setup, "the job failed before its first step") {
				t.Errorf("the setup log is %q (ended %t)", setup, ended)
			}
			if _, err := os.Stat(filepath.Join(f.claim.RunDir, "ran")); err == nil {
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
	uses := plan.Step{Uses: "parallelworks/checkout", With: plan.Literal(map[string]any{"repo": "x", "branch": "main"})}
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
	f := newFakeCoordinator(t, newJob(steps...))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
		if got := codes(f.finishedEvent(step, 0, false).Diagnostics); !slices.Equal(got, []string{want}) {
			t.Errorf("step %d's diagnostics are %v, want %s", step, got, want)
		}
	}
}

// TestOutputsLineTooLong checks that, as in format 1, a $OUTPUTS file with
// a line of 64 KiB or longer is read as having no outputs, and that the
// step's event warns of it.
func TestOutputsLineTooLong(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("echo a=1 >> $OUTPUTS; head -c 70000 /dev/zero | tr '\\0' b >> $OUTPUTS; echo >> $OUTPUTS; echo c=3 >> $OUTPUTS")))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success 0")
	e := f.finishedEvent(0, 0, false)
	if len(e.Outputs) != 0 || !slices.Equal(codes(e.Diagnostics), []string{"outputs_line_too_long"}) {
		t.Errorf("the step's outputs are %v, and its diagnostics %v", e.Outputs, codes(e.Diagnostics))
	}
	if end := checkEnd(t, f, plan.OutcomeSuccess, ""); len(end.Outputs) != 0 {
		t.Errorf("the job's outputs are %v", end.Outputs)
	}
}

// TestPlanTooNew checks that a worker that doesn't read the run's plan
// version exits without running anything.
func TestPlanTooNew(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("true")))
	f.planTooNew = true
	if err := f.runWorker(t.Context(), Config{}); !errors.Is(err, errPlanTooNew) {
		t.Fatalf("got %v, want %v", err, errPlanTooNew)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Errorf("the worker made the requests %v", f.requests)
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
			f := newFakeCoordinator(t, newJob(command("echo over tls")))
			// HTTP/1.1, though the server offers HTTP/2, so that a
			// replica's 503 can send the next request to another.
			var protos sync.Map
			f.fault = func(r *http.Request) fault {
				protos.Store(r.Proto, true)
				return answer
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
			err := f.runWorker(t.Context(), Config{Environ: []string{
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
	f := newFakeCoordinator(t, newJob(fifo, link, through, device))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := f.runWorker(ctx, Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
		"started 3.0", "finished 3.0 failure 0",
	)
	for _, step := range []int{0, 3} {
		if got := codes(f.finishedEvent(step, 0, false).Diagnostics); !slices.Equal(got, []string{"outputs_not_file"}) {
			t.Errorf("step %d's diagnostics are %v", step, got)
		}
	}
	for step, want := range map[int]map[string]string{1: {"l": "1"}, 2: {"a": "2"}} {
		if got := f.finishedEvent(step, 0, false).Outputs; !reflect.DeepEqual(got, want) {
			t.Errorf("step %d's outputs are %v, want %v", step, got, want)
		}
	}
	if data, err := os.ReadFile(filepath.Join(f.claim.RunDir, "target")); err != nil || string(data) != "l=1\na=2\n" {
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
	f := newFakeCoordinator(t, newJob(command("head -c 1100000 /dev/zero | tr '\\0' a >> $OUTPUTS"), shared, own))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 failure 0",
		"started 1.0", "finished 1.0 success 0",
		"started 2.0", "finished 2.0 success 0",
	)
	if got := codes(f.finishedEvent(0, 0, false).Diagnostics); !slices.Equal(got, []string{"outputs_too_large"}) {
		t.Errorf("step 0's diagnostics are %v", got)
	}
	if e := f.finishedEvent(1, 0, false); len(e.Diagnostics) != 0 || len(e.Outputs) != 0 {
		t.Errorf("step 1 has the diagnostics %v and the outputs %v", codes(e.Diagnostics), e.Outputs)
	}
	if got := f.finishedEvent(2, 0, false).Outputs; !reflect.DeepEqual(got, map[string]string{"c": "3"}) {
		t.Errorf("step 2's outputs are %v", got)
	}
}

// TestDirRemoved checks that a step that removes the worker's directory,
// as rm -rf "$TMPDIR"/* does, doesn't keep the job from going on: the
// worker makes it again.
func TestDirRemoved(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command(`rm -rf "$(dirname "$OUTPUTS")"`), command("echo next; echo a=1 >> $OUTPUTS")))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	secret := func(f *fakeCoordinator) {
		f.claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hunter2"}, Secrets: []string{"vars.token"}}
	}
	t.Run("step", func(t *testing.T) {
		t.Parallel()
		step := command("true")
		step.WorkingDirectory = plan.Literal("/proc/hunter2/x")
		f := newFakeCoordinator(t, newJob(step))
		secret(f)
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		for _, d := range f.finishedEvent(0, 0, false).Diagnostics {
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
		f := newFakeCoordinator(t, newJob(command("true")))
		secret(f)
		f.claim.RunDir = filepath.Join(file, "hunter2")
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		end := checkEnd(t, f, plan.OutcomeFailure, plan.ReasonSetupFailed)
		if p := end.Problem; p == nil || strings.Contains(fmt.Sprint(p.Params, p.Detail), "hunter2") {
			t.Errorf("the attempt's problem quotes the secret: %+v", p)
		}
		if setup, _ := f.log(logKey{part: workerapi.LogSetup}); strings.Contains(setup, "hunter2") {
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
	f := newFakeCoordinator(t, j)
	f.claim.Retry = 2
	f.claim.Env = map[string]workerapi.EnvVar{
		"PW_USER": {Value: "alice"}, "PW_JOB_ID": {Value: "wf-00042"}, "PW_API_KEY": {Value: "k3y-s3cret", Secret: true},
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "alice wf-00042 *** "+f.claim.RunDir+" 2 3 7\n")
}

// TestEventsLost checks that a worker whose coordinator has lost events it
// said it had stored, which the worker no longer has, stops its job.
func TestEventsLost(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("true"), command("sleep 30")))
	var forgot atomic.Bool
	f.fault = func(r *http.Request) fault {
		if r.URL.Path != workerapi.PathEvents {
			return answer
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		// Forget them as the second step starts.
		if strings.Contains(string(body), `"step":1`) && !forgot.Swap(true) {
			return forget
		}
		return answer
	}
	start := time.Now()
	if err := f.runWorker(t.Context(), Config{}); err == nil || !strings.Contains(err.Error(), "lost") {
		t.Fatalf("got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("the worker took %v to stop", elapsed)
	}
}

// TestRedirectRefused checks that a worker follows no redirect: it talks
// only to the coordinator it was given.
func TestRedirectRefused(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("true")))
	var claims atomic.Int32
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathClaim {
			claims.Add(1)
			return redirect
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); err == nil {
		t.Fatal("claimed through a redirect")
	}
	if n := claims.Load(); n != 1 {
		t.Errorf("the worker made %d claims", n)
	}
}
