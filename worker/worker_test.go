package worker

import (
	"context"
	"errors"
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
	j.Outputs = map[string]plan.Value{"b": plan.Literal("steps.s.b")}
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
	ignore.If, ignore.IgnoreErrors = plan.Literal("failure()"), plan.Literal(true)
	always := command("echo always")
	always.If = plan.Literal("always()")
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
	succeeds.Retry = &plan.StepRetry{MaxRetries: number(3), Interval: number(0)}
	fails := command("exit 1")
	fails.If = plan.Literal("always()")
	fails.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: number(0)}
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
		slow.Retry = &plan.StepRetry{MaxRetries: number(2), Interval: number(0), Timeout: number(20)}
		after := command("echo after")
		after.If = plan.Literal("always()")
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
		slow.Retry = &plan.StepRetry{MaxRetries: number(1), Interval: number(0), Timeout: number(1)}
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
		after := command("echo after")
		after.If = plan.Literal("always()")
		j := newJob(command("sleep 30"), after)
		j.TimeoutSeconds = 1
		f := newFakeCoordinator(t, j)
		if err := f.runWorker(t.Context(), Config{}); err != nil {
			t.Fatal(err)
		}
		checkEvents(t, f, "started 0.0", "finished 0.0 failure -1", "finished 1.0 failure -1")
		checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
	})
}

// TestCancel checks that a job the coordinator cancels stops: its running
// step gets SIGTERM, and SIGKILL after the cancellation's grace period if
// it ignores that, its other steps don't run, its cleanup: commands do,
// and it ends cancelled.
func TestCancel(t *testing.T) {
	t.Parallel()
	stubborn := command("trap '' TERM; echo waiting; sleep 30")
	stubborn.Cleanup = plan.Literal("echo cleaned > $PW_JOB_DIR/cleaned")
	never := command("echo never")
	never.If = plan.Literal("always()")
	f := newFakeCoordinator(t, newJob(stubborn, never))
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
		"finished 1.0 cancelled -1",
		"cleanup started 0.0", "cleanup finished 0.0 success 0",
	)
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "waiting\n")
	if _, err := os.Stat(filepath.Join(f.claim.RunDir, "cleaned")); err != nil {
		t.Errorf("the cleanup: command didn't run: %v", err)
	}
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
// its events and log.
func TestCleanupOrder(t *testing.T) {
	t.Parallel()
	steps := []plan.Step{command("true"), command("true"), command("exit 1"), command("true")}
	steps[1].If = plan.Literal("false")
	for i := range steps {
		steps[i].Cleanup = plan.Literal("echo " + strconv.Itoa(i) + " >> $PW_JOB_DIR/order; echo cleaning " + strconv.Itoa(i))
	}
	steps[2].Cleanup = plan.Literal(literal[string](steps[2].Cleanup) + "; exit 5")
	f := newFakeCoordinator(t, newJob(steps...))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f,
		"started 0.0", "finished 0.0 success 0",
		"finished 1.0 skipped -1",
		"started 2.0", "finished 2.0 failure 1",
		"finished 3.0 skipped -1",
		"cleanup started 2.0", "cleanup finished 2.0 failure 5",
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
			step.Retry = &plan.StepRetry{MaxRetries: number(3), Interval: number(0)}
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
// and its steps made from them, are masked in logs and outputs before
// they leave the worker.
func TestSecretsMasked(t *testing.T) {
	t.Parallel()
	step := command(`echo "token=$TOKEN extra=$EXTRA matrix=matrix-secret"
echo "t=$TOKEN" >> $OUTPUTS; echo "e=$EXTRA" >> $OUTPUTS`)
	step.Env = map[string]plan.Value{"TOKEN": plan.Literal("hunter2"), "EXTRA": plan.Literal("secret:swordfish")}
	j := newJob(step)
	j.Masked = []string{"matrix-secret"}
	f := newFakeCoordinator(t, j)
	f.claim.Run = plan.RunValues{Format: 1, Vars: map[string]string{"token": "hunter2"}, Secrets: []string{"vars.token"}}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "token=*** extra=*** matrix=***\n")
	want := map[string]string{"t": "***", "e": "***"}
	if got := f.finishedEvent(0, 0, false).Outputs; !reflect.DeepEqual(got, want) {
		t.Errorf("the step's outputs are %v, want %v", got, want)
	}
	if end := checkEnd(t, f, plan.OutcomeSuccess, ""); !reflect.DeepEqual(end.Outputs, want) {
		t.Errorf("the job's outputs are %v, want %v", end.Outputs, want)
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
// coordinator says it has it to, when it has lost what it had stored.
func TestLostLog(t *testing.T) {
	t.Parallel()
	const size = 2*workerapi.MaxLogUpload + 1000
	f := newFakeCoordinator(t, newJob(command("head -c "+strconv.Itoa(size)+" /dev/zero | tr '\\0' a; echo")))
	var requests counter
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathLogs && r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) &&
			requests.next(r.URL.Path) == 2 {
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
	f.fault = func(r *http.Request) fault {
		if r.URL.Path == workerapi.PathLogs && r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) && !finished.Load() {
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

// TestSelfFencing checks that a worker whose job fences itself stops it
// once it hasn't renewed its lease for a lease period, and that one whose
// job doesn't keeps running it, cut off as long.
func TestSelfFencing(t *testing.T) {
	t.Parallel()
	for _, fences := range []bool{true, false} {
		t.Run("fences "+strconv.FormatBool(fences), func(t *testing.T) {
			t.Parallel()
			// Long enough that the job ends by itself only if it isn't
			// fenced: cut off three times its lease, or ten.
			script := "sleep 3"
			if fences {
				script = "sleep 10"
			}
			f := newFakeCoordinator(t, newJob(command(script)))
			f.claim.SelfFencing, f.claim.LeaseSeconds, f.claim.HeartbeatSeconds = fences, 1, 1
			var started atomic.Bool
			f.onEvent = func(e workerapi.Event) { started.Store(true) }
			f.fault = func(r *http.Request) fault {
				if started.Load() && r.URL.Path == workerapi.PathHeartbeat {
					return unavailable
				}
				return answer
			}
			start := time.Now()
			err := f.runWorker(t.Context(), Config{})
			if !fences {
				if err != nil {
					t.Fatal(err)
				}
				checkEnd(t, f, plan.OutcomeSuccess, "")
				return
			}
			if !errors.Is(err, errLeaseLapsed) {
				t.Fatalf("got %v, want %v", err, errLeaseLapsed)
			}
			if elapsed := time.Since(start); elapsed > 8*time.Second {
				t.Errorf("the worker took %v to stop a job whose lease is a second", elapsed)
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
		{"unreadable job", func(c *workerapi.Claim) {
			c.Job = []byte(`{"key":"build","steps":[],"path":"/jobs/build","isolation":"vm"}`)
		}, "job_unreadable", []string{"reason"}},
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
// shell that isn't there, and outputs too large to keep.
func TestStepsRefused(t *testing.T) {
	t.Parallel()
	uses := plan.Step{Uses: "parallelworks/checkout", With: plan.Literal(map[string]any{"repo": "x", "branch": "main"})}
	onHost := command("hostname")
	onHost.SSH = plan.Literal(map[string]any{"remoteHost": "elsewhere"})
	noShell := command("true")
	noShell.Shell = "/nonexistent/bash {0}"
	large := command("head -c 1100000 /dev/zero | tr '\\0' a > $OUTPUTS")
	steps := []plan.Step{uses, onHost, noShell, large}
	for i := range steps {
		steps[i].If = plan.Literal("always()")
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
	)
	checkEnd(t, f, plan.OutcomeFailure, "")
	for step, want := range []string{"uses_not_supported", "step_ssh_not_supported", "step_not_started", "outputs_too_large"} {
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

// TestStopped checks that a worker whose context ends stops its job's
// steps and sends what it has, without completing the attempt: the
// coordinator decides how it ended.
func TestStopped(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("echo started; sleep 30")))
	ctx, cancel := context.WithCancel(t.Context())
	f.onEvent = func(e workerapi.Event) { cancel() }
	if err := f.runWorker(ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if f.end != nil {
		t.Error("a stopped worker completed its attempt")
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
			srv := httptest.NewUnstartedServer(f)
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
				if !errors.Is(err, errKeyMismatch) {
					t.Fatalf("got %v, want %v", err, errKeyMismatch)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			checkEnd(t, f, plan.OutcomeSuccess, "")
			checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "over tls\n")
		})
	}
}
