package worker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// deadlineIn returns the deadline d from now, in whole seconds, rounded
// up, and the environment of a worker that has it.
func deadlineIn(d time.Duration) (time.Time, string) {
	at := time.Unix(time.Now().Add(d+time.Second-1).Unix(), 0)
	return at, workerapi.EnvDeadline + "=" + strconv.FormatInt(at.Unix(), 10)
}

// bounded returns a context that ends after 30 s, so that a worker that
// doesn't stop when it should fails its test then, rather than hang it.
func bounded(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestDeadline checks that a worker stops its job at the job's own
// timeout or at its deadline, whichever comes first, as meridian batch
// gives it, half the member's margin before its allocation ends: its
// step fails, the steps that outlive it run, and so do its cleanups, its
// logs are delivered, and the job fails with the reason timeout.
func TestDeadline(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		timeout  int64
		deadline time.Duration
	}{
		// A worker that started at once: its deadline is half a 300 s
		// margin after the job's timeout, which comes first.
		{"a worker that started at once", 1, time.Second + 150*time.Second},
		// A worker that started late, as after the coordinator was away:
		// its deadline comes before the job's timeout.
		{"a worker that started late", 60, 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			slow := command("echo waiting; sleep 300")
			slow.Cleanup = plan.Literal("echo cleaned > $PW_JOB_DIR/cleaned")
			after := command("echo after")
			after.If = cond("always()")
			j := newJob(slow, after, command("echo skipped"))
			j.TimeoutSeconds = tc.timeout
			f := workerapitest.NewFake(t, j)
			_, deadline := deadlineIn(tc.deadline)
			if err := runWorker(t, f, bounded(t), Config{Environ: []string{deadline}}); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f,
				"started 0.0", "finished 0.0 failure -1",
				"started 1.0", "finished 1.0 success 0",
				"finished 2.0 skipped -1",
				"cleanup started 0.0", "cleanup finished 0.0 success 0",
			)
			checkEnd(t, f, plan.OutcomeFailure, plan.ReasonTimeout)
			checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "waiting\n")
			checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "after\n")
			if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "cleaned")); err != nil {
				t.Errorf("the cleanup: command didn't run: %v", err)
			}
		})
	}
}

// TestDeadlineBeforeClaim checks that a worker that can't claim its
// attempt by its deadline, as one that can't reach the coordinator, or
// whose deadline has passed as it starts, doesn't, and says so: the job
// would have no time to run.
func TestDeadlineBeforeClaim(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		deadline time.Duration
	}{
		{"unreachable until then", time.Second},
		{"passed", -time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, newJob(command("true")))
			// No coordinator serves this socket yet.
			socket := filepath.Join(t.TempDir(), "none.sock")
			at, deadline := deadlineIn(tc.deadline)
			err := runWorker(t, f, bounded(t), Config{Environ: []string{workerapi.EnvCoordinator + "=" + socket, deadline}})
			p := problemOf(err, problems.WorkerDeadlinePassed)
			if p == nil {
				t.Fatalf("got %v, want worker_deadline_passed", err)
			}
			if want := at.UTC().Format(time.RFC3339); p.Params["deadline"] != want {
				t.Errorf("the problem's deadline is %v, want %s", p.Params["deadline"], want)
			}
			if f.Claimed() {
				t.Error("the worker claimed the attempt")
			}
		})
	}
}

// TestDeadlineBeforeFirstStep checks that a worker whose deadline passes
// once it has claimed its attempt, but before its job's first step,
// starts none of the job, its steps that outlive a timeout and its
// cleanups among them: the attempt fails before its first step, which
// places the job again. Unless the job's own timeout came first, or a
// cancel, which stop the job as they do at any time.
func TestDeadlineBeforeFirstStep(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		timeout int64
		cancel  bool
		// end is how the attempt ends: as a failure before its first
		// step, or with the outcome and the reason its steps give it.
		end     plan.Outcome
		reason  plan.Reason
		events  []string
		cleaned bool
	}{
		{name: "the deadline first", end: plan.OutcomeFailure, reason: plan.ReasonSetupFailed},
		{
			// Its first step may start before the timeout, which passed
			// as the job was set up, stops it, or not.
			name: "the job's timeout first", timeout: 1, end: plan.OutcomeFailure, reason: plan.ReasonTimeout, cleaned: true,
			events: []string{"finished 0.0 failure -1", "started 1.0", "finished 1.0 success 0", "cleanup started 0.0", "cleanup finished 0.0 success 0"},
		},
		{
			name: "a cancel first", cancel: true, end: plan.OutcomeCancelled, reason: plan.ReasonCancelled, cleaned: true,
			events: []string{"finished 0.0 cancelled -1", "started 1.0", "finished 1.0 success 0", "cleanup started 0.0", "cleanup finished 0.0 success 0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := command("echo first")
			first.Cleanup = plan.Literal("echo cleaned > $PW_JOB_DIR/cleaned")
			after := command("echo after")
			after.If = cond("always()")
			j := newJob(first, after)
			j.TimeoutSeconds = tc.timeout
			f := workerapitest.NewFake(t, j)
			if tc.cancel {
				f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
			}
			at, deadline := deadlineIn(3 * time.Second)
			// The job's setup takes until past the deadline.
			slow := func(*attempt) { <-time.After(time.Until(at) + 200*time.Millisecond) }
			if err := runWorker(t, f, bounded(t), Config{Environ: []string{deadline}, setUpEnded: slow}); err != nil {
				t.Fatal(err)
			}
			end := f.End()
			if end == nil || end.Result.Outcome != tc.end || end.Reason != tc.reason {
				t.Fatalf("the attempt ended %+v", end)
			}
			if tc.reason == plan.ReasonSetupFailed {
				// A workflow's problem, as every one a worker's end gives is,
				// by its type's URI too.
				p := end.Problem
				if p == nil || p.Code != problems.WorkerDeadlinePassed.Code() || p.Type != problems.WorkerDeadlinePassed.Type().URI() || strings.Contains(p.Type, "/command/") {
					t.Fatalf("the attempt's problem is %+v", p)
				}
				if _, ok := problems.Registry.Lookup(p.Code); !ok {
					t.Errorf("%s isn't a workflow's problem", p.Code)
				}
			}
			got := stepEvents(t, f)
			if tc.timeout > 0 {
				got = slices.DeleteFunc(got, func(e string) bool { return e == "started 0.0" })
			}
			if !slices.Equal(got, tc.events) {
				t.Errorf("the events are %q, want %q", got, tc.events)
			}
			if _, err := os.Stat(filepath.Join(f.Claim.RunDir, "cleaned")); (err == nil) != tc.cleaned {
				t.Errorf("the cleanup ran: %t, want %t", err == nil, tc.cleaned)
			}
		})
	}
}
