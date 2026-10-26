//go:build unix

package worker

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// TestCancelBetweenTries checks that a cancellation while a step waits to
// be tried again ends the step, and its job, cancelled, as in format 1.
func TestCancelBetweenTries(t *testing.T) {
	t.Parallel()
	step := command("exit 1")
	step.Retry = &plan.StepRetry{MaxRetries: number(3), Interval: number(30)}
	f := workerapitest.NewFake(t, newJob(step))
	f.OnEvent = func(e workerapi.Event) {
		if e.Finished {
			f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1})
		}
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 failure 1", "finished 0.1 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
}

// TestFenceShortensGrace checks that a job that fences itself stops
// before its lease lapses even while a cancellation gives its step a
// grace period longer than the lease: the worker sends SIGKILL sooner.
func TestFenceShortensGrace(t *testing.T) {
	t.Parallel()
	const lease = 2 * time.Second
	f := workerapitest.NewFake(t, newJob(command(`trap "" TERM; echo started; while :; do date +%s%N > "$PW_JOB_DIR/alive"; sleep 0.1; done`)))
	f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = true, int64(lease/time.Second), 1
	var cutOff atomic.Int64
	var once sync.Once
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(func() {
				f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 60})
				cutOff.Store(time.Now().Add(500 * time.Millisecond).UnixNano())
			})
		}
		if c := cutOff.Load(); c != 0 && time.Now().UnixNano() > c && r.URL.Path == workerapi.PathHeartbeat {
			return workerapitest.Unavailable
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, errLeaseLapsed) {
		t.Fatalf("got %v, want %v", err, errLeaseLapsed)
	}
	data, err := os.ReadFile(filepath.Join(f.Claim.RunDir, "alive"))
	if err != nil {
		t.Fatal(err)
	}
	last, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	// The last heartbeat that succeeded was sent before the cut-off, so
	// the lease lapses within a lease of it.
	lapse := time.Unix(0, cutOff.Load()).Add(lease)
	if over := time.Unix(0, last).Sub(lapse); over > 0 {
		t.Errorf("the step ran %v past its lease", over)
	}
}

// TestFenceStopsEveryGroup checks that a job that fences itself stops what
// its earlier steps left running in the background too.
func TestFenceStopsEveryGroup(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(
		command(`nohup sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PW_JOB_DIR/bg"`),
		command("sleep 30"),
	))
	f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = true, 3, 1
	var started atomic.Bool
	f.OnEvent = func(e workerapi.Event) {
		if !e.Finished && e.Step == 1 {
			started.Store(true)
		}
	}
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if started.Load() && r.URL.Path == workerapi.PathHeartbeat {
			return workerapitest.Unavailable
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, errLeaseLapsed) {
		t.Fatalf("got %v, want %v", err, errLeaseLapsed)
	}
	data, err := os.ReadFile(filepath.Join(f.Claim.RunDir, "bg"))
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	if pid <= 0 {
		t.Fatalf("no background process: %q", data)
	}
	if err := syscall.Kill(pid, 0); err == nil {
		t.Errorf("step 0's background process %d still runs after the worker fenced itself", pid)
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// TestSelfFencing checks that a worker whose job fences itself stops it
// once heartbeats have failed for most of its lease, before the lease
// lapses, and that one whose job doesn't keeps running it, cut off as
// long.
func TestSelfFencing(t *testing.T) {
	t.Parallel()
	for _, fences := range []bool{true, false} {
		t.Run("fences "+strconv.FormatBool(fences), func(t *testing.T) {
			t.Parallel()
			// Long enough that the job ends by itself only if it isn't
			// fenced.
			script := "sleep 4"
			if fences {
				script = "sleep 30"
			}
			f := workerapitest.NewFake(t, newJob(command(script)))
			f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = fences, 3, 1
			var cutOff atomic.Int64
			f.OnEvent = func(e workerapi.Event) {
				if cutOff.Load() == 0 {
					cutOff.Store(time.Now().UnixNano())
				}
			}
			f.Fault = func(r *http.Request) workerapitest.Fault {
				if cutOff.Load() != 0 && r.URL.Path == workerapi.PathHeartbeat {
					return workerapitest.Unavailable
				}
				return workerapitest.Answer
			}
			err := runWorker(t, f, t.Context(), Config{})
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
			if elapsed := time.Since(time.Unix(0, cutOff.Load())); elapsed > 3*time.Second {
				t.Errorf("the worker stopped %v after its heartbeats began to fail, past its lease of 3s", elapsed)
			}
		})
	}
}

// stopAt returns a Config whose worker is stopped, as stop does, as the
// step at index step, or its cleanup, if cleanup is set, has ended, and
// goes on once the stop has halted it.
func stopAt(step int, cleanup bool, stop func(a *attempt)) Config {
	return Config{stepEnded: func(a *attempt, i int, c bool) {
		if i == step && c == cleanup {
			stop(a)
			<-a.halted.c
		}
	}}
}

// TestStoppedAfterJob checks that a worker whose context ends once its
// job's steps, and their cleanups, have ended, as at a scheduler's time
// limit, still evaluates the job's outputs and sends the attempt's end:
// here the instant after the last ended, before the worker looks at what
// stopped it.
func TestStoppedAfterJob(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		job     *plan.ReadyJob
		cleanup bool
		events  []string
	}{
		{
			"after the last step", newJob(command("echo done=1 >> $OUTPUTS")), false,
			[]string{"started 0.0", "finished 0.0 success 0"},
		},
		{
			"after the last cleanup", newJob(withCleanup(command("echo done=1 >> $OUTPUTS"), "true")), true,
			[]string{"started 0.0", "finished 0.0 success 0", "cleanup started 0.0", "cleanup finished 0.0 success 0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, tc.job)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := runWorker(t, f, ctx, stopAt(0, tc.cleanup, func(*attempt) { cancel() })); err != nil {
				t.Fatal(err)
			}
			checkEvents(t, f, tc.events...)
			if end := checkEnd(t, f, plan.OutcomeSuccess, ""); end.Result.Outputs["done"] != "1" {
				t.Errorf("the job's outputs are %v", end.Result.Outputs)
			}
		})
	}
}

// TestStoppedBeforeJobEnded checks that a worker whose context ends after
// a step, but before the job's next step or a cleanup has run, sends no
// end, and runs nothing more: the job hasn't ended, and the coordinator
// decides how the attempt did.
func TestStoppedBeforeJobEnded(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		job    *plan.ReadyJob
		events []string
	}{
		{"before the next step", newJob(command("true"), command("true")), []string{"started 0.0", "finished 0.0 success 0"}},
		{"before a cleanup", newJob(withCleanup(command("true"), "true")), []string{"started 0.0", "finished 0.0 success 0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, tc.job)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := runWorker(t, f, ctx, stopAt(0, false, func(*attempt) { cancel() })); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
			checkEvents(t, f, tc.events...)
			if f.End() != nil {
				t.Error("a worker stopped before its job ended completed its attempt")
			}
		})
	}
}

// TestFencedAfterJob checks that an attempt that ends without the worker
// once its job's steps have ended, as when another attempt supersedes it,
// sends no end, as before.
func TestFencedAfterJob(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	err := runWorker(t, f, t.Context(), stopAt(0, false, func(a *attempt) { a.halt(errFenced) }))
	if !errors.Is(err, errFenced) {
		t.Fatalf("got %v", err)
	}
	if f.End() != nil {
		t.Error("a superseded attempt completed")
	}
}

// withCleanup returns s with the cleanup: command cleanup.
func withCleanup(s plan.Step, cleanup string) plan.Step {
	s.Cleanup = plan.Literal(cleanup)
	return s
}

// TestStopped checks that a worker whose context ends while its job runs
// stops its steps and sends what it has, without completing the attempt:
// the coordinator decides how it ended.
func TestStopped(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo started; sleep 30")))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.OnEvent = func(e workerapi.Event) { cancel() }
	if err := runWorker(t, f, ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if f.End() != nil {
		t.Error("a stopped worker completed its attempt")
	}
}

// TestControlBacksOff checks that a worker whose coordinator answers its
// control poll at once, with nothing new, doesn't ask it again at once.
func TestControlBacksOff(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("sleep 1")))
	f.ControlAtOnce = true
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	if n := f.Requests()[workerapi.PathControl]; n > 100 {
		t.Errorf("the worker polled %d times in a second", n)
	}
}

// TestHeartbeatStalls checks that one heartbeat that the coordinator never
// answers, as a connection that is half open would hold it, doesn't stop a
// job that fences itself: the worker gives up on it in time to send
// another. It also heartbeats more often than the coordinator asks, when
// that wouldn't leave it time to try.
func TestHeartbeatStalls(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("sleep 4")))
	f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = true, 5, 2
	var n atomic.Int32
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathHeartbeat && n.Add(1) == 1 {
			return workerapitest.Hang
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatalf("one stalled heartbeat stopped the job: %v", err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
}

// TestFenceKeepsSending checks that a job that fences itself goes on
// sending what it has until its lease lapses: its log reaches a
// coordinator that comes back between the job stopping and the lease
// lapsing.
func TestFenceKeepsSending(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo hello; sleep 30")))
	f.Claim.SelfFencing, f.Claim.LeaseSeconds, f.Claim.HeartbeatSeconds = true, 3, 1
	var cutOff atomic.Int64
	f.OnEvent = func(e workerapi.Event) { cutOff.CompareAndSwap(0, time.Now().UnixNano()) }
	f.Fault = func(r *http.Request) workerapitest.Fault {
		c := cutOff.Load()
		if c == 0 {
			return workerapitest.Answer
		}
		since := time.Since(time.Unix(0, c))
		switch {
		case r.URL.Path == workerapi.PathHeartbeat:
			return workerapitest.Unavailable
		case r.URL.Path == workerapi.PathLogs && since < 2200*time.Millisecond:
			// Back after the job has stopped, at 1.8 s, and before the
			// lease lapses, at 3 s.
			return workerapitest.Unavailable
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, t.Context(), Config{}); !errors.Is(err, errLeaseLapsed) {
		t.Fatalf("got %v, want %v", err, errLeaseLapsed)
	}
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "hello\n")
}

// TestStoppedKeepsLastWords checks that a worker stopped from outside
// sends what a step prints in its grace period: the flush period starts
// once the steps have stopped.
func TestStoppedKeepsLastWords(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command(`trap 'sleep 1.5; echo last words; exit 0' TERM; echo started; while :; do sleep 0.1; done`)))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var once sync.Once
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(cancel)
		}
		return workerapitest.Answer
	}
	if err := runWorker(t, f, ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if log, ended := logOf(f, logKey{workerapi.LogStep, 0, 0}); !ended || !strings.HasPrefix(log, "started\n") || !strings.HasSuffix(log, "last words\n") {
		t.Errorf("the log is %q (ended %t), without what the step printed last", log, ended)
	}
}
