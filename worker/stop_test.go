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
)

// TestCancelBetweenTries checks that a cancellation while a step waits to
// be tried again ends the step, and its job, cancelled, as in format 1.
func TestCancelBetweenTries(t *testing.T) {
	t.Parallel()
	step := command("exit 1")
	step.Retry = &plan.StepRetry{MaxRetries: number(3), Interval: number(30)}
	f := newFakeCoordinator(t, newJob(step))
	f.onEvent = func(e workerapi.Event) {
		if e.Finished {
			f.setControlLocked(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 1})
		}
	}
	if err := f.runWorker(t.Context(), Config{}); err != nil {
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
	f := newFakeCoordinator(t, newJob(command(`trap "" TERM; echo started; while :; do date +%s%N > "$PW_JOB_DIR/alive"; sleep 0.1; done`)))
	f.claim.SelfFencing, f.claim.LeaseSeconds, f.claim.HeartbeatSeconds = true, int64(lease/time.Second), 1
	var cutOff atomic.Int64
	var once sync.Once
	f.fault = func(r *http.Request) fault {
		if r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			once.Do(func() {
				f.setControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled, GraceSeconds: 60})
				cutOff.Store(time.Now().Add(500 * time.Millisecond).UnixNano())
			})
		}
		if c := cutOff.Load(); c != 0 && time.Now().UnixNano() > c && r.URL.Path == workerapi.PathHeartbeat {
			return unavailable
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); !errors.Is(err, errLeaseLapsed) {
		t.Fatalf("got %v, want %v", err, errLeaseLapsed)
	}
	data, err := os.ReadFile(filepath.Join(f.claim.RunDir, "alive"))
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
	f := newFakeCoordinator(t, newJob(
		command(`nohup sleep 60 >/dev/null 2>&1 </dev/null & echo $! > "$PW_JOB_DIR/bg"`),
		command("sleep 30"),
	))
	f.claim.SelfFencing, f.claim.LeaseSeconds, f.claim.HeartbeatSeconds = true, 3, 1
	var started atomic.Bool
	f.onEvent = func(e workerapi.Event) {
		if !e.Finished && e.Step == 1 {
			started.Store(true)
		}
	}
	f.fault = func(r *http.Request) fault {
		if started.Load() && r.URL.Path == workerapi.PathHeartbeat {
			return unavailable
		}
		return answer
	}
	if err := f.runWorker(t.Context(), Config{}); !errors.Is(err, errLeaseLapsed) {
		t.Fatalf("got %v, want %v", err, errLeaseLapsed)
	}
	data, err := os.ReadFile(filepath.Join(f.claim.RunDir, "bg"))
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
			f := newFakeCoordinator(t, newJob(command(script)))
			f.claim.SelfFencing, f.claim.LeaseSeconds, f.claim.HeartbeatSeconds = fences, 3, 1
			var cutOff atomic.Int64
			f.onEvent = func(e workerapi.Event) {
				if cutOff.Load() == 0 {
					cutOff.Store(time.Now().UnixNano())
				}
			}
			f.fault = func(r *http.Request) fault {
				if cutOff.Load() != 0 && r.URL.Path == workerapi.PathHeartbeat {
					return unavailable
				}
				return answer
			}
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
			if elapsed := time.Since(time.Unix(0, cutOff.Load())); elapsed > 3*time.Second {
				t.Errorf("the worker stopped %v after its heartbeats began to fail, past its lease of 3s", elapsed)
			}
		})
	}
}

// TestStoppedAfterJob checks that a worker whose context ends once its
// job's steps have ended, as at a scheduler's time limit, still sends the
// attempt's end.
func TestStoppedAfterJob(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("echo done")))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.onEvent = func(e workerapi.Event) {
		if e.Finished {
			cancel()
		}
	}
	if err := f.runWorker(ctx, Config{}); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
}

// TestStopped checks that a worker whose context ends while its job runs
// stops its steps and sends what it has, without completing the attempt:
// the coordinator decides how it ended.
func TestStopped(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("echo started; sleep 30")))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.onEvent = func(e workerapi.Event) { cancel() }
	if err := f.runWorker(ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.end != nil {
		t.Error("a stopped worker completed its attempt")
	}
}

// TestControlBacksOff checks that a worker whose coordinator answers its
// control poll at once, with nothing new, doesn't ask it again at once.
func TestControlBacksOff(t *testing.T) {
	t.Parallel()
	f := newFakeCoordinator(t, newJob(command("sleep 1")))
	f.controlAtOnce = true
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := f.requests[workerapi.PathControl]; n > 100 {
		t.Errorf("the worker polled %d times in a second", n)
	}
}
