package coordinator

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/parallelworks/meridian/internal/testtiming"
)

// TestDefaultTiming checks DefaultTiming against what the worker API asks
// of it (workerapi.Claim): a worker whose job fences itself may start
// stopping it once six tenths of the lease have passed without a renewal,
// and has stopped it by nine tenths.
func TestDefaultTiming(t *testing.T) {
	tm := DefaultTiming()
	if tm.Heartbeat > tm.Lease/5 {
		t.Errorf("Heartbeat %v is more than a fifth of Lease %v", tm.Heartbeat, tm.Lease)
	}
	if stopFrom := tm.Lease * 6 / 10; 3*tm.Heartbeat > stopFrom {
		t.Errorf("a worker can't heartbeat three times, every %v, before it may stop its job at %v", tm.Heartbeat, stopFrom)
	}
	if stopped := tm.Lease * 9 / 10; tm.Lease+tm.FenceMargin-stopped < 15*time.Second {
		t.Errorf("a new attempt may start %v after the worker has stopped its job, at %v; want at least 15s", tm.Lease+tm.FenceMargin-stopped, stopped)
	}
	// A worker waits 30 seconds at most between tries.
	if tm.Grace < 2*30*time.Second {
		t.Errorf("Grace %v leaves a worker cut off fewer than two tries", tm.Grace)
	}
	if tm.WakeJump <= 0 || tm.WakeJump >= tm.Grace {
		t.Errorf("WakeJump %v isn't between 0 and Grace %v", tm.WakeJump, tm.Grace)
	}
}

// A test of the meridian command can shorten the timing the coordinators
// it opens run with, each period scaled, the lease and its heartbeat to
// whole seconds, a fifth at most; and a scale it can't, it is refused.
func TestTimingScaled(t *testing.T) {
	t.Cleanup(func() { testtiming.Scale = 0 })
	for _, s := range []float64{0.1, 0.15, 0.5, 1} {
		testtiming.Scale = s
		tm, err := openTiming()
		if err != nil {
			t.Fatal(err)
		}
		if tm.Lease%time.Second != 0 || tm.Heartbeat%time.Second != 0 || tm.Heartbeat < time.Second || 5*tm.Heartbeat > tm.Lease {
			t.Errorf("scaled by %v, the lease is %v, with a heartbeat every %v", s, tm.Lease, tm.Heartbeat)
		}
		if want := time.Duration(float64(DefaultTiming().Grace) * s); tm.Grace != want {
			t.Errorf("scaled by %v, Grace is %v, want %v", s, tm.Grace, want)
		}
		// The worker's own time to stop isn't scaled, since the worker
		// doesn't scale it.
		if tm.WorkerStop != DefaultTiming().WorkerStop {
			t.Errorf("scaled by %v, WorkerStop is %v", s, tm.WorkerStop)
		}
		// No period is left out, as one added to Timing would be by a
		// scaling that lists them.
		v := reflect.ValueOf(tm)
		for i := range v.NumField() {
			if v.Field(i).IsZero() {
				t.Errorf("scaled by %v, %s is zero", s, v.Type().Field(i).Name)
			}
		}
	}
	testtiming.Scale = 0.1
	c, err := Open(t.Context(), Options{Database: filepath.Join(privateDir(t), "meridian.db"), Keys: newTestKeys(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if c.timing.Lease != 6*time.Second || c.timing.Heartbeat != time.Second {
		t.Errorf("a coordinator opened with a scale of 0.1 has %+v", c.timing)
	}
	for _, s := range []float64{0.05, 2, -1} {
		testtiming.Scale = s
		if _, err := Open(t.Context(), Options{Database: filepath.Join(privateDir(t), "meridian.db"), Keys: newTestKeys(t)}); err == nil {
			t.Errorf("a coordinator opened with a scale of %v", s)
		}
	}
	testtiming.Scale = 0
	if tm, err := openTiming(); err != nil || tm != DefaultTiming() {
		t.Errorf("unscaled, the timing is %+v, %v", tm, err)
	}
}
