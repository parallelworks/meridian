package coordinator

import (
	"testing"
	"time"
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
