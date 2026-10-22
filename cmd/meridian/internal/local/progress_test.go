package local

import (
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/plan"
)

// syncBuffer is a strings.Builder that is safe to write from one goroutine
// and read from another.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestProgress(t *testing.T) {
	var out syncBuffer
	p := NewProgress(&out)
	at := time.Date(2026, 10, 22, 14, 2, 7, 0, time.Local)
	for _, e := range []coordinator.Event{
		{Time: at, Kind: coordinator.RunStarted, Run: "r1"},
		{Time: at, Kind: coordinator.JobReady, Run: "r1", Job: "build#2"},
		{Time: at, Kind: coordinator.JobPlacing, Run: "r1", Job: "build#2", Attempt: 1, Member: "local/host"},
		{Time: at, Kind: coordinator.JobRunning, Run: "r1", Job: "build#2", Attempt: 1, Member: "local/host"},
		// Reported again by a coordinator that took the job over.
		{Time: at, Kind: coordinator.JobRunning, Run: "r1", Job: "build#2", Attempt: 1, Member: "local/host"},
		{Time: at, Kind: coordinator.StepStarted, Run: "r1", Job: "build#2", Attempt: 1, Step: 0},
		{Time: at, Kind: coordinator.StepFinished, Run: "r1", Job: "build#2", Attempt: 1, Step: 0, Outcome: plan.OutcomeFailure, ExitCode: 3},
		{Time: at, Kind: coordinator.StepStarted, Run: "r1", Job: "build#2", Attempt: 1, Step: 0, Try: 1},
		{Time: at, Kind: coordinator.StepFinished, Run: "r1", Job: "build#2", Attempt: 1, Step: 0, Try: 1, Outcome: plan.OutcomeSuccess},
		{Time: at, Kind: coordinator.StepFinished, Run: "r1", Job: "build#2", Attempt: 1, Step: 1, Cleanup: true, Outcome: plan.OutcomeCancelled, ExitCode: -1, Reason: plan.ReasonTimeout},
		{Time: at, Kind: coordinator.JobFinished, Run: "r1", Job: "build#2", Attempt: 1, Outcome: plan.OutcomeSuccess},
		{Time: at, Kind: coordinator.RunFinished, Run: "r1", Outcome: plan.OutcomeSuccess},
	} {
		p.Event(t.Context(), e)
	}
	p.Close()
	p.Event(t.Context(), coordinator.Event{Kind: coordinator.RunStarted, Run: "after"})
	want := `14:02:07 run r1 started
14:02:07 build#2 ready
14:02:07 build#2 attempt 1 placing on local/host
14:02:07 build#2 attempt 1 running on local/host
14:02:07 build#2 step 0 started
14:02:07 build#2 step 0 finished: failure (exit status 3)
14:02:07 build#2 step 0 retry 1 started
14:02:07 build#2 step 0 retry 1 finished: success (exit status 0)
14:02:07 build#2 step 1 cleanup finished: cancelled (timeout)
14:02:07 build#2 finished: success
14:02:07 run r1 finished: success
`
	if got := out.String(); got != want {
		t.Errorf("Progress wrote\n%s\nwant\n%s", got, want)
	}
}

// blockedWriter blocks every write until it is released, as a paused
// terminal does.
type blockedWriter struct {
	release chan struct{}
	out     syncBuffer
}

func (w *blockedWriter) Write(p []byte) (int, error) {
	<-w.release
	return w.out.Write(p)
}

// TestProgressNeverBlocks checks that Event returns while the display's
// writer is blocked, and that what it leaves out it counts.
func TestProgressNeverBlocks(t *testing.T) {
	w := &blockedWriter{release: make(chan struct{})}
	p := NewProgress(w)
	const n = progressBuffer * 3
	done := make(chan struct{})
	go func() {
		for i := range n {
			p.Event(t.Context(), coordinator.Event{Kind: coordinator.StepStarted, Run: "r1", Job: "j", Attempt: 1, Step: i})
		}
		close(done)
	}()
	<-done
	close(w.release)
	p.Close()
	got := w.out.String()
	lines := strings.Count(got, "\n")
	if !strings.Contains(got, "events not shown)") {
		t.Errorf("Progress didn't say it left events out")
	}
	if lines < progressBuffer || lines >= n {
		t.Errorf("Progress wrote %d lines for %d events", lines, n)
	}
}

func TestProgressCloseTwice(t *testing.T) {
	p := NewProgress(io.Discard)
	p.Close()
	p.Close()
}
