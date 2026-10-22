package local

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/meridian/coordinator"
)

// progressBuffer is how many events Progress holds while it writes others.
const progressBuffer = 1024

// Progress is the command's progress display: it writes a line for each
// event of a run as it happens, such as
//
//	14:02:07 build#2 attempt 1 running on local/host
//
// Event never blocks, since it holds up the job it reports on: Progress
// writes from a goroutine of its own, and when it falls more than a
// thousand events behind, as when its terminal is paused, it leaves events
// out and says how many. An event reported again, as a coordinator that
// took a job over can, is shown once.
type Progress struct {
	w      io.Writer
	events chan coordinator.Event
	done   chan struct{}

	mu      sync.Mutex
	closed  bool
	dropped int
}

var _ coordinator.Events = (*Progress)(nil)

// NewProgress returns a Progress that writes to w, until Close.
func NewProgress(w io.Writer) *Progress {
	p := &Progress{w: w, events: make(chan coordinator.Event, progressBuffer), done: make(chan struct{})}
	go p.write()
	return p
}

// Event shows e, once those before it are shown.
func (p *Progress) Event(_ context.Context, e coordinator.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	select {
	case p.events <- e:
	default:
		p.dropped++
	}
}

// Close shows the events Progress holds, and stops it. Events after it
// aren't shown.
func (p *Progress) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.events)
	}
	p.mu.Unlock()
	<-p.done
}

// eventKey is what tells one event from another: its fields but its time
// and what it says of how things ended.
type eventKey struct {
	kind          coordinator.EventKind
	run, job      string
	attempt, step int
	try           int
	cleanup       bool
}

func (p *Progress) write() {
	defer close(p.done)
	seen := map[eventKey]bool{}
	shown := 0
	for e := range p.events {
		k := eventKey{e.Kind, e.Run, e.Job, e.Attempt, e.Step, e.Try, e.Cleanup}
		if seen[k] {
			continue
		}
		seen[k] = true
		p.mu.Lock()
		dropped := p.dropped - shown
		shown = p.dropped
		p.mu.Unlock()
		if dropped > 0 {
			fmt.Fprintf(p.w, "(%d events not shown)\n", dropped)
		}
		fmt.Fprintln(p.w, line(e))
	}
	p.mu.Lock()
	dropped := p.dropped - shown
	p.mu.Unlock()
	if dropped > 0 {
		fmt.Fprintf(p.w, "(%d events not shown)\n", dropped)
	}
}

// line is the line Progress shows for e.
func line(e coordinator.Event) string {
	var b strings.Builder
	if !e.Time.IsZero() {
		b.WriteString(e.Time.In(time.Local).Format(time.TimeOnly))
		b.WriteByte(' ')
	}
	switch e.Kind {
	case coordinator.RunStarted:
		fmt.Fprintf(&b, "run %s started", e.Run)
	case coordinator.RunFinished:
		fmt.Fprintf(&b, "run %s finished: %s", e.Run, e.Outcome)
	case coordinator.JobReady:
		fmt.Fprintf(&b, "%s ready", e.Job)
	case coordinator.JobPlacing:
		fmt.Fprintf(&b, "%s attempt %d placing on %s", e.Job, e.Attempt, e.Member)
	case coordinator.JobRunning:
		fmt.Fprintf(&b, "%s attempt %d running", e.Job, e.Attempt)
		if e.Member != "" {
			fmt.Fprintf(&b, " on %s", e.Member)
		}
	case coordinator.JobFinished:
		fmt.Fprintf(&b, "%s finished: %s", e.Job, e.Outcome)
	case coordinator.StepStarted, coordinator.StepFinished:
		fmt.Fprintf(&b, "%s step %d", e.Job, e.Step)
		if e.Cleanup {
			b.WriteString(" cleanup")
		}
		if e.Try > 0 {
			fmt.Fprintf(&b, " retry %d", e.Try)
		}
		if e.Kind == coordinator.StepStarted {
			b.WriteString(" started")
			break
		}
		fmt.Fprintf(&b, " finished: %s", e.Outcome)
		if e.ExitCode >= 0 {
			fmt.Fprintf(&b, " (exit status %d)", e.ExitCode)
		}
	default:
		fmt.Fprintf(&b, "%s %s", e.Kind, e.Job)
	}
	if e.Reason != "" {
		fmt.Fprintf(&b, " (%s)", e.Reason)
	}
	return b.String()
}
