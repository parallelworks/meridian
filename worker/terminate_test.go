//go:build unix

package worker

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer that a process's output is copied into
// while a test reads it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
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

// TestTerminateOnce checks that a step's process group is sent SIGTERM
// once, though it is stopped twice, as a halt stops it: as its step's stop
// (runProcess) and as the reap of every group the attempt started (reap).
// A second SIGTERM would run the step's trap again, and stop what the
// first runs, as a sleep in it, cutting short the grace period the step
// has from the first, as format 1 gives it one.
func TestTerminateOnce(t *testing.T) {
	t.Parallel()
	a := &attempt{
		groups: map[int]*exec.Cmd{}, termed: map[int]bool{},
		reaped: make(chan struct{}), stopChanged: make(chan struct{}), haltKill: time.Now().Add(time.Minute),
	}
	done := filepath.Join(t.TempDir(), "done")
	cmd := exec.Command("/bin/bash", "-c", `n=0; trap 'n=$((n+1)); echo "term $n"' TERM; echo started; while [ ! -e "$DONE" ]; do sleep 0.02; done; echo "done $n"`)
	cmd.Env = append(os.Environ(), "DONE="+done)
	var out syncBuffer
	stop := make(chan stopRequest, 1)
	ended := make(chan error, 1)
	go func() {
		_, _, err := runProcess(cmd, &out, stop, a.startedGroup, a.terminate)
		ended <- err
	}()
	waitFor(t, "the step to start", func() bool { return strings.Contains(out.String(), "started\n") })
	stop <- stopRequest{cause: stopCancelled, kill: time.Now().Add(time.Minute), afterKill: time.Second}
	waitFor(t, "the step's trap", func() bool { return strings.Contains(out.String(), "term 1\n") })
	// The reap of the attempt's groups, as a halt makes it, which ends
	// once the step's group has.
	go a.reap()
	time.Sleep(200 * time.Millisecond) //nolint:forbidigo // the time a second SIGTERM would take to run the trap again
	if err := os.WriteFile(done, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-ended; err != nil {
		t.Fatal(err)
	}
	<-a.reaped
	// The leader, which runProcess leaves unreaped, as its step does
	// (endedGroup), lest it be a zombie another test finds.
	_ = cmd.Wait()
	// Bash says Terminated of the sleep the SIGTERM ended, too.
	if got := out.String(); strings.Count(got, "term ") != 1 || !strings.HasSuffix(got, "term 1\ndone 1\n") {
		t.Errorf("the step printed %q, as one sent SIGTERM more than once", got)
	}
}
