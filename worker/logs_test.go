package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// newTestSpool returns a spool, with a limit, in a private directory of
// the test's.
func newTestSpool(t *testing.T, limit int64) *spool {
	t.Helper()
	d, err := makePrivateDir([]parentDir{{path: t.TempDir()}}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.removeAll)
	s := newSpool(d, limit)
	t.Cleanup(s.close)
	return s
}

// TestSpoolShrinks checks that a log's file holds what the coordinator
// hasn't stored, and little more, while its step goes on printing: once
// the bytes sent are many times those not, or as many as the spool's
// limit, the rest are copied to a file of their own, and once all are
// sent, the file is emptied.
func TestSpoolShrinks(t *testing.T) {
	t.Parallel()
	s := newTestSpool(t, 1<<30)
	l := s.open(logKey{workerapi.LogStep, 0, 0})
	size := func() int64 {
		t.Helper()
		info, err := l.file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		return info.Size()
	}
	write := func(text string) {
		t.Helper()
		if _, err := io.WriteString(l, text); err != nil {
			t.Fatal(err)
		}
	}
	stored := func(length int64) {
		if s.stored(l, length, false) {
			s.compact(l)
		}
	}
	write(strings.Repeat("a", 90<<10) + strings.Repeat("b", 10<<10))
	stored(10 << 10)
	if got := size(); got != 100<<10 {
		t.Fatalf("with a tenth sent, the file holds %d bytes, want all %d", got, 100<<10)
	}
	stored(90 << 10)
	if got := size(); got != 10<<10 {
		t.Fatalf("with 90 KiB sent, the file holds %d bytes, want the 10 KiB unsent", got)
	}
	write("c")
	u, _ := s.next()
	data := make([]byte, u.n)
	if _, err := u.file.ReadAt(data, u.at); err != nil {
		t.Fatal(err)
	}
	if want := strings.Repeat("b", 10<<10) + "c"; u.offset != 90<<10 || string(data) != want {
		t.Fatalf("the next upload is %d bytes from %d, %q…, want the unsent bytes", len(data), u.offset, data[:min(len(data), 8)])
	}
	stored(100<<10 + 1)
	if got := size(); got != 0 {
		t.Errorf("with every byte sent, the file holds %d bytes", got)
	}
}

// TestSpoolShrinksAtLimit checks that a log's file holds less than the
// spool's limit of what has been sent, even when what hasn't is more than
// a quarter of it.
func TestSpoolShrinksAtLimit(t *testing.T) {
	t.Parallel()
	const limit = 40 << 10
	s := newTestSpool(t, limit)
	l := s.open(logKey{workerapi.LogStep, 0, 0})
	for _, step := range []struct {
		write  int
		stored int64
		want   int64
	}{
		{40 << 10, 25 << 10, 40 << 10},
		{25 << 10, 40 << 10, 25 << 10},
	} {
		if _, err := io.WriteString(l, strings.Repeat("x", step.write)); err != nil {
			t.Fatal(err)
		}
		if s.stored(l, step.stored, false) {
			s.compact(l)
		}
		info, err := l.file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != step.want {
			t.Errorf("with %d bytes stored, the file holds %d, want %d", step.stored, info.Size(), step.want)
		}
	}
}

// TestSpoolLoseRace checks that a log whose file fails goes on in a new
// one while its step writes to it.
func TestSpoolLoseRace(t *testing.T) {
	t.Parallel()
	s := newTestSpool(t, 1<<30)
	l := s.open(logKey{workerapi.LogStep, 0, 0})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = io.WriteString(l, "line\n")
			}
		}
	}()
	for range 50 {
		s.mu.Lock()
		f := l.file
		s.mu.Unlock()
		s.lose(l, f)
	}
	close(stop)
	<-done
}

// TestLogPace checks that a step that prints a line at a time has its lines
// sent together, an upload at most each logPace, and that the end of its
// log is sent at once, without waiting for the pace.
func TestLogPace(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	s := newTestSpool(t, 1<<30)
	key := logKey{workerapi.LogStep, 0, 0}
	l := s.open(key)
	pacedSend(t, f, s, 5*time.Second)
	var want strings.Builder
	for i := range 20 {
		line := fmt.Sprintf("line %d\n", i)
		want.WriteString(line)
		if _, err := io.WriteString(l, line); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // a step that prints a line at a time
	}
	closed := time.Now()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.wait(t.Context().Done()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(closed); took > 2*time.Second {
		t.Errorf("the log's end was sent %s after it ended, as if it waited for the pace", took)
	}
	if got, ended := logOf(f, key); got != want.String() || !ended {
		t.Errorf("the log stored is %q, ended %v; want %q", got, ended, want.String())
	}
	if n := f.Requests()[workerapi.PathLogs]; n > 3 {
		t.Errorf("20 lines printed in a pace took %d uploads", n)
	}
}

// TestLogPaceFullUploads checks that a log with more than an upload holds
// is sent upload after upload, without waiting for the pace between them.
func TestLogPaceFullUploads(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	s := newTestSpool(t, 1<<30)
	key := logKey{workerapi.LogStep, 0, 0}
	data := strings.Repeat("x", 2*workerapi.MaxLogUpload+10)
	if _, err := io.WriteString(s.open(key), data); err != nil {
		t.Fatal(err)
	}
	pacedSend(t, f, s, 5*time.Second)
	waitFor(t, "the coordinator to have the whole log", func() bool {
		got, _ := logOf(f, key)
		return len(got) == len(data)
	})
	if n := f.Requests()[workerapi.PathLogs]; n != 3 {
		t.Errorf("a log of two uploads and a bit took %d uploads", n)
	}
}

// TestLogPaceEndPending checks that the pace after an upload of one log
// doesn't hold back the end of another that has ended.
func TestLogPaceEndPending(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("true")))
	s := newTestSpool(t, 1<<30)
	going, done := logKey{workerapi.LogStep, 0, 0}, logKey{workerapi.LogStep, 0, 1}
	if _, err := io.WriteString(s.open(going), "going\n"); err != nil {
		t.Fatal(err)
	}
	l := s.open(done)
	if _, err := io.WriteString(l, "done\n"); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	pacedSend(t, f, s, 5*time.Second)
	waitFor(t, "the ended log's end", func() bool {
		got, ended := logOf(f, done)
		return got == "done\n" && ended
	})
}

// pacedSend sends s's logs to f, with uploads paced by pace, until the
// test ends.
func pacedSend(t *testing.T, f *workerapitest.Fake, s *spool, pace time.Duration) {
	t.Helper()
	tm := testTiming
	tm.logPace = pace
	c := claimedClientTimed(t, f, tm)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.send(ctx, c, func(err error) { t.Errorf("the worker stopped: %v", err) })
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// waitFor waits up to two seconds, well short of the 5 s pace the tests
// set, for cond, and fails the test if it doesn't hold by then.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("waited 2 s for %s, as if the worker waited for the pace", what)
		}
		time.Sleep(5 * time.Millisecond) //nolint:forbidigo // polling the fake, which has no way to wait for a log
	}
}

// TestCompactionLostLog checks that a log stays whole, at the right
// offsets, when its file is compacted as its step prints, and the
// coordinator, some way off, loses what it had stored.
func TestCompactionLostLog(t *testing.T) {
	t.Parallel()
	pad := strings.Repeat("x", 200)
	f := workerapitest.NewFake(t, newJob(command(`for i in $(seq 1 400); do echo "line $i `+pad+`"; sleep 0.003; done`)))
	var uploads atomic.Int32
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path == workerapi.PathLogs && r.URL.Query().Get(workerapi.QueryPart) == string(workerapi.LogStep) {
			time.Sleep(10 * time.Millisecond) //nolint:forbidigo // a coordinator some way off, so that the step prints faster than it stores
			if uploads.Add(1) == 30 {
				return workerapitest.Forget
			}
		}
		return workerapitest.Answer
	}
	err := runWorker(t, f, t.Context(), Config{})
	if err != nil {
		// Lost bytes from before the file was compacted can't be sent
		// again: the attempt ends, rather than send a log with a hole.
		if p := problemOf(err, problems.CoordinatorLostData); p == nil || p.Params["data"] != "log" {
			t.Fatal(err)
		}
		return
	}
	got, _ := logOf(f, logKey{workerapi.LogStep, 0, 0})
	for i, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
		if want := fmt.Sprintf("line %d %s", i+1, pad); line != want {
			t.Fatalf("line %d of the log is %.20q…", i+1, line)
		}
	}
	if n := strings.Count(got, "\n"); n != 400 {
		t.Errorf("the log has %d lines, want 400", n)
	}
}
