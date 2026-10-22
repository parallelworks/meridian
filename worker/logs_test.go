package worker

import (
	"io"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/workerapi"
)

// newTestSpool returns a spool, with a limit, in a private directory of
// the test's.
func newTestSpool(t *testing.T, limit int64) *spool {
	t.Helper()
	d, err := makePrivateDir(t.TempDir(), false)
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
