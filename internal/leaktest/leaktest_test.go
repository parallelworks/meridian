package leaktest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// A recorder is a test that Check checks, whose cleanups run when the
// recorder's test says, and whose errors it records.
type recorder struct {
	testing.TB
	cleanups []func()
	errs     []string
}

func (r *recorder) Helper() {}

func (r *recorder) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }

func (r *recorder) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// end runs the recorder's cleanups, the last registered first.
func (r *recorder) end() {
	for _, f := range slices.Backward(r.cleanups) {
		f()
	}
}

// leaked is where the goroutine a test leaks waits, which the stack Check
// shows names.
func leaked(stop <-chan struct{}) { <-stop }

// TestCheck checks that Check fails a test that leaves a goroutine of its
// own running, with its stack, even when a goroutine that ran before the
// test ended during it, so that as many run as did; and passes one whose
// goroutines ended.
func TestCheck(t *testing.T) {
	defer func(w time.Duration) { wait = w }(wait)
	wait = 200 * time.Millisecond

	earlier, leak := make(chan struct{}), make(chan struct{})
	defer close(leak)
	go leaked(earlier)
	r := &recorder{TB: t}
	Check(r)
	close(earlier)
	go leaked(leak)
	r.end()
	if len(r.errs) != 1 || !strings.Contains(r.errs[0], "leaktest.leaked") {
		t.Errorf("a leaked goroutine gave %q", r.errs)
	}

	ok := &recorder{TB: t}
	Check(ok)
	done := make(chan struct{})
	go func() { close(done) }()
	<-done
	ok.end()
	if len(ok.errs) != 0 {
		t.Errorf("goroutines that ended gave %q", ok.errs)
	}
}
