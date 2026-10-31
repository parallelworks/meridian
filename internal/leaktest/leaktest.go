// Package leaktest checks that a test leaves no goroutine of its own
// running once it has ended.
package leaktest

import (
	"runtime"
	"testing"
	"time"
)

// wait is how long Check waits for goroutines to end, which nothing
// signals: those of a connection that the test closed, which end once
// their peer has heard of it.
const wait = 10 * time.Second

// Check fails the test if, once it has ended and every cleanup registered
// after Check has run, more goroutines run than ran when it was called,
// after waiting up to wait for them to end, and shows what they are. A
// test calls it first, so that its cleanup runs last, after those that
// close what the test opened. Tests that run in parallel with it make the
// count meaningless, so it is for tests that don't.
func Check(t testing.TB) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(wait)
		for runtime.NumGoroutine() > before {
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<20)
				t.Errorf("%d goroutines run, and %d ran before the test:\n%s", runtime.NumGoroutine(), before, buf[:runtime.Stack(buf, true)])
				return
			}
			time.Sleep(10 * time.Millisecond) //nolint:forbidigo // goroutines ending, which nothing signals
		}
	})
}
