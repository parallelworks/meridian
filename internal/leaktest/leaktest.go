// Package leaktest checks that a test leaves no goroutine of its own
// running once it has ended.
package leaktest

import (
	"bytes"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// wait is how long Check waits for goroutines to end, which nothing
// signals: those of a connection that the test closed, which end once
// their peer has heard of it. The package's own test shortens it.
var wait = 10 * time.Second

// header is the line a goroutine's stack starts with in runtime.Stack's
// dump of every goroutine, which gives its ID.
var header = regexp.MustCompile(`(?m)^goroutine (\d+) \[`)

// Check fails the test if, once it has ended and every cleanup registered
// after Check has run, a goroutine runs that didn't when it was called,
// after waiting up to wait for those to end, and shows their stacks. It
// knows goroutines by their IDs, which the runtime never gives again, so a
// goroutine of an earlier test that ends meanwhile hides none. A test calls
// it first, so that its cleanup runs last, after those that close what the
// test opened. Tests that run in parallel with it start goroutines of
// their own, which it would take for the test's, so it is for tests that
// don't.
func Check(t testing.TB) {
	t.Helper()
	before := map[string]bool{}
	for id := range stacks() {
		before[id] = true
	}
	t.Cleanup(func() {
		deadline := time.Now().Add(wait)
		for {
			var started []string
			for id, stack := range stacks() {
				if !before[id] {
					started = append(started, stack)
				}
			}
			if len(started) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("%d goroutines the test started run on:\n\n%s", len(started), strings.Join(started, "\n\n"))
				return
			}
			time.Sleep(10 * time.Millisecond) //nolint:forbidigo // goroutines ending, which nothing signals
		}
	})
}

// stacks returns the stack of every goroutine, by its ID.
func stacks() map[string]string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]string{}
	for stack := range bytes.SplitSeq(buf, []byte("\n\n")) {
		if m := header.FindSubmatch(stack); m != nil {
			out[string(m[1])] = string(stack)
		}
	}
	return out
}
