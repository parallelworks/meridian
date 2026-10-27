// Package versiontest lets a test of a side of the worker API make a member
// of the API's bodies one that a later version added than the one the
// API's table gives it (workerapi.CheckVersion), so that the test can check
// that the side sends and reads it only in an attempt of that version,
// before any version has such a member.
//
// A release never sets one: no package outside this module can import
// this one, and no code of the module's but a _test.go file calls Set. A
// test that does doesn't run in parallel with others that send or read
// the API's bodies, which the member it sets would change.
package versiontest

import (
	"sync"
	"sync/atomic"
)

var (
	mu    sync.Mutex
	since map[string]int
	// generation counts the changes to since, so that what is cached of
	// the API's table is known to be stale: it is 0 while no test has set
	// a member.
	generation atomic.Int64
)

// Set makes member, a type of package workerapi's and a JSON name, as
// Event.cleanup, one of version until restore is called.
func Set(member string, version int) (restore func()) {
	mu.Lock()
	defer mu.Unlock()
	old, had := since[member]
	if since == nil {
		since = map[string]int{}
	}
	since[member] = version
	generation.Add(1)
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if had {
			since[member] = old
		} else {
			delete(since, member)
		}
		generation.Add(1)
	}
}

// Since returns the version a test made member, and whether it made it
// one.
func Since(member string) (version int, ok bool) {
	mu.Lock()
	defer mu.Unlock()
	version, ok = since[member]
	return version, ok
}

// Generation returns the generation of what tests have set, which each
// Set, and each restore, changes: 0 while none has set anything.
func Generation() int64 { return generation.Load() }
