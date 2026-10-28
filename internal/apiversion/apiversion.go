// Package apiversion is a hook into the worker API's table of the version
// that added each member of its bodies (workerapi.CheckVersion): through
// it, a test of a side of the API makes a member one that a later version
// added than the one the table gives it, so that the test can check that
// the side sends and reads it only in an attempt of that version, before
// any version has such a member. workerapi reads it, so every binary that
// speaks the API links it; in a release it holds nothing, and its
// generation stays 0, which workerapi's cache of the table is keyed by.
//
// A release never sets one: no package outside this module can import
// this one, and no code of the module's but a _test.go file calls
// SetSince. A test that does doesn't run in parallel with others that send
// or read the API's bodies, which the member it sets would change.
package apiversion

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

// SetSince makes member, a type of package workerapi's and a JSON name, as
// Event.cleanup, one of version until restore is called.
func SetSince(member string, version int) (restore func()) {
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
// SetSince, and each restore, changes: 0 while none has set anything.
func Generation() int64 { return generation.Load() }
