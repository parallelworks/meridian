package workerapisrv

import (
	"net"
	"net/http"
	"testing"
	"time"
)

// Past the limit, a new connection closes the one that has waited longest
// for a request, but only once it has waited grace, so that one that has
// just connected, or has just been answered, has time to send its request;
// while every connection is handling a request, a new one waits for one to
// be answered and to wait grace; and closing the set ends an add that
// waits.
func TestConnSetGrace(t *testing.T) {
	const grace = 100 * time.Millisecond
	cs := newConnSet(2, grace)
	conn := func() *peerConn {
		a, b := net.Pipe()
		t.Cleanup(func() {
			a.Close()
			b.Close()
		})
		return &peerConn{Conn: a, set: cs}
	}
	held := func(c *peerConn) bool {
		cs.mu.Lock()
		defer cs.mu.Unlock()
		_, ok := cs.held[c]
		return ok
	}
	added := make(chan time.Time, 1)
	addLater := func(c *peerConn) {
		go func() {
			if cs.add(c) {
				added <- time.Now()
			}
		}()
	}
	wait := func(what string) time.Time {
		t.Helper()
		select {
		case at := <-added:
			return at
		case <-time.After(10 * time.Second):
			t.Fatalf("%s wasn't added", what)
			return time.Time{}
		}
	}

	before := time.Now()
	first, second := conn(), conn()
	if !cs.add(first) || !cs.add(second) {
		t.Fatal("the set didn't take two connections")
	}
	third := conn()
	addLater(third)
	at := wait("a third connection")
	if took := at.Sub(before); took < grace {
		t.Errorf("the third connection took a place %v after the first's, before it had waited %v", took, grace)
	}
	if held(first) || !held(second) || !held(third) {
		t.Errorf("the set holds the first %t, the second %t and the third %t; want the second and the third", held(first), held(second), held(third))
	}

	// Every connection handling a request.
	cs.setState(second, http.StateActive)
	cs.setState(third, http.StateActive)
	fourth := conn()
	addLater(fourth)
	select {
	case <-added:
		t.Fatal("a fourth connection was added while every one handled a request")
	case <-time.After(3 * grace):
	}
	answered := time.Now()
	cs.setState(second, http.StateIdle)
	at = wait("a fourth connection")
	if took := at.Sub(answered); took < grace {
		t.Errorf("the fourth connection took the second's place %v after it was answered, before it had waited %v", took, grace)
	}
	if held(second) || !held(third) || !held(fourth) {
		t.Errorf("the set holds the second %t, the third %t and the fourth %t; want the third and the fourth", held(second), held(third), held(fourth))
	}

	// Closed, the set takes no more.
	cs.setState(fourth, http.StateActive)
	refused := make(chan bool, 1)
	go func() { refused <- !cs.add(conn()) }()
	cs.close()
	select {
	case ok := <-refused:
		if !ok {
			t.Error("a connection was added once the set was closed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("closing the set didn't end an add that waited")
	}
}
