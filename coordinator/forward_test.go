package coordinator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/runlog/runlogtest"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// forwardingDriver is a testDriver whose members' workers reach the
// coordinator through a listener on the member (backend.Forwarder), which
// it never forwards.
type forwardingDriver struct{ *testDriver }

var _ backend.Forwarder = forwardingDriver{}

func (forwardingDriver) MemberKey(*backend.Member) (string, error) { return "ssh-test-member", nil }

func (forwardingDriver) Endpoint(context.Context, *backend.Member) (backend.Endpoint, error) {
	return backend.Endpoint{Address: "/nowhere/test.sock"}, nil
}

func (forwardingDriver) Forward(context.Context, *backend.Member) (net.Listener, error) {
	return nil, errors.New("test: no listener")
}

// downGroups is groupsOf, which can't answer while down is set, and counts
// the times it is asked.
type downGroups struct {
	groupsOf
	down  atomic.Bool
	asked atomic.Int32
}

func (g *downGroups) Group(ctx context.Context, name string) (*backend.Group, error) {
	g.asked.Add(1)
	if g.down.Load() {
		return nil, errors.New("test: the groups can't be read")
	}
	return g.groupsOf.Group(ctx, name)
}

// TestForwardAllBacksOff checks that the look for the members of attempts
// placed on members reached through listeners asks again for the member of
// an attempt it couldn't find only after a wait that doubles, from twice
// forwardLook up to forwardLookMost, rather than readying its job at every
// look; and that it forgets the attempt once it has ended.
func TestForwardAllBacksOff(t *testing.T) {
	parallel(t)
	d := forwardingDriver{newTestDriver()}
	d.worker = func(backend.PlaceRequest) testEnd { return testEnd{never: true} }
	g := &downGroups{groupsOf: groupsOf{"local": {Name: "local", Members: []backend.Member{{Name: "m1", Backend: testBackend}}}}}
	c := open(t, Options{Groups: g, Drivers: map[string]backend.Driver{testBackend: d}})
	d.c, d.ctx = c, t.Context()
	t.Cleanup(d.workers.Wait)
	id, _ := create(t, c, oneJob, nil, nil)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.supervise(t.Context(), id, "main", nil); err != nil {
		t.Fatal(err)
	}
	a := latestAttempt(t, c, id, "main")
	if a.Placement.MemberKey != "ssh-test-member" {
		t.Fatalf("the attempt was placed with the member key %q", a.Placement.MemberKey)
	}
	g.down.Store(true)
	l := lookups{}
	t0 := time.Now()
	for _, look := range []struct {
		after time.Duration
		asks  bool
	}{
		{0, true},
		{forwardLook, false},
		{2 * forwardLook, true},
		{5 * forwardLook, false},
		{6 * forwardLook, true},
		{13 * forwardLook, false},
		{14 * forwardLook, true},
	} {
		before := g.asked.Load()
		err := c.forwardAll(t.Context(), l, t0.Add(look.after))
		if asked := g.asked.Load() != before; asked != look.asks || asked != (err != nil) {
			t.Errorf("at %v, the look asked for the group: %v, with %v; want %v", look.after, asked, err, look.asks)
		}
	}
	for range 10 {
		l.failed(a.ID, t0)
	}
	if w := l[a.ID].wait; w != forwardLookMost {
		t.Errorf("the wait grows to %v, want %v", w, forwardLookMost)
	}
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndLost, Outcome: plan.OutcomeFailure, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.forwardAll(t.Context(), l, t0); err != nil || len(l) != 0 {
		t.Errorf("once the attempt ended, the look gave %v, and keeps %v", err, l)
	}
}

// listeningDriver is a forwardingDriver whose Forward listens on a socket
// of its own, in dir, each time, and counts the listeners it made and
// those closed since.
type listeningDriver struct {
	forwardingDriver
	dir              string
	forwards, closed atomic.Int32
}

func (d *listeningDriver) Forward(ctx context.Context, _ *backend.Member) (net.Listener, error) {
	n := d.forwards.Add(1)
	l, err := (&net.ListenConfig{}).Listen(ctx, "unix", filepath.Join(d.dir, fmt.Sprintf("m%d.sock", n)))
	if err != nil {
		return nil, err
	}
	return &countedListener{Listener: l, closed: &d.closed}, nil
}

// countedListener counts itself in closed once it is closed.
type countedListener struct {
	net.Listener
	once   sync.Once
	closed *atomic.Int32
}

func (l *countedListener) Close() error {
	l.once.Do(func() { l.closed.Add(1) })
	return l.Listener.Close()
}

// TestServeMembersLetsGo checks that ServeMembers lets go of a member's
// listener, closing it, and its lease in the store, once no attempt has
// needed the member for forwardIdle, but not sooner, so that a member
// between one job and the next keeps it; that a placement on the member
// holds it again, which an attempt that hasn't ended keeps however long;
// and that it lets go of every listener as it stops.
func TestServeMembersLetsGo(t *testing.T) {
	parallel(t)
	// A Unix socket's path is short: the test's own temporary directory
	// can be too long for one.
	dir, err := os.MkdirTemp("", "meridian-fw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	d := &listeningDriver{forwardingDriver: forwardingDriver{newTestDriver()}, dir: dir}
	d.worker = func(backend.PlaceRequest) testEnd { return testEnd{never: true} }
	g := groupsOf{"local": {Name: "local", Members: []backend.Member{{Name: "m1", Backend: testBackend}}}}
	c := open(t, Options{Groups: g, Drivers: map[string]backend.Driver{testBackend: d}})
	d.c, d.ctx = c, t.Context()
	t.Cleanup(d.workers.Wait)
	ctx, stop := context.WithCancel(context.WithoutCancel(t.Context()))
	defer stop()
	served := make(chan error, 1)
	go func() { served <- c.ServeMembers(ctx, &runlogtest.Logs{}) }()
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.forwarding != nil
	})
	const key = "ssh-test-member"
	place := func() *store.Attempt {
		t.Helper()
		id, _ := create(t, c, oneJob, nil, nil)
		if _, err := c.startRun(t.Context(), id, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.supervise(t.Context(), id, "main", nil); err != nil {
			t.Fatal(err)
		}
		return latestAttempt(t, c, id, "main")
	}
	held := func() bool {
		held, err := c.store.EndpointHeld(key)
		if err != nil {
			t.Fatal(err)
		}
		return held
	}

	a := place()
	waitFor(t, func() bool { return d.forwards.Load() == 1 && held() })
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(a.ID, store.AttemptEnd{Kind: store.EndLost, Outcome: plan.OutcomeFailure, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	l := lookups{}
	// A member between one job and the next keeps its listener.
	if err := c.forwardAll(ctx, l, time.Now().Add(forwardIdle-time.Minute)); err != nil || !c.holding(key) || d.closed.Load() != 0 {
		t.Fatalf("within forwardIdle of the attempt's end, the listener is held: %t, with %d closed, %v", c.holding(key), d.closed.Load(), err)
	}
	// Once no attempt has needed it for forwardIdle, it is let go.
	if err := c.forwardAll(ctx, l, time.Now().Add(forwardIdle)); err != nil || c.holding(key) {
		t.Fatalf("after forwardIdle, the listener is still held: %t, %v", c.holding(key), err)
	}
	waitFor(t, func() bool { return d.closed.Load() == 1 && !held() })

	// A placement on the member holds it again, and an attempt that
	// hasn't ended keeps it however long.
	b := place()
	waitFor(t, func() bool { return d.forwards.Load() == 2 && held() })
	later := time.Now().Add(10 * forwardIdle)
	if err := c.forwardAll(ctx, l, later); err != nil || !c.holding(key) || d.closed.Load() != 1 {
		t.Fatalf("an attempt that hasn't ended let its member go: held %t, with %d closed, %v", c.holding(key), d.closed.Load(), err)
	}
	// Its member is kept for forwardIdle from the last look that found
	// the attempt, not from its placement.
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(b.ID, store.AttemptEnd{Kind: store.EndLost, Outcome: plan.OutcomeFailure, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.forwardAll(ctx, l, later.Add(forwardIdle-time.Minute)); err != nil || !c.holding(key) || d.closed.Load() != 1 {
		t.Fatalf("within forwardIdle of the last look that found the attempt, held %t, with %d closed, %v", c.holding(key), d.closed.Load(), err)
	}

	// As ServeMembers stops, it lets go of every listener.
	stop()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if n := d.closed.Load(); n != 2 || held() || d.forwards.Load() != 2 {
		t.Errorf("once ServeMembers stopped, %d of %d listeners are closed, and the lease is held: %t", n, d.forwards.Load(), held())
	}
}
