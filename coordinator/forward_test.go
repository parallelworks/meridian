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
// serveMembers returns a coordinator whose group local has one member,
// m1, of a listeningDriver, which it returns too, whose workers never end,
// once its ServeMembers runs; and what stops ServeMembers, and returns
// what it did.
func serveMembers(t *testing.T) (*Coordinator, *listeningDriver, func() error) {
	t.Helper()
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
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	served := make(chan error, 1)
	go func() { served <- c.ServeMembers(ctx, &runlogtest.Logs{}) }()
	stop := sync.OnceValue(func() error {
		cancel()
		return <-served
	})
	t.Cleanup(func() { _ = stop() })
	waitFor(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.forwarding != nil
	})
	return c, d, stop
}

// endAttempt ends the attempt id, as lost.
func endAttempt(t *testing.T, c *Coordinator, id string) {
	t.Helper()
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		return tx.EndAttempt(id, store.AttemptEnd{Kind: store.EndLost, Outcome: plan.OutcomeFailure, Reason: plan.ReasonWorkerLost})
	}); err != nil {
		t.Fatal(err)
	}
}

// memberKeyM1 is the key of serveMembers' member m1, as a
// forwardingDriver gives every member.
const memberKeyM1 = "ssh-test-member"

// placeOnM1 places the one job of a new run on serveMembers' member m1,
// and returns its attempt.
func placeOnM1(t *testing.T, c *Coordinator) *store.Attempt {
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

// m1Held reports whether a process holds the lease of member m1's
// listener.
func m1Held(t *testing.T, c *Coordinator) bool {
	t.Helper()
	held, err := c.store.EndpointHeld(memberKeyM1)
	if err != nil {
		t.Fatal(err)
	}
	return held
}

// storeGraceFrom returns when the store's grace runs from.
func storeGraceFrom(t *testing.T, c *Coordinator) time.Time {
	t.Helper()
	var from time.Time
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		from, err = tx.GraceFrom()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return from
}

// m1Endpoint returns the store's record of member m1's lease.
func m1Endpoint(t *testing.T, c *Coordinator) store.Endpoint {
	t.Helper()
	var e *store.Endpoint
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		e, err = tx.Endpoint(memberKeyM1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return *e
}

func TestServeMembersLetsGo(t *testing.T) {
	parallel(t)
	c, d, stop := serveMembers(t)
	ctx := t.Context()
	const key = memberKeyM1
	place := func() *store.Attempt { t.Helper(); return placeOnM1(t, c) }
	held := func() bool { t.Helper(); return m1Held(t, c) }
	graceFrom := func() time.Time { t.Helper(); return storeGraceFrom(t, c) }
	endpoint := func() store.Endpoint { t.Helper(); return m1Endpoint(t, c) }

	a := place()
	waitFor(t, func() bool { return d.forwards.Load() == 1 && held() })
	// The member's lease, taken for the first time, starts a grace, in
	// which its workers' leases, which could have lapsed while no process
	// held it, don't count.
	if e := endpoint(); !e.GraceFrom.Equal(e.TakenAt) || !graceFrom().Equal(e.TakenAt) {
		t.Errorf("the lease taken first is %+v, and the store's grace is from %v", e, graceFrom())
	}
	endAttempt(t, c, a.ID)
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
	graceBefore := graceFrom()
	b := place()
	waitFor(t, func() bool { return d.forwards.Load() == 2 && held() })
	// It takes the lease again as idle, starting no grace, so that the
	// leases of the store's other attempts, on any member, lapse as they
	// would, however often members come and go.
	if e := endpoint(); !e.GraceFrom.IsZero() || !graceFrom().Equal(graceBefore) {
		t.Errorf("the lease taken again is %+v, and the store's grace is from %v, was %v", e, graceFrom(), graceBefore)
	}
	if _, graceEnds, err := c.leaseClock(t.Context()); err != nil || graceEnds.After(time.Now()) {
		t.Errorf("the lease taken again keeps lapsed leases from counting until %v, %v", graceEnds, err)
	}
	later := time.Now().Add(10 * forwardIdle)
	if err := c.forwardAll(ctx, l, later); err != nil || !c.holding(key) || d.closed.Load() != 1 {
		t.Fatalf("an attempt that hasn't ended let its member go: held %t, with %d closed, %v", c.holding(key), d.closed.Load(), err)
	}
	// Its member is kept for forwardIdle from the last look that found
	// the attempt, not from its placement.
	endAttempt(t, c, b.ID)
	if err := c.forwardAll(ctx, l, later.Add(forwardIdle-time.Minute)); err != nil || !c.holding(key) || d.closed.Load() != 1 {
		t.Fatalf("within forwardIdle of the last look that found the attempt, held %t, with %d closed, %v", c.holding(key), d.closed.Load(), err)
	}

	// A placement on the member keeps it however long the placement
	// takes, which records the attempt's member only as it ends; and
	// once it has ended, it is let go as before.
	m := backend.Member{Name: "m1", Backend: testBackend}
	_, placed := c.hold(d, &m, key, true)
	if err := c.forwardAll(ctx, l, later.Add(10*forwardIdle)); err != nil || !c.holding(key) || d.closed.Load() != 1 {
		t.Fatalf("a placement that took long lost its member's listener: held %t, with %d closed, %v", c.holding(key), d.closed.Load(), err)
	}
	placed()
	if err := c.forwardAll(ctx, l, later.Add(11*forwardIdle)); err != nil || c.holding(key) {
		t.Fatalf("forwardIdle after the placement ended, the listener is still held: %t, %v", c.holding(key), err)
	}
	waitFor(t, func() bool { return d.closed.Load() == 2 && !held() })

	// As ServeMembers stops, it lets go of every listener.
	place()
	waitFor(t, func() bool { return d.forwards.Load() == 3 && held() })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if n := d.closed.Load(); n != 3 || held() || d.forwards.Load() != 3 {
		t.Errorf("once ServeMembers stopped, %d of %d listeners are closed, and the lease is held: %t", n, d.forwards.Load(), held())
	}
}

// TestServeMembersForgetsIdle checks that ServeMembers remembers a member
// it let go of as idle for forwardIdleForget, and then forgets it: a
// member that comes back within it takes its lease again as idle,
// starting no grace, and one that comes back after it takes its lease as
// one this process never held does, starting one, so that what it
// remembers is bounded by time.
func TestServeMembersForgetsIdle(t *testing.T) {
	parallel(t)
	c, d, _ := serveMembers(t)
	idle := func() (time.Time, bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		at, ok := c.forwarding.idle[memberKeyM1]
		return at, ok
	}
	l := lookups{}
	// The member's job is placed and ends, and the member is let go, at
	// gone, once no attempt has needed it for forwardIdle.
	a := placeOnM1(t, c)
	waitFor(t, func() bool { return d.forwards.Load() == 1 && m1Held(t, c) })
	endAttempt(t, c, a.ID)
	gone := time.Now().Add(forwardIdle)
	if err := c.forwardAll(t.Context(), l, gone); err != nil || c.holding(memberKeyM1) {
		t.Fatalf("after forwardIdle, the listener is still held: %t, %v", c.holding(memberKeyM1), err)
	}
	waitFor(t, func() bool { return d.closed.Load() == 1 && !m1Held(t, c) })
	if at, ok := idle(); !ok || !at.Equal(gone) {
		t.Fatalf("the member let go of is remembered as idle: %t, from %v, want %v", ok, at, gone)
	}
	// Within forwardIdleForget, it is remembered.
	if err := c.forwardAll(t.Context(), l, gone.Add(forwardIdleForget-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, ok := idle(); !ok {
		t.Fatal("the member was forgotten within forwardIdleForget of its letting go")
	}
	// Once it has passed, it is forgotten.
	if err := c.forwardAll(t.Context(), l, gone.Add(forwardIdleForget)); err != nil {
		t.Fatal(err)
	}
	if _, ok := idle(); ok {
		t.Fatal("the member was remembered forwardIdleForget after its letting go")
	}
	// Coming back, it takes its lease as one never held does: a grace
	// starts from its taking.
	placeOnM1(t, c)
	waitFor(t, func() bool { return d.forwards.Load() == 2 && m1Held(t, c) })
	if e := m1Endpoint(t, c); e.GraceFrom.IsZero() || !e.GraceFrom.Equal(e.TakenAt) || !storeGraceFrom(t, c).Equal(e.TakenAt) {
		t.Errorf("the lease of a member forgotten is %+v, and the store's grace is from %v", e, storeGraceFrom(t, c))
	}
}

// TestResumePlaceLetsGo checks that a placement that a supervisor resumes,
// as one that ended before it recorded it, needs its member's listener
// only until it is recorded: forwardIdle after the attempt ends, it is let
// go.
func TestResumePlaceLetsGo(t *testing.T) {
	parallel(t)
	c, d, _ := serveMembers(t)
	const key = "ssh-test-member"
	id, _ := create(t, c, oneJob, nil, nil)
	if _, err := c.startRun(t.Context(), id, nil); err != nil {
		t.Fatal(err)
	}
	var a *store.Attempt
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		var err error
		if a, _, err = tx.NewAttempt(c.newAttempt(id, "main", false)); err != nil {
			return err
		}
		return tx.ChoosePlace(a.ID, store.Place{Group: "local", Member: "m1", Backend: testBackend})
	}); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.placements[a.ID] = backend.Placement{ID: "test-resumed", WorkerDigest: testDigest}
	d.mu.Unlock()
	if _, err := c.resumePlace(t.Context(), storedRun(t, c, id), storedJob(t, c, id, "main"), a); err != nil {
		t.Fatal(err)
	}
	if got := latestAttempt(t, c, id, "main"); got.Placement.ID != "test-resumed" || got.Placement.MemberKey != key || !c.holding(key) {
		t.Fatalf("the resumed attempt is %+v, and its member is held: %t", got, c.holding(key))
	}
	endAttempt(t, c, a.ID)
	if err := c.forwardAll(t.Context(), lookups{}, time.Now().Add(forwardIdle)); err != nil || c.holding(key) {
		t.Fatalf("forwardIdle after the resumed attempt ended, its member is still held: %t, %v", c.holding(key), err)
	}
	waitFor(t, func() bool { return d.closed.Load() == d.forwards.Load() })
}
