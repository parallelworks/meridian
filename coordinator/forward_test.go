package coordinator

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
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
