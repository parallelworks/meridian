package coordinator

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
)

// TestWakeAgainWhileRunning checks that a wake of a supervisor that hopper
// runs is tried again for as long as hopper runs it, past the tries that
// its waker waits for, and so lands once hopper has recorded the wait its
// look returned: here a look that takes longer than those tries, as one
// whose wait a loaded host's hopper records late. Before, the wake gave up
// after its tries, and the supervisor, or a marker of a need on any
// instance, waited its whole wait, as TestAnyFirstSuccess did under load.
func TestWakeAgainWhileRunning(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	placing, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	driverOf(c).set(func(d *testDriver) {
		d.place = func(backend.PlaceRequest) error {
			once.Do(func() {
				close(placing)
				<-release
			})
			return nil
		}
	})
	id, _ := create(t, c, "jobs:\n  a:\n    steps:\n      - run: echo\n", nil, nil)
	serve(t, c)
	<-placing

	// The supervisor of a looks, as hopper runs it, until release.
	var job *store.Job
	if err := c.store.View(t.Context(), func(tx *store.Tx) error {
		var err error
		job, err = tx.Job(id, "a")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		tries []time.Time
	)
	waits := unended(id, "a")
	w := &waking{id: job.SupervisorID, run: id, key: "a", waits: func(tx *store.Tx) (bool, error) {
		mu.Lock()
		tries = append(tries, time.Now())
		mu.Unlock()
		return waits(tx)
	}}
	c.wakeAgain(t.Context(), w)
	// Well past the tries the waker waits for.
	time.Sleep(10 * wakeTries * wakeAgainAfter) //nolint:forbidigo // the look outlasts the waker's tries
	released := time.Now()
	close(release)
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return tries[len(tries)-1].After(released)
	})
}

// TestWakeAgainAfterTogether checks that what a transaction wakes again
// once it commits, in as many calls of wakeAgainAfter as it makes, is
// woken again together, in about the time of one wake: here ten, which a
// wake each after the last, as the transaction registered each, took ten
// times as long. The rewake hook hears of them once the transaction
// commits, and a transaction that rolls back wakes none of them, nor tells
// the hook of them.
func TestWakeAgainAfterTogether(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	var (
		mu      sync.Mutex
		tried   int
		rewoken int
	)
	c.rewake = func(ws []*waking) {
		mu.Lock()
		defer mu.Unlock()
		rewoken += len(ws)
	}
	// A waking that no longer waits, as hopper has recorded its wait by
	// the first try, which a try reads and wakes no more.
	wakings := func(n int) []*waking {
		ws := make([]*waking, n)
		for i := range ws {
			ws[i] = &waking{run: "1", key: "job", waits: func(*store.Tx) (bool, error) {
				mu.Lock()
				defer mu.Unlock()
				tried++
				return false, nil
			}}
		}
		return ws
	}
	const n = 10
	began := time.Now()
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		for _, w := range wakings(n) {
			c.wakeAgainAfter(tx, []*waking{w})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	took := time.Since(began)
	mu.Lock()
	if tried != n || rewoken != n {
		t.Errorf("of %d woken again, %d were tried, and the hook heard of %d", n, tried, rewoken)
	}
	mu.Unlock()
	// One wake waits wakeAgainAfter before its first try; ten, one after
	// another, waited ten times that.
	if took > 4*wakeAgainAfter {
		t.Errorf("waking %d again took %v, more than one wake's %v", n, took, wakeAgainAfter)
	}

	errRolledBack := errors.New("rolled back")
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		c.wakeAgainAfter(tx, wakings(n))
		return errRolledBack
	}); !errors.Is(err, errRolledBack) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if tried != n || rewoken != n {
		t.Errorf("a transaction that rolled back woke %d again, and told the hook of %d", tried-n, rewoken-n)
	}
}
