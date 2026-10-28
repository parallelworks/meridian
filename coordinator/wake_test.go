package coordinator

import (
	"errors"
	"slices"
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
// woken again together, in one wakeAgain, which the rewake hook hears of
// once: here ten, which, registered a wakeAgain each, were each tried
// after the last had had its tries. The hook hears of them once the
// transaction commits, and a transaction that rolls back wakes none of
// them, nor tells the hook of them.
func TestWakeAgainAfterTogether(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	var (
		mu    sync.Mutex
		tried int
		heard []int
	)
	c.rewake = func(ws []*waking) {
		mu.Lock()
		defer mu.Unlock()
		heard = append(heard, len(ws))
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
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		for _, w := range wakings(n) {
			c.wakeAgainAfter(tx, []*waking{w})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if tried != n || !slices.Equal(heard, []int{n}) {
		t.Errorf("of %d woken again, %d were tried, in wakes of %v, want one of %d", n, tried, heard, n)
	}
	mu.Unlock()

	errRolledBack := errors.New("rolled back")
	if err := c.store.Tx(t.Context(), func(tx *store.Tx) error {
		c.wakeAgainAfter(tx, wakings(n))
		return errRolledBack
	}); !errors.Is(err, errRolledBack) {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if tried != n || len(heard) != 1 {
		t.Errorf("a transaction that rolled back woke %d again, and told the hook of %v", tried-n, heard[1:])
	}
}
