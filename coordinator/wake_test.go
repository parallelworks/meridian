package coordinator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hopperdriver "github.com/parallelworks/hopper/driver"

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
// While hopper runs it, no try takes the store's write lock: each reads
// first that hopper runs it, which a wake would fail for.
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
	mu.Lock()
	if len(tries) != 0 {
		t.Errorf("%d tries took the write lock while hopper ran the supervisor", len(tries))
	}
	mu.Unlock()
	released := time.Now()
	close(release)
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(tries) > 0 && tries[len(tries)-1].After(released)
	})
}

// failingWaking is a waking, of no job hopper has, whose waits fails with
// err, which tries counts, and on each try calls tried, if it is set, with
// the try's number, from 1.
func failingWaking(tries *atomic.Int32, err error, tried func(n int32)) *waking {
	return &waking{run: "1", key: "job", waits: func(*store.Tx) (bool, error) {
		n := tries.Add(1)
		if tried != nil {
			tried(n)
		}
		return false, err
	}}
}

// errBusy is a failure a try of a wake meets that a later one may not, as
// a store busy for longer than it waits.
var errBusy = errors.New("busy")

// TestWakeAgainPastFailure checks that a try of a wake that fails is made
// again, as on a store busy for longer than it waits, until one doesn't,
// and that one of a job the store doesn't have isn't. Before, a failure
// ended the wake as one that had landed.
func TestWakeAgainPastFailure(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	var failing, missing atomic.Int32
	w := &waking{run: "1", key: "job", waits: func(*store.Tx) (bool, error) {
		if failing.Add(1) < 3 {
			return false, errBusy
		}
		return false, nil
	}}
	c.wakeAgain(t.Context(), w, failingWaking(&missing, store.ErrNotFound, nil))
	if n := failing.Load(); n != 3 {
		t.Errorf("a wake whose first two tries failed was tried %d times, want 3", n)
	}
	if n := missing.Load(); n != 1 {
		t.Errorf("a wake of a job the store doesn't have was tried %d times, want 1", n)
	}
}

// TestWakeAgainOutlivesCaller checks that the tries of a wake that its
// caller, done, no longer waits for are made in the background, while the
// coordinator is open. Before, they were dropped.
func TestWakeAgainOutlivesCaller(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	ctx, cancel := context.WithCancel(t.Context())
	var tries atomic.Int32
	c.wakeAgain(ctx, failingWaking(&tries, errBusy, func(n int32) {
		if n == 1 {
			cancel()
		}
	}))
	if n := tries.Load(); n != 1 {
		t.Fatalf("a caller done after its first try waited for %d", n)
	}
	waitFor(t, func() bool { return tries.Load() > 1 })
}

// TestCloseEndsWakes checks that Close ends the wakes the coordinator
// makes in the background, and waits for them: it doesn't return while
// one is being tried, and returns at once after, though the wake would be
// tried again for a while. Before, Close neither ended nor waited for
// them, and each went on until it found the store closed.
func TestCloseEndsWakes(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	var tries atomic.Int32
	trying, release := heldTry(t, c, t.Context(), &tries)
	<-trying
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned, with %v, while a wake was being tried", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for a wake that it could have ended")
	}
	if n := tries.Load(); n != wakeTries {
		t.Errorf("a wake was tried %d times, want %d: none after Close", n, wakeTries)
	}
}

// TestWakeAgainInBackgroundOwnsNoJob checks that the wakes made in the
// background don't run as the hopper job that gave them, which hoppersqlite
// holds the owner of its transactions: a write of the job's, as the job
// goes on, while one of theirs holds the store's write lock, waits its
// turn. Before, they ran with the job's context, and the job's write
// failed at once (hoppersqlite.ErrLockHeldByOwner).
func TestWakeAgainInBackgroundOwnsNoJob(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	job := hopperdriver.WithOwner(t.Context(), &testOwner{"a job's attempt"})
	var tries atomic.Int32
	trying, release := heldTry(t, c, job, &tries)
	<-trying
	wrote := make(chan error, 1)
	go func() { wrote <- c.store.Tx(job, func(*store.Tx) error { return nil }) }()
	select {
	case err := <-wrote:
		t.Fatalf("a write of the job's, as a wake in the background held the write lock, returned %v at once; want it to wait", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}

// heldTry has c wake again, with ctx, a waking whose tries fail, which
// tries counts, until the first try made in the background: it holds that
// one, in its transaction, until release, or the test's end. trying is
// closed once it does.
func heldTry(t *testing.T, c *Coordinator, ctx context.Context, tries *atomic.Int32) (trying <-chan struct{}, release func()) {
	t.Helper()
	held, released := make(chan struct{}), make(chan struct{})
	release = sync.OnceFunc(func() { close(released) })
	t.Cleanup(release)
	c.wakeAgain(ctx, failingWaking(tries, errBusy, func(n int32) {
		if n == wakeTries {
			// The first of the background's.
			close(held)
			<-released
		}
	}))
	return held, release
}

// A testOwner is the owner of a test's transactions, as a hopper job's
// attempt is of its worker's (hopperdriver.WithOwner).
type testOwner struct{ name string }

func (o *testOwner) String() string { return o.name }

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
