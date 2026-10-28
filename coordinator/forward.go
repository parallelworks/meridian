package coordinator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/backoff"
)

// How often ServeMembers looks for the members of attempts that haven't
// ended, whose listeners it doesn't hold, as those of a coordinator that
// has stopped, which it takes over (forwardAll); and, for an attempt whose
// member it couldn't find, as when its group can't be read, how long it
// waits at most before it looks for it again: twice forwardLook after the
// first time, doubling each time after (lookups).
const (
	forwardLook     = 5 * time.Second
	forwardLookMost = 5 * time.Minute
)

// How long a member's holder waits before it forwards again, after its
// listener failed or ended, as with the connection it was made over: from
// forwardFirst, doubling to forwardMost, less up to a quarter at random,
// and from forwardFirst again after a listener that lasted forwardMost.
const (
	forwardFirst = time.Second
	forwardMost  = 30 * time.Second
)

// forwardIdle is how long a member's holder holds its listener once no
// attempt needs it, before it lets it go (releaseIdle): long enough that
// a member between one job and the next keeps it.
const forwardIdle = 5 * time.Minute

// A forwarding is what ServeMembers serves members' listeners with while
// it runs: its context, and the worker API's server.
type forwarding struct {
	ctx context.Context
	srv *workerapisrv.Server
	// holders are the members whose listeners this process holds, or
	// waits to, by their keys; and idle the keys of those it let go of
	// as no attempt needed them (releaseIdle), and hasn't held again,
	// whose leases it takes again as idle (store.HoldIdleEndpoint). The
	// coordinator's mu guards both. A key leaves idle only as its member
	// is held again, never as the member leaves its group: idle holds one
	// key for each member ServeMembers ever let go of, as a meridian run
	// serves one tree's few, and needs pruning before a coordinator that
	// serves for long, as meridian serve would.
	holders map[string]*holder
	idle    map[string]bool
	// lookups are when to look again for the members of the attempts
	// whose members forwardAll couldn't find, which only ServeMembers
	// touches.
	lookups lookups
	wg      sync.WaitGroup
}

// lookups are when forwardAll is to look again for the member of each
// attempt whose member it couldn't find, by the attempt's ID, so that one
// whose member it keeps failing to find, as one of a group it can't read,
// isn't readied and logged at every look.
type lookups map[string]lookup

// A lookup is when to look again for an attempt's member, after how long
// a wait.
type lookup struct {
	at   time.Time
	wait time.Duration
}

// due reports whether to look for the member of the attempt id at now.
func (l lookups) due(id string, now time.Time) bool {
	return !now.Before(l[id].at)
}

// failed records that the member of the attempt id wasn't found at now,
// and is looked for again after twice the wait before, from twice
// forwardLook, up to forwardLookMost.
func (l lookups) failed(id string, now time.Time) {
	wait := min(max(2*l[id].wait, 2*forwardLook), forwardLookMost)
	l[id] = lookup{at: now.Add(wait), wait: wait}
}

// keep forgets the attempts that live doesn't hold.
func (l lookups) keep(live map[string]bool) {
	maps.DeleteFunc(l, func(id string, _ lookup) bool { return !live[id] })
}

// A holder holds a member's listener for the process: it takes the
// member's lease in the store, forwards the member's listener, serves the
// worker API on it, and forwards it again whenever it ends, until
// ServeMembers stops, or it is let go, once no attempt has needed it for
// forwardIdle (releaseIdle), which cancel does. ready is closed once it
// knows how it is placed to serve the member's workers, which err says
// (status).
type holder struct {
	ready chan struct{}
	once  sync.Once
	mu    sync.Mutex
	err   error
	// cancel lets the holder go: it closes the member's listener and
	// releases its lease. needed is the latest time an attempt needed it,
	// as a placement on the member, or one that hadn't ended, and placing
	// how many placements on the member use it now, each from its forward
	// until it is recorded, or fails, which keeps it held however long
	// they take. The coordinator's mu guards them, as it does the holders.
	cancel  context.CancelFunc
	needed  time.Time
	placing int
}

// report says how the holder is placed to serve the member's workers: nil
// once it forwards the member's listener, or another process does, and
// otherwise why it can't, as a member whose host refuses to forward it.
func (h *holder) report(err error) {
	h.mu.Lock()
	h.err = err
	h.mu.Unlock()
	h.once.Do(func() { close(h.ready) })
}

// errHolderStopped is what a holder that stopped before it knew how it was
// placed says to those that wait for it (status).
var errHolderStopped = errors.New("coordinator: the member's listener was let go before it was forwarded")

// stopped says, to whoever still waits to learn how the holder is placed,
// that it stopped first (errHolderStopped); what it reported before
// stands.
func (h *holder) stopped() {
	h.once.Do(func() {
		h.mu.Lock()
		h.err = errHolderStopped
		h.mu.Unlock()
		close(h.ready)
	})
}

// status waits until the holder knows how it is placed, or ctx is done,
// and says it (report).
func (h *holder) status(ctx context.Context) error {
	select {
	case <-h.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// ServeMembers serves the worker API to the workers of members whose
// backends reach them through listeners on the members
// (backend.Forwarder), as the ssh backend's do through sockets forwarded
// over SSH, until ctx is done, storing the logs they upload with logs. Of
// each member that an attempt placed through this process needs, and of
// the members of every attempt the store has that hasn't ended, it holds
// the listener once no other process does: it takes the member's lease in
// the store, named by the member's key, waiting for the process that
// holds it to release it, as one that stopped does, and so takes over its
// members' workers. The workers of a member it can't reach meanwhile go
// on, and reach it once it forwards their listener again. It lets go of a
// member's listener, and its lease, once no attempt has needed the member
// for forwardIdle, and holds it again for the next attempt that does
// (releaseIdle), so that a long-lived process doesn't keep a listener on
// every member it ever served.
func (c *Coordinator) ServeMembers(ctx context.Context, logs Logs) error {
	if err := c.writable(); err != nil {
		return err
	}
	srv, release, err := c.useServer(logs)
	if err != nil {
		return err
	}
	defer release()
	f := &forwarding{ctx: ctx, srv: srv, holders: map[string]*holder{}, idle: map[string]bool{}, lookups: lookups{}}
	c.mu.Lock()
	if c.forwarding != nil {
		c.mu.Unlock()
		return errors.New("coordinator: ServeMembers is already running")
	}
	c.forwarding = f
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.forwarding = nil
		c.mu.Unlock()
		f.wg.Wait()
	}()
	tick := time.NewTicker(forwardLook)
	defer tick.Stop()
	for {
		if err := c.forwardAll(ctx, f.lookups, time.Now()); err != nil && ctx.Err() == nil {
			c.log.WarnContext(ctx, "coordinator: couldn't look for the members whose listeners to hold", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// forwardAll holds the listener of the member of each attempt of the
// store's that hasn't ended, and was placed on a member whose backend
// reaches it through one (store.Tx.MemberAttempts), unless this process
// holds it, or waits to, already. The member is the one the attempt's
// group has, as its job readies again, never the store's: a member's host
// can come from a variable or a password. Of an attempt whose member it
// couldn't find, it looks again only once l says to, at now. It lets go
// of the listeners of the members that no attempt has needed for
// forwardIdle (releaseIdle).
func (c *Coordinator) forwardAll(ctx context.Context, l lookups, now time.Time) error {
	type found struct {
		run *store.Run
		job *store.Job
		a   *store.Attempt
	}
	var all []found
	needed := map[string]bool{}
	err := c.store.View(ctx, func(tx *store.Tx) error {
		as, err := tx.MemberAttempts()
		if err != nil {
			return err
		}
		live := make(map[string]bool, len(as))
		for _, a := range as {
			live[a.ID] = true
			needed[a.Placement.MemberKey] = true
		}
		l.keep(live)
		for _, a := range as {
			if c.holding(a.Placement.MemberKey) || !l.due(a.ID, now) {
				continue
			}
			run, err := tx.Run(a.Run)
			if err != nil {
				return err
			}
			job, err := tx.Job(a.Run, a.Job)
			if err != nil {
				return err
			}
			all = append(all, found{run, job, a})
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.releaseIdle(needed, now)
	var errs []error
	for _, f := range all {
		if c.holding(f.a.Placement.MemberKey) {
			continue
		}
		cand, _, err := c.placedMember(ctx, f.run, f.job, f.a)
		if err != nil {
			l.failed(f.a.ID, now)
			if !errors.Is(err, errMemberGone) {
				errs = append(errs, err)
			}
			continue
		}
		delete(l, f.a.ID)
		fw, ok := cand.driver.(backend.Forwarder)
		if !ok {
			continue
		}
		c.hold(fw, cand.member, f.a.Placement.MemberKey, false)
	}
	return errors.Join(errs...)
}

// releaseIdle lets go of the listener of each member this process holds,
// or waits to, that no attempt has needed for forwardIdle, at now: neither
// an attempt that hasn't ended, of the members whose keys needed holds,
// as the store said before now, nor a placement on it since, or that
// still runs (hold). It closes the member's listener and releases its
// lease in the store; an attempt that needs the member again holds it
// again, taking its lease as idle (store.HoldIdleEndpoint): no worker of
// the member's can wait to reach it meanwhile, so the store's grace, in
// which no lapsed lease of any member's counts, doesn't start again,
// however often members come and go. A member between one job and the
// next keeps it.
func (c *Coordinator) releaseIdle(needed map[string]bool, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.forwarding
	if f == nil {
		return
	}
	for key, h := range f.holders {
		switch {
		case needed[key] || h.placing > 0:
			h.needed = latest(h.needed, now)
		case now.Sub(h.needed) >= forwardIdle:
			// Its goroutine closes the listener and releases the lease as
			// it ends; a hold meanwhile starts another, which waits for
			// the lease.
			h.cancel()
			delete(f.holders, key)
			f.idle[key] = true
		}
	}
}

// holding reports whether this process holds the listener of the member
// whose key is key, or waits to.
func (c *Coordinator) holding(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.forwarding != nil && c.forwarding.holders[key] != nil
}

// forward returns where m's workers reach the coordinator, m being a
// Forwarder's member, once this process holds m's listener, or another
// process does, as ServeMembers holds it; or why m's workers can't reach
// it, as a host that refuses to forward its listener
// (problems.SSHForwardingRefused). Where ServeMembers doesn't run in this
// process, it is where the process that runs it holds m's listener, or
// will. The placement that asks calls placed once it is recorded, or has
// failed, whatever forward returns: until then, m's listener isn't let go
// (releaseIdle), however long the placement takes.
func (c *Coordinator) forward(ctx context.Context, fw backend.Forwarder, m *backend.Member) (_ backend.Endpoint, placed func(), _ error) {
	key, err := fw.MemberKey(m)
	if err != nil {
		return backend.Endpoint{}, func() {}, err
	}
	h, placed := c.hold(fw, m, key, true)
	if h != nil {
		if err := h.status(ctx); err != nil {
			return backend.Endpoint{}, placed, err
		}
	}
	e, err := fw.Endpoint(ctx, m)
	return e, placed, err
}

// hold returns the holder of the listener of m, whose key is key, which it
// starts if this process has none, while ServeMembers runs; or nil, where
// it doesn't. Either way the holder is needed now, which keeps it from
// being let go for forwardIdle (releaseIdle); and, for a placement, until
// it calls placed, which hold returns, and which is a no-op for any other
// caller. A holder of a member this process let go of as idle takes its
// lease again as idle (store.HoldIdleEndpoint).
func (c *Coordinator) hold(fw backend.Forwarder, m *backend.Member, key string, placing bool) (*holder, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.forwarding
	if f == nil || f.ctx.Err() != nil {
		return nil, func() {}
	}
	h := f.holders[key]
	if h != nil {
		h.needed = latest(h.needed, time.Now())
	} else {
		ctx, cancel := context.WithCancel(f.ctx)
		h = &holder{ready: make(chan struct{}), cancel: cancel, needed: time.Now()}
		idle := f.idle[key]
		delete(f.idle, key)
		f.holders[key] = h
		f.wg.Go(func() {
			defer func() {
				cancel()
				h.stopped()
				c.mu.Lock()
				// Another holder can have taken its place, once it was
				// let go.
				if f.holders[key] == h {
					delete(f.holders, key)
				}
				c.mu.Unlock()
			}()
			c.serveMember(ctx, f, h, fw, m, key, idle)
		})
	}
	if !placing {
		return h, func() {}
	}
	h.placing++
	return h, sync.OnceFunc(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		h.placing--
		h.needed = latest(h.needed, time.Now())
	})
}

// serveMember holds m's listener, whose key is key, as h, until ctx, h's,
// is done, as when ServeMembers stops or h is let go: once it holds m's
// lease, which it says it waits for where another process holds it, it
// forwards m's listener, and serves the worker API on it, and forwards it
// again whenever it fails, or ends. As it returns, the listener is closed
// and the lease released. Where idle is set, as for a member this process
// let go of as idle, it takes the lease as idle (store.HoldIdleEndpoint).
func (c *Coordinator) serveMember(ctx context.Context, f *forwarding, h *holder, fw backend.Forwarder, m *backend.Member, key string, idle bool) {
	if held, err := c.store.EndpointHeld(key); err == nil && held {
		// Another process serves m's workers, until it stops.
		h.report(nil)
	}
	take := c.store.HoldEndpoint
	if idle {
		take = c.store.HoldIdleEndpoint
	}
	lease, err := take(ctx, key, "meridian, process "+strconv.Itoa(os.Getpid()))
	if err != nil {
		if ctx.Err() == nil {
			h.report(fmt.Errorf("coordinator: hold the listener of member %s: %w", key, err))
		}
		return
	}
	defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	wait := forwardFirst
	for ctx.Err() == nil {
		l, err := fw.Forward(ctx, m)
		if err != nil {
			h.report(err)
			if ctx.Err() == nil {
				c.log.InfoContext(ctx, "coordinator: couldn't forward a member's listener, and tries again", "member", key)
			}
		} else {
			h.report(nil)
			began := time.Now()
			if err := f.srv.ServeMember(ctx, l, key); err != nil && ctx.Err() == nil {
				c.log.InfoContext(ctx, "coordinator: a member's listener ended, and is forwarded again", "member", key)
			}
			_ = l.Close()
			if time.Since(began) >= forwardMost {
				wait = forwardFirst
			}
		}
		timer := time.NewTimer(backoff.Jitter(wait))
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		wait = min(wait*2, forwardMost)
	}
}

// useServer returns the coordinator's server of the worker API, for
// ServeWorkers and ServeMembers, which it makes for the first that uses
// it, storing the logs workers upload with that caller's logs, which the
// callers at once share; and what to call once the caller is done with it.
// It answers polls and claims as the coordinator changes what they wait
// for while any caller uses it.
func (c *Coordinator) useServer(logs Logs) (*workerapisrv.Server, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.server == nil {
		srv, err := workerapisrv.New(workerapisrv.Options{
			Store: c.store, Logs: logs, Sealer: c.sealer,
			Readier:  claimReadier{c},
			Actions:  actions{c},
			Children: childAPI{c},
			Wake:     c.wakeWorkerJob,
			Log:      c.log,
		})
		if err != nil {
			return nil, nil, err
		}
		c.server = srv
	}
	c.serverUsers++
	c.controls = c.server
	srv := c.server
	return srv, sync.OnceFunc(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.serverUsers--; c.serverUsers == 0 {
			c.server, c.controls = nil, nil
		}
	}), nil
}
