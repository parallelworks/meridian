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

// A forwarding is what ServeMembers serves members' listeners with while
// it runs: its context, and the worker API's server.
type forwarding struct {
	ctx context.Context
	srv *workerapisrv.Server
	// holders are the members whose listeners this process holds, or
	// waits to, by their keys.
	holders map[string]*holder
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
// ServeMembers stops. ready is closed once it knows how it is placed to
// serve the member's workers, which err says (status).
type holder struct {
	ready chan struct{}
	once  sync.Once
	mu    sync.Mutex
	err   error
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
// on, and reach it once it forwards their listener again.
func (c *Coordinator) ServeMembers(ctx context.Context, logs Logs) error {
	if err := c.writable(); err != nil {
		return err
	}
	srv, release, err := c.useServer(logs)
	if err != nil {
		return err
	}
	defer release()
	f := &forwarding{ctx: ctx, srv: srv, holders: map[string]*holder{}, lookups: lookups{}}
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
// couldn't find, it looks again only once l says to, at now.
func (c *Coordinator) forwardAll(ctx context.Context, l lookups, now time.Time) error {
	type found struct {
		run *store.Run
		job *store.Job
		a   *store.Attempt
	}
	var all []found
	err := c.store.View(ctx, func(tx *store.Tx) error {
		as, err := tx.MemberAttempts()
		if err != nil {
			return err
		}
		live := make(map[string]bool, len(as))
		for _, a := range as {
			live[a.ID] = true
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
		c.hold(fw, cand.member, f.a.Placement.MemberKey)
	}
	return errors.Join(errs...)
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
// will.
func (c *Coordinator) forward(ctx context.Context, fw backend.Forwarder, m *backend.Member) (backend.Endpoint, error) {
	key, err := fw.MemberKey(m)
	if err != nil {
		return backend.Endpoint{}, err
	}
	if h := c.hold(fw, m, key); h != nil {
		if err := h.status(ctx); err != nil {
			return backend.Endpoint{}, err
		}
	}
	return fw.Endpoint(ctx, m)
}

// hold returns the holder of the listener of m, whose key is key, which it
// starts if this process has none, while ServeMembers runs; or nil, where
// it doesn't.
func (c *Coordinator) hold(fw backend.Forwarder, m *backend.Member, key string) *holder {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.forwarding
	if f == nil || f.ctx.Err() != nil {
		return nil
	}
	if h := f.holders[key]; h != nil {
		return h
	}
	h := &holder{ready: make(chan struct{})}
	f.holders[key] = h
	f.wg.Go(func() {
		defer func() {
			c.mu.Lock()
			delete(f.holders, key)
			c.mu.Unlock()
		}()
		c.serveMember(f, h, fw, m, key)
	})
	return h
}

// serveMember holds m's listener, whose key is key, as h, until f's
// context is done: once it holds m's lease, which it says it waits for
// where another process holds it, it forwards m's listener, and serves
// the worker API on it, and forwards it again whenever it fails, or ends.
func (c *Coordinator) serveMember(f *forwarding, h *holder, fw backend.Forwarder, m *backend.Member, key string) {
	ctx := f.ctx
	if held, err := c.store.EndpointHeld(key); err == nil && held {
		// Another process serves m's workers, until it stops.
		h.report(nil)
	}
	lease, err := c.store.HoldEndpoint(ctx, key, "meridian, process "+strconv.Itoa(os.Getpid()))
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
