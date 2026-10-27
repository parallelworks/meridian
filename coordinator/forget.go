package coordinator

import (
	"context"
	"slices"
	"time"

	"github.com/parallelworks/meridian/backend"
)

// A placed is where an attempt was placed, as its supervisor recorded it:
// what the coordinator keeps, until the attempt has ended, to have its
// backend forget the placement then (forgetLater).
type placed struct {
	cand      *candidate
	placement backend.Placement
}

// A worker exits soon after the coordinator has stored its attempt's end,
// which it waits for: the coordinator asks its backend to forget the
// placement again while it says the worker still runs, forgetFirst after
// the first time, twice as long each time after, up to forgetMost, for
// forgetFor in all.
const (
	forgetFirst = 50 * time.Millisecond
	forgetMost  = time.Second
	forgetFor   = time.Minute
)

// forgetGrace is how long Serve, as it stops, waits for the placements it
// is having its backends forget.
const forgetGrace = 2 * time.Second

// remember keeps where the attempt id was placed, on cand, as p, until the
// attempt has ended (forgetLater). It keeps maxMembers at most, and lets
// the one it has kept longest go to keep another: a placement it doesn't
// know of, as that one, one that another coordinator recorded, or one
// recorded before this one started, its backend removes itself in time,
// as the local backend does a day after its worker ended.
func (c *Coordinator) remember(id string, cand *candidate, p backend.Placement) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.placed[id]; !ok {
		if len(c.placed) >= maxMembers {
			delete(c.placed, c.placedOrder[0])
			c.placedOrder = c.placedOrder[1:]
		}
		c.placedOrder = append(c.placedOrder, id)
	}
	c.placed[id] = placed{cand: cand, placement: p}
}

// unremember lets the coordinator's record of the attempt id's placement
// go. Its caller holds c.mu.
func (c *Coordinator) unremember(id string) {
	if _, ok := c.placed[id]; !ok {
		return
	}
	delete(c.placed, id)
	if i := slices.Index(c.placedOrder, id); i >= 0 {
		c.placedOrder = slices.Delete(c.placedOrder, i, i+1)
	}
}

// forgetLater has the backend of the attempt id, which has ended, forget
// its placement, if the coordinator recorded it (remember): what the
// backend keeps of it, such as the local backend's files, so that what it
// keeps doesn't grow. Nothing asks the backend about an attempt that has
// ended, nor withdraws it.
func (c *Coordinator) forgetLater(id string) {
	c.mu.Lock()
	pl, ok := c.placed[id]
	c.unremember(id)
	c.mu.Unlock()
	if ok {
		c.forget(pl.cand, pl.placement)
	}
}

// forget has cand's driver forget the placement p, in the background,
// while Serve runs, and for a while as it stops (stopForgetting): once its
// worker has exited, asking again while the driver says it still runs.
func (c *Coordinator) forget(cand *candidate, p backend.Placement) {
	c.mu.Lock()
	ctx := c.forgetCtx
	if ctx != nil {
		c.forgetting.Add(1)
	}
	c.mu.Unlock()
	if ctx == nil {
		return
	}
	go func() {
		defer c.forgetting.Done()
		deadline := time.Now().Add(forgetFor)
		for wait := forgetFirst; ; wait = min(2*wait, forgetMost) {
			if cand.driver.Forget(ctx, cand.member, p) == nil {
				return
			}
			if time.Now().Add(wait).After(deadline) {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
		c.log.DebugContext(ctx, "coordinator: a backend didn't forget a placement whose attempt ended", "attempt", p.Attempt)
	}()
}

// stopForgetting waits, until ctx is done, for the placements the
// coordinator is having its backends forget, whose workers exit soon after
// their attempts end, then stops what is left: those, their backends
// remove in time.
func (c *Coordinator) stopForgetting(ctx context.Context, cancel context.CancelFunc) {
	c.mu.Lock()
	c.forgetCtx = nil
	c.mu.Unlock()
	waitGroup(ctx, &c.forgetting)
	cancel()
	c.forgetting.Wait()
}
