package coordinator

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// The plan cache's bounds: how many plans it holds, and how many bytes of
// their encodings, at most.
const (
	planCacheCount = 64
	planCacheBytes = 256 << 20
)

// A planCache holds the plans of the runs a coordinator works on, decoded,
// the most recently used first, so that each of a run's steps doesn't
// decode its plan again. A plan never changes once its run is stored, and
// nothing changes a decoded one, so the plans it gives are shared. It is
// keyed by run ID, and an entry is used only for the run whose plan has
// its digest: a run's ID can be given again to another run, after the
// transaction that first gave it rolls back.
type planCache struct {
	maxCount int
	maxBytes int64

	mu    sync.Mutex
	order *list.List // of *planEntry, the most recently used first
	runs  map[string]*list.Element
	bytes int64
}

type planEntry struct {
	run, digest string
	plan        *plan.Plan
	size        int64
}

func newPlanCache(maxCount int, maxBytes int64) *planCache {
	return &planCache{maxCount: maxCount, maxBytes: maxBytes, order: list.New(), runs: map[string]*list.Element{}}
}

// get returns run's plan, decoded: from the cache, or by decoding it, which
// the cache then keeps, unless it is larger than the cache. A plan of a
// version this release doesn't read is plan.ErrVersion, which is told
// without decoding it, and one that doesn't decode is what plan.Decode
// says.
func (c *planCache) get(run *store.Run) (*plan.Plan, error) {
	if run.PlanVersion > plan.Version {
		return nil, fmt.Errorf("%w %d of run %s's plan; this release reads versions up to %d", plan.ErrVersion, run.PlanVersion, run.ID, plan.Version)
	}
	c.mu.Lock()
	if e, ok := c.runs[run.ID]; ok {
		if entry := e.Value.(*planEntry); entry.digest == run.PlanDigest {
			c.order.MoveToFront(e)
			c.mu.Unlock()
			return entry.plan, nil
		}
		c.remove(e)
	}
	c.mu.Unlock()

	p, err := plan.Decode(run.Plan)
	if err != nil {
		return nil, err
	}
	size := int64(len(run.Plan))
	if size > c.maxBytes {
		return p, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.runs[run.ID]; ok {
		// Another call decoded it meanwhile.
		c.remove(e)
	}
	c.runs[run.ID] = c.order.PushFront(&planEntry{run: run.ID, digest: run.PlanDigest, plan: p, size: size})
	c.bytes += size
	for c.order.Len() > c.maxCount || c.bytes > c.maxBytes {
		c.remove(c.order.Back())
	}
	return p, nil
}

// remove removes the entry e. The cache's lock is held.
func (c *planCache) remove(e *list.Element) {
	entry := c.order.Remove(e).(*planEntry)
	delete(c.runs, entry.run)
	c.bytes -= entry.size
}
