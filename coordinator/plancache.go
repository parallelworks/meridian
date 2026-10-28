package coordinator

import (
	"fmt"
	"sync"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/lru"
	"github.com/parallelworks/meridian/plan"
)

// The plan cache's bounds: how many plans it holds, and how many bytes of
// memory they hold, by planSize's estimate, at most. A run's tree, whose
// every step that calls a workflow makes a run of its own, with a plan of
// its own, is as many plans as it has runs that call one, each read by its
// supervisors' looks: the bytes bound the cache, and the count only what
// a tree of small plans keeps beside them. It and the store's cache of
// start states, of 64 MiB, hold 256 MiB at most between them: a run's
// plan is commonly larger than its start state.
const (
	planCacheCount = 4096
	planCacheBytes = 192 << 20
)

// planSize is the memory a plan encoded as data holds once decoded,
// estimated from above: commonly 3 to 11 times as much as data, but as
// much as 42 times for a matrix that includes many small objects, so no
// factor on data's length would do.
func planSize(data []byte) int64 { return jsonvalue.Memory(data) }

// A planCache holds the plans of the runs a coordinator works on, decoded,
// the most recently used first, so that each of a run's steps doesn't
// decode its plan again. A plan never changes once its run is stored, and
// nothing changes a decoded one, so the plans it gives are shared. It is
// keyed by run ID, and an entry is used only for the run whose plan has
// its digest: a run's ID can be given again to another run, after the
// transaction that first gave it rolls back. A step run uses its caller's
// entry, which no run needs once the caller has ended. The cache
// doesn't keep an ended run's plan, which is read seldom, if at all: it
// lets one go as the store commits the run's end (forget), and as it is
// asked for a run that has ended.
type planCache struct {
	maxCount int
	maxBytes int64

	mu    sync.Mutex
	runs  lru.Cache[string, planEntry] // by run ID
	bytes int64
}

type planEntry struct {
	digest string
	plan   *plan.Plan
	size   int64
}

func newPlanCache(maxCount int, maxBytes int64) *planCache {
	return &planCache{maxCount: maxCount, maxBytes: maxBytes}
}

// get returns run's plan, decoded: from the cache, or by decoding it, which
// the cache then keeps, unless it is larger than the cache or its run has
// ended. A plan of a version this release doesn't read is plan.ErrVersion,
// which is told without decoding it, and one that doesn't decode is what
// plan.Decode says.
func (c *planCache) get(run *store.Run) (*plan.Plan, error) {
	if run.PlanVersion > plan.Version {
		return nil, fmt.Errorf("%w %d of run %s's plan; this release reads versions up to %d", plan.ErrVersion, run.PlanVersion, run.ID, plan.Version)
	}
	// A step run's plan is its caller's, which it shares, and whose end
	// it doesn't tell.
	key, ended := run.ID, run.State == store.RunEnded
	if run.StepRun {
		key, ended = run.Parent.Run, false
	}
	c.mu.Lock()
	if entry, ok := c.runs.Get(key); ok {
		if ended || entry.digest != run.PlanDigest {
			c.remove(key)
		}
		if entry.digest == run.PlanDigest {
			c.mu.Unlock()
			return entry.plan, nil
		}
	}
	c.mu.Unlock()

	p, err := plan.Decode(run.Plan)
	if err != nil {
		return nil, err
	}
	size := planSize(run.Plan)
	if ended || size > c.maxBytes {
		return p, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Another call may have decoded it meanwhile.
	c.remove(key)
	c.runs.Put(key, planEntry{digest: run.PlanDigest, plan: p, size: size})
	c.bytes += size
	for c.runs.Len() > c.maxCount || c.bytes > c.maxBytes {
		_, entry, _ := c.runs.RemoveOldest()
		c.bytes -= entry.size
	}
	return p, nil
}

// forget lets go of the plans of the runs ids, which have ended.
func (c *planCache) forget(ids ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		c.remove(id)
	}
}

// remove removes the entry of the run ID run, if there is one. The cache's
// lock is held.
func (c *planCache) remove(run string) {
	if entry, ok := c.runs.Remove(run); ok {
		c.bytes -= entry.size
	}
}
