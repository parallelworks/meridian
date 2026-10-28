package coordinator

import (
	"errors"
	"strconv"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/plan"
)

// storedPlan returns a run id of p, as the store gives it.
func storedPlan(t testing.TB, id string, p *plan.Plan) *store.Run {
	t.Helper()
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := plan.Digest(p)
	if err != nil {
		t.Fatal(err)
	}
	return &store.Run{ID: id, Plan: data, PlanVersion: p.Version, PlanDigest: digest}
}

func testPlans(t *testing.T) (a, b *plan.Plan) {
	t.Helper()
	for i, p := range []**plan.Plan{&a, &b} {
		created, _, err := compile.Create(t.Context(), "w.yaml", []byte("jobs:\n  main:\n    steps:\n      - run: echo "+strconv.Itoa(i)+"\n"), nil, compile.Options{})
		if err != nil {
			t.Fatal(err)
		}
		*p = created.Plan
	}
	return a, b
}

func TestPlanCache(t *testing.T) {
	parallel(t)
	// A tree's runs fit, as their supervisors read them in turn.
	if planCacheCount < 1024 || planCacheBytes < 64<<20 {
		t.Errorf("the coordinator keeps %d plans, and %d bytes of them, at most: too few for a tree of runs", planCacheCount, planCacheBytes)
	}
	a, b := testPlans(t)
	runs := []*store.Run{storedPlan(t, "1", a), storedPlan(t, "2", a), storedPlan(t, "3", b)}

	c := newPlanCache(2, 1<<20)
	first, err := c.get(runs[0])
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := c.get(runs[0]); again != first {
		t.Error("the plan wasn't kept")
	}
	_, _ = c.get(runs[1])
	_, _ = c.get(runs[2])
	if _, ok := c.runs.Peek("1"); c.runs.Len() != 2 || ok {
		t.Errorf("the cache holds %d plans, run 1's among them %v, want the 2 last used", c.runs.Len(), ok)
	}
	if c.bytes != planSize(runs[1].Plan)+planSize(runs[2].Plan) {
		t.Errorf("the cache counts %d bytes", c.bytes)
	}

	// A run whose ID is another's, given again, with another plan, has
	// its own.
	other := storedPlan(t, "3", a)
	if got, err := c.get(other); err != nil {
		t.Fatal(err)
	} else if d, _ := plan.Digest(got); d != other.PlanDigest {
		t.Error("the cache gave the plan of the run that had the ID before")
	}

	// The bytes bound the cache too; a plan larger than it isn't kept,
	// and the full cache forgets none of its others for it.
	c = newPlanCache(10, planSize(runs[0].Plan))
	_, _ = c.get(runs[0])
	_, _ = c.get(runs[2])
	if _, ok := c.runs.Peek("3"); c.runs.Len() != 1 || !ok {
		t.Errorf("the cache holds %d plans, run 3's among them %v", c.runs.Len(), ok)
	}
	created, _, err := compile.Create(t.Context(), "w.yaml", []byte("jobs:\n  main:\n    steps:\n      - run: echo 0\n      - run: echo 1\n"), nil, compile.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if p, err := c.get(storedPlan(t, "4", created.Plan)); err != nil || p == nil || c.runs.Len() != 1 || c.bytes != planSize(runs[2].Plan) {
		t.Errorf("a plan larger than the cache: %v, %v, and the cache holds %d, of %d bytes", p, err, c.runs.Len(), c.bytes)
	}

	// An ended run's plan isn't kept: let go of as the store ends the run,
	// or as it is asked for with the run ended.
	c = newPlanCache(10, 1<<20)
	for _, r := range runs {
		_, _ = c.get(r)
	}
	c.forget("1", "no run")
	if _, ok := c.runs.Peek("1"); ok || c.runs.Len() != 2 || c.bytes != planSize(runs[1].Plan)+planSize(runs[2].Plan) {
		t.Errorf("forgetting run 1, the cache holds %d, of %d bytes", c.runs.Len(), c.bytes)
	}
	ended := *runs[1]
	ended.State = store.RunEnded
	if p, err := c.get(&ended); err != nil || p == nil {
		t.Errorf("ended run 2's plan: %v, %v", p, err)
	}
	if _, ok := c.runs.Peek("2"); ok || c.runs.Len() != 1 || c.bytes != planSize(runs[2].Plan) {
		t.Errorf("asked for ended run 2's plan, the cache holds %d, of %d bytes", c.runs.Len(), c.bytes)
	}
	_, _ = c.get(&ended)
	if c.runs.Len() != 1 {
		t.Error("an ended run's plan was kept")
	}

	// A plan of another version is told without decoding it, and one
	// that doesn't decode isn't kept.
	newer := &store.Run{ID: "4", Plan: []byte("not a plan"), PlanVersion: plan.Version + 1}
	if _, err := c.get(newer); !errors.Is(err, plan.ErrVersion) {
		t.Errorf("get of a newer plan = %v, want plan.ErrVersion", err)
	}
	bad := &store.Run{ID: "5", Plan: []byte(`{"version":1}`), PlanVersion: plan.Version}
	_, err = c.get(bad)
	if _, kept := c.runs.Peek("5"); !errors.Is(err, plan.ErrMalformed) || kept {
		t.Errorf("get of a plan that doesn't decode = %v", err)
	}
}

// TestPlanForgottenAtEnd checks that the coordinator doesn't keep a run's
// plan once the run has ended.
func TestPlanForgottenAtEnd(t *testing.T) {
	parallel(t)
	c := open(t, Options{})
	serve(t, c)
	id, _ := create(t, c, "jobs:\n  main:\n    steps:\n      - run: echo hi\n", nil, nil)
	waitEnded(t, c, id)
	waitFor(t, func() bool {
		c.plans.mu.Lock()
		defer c.plans.mu.Unlock()
		_, ok := c.plans.runs.Peek(id)
		return !ok
	})
}
