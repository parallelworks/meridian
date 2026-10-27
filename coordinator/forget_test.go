package coordinator

import (
	"strconv"
	"testing"

	"github.com/parallelworks/meridian/backend"
)

// TestRememberEvicts checks that the coordinator keeps where maxMembers
// attempts were placed at most, and lets only the one it has kept longest
// go to keep another; that keeping one again doesn't count it twice; and
// that one it forgets leaves no trace.
func TestRememberEvicts(t *testing.T) {
	c := open(t, Options{})
	cand := &candidate{}
	for i := range maxMembers + 1 {
		c.remember(strconv.Itoa(i), cand, backend.Placement{Attempt: strconv.Itoa(i)})
	}
	c.remember("1", cand, backend.Placement{Attempt: "1"})
	kept := func(id string) bool {
		_, ok := c.placed[id]
		return ok
	}
	if kept("0") || !kept("1") || !kept(strconv.Itoa(maxMembers)) || len(c.placed) != maxMembers || len(c.placedOrder) != maxMembers {
		t.Errorf("past maxMembers, the coordinator keeps %d placements, in an order of %d", len(c.placed), len(c.placedOrder))
	}
	c.forgetLater("1")
	if kept("1") || len(c.placed) != maxMembers-1 || len(c.placedOrder) != maxMembers-1 {
		t.Errorf("forgotten, the coordinator keeps %d placements, in an order of %d", len(c.placed), len(c.placedOrder))
	}
}
