package lru

import (
	"slices"
	"testing"
)

// order returns c's keys, the one used longest ago first, in the order
// RemoveOldest would take them.
func order(c *Cache[string, int]) []string {
	var keys []string
	for n := c.oldest; n != nil; n = n.next {
		keys = append(keys, n.key)
	}
	return keys
}

// A Cache keeps its values in the order they were last put or got: Peek
// doesn't change it, Remove and RemoveOldest take a value out, and the
// zero Cache is empty and ready to use.
func TestCache(t *testing.T) {
	var c Cache[string, int]
	if _, ok := c.Get("a"); ok || c.Len() != 0 {
		t.Fatal("the zero Cache isn't empty")
	}
	if _, _, ok := c.RemoveOldest(); ok {
		t.Fatal("an empty Cache removed something")
	}
	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("c", 3)
	if got := order(&c); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("the order is %q", got)
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Errorf("Get(a) = %d, %v", v, ok)
	}
	if v, ok := c.Peek("b"); !ok || v != 2 {
		t.Errorf("Peek(b) = %d, %v", v, ok)
	}
	if got := order(&c); !slices.Equal(got, []string{"b", "c", "a"}) {
		t.Errorf("after Get(a) and Peek(b), the order is %q", got)
	}
	c.Put("b", 20)
	if got := order(&c); !slices.Equal(got, []string{"c", "a", "b"}) {
		t.Errorf("after Put(b), the order is %q", got)
	}
	if v, ok := c.Remove("a"); !ok || v != 1 {
		t.Errorf("Remove(a) = %d, %v", v, ok)
	}
	if _, ok := c.Remove("a"); ok {
		t.Error("Remove(a) again removed something")
	}
	if k, v, ok := c.RemoveOldest(); !ok || k != "c" || v != 3 {
		t.Errorf("RemoveOldest() = %s, %d, %v", k, v, ok)
	}
	if k, v, ok := c.RemoveOldest(); !ok || k != "b" || v != 20 || c.Len() != 0 || c.oldest != nil || c.newest != nil {
		t.Errorf("RemoveOldest() = %s, %d, %v, leaving %d", k, v, ok, c.Len())
	}
	c.Put("d", 4)
	if got := order(&c); !slices.Equal(got, []string{"d"}) || c.Len() != 1 {
		t.Errorf("after emptying and Put(d), the order is %q", got)
	}
}
