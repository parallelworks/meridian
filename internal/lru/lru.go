// Package lru keeps values by key in the order they were last used, so
// that a cache can forget the one used longest ago. It sets no bound of
// its own: each cache bounds what it keeps as it needs, by count, or by
// size, with Len and RemoveOldest. A Cache isn't safe for concurrent use:
// its user holds its own lock.
package lru

// A Cache holds values of type V by keys of type K, in the order they were
// last put or got. The zero Cache is empty and ready to use.
type Cache[K comparable, V any] struct {
	by map[K]*node[K, V]
	// oldest is the node used longest ago, and newest the one used last,
	// of a list linked from oldest by next.
	oldest, newest *node[K, V]
}

// A node is a key and its value, in a Cache's list.
type node[K comparable, V any] struct {
	key        K
	value      V
	prev, next *node[K, V]
}

// Put sets key's value, as the most recently used.
func (c *Cache[K, V]) Put(key K, value V) {
	if n, ok := c.by[key]; ok {
		n.value = value
		c.touch(n)
		return
	}
	if c.by == nil {
		c.by = map[K]*node[K, V]{}
	}
	n := &node[K, V]{key: key, value: value}
	c.by[key] = n
	c.push(n)
}

// Get returns key's value, and whether the cache has one, which it makes
// the most recently used.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	n, ok := c.by[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.touch(n)
	return n.value, true
}

// Peek returns key's value, and whether the cache has one, without making
// it the most recently used.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	n, ok := c.by[key]
	if !ok {
		var zero V
		return zero, false
	}
	return n.value, true
}

// Remove removes key, and returns its value, and whether the cache had
// one.
func (c *Cache[K, V]) Remove(key K) (V, bool) {
	n, ok := c.by[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.unlink(n)
	delete(c.by, key)
	return n.value, true
}

// RemoveOldest removes the value used longest ago, and returns its key and
// it, and false for an empty cache.
func (c *Cache[K, V]) RemoveOldest() (K, V, bool) {
	n := c.oldest
	if n == nil {
		var (
			key   K
			value V
		)
		return key, value, false
	}
	c.unlink(n)
	delete(c.by, n.key)
	return n.key, n.value, true
}

// Len returns how many values the cache holds.
func (c *Cache[K, V]) Len() int { return len(c.by) }

// touch makes n the most recently used.
func (c *Cache[K, V]) touch(n *node[K, V]) {
	if c.newest != n {
		c.unlink(n)
		c.push(n)
	}
}

// push puts n, which no list holds, last.
func (c *Cache[K, V]) push(n *node[K, V]) {
	n.prev, n.next = c.newest, nil
	if c.newest != nil {
		c.newest.next = n
	} else {
		c.oldest = n
	}
	c.newest = n
}

// unlink takes n out of the list.
func (c *Cache[K, V]) unlink(n *node[K, V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		c.oldest = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		c.newest = n.prev
	}
	n.prev, n.next = nil, nil
}
