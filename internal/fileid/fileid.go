// Package fileid tells which file a file is, and what it holds, as far as
// fstat says (ID), and keeps the digest of one file by it (Cache), so that
// a binary of tens of megabytes isn't hashed again unless it changed.
package fileid

import (
	"sync"
	"time"
)

// An ID is which file a file is, and what it holds, as far as fstat says:
// its device and inode, its size, and when its data and its status last
// changed, in nanoseconds.
type ID struct {
	dev, ino           uint64
	size, mtime, ctime int64
}

// A Cache keeps one digest, of the file whose ID it was kept with. Writing
// a file changes when its data and its status last changed, and a process
// can set its data's back (utimes), but not its status's, which any change
// sets to the time it happens, utimes among them: a file whose ID is the
// same holds what it held. Its zero value keeps none.
type Cache struct {
	mu     sync.Mutex
	id     ID
	digest string
}

// Get returns the digest kept of the file whose ID is id, and whether one
// is.
func (c *Cache) Get(id ID) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.digest == "" || c.id != id {
		return "", false
	}
	return c.digest, true
}

// Keep keeps digest, of the file whose ID is id, in place of what the
// cache kept, where the file's status last changed settled or more before
// now; and otherwise keeps nothing new, since a change within the clock's
// tick after could leave the time as it was.
func (c *Cache) Keep(id ID, digest string, settled time.Duration) {
	if time.Since(time.Unix(0, id.ctime)) < settled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id, c.digest = id, digest
}
