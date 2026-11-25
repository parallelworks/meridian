package fileid

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCache checks that a cache gives the digest it kept for the file
// whose ID it was kept with, and for no other; and keeps none of a file
// whose status changed less than settled before.
func TestCache(t *testing.T) {
	dir := t.TempDir()
	id := func(name string) ID {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		id, ok := Of(info)
		if !ok {
			t.Skip("a file's ID is known only on Linux")
		}
		return id
	}
	a, b := id("a"), id("b")
	var c Cache
	if _, ok := c.Get(a); ok {
		t.Error("an empty cache gave a digest")
	}
	c.Keep(a, "sha256:aa", time.Hour)
	if _, ok := c.Get(a); ok {
		t.Error("the digest of a file whose status just changed was kept")
	}
	c.Keep(a, "sha256:aa", 0)
	if got, ok := c.Get(a); !ok || got != "sha256:aa" {
		t.Errorf("the cache gave %q, %v", got, ok)
	}
	if _, ok := c.Get(b); ok {
		t.Error("the cache gave another file's digest")
	}
	c.Keep(b, "sha256:bb", 0)
	if _, ok := c.Get(a); ok {
		t.Error("the cache kept two digests")
	}
}
