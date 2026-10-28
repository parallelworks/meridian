//go:build unix

package safepath

import (
	"os"
	"path/filepath"
	"testing"
)

// WriteFile writes a file readable only by the user, replaces one that is
// there whole, and leaves nothing beside it: neither when it succeeds, nor
// when it can't rename its new file over the one it writes.
func TestWriteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "record")
	for _, data := range []string{"first", "second, longer"} {
		if err := WriteFile(path, []byte(data)); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != data {
			t.Fatalf("the file holds %q, %v, want %q", got, err, data)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Errorf("the file's mode is %v, %v", info.Mode(), err)
		}
	}
	// A directory where the file would go can't be renamed over.
	blocked := filepath.Join(dir, "blocked")
	if err := os.Mkdir(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "in"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(blocked, []byte("x")); err == nil {
		t.Error("WriteFile wrote over a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("the directory holds %q, want the file and the directory alone", names)
	}
	if err := WriteFile(filepath.Join(dir, "missing", "record"), nil); err == nil {
		t.Error("WriteFile wrote in a directory that isn't there")
	}
}
