//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package flock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLockExcludes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.lock")
	unlock, err := Lock(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("the lock file is %v (%v), want one only its user can read", info.Mode(), err)
	}

	// A second lock on the file, of another open file description, waits
	// for the first, until its context ends.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := Lock(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lock while held = %v, want context.DeadlineExceeded", err)
	}

	got := make(chan error, 1)
	go func() {
		unlock, err := Lock(t.Context(), path)
		if err == nil {
			unlock()
		}
		got <- err
	}()
	unlock()
	if err := <-got; err != nil {
		t.Fatalf("Lock once released = %v", err)
	}
}

func TestLockRefusesLink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.lock")
	if err := os.Symlink(filepath.Join(dir, "elsewhere"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(t.Context(), path); err == nil {
		t.Fatal("Lock through a symbolic link succeeded")
	}
	if _, err := os.Lstat(filepath.Join(dir, "elsewhere")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Lock made the link's target: %v", err)
	}
}
