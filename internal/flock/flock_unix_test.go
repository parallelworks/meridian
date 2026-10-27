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
	if _, err := TryLock(path); err == nil || errors.Is(err, ErrHeld) {
		t.Fatalf("TryLock through a symbolic link = %v", err)
	}
}

// TryLock takes a lock no one holds at once, refuses one another holds,
// as Lock waits for it, and removes the file once it releases it.
func TestTryLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.lock")
	release, err := TryLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("the lock file is %v (%v), want one only its user can read", info.Mode(), err)
	}
	if _, err := TryLock(path); !errors.Is(err, ErrHeld) {
		t.Errorf("TryLock while held = %v, want ErrHeld", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := Lock(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Lock while TryLock holds = %v, want context.DeadlineExceeded", err)
	}
	release()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the lock file is there once released: %v", err)
	}
	again, err := TryLock(path)
	if err != nil {
		t.Fatalf("TryLock once released = %v", err)
	}
	again()
}
