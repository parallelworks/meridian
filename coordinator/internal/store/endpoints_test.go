package store

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// skipWithoutFileLocks skips a test of endpoints on a system without file
// locks, where HoldEndpoint fails.
func skipWithoutFileLocks(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux", "darwin", "freebsd", "netbsd", "openbsd", "dragonfly":
	default:
		t.Skip("no file locks on " + runtime.GOOS)
	}
}

func TestHoldEndpoint(t *testing.T) {
	skipWithoutFileLocks(t)
	s := open(t)
	h, err := s.HoldEndpoint(t.Context(), "socket", "pid 1")
	if err != nil {
		t.Fatal(err)
	}
	e := h.Endpoint()
	if e.Name != "socket" || e.Holder != "pid 1" || e.TakenAt.IsZero() || !e.GraceFrom.Equal(e.TakenAt) {
		t.Errorf("the endpoint held is %+v", e)
	}
	inTx(t, s, func(tx *Tx) error {
		got, err := tx.Endpoint("socket")
		if err != nil || *got != e {
			t.Errorf("Endpoint = %+v, %v; want %+v", got, err, e)
		}
		if _, err := tx.Endpoint("other"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Endpoint of no endpoint = %v, want ErrNotFound", err)
		}
		if err := tx.RestartGrace("other"); !errors.Is(err, ErrNotFound) {
			t.Errorf("RestartGrace of no endpoint = %v, want ErrNotFound", err)
		}
		// The grace began a second ago, and restarts now.
		if _, err := tx.tx.ExecContext(tx.ctx, "UPDATE meridian_endpoints SET grace_from = grace_from - 1000000"); err != nil {
			return err
		}
		return tx.RestartGrace("socket")
	})
	inTx(t, s, func(tx *Tx) error {
		got, err := tx.Endpoint("socket")
		if err != nil {
			return err
		}
		if !got.TakenAt.Equal(e.TakenAt) || got.GraceFrom.Before(e.GraceFrom) {
			t.Errorf("once the grace restarted, the endpoint is %+v, was %+v", got, e)
		}
		return nil
	})

	// Another store of the same database, as another process's, waits for
	// the endpoint while it is held.
	other := openAt(t, s.file)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := other.HoldEndpoint(ctx, "socket", "pid 2"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("HoldEndpoint while held = %v, want context.DeadlineExceeded", err)
	}
	if h2, err := other.HoldEndpoint(t.Context(), "another", "pid 2"); err != nil {
		t.Errorf("HoldEndpoint of another endpoint = %v", err)
	} else if err := h2.Release(t.Context()); err != nil {
		t.Error(err)
	}

	taken := make(chan *Hold, 1)
	go func() {
		h, err := other.HoldEndpoint(t.Context(), "socket", "pid 2")
		if err != nil {
			t.Error(err)
		}
		taken <- h
	}()
	if err := h.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	h2 := <-taken
	if h2 == nil {
		return
	}
	if got := h2.Endpoint(); got.Holder != "pid 2" || got.TakenAt.Before(e.TakenAt) || !got.GraceFrom.Equal(got.TakenAt) {
		t.Errorf("the endpoint taken over is %+v", got)
	}
	if err := h2.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	inTx(t, s, func(tx *Tx) error {
		if _, err := tx.Endpoint("socket"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Endpoint once released = %v, want ErrNotFound", err)
		}
		return nil
	})

	for _, name := range []string{"", "Socket", "a/b", "../x", "a.b"} {
		if _, err := s.HoldEndpoint(t.Context(), name, "pid 1"); err == nil {
			t.Errorf("HoldEndpoint(%q) succeeded", name)
		}
	}
	if _, err := s.HoldEndpoint(t.Context(), "socket", ""); err == nil {
		t.Error("HoldEndpoint without a holder succeeded")
	}
}
