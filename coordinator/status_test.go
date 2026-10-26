package coordinator

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
)

// TestStatus checks a run's status through its life: created, then ended
// with its outcome, and a run the store doesn't have.
func TestStatus(t *testing.T) {
	c := open(t, Options{Variables: regionVars})
	if len(c.StoreID()) == 0 {
		t.Error("the store has no ID")
	}
	id, _ := create(t, c, regionWorkflow, regionSubmitted, regionVars)
	s, err := c.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if s.ID != id || s.State != RunCreated || s.RunDir != "/runs/"+id || s.Outcome != "" {
		t.Errorf("a run created is %+v", s)
	}
	serve(t, c)
	waitEnded(t, c, id)
	if s, err = c.Status(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if s.State != RunEnded || s.Outcome != plan.OutcomeSuccess || len(s.Diagnostics) != 0 {
		t.Errorf("a run ended is %+v", s)
	}
	for _, missing := range []string{"2", "x", ""} {
		if _, err := c.Status(t.Context(), missing); !errors.Is(err, ErrRunNotFound) {
			t.Errorf("Status(%q) = %v, want ErrRunNotFound", missing, err)
		}
	}
}

// TestHoldEndpoint checks that one coordinator of a store at a time holds
// an endpoint: another waits for it, and takes it once the first releases
// it.
func TestHoldEndpoint(t *testing.T) {
	db := filepath.Join(privateDir(t), "meridian.db")
	first := open(t, Options{Database: db})
	second := open(t, Options{Database: db})
	h, err := first.HoldEndpoint(t.Context(), "socket", "first")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := second.HoldEndpoint(ctx, "socket", "second"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a second hold of an endpoint held: %v", err)
	}
	took := make(chan error, 1)
	go func() {
		h2, err := second.HoldEndpoint(t.Context(), "socket", "second")
		if err == nil {
			err = h2.Release(t.Context())
		}
		took <- err
	}()
	if err := h.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-took:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the endpoint wasn't taken once its holder released it")
	}
}
