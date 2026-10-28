package workerapisrv

import (
	"testing"
	"time"
)

// A server lets go the budget of an attempt whose end it stored, and, once
// budgetIdle has passed since it last did, those no upload has spent from
// for as long; it keeps the rest.
func TestLogBudgetsLetGo(t *testing.T) {
	t.Parallel()
	var b logBudgets
	idle, busy, ended := b.of("idle"), b.of("busy"), b.of("ended")
	b.drop("ended")
	if b.of("ended") == ended {
		t.Error("the budget of an attempt whose end was stored was kept")
	}
	b.mu.Lock()
	ago := time.Now().Add(-budgetIdle)
	b.swept, idle.used = ago, ago
	b.mu.Unlock()
	b.of("another")
	if b.of("idle") == idle {
		t.Error("a budget idle for budgetIdle was kept")
	}
	if b.of("busy") != busy {
		t.Error("a budget in use was let go")
	}
}
