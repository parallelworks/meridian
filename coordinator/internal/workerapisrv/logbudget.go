package workerapisrv

import (
	"context"
	"sync"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/store"
)

// logReserve is how many bytes of an attempt's limit on its logs a server
// reserves at a time, in one write (store.Tx.AddLogBytes), to spend as
// uploads come, in memory: a step that prints a line a millisecond would
// otherwise take a write, and the store's lock, for each of its uploads.
const logReserve = 1 << 20

// budgetIdle is how long a server keeps what it reserved of an attempt's
// limit while none of the attempt's logs is uploaded: an attempt whose end
// it didn't store, as one that was cancelled, whose lease ran out, or that
// another replica served, uploads no more.
const budgetIdle = 10 * time.Minute

// logBudgets are what a server has reserved of attempts' limits on their
// logs, and not spent, by attempt; and when it last let go those idle for
// budgetIdle, swept.
type logBudgets struct {
	mu    sync.Mutex
	m     map[string]*logBudget
	swept time.Time
}

// A logBudget is what a server has reserved of an attempt's limit on its
// logs, and not spent: left bytes; whether the store has none of the limit
// left to reserve, drained; and when an upload last spent from it, used,
// which logBudgets.mu guards. mu orders the attempt's uploads' spending,
// and its reservations.
type logBudget struct {
	used    time.Time
	mu      sync.Mutex
	left    int64
	drained bool
}

// of returns the attempt's budget, a new one if the server has none, and
// lets go, at most once in budgetIdle, the budgets idle for as long.
func (b *logBudgets) of(attempt string) *logBudget {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Sub(b.swept) >= budgetIdle {
		for id, lb := range b.m {
			if now.Sub(lb.used) >= budgetIdle {
				delete(b.m, id)
			}
		}
		b.swept = now
	}
	lb := b.m[attempt]
	if lb == nil {
		if b.m == nil {
			b.m = map[string]*logBudget{}
		}
		lb = &logBudget{}
		b.m[attempt] = lb
	}
	lb.used = now
	return lb
}

// drop lets the attempt's budget go, once its end is stored, and no log of
// it is uploaded more.
func (b *logBudgets) drop(attempt string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.m, attempt)
}

// spent reports whether the attempt's limit on its logs has run out: the
// store has none of it left to reserve, and the server has spent what it
// reserved.
func (lb *logBudget) spent() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.drained && lb.left == 0
}

// spendLogBytes spends n bytes of the attempt's limit on its logs from its
// budget, lb, and returns how many of them it may keep: n, or fewer, down
// to none, once the limit has run out. When lb runs short, it reserves
// more, s.logReserve or what n lacks if that is more, in one write. The
// store counts what every server reserves, and a server is given only what
// the limit has left past that, so the bytes the attempt's logs keep, over
// every server that serves its uploads, as after a takeover, never pass the
// limit; what a server reserved and didn't spend goes unused.
func (s *Server) spendLogBytes(ctx context.Context, lb *logBudget, attempt string, n int64) (int64, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.left < n && !lb.drained {
		ask := max(n-lb.left, s.logReserve)
		var counted int64
		if err := s.store.Tx(ctx, func(tx *store.Tx) error {
			var err error
			counted, err = tx.AddLogBytes(attempt, ask)
			return err
		}); err != nil {
			return 0, err
		}
		given := min(ask, max(0, s.maxLogBytes-(counted-ask)))
		lb.left += given
		lb.drained = given < ask
	}
	keep := min(n, lb.left)
	lb.left -= keep
	return keep, nil
}
