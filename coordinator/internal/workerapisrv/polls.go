package workerapisrv

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/store"
)

// recheck is how often the server reads again the controls of the attempts
// it holds polls for, for one another process stored, whose Notify doesn't
// reach this one.
const recheck = time.Second

// polls are the control polls a server holds, by attempt: each attempt's
// wait on a channel that is closed, and replaced, once its control may be
// newer, by Notify or by the recheck. One recheck, while any poll waits,
// reads the controls' versions of every attempt polled for at once.
type polls struct {
	store *store.Store
	log   *slog.Logger

	mu       sync.Mutex
	attempts map[string]*polled
	checking bool
}

// polled is what the server holds of an attempt's polls: how many wait,
// the channel they wait on, and the newest version of its control they
// read.
type polled struct {
	waiting int
	changed chan struct{}
	version int64
}

func newPolls(s *store.Store, log *slog.Logger) *polls {
	return &polls{store: s, log: log, attempts: map[string]*polled{}}
}

// watch registers a poll of the attempt id, which read its control at
// version, and returns a channel closed once the control may be newer, and
// the function that ends the poll's wait.
func (p *polls) watch(ctx context.Context, id string, version int64) (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	a := p.attempts[id]
	if a == nil {
		a = &polled{changed: make(chan struct{}), version: version}
		p.attempts[id] = a
	}
	a.waiting++
	a.version = max(a.version, version)
	if !p.checking {
		p.checking = true
		// The recheck outlives the poll that starts it: its context
		// keeps the poll's values, but not its end.
		go p.recheck(context.WithoutCancel(ctx))
	}
	var once sync.Once
	return a.changed, func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if a.waiting--; a.waiting == 0 {
				delete(p.attempts, id)
			}
		})
	}
}

// wake wakes the polls of the attempts ids, or of every attempt for none.
func (p *polls) wake(ids []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(ids) == 0 {
		ids = slices.Collect(maps.Keys(p.attempts))
	}
	for _, id := range ids {
		if a := p.attempts[id]; a != nil {
			close(a.changed)
			a.changed = make(chan struct{})
		}
	}
}

// recheck reads again, every second while any poll waits, the versions of
// the controls polled for, and wakes the polls of those that are newer.
func (p *polls) recheck(ctx context.Context) {
	tick := time.NewTicker(recheck)
	defer tick.Stop()
	for range tick.C {
		p.mu.Lock()
		if len(p.attempts) == 0 {
			p.checking = false
			p.mu.Unlock()
			return
		}
		ids := slices.Collect(maps.Keys(p.attempts))
		p.mu.Unlock()
		var versions map[string]int64
		err := p.store.View(ctx, func(tx *store.Tx) error {
			var err error
			versions, err = tx.ControlVersions(ids)
			return err
		})
		if err != nil {
			p.log.Warn("workerapisrv: read the controls of the attempts polled for", "error", err)
			continue
		}
		p.mu.Lock()
		for id, v := range versions {
			if a := p.attempts[id]; a != nil && v > a.version {
				a.version = v
				close(a.changed)
				a.changed = make(chan struct{})
			}
		}
		p.mu.Unlock()
	}
}
