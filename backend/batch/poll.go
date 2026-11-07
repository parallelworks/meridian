package batch

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/meridian/backend"
)

// A poller keeps a snapshot of what the scheduler says of every job the
// driver watches, so that however many attempts the coordinator checks,
// the cluster is asked at most once each timing.poll, by one caller at a
// time.
type poller struct {
	d *Driver

	mu sync.Mutex
	// live are the jobs watched, by ID.
	live map[string]*watch
	snap snapshot
	// running is closed once the refresh in flight ends, or nil.
	running chan struct{}
}

// A watch is a job the driver watches.
type watch struct {
	// sub is its submission's directory, or "".
	sub string
	// asked is when a caller last asked about it.
	asked time.Time
	// ended is when a check first found it ended without the records that
	// say how, and stuck when one first found it pending for a reason
	// that won't clear, from which its grace is counted.
	ended, stuck time.Time
}

// A snapshot is what the scheduler said of the jobs it covers, at at.
type snapshot struct {
	at      time.Time
	covered map[string]bool
	// jobs are those the queue, or accounting, lists.
	jobs map[string]JobStatus
	err  error
}

// watch has the poller watch the job id, of the submission sub.
func (p *poller) watch(id, sub string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := p.live[id]
	if w == nil {
		w = &watch{}
		p.live[id] = w
	}
	if sub != "" {
		w.sub = sub
	}
	w.asked = time.Now()
}

// forget stops watching the job id.
func (p *poller) forget(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.live, id)
}

// stuckFor reports whether the job id, which a check finds stuck or not,
// has been found pending for a reason that won't clear for timing.stuck,
// counted from the first check that found it so since one that didn't.
func (p *poller) stuckFor(id string, stuck bool) bool {
	if !stuck {
		p.mu.Lock()
		defer p.mu.Unlock()
		if w := p.live[id]; w != nil {
			w.stuck = time.Time{}
		}
		return false
	}
	return p.over(id, p.d.timing.stuck, func(w *watch) *time.Time { return &w.stuck })
}

// graceOver reports whether the job id has been found ended without its
// records for timing.grace, counted from the first time this asks.
func (p *poller) graceOver(id string) bool {
	return p.over(id, p.d.timing.grace, func(w *watch) *time.Time { return &w.ended })
}

// over reports whether d has passed since the time of the job id's that
// since gives, which the first call sets.
func (p *poller) over(id string, d time.Duration, since func(*watch) *time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := p.live[id]
	if w == nil {
		return true
	}
	now, t := p.d.timing.now(), since(w)
	if t.IsZero() {
		*t = now
	}
	return now.Sub(*t) >= d
}

// subOf returns the directory of the job id's submission, as the poller
// keeps it, or "".
func (p *poller) subOf(id string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w := p.live[id]; w != nil {
		return w.sub
	}
	return ""
}

// status returns what the scheduler says of the job id, of the submission
// sub, from a snapshot that covers it, at most timing.poll old or made
// since it was asked, and false where neither the queue nor accounting
// lists it. It waits for the snapshot: one that a caller's refresh, or the
// next once the current one is old, makes.
func (p *poller) status(ctx context.Context, id, sub string) (JobStatus, bool, error) {
	asked := time.Now()
	p.watch(id, sub)
	for {
		p.mu.Lock()
		s := p.snap
		fresh := !s.at.IsZero() && time.Since(s.at) < p.d.timing.poll
		// A refresh slower than timing.poll gives a snapshot that is old
		// already: it is the newest there is.
		if s.covered[id] && (fresh || !s.at.Before(asked)) {
			p.mu.Unlock()
			if s.err != nil {
				return JobStatus{}, false, s.err
			}
			st, ok := s.jobs[id]
			return st, ok, nil
		}
		wait := p.running
		if wait == nil && !fresh {
			wait = make(chan struct{})
			p.running = wait
			go p.refresh(ctx, wait)
		}
		p.mu.Unlock()
		if err := p.await(ctx, wait, s.at.Add(p.d.timing.poll)); err != nil {
			return JobStatus{}, false, err
		}
	}
}

// await waits for the refresh wait to end, or, where none runs, as for a
// snapshot that doesn't cover a job, made too lately to make another,
// until next, when the next can be made.
func (p *poller) await(ctx context.Context, wait chan struct{}, next time.Time) error {
	var timer <-chan time.Time
	if wait == nil {
		t := time.NewTimer(time.Until(next))
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-wait:
	case <-timer:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// refresh makes a snapshot of every job watched, and closes done. A job
// nobody has asked about for timing.idle is no longer watched. It goes on
// if the caller that started it stops waiting, for the others.
func (p *poller) refresh(ctx context.Context, done chan struct{}) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.d.timing.refresh)
	defer cancel()
	now := time.Now()
	p.mu.Lock()
	subs := make(map[string]string, len(p.live))
	for id, w := range p.live {
		if now.Sub(w.asked) > p.d.timing.idle {
			delete(p.live, id)
			continue
		}
		subs[id] = w.sub
	}
	p.mu.Unlock()
	ids := slices.Sorted(maps.Keys(subs))
	s := snapshot{at: now, covered: make(map[string]bool, len(ids)), jobs: map[string]JobStatus{}}
	for _, id := range ids {
		s.covered[id] = true
	}
	s.err = p.collect(ctx, ids, subs, s.jobs)
	p.mu.Lock()
	p.snap = s
	p.running = nil
	p.mu.Unlock()
	close(done)
}

// collect asks the queue about ids, in chunks, then accounting about
// those the queue no longer lists whose records don't say how they ended.
// Accounting's word on a job the queue no longer lists counts only where
// it has ended: one it holds as pending or running is a record it never
// closed, as a controller that stopped leaves.
func (p *poller) collect(ctx context.Context, ids []string, subs map[string]string, jobs map[string]JobStatus) error {
	info, err := p.d.info(ctx)
	if err != nil {
		return err
	}
	for c := range slices.Chunk(ids, chunk) {
		st, err := p.d.Scheduler.Statuses(ctx, c, info.Accounting)
		if err != nil {
			return err
		}
		maps.Copy(jobs, st)
	}
	var missing []string
	for _, id := range ids {
		if _, ok := jobs[id]; !ok && !hasExit(subs[id]) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 || !info.Accounting {
		return nil
	}
	for c := range slices.Chunk(missing, chunk) {
		st, err := p.d.Scheduler.Accounting(ctx, c)
		if err != nil {
			return err
		}
		for id, s := range st {
			if s.State == backend.Dead {
				jobs[id] = s
			}
		}
	}
	return nil
}

// hasExit reports whether the submission sub has its exit record.
func hasExit(sub string) bool {
	if sub == "" {
		return false
	}
	_, err := os.Lstat(filepath.Join(sub, ExitFile))
	return err == nil
}
