package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// What Follow reads at a time: a run's events, a log's bytes, and the
// bytes of one log it reports before it turns to the next, so that each
// log, and the run's next events, have their turn.
const (
	followEvents = 256
	followRead   = 64 << 10
	followShare  = 1 << 20
)

// defaultFollowEvery is how often Follow reads a run's events again, for
// those another process stores, and its logs, which aren't in the store.
const defaultFollowEvery = 200 * time.Millisecond

// defaultFollowHold is how long after a step's end is stored, by the
// store's clock, Follow holds it back for the step's log to end: a worker
// sends its events and its logs apart, so a step's end can be stored
// before the last of its log. holdEvery is how often Follow reads the log
// as it holds the end.
const (
	defaultFollowHold = 2 * time.Second
	holdEvery         = 50 * time.Millisecond
)

// FollowOptions say what Follow reports of a run, and to what.
type FollowOptions struct {
	// Event hears each of the run's events, in order. It is required.
	Event func(Event) error
	// Logs, when it isn't nil, is where Follow reads the logs of the run's
	// attempts, which it reports to Log as they are stored: the setup log
	// of each attempt its worker claimed, and the log of each step's try,
	// and of each step's cleanup, that started.
	Logs Logs
	Log  func(LogPart) error
	// Stored has Follow report only what is stored now, and return: the
	// run's events, to its last, and its logs, as far as they are stored,
	// whether or not the run has ended, without holding a step's end back
	// for its log.
	Stored bool
	// Tree has Follow report the runs the run's steps call too, as it
	// reports the run, each from its child.created on, and the runs their
	// steps call, at any depth: each event and log of one says which step's
	// try called it (Event.Callers, LogPart.Callers).
	Tree bool
}

// A Caller is the try of a step that called a child run: the run whose
// step it is, the key of the step's job, the number of the attempt that ran
// the step, from 1, the step's index in its job, and its try, from 0.
type Caller struct {
	Run, Job           string
	Attempt, Step, Try int
}

// A LogPart is part of one of a run's logs, as Follow reads it: its next
// bytes, in order, as its worker uploaded them, with each secret masked.
type LogPart struct {
	Key LogKey
	// Callers are the tries of the steps that called the run the log is
	// of, from the run Follow follows down, for one its steps called, at
	// any depth (FollowOptions.Tree), or none for the run's own.
	Callers []Caller
	Data    []byte
	// Ended is set on a log's last part, once the log has ended; Cut
	// instead, once the run has ended, when the log's attempt ended before
	// the log did, as one whose worker was lost does: nothing more of it is
	// stored. Either part can have no bytes.
	Ended bool
	Cut   bool
}

// Follow reports the run id's events to opts.Event, from its first, in
// order, as they happen, and, with opts.Logs, its logs to opts.Log as they
// are stored, until the run has ended and everything of it is reported:
// then it returns how the run ended. Each event is reported once its
// logs that are stored so far are, of a step's that finished, its step's,
// and of an attempt's or a job's that ended, its attempt's or its job's;
// the run's end, last, once every log is. A step's end is held back, and
// the events after it, until the step's log has ended, for 2 seconds at
// most after it was stored, since a worker sends its logs apart from its
// events: then it is reported, and the log followed on. A run the store
// doesn't have is ErrRunNotFound.
//
// With opts.Tree, it reports the runs the run's steps call as it reports
// the run, each as it reports a run, from its child.created on, and each
// event and log of one with the steps that called it (Caller); the run's
// end, once theirs are reported, which come before it, as a root ends
// after the runs of its tree.
//
// Follow reads the run's events again as soon as a transaction of this
// process's adds one, and every so often for those another process adds,
// and for the logs, which aren't in the store. An error from opts.Event or
// opts.Log, or from the context, ends it.
func (c *Coordinator) Follow(ctx context.Context, id string, opts FollowOptions) (*RunStatus, error) {
	switch {
	case opts.Event == nil:
		return nil, errors.New("coordinator: Follow without an Event to report to")
	case (opts.Logs == nil) != (opts.Log == nil):
		return nil, errors.New("coordinator: Follow with Logs but no Log to report them to, or the other way round")
	}
	f := c.newFollower(id, nil, opts)
	tick := time.NewTicker(c.followEvery)
	defer tick.Stop()
	for {
		// Taken before the events are read, so that one added after the
		// read wakes the wait below.
		changed := c.store.Changes()
		heads, err := f.heads(ctx)
		if err != nil {
			return nil, err
		}
		read, err := f.poll(ctx, true, heads)
		if err != nil {
			return nil, err
		}
		if f.done || opts.Stored && !read {
			// Every event stored is reported, and the logs, as far as they
			// are stored.
			return c.Status(ctx, id)
		}
		if read {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		case <-tick.C:
		}
	}
}

// newFollower returns a follower of the run id, which the steps' tries
// callers called, from the run Follow follows down, or none for that run.
func (c *Coordinator) newFollower(id string, callers []Caller, opts FollowOptions) *follower {
	return &follower{c: c, run: id, callers: callers, opts: opts, ended: map[attemptKey]bool{}}
}

// poll reports what is stored of the follower's run past what it
// reported, as Follow says: its next events, and its logs' next bytes; and
// of the runs its steps called that it follows (FollowOptions.Tree). It
// reports whether it read any, and has done with the run once it has
// reported its end (done). top is set for the run Follow follows, whose
// end it reports after the ends of the runs of its tree. It reads the
// store only for the runs that heads say have something new there, or
// that heads say nothing of (follower.heads).
func (f *follower) poll(ctx context.Context, top bool, heads map[string]store.RunHead) (bool, error) {
	read := false
	if !f.done {
		var (
			events []store.RunEvent
			run    *store.Run
			now    time.Time
			readAt time.Time
		)
		if f.news(heads) {
			err := f.c.store.View(ctx, func(tx *store.Tx) error {
				var err error
				if events, err = tx.RunEvents(f.run, f.after, followEvents); err != nil {
					return err
				}
				if len(events) > 0 {
					now, err = tx.Now()
					return err
				}
				run, err = tx.Run(f.run)
				return err
			})
			readAt = time.Now()
			switch {
			case top && errors.Is(err, store.ErrNotFound):
				return false, ErrRunNotFound
			case err != nil && !top:
				// The run a step called, not the one Follow follows.
				return false, fmt.Errorf("coordinator: run %s, which a step of run %s called: %w", f.run, f.callers[len(f.callers)-1].Run, err)
			case err != nil:
				return false, err
			}
		}
		for _, e := range events {
			f.after = e.Seq
			ev := eventOf(f.run, e)
			ev.Callers = f.callers
			if e.Kind == store.EventRunFinished {
				return true, f.end(ctx, ev, top)
			}
			// Held, if it is a step's end, until so long after it was
			// stored, by the store's clock, which was now as the events
			// were read.
			until := readAt.Add(f.c.followHold - now.Sub(e.At))
			if f.opts.Stored {
				until = time.Time{}
			}
			if err := f.event(ctx, ev, until); err != nil {
				return false, err
			}
			if f.opts.Tree && e.Kind == store.EventChildCreated {
				callers := append(slices.Clip(f.callers), Caller{Run: f.run, Job: ev.Job, Attempt: ev.Attempt, Step: ev.Step, Try: ev.Try})
				f.children = append(f.children, f.c.newFollower(ev.Child, callers, f.opts))
			}
		}
		if len(events) == 0 && run != nil && run.State == store.RunEnded {
			// A run that ended before its events were kept.
			return true, f.end(ctx, Event{
				Kind: RunFinished, Run: f.run, Callers: f.callers, Time: run.EndedAt, Outcome: run.Outcome, Reason: run.Reason, Diagnostics: run.Diagnostics,
			}, top)
		}
		logs, err := f.readLogs(ctx, nil)
		if err != nil {
			return false, err
		}
		read = len(events) > 0 || logs
	}
	children, err := f.pollChildren(ctx, heads)
	return read || children, err
}

// heads reads, in one query, what the store says of the runs of the
// follower's run's tree (store.Tx.TreeHeads), when it follows more than its
// own: so that a poll reads only those runs with something new, rather
// than each in a transaction of its own. It returns nil, which says
// nothing of any run, while it follows one, which a poll reads anyway. A
// run the store hasn't isn't in it, and a poll reads it, and says so.
func (f *follower) heads(ctx context.Context) (map[string]store.RunHead, error) {
	if len(f.children) == 0 {
		return nil, nil
	}
	var heads map[string]store.RunHead
	err := f.c.store.View(ctx, func(tx *store.Tx) error {
		var err error
		heads, err = tx.TreeHeads(f.run)
		return err
	})
	return heads, err
}

// news reports whether heads say that the store may have something new of
// the follower's run, past what it reported: an event, or its end, which
// a run that ended before its events were kept has none of; or whether
// they say nothing of it.
func (f *follower) news(heads map[string]store.RunHead) bool {
	h, ok := heads[f.run]
	return !ok || h.LastEvent > f.after || h.Ended
}

// pollChildren polls the followers of the runs the follower's run's steps
// called, reading the store only for those heads say may have something
// new (poll), and stops following those that have done with their run, and
// with the runs its steps called (finished). It reports whether any read.
func (f *follower) pollChildren(ctx context.Context, heads map[string]store.RunHead) (bool, error) {
	read := false
	kept := f.children[:0]
	for _, child := range f.children {
		r, err := child.poll(ctx, false, heads)
		if err != nil {
			return read, err
		}
		read = read || r
		if !child.finished() {
			kept = append(kept, child)
		}
	}
	clear(f.children[len(kept):])
	f.children = kept
	return read, nil
}

// finished reports whether the follower has done with its run, and with
// the runs its steps called.
func (f *follower) finished() bool {
	return f.done && len(f.children) == 0
}

// A follower is what Follow keeps of a run as it follows it: the steps'
// tries that called it, from the run Follow follows down; the last of its
// events it reported, and whether it has reported its end; the logs it
// follows, in the order it began to; the attempts that have ended; and
// the followers of the runs its steps called (FollowOptions.Tree).
type follower struct {
	c        *Coordinator
	run      string
	callers  []Caller
	opts     FollowOptions
	after    int64
	done     bool
	logs     []*followedLog
	ended    map[attemptKey]bool
	children []*follower
}

// An attemptKey names one of a run's attempts: its job's key and its
// number.
type attemptKey struct {
	job    string
	number int
}

// A followedLog is a log Follow follows, and how much of it it reported.
type followedLog struct {
	key    LogKey
	offset int64
}

// event reports e, once the logs it ends are reported as far as they are
// stored, a step's end once its log has ended, or once until has passed,
// and begins to follow the logs it begins.
func (f *follower) event(ctx context.Context, e Event, until time.Time) error {
	if f.opts.Logs != nil {
		var of func(LogKey) bool
		switch e.Kind {
		case StepFinished:
			key := LogKey{Run: f.run, Job: e.Job, Attempt: e.Attempt, Part: workerapi.LogStep, Step: e.Step, Try: e.Try}
			if e.Cleanup {
				key.Part = workerapi.LogCleanup
			}
			if err := f.hold(ctx, key, until); err != nil {
				return err
			}
			of = func(k LogKey) bool { return k == key }
		case AttemptEnded:
			f.ended[attemptKey{e.Job, e.Attempt}] = true
			of = func(k LogKey) bool { return k.Job == e.Job && k.Attempt == e.Attempt }
		case JobFinished:
			of = func(k LogKey) bool { return k.Job == e.Job }
		}
		if of != nil {
			if _, err := f.readLogs(ctx, of); err != nil {
				return err
			}
		}
	}
	if err := f.opts.Event(e); err != nil {
		return err
	}
	if f.opts.Logs == nil {
		return nil
	}
	key := LogKey{Run: f.run, Job: e.Job, Attempt: e.Attempt}
	switch e.Kind {
	case JobRunning:
		key.Part = workerapi.LogSetup
	case StepStarted:
		key.Part, key.Step, key.Try = workerapi.LogStep, e.Step, e.Try
		if e.Cleanup {
			key.Part = workerapi.LogCleanup
		}
	default:
		return nil
	}
	if !slices.ContainsFunc(f.logs, func(l *followedLog) bool { return l.key == key }) {
		f.logs = append(f.logs, &followedLog{key: key})
	}
	return nil
}

// hold reads the log key, if Follow follows it, until it has ended, or
// until has passed.
func (f *follower) hold(ctx context.Context, key LogKey, until time.Time) error {
	following := func() bool { return slices.ContainsFunc(f.logs, func(l *followedLog) bool { return l.key == key }) }
	tick := time.NewTicker(holdEvery)
	defer tick.Stop()
	for following() {
		if _, err := f.readLogs(ctx, func(k LogKey) bool { return k == key }); err != nil {
			return err
		}
		if !following() || !time.Now().Before(until) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

// end reports the logs that are left, each to its end, or cut short when
// its attempt ended before it did, and then e, the run's end, and has done
// with the run. For top, the run Follow follows, it first reports the
// runs of its tree to their ends, which have ended before it: a root's end
// waits for theirs. With FollowOptions.Stored, it reports only what is
// stored of them, as of a child's tree whose own children still run.
func (f *follower) end(ctx context.Context, e Event, top bool) error {
	switch {
	case top && f.opts.Stored:
		for {
			read, err := f.pollChildren(ctx, nil)
			if err != nil {
				return err
			}
			if !read {
				break
			}
		}
	case top:
		if err := f.drain(ctx); err != nil {
			return err
		}
	}
	for len(f.logs) > 0 {
		l := f.logs[0]
		for {
			done, read, err := f.readLog(ctx, l, true)
			if err != nil {
				return err
			}
			if done || !read {
				break
			}
		}
		f.logs = f.logs[1:]
	}
	if err := f.opts.Event(e); err != nil {
		return err
	}
	f.done = true
	return nil
}

// drain reports what is left of the runs the follower's run's steps
// called, and theirs, to their ends, reading them again every so often
// while one has more to come.
func (f *follower) drain(ctx context.Context) error {
	tick := time.NewTicker(f.c.followEvery)
	defer tick.Stop()
	for len(f.children) > 0 {
		heads, err := f.heads(ctx)
		if err != nil {
			return err
		}
		read, err := f.pollChildren(ctx, heads)
		if err != nil || read {
			if err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

// readLogs reports what is stored of the logs it follows, of those of
// reports, or of all of them for nil, past what it reported, and stops
// following those that have ended. It reports whether it read any bytes.
func (f *follower) readLogs(ctx context.Context, of func(LogKey) bool) (bool, error) {
	anyRead := false
	kept := f.logs[:0]
	for _, l := range f.logs {
		if of != nil && !of(l.key) {
			kept = append(kept, l)
			continue
		}
		done, read, err := f.readLog(ctx, l, false)
		if err != nil {
			return anyRead, err
		}
		anyRead = anyRead || read
		if !done {
			kept = append(kept, l)
		}
	}
	clear(f.logs[len(kept):])
	f.logs = kept
	return anyRead, nil
}

// readLog reports what is stored of the log l past what it reported, up to
// its share, and reports whether the log is done with, as one that has
// ended is, and whether it read any bytes. Once the run has ended (last),
// a log whose attempt ended, and that has no more bytes, is done with, cut
// short.
func (f *follower) readLog(ctx context.Context, l *followedLog, last bool) (done, read bool, err error) {
	for reported := 0; reported < followShare; {
		data, ended, err := f.opts.Logs.Read(ctx, l.key, l.offset, followRead)
		if err != nil {
			return false, read, err
		}
		if len(data) > 0 {
			read = true
			l.offset += int64(len(data))
			reported += len(data)
			if err := f.opts.Log(LogPart{Key: l.key, Callers: f.callers, Data: data}); err != nil {
				return false, read, err
			}
			continue
		}
		switch {
		case ended:
			return true, read, f.opts.Log(LogPart{Key: l.key, Callers: f.callers, Ended: true})
		case last && f.ended[attemptKey{l.key.Job, l.key.Attempt}]:
			return true, read, f.opts.Log(LogPart{Key: l.key, Callers: f.callers, Cut: true})
		}
		return false, read, nil
	}
	return false, read, nil
}

// eventOf is the run's event e as Follow reports it.
func eventOf(run string, e store.RunEvent) Event {
	ev := Event{
		Seq: e.Seq, Time: e.At, Kind: EventKind(e.Kind), Run: run, Job: e.Job, Attempt: e.Attempt,
		Outcome: e.Outcome, Reason: e.Reason, ExitCode: -1, Diagnostics: e.Diagnostics,
	}
	if e.Group != "" {
		ev.Member = e.Group + "/" + e.Member
	}
	if l := e.Called; l != nil {
		ev.Child, ev.Step, ev.Try = e.Child, l.Step, l.Try
	}
	if s := e.Step; s != nil {
		ev.Time, ev.Step, ev.Try, ev.Cleanup = s.Time, s.Step, s.Try, s.Cleanup
		if s.Finished {
			ev.Outcome, ev.ExitCode, ev.Diagnostics = s.Outcome, s.ExitCode, s.Diagnostics
		}
	}
	if end := e.End; end != nil {
		ev.End, ev.Outcome, ev.Reason = AttemptEnd(end.Kind), end.Outcome, end.Reason
		if p := end.Problem; p != nil {
			ev.Diagnostics = problems.Diagnostics{diagnosticOfProblem(p)}
		}
	}
	return ev
}

// diagnosticOfProblem is the problem p as a diagnostic, about the run as a
// whole.
func diagnosticOfProblem(p *problem.Problem) problems.Diagnostic {
	return problems.Diagnostic{Type: p.Type, Code: p.Code, Detail: p.Detail, Params: p.Params}
}
