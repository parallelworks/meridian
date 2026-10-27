package coordinator

import (
	"context"
	"errors"
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
}

// A LogPart is part of one of a run's logs, as Follow reads it: its next
// bytes, in order, as its worker uploaded them, with each secret masked.
type LogPart struct {
	Key  LogKey
	Data []byte
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
	f := &follower{c: c, run: id, opts: opts, ended: map[attemptKey]bool{}}
	tick := time.NewTicker(c.followEvery)
	defer tick.Stop()
	var after int64
	for {
		// Taken before the events are read, so that one added after the
		// read wakes the wait below.
		changed := c.store.Changes()
		var (
			events []store.RunEvent
			run    *store.Run
			now    time.Time
		)
		err := c.store.View(ctx, func(tx *store.Tx) error {
			var err error
			if events, err = tx.RunEvents(id, after, followEvents); err != nil {
				return err
			}
			if len(events) > 0 {
				now, err = tx.Now()
				return err
			}
			run, err = tx.Run(id)
			return err
		})
		readAt := time.Now()
		if errors.Is(err, store.ErrNotFound) {
			return nil, ErrRunNotFound
		}
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			after = e.Seq
			if e.Kind == store.EventRunFinished {
				return f.end(ctx, eventOf(id, e))
			}
			// Held, if it is a step's end, until so long after it was
			// stored, by the store's clock, which was now as the events
			// were read.
			until := readAt.Add(c.followHold - now.Sub(e.At))
			if opts.Stored {
				until = time.Time{}
			}
			if err := f.event(ctx, eventOf(id, e), until); err != nil {
				return nil, err
			}
		}
		if len(events) == 0 && run.State == store.RunEnded {
			// A run that ended before its events were kept.
			return f.end(ctx, Event{Kind: RunFinished, Run: id, Time: run.EndedAt, Outcome: run.Outcome, Reason: run.Reason, Diagnostics: run.Diagnostics})
		}
		if opts.Stored && len(events) < followEvents {
			// Every event stored is reported: the logs are, as far as
			// they are stored.
			for {
				read, err := f.readLogs(ctx, nil)
				if err != nil {
					return nil, err
				}
				if !read {
					return c.Status(ctx, id)
				}
			}
		}
		read, err := f.readLogs(ctx, nil)
		if err != nil {
			return nil, err
		}
		if len(events) == followEvents || read {
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

// A follower is what Follow keeps of a run as it follows it: the logs it
// follows, in the order it began to, and the attempts that have ended.
type follower struct {
	c     *Coordinator
	run   string
	opts  FollowOptions
	logs  []*followedLog
	ended map[attemptKey]bool
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
// its attempt ended before it did, and then e, the run's end, and returns
// how the run ended.
func (f *follower) end(ctx context.Context, e Event) (*RunStatus, error) {
	for len(f.logs) > 0 {
		l := f.logs[0]
		for {
			done, read, err := f.readLog(ctx, l, true)
			if err != nil {
				return nil, err
			}
			if done || !read {
				break
			}
		}
		f.logs = f.logs[1:]
	}
	if err := f.opts.Event(e); err != nil {
		return nil, err
	}
	return f.c.Status(ctx, f.run)
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
			if err := f.opts.Log(LogPart{Key: l.key, Data: data}); err != nil {
				return false, read, err
			}
			continue
		}
		switch {
		case ended:
			return true, read, f.opts.Log(LogPart{Key: l.key, Ended: true})
		case last && f.ended[attemptKey{l.key.Job, l.key.Attempt}]:
			return true, read, f.opts.Log(LogPart{Key: l.key, Cut: true})
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
