package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A logKey is which of an attempt's logs one is: its setup's, or a step's
// try's or cleanup's.
type logKey struct {
	part workerapi.LogPart
	step int
	try  int
}

// A spool holds an attempt's logs, masked, on disk, until the coordinator
// has stored them, and keeps at most its limit of bytes that it hasn't
// (limit): past
// that, it drops what is written, and counts it, so a job cut off from the
// coordinator keeps running. A log's bytes are kept in a file of its own,
// which is removed as soon as it is made. The file is emptied whenever the
// coordinator has all of it, and while a step goes on printing, its
// unsent bytes are copied to a new file once those sent are compactAt at
// least, and four times as many, or as many as the limit: the disk holds
// what is still to send, and less than the limit of what has been sent,
// besides the unsent bytes again while they are copied.
type spool struct {
	dir *privateDir
	// given is the limit Config.SpoolLimit gives, or 0 for the default of
	// the directory the worker keeps its files in now (limit).
	given int64

	mu sync.Mutex
	// pending is how many bytes the logs hold that the coordinator hasn't
	// stored.
	pending int64
	// logs are those whose bytes or end the coordinator doesn't have yet,
	// in the order they were opened, which is the order they are sent in.
	logs    []*logStream
	made    int
	changed chan struct{}
	// closing is closed, and made again, as a log is closed (spool.pace).
	closing chan struct{}
}

func newSpool(dir *privateDir, limit int64) *spool {
	return &spool{dir: dir, given: limit, changed: make(chan struct{}), closing: make(chan struct{})}
}

// limit returns how many bytes the spool keeps that the coordinator hasn't
// stored: the limit it was given, or else DefaultSpoolLimit on disk, and
// memorySpoolLimit in XDG_RUNTIME_DIR, in memory, as the directory the
// worker keeps its files in is now, which it can move to another parent.
func (s *spool) limit() int64 {
	switch {
	case s.given > 0:
		return s.given
	case s.dir.inMemory():
		return memorySpoolLimit
	}
	return DefaultSpoolLimit
}

// A logStream is one log of an attempt. Its writes never fail: what it
// can't keep, it drops and counts.
type logStream struct {
	s   *spool
	key logKey

	// These are guarded by s.mu. file holds the log's bytes from base, or
	// is nil if the worker couldn't make one. length is how many bytes the
	// log has, those dropped aside, and stored how many the coordinator
	// has.
	file                 *os.File
	length, base, stored int64
	// dropped is how many bytes written to the log weren't kept.
	dropped int64
	// closed is set once nothing more is written to the log, and ended
	// once the coordinator has its end.
	closed, ended bool
}

// open opens a new log. A log the worker can't make a file for, as on a
// disk that is full, keeps nothing, and counts what it drops.
func (s *spool) open(key logKey) *logStream {
	l := &logStream{s: s, key: key}
	l.file, _ = s.newFile()
	s.mu.Lock()
	s.logs = append(s.logs, l)
	s.notify()
	s.mu.Unlock()
	return l
}

// newFile makes a file for a log's bytes, in the worker's directory, which
// has no name: the open file keeps its bytes, and nothing is left behind
// however the worker ends.
func (s *spool) newFile() (*os.File, error) {
	s.mu.Lock()
	s.made++
	name := "log-" + strconv.Itoa(s.made)
	s.mu.Unlock()
	f, made, _, err := s.dir.create(name, os.O_RDWR|os.O_EXCL)
	if err != nil {
		return nil, err
	}
	made.remove()
	return f, nil
}

// notify wakes whoever waits for a change. Its caller holds s.mu.
func (s *spool) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// Write keeps as much of p as the spool has room for, and drops the rest.
func (l *logStream) Write(p []byte) (int, error) {
	s := l.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.closed {
		return 0, os.ErrClosed
	}
	keep := min(int64(len(p)), max(s.limit()-s.pending, 0))
	if keep > 0 {
		if l.file == nil {
			keep = 0
		} else if _, err := l.file.WriteAt(p[:keep], l.length-l.base); err != nil {
			// A disk that is full keeps nothing more: the log goes on
			// without it, as when the spool is full.
			keep = 0
		} else {
			l.length += keep
			s.pending += keep
			s.notify()
		}
	}
	l.dropped += int64(len(p)) - keep
	return len(p), nil
}

// Close ends the log: nothing more is written to it.
func (l *logStream) Close() error {
	s := l.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if !l.closed {
		l.closed = true
		s.notify()
		close(s.closing)
		s.closing = make(chan struct{})
	}
	return nil
}

// droppedBytes returns how many bytes written to the log weren't kept.
func (l *logStream) droppedBytes() int64 {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	return l.dropped
}

// An upload is the next part of a log to send.
type upload struct {
	log    *logStream
	file   *os.File
	offset int64
	// at is where in the file its bytes start, and n how many there are.
	at, n int64
	end   bool
}

// next returns the next upload to send, the first log's with bytes or an
// end the coordinator doesn't have, or none, with a channel closed when
// there is more.
func (s *spool) next() (*upload, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.logs {
		n := min(l.length-l.stored, workerapi.MaxLogUpload)
		if n > 0 || l.closed {
			return &upload{log: l, file: l.file, offset: l.stored, at: l.stored - l.base, n: n, end: l.closed && l.stored+n == l.length}, nil
		}
	}
	return nil, s.changed
}

// compactAt is how many bytes a log's file holds that the coordinator has,
// at least, before the worker copies those it hasn't to a new file: when
// they are at least four times as many, so that what it copies is at most
// a quarter of what it frees, or as many as the spool's limit.
const compactAt = 16 << 10

// stored records that the coordinator's log is length bytes long, and has
// ended if ended is set. It reports whether the log's file should be
// compacted, which compact does without s.mu.
func (s *spool) stored(l *logStream, length int64, ended bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.notify()
	length = min(length, l.length)
	if length > l.stored {
		s.pending -= length - l.stored
		l.stored = length
	}
	if ended && l.closed && l.stored == l.length {
		l.ended = true
		if l.file != nil {
			l.file.Close()
		}
		for i, m := range s.logs {
			if m == l {
				s.logs = append(s.logs[:i], s.logs[i+1:]...)
				break
			}
		}
		return false
	}
	switch sent, unsent := l.stored-l.base, l.length-l.stored; {
	case l.file == nil:
	case unsent == 0 && sent > 0:
		// The coordinator has every byte: start the file again.
		if err := l.file.Truncate(0); err == nil {
			l.base = l.length
		}
	case sent >= compactAt && (sent >= 4*unsent || sent >= s.limit()):
		return true
	}
	return false
}

// compact copies the bytes of a log the coordinator doesn't have to a new
// file, which holds only them, and the log goes on there; the worker keeps
// the old file if it can't. Only the uploader, which calls it, reads a
// log's bytes or drops them, and those it copies are written already, so
// it copies them without s.mu, and the step's writes don't wait for it;
// what the step writes as it copies, it copies after, holding s.mu.
func (s *spool) compact(l *logStream) {
	f, err := s.newFile()
	if err != nil {
		return
	}
	s.mu.Lock()
	old, from, to, base := l.file, l.stored-l.base, l.length-l.base, l.stored
	s.mu.Unlock()
	if _, err := io.Copy(io.NewOffsetWriter(f, 0), io.NewSectionReader(old, from, to-from)); err != nil {
		f.Close()
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if more := l.length - l.base - to; more > 0 {
		if _, err := io.Copy(io.NewOffsetWriter(f, to-from), io.NewSectionReader(old, to, more)); err != nil {
			f.Close()
			return
		}
	}
	old.Close()
	l.file, l.base = f, base
}

// lose records that the bytes of a log after the coordinator's length
// can't be read from f, its file, which has failed: they are dropped, and
// the log goes on in a new file, if the worker can make one.
func (s *spool) lose(l *logStream, f *os.File) {
	next, _ := s.newFile()
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.file != f {
		if next != nil {
			next.Close()
		}
		return
	}
	lost := l.length - l.stored
	s.pending -= lost
	l.dropped += lost
	l.length = l.stored
	l.file.Close()
	l.file, l.base = next, l.stored
	s.notify()
}

// wait waits until the coordinator has every log whole, and its end, or
// stop is closed.
func (s *spool) wait(stop <-chan struct{}) error {
	for {
		s.mu.Lock()
		done, changed := len(s.logs) == 0, s.changed
		s.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-changed:
		case <-stop:
			return errStopped
		}
	}
}

// close closes every log's file.
func (s *spool) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.logs {
		if l.file != nil {
			l.file.Close()
		}
	}
	s.logs = nil
}

// send sends the spool's logs to the coordinator as they are written, in
// the order they were opened, until ctx is done or the coordinator refuses
// them, which it reports to stop.
func (s *spool) send(ctx context.Context, c *client, stop func(error)) {
	stalls := 0
	for {
		u, changed := s.next()
		if u == nil {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return
			}
		}
		// A buffer of its own: the HTTP client can go on reading a
		// request's body after it returns.
		data := make([]byte, u.n)
		if u.n > 0 {
			if _, err := u.file.ReadAt(data, u.at); err != nil {
				s.lose(u.log, u.file)
				continue
			}
		}
		began := time.Now()
		ack, err := c.upload(ctx, u.log.key, u.offset, data, u.end)
		var order *outOfOrderError
		switch {
		case err == nil:
		case errors.As(err, &order) && order.expected >= u.offset:
			// The coordinator has more of the log than the worker sent:
			// it has the rest of these bytes already.
			ack = workerapi.LogAck{Length: order.expected}
		case errors.As(err, &order):
			// The coordinator has less of the log than it said it had:
			// send again from where it says, if the worker still has it.
			if !s.rewind(u.log, order.expected) {
				stop(problems.CoordinatorLostData.Problem(problems.Params{"data": "log", "from": order.expected}))
				return
			}
			continue
		default:
			if ctx.Err() == nil {
				stop(err)
			}
			return
		}
		if ack.Length <= u.offset && u.n > 0 || u.end && !ack.Ended {
			// The coordinator stored none of it: wait before trying
			// again, rather than ask it again at once.
			if sleep(ctx, c.timing.backoff(stalls)) != nil {
				return
			}
			stalls++
		} else {
			stalls = 0
		}
		if s.stored(u.log, ack.Length, ack.Ended) {
			s.compact(u.log)
		}
		if !u.end && u.n < workerapi.MaxLogUpload && s.pace(ctx, began.Add(c.timing.logPace)) != nil {
			return
		}
	}
}

// pace waits, after an upload that sent all its log had, until until, so
// that what a step prints meanwhile goes in one upload: a step that prints
// a line at a time would otherwise have an upload, and the coordinator's
// work for it, for nearly each line. It doesn't wait while a log has ended
// whose end the coordinator doesn't have yet, and stops waiting as soon as
// one ends, so the end of each step's log, which its job's end waits for,
// is sent at once.
func (s *spool) pace(ctx context.Context, until time.Time) error {
	s.mu.Lock()
	closing := s.closing
	ending := slices.ContainsFunc(s.logs, func(l *logStream) bool { return l.closed })
	s.mu.Unlock()
	d := time.Until(until)
	if ending || d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-closing:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// rewind records that the coordinator has only length bytes of a log, fewer
// than it said it had, and reports whether the worker still has the rest,
// so that it can send them again: its file keeps only those after the last
// that the coordinator had when it was emptied.
func (s *spool) rewind(l *logStream, length int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if length < l.base || l.file == nil {
		return false
	}
	s.pending += l.stored - length
	l.stored = length
	return true
}
