package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

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
// has stored them, and keeps at most limit bytes that it hasn't: past
// that, it drops what is written, and counts it, so a job cut off from the
// coordinator keeps running. A log's bytes are kept in a file of its own,
// which is removed as soon as it is made. The file is emptied whenever the
// coordinator has all of it, and while a step goes on printing, its
// unsent bytes are copied to a new file once those sent are four times as
// many, and compactAt at least, so the disk holds what is still to send,
// and of what has been sent at most four times as much, or compactAt.
type spool struct {
	dir   string
	limit int64

	mu sync.Mutex
	// pending is how many bytes the logs hold that the coordinator hasn't
	// stored.
	pending int64
	// logs are those whose bytes or end the coordinator doesn't have yet,
	// in the order they were opened, which is the order they are sent in.
	logs    []*logStream
	made    int
	changed chan struct{}
}

func newSpool(dir string, limit int64) *spool {
	return &spool{dir: dir, limit: limit, changed: make(chan struct{})}
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

// newFile makes a file for a log's bytes, which has no name: the open
// file keeps its bytes, and nothing is left behind however the worker
// ends. It makes the spool's directory again if a step has removed it.
func (s *spool) newFile() (*os.File, error) {
	s.mu.Lock()
	s.made++
	name := filepath.Join(s.dir, "log-"+strconv.Itoa(s.made))
	s.mu.Unlock()
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL|openNoFollow, 0o600) //nolint:gosec // a new file in the worker's own directory
	if err != nil {
		return nil, err
	}
	if err := os.Remove(name); err != nil {
		f.Close()
		return nil, err
	}
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
	keep := min(int64(len(p)), max(s.limit-s.pending, 0))
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
// a quarter of what it frees.
const compactAt = 16 << 10

// stored records that the coordinator's log is length bytes long, and has
// ended if ended is set.
func (s *spool) stored(l *logStream, length int64, ended bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	length = min(length, l.length)
	if length > l.stored {
		s.pending -= length - l.stored
		l.stored = length
	}
	switch sent, unsent := l.stored-l.base, l.length-l.stored; {
	case l.file == nil:
	case unsent == 0 && sent > 0:
		// The coordinator has every byte: start the file again.
		if err := l.file.Truncate(0); err == nil {
			l.base = l.length
		}
	case sent >= compactAt && sent >= 4*unsent:
		s.compact(l)
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
	}
	s.notify()
}

// compact copies the bytes of a log the coordinator doesn't have to a new
// file, which holds only them. The worker keeps the old file if it can't.
// Its caller holds s.mu, so the step's writes wait for it, but it copies a
// quarter of what it frees at most.
func (s *spool) compact(l *logStream) {
	s.mu.Unlock()
	f, err := s.newFile()
	s.mu.Lock()
	if err != nil {
		return
	}
	unsent := io.NewSectionReader(l.file, l.stored-l.base, l.length-l.stored)
	if _, err := io.Copy(io.NewOffsetWriter(f, 0), unsent); err != nil {
		f.Close()
		return
	}
	l.file.Close()
	l.file, l.base = f, l.stored
}

// lose records that the bytes of a log after the coordinator's length
// can't be read from its file, which has failed: they are dropped, and
// the log goes on in a new file, if the worker can make one.
func (s *spool) lose(l *logStream, f *os.File) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.file != f {
		return
	}
	lost := l.length - l.stored
	s.pending -= lost
	l.dropped += lost
	l.length = l.stored
	l.file.Close()
	s.mu.Unlock()
	l.file, _ = s.newFile()
	s.mu.Lock()
	l.base = l.stored
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
				stop(fmt.Errorf("worker: the coordinator has lost a log's bytes from %d, which the worker no longer has", order.expected))
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
		s.stored(u.log, ack.Length, ack.Ended)
	}
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
