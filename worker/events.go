package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// An eventQueue holds an attempt's events until the coordinator has stored
// them, and numbers them, from 1.
type eventQueue struct {
	mu sync.Mutex
	// last is the sequence number of the last event added, and stored the
	// last the coordinator has stored.
	last, stored int64
	// pending are the events after stored, in order.
	pending []workerapi.Event
	// changed is closed, and replaced, whenever an event is added or
	// stored.
	changed chan struct{}
}

func newEventQueue() *eventQueue {
	return &eventQueue{changed: make(chan struct{})}
}

// add numbers e and queues it to send, and returns its sequence number.
func (q *eventQueue) add(e workerapi.Event) int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.last++
	e.Seq = q.last
	q.pending = append(q.pending, e)
	q.notify()
	return e.Seq
}

// notify wakes whoever waits for a change. Its caller holds q.mu.
func (q *eventQueue) notify() {
	close(q.changed)
	q.changed = make(chan struct{})
}

// maxEvents and maxEventBytes bound one request's events: the bytes are
// counted from their outputs, which are most of an event.
const (
	maxEvents     = 64
	maxEventBytes = 4 << 20
)

// batch returns the next events to send, or none, with a channel closed
// when there are more.
func (q *eventQueue) batch() ([]workerapi.Event, <-chan struct{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n, size := 0, 0
	for n < len(q.pending) && n < maxEvents {
		for k, v := range q.pending[n].Outputs {
			size += len(k) + len(v)
		}
		if n > 0 && size > maxEventBytes {
			break
		}
		n++
	}
	return q.pending[:n:n], q.changed
}

// encodable puts what notEncoded makes of each of events that doesn't
// encode in its place, and reports whether there was one, and each that
// notEncoded made encodes.
func (q *eventQueue) encodable(events []workerapi.Event, notEncoded func(workerapi.Event, error) workerapi.Event) bool {
	replaced := false
	for _, e := range events {
		_, err := workerapi.Marshal(&e)
		if err == nil {
			continue
		}
		r := notEncoded(e, err)
		r.Seq = e.Seq
		if _, err := workerapi.Marshal(&r); err != nil {
			return false
		}
		q.mu.Lock()
		if k := r.Seq - q.stored - 1; k >= 0 && k < int64(len(q.pending)) {
			q.pending[k] = r
		}
		q.mu.Unlock()
		replaced = true
	}
	return replaced
}

// stored records that the coordinator has stored every event up to seq.
func (q *eventQueue) storedUpTo(seq int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if seq <= q.stored {
		return
	}
	seq = min(seq, q.last)
	q.pending = q.pending[seq-q.stored:]
	q.stored = seq
	q.notify()
}

// storedSeq returns the sequence number of the last event the
// coordinator has stored, or 0 for none.
func (q *eventQueue) storedSeq() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.stored
}

// lastSeq returns the sequence number of the last event, or 0 for none.
func (q *eventQueue) lastSeq() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.last
}

// isStored reports whether the coordinator has stored the event seq, with a
// channel closed when that may have changed.
func (q *eventQueue) isStored(seq int64) (bool, <-chan struct{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.stored >= seq, q.changed
}

// errStopped is what a wait returns when what it waited for can no longer
// happen: the worker is stopping.
var errStopped = errors.New("worker: stopped") //nolint:forbidigo // never a user's: the worker stops only once it has halted, with why

// wait waits until the coordinator has stored every event up to seq, or
// stop is closed.
func (q *eventQueue) wait(seq int64, stop <-chan struct{}) error {
	for {
		stored, changed := q.isStored(seq)
		if stored {
			return nil
		}
		select {
		case <-changed:
		case <-stop:
			return errStopped
		}
	}
}

// send sends the queue's events as they come, until ctx is done or the
// coordinator refuses them, which it reports to stop. An event the worker
// can't encode, as err says, it sends as notEncoded makes it.
func (q *eventQueue) send(ctx context.Context, c *client, stop func(error), notEncoded func(e workerapi.Event, err error) workerapi.Event) {
	stalls := 0
	for {
		events, changed := q.batch()
		if len(events) == 0 {
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return
			}
		}
		ack, err := c.events(ctx, events)
		var (
			order *outOfOrderError
			enc   *encodeError
		)
		switch {
		case err == nil:
		case errors.As(err, &enc):
			// The worker's own bug, not the attempt's end: stopping would
			// leave the coordinator a lost worker. What doesn't encode is
			// sent as what does.
			if !q.encodable(events, notEncoded) {
				stop(err)
				return
			}
			continue
		case errors.As(err, &order) && order.expected > q.storedSeq():
			ack.Seq = order.expected - 1
		case errors.As(err, &order):
			// The coordinator has lost events it said it had stored,
			// which the worker no longer has.
			stop(problems.CoordinatorLostData.Problem(problems.Params{"data": "events", "from": order.expected}))
			return
		default:
			if ctx.Err() == nil {
				stop(err)
			}
			return
		}
		if last := events[len(events)-1].Seq; ack.Seq > last {
			stop(problems.CoordinatorStoredUnsent.Problem(problems.Params{"seq": ack.Seq, "last": last}))
			return
		}
		if ack.Seq < events[0].Seq {
			// The coordinator stored none of them: wait before trying
			// again, rather than ask it again at once.
			if sleep(ctx, c.timing.backoff(stalls)) != nil {
				return
			}
			stalls++
		} else {
			stalls = 0
		}
		q.storedUpTo(ack.Seq)
	}
}
