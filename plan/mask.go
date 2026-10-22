package plan

import (
	"encoding/json/jsontext"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/problems"
)

// A Masker replaces the values it is given, wherever they appear in text,
// with problems.Masked: the text of a run's secrets, and of what was made
// from them. The worker masks its job's logs and its steps' outputs with
// one, before they leave its host, and End masks the job's outputs with
// the same, so that they are masked alike. A Masker is safe for concurrent
// use.
//
// It is given strings alone: the text of a secret, and the strings made
// from one, never the text of a boolean, a number or null, which would mask
// words such as true wherever they appear (RunValues.Masked). A value that
// is empty is never masked; any other is, however short. Each value is
// masked as the text a log or an output can show it in too: without the
// line breaks it ends with, each of its lines of at least 4 bytes on its
// own, as a secret printed a line at a time shows, and as JSON writes it
// in a string, with its quotes and line breaks escaped. What a script makes
// of a secret otherwise, such as its base64, isn't known to be one, and
// isn't masked.
type Masker struct {
	mu sync.Mutex
	// values are the texts it masks, each once, and seen holds them.
	// matcher finds them, made from values once it is first needed after
	// they change.
	values  []string
	seen    map[string]bool
	matcher *matcher
}

// NewMasker returns a Masker that masks values.
func NewMasker(values ...string) *Masker {
	m := new(Masker)
	m.Add(values...)
	return m
}

// Add masks values too, from now on.
func (m *Masker) Add(values ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range values {
		for _, text := range shownAs(v) {
			if text == "" || m.seen[text] {
				continue
			}
			if m.seen == nil {
				m.seen = map[string]bool{}
			}
			m.seen[text] = true
			m.values = append(m.values, text)
			m.matcher = nil
		}
	}
}

// minLine is how long a line of a value of more than one line must be to be
// masked on its own, so that the short lines of a key, such as a PEM's last,
// don't mask the same text everywhere in a log.
const minLine = 4

// shownAs returns the texts that v, a value to mask, can show as: v, v
// without the line breaks it ends with, each line of v of at least minLine
// bytes, without its carriage return, and v as JSON writes it in a string.
func shownAs(v string) []string {
	out := []string{v}
	if strings.ContainsAny(v, "\r\n") {
		out = append(out, strings.TrimRight(v, "\r\n"))
		for line := range strings.SplitSeq(v, "\n") {
			if line = strings.TrimSuffix(line, "\r"); len(line) >= minLine {
				out = append(out, line)
			}
		}
	}
	if quoted, err := jsontext.AppendQuote(nil, v); err == nil {
		if escaped := string(quoted[1 : len(quoted)-1]); escaped != v {
			out = append(out, escaped)
		}
	}
	return out
}

// current returns what finds m's values, which it makes if they changed
// since it last did.
func (m *Masker) current() *matcher {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.matcher == nil {
		m.matcher = newMatcher(m.values)
	}
	return m.matcher
}

// Mask returns s with each value m masks replaced: each place in s where
// one of them appears, and where two or more overlap, all of the text they
// cover, as one. Two values that are next to each other are masked one
// after the other.
func (m *Masker) Mask(s string) string {
	spans, _ := m.current().find(s)
	return masked(s, spans)
}

// masked returns s with the text of each of spans that starts in it
// replaced with problems.Masked. Spans are in order, and one that starts in
// s ends in it.
func masked(s string, spans []span) string {
	if len(spans) == 0 || spans[0].start >= len(s) {
		return s
	}
	var b strings.Builder
	end := 0
	for _, sp := range spans {
		if sp.start >= len(s) {
			break
		}
		b.WriteString(s[end:sp.start])
		b.WriteString(problems.Masked)
		end = sp.end
	}
	b.WriteString(s[end:])
	return b.String()
}

// A span is the bytes of text from start up to end.
type span struct{ start, end int }

// A matcher finds where any of its values appear in text in one pass over
// it, however many values it has: an Aho-Corasick automaton of its values'
// bytes. Each state is the text read so far's longest suffix that starts a
// value; next holds the states' edges, by state and byte, and fail each
// state's next longest suffix that does. A matcher doesn't change once it
// is made.
type matcher struct {
	next map[edge]int
	fail []int
	// depth is the length of each state's text, longest the length of the
	// longest value that ends it, or 0, and more whether a value goes on
	// past it.
	depth, longest []int
	more           []bool
}

// An edge is a state's edge for a byte.
type edge struct {
	state int
	b     byte
}

func newMatcher(values []string) *matcher {
	m := &matcher{next: map[edge]int{}, fail: []int{0}, depth: []int{0}, longest: []int{0}, more: []bool{false}}
	// children are each state's edges, by the byte each reads.
	type child struct {
		b     byte
		state int
	}
	children := [][]child{nil}
	for _, v := range values {
		state := 0
		for i := range len(v) {
			next, ok := m.next[edge{state, v[i]}]
			if !ok {
				next = len(m.fail)
				m.next[edge{state, v[i]}] = next
				m.fail = append(m.fail, 0)
				m.depth = append(m.depth, m.depth[state]+1)
				m.longest = append(m.longest, 0)
				m.more = append(m.more, false)
				children = append(children, nil)
				children[state] = append(children[state], child{v[i], next})
				m.more[state] = true
			}
			state = next
		}
		m.longest[state] = len(v)
	}
	// Each state's fail is found from its parent's, in the order of their
	// depths.
	var queue []int
	for _, c := range children[0] {
		queue = append(queue, c.state)
	}
	for len(queue) > 0 {
		state := queue[0]
		queue = queue[1:]
		for _, c := range children[state] {
			for f := m.fail[state]; ; f = m.fail[f] {
				if t, ok := m.next[edge{f, c.b}]; ok {
					m.fail[c.state] = t
					break
				}
				if f == 0 {
					break
				}
			}
			m.longest[c.state] = max(m.longest[c.state], m.longest[m.fail[c.state]])
			queue = append(queue, c.state)
		}
	}
	return m
}

// step returns the state after state reads b.
func (m *matcher) step(state int, b byte) int {
	for {
		if t, ok := m.next[edge{state, b}]; ok {
			return t
		}
		if state == 0 {
			return 0
		}
		state = m.fail[state]
	}
}

// find returns where the matcher's values appear in s, in order, with those
// that overlap as one span, and the state it ends in.
func (m *matcher) find(s string) (spans []span, end int) {
	state := 0
	for i := range len(s) {
		state = m.step(state, s[i])
		l := m.longest[state]
		if l == 0 {
			continue
		}
		// Every value that ends here is within the longest one.
		sp := span{i + 1 - l, i + 1}
		for len(spans) > 0 && sp.start < spans[len(spans)-1].end {
			sp.start = min(sp.start, spans[len(spans)-1].start)
			spans = spans[:len(spans)-1]
		}
		spans = append(spans, sp)
	}
	return spans, state
}

// held returns how many bytes at the end of text whose reading ended in
// state could be the start of a value that goes on past it: the length of
// its longest suffix that a value starts with and goes on past.
func (m *matcher) held(state int) int {
	for ; state != 0; state = m.fail[state] {
		if m.more[state] {
			return m.depth[state]
		}
	}
	return 0
}

// Writer returns a writer that writes what it is given to w masked, as a
// log is uploaded: it holds back the end of what it is given that could be
// the start of a value it masks, until it knows whether the value goes on
// past it, and writes it when it is closed. What it writes is what Mask
// gives for all that it is given, however that is split into writes, while
// the values m masks don't change.
//
// It is safe for concurrent use, as a step's output and its errors share
// one, and each write is masked whole, after the one before it. Once w
// fails a write, what it didn't write is written first by the next write,
// or by Close, so nothing is written twice; Write takes what it is given
// all the same, and says so. Writing after Close fails. Closing it doesn't
// close w.
func (m *Masker) Writer(w io.Writer) io.WriteCloser {
	return &maskWriter{m: m, w: w}
}

// errClosed is what writing to a Writer after it is closed gives.
var errClosed = errors.New("plan: write to a closed masking writer")

// A maskWriter is what Masker.Writer returns.
type maskWriter struct {
	mu sync.Mutex
	m  *Masker
	w  io.Writer
	// held is what it was given that it hasn't masked yet, and pending what
	// it masked that w hasn't written yet.
	held    []byte
	pending string
	closed  bool
}

func (mw *maskWriter) Write(p []byte) (int, error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	if mw.closed {
		return 0, errClosed
	}
	mw.held = append(mw.held, p...)
	s := string(mw.held)
	m := mw.m.current()
	spans, end := m.find(s)
	// What a value could start at and go on past what has been given is
	// held back. A value that starts before that is all there, and the
	// text it covers is masked whole now or held back whole.
	cut := len(s) - m.held(end)
	for _, sp := range spans {
		if sp.start < cut && sp.end > cut {
			cut = sp.start
			break
		}
	}
	mw.pending += masked(s[:cut], spans)
	mw.held = append(mw.held[:0], s[cut:]...)
	return len(p), mw.flush()
}

// Close writes what the writer holds back, masked.
func (mw *maskWriter) Close() error {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	if mw.closed {
		return errClosed
	}
	mw.closed = true
	s := string(mw.held)
	spans, _ := mw.m.current().find(s)
	mw.pending += masked(s, spans)
	mw.held = nil
	return mw.flush()
}

// flush writes what w hasn't written yet, and keeps what it doesn't write.
func (mw *maskWriter) flush() error {
	if mw.pending == "" {
		return nil
	}
	n, err := io.WriteString(mw.w, mw.pending)
	mw.pending = mw.pending[n:]
	if err == nil && mw.pending != "" {
		err = io.ErrShortWrite
	}
	return err
}
