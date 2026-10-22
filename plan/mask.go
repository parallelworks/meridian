package plan

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

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
// masked as the text a log or an output can show it in too:
//
//   - without the line breaks it ends with;
//   - each of its lines on its own, as a secret printed a line at a time
//     shows, as GitHub Actions masks one, but for a line of fewer than 8
//     bytes, trimmed of space, or of nothing but punctuation, or that is
//     true, false or null, which would mask common text everywhere;
//   - as JSON writes it in a string, with its quotes, line breaks and
//     control characters escaped: as encoding/json/v2 does, as
//     encoding/json does, with <, >, & and U+2028 and U+2029 escaped too,
//     with only U+2028 and U+2029 escaped, and with every character that
//     isn't ASCII escaped, as Python's json.dumps does.
//
// What a script makes of a secret otherwise, such as its base64, isn't
// known to be one, and isn't masked.
type Masker struct {
	mu sync.Mutex
	// lines is set when it masks each line of a value of more than one on
	// its own.
	lines bool
	// seen holds the texts it masks, and pending those it hasn't made into
	// a matcher yet. levels find the rest: each made from the texts added
	// since the one before it was made, merged with the levels after it
	// while they are no smaller, so that there are only as many as the
	// bits of how many texts there are, and each text is made into a
	// matcher only as often as there are levels.
	seen    map[string]bool
	pending []string
	levels  []*matcher
	// parent is the Masker it extends (extend), and base what finds its
	// values, which it shares, once it needs it, and never rebuilds.
	parent *Masker
	base   []*matcher
}

// NewMasker returns a Masker that masks values.
func NewMasker(values ...string) *Masker {
	m := &Masker{lines: true}
	m.Add(values...)
	return m
}

// newLeakMasker returns a Masker that masks values as they show but for
// their lines: what tells a step's output that holds a secret, which only
// a secret's own text, and not a line of it, can tell.
func newLeakMasker(values ...string) *Masker {
	m := &Masker{}
	m.Add(values...)
	return m
}

// Add masks values too, from now on.
func (m *Masker) Add(values ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range values {
		for _, text := range shownAs(v, m.lines) {
			if text == "" || m.seen[text] {
				continue
			}
			if m.seen == nil {
				m.seen = map[string]bool{}
			}
			m.seen[text] = true
			m.pending = append(m.pending, text)
		}
	}
}

// minLine is how long a line of a value of more than one line must be,
// trimmed of space, to be masked on its own.
const minLine = 8

// shownAs returns the texts that v, a value to mask, can show as: v, v
// without the line breaks it ends with, each of its lines that is masked
// on its own, if lines is set, and its JSON forms (jsonForms).
func shownAs(v string, lines bool) []string {
	out := []string{v}
	if strings.ContainsAny(v, "\r\n") {
		out = append(out, strings.TrimRight(v, "\r\n"))
		if lines {
			for line := range strings.SplitSeq(v, "\n") {
				if line = strings.TrimSpace(line); maskedLine(line) {
					out = append(out, line)
				}
			}
		}
	}
	return append(out, jsonForms(v)...)
}

// maskedLine reports whether a line of a value, trimmed of space, is masked
// on its own: one of at least minLine bytes with a letter or a digit in
// it, which isn't true, false or null.
func maskedLine(line string) bool {
	switch line {
	case "true", "false", "null":
		return false
	}
	return len(line) >= minLine && strings.IndexFunc(line, func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }) >= 0
}

// jsonForms returns v as the JSON of a string holds it, without its quotes,
// in each of the escapings a Masker covers, each that differs from v once.
func jsonForms(v string) []string {
	quoted, err := jsontext.AppendQuote(nil, v)
	if err != nil {
		return nil
	}
	base := string(quoted[1 : len(quoted)-1])
	js := strings.NewReplacer("\u2028", `\u2028`, "\u2029", `\u2029`).Replace(base)
	html := strings.NewReplacer("<", `\u003c`, ">", `\u003e`, "&", `\u0026`).Replace(js)
	var ascii strings.Builder
	for _, r := range base {
		switch {
		case r < utf8.RuneSelf:
			ascii.WriteRune(r)
		case r > 0xffff:
			r1, r2 := utf16.EncodeRune(r)
			fmt.Fprintf(&ascii, `\u%04x\u%04x`, r1, r2)
		default:
			fmt.Fprintf(&ascii, `\u%04x`, r)
		}
	}
	var out []string
	for _, form := range []string{base, js, html, ascii.String()} {
		if form != v && !slices.Contains(out, form) {
			out = append(out, form)
		}
	}
	return out
}

// current returns what finds m's values: its levels, after it makes the
// texts added since it last did into one, and merges the levels that are
// no larger than the one after them.
func (m *Masker) current() finder {
	var base finder
	if m.parent != nil {
		base = m.parent.current()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.parent != nil && m.base == nil {
		m.base = base
	}
	if len(m.pending) > 0 {
		m.levels = append(m.levels, newMatcher(m.pending))
		m.pending = nil
		for n := len(m.levels); n > 1 && len(m.levels[n-2].values) <= len(m.levels[n-1].values); n = len(m.levels) {
			merged := newMatcher(slices.Concat(m.levels[n-2].values, m.levels[n-1].values))
			m.levels = append(m.levels[:n-2], merged)
		}
	}
	return finder(slices.Concat(m.base, m.levels))
}

// extend returns a Masker that masks what m does now, and what is added to
// it from now on, which m doesn't: it shares what m builds to find its
// values, the first time it masks, so that a Masker of a large secret is
// built once, and only if it is used, and extended for each use. lines
// says whether it masks each line of a value of more than one on its own,
// as NewMasker's do. m isn't added to once it is extended: what the
// Masker masks of m's values is what m masks when it first masks.
func (m *Masker) extend(lines bool) *Masker {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &Masker{lines: lines, seen: maps.Clone(m.seen), parent: m}
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

// A finder finds a Masker's values with each of its matchers.
type finder []*matcher

// find returns where the finder's values appear in s, in order, with those
// that overlap as one span, and how many bytes at the end of s could be the
// start of a value that goes on past it (matcher.held).
func (f finder) find(s string) (spans []span, held int) {
	for _, m := range f {
		found, end := m.find(s)
		spans = append(spans, found...)
		held = max(held, m.held(end))
	}
	if len(f) < 2 {
		return spans, held
	}
	slices.SortFunc(spans, func(a, b span) int { return a.start - b.start })
	out := spans[:0]
	for _, sp := range spans {
		if n := len(out); n > 0 && sp.start < out[n-1].end {
			out[n-1].end = max(out[n-1].end, sp.end)
			continue
		}
		out = append(out, sp)
	}
	return out, held
}

// A matcher finds where any of its values appear in text in one pass over
// it, however many values it has: an Aho-Corasick automaton of its values'
// bytes. Each state is the text read so far's longest suffix that starts a
// value; next holds the states' edges, by state and byte, but the first
// state's, which root holds, and fail each state's next longest suffix that
// does. A matcher doesn't change once it is made.
type matcher struct {
	values []string
	root   [256]int
	next   map[edge]int
	fail   []int
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
	m := &matcher{values: values, next: map[edge]int{}, fail: []int{0}, depth: []int{0}, longest: []int{0}, more: []bool{false}}
	// children are each state's edges, by the byte each reads.
	type child struct {
		b     byte
		state int
	}
	children := [][]child{nil}
	for _, v := range values {
		state := 0
		for i := range len(v) {
			next, ok := m.edge(state, v[i])
			if !ok {
				next = len(m.fail)
				if state == 0 {
					m.root[v[i]] = next
				} else {
					m.next[edge{state, v[i]}] = next
				}
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
				if t, ok := m.edge(f, c.b); ok {
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

// edge returns the state state's edge for b leads to, if it has one.
func (m *matcher) edge(state int, b byte) (int, bool) {
	if state == 0 {
		t := m.root[b]
		return t, t != 0
	}
	t, ok := m.next[edge{state, b}]
	return t, ok
}

// step returns the state after state reads b.
func (m *matcher) step(state int, b byte) int {
	for {
		if t, ok := m.edge(state, b); ok {
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
	for i := 0; i < len(s); i++ {
		if state == 0 {
			// Skip what no value starts with.
			for i < len(s) && m.root[s[i]] == 0 {
				i++
			}
			if i == len(s) {
				break
			}
		}
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
// all the same, and says so, until what w hasn't written reaches 1 MiB,
// when Write takes nothing more until w writes it. Writing after Close
// fails. Closing it doesn't close w.
func (m *Masker) Writer(w io.Writer) io.WriteCloser {
	return &maskWriter{m: m, w: w}
}

// maxPending is how much a Writer keeps of what its writer hasn't written.
const maxPending = 1 << 20

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
	if len(mw.pending) >= maxPending {
		if err := mw.flush(); err != nil {
			return 0, err
		}
	}
	mw.held = append(mw.held, p...)
	s := string(mw.held)
	spans, held := mw.m.current().find(s)
	// What a value could start at and go on past what has been given is
	// held back. A value that starts before that is all there, and the
	// text it covers is masked whole now or held back whole.
	cut := len(s) - held
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
