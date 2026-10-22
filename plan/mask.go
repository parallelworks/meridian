package plan

import (
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/problems"
)

// A Masker replaces the values it is given, wherever they appear in text,
// with problems.Masked: the values of a run's secrets, and of what was made
// from them. The worker masks its job's logs and its steps' outputs with
// one, before they leave its host, and End masks the job's outputs with
// the same, so that they are masked alike. A value that is empty is never
// masked. A Masker is safe for concurrent use.
type Masker struct {
	mu sync.RWMutex
	// values are the values it masks, sorted, each once, and longest the
	// length of the longest. Add replaces values rather than change it, so
	// that a reader can keep the one it read.
	values  []string
	longest int
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
	next := slices.Clone(m.values)
	for _, v := range values {
		if v != "" {
			next = append(next, v)
			m.longest = max(m.longest, len(v))
		}
	}
	slices.Sort(next)
	m.values = slices.Compact(next)
}

// masked returns the values m masks, which mustn't be changed, and the
// length of the longest.
func (m *Masker) masked() ([]string, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.values, m.longest
}

// Mask returns s with each value m masks replaced: each place in s where
// one of them appears, and where two or more overlap, all of the text they
// cover, as one. Two values that are next to each other are masked one
// after the other.
func (m *Masker) Mask(s string) string {
	values, _ := m.masked()
	return masked(s, find(s, values))
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

// find returns where values appear in s, in order, with those that
// overlap as one span.
func find(s string, values []string) []span {
	var found []span
	for _, v := range values {
		for at := 0; at+len(v) <= len(s); {
			i := strings.Index(s[at:], v)
			if i < 0 {
				break
			}
			found = append(found, span{at + i, at + i + len(v)})
			at += i + 1
		}
	}
	if len(found) == 0 {
		return nil
	}
	slices.SortFunc(found, func(a, b span) int { return a.start - b.start })
	out := found[:1]
	for _, sp := range found[1:] {
		last := &out[len(out)-1]
		if sp.start < last.end {
			last.end = max(last.end, sp.end)
			continue
		}
		out = append(out, sp)
	}
	return out
}

// Writer returns a writer that writes what it is given to w masked, as a
// log is uploaded: it holds back the end of what it is given that could be
// the start of a value it masks, shorter than the longest, until it knows
// whether the value goes on past it, and writes it when it is closed. What
// it writes is what Mask gives for all that it is given, however that is
// split into writes, while the values m masks don't change. Closing it
// doesn't close w.
func (m *Masker) Writer(w io.Writer) io.WriteCloser {
	return &maskWriter{m: m, w: w}
}

// A maskWriter is what Masker.Writer returns.
type maskWriter struct {
	m *Masker
	w io.Writer
	// held is what it was given that it hasn't written yet.
	held []byte
}

func (mw *maskWriter) Write(p []byte) (int, error) {
	mw.held = append(mw.held, p...)
	values, longest := mw.m.masked()
	s := string(mw.held)
	spans := find(s, values)
	// What a value could start at and go on past what has been given is
	// held back: the text from the first place whose text to the end
	// starts a value longer than it. A value that starts before that is all
	// there, and the text it covers is written whole or held back whole.
	cut := len(s)
	for at := max(len(s)-longest+1, 0); at < len(s); at++ {
		if startsValue(s[at:], values) {
			cut = at
			break
		}
	}
	for _, sp := range spans {
		if sp.start < cut && sp.end > cut {
			cut = sp.start
			break
		}
	}
	if err := mw.write(masked(s[:cut], spans)); err != nil {
		// What p held isn't written, and the caller can give it again.
		mw.held = mw.held[:len(mw.held)-len(p)]
		return 0, err
	}
	mw.held = append(mw.held[:0], s[cut:]...)
	return len(p), nil
}

// startsValue reports whether one of values, which are sorted, is longer
// than s and starts with it.
func startsValue(s string, values []string) bool {
	i, _ := slices.BinarySearch(values, s)
	for ; i < len(values) && strings.HasPrefix(values[i], s); i++ {
		if len(values[i]) > len(s) {
			return true
		}
	}
	return false
}

// Close writes what the writer holds back, masked.
func (mw *maskWriter) Close() error {
	s := string(mw.held)
	values, _ := mw.m.masked()
	mw.held = nil
	return mw.write(masked(s, find(s, values)))
}

// write writes s, unless it is empty.
func (mw *maskWriter) write(s string) error {
	if s == "" {
		return nil
	}
	_, err := io.WriteString(mw.w, s)
	return err
}
