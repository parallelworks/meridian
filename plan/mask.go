package plan

import "io"

// A Masker replaces the values it is given, wherever they appear in text,
// with problems.Masked: the values of a run's secrets, and of what was made
// from them. The worker masks its job's logs and its steps' outputs with
// one, before they leave its host, and End masks the job's outputs with
// the same, so that they are masked alike. A value that is empty is never
// masked. A Masker is safe for concurrent use.
type Masker struct{}

// NewMasker returns a Masker that masks values.
//
// It isn't built yet, and stops its caller.
func NewMasker(values ...string) *Masker {
	notImplemented("NewMasker")
	return nil
}

// Add masks values too, from now on.
//
// It isn't built yet, and stops its caller.
func (m *Masker) Add(values ...string) {
	notImplemented("Masker.Add")
}

// Mask returns s with each value m masks replaced, the longest first where
// two overlap.
//
// It isn't built yet, and stops its caller.
func (m *Masker) Mask(s string) string {
	notImplemented("Masker.Mask")
	return ""
}

// Writer returns a writer that writes what it is given to w masked, as a
// log is uploaded: it holds back the end of what it is given, as long as
// the longest value it masks, until it knows whether a value goes on past
// it, and writes it when it is closed.
//
// It isn't built yet, and stops its caller.
func (m *Masker) Writer(w io.Writer) io.WriteCloser {
	notImplemented("Masker.Writer")
	return nil
}
