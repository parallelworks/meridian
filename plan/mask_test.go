package plan_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

func TestMask(t *testing.T) {
	for _, tc := range []struct {
		values []string
		s      string
		want   string
	}{
		{nil, "nothing to mask", "nothing to mask"},
		{[]string{""}, "an empty value is never masked", "an empty value is never masked"},
		{[]string{"s3cret"}, "token=s3cret; again s3cret", "token=***; again ***"},
		{[]string{"s3cret"}, "s3crets3cret", "******"},
		{[]string{"abc", "abcdef"}, "xabcdefx abc", "x***x ***"},
		{[]string{"abc", "bcd"}, "abcd", "***"},
		{[]string{"aa"}, "aaa", "***"},
		{[]string{"a", "b"}, "ab", "******"},
		{[]string{"é"}, "café", "caf***"},
	} {
		if got := plan.NewMasker(tc.values...).Mask(tc.s); got != tc.want {
			t.Errorf("%q masking %q = %q, want %q", tc.values, tc.s, got, tc.want)
		}
	}
	m := plan.NewMasker("one")
	m.Add("two", "")
	if got := m.Mask("one two three"); got != "*** *** three" {
		t.Errorf("after Add: %q", got)
	}
}

// A Writer writes what Mask gives for all it was given, however that is
// split into writes, so a value split between two writes is masked.
func TestMaskWriter(t *testing.T) {
	values := []string{"s3cret", "cre", "tok", "token-long-value", "aa", "x"}
	m := plan.NewMasker(values...)
	r := rand.New(rand.NewPCG(1, 2))
	alphabet := []string{"s3cret", "s3", "cret", "token-long-", "value", "tok", "a", "aa", "x", "-", " ", "\n", "é"}
	for range 500 {
		var text strings.Builder
		for range r.IntN(30) {
			text.WriteString(alphabet[r.IntN(len(alphabet))])
		}
		s := text.String()
		var out bytes.Buffer
		w := m.Writer(&out)
		for rest := s; rest != ""; {
			n := min(len(rest), 1+r.IntN(8))
			if k, err := w.Write([]byte(rest[:n])); err != nil || k != n {
				t.Fatalf("Write = %d, %v", k, err)
			}
			rest = rest[n:]
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if want := m.Mask(s); out.String() != want {
			t.Fatalf("writing %q in parts gave %q, want %q", s, out.String(), want)
		}
	}
}

// A Writer holds back no more than it must: what can't be the start of a
// value is written at once.
func TestMaskWriterHoldsBackLittle(t *testing.T) {
	var out bytes.Buffer
	w := plan.NewMasker("secret").Writer(&out)
	if _, err := w.Write([]byte("line one\nsec")); err != nil {
		t.Fatal(err)
	}
	if out.String() != "line one\n" {
		t.Errorf("wrote %q before it knew more", out.String())
	}
	if _, err := w.Write([]byte("ret!")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if out.String() != "line one\n***!" {
		t.Errorf("wrote %q", out.String())
	}
}

// A Masker can be added to while it masks.
func TestMaskerConcurrent(t *testing.T) {
	m := plan.NewMasker("a")
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for j := range 100 {
				m.Add(strings.Repeat("b", i*100+j+1))
				_ = m.Mask("ab")
				var out bytes.Buffer
				w := m.Writer(&out)
				_, _ = w.Write([]byte("abc"))
				_ = w.Close()
			}
		})
	}
	wg.Wait()
	if got := m.Mask("ab"); got != "******" {
		t.Errorf("Mask = %q", got)
	}
}

// A writer that fails part of a write: what it didn't write is written
// first by the next write, once.
type flakyWriter struct {
	out  bytes.Buffer
	fail int
}

func (w *flakyWriter) Write(p []byte) (int, error) {
	if w.fail > 0 {
		w.fail--
		n := len(p) / 2
		w.out.Write(p[:n])
		return n, errors.New("short")
	}
	return w.out.Write(p)
}

// A Writer that its writer fails writes nothing twice, and a write after
// Close fails. Writes from more than one goroutine are each masked whole.
func TestMaskWriterFailures(t *testing.T) {
	var out flakyWriter
	out.fail = 1
	w := plan.NewMasker("s3cret").Writer(&out)
	if n, err := w.Write([]byte("one s3cret two\n")); n != 15 || err == nil {
		t.Errorf("a failed write: %d, %v", n, err)
	}
	if _, err := w.Write([]byte("three\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if out.out.String() != "one *** two\nthree\n" {
		t.Errorf("wrote %q", out.out.String())
	}
	if _, err := w.Write([]byte("late")); err == nil {
		t.Error("a write after Close")
	}

	var shared bytes.Buffer
	cw := plan.NewMasker("s3cret").Writer(&shared)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				_, _ = cw.Write([]byte("s3cret\n"))
			}
		})
	}
	wg.Wait()
	if err := cw.Close(); err != nil {
		t.Fatal(err)
	}
	if want := strings.Repeat("***\n", 800); shared.String() != want {
		t.Errorf("concurrent writes gave %d bytes, want %d", shared.Len(), len(want))
	}
}
