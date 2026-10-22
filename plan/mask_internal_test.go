package plan

import (
	"math/rand/v2"
	"slices"
	"testing"
)

func naiveFind(s string, values []string) []span {
	var found []span
	for _, v := range values {
		if v == "" {
			continue
		}
		for i := 0; i+len(v) <= len(s); i++ {
			if s[i:i+len(v)] == v {
				found = append(found, span{i, i + len(v)})
			}
		}
	}
	slices.SortStableFunc(found, func(a, b span) int { return a.start - b.start })
	var out []span
	for _, f := range found {
		if n := len(out); n > 0 && f.start < out[n-1].end {
			out[n-1].end = max(out[n-1].end, f.end)
			continue
		}
		out = append(out, f)
	}
	return out
}

func TestMatcherAgainstNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 5))
	alpha := []byte{'a', 'b', '\n', '"', 0xff, 0}
	word := func(max int) string {
		b := make([]byte, r.IntN(max+1))
		for i := range b {
			b[i] = alpha[r.IntN(len(alpha))]
		}
		return string(b)
	}
	for range 100000 {
		var values []string
		for range 1 + r.IntN(6) {
			values = append(values, word(5))
		}
		s := word(40)
		var nonEmpty []string
		for _, v := range values {
			if v != "" {
				nonEmpty = append(nonEmpty, v)
			}
		}
		got, _ := newMatcher(nonEmpty).find(s)
		want := naiveFind(s, values)
		if !slices.Equal(got, want) {
			t.Fatalf("values %q in %q: %v, want %v", values, s, got, want)
		}
	}
}
