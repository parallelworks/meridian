package hint_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/hint"
)

func TestSuggest(t *testing.T) {
	names := []string{"runs-on", "steps", "timeout", "working-directory"}
	for _, tc := range []struct {
		name, want string
	}{
		{"runs_on", "runs-on"},
		{"RunsOn", "runs-on"},
		{"timout", "timeout"},
		{"workingdirectory", "working-directory"},
		{"steps", "steps"},
		// Short names match only by case and separators.
		{"stp", ""},
		{"nothing-like-it", ""},
	} {
		got, ok := hint.Suggest(tc.name, names)
		if got != tc.want || ok != (tc.want != "") {
			t.Errorf("Suggest(%q) = %q, %v; want %q", tc.name, got, ok, tc.want)
		}
	}
}

// TestSuggestMatchesPlain checks that Suggest, which skips what can't be
// closer and stops counting edits past the closest so far, suggests what
// comparing every name in full suggests.
func TestSuggestMatchesPlain(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	// Letters close to each other make near misses likely, and the rest
	// cover case, separators, text that isn't ASCII and bytes that aren't
	// UTF-8.
	alphabet := []string{"a", "b", "c", "A", "B", "-", "_", "é", "É", "K", "k", "K", "\xff"}
	word := func() string {
		var b strings.Builder
		for range rng.IntN(12) {
			b.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		return b.String()
	}
	for i := range 20000 {
		names := make([]string, rng.IntN(8))
		for j := range names {
			names[j] = word()
		}
		name := word()
		if i%3 == 0 && len(names) > 0 {
			// A near miss of one of the names.
			name = names[rng.IntN(len(names))] + alphabet[rng.IntN(len(alphabet))]
		}
		got, gotOK := hint.Suggest(name, names)
		want, wantOK := plainSuggest(name, names)
		if got != want || gotOK != wantOK {
			t.Fatalf("Suggest(%q, %q) = %q, %v; want %q, %v", name, names, got, gotOK, want, wantOK)
		}
	}
}

// plainSuggest is Suggest as it is written plainly: it normalizes and
// compares each name in full.
func plainSuggest(name string, names []string) (string, bool) {
	separators := strings.NewReplacer("-", "", "_", "")
	norm := func(s string) string { return strings.ToLower(separators.Replace(s)) }
	best, bestDist := "", max(1, len(name)/4)+1
	for _, n := range names {
		if norm(n) == norm(name) {
			return n, true
		}
		if d := levenshtein(name, n); d < bestDist && len(name) > 3 {
			best, bestDist = n, d
		}
	}
	return best, best != ""
}

func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}

func BenchmarkSuggest(b *testing.B) {
	names := make([]string, 500)
	for i := range names {
		names[i] = fmt.Sprintf("build-job-%04d", i)
	}
	for b.Loop() {
		hint.Suggest("buld-job-0250", names)
		hint.Suggest("nothing-like-any-of-them", names)
	}
}
