// Package hint helps diagnostics say what was probably meant, and list
// things as a sentence does.
package hint

import "strings"

// separators are what Suggest ignores when it compares names, along with
// case.
var separators = strings.NewReplacer("-", "", "_", "")

// Suggest returns the name in names that name, a misspelling, most likely
// meant: one that differs only in case, hyphens and underscores, or else in
// a letter or so.
func Suggest(name string, names []string) (string, bool) {
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

// levenshtein returns the edit distance between a and b, in characters.
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

// JoinList joins items as a sentence does, with conj, such as "and", before
// the last: "a", "a and b", "a, b and c".
func JoinList(items []string, conj string) string {
	if len(items) < 2 {
		return strings.Join(items, "")
	}
	return strings.Join(items[:len(items)-1], ", ") + " " + conj + " " + items[len(items)-1]
}
