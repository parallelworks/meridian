// Package hint helps diagnostics say what was probably meant.
package hint

import (
	"strings"
	"unicode/utf8"
)

// separators are what Suggest ignores when it compares names, along with
// case.
var separators = strings.NewReplacer("-", "", "_", "")

// normalize returns name as Suggest compares it: without hyphens and
// underscores, in lower case.
func normalize(name string) string {
	return strings.ToLower(separators.Replace(name))
}

// Suggest returns the name in names that name, a misspelling, most likely
// meant: one that differs only in case, hyphens and underscores, or else in
// a letter or so.
func Suggest(name string, names []string) (string, bool) {
	want := normalize(name)
	// Only a name longer than three bytes is compared letter by letter.
	spell := len(name) > 3
	var d distance
	if spell {
		d.a = []rune(name)
	}
	best, bestDist := "", max(1, len(name)/4)+1
	for _, n := range names {
		if sameName(n, want) {
			return n, true
		}
		if spell {
			if dist := d.within(n, bestDist-1); dist < bestDist {
				best, bestDist = n, dist
			}
		}
	}
	return best, best != ""
}

// sameName reports whether name is want, a normalized name, once it is
// normalized itself. It compares a name that is ASCII without making it
// anew.
func sameName(name, want string) bool {
	j := 0
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= utf8.RuneSelf:
			return normalize(name) == want
		case c == '-' || c == '_':
			continue
		case 'A' <= c && c <= 'Z':
			c += 'a' - 'A'
		}
		if j == len(want) || want[j] != c {
			return false
		}
		j++
	}
	return j == len(want)
}

// A distance computes edit distances from one name, a, in characters. It
// keeps its buffers from one name to the next.
type distance struct {
	a, b      []rune
	prev, cur []int
}

// within returns the edit distance between d.a and b if it is at most k,
// and k+1 if it is more. It stops as soon as it knows the distance is more
// than k, and counts only the cells of the table that a path within k can
// pass through: those within k of its diagonal.
func (d *distance) within(b string, k int) int {
	m, n := len(d.a), utf8.RuneCountInString(b)
	if k < 0 || max(m-n, n-m) > k {
		return k + 1
	}
	d.b = d.b[:0]
	for _, r := range b {
		d.b = append(d.b, r)
	}
	if cap(d.prev) < n+1 {
		d.prev, d.cur = make([]int, n+1), make([]int, n+1)
	}
	prev, cur := d.prev[:n+1], d.cur[:n+1]
	// A cell outside the band costs more than k, which over stands for.
	over := k + 1
	for j := range prev {
		prev[j] = min(j, over)
	}
	for i := 1; i <= m; i++ {
		lo, hi := max(1, i-k), min(n, i+k)
		if lo == 1 {
			cur[0] = min(i, over)
		} else {
			cur[lo-1] = over
		}
		least := cur[lo-1]
		for j := lo; j <= hi; j++ {
			cost := 1
			if d.a[i-1] == d.b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
			least = min(least, cur[j])
		}
		if hi < n {
			cur[hi+1] = over
		}
		if least > k {
			return over
		}
		prev, cur = cur, prev
	}
	return min(prev[n], over)
}
