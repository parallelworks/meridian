package expr

import (
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

// scanSpacedColon is how spacedColonAhead found its answer before
// spacedColons: a scan from pos, just after a ?, to the end of the group,
// for each ?. spacedColons must agree with it wherever the parser asks.
func scanSpacedColon(src string, pos int) bool {
	depth, later := 0, 0
	var quote byte
	for i := pos; i < len(src); i++ {
		c := src[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			if depth == 0 {
				return false
			}
			depth--
		case depth > 0:
		case c == ',':
			return false
		case c == '?' && i+1 < len(src) && src[i+1] == '?':
			i++
		case c == '?' && isSpace(src[i-1]):
			later++
		case c == ':' && isSpace(src[i-1]):
			if later == 0 {
				return true
			}
			later--
		}
	}
	return false
}

// parsesAsScanned checks that src parses, or fails to, as it did when each
// ? scanned ahead for its ':' (scanSpacedColon), and that spacedColons
// gives the scan's answer at each offset just after a ? that isn't in
// quotes or followed by another, which are the offsets the parser can ask
// about.
func parsesAsScanned(t *testing.T, src string) {
	t.Helper()
	scanned := make([]bool, len(src)+1)
	for i := 1; i <= len(src); i++ {
		if src[i-1] == '?' {
			scanned[i] = scanSpacedColon(src, i)
		}
	}
	want, wantErr := (&parser{src: src, spaced: scanned}).parse()
	got, gotErr := Parse(src)
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotErr, wantErr) {
		t.Fatalf("%q parses as %#v, %v; as scanned, %#v, %v", src, got, gotErr, want, wantErr)
	}
	spaced := spacedColons(src)
	var quote byte
	for i := 1; i <= len(src); i++ {
		if c := src[i-1]; quote != 0 && c == quote {
			quote = 0
		} else if quote == 0 && (c == '\'' || c == '"' || c == '`') {
			quote = c
		}
		if src[i-1] == '?' && quote == 0 && (i == len(src) || src[i] != '?') && spaced[i] != scanned[i] {
			t.Fatalf("%q at %d: spacedColons says %v, a scan %v", src, i, spaced[i], scanned[i])
		}
	}
}

// ternarySeeds are expressions whose ternaries' colons a scan from each ?
// finds in different places, or not at all.
var ternarySeeds = []string{
	"true ? 12:30 : 13:00", "a ? b ? 12:30 : d : e", "inputs.flag?1:2", "true?1:x ? 2 : 3",
	"true ? false?1:2 : 3", "true ? (false?1:2) : 3", "a?b?1:2:3", "a ? b ?? c : d", "a ??? b : c",
	"format('{0}', a?1:2, b ? 3 : 4)", "a ? (b, c : d) : e", "a ? '?' : 'b : c' : d", "a?[1, 2]:3",
	"x ? inputs[(] ? 1 : 2 : 3", "x?inputs[)] ? 1 : 2", "a?b[(]?1:2", "x ? {'a': 1} : 2", "a ?\t1\n: 2",
	"map(l, x => x ? 1 : 2) ? 3 : 4", "a ? b : c ? d : e", "(a ? b : c) ? d : e", "a?`:`:b",
}

func TestSpacedColons(t *testing.T) {
	for _, src := range ternarySeeds {
		parsesAsScanned(t, src)
	}
	// Random expressions of pieces that end a ternary's group, or that a
	// scan reads differently from the parser.
	pieces := []string{
		"a", "b", "1", "12:30", "inputs.a", "inputs[(]", "x[)]", " ", "  ", "\t", "?", "??", ":", " ?", " :",
		",", "(", ")", "[", "]", "{", "}", "'", `"`, "`", "'a'", "+", "!", "-", "map(l, x => ", "format(",
	}
	r := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		var b strings.Builder
		for range r.IntN(24) {
			b.WriteString(pieces[r.IntN(len(pieces))])
		}
		parsesAsScanned(t, b.String())
	}
}

func FuzzSpacedColons(f *testing.F) {
	for _, src := range ternarySeeds {
		f.Add(src)
	}
	f.Fuzz(func(t *testing.T, src string) {
		if len(src) > 1024 {
			return
		}
		parsesAsScanned(t, src)
	})
}
