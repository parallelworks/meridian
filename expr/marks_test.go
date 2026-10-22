package expr_test

import (
	"encoding/json"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
)

// readKeys are the keys a read's path is made of: indexes, slices and keys,
// spelled every way a path can spell them, with bounds past any list and too
// large to add to.
var readKeys = []string{
	"0", "1", "2", "3", "5", "00", "01", "+0", "+1", "-0", "-1", "007",
	":", "0:", "1:", "2:", ":1", ":2", ":3", "1:3", "3:1", "2:2", "-1:", ":-1", "-2:-1",
	"+1:", "01:", "-0:", "9:", "1:9", ":0", "0:0", "+0:+2", "1:-0", "-0:-0",
	"9223372036854775807:", "1:9223372036854775807", "9223372036854775806:", "9223372036854775807:9223372036854775807",
	"a", "x", "*",
}

// mapKeys are the keys of the mappings in the values read, some of which a
// path also reads as an index or a slice.
var mapKeys = []string{"a", "x", "0", "1", "00", "1:", "-1", ":", "0:1"}

// randomValue returns a value depth deep at most: lists, mappings, strings
// that hold JSON lists and mappings or none, and the other JSON values.
func randomValue(r *rand.Rand, depth int) any {
	if depth <= 0 || r.IntN(4) == 0 {
		switch r.IntN(9) {
		case 0:
			return float64(r.IntN(100))
		case 1:
			return "abcdef"[:r.IntN(6)]
		case 2:
			return `["p","q","r","s"]`
		case 3:
			return `{"0":"j","1:":"k","a":"l"}`
		case 4:
			return `z[10,20,30]`
		case 5:
			return nil
		case 6:
			return `[["u","v"],["w"]]`
		case 7:
			return "x" + strconv.Itoa(r.IntN(1000))
		default:
			return true
		}
	}
	if r.IntN(4) == 0 {
		m := map[string]any{}
		for _, k := range mapKeys {
			if r.IntN(3) == 0 {
				m[k] = randomValue(r, depth-1)
			}
		}
		return m
	}
	l := make([]any, r.IntN(7))
	for i := range l {
		l[i] = randomValue(r, depth-1)
	}
	return l
}

// valuePaths returns the paths of each list's item and mapping's value in
// v, after prefix.
func valuePaths(v any, prefix []string) [][]string {
	var out [][]string
	var add func(v any, prefix []string)
	add = func(v any, prefix []string) {
		switch v := v.(type) {
		case []any:
			for i, c := range v {
				p := append(slices.Clone(prefix), strconv.Itoa(i))
				out = append(out, p)
				add(c, p)
			}
		case map[string]any:
			for k, c := range v {
				p := append(slices.Clone(prefix), k)
				out = append(out, p)
				add(c, p)
			}
		}
	}
	add(v, prefix)
	return out
}

// replaced returns a copy of v with the value at path, which it holds,
// replaced by with.
func replaced(v any, path []string, with any) any {
	if len(path) == 0 {
		return with
	}
	switch v := v.(type) {
	case []any:
		out := slices.Clone(v)
		i, _ := strconv.Atoi(path[0])
		out[i] = replaced(v[i], path[1:], with)
		return out
	case map[string]any:
		out := maps.Clone(v)
		out[path[0]] = replaced(v[path[0]], path[1:], with)
		return out
	}
	return v
}

// standIns replace a value to see whether a read depends on it: one of
// each kind, a list and JSON list longer than those read, and a mapping with
// the keys read.
var standIns = []any{
	-777.5, "SENTé",
	[]any{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"},
	map[string]any{"a": "S", "x": "T", "0": "U", "1:": "V"},
	`["J0","J1","J2","J3","J4","J5","J6","J7","J8","J9","J10"]`,
}

// A reference whose result depends on a value is marked when the value is:
// for random values, and random paths of indexes, slices and keys, which
// read them, each value whose replacement changes what a path reads marks
// the reference when it is marked, whether the reference's group compares
// it with the marked paths or has indexed them.
func TestMarkedWhereReadsDepend(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 42))
	gives := func(tmpl *expr.Template, values any) string {
		v, err := tmpl.Eval(expr.Scope{Contexts: map[string]any{"inputs": values}})
		if err != nil {
			return "error " + err.Error()
		}
		b, err := json.Marshal(v)
		if err != nil {
			return "not JSON " + err.Error()
		}
		return string(b)
	}
	other, err := expr.ParseTemplate("${{ inputs.other }}")
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		values := map[string]any{"r": randomValue(r, 4)}
		if r.IntN(2) == 0 {
			values["1:"] = randomValue(r, 2)
		}
		paths := valuePaths(values, nil)
		reads := make([]*expr.Template, 100)
		srcs := make([]string, len(reads))
		// depends[i][j] is set if read i depends on the value at paths[j].
		depends := make([][]bool, len(reads))
		for i := range reads {
			var b strings.Builder
			b.WriteString("${{ inputs")
			if _, ok := values["1:"]; ok && r.IntN(4) == 0 {
				b.WriteString("['1:']")
			} else {
				b.WriteString(".r")
			}
			for range 1 + r.IntN(4) {
				b.WriteString("['" + readKeys[r.IntN(len(readKeys))] + "']")
			}
			b.WriteString(" }}")
			srcs[i] = b.String()
			if reads[i], err = expr.ParseTemplate(srcs[i]); err != nil {
				t.Fatal(err)
			}
			given := gives(reads[i], values)
			depends[i] = make([]bool, len(paths))
			for j, p := range paths {
				for _, s := range standIns {
					if gives(reads[i], replaced(values, p, s)) != given {
						depends[i][j] = true
						break
					}
				}
			}
		}
		for j, p := range paths {
			marks := expr.NewMarks(slices.Concat([]string{"inputs"}, p))
			scope := expr.Scope{Contexts: map[string]any{"inputs": values}, Marked: marks}
			indexed := scope.Budgeted()
			for range 40 {
				if _, _, err := other.EvalMarked(indexed); err != nil {
					t.Fatal(err)
				}
			}
			for i, read := range reads {
				if !depends[i][j] {
					continue
				}
				_, compared, _ := read.EvalMarked(scope)
				_, found, _ := read.EvalMarked(indexed)
				if !compared || !found {
					t.Fatalf("%s depends on %q, but isn't marked by it: %v compared with it, %v indexed; values %s",
						srcs[i], p, compared, found, gives(other, map[string]any{"other": values}))
				}
			}
		}
	}
}
