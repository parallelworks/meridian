package plan

import (
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// Values named by their paths, as RunValues and Contexts name them: a
// context, then the keys that lead to the value from there, a list's items
// by their indexes, joined with dots.

// splitPath returns the keys of a value's path, its context's name first.
func splitPath(name string) []string { return strings.Split(name, ".") }

// tree returns r's values as one mapping, by context, in which each path
// names what it names in r: inputs, env, org, vars, remote and sessions.
// It shares no list or mapping with r.
func (r RunValues) tree() map[string]any {
	t := r.view()
	if v, ok := t["inputs"]; ok {
		t["inputs"] = jsonvalue.Clone(v)
	}
	return t
}

// view returns r's values as tree does, but for the input values, which it
// shares with r rather than copy, for what only reads them.
func (r RunValues) view() map[string]any {
	t := map[string]any{}
	if !r.Inputs.IsZero() {
		t["inputs"] = r.Inputs.Get()
	}
	for name, m := range r.stringContexts() {
		if *m != nil {
			t[name] = anyMap(*m)
		}
	}
	return t
}

// fromTree returns the values that t, as tree gives them, holds.
func fromTree(t map[string]any) RunValues {
	var r RunValues
	if v, ok := t["inputs"]; ok {
		r.Inputs = DataOf(v)
	}
	for name, m := range r.stringContexts() {
		if v, ok := t[name].(map[string]any); ok {
			*m = stringMap(v)
		}
	}
	return r
}

// stringContexts returns r's contexts whose values are strings, by name.
func (r *RunValues) stringContexts() map[string]*map[string]string {
	return map[string]*map[string]string{
		"env": &r.Env, "org": &r.Org, "vars": &r.Vars, "remote": &r.Remote, "sessions": &r.Sessions,
	}
}

// privatePaths returns the paths of what of r is private: org, vars, and
// what Secrets and Private name, sorted, without a path under another.
func (r RunValues) privatePaths() [][]string {
	paths := [][]string{{"org"}, {"vars"}}
	for _, names := range [][]string{r.Secrets, r.Private} {
		for _, name := range names {
			paths = append(paths, splitPath(name))
		}
	}
	return outermost(paths)
}

// outermost returns paths sorted, without one that is another or is under
// another.
func outermost(paths [][]string) [][]string {
	slices.SortFunc(paths, slices.Compare)
	out := paths[:0]
	for _, p := range paths {
		if n := len(out); n > 0 && isPrefix(out[n-1], p) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// isPrefix reports whether path is prefix or is under it.
func isPrefix(prefix, path []string) bool {
	return len(prefix) <= len(path) && slices.Equal(prefix, path[:len(prefix)])
}

// setPath sets the value at path in t, making the mappings and lists on
// the way to it that aren't there.
func setPath(t map[string]any, path []string, v any) {
	t[path[0]] = setIn(t[path[0]], path[1:], v)
}

// setIn returns c with the value at path in it set to v, making the
// mappings and lists on the way that aren't there: a list's item is at its
// index, after a null for each item before it that the list doesn't have.
// What is on the way and can't hold the next key is replaced.
func setIn(c any, path []string, v any) any {
	if len(path) == 0 {
		return v
	}
	key := path[0]
	m, isMap := c.(map[string]any)
	if n, ok := index(key); ok && !isMap {
		l, _ := c.([]any)
		if n >= len(l) {
			l = append(l, make([]any, n+1-len(l))...)
		}
		l[n] = setIn(l[n], path[1:], v)
		return l
	}
	if !isMap {
		m = map[string]any{}
	}
	m[key] = setIn(m[key], path[1:], v)
	return m
}

// index returns the index that key names, if it is one as a path writes
// it: 0, or digits that don't start with 0.
func index(key string) (int, bool) {
	n, err := strconv.Atoi(key)
	return n, err == nil && n >= 0 && strconv.Itoa(n) == key
}

// removePath removes the value at path in t: a mapping's key, or a list's
// item, whose place a null keeps so that the items after it keep their
// indexes.
func removePath(t map[string]any, path []string) {
	var v any = t
	for i, key := range path {
		last := i == len(path)-1
		switch c := v.(type) {
		case map[string]any:
			if last {
				delete(c, key)
				return
			}
			v = c[key]
		case []any:
			n, ok := index(key)
			if !ok || n >= len(c) {
				return
			}
			if last {
				c[n] = nil
				return
			}
			v = c[n]
		default:
			return
		}
	}
}

// anyMap returns m as a mapping of JSON values, or nil for nil.
func anyMap(m map[string]string) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// stringMap returns m, a mapping of strings, as strings.
func stringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k], _ = v.(string)
	}
	return out
}
