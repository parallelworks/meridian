package compile

import (
	"slices"
	"strconv"
	"testing"

	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
)

// Each row's fields are the row's own, copied from the template's: a group
// in the row holds the row's fields, not the template's or another row's.
func TestRowsHaveTheirOwnFields(t *testing.T) {
	doc, diags := format.Load("workflow.yaml", []byte(`on:
  execute:
    inputs:
      l:
        type: list
        template:
          g:
            type: group
            items:
              a: {type: string, optional: true}
              b: {type: string, optional: true}
          c: {type: string, optional: true}
jobs:
  main:
    steps:
      - run: echo ok
`))
	if diags.HasErrors() {
		t.Fatal(diags)
	}
	r := newInputResolver(doc, map[string]any{"l": []any{map[string]any{}, map[string]any{}, map[string]any{}}}, Options{})
	r.resolve()
	l := r.fields[slices.IndexFunc(r.fields, func(f *field) bool { return f.key == "l" })]
	if len(l.rows) != 3 {
		t.Fatalf("l has %d rows, want 3", len(l.rows))
	}
	for i, row := range l.rows {
		var keys []string
		for _, f := range row {
			keys = append(keys, f.key)
			if f.index[0] != i || f.in != l || !slices.Equal(f.path[:2], []string{"l", strconv.Itoa(i)}) {
				t.Errorf("row %d's field %s is at %v, index %v", i, f.key, f.path, f.index)
			}
		}
		if !slices.Equal(keys, []string{"c", "g", "a", "b"}) {
			t.Fatalf("row %d's fields are %v", i, keys)
		}
		g := row[1]
		if !slices.Equal(g.children, row[2:]) || row[2].group != g || row[3].group != g || row[0].group != nil {
			t.Errorf("row %d's group holds %v, and its fields are in %p and %p, not %p", i, g.children, row[2].group, row[3].group, g)
		}
	}
	for _, f := range r.templates[jsonptr.Format([]string{"on", "execute", "inputs", "l", "template"})].fields {
		if f.key == "g" && len(f.children) != 2 {
			t.Errorf("the template's group holds %d fields, want 2", len(f.children))
		}
	}
}
