package jsonptr_test

import (
	"slices"
	"testing"

	"github.com/parallelworks/meridian/internal/jsonptr"
)

func TestFormatAndSplit(t *testing.T) {
	for _, tc := range []struct {
		pointer string
		tokens  []string
	}{
		{"", nil},
		{"/", []string{""}},
		{"/jobs/build", []string{"jobs", "build"}},
		{"/a~1b/m~0n", []string{"a/b", "m~n"}},
		// ~01 is ~1, not /.
		{"/~01", []string{"~1"}},
		{"/steps/0", []string{"steps", "0"}},
	} {
		if got := jsonptr.Format(tc.tokens); got != tc.pointer {
			t.Errorf("Format(%q) = %q, want %q", tc.tokens, got, tc.pointer)
		}
		got, err := jsonptr.Split(tc.pointer)
		if err != nil || !slices.Equal(got, tc.tokens) {
			t.Errorf("Split(%q) = %q, %v, want %q", tc.pointer, got, err, tc.tokens)
		}
	}
	for _, bad := range []string{"jobs", "/a~", "/a~2", "/~~1"} {
		if got, err := jsonptr.Split(bad); err == nil {
			t.Errorf("Split(%q) = %q, want an error", bad, got)
		}
	}
	if got := jsonptr.Append("/jobs", "a/b", "0"); got != "/jobs/a~1b/0" {
		t.Errorf("Append = %q", got)
	}
}

func TestLookup(t *testing.T) {
	v := map[string]any{
		"list": []any{"a", "b", map[string]any{"": "empty key"}},
		"a/b":  1,
	}
	for _, tc := range []struct {
		tokens []string
		want   any
		ok     bool
	}{
		{nil, v, true},
		{[]string{"a/b"}, 1, true},
		{[]string{"list", "1"}, "b", true},
		{[]string{"list", "2", ""}, "empty key", true},
		{[]string{"missing"}, nil, false},
		{[]string{"a/b", "x"}, nil, false},
		// An index is written as RFC 6901 writes one, and is in range.
		{[]string{"list", "01"}, nil, false},
		{[]string{"list", "+1"}, nil, false},
		{[]string{"list", "-1"}, nil, false},
		{[]string{"list", ""}, nil, false},
		{[]string{"list", "-"}, nil, false},
		{[]string{"list", "3"}, nil, false},
		{[]string{"list", "99999999999999999999999"}, nil, false},
	} {
		got, ok := jsonptr.Lookup(v, tc.tokens)
		if ok != tc.ok || tc.ok && len(tc.tokens) > 0 && got != tc.want {
			t.Errorf("Lookup(%q) = %v, %v; want %v, %v", tc.tokens, got, ok, tc.want, tc.ok)
		}
	}
}
