package hint_test

import (
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

func TestJoinList(t *testing.T) {
	for _, tc := range []struct {
		items []string
		want  string
	}{
		{nil, ""},
		{[]string{"a"}, "a"},
		{[]string{"a", "b"}, "a or b"},
		{[]string{"a", "b", "c"}, "a, b or c"},
	} {
		if got := hint.JoinList(tc.items, "or"); got != tc.want {
			t.Errorf("JoinList(%q) = %q, want %q", tc.items, got, tc.want)
		}
	}
}
