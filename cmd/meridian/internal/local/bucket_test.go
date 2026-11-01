package local

import (
	"reflect"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// TestResolveBucket checks that a bucket input's pw://user/name URI gives
// the bucket's details as format 1 gives them, but for what only the
// platform's record holds: its type, its name and its owner, as the URI
// names them, and the URI; that "" is kept, and a mapping, the details a
// calling run passes, at a called workflow's input, where a run's own
// refuses it; that each URI of a list does, with multi; and that what
// isn't such a URI, or is of the wrong shape, is refused.
func TestResolveBucket(t *testing.T) {
	c := load(t, nil, nil)
	resolve := func(def map[string]any, v any) (any, error) {
		t.Helper()
		return c.ResolveInput(t.Context(), "bucket", def, v, false)
	}
	called := func(def map[string]any, v any) (any, error) {
		t.Helper()
		return c.ResolveInput(t.Context(), "bucket", def, v, true)
	}
	data := map[string]any{"type": "bucket", "name": "data", "user": "alice", "uri": "pw://alice/data"}
	details := map[string]any{"type": "bucket", "name": "results", "user": "bob", "uri": "pw://bob/results", "csp": "aws"}
	for _, tc := range []struct {
		def    map[string]any
		value  any
		want   any
		called bool
	}{
		{nil, "pw://alice/data", data, false},
		{nil, "pw://alice/data", data, true},
		{nil, "", "", false},
		{nil, details, details, true},
		{map[string]any{"multi": true}, []any{"pw://alice/data"}, []any{data}, false},
		{map[string]any{"multi": true}, []any{"pw://alice/data", details}, []any{data, details}, true},
		{map[string]any{"multi": true}, []any{}, []any{}, false},
		{map[string]any{"multi": true}, "", "", false},
		// The details a calling run passes have no credentials a run here
		// made, and an empty value names no bucket to make them for.
		{map[string]any{"generateCredentials": true}, details, details, true},
		{map[string]any{"generateCredentials": true}, "", "", false},
		{map[string]any{"generateCredentials": "false"}, "pw://alice/data", data, false},
	} {
		get := resolve
		if tc.called {
			get = called
		}
		got, err := get(tc.def, tc.value)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ResolveInput(bucket %v, %v), called %v = %v, %v, want %v", tc.def, tc.value, tc.called, got, err, tc.want)
		}
	}
	// A run's own input takes only a URI, as a platform's form gives it:
	// details there are refused, in a list too.
	for _, tc := range []struct {
		def   map[string]any
		value any
	}{
		{nil, details},
		{map[string]any{"multi": true}, []any{"pw://alice/data", details}},
		{map[string]any{"generateCredentials": true}, details},
	} {
		_, err := resolve(tc.def, tc.value)
		kindtest.Want(t, err, problems.BucketURIInvalid)
	}
	// Its details are the caller's: changing them changes nothing of the
	// next.
	got, _ := resolve(nil, "pw://alice/data")
	got.(map[string]any)["uri"] = "changed"
	if again, _ := resolve(nil, "pw://alice/data"); !reflect.DeepEqual(again, data) {
		t.Errorf("a change to what ResolveInput gave reached the next: %v", again)
	}

	for _, v := range []any{"data", "pw://data", "pw://", "pw:///data", "pw://alice/", "pw://alice/data/x", "alice/data", "s3://alice/data", "PW://alice/data"} {
		_, err := resolve(nil, v)
		if p := kindtest.Want(t, err, problems.BucketURIInvalid); p.Params["value"] != problems.Show(v) {
			t.Errorf("ResolveInput(bucket, %q): %v", v, p.Params)
		}
	}
	for _, v := range []any{[]any{"pw://alice/data", ""}, []any{int64(3)}} {
		_, err := resolve(map[string]any{"multi": true}, v)
		kindtest.Want(t, err, problems.BucketURIInvalid)
	}
	for _, generate := range []any{true, "true"} {
		_, err := resolve(map[string]any{"generateCredentials": generate}, "pw://alice/data")
		kindtest.Want(t, err, problems.BucketCredentialsNeedPlatform)
		_, err = resolve(map[string]any{"generateCredentials": generate, "multi": true}, []any{"pw://alice/data"})
		kindtest.Want(t, err, problems.BucketCredentialsNeedPlatform)
	}
	for _, v := range []any{int64(3), true, []any{"pw://alice/data"}} {
		_, err := resolve(nil, v)
		kindtest.Want(t, err, problems.InputNotText)
	}
	for _, v := range []any{"pw://alice/data", details, int64(3)} {
		_, err := resolve(map[string]any{"multi": true}, v)
		kindtest.Want(t, err, problems.InputNotList)
	}
}
