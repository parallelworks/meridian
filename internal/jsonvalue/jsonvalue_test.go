package jsonvalue_test

import (
	"reflect"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

func TestClone(t *testing.T) {
	v := map[string]any{"list": []any{map[string]any{"x": 1}}, "s": "text"}
	c := jsonvalue.Clone(v).(map[string]any)
	if !reflect.DeepEqual(c, v) {
		t.Fatalf("Clone = %v, want %v", c, v)
	}
	c["list"].([]any)[0].(map[string]any)["x"] = 2
	c["s"] = "changed"
	if v["list"].([]any)[0].(map[string]any)["x"] != 1 || v["s"] != "text" {
		t.Errorf("changing the copy changed the original: %v", v)
	}
	if m := jsonvalue.CloneMap(nil); m == nil || len(m) != 0 {
		t.Errorf("CloneMap(nil) = %#v, want an empty mapping", m)
	}
}

func TestSorted(t *testing.T) {
	var keys []string
	var values []int
	for k, v := range jsonvalue.Sorted(map[string]int{"b": 2, "c": 3, "a": 1}) {
		keys, values = append(keys, k), append(values, v)
		if k == "b" {
			break
		}
	}
	if !slices.Equal(keys, []string{"a", "b"}) || !slices.Equal(values, []int{1, 2}) {
		t.Errorf("got %v, %v", keys, values)
	}
}
