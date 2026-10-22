package jsonvalue_test

import (
	"encoding/json/jsontext"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// errNumber is what number fails with.
var errNumber = errors.New("no such number")

// number makes a number's value its text, marked as a number, so that a
// test sees the text Decode gives it. It fails for 13.
func number(text string) (any, error) {
	if text == "13" {
		return nil, errNumber
	}
	return "#" + text, nil
}

func TestDecode(t *testing.T) {
	for _, tc := range []struct {
		name, json string
		want       any
	}{
		{"null", `null`, nil},
		{"booleans and text", `[true, false, "x"]`, []any{true, false, "x"}},
		// A number's text is as it is written.
		{"numbers", `[1.50, -0, 1e3, 0]`, []any{"#1.50", "#-0", "#1e3", "#0"}},
		// An empty list or mapping is one, not nil, so that it encodes as
		// [] or {} and not as null.
		{"empty list", `[]`, []any{}},
		{"empty mapping", `{}`, map[string]any{}},
		{"nested", `{"a": [{"b": []}, {}], "c": null}`, map[string]any{"a": []any{map[string]any{"b": []any{}}, map[string]any{}}, "c": nil}},
		// A key given twice has its later value, of whatever type, as
		// encoding/json gives it.
		{"repeated key", `{"a": 1, "b": 2, "a": 3}`, map[string]any{"a": "#3", "b": "#2"}},
		{"repeated key, another type", `{"k": [1, 2], "k": {"x": 1}}`, map[string]any{"k": map[string]any{"x": "#1"}}},
		{"repeated key, nested", `{"a": {"b": 1, "b": null}}`, map[string]any{"a": map[string]any{"b": nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := jsontext.NewDecoder(strings.NewReader(tc.json), jsontext.AllowDuplicateNames(true))
			got, err := jsonvalue.Decode(dec, number)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Decode(%s) = %#v, want %#v", tc.json, got, tc.want)
			}
		})
	}
}

// An error says where in the value it is: the index of each list item and
// the key of each mapping's value it is in, outermost first.
func TestDecodeErrors(t *testing.T) {
	for _, tc := range []struct {
		name, json, prefix string
		// syntax says the error is the decoder's, and not number's.
		syntax bool
	}{
		{"number", `13`, "", false},
		{"in a list", `[1, 13]`, "1: ", false},
		{"in a mapping", `{"a": 1, "b": 13}`, "b: ", false},
		{"nested", `{"a": [0, {"b": [1, [2, 13]]}]}`, "a: 1: b: 1: 1: ", false},
		{"key with a colon", `{"a: b": [13]}`, "a: b: 0: ", false},
		{"syntax in a list", `[1, }`, "1: ", true},
		{"syntax in a mapping", `{"a": {"b" 1}}`, "a: b: ", true},
		{"cut short", `[[1, 2`, "0: 2: ", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := jsonvalue.Decode(jsontext.NewDecoder(strings.NewReader(tc.json)), number)
			if err == nil {
				t.Fatalf("Decode(%s) succeeds", tc.json)
			}
			inner, ok := strings.CutPrefix(err.Error(), tc.prefix)
			_, syntax := errors.AsType[*jsontext.SyntacticError](err)
			switch {
			case !ok:
				t.Errorf("Decode(%s) fails with %q, want it to start %q", tc.json, err, tc.prefix)
			case tc.syntax && (!syntax || !strings.HasPrefix(inner, "jsontext: ")):
				t.Errorf("Decode(%s) fails with %q, want the decoder's error after %q", tc.json, err, tc.prefix)
			case !tc.syntax && (!errors.Is(err, errNumber) || inner != errNumber.Error()):
				t.Errorf("Decode(%s) fails with %q, want number's error after %q", tc.json, err, tc.prefix)
			}
		})
	}
	// A decoder that doesn't allow a key given twice fails on one.
	if _, err := jsonvalue.Decode(jsontext.NewDecoder(strings.NewReader(`{"a": 1, "a": 2}`)), number); err == nil {
		t.Error("a repeated key decodes with a decoder that doesn't allow it")
	}
}

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
