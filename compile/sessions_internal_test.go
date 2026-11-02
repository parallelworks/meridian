package compile

import (
	"encoding/json/v2"
	"maps"
	"os"
	"slices"
	"testing"
)

// TestUpdateSessionKeys checks that the keys UpdateSession reads
// update-session's with: and its targetInfo as, case aside, are those the
// schema of format 1 defines for them.
func TestUpdateSessionKeys(t *testing.T) {
	data, err := os.ReadFile("../format/schema/1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	var with map[string]any
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			if cond, ok := v["if"].(map[string]any); ok && lookupPath(cond, "properties", "uses", "const") == "parallelworks/update-session" {
				with, _ = lookupPath(v, "then", "properties", "with", "properties").(map[string]any)
			}
			for _, x := range v {
				walk(x)
			}
		case []any:
			for _, x := range v {
				walk(x)
			}
		}
	}
	walk(schema)
	if got, want := slices.Sorted(maps.Keys(with)), slices.Sorted(slices.Values(updateSessionKeys)); !slices.Equal(got, want) {
		t.Errorf("the schema defines update-session's with: as %q, and UpdateSession reads %q", got, want)
	}
	info, _ := lookupPath(with, "targetInfo", "properties").(map[string]any)
	if got, want := slices.Sorted(maps.Keys(info)), slices.Sorted(slices.Values(targetInfoKeys)); !slices.Equal(got, want) {
		t.Errorf("the schema defines targetInfo as %q, and UpdateSession reads %q", got, want)
	}
}

// lookupPath returns what is at the keys of v, nested mappings, or nil.
func lookupPath(v any, keys ...string) any {
	for _, k := range keys {
		m, _ := v.(map[string]any)
		v = m[k]
	}
	return v
}
