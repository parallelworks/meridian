package workerapi_test

import (
	"encoding/json/v2"
	"fmt"
	"io/fs"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// The API's schema describes each body's type, and each type of the
// package's in one, by its name, as the type is: a property for each of
// its JSON members, and no other; each member it always writes required;
// no member it doesn't have; each integer bounded as its Go type is
// (schematest.Bounds); and each member a version after the first added
// said to be of the version the since table gives it, and no other. What
// a body holds of package plan's, or of foundation's, the schema refers to
// the plan's schema for, but a problem, which it describes as foundation's
// problem.Problem writes it.
func TestSchemaDescribesTheBodies(t *testing.T) {
	data, err := fs.ReadFile(plan.Schemas, "schema/2/workerapi.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	defs, _ := doc["$defs"].(map[string]any)
	check := func(typ reflect.Type, name string) {
		def, _ := defs[name].(map[string]any)
		if def == nil {
			t.Errorf("the schema has no definition of %s", name)
			return
		}
		props, _ := def["properties"].(map[string]any)
		var described, required, members, always []string
		for k := range props {
			described = append(described, k)
		}
		for _, r := range def["required"].([]any) {
			required = append(required, r.(string))
		}
		for f := range typ.Fields() {
			member, options, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || member == "-" {
				continue
			}
			members = append(members, member)
			if !strings.Contains(options, "omitempty") && !strings.Contains(options, "omitzero") {
				always = append(always, member)
			}
			schematest.Bounds(t, name+"."+member, f.Type, props[member])
			prop, _ := props[member].(map[string]any)
			description, _ := prop["description"].(string)
			if version := workerapi.Since[name+"."+member]; version > 1 {
				if !strings.HasPrefix(description, fmt.Sprintf("Added in worker API version %d. ", version)) {
					t.Errorf("%s.%s: the schema doesn't say that worker API version %d added it", name, member, version)
				}
			} else if strings.Contains(description, "Added in worker API version") {
				t.Errorf("%s.%s: the schema says a later version added it, where it is of the first", name, member)
			}
		}
		for _, l := range [][]string{described, required, members, always} {
			slices.Sort(l)
		}
		if !slices.Equal(described, members) {
			t.Errorf("%s: the schema describes %v, where it has %v", name, described, members)
		}
		if !slices.Equal(required, always) {
			t.Errorf("%s: the schema requires %v, where it always writes %v", name, required, always)
		}
		if def["additionalProperties"] != false {
			t.Errorf("%s: the schema allows members it doesn't describe", name)
		}
	}
	pkg := reflect.TypeFor[workerapi.Claim]().PkgPath()
	seen := map[string]bool{}
	var walk func(typ reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || typ.PkgPath() != pkg || seen[typ.Name()] {
			return
		}
		seen[typ.Name()] = true
		check(typ, typ.Name())
		for f := range typ.Fields() {
			walk(f.Type)
		}
	}
	bodies := []any{
		workerapi.ClaimRequest{},
		workerapi.Claim{},
		workerapi.Control{},
		workerapi.Events{},
		workerapi.EventsAck{},
		workerapi.LogAck{},
		workerapi.Complete{},
	}
	for _, body := range bodies {
		walk(reflect.TypeOf(body))
	}
	check(reflect.TypeFor[problem.Problem](), "Problem")
	for name := range defs {
		if !seen[name] && name != "Problem" && name != "Diagnostics" {
			t.Errorf("the schema defines %s, which is in no body", name)
		}
	}
}

// Each plan version's copy of the API's schema describes the bodies alike,
// as the API's version decides them and not the plan's: it differs from
// the newest plan version's only in the plan version it is for, which its
// title and description name and its claim's planVersion holds. So an
// addition to the API that one copy lacks fails here, though the other
// tests read only the newest.
func TestSchemaIsAlikeForEachPlanVersion(t *testing.T) {
	dirs, err := fs.ReadDir(plan.Schemas, "schema")
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no versions: %v", err)
	}
	var versions []int
	for _, dir := range dirs {
		v, err := strconv.Atoi(dir.Name())
		if err != nil {
			t.Fatalf("schema/%s: %v", dir.Name(), err)
		}
		versions = append(versions, v)
	}
	slices.Sort(versions)
	read := func(version int) map[string]any {
		t.Helper()
		data, err := fs.ReadFile(plan.Schemas, fmt.Sprintf("schema/%d/workerapi.schema.json", version))
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		of := fmt.Sprintf("for runs of plan version %d", version)
		for _, k := range []string{"title", "description"} {
			text, _ := doc[k].(string)
			if !strings.Contains(text, of) {
				t.Errorf("schema/%d: its %s doesn't say it is %s", version, k, of)
			}
			doc[k] = strings.ReplaceAll(text, of, "for runs of plan version N")
		}
		defs, _ := doc["$defs"].(map[string]any)
		claim, _ := defs["Claim"].(map[string]any)
		props, _ := claim["properties"].(map[string]any)
		planVersion, _ := props["planVersion"].(map[string]any)
		if planVersion["const"] != float64(version) {
			t.Errorf("schema/%d: a claim's planVersion is %v", version, planVersion["const"])
		}
		delete(planVersion, "const")
		return doc
	}
	newest := versions[len(versions)-1]
	want := read(newest)
	wantDefs, _ := want["$defs"].(map[string]any)
	for _, version := range versions[:len(versions)-1] {
		got := read(version)
		gotDefs, _ := got["$defs"].(map[string]any)
		for name := range union(wantDefs, gotDefs) {
			if !reflect.DeepEqual(wantDefs[name], gotDefs[name]) {
				t.Errorf("schema/%d describes %s otherwise than schema/%d", version, name, newest)
			}
		}
		for k := range union(want, got) {
			if k != "$defs" && !reflect.DeepEqual(want[k], got[k]) {
				t.Errorf("schema/%d's %s is otherwise than schema/%d's", version, k, newest)
			}
		}
	}
}

// union is the keys of a and of b.
func union(a, b map[string]any) map[string]bool {
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	return keys
}
