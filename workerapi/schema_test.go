package workerapi_test

import (
	"encoding/json/v2"
	"io/fs"
	"reflect"
	"slices"
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
// no member it doesn't have; and each integer bounded as its Go type is
// (schematest.Bounds). What a body holds of package plan's, or
// of foundation's, the schema refers to the plan's schema for, but a
// problem, which it describes as foundation's problem.Problem writes it.
func TestSchemaDescribesTheBodies(t *testing.T) {
	data, err := fs.ReadFile(plan.Schemas, "schema/1/workerapi.schema.json")
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
