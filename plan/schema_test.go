package plan_test

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
)

// schemaDefs returns the definitions of the plan's schema of this version.
func schemaDefs(t *testing.T) map[string]any {
	t.Helper()
	data, err := fs.ReadFile(plan.Schemas, "schema/1/plan.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	defs, _ := doc["$defs"].(map[string]any)
	return defs
}

// The schema describes each encoded type, by its name, as the type is: a
// property for each of its JSON members, and no other; each member it
// always writes required; and no member it doesn't have. A diagnostic's
// definition is foundation's problem.FieldError's members, as it writes
// them.
func TestSchemaDescribesTheTypes(t *testing.T) {
	defs := schemaDefs(t)
	check := func(typ reflect.Type, name string) {
		def, _ := defs[name].(map[string]any)
		if def == nil {
			t.Errorf("the schema has no definition of %s", name)
			return
		}
		props, _ := def["properties"].(map[string]any)
		var required []string
		for _, r := range asList(def["required"]) {
			required = append(required, r.(string))
		}
		var members, always []string
		for f := range typ.Fields() {
			member := jsonName(f)
			if member == "" {
				continue
			}
			members = append(members, member)
			if _, options, _ := strings.Cut(f.Tag.Get("json"), ","); !strings.Contains(options, "omitempty") && !strings.Contains(options, "omitzero") {
				always = append(always, member)
			}
		}
		slices.Sort(members)
		slices.Sort(always)
		slices.Sort(required)
		var described []string
		for k := range props {
			described = append(described, k)
		}
		slices.Sort(described)
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
	seen := map[reflect.Type]bool{}
	var walk func(typ reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || slices.Contains(opaqueTypes, typ) || seen[typ] {
			return
		}
		seen[typ] = true
		check(typ, typ.Name())
		for f := range typ.Fields() {
			if jsonName(f) != "" {
				walk(f.Type)
			}
		}
	}
	for _, root := range encodedRoots {
		walk(root)
	}
	check(reflect.TypeFor[problem.FieldError](), "Diagnostic")
	// Each definition is of a type, or of how the types are written.
	names := map[string]bool{}
	for typ := range seen {
		names[typ.Name()] = true
	}
	for name := range defs {
		if !names[name] && !slices.Contains(writtenDefs, name) {
			t.Errorf("the schema defines %s, which is no encoded type", name)
		}
	}
}

// encodedRoots are the types the plan's encoding has, which hold the rest.
var encodedRoots = []reflect.Type{
	reflect.TypeFor[plan.Plan](), reflect.TypeFor[plan.ReadyJob](), reflect.TypeFor[plan.Record](),
	reflect.TypeFor[plan.RunValues](), reflect.TypeFor[plan.JobResult](),
}

// writtenDefs are the schema's definitions that aren't of a struct type:
// of how a member is written.
var writtenDefs = []string{
	"Diagnostic", "Reason", "Outcome", "Pointers", "Names", "Keys", "Strings", "Values",
	"Data", "Value", "Template", "Expr", "Node", "Path",
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

// The schemas refuse what the encodings never hold.
func TestSchemasRefuse(t *testing.T) {
	good := string(encoded(t, plan.Encode, fullPlan(t)))
	s := schematest.Schema(t, "plan/schema/1/plan.schema.json")
	for name, data := range map[string]string{
		"another version":        strings.Replace(good, `"version":1`, `"version":2`, 1),
		"an unknown member":      strings.Replace(good, `"release":"v1.0.0"`, `"release":"v1.0.0","later":true`, 1),
		"a job without its path": strings.Replace(good, `,"path":"/jobs/build"}]`, `}]`, 1),
		"an unknown node":        strings.Replace(good, `{"word":"debug"}`, `{"words":"debug"}`, 1),
		"a node of two kinds":    strings.Replace(good, `{"word":"debug"}`, `{"word":"debug","lit":1}`, 1),
		"a template of text":     strings.Replace(good, `{"${{":["hello ",{"src":" inputs.name ","tree":{"path":["name"],"ref":"inputs"}}]}`, `{"${{":["hello "]}`, 1),
		"an unmarked $ key":      strings.Replace(good, `"$":{"$wizard":"x"}`, `"$wizard":"x"`, 1),
		"a segment of two kinds": strings.Replace(good, `{"each":true}`, `{"each":true,"index":true}`, 1),
		"a number as text":       strings.Replace(good, `"format":1`, `"format":"1"`, 1),
		"a name that's empty":    strings.Replace(good, `"private":["inputs.region"]`, `"private":[""]`, 1),
		"a diagnostic's extra":   strings.Replace(good, `"code":"unknown_key"`, `"code":"unknown_key","severity":"warning"`, 1),
	} {
		if data == good {
			t.Fatalf("%s: the golden plan changed", name)
		}
		if err := schematest.Check(s, []byte(data)); err == nil {
			t.Errorf("%s: fits the schema", name)
		}
	}
	jobs := schematest.Schema(t, "plan/schema/1/readyjob.schema.json")
	job := string(encoded(t, plan.EncodeReadyJob, readyJobs(t)[1]))
	if err := schematest.Check(jobs, []byte(strings.Replace(job, `"target":{"group":"gpu"}`, `"target":{"group":"gpu","inline":{"name":"ssh","backend":"ssh","isolation":"host","ssh":null}}`, 1))); err == nil {
		t.Error("a target of two kinds fits the schema")
	}
}
