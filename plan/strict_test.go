package plan_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
)

// decoders are how each of the goldens' types decodes, but a job's result,
// which decodes as the worker API's bodies do (workerapi.Unmarshal), whose
// tests check it.
var decoders = map[reflect.Type]func([]byte) error{
	reflect.TypeFor[plan.Plan](): func(data []byte) error {
		_, err := plan.Decode(data)
		return err
	},
	reflect.TypeFor[plan.ReadyJob](): func(data []byte) error {
		_, err := plan.DecodeReadyJob(plan.Version, data)
		return err
	},
	reflect.TypeFor[plan.Record](): func(data []byte) error {
		_, err := plan.DecodeRecord(data)
		return err
	},
}

// Decoding refuses what the encodings never hold, as the schemas do: what
// encoding/json/v2 takes and they refuse, of each kind, among it. A plan, a
// ready job or a record is refused with ErrMalformed, but one of another
// version, with ErrVersion.
func TestDecodingRefusesWhatTheSchemasRefuse(t *testing.T) {
	type refusal struct {
		name, old, new string
		// at, if it isn't "", is where v replaces what the good one holds,
		// as schematest.Set replaces it.
		at jsontext.Pointer
		v  any
	}
	jobs := readyJobs(t)
	for _, kind := range []struct {
		typ     reflect.Type
		schema  string
		good    []byte
		refuses []refusal
	}{
		{reflect.TypeFor[plan.Plan](), "plan/schema/1/plan.schema.json", encoded(t, plan.Encode, fullPlan(t)), []refusal{
			{name: "another version", old: `"version":1`, new: `"version":2`},
			{name: "an unknown member", old: `"release":"v1.0.0"`, new: `"release":"v1.0.0","later":true`},
			{name: "a number as text", old: `"format":1`, new: `"format":"1"`},
			// A member that is always written, missing.
			{name: "a run without its source", old: `"source":{"location":"workflow.yaml","revision":"sha256:00","digest":"sha256:00","ref":{"uses":"workflow/build"}},`, new: ``},
			{name: "a run without its format", old: `"format":1,`, new: ``},
			{name: "a run without its release", old: `"release":"v1.0.0",`, new: ``},
			{name: "a job without its key", old: `{"key":"build",`, new: `{`},
			{name: "a job without its name", old: `"name":"build","strategy"`, new: `"strategy"`},
			{name: "a job without its path", old: `,"path":"/jobs/build"}]`, new: `}]`},
			{name: "a step without its path", old: `,"path":"/jobs/build/steps/1"}`, new: `}`},
			{name: "a diagnostic without its code", old: `"code":"unknown_key",`, new: ``},
			// Null where a member is never null.
			{name: "a step that is null", old: `"steps":[{"id":"greet"`, new: `"steps":[null,{"id":"greet"`},
			{name: "a key that is null", old: `{"key":"build",`, new: `{"key":null,`},
			{name: "a name that is null", old: `"secrets":["inputs.pw"`, new: `"secrets":[null`},
			{name: "inputs that are null", old: `"inputs":{"$":{"$x":"${{ data }}"},"n":3,"name":"Alice"}`, new: `"inputs":null`},
			// What a template's parts and a tree always have, missing.
			{name: "a template part that is null", old: `{"${{":["hello ",`, new: `{"${{":[null,`},
			{name: "an expression without its source", old: `{"src":" inputs.os ","tree":`, new: `{"tree":`},
			{name: "a reference without its path", old: `"tree":{"path":["os"],"ref":"inputs"}`, new: `"tree":{"ref":"inputs"}`},
			{name: "an index that isn't true", old: `{"index":true}`, new: `{"index":-1}`},
			{name: "an unknown node", old: `{"word":"debug"}`, new: `{"words":"debug"}`},
			{name: "a node of two kinds", old: `{"word":"debug"}`, new: `{"word":"debug","lit":1}`},
			{name: "a template of text", old: `{"${{":["hello ",{"src":" inputs.name ","tree":{"path":["name"],"ref":"inputs"}}]}`, new: `{"${{":["hello "]}`},
			{name: "an unmarked $ key", old: `"$":{"$wizard":"x"}`, new: `"$wizard":"x"`},
			{name: "a segment of two kinds", old: `{"each":true}`, new: `{"each":true,"index":true}`},
			// What a member holds that its type refuses.
			{name: "a name that's empty", old: `"private":["inputs.region"]`, new: `"private":[""]`},
			{name: "format 0", old: `"format":1`, new: `"format":0`},
			{name: "format -1", old: `"format":1`, new: `"format":-1`},
			{name: "format past 32 bits", old: `"format":1`, new: `"format":2147483648`},
			{name: "a pointer that isn't one", old: `"withheld":["/on/`, new: `"withheld":["on/`},
			{name: "a diagnostic's extra", old: `"code":"unknown_key"`, new: `"code":"unknown_key","severity":"warning"`},
			{name: "a diagnostic's in that isn't one", old: `"code":"unknown_key"`, new: `"code":"unknown_key","in":"body"`},
		}},
		{reflect.TypeFor[plan.ReadyJob](), "plan/schema/1/readyjob.schema.json", encoded(t, plan.EncodeReadyJob, jobs[0]), []refusal{
			{name: "a ready job without its key", old: `{"key":"build#1",`, new: `{`},
			{name: "a ready job without its steps", at: "/steps", v: schematest.Removed},
			{name: "a ready job's step without its path", old: `,"path":"/jobs/build/steps/1"}`, new: `}`},
			{name: "a target's environment without its cluster", old: `"cluster":"owner/cluster",`, new: ``},
			{name: "a field's name that's empty", old: `"privateFields":["target"`, new: `"privateFields":[""`},
			{name: "a matrix that is null", old: `"matrix":{"os":"linux"}`, new: `"matrix":null`},
			{name: "a retry's reasons left out", at: "/retry/reasons", v: schematest.Removed},
		}},
		{reflect.TypeFor[plan.ReadyJob](), "plan/schema/1/readyjob.schema.json", encoded(t, plan.EncodeReadyJob, jobs[1]), []refusal{
			{name: "a target of two kinds", old: `"target":{"group":"gpu"}`, new: `"target":{"group":"gpu","inline":{"name":"ssh","backend":"ssh","isolation":"host","ssh":null}}`},
		}},
		{reflect.TypeFor[plan.Record](), "plan/schema/1/record.schema.json", encoded(t, plan.EncodeRecord, sampleRecord()), []refusal{
			{name: "submitted values that are null", old: `"submitted":{"n":3,"name":"Alice","note":"${{ not.parsed }}","pw":"hunter2","rows":[{"pass":"p","user":"a"},null]}`, new: `"submitted":null`},
			{name: "submitted values that are a list", old: `"submitted":{"n":3,"name":"Alice","note":"${{ not.parsed }}","pw":"hunter2","rows":[{"pass":"p","user":"a"},null]}`, new: `"submitted":[]`},
			{name: "a plan's digest that is null", at: "/planDigest"},
			{name: "a run's values without their format", old: `"run":{"format":1,`, new: `"run":{`},
			{name: "a run's values of format 0", old: `"run":{"format":1,`, new: `"run":{"format":0,`},
			{name: "a secret's name that's empty", old: `"secrets":["env.TOKEN"`, new: `"secrets":[""`},
			{name: "an instance without its matrix", old: `,"matrix":{"region":"east"}`, new: ``},
			{name: "an index before the first", old: `"index":0`, new: `"index":-1`},
			{name: "an index past 32 bits", old: `"index":0`, new: `"index":2147483648`},
		}},
		{reflect.TypeFor[plan.JobResult](), "plan/schema/1/jobresult.schema.json", encoded(t, marshalResult, fullJobResult()), []refusal{
			{name: "an outcome that isn't one", old: `"outcome":"failure"`, new: `"outcome":"failed"`},
			{name: "an empty outcome", old: `"outcome":"failure"`, new: `"outcome":""`},
		}},
	} {
		decode := decoders[kind.typ]
		if decode == nil {
			decode = func(data []byte) error {
				var r plan.JobResult
				return json.Unmarshal(data, &r, json.RejectUnknownMembers(true))
			}
		}
		s := schematest.Schema(t, kind.schema)
		good := string(kind.good)
		for _, r := range kind.refuses {
			data := strings.Replace(good, r.old, r.new, 1)
			if r.at != "" {
				data = string(schematest.Set(t, kind.good, r.at, r.v))
			}
			if data == good {
				t.Fatalf("%s: the golden %s changed", r.name, kind.typ.Name())
			}
			if err := schematest.Check(s, []byte(data)); err == nil {
				t.Errorf("%s: fits the schema", r.name)
			}
			err := decode([]byte(data))
			switch {
			case err == nil:
				t.Errorf("%s: decodes", r.name)
			case kind.typ == reflect.TypeFor[plan.JobResult]():
			case r.name == "another version" && !errors.Is(err, plan.ErrVersion),
				r.name != "another version" && !errors.Is(err, plan.ErrMalformed):
				t.Errorf("%s: %v, want ErrVersion for another version and ErrMalformed otherwise", r.name, err)
			}
		}
	}
}

// Decoding takes what the schemas take, and refuses what they refuse, of
// each change to the goldens that Agree makes: each member left out, an
// unknown one added, and each value null, true, or for a number, one at an
// integer's bounds and past them. A binary operator without its y is a
// unary one to the schema, which leaves which operators there are, as
// which contexts and functions there are, to the decoder.
func TestDecodingAgreesWithTheSchemas(t *testing.T) {
	operators := func(change string) bool { return change == `no "y"` }
	for _, g := range goldens {
		decode := decoders[g.typ]
		if decode == nil {
			continue
		}
		for i, data := range g.encode(t) {
			t.Run(fmt.Sprintf("%s/%d", g.file, i), func(t *testing.T) {
				t.Parallel()
				schematest.Agree(t, g.schema, g.file, data, decode, operators)
			})
		}
	}
}

// A plan, or a ready job, of this version whose expressions name what this
// release doesn't know, though the schema takes it, as a function its
// format gained, is a newer release's: decoding it fails with ErrVersion,
// which a coordinator leaves to a replica of a release that reads it,
// rather than ErrMalformed, which fails the run.
func TestDecodingANewerReleasesNames(t *testing.T) {
	plans := encoded(t, plan.Encode, fullPlan(t))
	job := encoded(t, plan.EncodeReadyJob, readyJobs(t)[0])
	for _, tc := range []struct{ name, old, new string }{
		{"a function", `"call":"contains"`, `"call":"containsAll"`},
		{"a function's arguments", `{"lit":"a"}],"call":"contains"`, `{"lit":"a"},{"lit":"b"}],"call":"contains"`},
		{"a context", `"ref":"inputs"`, `"ref":"secrets"`},
		{"a binary operator", `"op":"=="`, `"op":"=~"`},
		{"a unary operator", `"op":"!"`, `"op":"~"`},
	} {
		for _, kind := range []struct {
			what, schema string
			good         []byte
			decode       func([]byte) error
		}{
			{"plan", "plan/schema/1/plan.schema.json", plans, decoders[reflect.TypeFor[plan.Plan]()]},
			{"ready job", "plan/schema/1/readyjob.schema.json", job, decoders[reflect.TypeFor[plan.ReadyJob]()]},
		} {
			data := strings.Replace(string(kind.good), tc.old, tc.new, 1)
			if data == string(kind.good) {
				t.Fatalf("%s: the golden %s changed", tc.name, kind.what)
			}
			schematest.Validate(t, schematest.Schema(t, kind.schema), tc.name, []byte(data))
			if err := kind.decode([]byte(data)); !errors.Is(err, plan.ErrVersion) || errors.Is(err, plan.ErrMalformed) {
				t.Errorf("a %s with %s this release doesn't know: %v, want ErrVersion", kind.what, tc.name, err)
			}
		}
	}
}

// An integer member is a whole number written as Encode writes one:
// decoding refuses 1.0, 1e0 and -0.0, which JSON Schema takes for
// integers, as the schemas' description says.
func TestDecodingRefusesIntegersWrittenAsFractions(t *testing.T) {
	plans := string(encoded(t, plan.Encode, fullPlan(t)))
	record := string(encoded(t, plan.EncodeRecord, sampleRecord()))
	for _, tc := range []struct {
		name, good, old, new, schema string
		decode                       func([]byte) error
	}{
		{"a format of 1.0", plans, `"format":1`, `"format":1.0`, "plan/schema/1/plan.schema.json", decoders[reflect.TypeFor[plan.Plan]()]},
		{"a format of 1e0", plans, `"format":1`, `"format":1e0`, "plan/schema/1/plan.schema.json", decoders[reflect.TypeFor[plan.Plan]()]},
		{"an index of -0.0", record, `"index":0`, `"index":-0.0`, "plan/schema/1/record.schema.json", decoders[reflect.TypeFor[plan.Record]()]},
	} {
		data := strings.Replace(tc.good, tc.old, tc.new, 1)
		if data == tc.good {
			t.Fatalf("%s: the golden changed", tc.name)
		}
		schematest.Validate(t, schematest.Schema(t, tc.schema), tc.name, []byte(data))
		if err := tc.decode([]byte(data)); !errors.Is(err, plan.ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", tc.name, err)
		}
	}
}
