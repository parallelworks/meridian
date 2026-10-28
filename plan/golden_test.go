package plan_test

import (
	"bytes"
	"encoding/json/v2"
	"flag"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/schematest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

var update = flag.Bool("update", false, "rewrite the golden plan, ready jobs, record and job result in testdata")

// expressions holds an expression of each kind of node a tree has, and
// each kind of segment a path has (expr's JSON).
var expressions = map[string]any{
	"literals":  "${{ 1 }} ${{ 'text' }} ${{ null }} ${{ true }} ${{ 1.5e300 }}",
	"numbers":   "${{ NaN }} ${{ 1e999 }} ${{ -1e999 }}",
	"word":      "${{ debug }}",
	"reference": "${{ inputs['my-key'].*.n }} ${{ inputs.rows[index].name }} ${{ inputs.l[1:2] }}",
	"relative":  "${{ ..region.default }}",
	"operators": "${{ !inputs.flag }} ${{ 'a' !in inputs.list }} ${{ inputs.a == 1 && inputs.b != 2 }}",
	"ternary":   "${{ inputs.a ? 'x' : 'y' }}",
	"member":    "${{ fromJSON(inputs.j).k.*.v }}",
	"call":      "${{ contains(inputs.list, 'a') }}",
	"lambda":    "${{ map(inputs.l, (x, i) => x.a) }} ${{ filter(inputs.l, x => x) }}",
	"slices":    "${{ (inputs.a):(inputs.b) }} ${{ (inputs.a):5 }} ${{ (inputs.a): }} ${{ 2:(inputs.b) }}",
}

// fullPlan is a plan that sets every member of each of its types, and
// holds an expression of each kind.
func fullPlan(t testing.TB) *plan.Plan {
	source := plan.Source{
		Location: "github.com/example/workflows/sub.yaml", Revision: "0123456789abcdef0123456789abcdef01234567", Digest: "sha256:01",
		Ref:    plan.SourceRef{Uses: "github/example/workflows@v1", Path: "sub.yaml", Host: "github.com"},
		Remote: map[string]string{"repo": "https://github.com/example/workflows", "branch": "v1", "yaml": "sub.yaml"},
	}
	return &plan.Plan{
		Version: plan.Version,
		Run: plan.Run{
			Source:      plan.Source{Location: "workflow.yaml", Revision: "sha256:00", Digest: "sha256:00", Ref: plan.SourceRef{Uses: "workflow/build"}},
			Format:      1,
			Release:     "v1.0.0",
			Name:        "build",
			Description: "Builds the project.",
			Icon:        "icon.svg",
			Thumbnail:   "thumbnail.png",
			Inputs:      plan.DataOf(map[string]any{"name": "Alice", "n": int64(3), "$": map[string]any{"$x": "${{ data }}"}}),
			Definitions: mustValue(t, map[string]any{
				"name":   map[string]any{"type": "string", "default": "${{ org.NAME }}", "hidden": "${{ vars.H }}"},
				"region": map[string]any{"type": "string", "default": "east"},
				"$meta":  map[string]any{"$wizard": "x"},
			}),
			Withheld:      []string{"/on/execute/inputs/pw/default"},
			Decisions:     []string{"/on/execute/inputs/name/hidden"},
			Env:           map[string]plan.Value{"GREETING": mustValue(t, "hello ${{ inputs.name }}")},
			Timeout:       mustValue(t, "${{ inputs.n }}h"),
			Remote:        map[string]string{"repo": "https://example.com/r", "branch": "main"},
			Sessions:      mustValue(t, map[string]any{"desktop": map[string]any{"useTLS": false, "redirect": "${{ inputs.x }}"}}),
			SessionNames:  map[string]string{"desktop": "desktop-1"},
			Secrets:       []string{"inputs.pw", "inputs.typed", "vars.TOKEN"},
			Derived:       []string{"inputs.typed"},
			Private:       []string{"inputs.region"},
			Variables:     []string{"org.NAME", "vars.H"},
			TreeVariables: []string{"org", "vars.DEPLOY~1KEY"},
			Permissions:   []string{"*"},
			Limits:        plan.Limits{MaxInstances: 10},
			Parent: &plan.ParentRun{
				Run: "7", Job: "deploy#1", Step: 2, Try: 1, Root: "3", Depth: 2,
				Env:     map[string]string{"STAGE": "prod", "PW_MATRIX_INDEX": "1"},
				Private: []string{"env.REGION", "inputs.region"},
				Secrets: []string{"env.TOKEN", "inputs.creds.key"},
			},
		},
		Jobs: []plan.Job{{
			Key:  "build",
			Name: "build",
			Strategy: &plan.Strategy{
				Matrix:      mustValue(t, map[string]any{"os": []any{"linux", "${{ inputs.os }}"}, "include": []any{map[string]any{"os": "mac"}}}),
				FailFast:    mustValue(t, "${{ vars.FAST }}"),
				MaxParallel: plan.Literal(int64(2)),
			},
			Needs:            []plan.Need{{Job: "prepare"}, {Job: "probe", Any: true}},
			NeedsExpr:        mustValue(t, "${{ fromJSON(vars.NEEDS) }}"),
			If:               mustValue(t, "${{ always() && needs.prepare.result == 'failure' }}"),
			RunsOn:           mustValue(t, map[string]any{"execution-group": "${{ inputs.group }}"}),
			Resources:        mustValue(t, map[string]any{"cpu": int64(2), "memory": "8Gi"}),
			SSH:              mustValue(t, map[string]any{"remoteHost": "${{ inputs.host }}"}),
			Retry:            &plan.JobRetry{Reasons: []string{"worker-lost"}, MaxRetries: plan.Literal(int64(3)), Interval: plan.Literal("30s")},
			Timeout:          plan.Literal("1h"),
			Env:              map[string]plan.Value{"OS": mustValue(t, "${{ matrix.os }}")},
			WorkingDirectory: plan.Literal("/work"),
			Steps: []plan.Step{{
				ID:               "greet",
				Name:             mustValue(t, "greet on ${{ matrix.os }}"),
				If:               plan.Literal(nil),
				Run:              mustValue(t, "echo ${{ env.GREETING }}"),
				With:             mustValue(t, expressions),
				Env:              map[string]plan.Value{"X": plan.Literal("x")},
				Shell:            "bash",
				WorkingDirectory: plan.Literal(nil),
				SSH:              plan.Literal(map[string]any{"remoteHost": "h"}),
				Retry: &plan.StepRetry{
					MaxRetries: plan.Literal(int64(2)), Interval: plan.Literal("10s"), Timeout: plan.Literal("1m"),
					CleanOnRetry: mustValue(t, "${{ inputs.clean }}"),
				},
				Timeout:      plan.Literal("5m"),
				IgnoreErrors: plan.Literal(true),
				EarlyCancel:  "any-job-failed",
				Cleanup:      mustValue(t, "rm -rf ${{ inputs.dir }}"),
				Path:         "/jobs/build/steps/0",
			}, {
				Uses:   "github/example/workflows@v1",
				Source: &source,
				Path:   "/jobs/build/steps/1",
			}, {
				Uses:     plan.CheckoutUses,
				Checkout: &plan.Checkout{Repo: "https://github.com/example/app.git", Revision: "main", Commit: "fedcba9876543210fedcba9876543210fedcba98"},
				With:     plan.Literal(map[string]any{"repo": "https://github.com/example/app.git", "branch": "main", "sparse_checkout": []any{"app"}}),
				Path:     "/jobs/build/steps/2",
			}},
			Outputs: map[string]plan.Value{"out": mustValue(t, "${{ steps.greet.outputs.x }}")},
			Path:    "/jobs/build",
		}},
		Warnings: []problems.Diagnostic{withoutDetail(problems.UnknownKey.At(
			"workflow.yaml", problems.Position{Line: 3, Column: 9}, "/jobs/build/x",
			problems.Params{"key": "x", "suggested": "false", "suggestion": ""},
		))},
		Calls: []plan.Call{
			{Caller: "sha256:01", Step: "/jobs/deploy/steps/0", Source: source},
			{Caller: "sha256:01", Step: "/jobs/deploy/steps/1", Source: plan.Source{Location: "github.com/example/workflows/next.yaml", Revision: "89abcdef0123456789abcdef0123456789abcdef", Digest: "sha256:02", Ref: plan.SourceRef{Uses: "github/example/workflows@v2", Path: "next.yaml"}}},
		},
		Checkouts: []plan.CheckoutPin{
			{Caller: "sha256:01", Step: "/jobs/deploy/steps/2", Checkout: plan.Checkout{Repo: "user@example.com:tools.git", Revision: "v2", Commit: "0123456789abcdef0123456789abcdef01234567"}},
		},
	}
}

// readyJobs are ready jobs that set every member of each of their types,
// between them, a target of each kind among them.
func readyJobs(t testing.TB) []*plan.ReadyJob {
	steps := []plan.Step{{Run: mustValue(t, "echo ${{ matrix.os }}"), Path: "/jobs/build/steps/0"}}
	return []*plan.ReadyJob{{
		Key:    "build#1",
		Name:   "build",
		Matrix: plan.DataOf(map[string]any{"os": "linux"}),
		Target: plan.Target{Environment: &plan.Environment{
			Cluster: "owner/cluster", Name: "batch", Params: plan.DataOf(map[string]any{"partition": "gpu"}),
		}},
		Resources:        plan.Resources{MilliCPU: 2000, Memory: 8 << 30, GPU: 1},
		Retry:            &plan.ReadyJobRetry{Reasons: []plan.Reason{plan.ReasonWorkerLost, plan.ReasonTimeout}, MaxRetries: 3, IntervalSeconds: 60},
		TimeoutSeconds:   3600,
		WorkingDirectory: mustValue(t, "${{ env.PW_PARENT_JOB_DIR }}/work"),
		IfAlways:         true,
		Env:              map[string]plan.Value{"OS": mustValue(t, "${{ matrix.os }}")},
		Steps:            fullPlan(t).Jobs[0].Steps,
		Outputs:          map[string]plan.Value{"out": mustValue(t, "${{ steps.greet.outputs.x }}")},
		Private:          []string{"matrix"},
		Secrets:          []string{"matrix"},
		PrivateFields:    []string{"target", "timeoutSeconds"},
		SecretFields:     []string{"target"},
		Masked:           []string{"s3cr3t-value"},
		Path:             "/jobs/build",
	}, {
		Key: "group", Name: "group", Target: plan.Target{Group: "gpu"}, Steps: steps, Path: "/jobs/group",
	}, {
		Key: "compute", Name: "compute", Path: "/jobs/compute", Steps: steps,
		Target: plan.Target{ComputeTarget: &plan.ComputeTarget{
			Mode: "worker", TargetID: "cluster", EnvironmentID: "env", SchedulingParams: plan.DataOf(map[string]any{"q": "x"}),
			NewWorker: true, WorkerID: "w-1",
		}},
	}, {
		Key: "ssh", Name: "ssh", Path: "/jobs/ssh", Steps: steps,
		Target: plan.Target{Inline: &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "h"})}},
	}}
}

// fullJobResult is a job's result that sets every member.
func fullJobResult() *plan.JobResult {
	return &plan.JobResult{
		Outcome:      plan.OutcomeFailure,
		Outputs:      map[string]string{"dsn": "postgres://u:hunter2@db", "count": "3"},
		Private:      []string{"dsn"},
		Secrets:      []string{"dsn"},
		Derived:      []string{"dsn"},
		Steps:        map[string]map[string]string{"login": {"token": "t0k3n", "user": "a"}},
		SecretSteps:  map[string][]string{"login": {"token"}},
		DerivedSteps: map[string][]string{"login": {"token"}},
		NeedsFailed:  true,
		Cancelled:    true,
	}
}

// golden is a golden file: what each of its values encodes as, and the
// type it is, with the schema it fits.
type golden struct {
	file   string
	typ    reflect.Type
	schema string
	encode func(t testing.TB) [][]byte
}

var goldens = []golden{
	{"plan.json", reflect.TypeFor[plan.Plan](), "plan/schema/2/plan.schema.json", func(t testing.TB) [][]byte {
		return [][]byte{encoded(t, plan.Encode, fullPlan(t))}
	}},
	{"readyjobs.json", reflect.TypeFor[plan.ReadyJob](), "plan/schema/2/readyjob.schema.json", func(t testing.TB) [][]byte {
		var out [][]byte
		for _, j := range readyJobs(t) {
			out = append(out, encoded(t, plan.EncodeReadyJob, j))
		}
		return out
	}},
	{"record.json", reflect.TypeFor[plan.Record](), "plan/schema/2/record.schema.json", func(t testing.TB) [][]byte {
		r := sampleRecord()
		r.Version = plan.Version
		r.Inherited = &plan.Inherited{
			Env:    map[string]string{"REGION": "east", "TOKEN": "t0k3n"},
			Masked: []string{"t0k3n", "hunter2"},
		}
		r.StepRun = &plan.StepRun{
			SSH:     &plan.Inline{Name: "ssh", Backend: "ssh", Isolation: "host", SSH: plan.DataOf(map[string]any{"remoteHost": "farm"})},
			Private: true,
			State:   plan.DataOf(map[string]any{"steps": []any{}}),
		}
		return [][]byte{encoded(t, plan.EncodeRecord, r)}
	}},
	{"jobresult.json", reflect.TypeFor[plan.JobResult](), "plan/schema/2/jobresult.schema.json", func(t testing.TB) [][]byte {
		return [][]byte{encoded(t, marshalResult, fullJobResult())}
	}},
}

// marshalResult encodes a job's result as the worker API does
// (workerapi.Marshal).
func marshalResult(r *plan.JobResult) ([]byte, error) {
	return json.Marshal(r, json.Deterministic(true))
}

func encoded[T any](t testing.TB, encode func(T) ([]byte, error), v T) []byte {
	t.Helper()
	data, err := encode(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// file returns what a golden file holds of values: the one, or a list of
// them, a line each.
func file(values [][]byte) []byte {
	if len(values) == 1 {
		return values[0]
	}
	return append(append([]byte("[\n"), bytes.Join(values, []byte(",\n"))...), "\n]"...)
}

// The golden plan, ready jobs, record and job result encode to the bytes
// testdata holds, which -update rewrites, and each decodes to what encodes
// the same again.
func TestGoldens(t *testing.T) {
	for _, g := range goldens {
		values := g.encode(t)
		path := "testdata/" + g.file
		if *update {
			if err := os.WriteFile(path, file(values), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := file(values); !bytes.Equal(got, want) {
			t.Errorf("%s: encodes as\n%s\nwant\n%s", g.file, got, want)
		}
		for i, data := range values {
			var again []byte
			switch g.typ {
			case reflect.TypeFor[plan.Plan]():
				p, err := plan.Decode(data)
				if err != nil {
					t.Fatalf("%s: %v", g.file, err)
				}
				again = encoded(t, plan.Encode, p)
			case reflect.TypeFor[plan.ReadyJob]():
				j, err := plan.DecodeReadyJob(plan.Version, data)
				if err != nil {
					t.Fatalf("%s %d: %v", g.file, i, err)
				}
				again = encoded(t, plan.EncodeReadyJob, j)
			case reflect.TypeFor[plan.Record]():
				r, err := plan.DecodeRecord(data)
				if err != nil {
					t.Fatalf("%s: %v", g.file, err)
				}
				again = encoded(t, plan.EncodeRecord, r)
			default:
				var r plan.JobResult
				if err := json.Unmarshal(data, &r, json.RejectUnknownMembers(true)); err != nil {
					t.Fatalf("%s: %v", g.file, err)
				}
				again = encoded(t, marshalResult, &r)
			}
			if !bytes.Equal(again, data) {
				t.Errorf("%s: decoding and encoding changed it:\n%s\n%s", g.file, data, again)
			}
		}
	}
}

// The goldens set, between them, every member of each type they hold, so
// that each member's encoding is pinned; and the plan holds an expression
// with each kind of node and path segment.
func TestGoldensSetEveryMember(t *testing.T) {
	for _, g := range goldens {
		seen := map[string]map[string]bool{}
		for _, data := range g.encode(t) {
			var doc any
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			setMembers(g.typ, doc, seen)
		}
		for typ, members := range seen {
			for name, set := range members {
				if !set {
					t.Errorf("%s: no %s.%s", g.file, typ, name)
				}
			}
		}
	}
	data := goldens[0].encode(t)[0]
	for _, key := range []string{
		`"lit":`, `"num":"NaN"`, `"num":"Infinity"`, `"num":"-Infinity"`, `"word":`, `"ref":`, `"up":`, `"op":"!"`, `"not":true`,
		`"if":{`, `"member":`, `"call":`, `"lambda":`, `"param":`, `"slice":`, `,null]`, `"concat":`,
		`{"quoted":`, `{"bare":`, `{"index":true}`, `{"each":true}`, `"$":{"$`,
	} {
		if !bytes.Contains(data, []byte(key)) {
			t.Errorf("the golden plan holds no %s", key)
		}
	}
}

// setMembers notes, in seen by type, which of each struct type's JSON
// members doc, typ's JSON, sets, walking into what they hold.
func setMembers(typ reflect.Type, doc any, seen map[string]map[string]bool) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Slice:
		list, _ := doc.([]any)
		for _, item := range list {
			setMembers(typ.Elem(), item, seen)
		}
		return
	case reflect.Map:
		m, _ := doc.(map[string]any)
		for _, item := range m {
			setMembers(typ.Elem(), item, seen)
		}
		return
	case reflect.Struct:
	default:
		return
	}
	if slices.Contains(opaqueTypes, typ) {
		return
	}
	m, _ := doc.(map[string]any)
	if seen[typ.Name()] == nil {
		seen[typ.Name()] = map[string]bool{}
	}
	for f := range typ.Fields() {
		name := jsonName(f)
		if name == "" {
			continue
		}
		v, ok := m[name]
		seen[typ.Name()][name] = seen[typ.Name()][name] || ok
		if ok {
			setMembers(f.Type, v, seen)
		}
	}
}

// opaqueTypes are the encoded types whose encoding isn't of their Go
// members: values and data, as Value and Data say, and diagnostics, as
// foundation's problem.FieldError writes them.
var opaqueTypes = []reflect.Type{
	reflect.TypeFor[plan.Value](), reflect.TypeFor[plan.Data](), reflect.TypeFor[problems.Diagnostic](),
}

// jsonName returns the name f has in JSON, or "" for a field that has
// none.
func jsonName(f reflect.StructField) string {
	if !f.IsExported() {
		return ""
	}
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	if name == "-" {
		return ""
	}
	if name == "" {
		return f.Name
	}
	return name
}

// The goldens of version 1, as testdata/1 holds them, which version 1's
// release wrote, decode, as the plan, ready job, record and job result of a
// run of that version, and encode the same again, and fit version 1's
// schemas: each version is frozen, and read as it was written.
func TestGoldensOfVersion1(t *testing.T) {
	for _, g := range goldens {
		data, err := os.ReadFile("testdata/1/" + g.file)
		if err != nil {
			t.Fatal(err)
		}
		values := [][]byte{data}
		if list, ok := bytes.CutPrefix(data, []byte("[\n")); ok {
			values = bytes.Split(bytes.TrimSuffix(list, []byte("\n]")), []byte(",\n"))
		}
		s := schematest.Schema(t, strings.Replace(g.schema, "/schema/2/", "/schema/1/", 1))
		for i, data := range values {
			schematest.Validate(t, s, "1/"+g.file, data)
			var again []byte
			switch g.typ {
			case reflect.TypeFor[plan.Plan]():
				p, err := plan.Decode(data)
				if err != nil || p.Version != 1 {
					t.Fatalf("1/%s: %v, version %v", g.file, err, p)
				}
				again = encoded(t, plan.Encode, p)
			case reflect.TypeFor[plan.ReadyJob]():
				j, err := plan.DecodeReadyJob(1, data)
				if err != nil {
					t.Fatalf("1/%s %d: %v", g.file, i, err)
				}
				again = encoded(t, plan.EncodeReadyJob, j)
			case reflect.TypeFor[plan.Record]():
				r, err := plan.DecodeRecord(data)
				if err != nil || r.Version != 1 {
					t.Fatalf("1/%s: %v, %v", g.file, err, r)
				}
				again = encoded(t, plan.EncodeRecord, r)
			default:
				var r plan.JobResult
				if err := json.Unmarshal(data, &r, json.RejectUnknownMembers(true)); err != nil {
					t.Fatalf("1/%s: %v", g.file, err)
				}
				again = encoded(t, marshalResult, &r)
			}
			if !bytes.Equal(again, data) {
				t.Errorf("1/%s: decoding and encoding changed it:\n%s\n%s", g.file, data, again)
			}
		}
	}
}

// The goldens fit their schemas, and so do the plans testdata held before.
func TestGoldensFitTheSchemas(t *testing.T) {
	for _, g := range goldens {
		s := schematest.Schema(t, g.schema)
		for _, data := range g.encode(t) {
			schematest.Validate(t, s, g.file, data)
		}
	}
	s := schematest.Schema(t, "plan/schema/1/plan.schema.json")
	for _, name := range []string{"edges.json", "spack-builder.json"} {
		data, err := os.ReadFile("testdata/" + name)
		if err != nil {
			t.Fatal(err)
		}
		schematest.Validate(t, s, name, data)
	}
}
