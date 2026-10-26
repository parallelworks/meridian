package compile

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/plan"
)

// detailsOf is a platform whose Inputs resolver gives a resource's details
// for its name, and a mapping with an id made from each part.
type detailsOf struct{}

func (detailsOf) ResolveInput(_ string, _ map[string]any, v any) (any, error) {
	switch v := v.(type) {
	case string:
		return map[string]any{"name": v, "id": "id-" + v}, nil
	case map[string]any:
		out := maps.Clone(v)
		for k, x := range v {
			out[k+"_id"] = fmt.Sprint("id-", x)
		}
		return out, nil
	}
	return v, nil
}

// Start gives a run whose other inputs and env: read a platform input's
// details what Create gives it knowing every variable and password, with
// the input in each place an input can be, with each kind of default, and
// given each kind of value; and the plan doesn't change with a variable's
// value, nor hold a secret's or a password's.
func TestPlatformInputsStartAgreesWithCreate(t *testing.T) {
	places := map[string]struct{ defs, ref string }{
		"an input":            {"c: {type: compute-clusters%s}", "inputs.c"},
		"a group's":           {"g: {type: group, items: {c: {type: compute-clusters%s}}}", "inputs.g.c"},
		"a flattened group's": {"g: {type: group, flatten: true, items: {c: {type: compute-clusters%s}}}", "inputs.c"},
	}
	defaults := map[string]string{
		"none":             ", optional: true",
		"written out":      ", default: c0",
		"reads a variable": ", default: '${{ vars.V }}'",
		"reads a secret":   ", default: '${{ vars.S }}'",
		"reads an input":   ", default: '${{ inputs.n }}'",
		"reads a password": ", default: '${{ inputs.pw }}'",
	}
	given := map[string]any{"none": nil, "a name": "c1", "the variable's": "VARV", "the secret's": "SECRETVAL", "the password's": "PWVAL"}
	compared := 0
	for place, p := range places {
		for dflt, d := range defaults {
			for what, v := range given {
				name := place + ", default " + dflt + ", given " + what
				var b strings.Builder
				b.WriteString("on:\n  execute:\n    inputs:\n")
				for _, l := range []string{
					fmt.Sprintf(p.defs, d), "n: {type: string, default: c2}", "pw: {type: password, optional: true}",
					"d: {type: string, default: '${{ " + p.ref + ".id }}-x'}",
					"h: {type: string, default: hh, hidden: '${{ " + p.ref + ".id != ''id-c1'' }}', ignore: '${{ .hidden }}'}",
				} {
					b.WriteString("      " + l + "\n")
				}
				b.WriteString("env:\n  E: ${{ " + p.ref + ".id }}\njobs:\n  main:\n    steps:\n      - run: echo\n")
				src := b.String()
				submitted := func() map[string]any {
					m := map[string]any{"pw": "PWVAL"}
					switch {
					case v == nil:
					case place == "a group's":
						m["g"] = map[string]any{"c": v}
					default:
						m["c"] = v
					}
					return m
				}
				doc, diags := format.Load("workflow.yaml", []byte(src))
				if diags.HasErrors() {
					t.Fatalf("%s: %v", name, diags)
				}
				oracle := newInputResolver(doc, submitted(), Options{Inputs: detailsOf{}, Contexts: map[string]any{"vars": map[string]any{"V": "VARV", "S": "SECRETVAL"}}})
				oracle.resolve()
				if oracle.diags.HasErrors() {
					continue
				}
				for _, f := range oracle.ignoredLater {
					oracle.drop(f)
				}
				var plans []string
				for _, value := range []string{"VARV", "OTHER"} {
					opts := Options{Inputs: detailsOf{}, Contexts: map[string]any{"vars": map[string]any{"V": value, "S": expr.Unknown{Name: "vars"}}}}
					created, _, err := Create(t.Context(), "workflow.yaml", []byte(src), submitted(), opts)
					if err != nil {
						t.Fatal(err)
					}
					if created == nil {
						// A condition reads a default that reads a secret,
						// which isn't known when the run is created.
						break
					}
					data, err := plan.Encode(created.Plan)
					if err != nil {
						t.Fatal(err)
					}
					plans = append(plans, string(data))
					if strings.Contains(dflt, "secret") && strings.Contains(string(data), "SECRETVAL") || strings.Contains(dflt, "password") && strings.Contains(string(data), "PWVAL") {
						t.Errorf("%s: the plan holds what a default that reads a secret or a password reads: %s", name, data)
					}
					if value != "VARV" {
						continue
					}
					vars := caseVariables{"vars.V": {Value: "VARV"}, "vars.S": {Value: "SECRETVAL", Secret: true}}
					res, sd, err := Start(t.Context(), created.Plan, StartInput{RunID: "r", Variables: vars, Record: created.Record, Inputs: detailsOf{}})
					if err != nil || res == nil {
						t.Fatalf("%s: %v %v", name, sd, err)
					}
					compared++
					if want, got := canonicalValues(t, oracle.values), canonicalValues(t, res.Run.Inputs.Get()); want != got {
						t.Errorf("%s: Start gives\n%s\nwhere Create, knowing everything, gives\n%s", name, got, want)
					}
					if e := res.Run.Env["E"]; v != nil && !strings.HasPrefix(e, "id-") {
						t.Errorf("%s: env.E = %q, want what reads the details", name, e)
					}
				}
				if len(plans) == 2 && plans[0] != plans[1] {
					t.Errorf("%s: the plan changes with a plain variable's value:\n%s\n%s", name, plans[0], plans[1])
				}
			}
		}
	}
	if compared < 80 {
		t.Errorf("compared %d runs, want more", compared)
	}
}
