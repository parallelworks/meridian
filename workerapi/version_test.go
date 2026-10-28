package workerapi_test

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/versiontest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/workerapi"
)

// TestBodiesAreVersioned checks the rule the package doc gives for
// changing a body: every member of every body of the package's types has
// the version of the API that gave it, in the package's since table, none
// later than workerapi.Version; a member added after the first is left out
// when it is zero, omitzero, so that a body of an attempt of an earlier
// version never holds it, as CheckVersion counts what a body holds; and a
// ClaimRequest never changes. What a body carries of
// a run, of package plan's types, the plan versions, in its own table.
func TestBodiesAreVersioned(t *testing.T) {
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
		for f := range typ.Fields() {
			tag := f.Tag.Get("json")
			name, options, _ := strings.Cut(tag, ",")
			member := typ.Name() + "." + name
			version, ok := workerapi.Since[member]
			switch {
			case !ok:
				t.Errorf("%s has no version: give it the newest version of the API no release has written, raising workerapi.Version if a release has written it", member)
			case version > workerapi.Version:
				t.Errorf("%s is from version %d, after workerapi.Version %d", member, version, workerapi.Version)
			case version > 1 && typ.Name() == "ClaimRequest":
				t.Errorf("%s is added to a ClaimRequest, which never changes", member)
			case version > 1 && !strings.Contains(options, "omitzero"):
				// omitempty drops an empty list or mapping too, which
				// CheckVersion would count as there.
				t.Errorf("%s is from version %d, and isn't left out when zero (omitzero)", member, version)
			}
			walk(f.Type)
		}
	}
	for _, body := range []any{
		workerapi.ClaimRequest{},
		workerapi.Claim{},
		workerapi.Control{},
		workerapi.Events{},
		workerapi.EventsAck{},
		workerapi.LogAck{},
		workerapi.Complete{},
		workerapi.ActionRequest{},
		workerapi.ActionAnswer{},
		workerapi.ChildRequest{},
		workerapi.Child{},
		workerapi.ChildStop{},
	} {
		walk(reflect.TypeOf(body))
	}
	for member := range workerapi.Since {
		if typ, _, _ := strings.Cut(member, "."); !seen[typ] {
			t.Errorf("%s is in no body", member)
		}
	}
}

// TestCheckVersion checks that a body of an attempt of a version is
// refused when it sets a member a later version added, wherever the
// package's types hold it, through a pointer, a list or a mapping, and
// taken when it leaves it empty or the attempt's version has it; and that
// MarshalAt, UnmarshalAt and WriteBody refuse what CheckVersion does,
// WriteBody before it writes anything.
func TestCheckVersion(t *testing.T) {
	// As though version 2 had added these.
	for _, member := range []string{"Event.cleanup", "EnvVar.secret", "Container.image", "EventsAck.seq"} {
		t.Cleanup(versiontest.Set(member, 2))
	}
	claim := func(change func(*workerapi.Claim)) *workerapi.Claim {
		c := &workerapi.Claim{RunID: "run", Env: map[string]workerapi.EnvVar{"A": {Value: "a"}}, Container: &workerapi.Container{Runtime: "podman"}}
		change(c)
		return c
	}
	for _, tc := range []struct {
		name  string
		body  any
		later bool
	}{
		{"a later member, empty", &workerapi.EventsAck{}, false},
		{"a later member, set", &workerapi.EventsAck{Seq: 1}, true},
		{"in a list, empty", &workerapi.Events{Events: []workerapi.Event{{Seq: 1}}}, false},
		{"in a list, set", &workerapi.Events{Events: []workerapi.Event{{Seq: 1}, {Seq: 2, Cleanup: true}}}, true},
		{"in a mapping, empty", claim(func(*workerapi.Claim) {}), false},
		{"in a mapping, set", claim(func(c *workerapi.Claim) { c.Env["B"] = workerapi.EnvVar{Secret: true} }), true},
		{"through a pointer, set", claim(func(c *workerapi.Claim) { c.Container.Image = "x" }), true},
		{"a body of plan's", &plan.JobResult{Outcome: plan.OutcomeSuccess}, false},
		{"nil", nil, false},
	} {
		for _, version := range []int{1, 2} {
			err := workerapi.CheckVersion(version, tc.body)
			if want := tc.later && version < 2; (err != nil) != want {
				t.Errorf("%s, of version %d: %v, want an error: %v", tc.name, version, err, want)
			}
		}
	}

	refused := &workerapi.Events{Events: []workerapi.Event{{Seq: 1, Cleanup: true}}}
	if _, err := workerapi.MarshalAt(1, refused); err == nil {
		t.Error("MarshalAt took a member of a later version")
	}
	if _, err := workerapi.MarshalAt(2, refused); err != nil {
		t.Errorf("MarshalAt of the version that has the member: %v", err)
	}
	data, err := workerapi.Marshal(refused)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerapi.UnmarshalAt(1, data, new(workerapi.Events)); err == nil {
		t.Error("UnmarshalAt read a member of a later version")
	}
	if err := workerapi.UnmarshalAt(2, data, new(workerapi.Events)); err != nil {
		t.Errorf("UnmarshalAt of the version that has the member: %v", err)
	}
	rec := httptest.NewRecorder()
	if err := workerapi.WriteBody(rec, 1, refused); err == nil || rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Errorf("WriteBody of a member of a later version = %v, and wrote %q", err, rec.Body)
	}
}

// TestNewerMemberRefused checks what a side does with a body of a later
// version than it speaks, with a member it doesn't know: it refuses it,
// rather than run without that part. The rule keeps such a body from
// reaching it: each side sends a member only in an attempt whose version
// (Claim.APIVersion) has it, and the coordinator serves an attempt at the
// lower of the worker's version and its own.
func TestNewerMemberRefused(t *testing.T) {
	var c workerapi.Control
	err := workerapi.Unmarshal([]byte(`{"version":3,"cancel":true,"drain":true}`), &c)
	if err == nil {
		t.Fatal("read a Control with a member of a later version")
	}
	// The same body without it is read.
	if err := workerapi.Unmarshal([]byte(`{"version":3,"cancel":true}`), &c); err != nil || !c.Cancel {
		t.Fatalf("got %+v, %v", c, err)
	}
}

// TestSpeaks checks that a claim that leaves its version of the API out,
// as a worker of a release from before the API had one does, speaks
// version 1, and is read.
func TestSpeaks(t *testing.T) {
	var req workerapi.ClaimRequest
	if err := workerapi.Unmarshal([]byte(`{"credential":"c","planVersion":1,"worker":{"version":"v","digest":"d"}}`), &req); err != nil {
		t.Fatal(err)
	}
	if got := req.Speaks(); got != 1 {
		t.Errorf("a claim without its version speaks %d", got)
	}
	req.APIVersion = 3
	if got := req.Speaks(); got != 3 {
		t.Errorf("a claim of version 3 speaks %d", got)
	}
}
