package workerapi_test

import (
	"encoding/json/jsontext"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// since is the plan version that gave each member of the API's bodies, and
// of the types of package plan in them, by its type and JSON name. A
// member added later is in a later version, which raises plan.Version.
var since = map[string]int{
	"ClaimRequest.credential": 1, "ClaimRequest.planVersion": 1, "ClaimRequest.worker": 1,
	"Worker.version": 1, "Worker.digest": 1,

	"Claim.runID": 1, "Claim.attemptID": 1, "Claim.planVersion": 1, "Claim.job": 1,
	"Claim.attempt": 1, "Claim.group": 1, "Claim.member": 1,
	"Claim.leaseSeconds": 1, "Claim.heartbeatSeconds": 1, "Claim.selfFencing": 1,
	"Claim.run": 1, "Claim.needs": 1, "Claim.runDir": 1, "Claim.env": 1, "Claim.retry": 1,
	"Claim.container": 1, "Claim.traceParent": 1,
	"EnvVar.value": 1, "EnvVar.secret": 1,
	"Container.runtime": 1, "Container.image": 1,

	"RunValues.format": 1, "RunValues.inputs": 1, "RunValues.env": 1, "RunValues.org": 1,
	"RunValues.vars": 1, "RunValues.remote": 1, "RunValues.sessions": 1,
	"RunValues.private": 1, "RunValues.secrets": 1, "RunValues.derived": 1,
	"JobResult.outcome": 1, "JobResult.outputs": 1, "JobResult.steps": 1, "JobResult.needsFailed": 1, "JobResult.private": 1, "JobResult.cancelled": 1,
	"JobResult.secrets": 1, "JobResult.secretSteps": 1, "JobResult.derived": 1, "JobResult.derivedSteps": 1,

	"Control.version": 1, "Control.cancel": 1, "Control.reason": 1, "Control.graceSeconds": 1,
	"Control.anyJobFailed": 1,

	"Events.events": 1,
	"Event.seq":     1, "Event.time": 1, "Event.finished": 1, "Event.step": 1, "Event.try": 1,
	"Event.cleanup": 1, "Event.outcome": 1, "Event.exitCode": 1, "Event.outputs": 1,
	"Event.diagnostics": 1,
	"EventsAck.seq":     1,

	"LogAck.length": 1, "LogAck.ended": 1,

	"Complete.seq": 1, "Complete.result": 1, "Complete.reason": 1, "Complete.problem": 1,
	"Complete.diagnostics": 1,
}

// opaque are the types in bodies whose encoding isn't the API's to
// version: the plan's data and its encoded job, foundation's problems and
// Meridian's diagnostics, which are kept as they are, and times.
var opaque = []reflect.Type{
	reflect.TypeFor[plan.Data](), reflect.TypeFor[jsontext.Value](), reflect.TypeFor[time.Time](),
	reflect.TypeFor[problems.Diagnostic](), reflect.TypeFor[problem.Problem](),
}

// TestBodiesAreVersioned checks the rule the package doc gives for
// changing a body: every member of every body has the plan version that
// gave it, none later than plan.Version; a member added after the first is
// left out when it is empty, so that a body for a run of an earlier
// version never holds it; and a ClaimRequest never changes.
func TestBodiesAreVersioned(t *testing.T) {
	seen := map[string]bool{}
	var walk func(typ reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || slices.Contains(opaque, typ) || seen[typ.Name()] {
			return
		}
		seen[typ.Name()] = true
		for f := range typ.Fields() {
			tag := f.Tag.Get("json")
			name, options, _ := strings.Cut(tag, ",")
			member := typ.Name() + "." + name
			version, ok := since[member]
			switch {
			case !ok:
				t.Errorf("%s has no version: give it the next plan version, and raise plan.Version", member)
			case version > plan.Version:
				t.Errorf("%s is from version %d, after plan.Version %d", member, version, plan.Version)
			case version > 1 && typ.Name() == "ClaimRequest":
				t.Errorf("%s is added to a ClaimRequest, which never changes", member)
			case version > 1 && !strings.Contains(options, "omitzero") && !strings.Contains(options, "omitempty"):
				t.Errorf("%s is from version %d, and isn't left out when empty", member, version)
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
	} {
		walk(reflect.TypeOf(body))
	}
	for member := range since {
		if typ, _, _ := strings.Cut(member, "."); !seen[typ] {
			t.Errorf("%s is in no body", member)
		}
	}
}

// TestNewerMemberRefused checks what a side does with a body for a run of
// a later version than it reads, with a member it doesn't know: it refuses
// it, rather than run without that part. The rule keeps such a body from
// reaching it: the coordinator refuses a worker that doesn't read the
// run's version, and a replica doesn't answer for such a run.
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
