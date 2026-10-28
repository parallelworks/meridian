package workerapi

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/internal/apiversion"
)

// since is the version of the API that gave each member of its bodies,
// and of the package's types in them, by its type and JSON name. What a
// body carries of a run, of package plan's types, isn't in it: the plan's
// own version says what of a plan a side reads (plan.Plan.MinVersion,
// which plan.Decode's schema check guards). Foundation's problems and
// Meridian's diagnostics are kept as they are. A member added later is in the newest version no
// release has written, which is Version, or which raises Version if a
// release has written it, and is left out when it is empty
// (TestBodiesAreVersioned).
var since = map[string]int{
	"ClaimRequest.credential": 1, "ClaimRequest.apiVersion": 1, "ClaimRequest.planVersion": 1, "ClaimRequest.worker": 1,
	"Worker.version": 1, "Worker.digest": 1,

	"Claim.runID": 1, "Claim.attemptID": 1, "Claim.apiVersion": 1, "Claim.planVersion": 1, "Claim.job": 1,
	"Claim.attempt": 1, "Claim.group": 1, "Claim.member": 1,
	"Claim.leaseSeconds": 1, "Claim.heartbeatSeconds": 1, "Claim.selfFencing": 1,
	"Claim.run": 1, "Claim.needs": 1, "Claim.runDir": 1, "Claim.env": 1, "Claim.retry": 1,
	"Claim.container": 1, "Claim.traceParent": 1, "Claim.control": 2, "Claim.tree": 3, "Claim.runDirOnMember": 3,
	"Tree.jobID": 3, "Tree.rootDir": 3, "Tree.workflowName": 3, "Tree.masked": 3,
	"EnvVar.value": 1, "EnvVar.secret": 1,
	"Container.runtime": 1, "Container.image": 1,

	"Control.version": 1, "Control.cancel": 1, "Control.reason": 1, "Control.graceSeconds": 1,
	"Control.anyJobFailed": 1,

	"Events.events": 1,
	"Event.seq":     1, "Event.time": 1, "Event.finished": 1, "Event.step": 1, "Event.try": 1,
	"Event.cleanup": 1, "Event.lastTry": 2, "Event.outcome": 1, "Event.exitCode": 1, "Event.outputs": 1,
	"Event.diagnostics": 1,
	"EventsAck.seq":     1,

	"LogAck.length": 1, "LogAck.ended": 1,

	"Complete.seq": 1, "Complete.result": 1, "Complete.reason": 1, "Complete.problem": 1,
	"Complete.diagnostics": 1,

	"ActionRequest.step": 3, "ActionRequest.try": 3, "ActionRequest.with": 3,
	"ActionRequest.private": 3, "ActionRequest.secret": 3,
	"ActionAnswer.outcome": 3, "ActionAnswer.reason": 3, "ActionAnswer.diagnostics": 3,

	"ChildRequest.step": 3, "ChildRequest.try": 3, "ChildRequest.with": 3, "ChildRequest.env": 3,
	"ChildRequest.withPrivate": 3, "ChildRequest.withSecrets": 3, "ChildRequest.envPrivate": 3,
	"ChildRequest.envSecrets": 3, "ChildRequest.masked": 3, "ChildRequest.workingDirectory": 3,
	"Child.run": 3, "Child.ended": 3, "Child.outcome": 3, "Child.reason": 3, "Child.diagnostics": 3,
	"ChildStop.step": 3, "ChildStop.try": 3, "ChildStop.reason": 3,
}

// CheckVersion returns an error if v, a body of an attempt of version of
// the API (Claim.APIVersion), sets a member that a later version added,
// which the other side of the attempt doesn't read: every body a side
// sends, and reads, of an attempt goes through it, through MarshalAt,
// UnmarshalAt and WriteBody, so that neither sends the other what its
// version doesn't have. A member a later version added is left out of a
// body when it is zero (omitzero, TestBodiesAreVersioned), so one that is
// zero is taken, as it is of every earlier version's body.
func CheckVersion(version int, v any) error {
	return checkVersion(apiversion.Generation(), version, reflect.ValueOf(v))
}

// MarshalAt encodes v as Marshal does, as a body of an attempt of version
// of the API, once CheckVersion has checked it.
func MarshalAt(version int, v any) ([]byte, error) {
	if err := CheckVersion(version, v); err != nil {
		return nil, err
	}
	return Marshal(v)
}

// UnmarshalAt decodes data into v as Unmarshal does, as a body of an
// attempt of version of the API: a member a later version added is an
// error, as CheckVersion has it.
func UnmarshalAt(version int, data []byte, v any) error {
	if err := Unmarshal(data, v); err != nil {
		return err
	}
	return CheckVersion(version, v)
}

// WriteBody answers 200 with v, encoded as the API writes a body of an
// attempt of version of the API (MarshalAt), as both the coordinator's
// server and a test's Fake answer. It returns MarshalAt's error before it
// writes anything, for the caller to answer otherwise.
func WriteBody(w http.ResponseWriter, version int, v any) error {
	data, err := MarshalAt(version, v)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
	return nil
}

// checkVersion is CheckVersion of the value v, with what is cached of the
// table at the generation gen of what tests have set (apiversion).
func checkVersion(gen int64, version int, v reflect.Value) error {
	if !v.IsValid() || newest(gen, v.Type()) <= version {
		return nil
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return checkVersion(gen, version, v.Elem())
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			if err := checkVersion(gen, version, v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		for it := v.MapRange(); it.Next(); {
			if err := checkVersion(gen, version, it.Value()); err != nil {
				return err
			}
		}
	case reflect.Struct:
		typ := v.Type()
		for i, f := range fieldsOf(gen, typ) {
			if f.name == "" {
				continue
			}
			fv := v.Field(i)
			if f.since > version && !fv.IsZero() {
				return fmt.Errorf("workerapi: %s.%s is of version %d of the API, not of an attempt of version %d", typ.Name(), f.name, f.since, version)
			}
			if err := checkVersion(gen, version, fv); err != nil {
				return err
			}
		}
	}
	return nil
}

// A field is a field of one of the package's types: its JSON name, or ""
// for one that isn't a member, and the version that added it.
type field struct {
	name  string
	since int
}

// A cacheKey is a type, and the generation of what tests have set of the
// table (apiversion.Generation), which is 0 in a release.
type cacheKey struct {
	gen int64
	typ reflect.Type
}

// fields are the fields of each of the package's types (fieldsOf), and
// newests the newest version of a member each type holds (newest), by
// cacheKey.
var fields, newests sync.Map

// sinceOf returns the version that added member, as the table has it, or
// as a test set it (apiversion).
func sinceOf(gen int64, member string) int {
	if gen != 0 {
		if v, ok := apiversion.Since(member); ok {
			return v
		}
	}
	return since[member]
}

// fieldsOf returns the fields of typ, one of the package's struct types,
// in order.
func fieldsOf(gen int64, typ reflect.Type) []field {
	key := cacheKey{gen, typ}
	if f, ok := fields.Load(key); ok {
		return f.([]field)
	}
	out := make([]field, typ.NumField())
	for i := range typ.NumField() {
		sf := typ.Field(i)
		name, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
		if !sf.IsExported() || name == "-" {
			continue
		}
		out[i] = field{name: name, since: sinceOf(gen, typ.Name()+"."+name)}
	}
	fields.Store(key, out)
	return out
}

// newest returns the newest version of the API that added a member that a
// value of typ can hold, through the package's types, or 0 for a type
// that holds none of them: a value of no later version than that needs no
// walk.
func newest(gen int64, typ reflect.Type) int {
	key := cacheKey{gen, typ}
	if n, ok := newests.Load(key); ok {
		return n.(int)
	}
	n := newestIn(gen, typ, map[reflect.Type]bool{})
	newests.Store(key, n)
	return n
}

// newestIn is newest, of a type the types in walking hold, which it
// doesn't walk again.
func newestIn(gen int64, typ reflect.Type, walking map[reflect.Type]bool) int {
	if walking[typ] {
		return 0
	}
	walking[typ] = true
	defer delete(walking, typ)
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		return newestIn(gen, typ.Elem(), walking)
	case reflect.Struct:
		if typ.PkgPath() != pkgPath {
			return 0
		}
		n := 0
		for i, f := range fieldsOf(gen, typ) {
			if f.name != "" {
				n = max(n, f.since, newestIn(gen, typ.Field(i).Type, walking))
			}
		}
		return n
	}
	return 0
}

// pkgPath is the package's import path.
var pkgPath = reflect.TypeFor[Claim]().PkgPath()
