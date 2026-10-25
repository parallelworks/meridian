package plan_test

import (
	"reflect"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// TestEndResultReachesJobResult checks that each field of an EndResult
// reaches the JobResult a worker reports, and a coordinator keeps, in the
// field of the same name, as it is: a field added to one and not the
// other, or not copied, would put a secret in run state, or lose what the
// jobs that need the job read.
func TestEndResultReachesJobResult(t *testing.T) {
	var end plan.EndResult
	ev := reflect.ValueOf(&end).Elem()
	for i := range ev.NumField() {
		ev.Field(i).Set(nonZero(ev.Field(i).Type(), ev.Type().Field(i).Name))
	}
	got := end.Result(plan.OutcomeFailure)
	gv := reflect.ValueOf(got)
	for f := range ev.Type().Fields() {
		g := gv.FieldByName(f.Name)
		switch {
		case !g.IsValid():
			t.Errorf("JobResult has no %s", f.Name)
		case !reflect.DeepEqual(g.Interface(), ev.FieldByName(f.Name).Interface()):
			t.Errorf("JobResult.%s is %v, not the EndResult's %v", f.Name, g.Interface(), ev.FieldByName(f.Name).Interface())
		}
	}
	if got.Outcome != plan.OutcomeFailure || got.NeedsFailed || got.Cancelled {
		t.Errorf("the result is %s, needs failed %t, cancelled %t", got.Outcome, got.NeedsFailed, got.Cancelled)
	}
}

// nonZero returns a value of typ that isn't its zero value, made of s.
func nonZero(typ reflect.Type, s string) reflect.Value {
	v := reflect.New(typ).Elem()
	switch typ.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Slice:
		v.Set(reflect.Append(v, nonZero(typ.Elem(), s)))
	case reflect.Map:
		v.Set(reflect.MakeMap(typ))
		v.SetMapIndex(nonZero(typ.Key(), s), nonZero(typ.Elem(), s+"-value"))
	default:
		panic("nonZero: a field of type " + typ.String()) //nolint:forbidigo // a test's own bug: a field of a type it doesn't fill
	}
	return v
}
