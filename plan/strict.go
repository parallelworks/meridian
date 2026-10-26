package plan

import (
	"cmp"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/internal/jsonshape"
)

// The package's types decode only as their schemas describe them
// (Schemas). What encoding/json/v2 takes and a schema refuses of what a
// member holds, each type refuses as it decodes, wherever it is decoded:
// a format before the first, an empty name, a JSON Pointer that doesn't
// start with /, an index before the first, a target with two members, and
// an outcome that isn't one. And Decode, DecodeRecord and DecodeReadyJob
// check the members themselves against their types (shape): one that is
// always written that is missing, and null where it isn't written.

// shape is how the package's encoding writes its types, which decoding
// checks what it decodes against: a nil list or mapping as null, values as
// Value writes them, null among them, and data as Data does, which holds
// nothing where it is null.
var shape = &jsonshape.Options{
	NilAsNull: true,
	Any:       []reflect.Type{reflect.TypeFor[Value]()},
	Zero:      []reflect.Type{reflect.TypeFor[Data]()},
}

// checkShape checks data, which decoded into v without an error, against
// v's type (shape).
func checkShape(data []byte, v any) error {
	return jsonshape.Check(data, reflect.TypeOf(v).Elem(), shape)
}

// UnmarshalJSONFrom decodes a run, refusing a format before the first, an
// empty name, and a JSON Pointer that doesn't start with /.
func (r *Run) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	type plain Run
	if err := jsonv2.UnmarshalDecode(dec, (*plain)(r)); err != nil {
		return err
	}
	return cmp.Or(
		checkFormat(r.Format),
		checkNames("secrets", r.Secrets), checkNames("derived", r.Derived),
		checkNames("private", r.Private), checkNames("variables", r.Variables),
		checkPointers("withheld", r.Withheld), checkPointers("decisions", r.Decisions),
	)
}

// UnmarshalJSONFrom decodes a run's values, refusing a format before the
// first, and an empty name.
func (r *RunValues) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	type plain RunValues
	if err := jsonv2.UnmarshalDecode(dec, (*plain)(r)); err != nil {
		return err
	}
	return cmp.Or(
		checkFormat(r.Format),
		checkNames("private", r.Private), checkNames("secrets", r.Secrets), checkNames("derived", r.Derived),
	)
}

// UnmarshalJSONFrom decodes a ready job, refusing an empty name.
func (j *ReadyJob) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	type plain ReadyJob
	if err := jsonv2.UnmarshalDecode(dec, (*plain)(j)); err != nil {
		return err
	}
	return cmp.Or(
		checkNames("private", j.Private), checkNames("secrets", j.Secrets),
		checkNames("privateFields", j.PrivateFields), checkNames("secretFields", j.SecretFields),
	)
}

// UnmarshalJSONFrom decodes a target, refusing one with more than one
// member.
func (t *Target) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	raw, err := dec.ReadValue()
	if err != nil {
		return err
	}
	type plain Target
	if err := jsonv2.Unmarshal(raw, (*plain)(t), dec.Options()); err != nil {
		return err
	}
	var members map[string]jsontext.Value
	if err := jsonv2.Unmarshal(raw, &members); err != nil {
		return err
	}
	if len(members) > 1 {
		return errors.New("a target has more than one member")
	}
	return nil
}

// UnmarshalJSONFrom decodes a matrix's instance, refusing an index before
// the first.
func (i *Instance) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	type plain Instance
	if err := jsonv2.UnmarshalDecode(dec, (*plain)(i)); err != nil {
		return err
	}
	if i.Index < 0 {
		return fmt.Errorf("an instance's index is %d, before the first", i.Index)
	}
	return nil
}

// UnmarshalText decodes an outcome, refusing what isn't one.
func (o *Outcome) UnmarshalText(text []byte) error {
	switch out := Outcome(text); out {
	case OutcomeSuccess, OutcomeFailure, OutcomeCancelled, OutcomeSkipped:
		*o = out
		return nil
	}
	return fmt.Errorf("%q isn't an outcome", text)
}

// checkFormat returns an error for a workflow's format before the first.
func checkFormat(format int) error {
	if format < 1 {
		return fmt.Errorf("the format is %d, before the first", format)
	}
	return nil
}

// checkNames returns an error for an empty name among those a member
// names (JoinPath).
func checkNames(member string, names []string) error {
	if slices.Contains(names, "") {
		return fmt.Errorf("%s names nothing, with an empty name", member)
	}
	return nil
}

// checkPointers returns an error for what among a member's JSON Pointers
// isn't one: one other than "" that doesn't start with /.
func checkPointers(member string, pointers []string) error {
	for _, p := range pointers {
		if p != "" && !strings.HasPrefix(p, "/") {
			return fmt.Errorf("%s holds %q, which isn't a JSON Pointer", member, p)
		}
	}
	return nil
}
