package compile

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"reflect"

	"github.com/parallelworks/meridian/internal/jsonshape"
	"github.com/parallelworks/meridian/plan"
)

// startStateJSON is how a StartState is encoded: with the version of the
// plan it was started from, whose types it holds, so that a release reads
// only the states of the plans it reads. Its members never change within
// a version, as a plan's don't (docs/design.md, Plans across versions).
type startStateJSON struct {
	Version        int                        `json:"version"`
	Run            plan.RunValues             `json:"run"`
	TimeoutSeconds int64                      `json:"timeoutSeconds,omitzero"`
	TimeoutPrivate bool                       `json:"timeoutPrivate,omitzero"`
	Expansions     map[string]*plan.Expansion `json:"expansions,omitempty"`
}

// startStateShape is how EncodeStartState writes a state, which
// DecodeStartState checks it against: a nil list or mapping left out or
// written as an empty one, and data as plan.Data writes it, which holds
// nothing where it is null.
var startStateShape = &jsonshape.Options{Zero: []reflect.Type{reflect.TypeFor[plan.Data]()}}

// EncodeStartState encodes what run state holds of what Start gave, as the
// coordinator stores it: JSON, of this release's plan version, which the
// same state always encodes to, byte for byte.
func EncodeStartState(s *StartState) ([]byte, error) {
	return jsonv2.Marshal(startStateJSON{
		Version: plan.Version, Run: s.Run,
		TimeoutSeconds: s.TimeoutSeconds, TimeoutPrivate: s.TimeoutPrivate, Expansions: s.Expansions,
	}, jsonv2.Deterministic(true))
}

// DecodeStartState decodes a state that EncodeStartState encoded, as
// strictly as a plan is decoded: one of another version is plan.ErrVersion,
// and one that isn't a state of this one, with a member it doesn't know,
// one that is always written missing, null where it is never written, or a
// value its type refuses, is plan.ErrMalformed.
func DecodeStartState(data []byte) (*StartState, error) {
	var w startStateJSON
	err := jsonv2.Unmarshal(data, &w, jsonv2.RejectUnknownMembers(true))
	if err == nil && w.Version == plan.Version {
		if err := jsonshape.Check(data, reflect.TypeFor[startStateJSON](), startStateShape); err != nil {
			return nil, fmt.Errorf("%w: the run's start state: %w", plan.ErrMalformed, err)
		}
		return &StartState{Run: w.Run, TimeoutSeconds: w.TimeoutSeconds, TimeoutPrivate: w.TimeoutPrivate, Expansions: w.Expansions}, nil
	}
	// A state of another version need not decode as this one's, so its
	// version is read on its own, and only when it doesn't decode.
	var head struct {
		Version int `json:"version"`
	}
	switch herr := jsonv2.Unmarshal(data, &head); {
	case herr != nil:
		return nil, fmt.Errorf("%w: the run's start state: %w", plan.ErrMalformed, herr)
	case head.Version < 1:
		return nil, fmt.Errorf("%w: the run's start state names no version", plan.ErrMalformed)
	case head.Version != plan.Version:
		return nil, fmt.Errorf("%w %d of the run's start state; this release reads version %d", plan.ErrVersion, head.Version, plan.Version)
	}
	return nil, fmt.Errorf("%w: the run's start state: %w", plan.ErrMalformed, err)
}
