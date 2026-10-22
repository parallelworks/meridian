package local

import (
	"errors"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// A got is a problem a test got: its params and its detail.
type got struct {
	Params problems.Params
	Detail string
}

// wantProblem checks that err is, or wraps, a problem of kind k, and
// returns what it holds.
func wantProblem(t *testing.T, err error, k *problems.Kind) got {
	t.Helper()
	p, ok := errors.AsType[*problem.Problem](err)
	if !ok {
		t.Fatalf("err = %v, want a problem of kind %s", err, k.Code())
	}
	if p.Code != k.Code() {
		t.Fatalf("err is %s (%v), want %s", p.Code, p, k.Code())
	}
	return got{p.Params, p.Detail}
}
