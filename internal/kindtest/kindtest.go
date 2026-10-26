// Package kindtest helps tests check the problems Meridian reports.
package kindtest

import (
	"errors"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// A Got is what a test got of a problem: its params, and its detail, its
// message in English.
type Got struct {
	Params problems.Params
	Detail string
}

// Want fails the test unless err is, or wraps, a problem of kind k, and
// returns what it holds.
func Want(t testing.TB, err error, k *problems.Kind) Got {
	t.Helper()
	p, ok := errors.AsType[*problem.Problem](err)
	if !ok {
		t.Fatalf("err = %v, want a problem of kind %s", err, k.Code())
	}
	if p.Code != k.Code() {
		t.Fatalf("err is %s (%v), want %s", p.Code, p, k.Code())
	}
	return Got{p.Params, p.Detail}
}
