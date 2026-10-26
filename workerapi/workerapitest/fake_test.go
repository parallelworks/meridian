package workerapitest_test

import (
	"testing"

	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// The Fake serves the worker API as every coordinator must.
func TestFakeConforms(t *testing.T) {
	workerapitest.Conformance(t, &workerapitest.FakeTarget{})
}
