package problems

import "testing"

// TestParamCheck checks that, in a build with the paramcheck tag, as make
// test builds the tests, a problem or a diagnostic built without a param
// its kind declares panics, but for the input an Inputs resolver leaves
// to the compiler (InputProblem); and that every other build, as every
// binary is, builds it as it is.
func TestParamCheck(t *testing.T) {
	panics := func(build func()) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		build()
		return false
	}
	missing := Params{"value": "cpu"}
	for name, build := range map[string]func(){
		"Problem": func() { _ = ExecutionGroupUnknown.Problem(missing) },
		"At":      func() { ExecutionGroupUnknown.At("w.yaml", Position{}, "/x", missing) },
		"InputProblem without groups": func() {
			_ = ExecutionGroupUnknown.InputProblem(missing)
		},
	} {
		if got := panics(build); got != checkDeclared {
			t.Errorf("%s of a kind without a declared param: panicked %t, want %t", name, got, checkDeclared)
		}
	}
	if panics(func() { _ = ExecutionGroupUnknown.InputProblem(Params{"value": "cpu", "groups": `"local"`}) }) {
		t.Error("InputProblem without input panicked")
	}
}
