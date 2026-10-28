package problems

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// paramCheckChild names, in the environment of a run of this test binary
// that TestParamCheck starts, how the run builds a problem without a
// param its kind declares.
const paramCheckChild = "MERIDIAN_PARAMCHECK_CHILD"

// buildUndeclared builds a problem of a kind without a param it declares,
// by build: Problem and At without its input, and InputProblem without its
// groups.
func buildUndeclared(build string) {
	missing := Params{"value": "cpu"}
	switch build {
	case "Problem":
		_ = ExecutionGroupUnknown.Problem(missing)
	case "At":
		ExecutionGroupUnknown.At("w.yaml", Position{}, "/x", missing)
	case "InputProblem":
		_ = ExecutionGroupUnknown.InputProblem(Params{"value": "cpu", "input": "cluster"})
	}
}

// TestParamCheck checks that, in a build with the paramcheck tag, as make
// test builds the tests, a problem or a diagnostic built without a param
// its kind declares ends the test binary at once, with exit status 2,
// where no recover catches it, and says which kind, which param and where;
// but not the input an Inputs resolver leaves to the compiler
// (InputProblem); and that every other build, as every binary is, builds
// it as it is.
func TestParamCheck(t *testing.T) {
	if build := os.Getenv(paramCheckChild); build != "" {
		defer func() { _ = recover() }() // which an exit doesn't reach
		buildUndeclared(build)
		return
	}
	builds := map[string]string{"Problem": "input", "At": "input", "InputProblem": "groups"}
	if !checkDeclared {
		for build := range builds {
			buildUndeclared(build)
		}
		return
	}
	for build, param := range builds {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestParamCheck$", "-test.count=1") //nolint:gosec // the test's own binary
		cmd.Env = append(os.Environ(), paramCheckChild+"="+build)
		out, err := cmd.CombinedOutput()
		exit, ok := errors.AsType[*exec.ExitError](err)
		if !ok || exit.ExitCode() != 2 || !strings.Contains(string(out), `a execution_group_unknown problem built without its param "`+param+`"`) ||
			!strings.Contains(string(out), "buildUndeclared") {
			t.Errorf("%s without %s: %v, %s", build, param, err, out)
		}
	}
	// An Inputs resolver's problem may leave out its input alone.
	_ = ExecutionGroupUnknown.InputProblem(Params{"value": "cpu", "groups": `"local"`})
}
