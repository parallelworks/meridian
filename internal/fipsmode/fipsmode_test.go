package fipsmode_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/fipsmode"
	"github.com/parallelworks/meridian/problems"
)

// childEnv makes the test binary print its mode, for TestMode.
const childEnv = "FIPSMODE_TEST_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) != "" {
		fmt.Print(fipsmode.Mode())
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// The process's mode is what GODEBUG says, or go.mod's default, only,
// when it says nothing of fips140.
func TestMode(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for godebug, want := range map[string]string{"": "only", "fips140=only": "only", "fips140=on": "on", "fips140=off": "off"} {
		cmd := exec.CommandContext(t.Context(), exe)
		cmd.Env = append(withoutGODEBUG(os.Environ()), childEnv+"=1", "GODEBUG="+godebug)
		out, err := cmd.Output()
		if got := strings.TrimSpace(string(out)); err != nil || got != want {
			t.Errorf("with GODEBUG=%s, the mode is %q, %v; want %q", godebug, got, err, want)
		}
	}
}

func withoutGODEBUG(env []string) []string {
	var out []string
	for _, kv := range env {
		if !strings.HasPrefix(kv, "GODEBUG=") {
			out = append(out, kv)
		}
	}
	return out
}

// Only the mode only passes; any other is fips_not_enforced, with the
// mode.
func TestCheck(t *testing.T) {
	if err := fipsmode.Check("only"); err != nil {
		t.Errorf("only: %v", err)
	}
	for _, mode := range []string{"on", "off"} {
		p, ok := errors.AsType[*problem.Problem](fipsmode.Check(mode))
		if !ok || p.Code != problems.FIPSNotEnforced.Code() || p.Params["mode"] != mode {
			t.Errorf("%s: %v", mode, p)
		}
	}
}
