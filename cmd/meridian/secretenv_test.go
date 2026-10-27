//go:build linux || darwin || freebsd

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/harden"
)

// The values of the environment variables TestRunWithholdsSecrets sets.
const (
	envSecret = "e2e-secret-4f1c9a"
	envPlain  = "e2e-plain-77"
)

// TestRunWithholdsSecrets checks that a step doesn't get, in its
// environment, the value of a secret variable read from an environment
// variable of the shell that ran meridian run, which nothing masks in a
// job that doesn't read it; and that it gets a plain variable's, as the
// shell gives it.
func TestRunWithholdsSecrets(t *testing.T) {
	// The workers that the local backend starts get the command's
	// environment, which is the test's.
	t.Setenv("E2E_SECRET_TOKEN", envSecret)
	t.Setenv("E2E_PLAIN_BUILD", envPlain)
	env := runEnv(t)
	env["E2E_SECRET_TOKEN"], env["E2E_PLAIN_BUILD"] = envSecret, envPlain
	config := env[local.EnvConfigDir]
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	variables := "vars:\n  token: {secret: true, env: E2E_SECRET_TOKEN}\n  build: {env: E2E_PLAIN_BUILD}\n"
	if err := os.WriteFile(filepath.Join(config, local.VariablesFile), []byte(variables), 0o600); err != nil {
		t.Fatal(err)
	}
	workflow := writeFile(t, "w.yaml", `jobs:
  main:
    steps:
      - run: |
          env
          echo "secret: ${E2E_SECRET_TOKEN:-unset}"
          echo "plain: $E2E_PLAIN_BUILD"
`)
	r := runTest(t, env, nil, workflow)
	if r.code != exitOK {
		t.Fatalf("meridian run = %+v", r)
	}
	if strings.Contains(r.stdout, envSecret) || strings.Contains(r.stderr, envSecret) {
		t.Errorf("a step got the secret in its environment:\n%s", r.stdout)
	}
	if !strings.Contains(r.stdout, "main step 0| secret: unset\n") || !strings.Contains(r.stdout, "main step 0| plain: "+envPlain+"\n") {
		t.Errorf("the step's environment is as its log shows:\n%s", r.stdout)
	}
}

// TestRunHardens checks that meridian run refuses to coordinate a run
// when it can't keep the user's other processes out of its memory, where
// runs' secret records are, or on a system where it can't.
func TestRunHardens(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{errors.New("operation not permitted"), "coordinator_harden_failed"},
		{harden.ErrUnsupported, "coordinator_system_unsupported"},
	} {
		r := runTest(t, runEnv(t), func(c *cli) { c.harden = func() error { return tc.err } }, writeFile(t, "w.yaml", helloWorkflow))
		if r.code != exitError || !strings.Contains(r.stderr, "("+tc.code+")") || strings.Contains(r.stdout, "started") {
			t.Errorf("meridian run, hardening %v = %+v", tc.err, r)
		}
	}
}
