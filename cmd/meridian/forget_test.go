//go:build linux || darwin || freebsd

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
)

// TestRunForgets checks that meridian run leaves nothing of its attempts'
// placements in the state directory once its run has ended: the local
// backend's files of each, which the coordinator has it forget, so that
// what the state directory holds doesn't grow run after run.
func TestRunForgets(t *testing.T) {
	env := runEnv(t)
	workflow := writeFile(t, "w.yaml", helloWorkflow)
	for range 2 {
		if r := runTest(t, env, nil, workflow); r.code != exitOK {
			t.Fatalf("meridian run = %+v", r)
		}
		for _, dir := range []string{"placements", "workers", "live"} {
			entries, err := os.ReadDir(filepath.Join(env[local.EnvStateDir], dir))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				t.Errorf("the state directory's %s/ holds %s", dir, e.Name())
			}
		}
	}
}
