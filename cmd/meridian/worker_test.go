package main

import (
	"strings"
	"testing"

	"github.com/parallelworks/meridian/workerapi"
)

// TestWorker checks that meridian worker runs the worker, which takes
// nothing on its command line: what it claims comes from its environment.
func TestWorker(t *testing.T) {
	r := runCLI(t, nil, "", "worker", "extra")
	if r.code != exitError || !strings.Contains(r.stderr, "(arguments_invalid)") {
		t.Errorf("meridian worker extra = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "worker")
	if r.code != exitOK || !strings.Contains(r.stdout, "usage: meridian worker") || strings.Contains(r.stdout, "isn't built") {
		t.Errorf("meridian help worker = %+v", r)
	}
	// The test's own environment names no coordinator.
	t.Setenv(workerapi.EnvCoordinator, "")
	r = runCLI(t, nil, "", "worker")
	if r.code != exitError || !strings.Contains(r.stderr, workerapi.EnvCoordinator) || !strings.Contains(r.stderr, "(worker_environment_missing)") {
		t.Errorf("meridian worker with no coordinator = %+v", r)
	}
}
