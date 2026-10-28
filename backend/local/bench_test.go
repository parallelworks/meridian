//go:build linux || darwin || freebsd

package local

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/internal/safepath"
)

// BenchmarkPlace places a worker, as the coordinator does each attempt's:
// it checks the binary the worker runs, this test's, and its digest,
// starts the worker, a stub that exits at once, and records the placement.
func BenchmarkPlace(b *testing.B) {
	d, _ := newDriver(b, "exit")
	var attempts []string
	for b.Loop() {
		attempt := "a" + strconv.Itoa(len(attempts))
		if _, err := d.Place(b.Context(), host, backend.PlaceRequest{Attempt: attempt, BootstrapToken: "t"}); err != nil {
			b.Fatal(err)
		}
		attempts = append(attempts, attempt)
	}
	b.StopTimer()
	// Each worker's exit is recorded before the test's directory goes.
	for _, attempt := range attempts {
		deadline := time.Now().Add(time.Minute)
		for {
			if _, err := os.Lstat(d.path(attempt, ".exit")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				b.Fatalf("%s's worker didn't exit", attempt)
			}
			<-time.After(10 * time.Millisecond)
		}
	}
}

// BenchmarkProbe probes a member, as placement does for each attempt,
// beside the placements of 0, and of 1,000, workers that have ended, as
// the attempts of earlier runs leave them.
func BenchmarkProbe(b *testing.B) {
	for _, stale := range []int{0, 1000} {
		b.Run(strconv.Itoa(stale), func(b *testing.B) {
			d, _ := newDriver(b, "exit")
			if err := safepath.EnsureDir(filepath.Join(d.State, placementsDir), safepath.Private); err != nil {
				b.Fatal(err)
			}
			ended := endedRecord(b)
			for i := range stale {
				if err := safepath.WriteFile(d.path("stale"+strconv.Itoa(i), ".json"), mustMarshal(ended)); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				fit, err := d.Probe(b.Context(), host, backend.Demand{Resources: backend.Resources{MilliCPU: 100}})
				if err != nil || fit.Availability != backend.StartsNow {
					b.Fatalf("Probe = %+v, %v", fit, err)
				}
			}
		})
	}
}

// endedRecord is the record of a worker that has ended: a process of this
// boot's that has exited, and been reaped.
func endedRecord(b *testing.B) *record {
	b.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$") //nolint:gosec,noctx // this test binary, which exits at once
	cmd.Env = append(os.Environ(), stubEnv+"=exit", reportEnv+"="+filepath.Join(b.TempDir(), "report.json"))
	if err := cmd.Run(); err != nil {
		b.Fatal(err)
	}
	boot, err := bootID()
	if err != nil {
		b.Fatal(err)
	}
	return &record{PID: cmd.ProcessState.Pid(), Start: "1", Boot: boot, Resources: backend.Resources{MilliCPU: 1000}, Executable: os.Args[0], Digest: "sha256:00"}
}
