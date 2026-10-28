package racetest_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/internal/racetest"
)

// envRacy has the test binary, started again, race and exit.
const envRacy = "MERIDIAN_RACETEST_RACY"

func TestMain(m *testing.M) {
	if os.Getenv(envRacy) == "1" {
		race()
		os.Exit(0)
	}
	os.Exit(racetest.Main(m))
}

// TestMainGORACE checks that Main gives the processes the tests start a
// log_path of its own.
func TestMainGORACE(t *testing.T) {
	if !racetest.Enabled {
		t.Skip("the race detector is off")
	}
	if !strings.Contains(os.Getenv("GORACE"), "log_path="+os.TempDir()) {
		t.Errorf("GORACE is %q, without a log_path of Main's", os.Getenv("GORACE"))
	}
}

// race writes a variable from two goroutines at once, which the race
// detector reports.
func race() {
	var (
		n  int
		wg sync.WaitGroup
	)
	for range 2 {
		wg.Go(func() { n++ })
	}
	wg.Wait()
	fmt.Println(n)
}

// A recorder is a testing.TB that keeps what Check fails it with.
type recorder struct {
	testing.TB
	errs []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// TestCheck checks that Check reports the race of a process the test
// started with Env, and one in a worker's log, and nothing in other files:
// a binary, which holds the words of a report, or a step's log.
func TestCheck(t *testing.T) {
	if !racetest.Enabled {
		t.Skip("the race detector is off")
	}
	t.Parallel()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe)
	cmd.Env = append(os.Environ(), envRacy+"=1", racetest.Env(dir))
	if out, err := cmd.CombinedOutput(); err == nil || strings.Contains(string(out), "DATA RACE") {
		t.Fatalf("the racy process ended with %v, and wrote, not to its log: %s", err, out)
	}
	for name, text := range map[string]string{
		"workers/a.log": "starting\nWARNING: DATA RACE\nWrite at 0x00c000012345 by goroutine 7:\n",
		"workers/b.log": "nothing went wrong\n",
		"bin/meridian":  "WARNING: DATA RACE",
		"logs/step.log": "a step that prints\nWARNING: DATA RACE\n",
	} {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r := &recorder{TB: t}
	racetest.Check(r, dir)
	if len(r.errs) != 2 || !strings.Contains(strings.Join(r.errs, "\n"), "a.log") || !strings.Contains(strings.Join(r.errs, "\n"), filepath.Join(dir, "race.")) {
		t.Errorf("Check reported %d races: %q", len(r.errs), r.errs)
	}
}

// TestEnv checks that Env keeps the test's own GORACE, before its own
// log_path.
func TestEnv(t *testing.T) {
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	if got, want := racetest.Env("/tmp/x"), "GORACE=atexit_sleep_ms=0 log_path=/tmp/x/race"; got != want {
		t.Errorf("Env = %q, want %q", got, want)
	}
}
