// Package racetest finds the races that the processes tests start report,
// under the race detector, which nothing else sees. A process that finds
// a race reports it on its standard error, or in a file that GORACE's
// log_path names, and exits 66. But a worker the local backend starts
// writes its standard error to a log that the backend deletes once it
// forgets the worker, meridian host's goes to the ssh session, and nobody
// reads their exit statuses.
//
// Main has every process a package's tests start write its reports to
// files of a directory that nothing else deletes, and fails the tests
// with each. Watch and Env do the same for one test, which then fails
// itself, where the test starts the process with Env, or with Env set
// for the host it starts it on.
package racetest

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// marker begins each report the race detector writes.
const marker = "WARNING: DATA RACE"

// logName is the base of the files that a log_path names: the race
// detector adds the process's ID.
const logName = "race"

// workersDir is the directory of the logs of a local backend's workers,
// their standard error.
const workersDir = "workers"

// maxReport is how much of a report is shown.
const maxReport = 8 << 10

// Main runs the tests of m, with every process they start writing its race
// reports to files of a directory of its own (GORACE's log_path), and
// returns what m.Run does, but fails it, once the tests have run, with
// each report there. A package whose tests start the race-built test
// binary, or a worker, calls it from its TestMain, in place of m.Run.
// Without the race detector, it is m.Run.
//
// A process started with a GORACE of its own, as one with Env, writes its
// reports where that says.
func Main(m *testing.M) int {
	if !Enabled {
		return m.Run()
	}
	dir, err := os.MkdirTemp("", "meridian-race-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "racetest: %v\n", err)
		return 1
	}
	defer os.RemoveAll(dir)
	// Before any test runs, so that every process a test starts has it;
	// the race detector read this process's own as it started.
	if err := os.Setenv("GORACE", gorace(dir)); err != nil { //nolint:forbidigo // there is no test to set it for: it is the binary's
		fmt.Fprintf(os.Stderr, "racetest: %v\n", err)
		return 1
	}
	code := m.Run()
	for _, r := range reports(dir) {
		fmt.Fprintf(os.Stderr, "racetest: a process the tests started reported a race, in %s:\n%s\n", r.path, r.text)
		if code == 0 {
			code = 1
		}
	}
	return code
}

// Watch has t check the files under dirs once it ends (Check), after the
// cleanups registered after this call, and before those registered
// before it, as those of t.TempDir: so a test calls it once it has made
// dirs, and before it registers what stops the processes it starts.
func Watch(t testing.TB, dirs ...string) {
	t.Helper()
	t.Cleanup(func() { Check(t, dirs...) })
}

// Check fails t with each race report in the files under dirs where the
// processes a test starts write them: those of Env's log_path, and the
// logs of a local backend's workers, which keep their standard error.
// It does nothing in a binary built without the race detector, which the
// processes a test starts as itself aren't built with either.
func Check(t testing.TB, dirs ...string) {
	t.Helper()
	if !Enabled {
		return
	}
	for _, r := range reports(dirs...) {
		t.Errorf("a process the test started reported a race, in %s:\n%s", r.path, r.text)
	}
}

// Env returns the GORACE variable, as NAME=value, for a process a test
// starts: the test's own GORACE, with log_path in dir, where Check reads.
// dir mustn't hold a space, which GORACE takes to end the path.
func Env(dir string) string {
	return "GORACE=" + gorace(dir)
}

// gorace returns this process's GORACE, with log_path in dir after it.
func gorace(dir string) string {
	return strings.TrimSpace(os.Getenv("GORACE") + " log_path=" + filepath.Join(dir, logName))
}

// A report is a race report, at most maxReport of it, and the file it is
// in.
type report struct {
	path string
	text []byte
}

// reports returns the race reports in the files under dirs that hold
// them: those a log_path names, and workers' logs. It reads no other
// file, as a step's log, which can hold anything, or a binary, which
// holds the words of a report.
func reports(dirs ...string) []report {
	var out []report
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			// A directory that is gone, or that the test made unreadable, has
			// no report to read.
			if err != nil || !d.Type().IsRegular() {
				return nil //nolint:nilerr // see above
			}
			name := d.Name()
			if !strings.HasPrefix(name, logName+".") && (!strings.HasSuffix(name, ".log") || filepath.Base(filepath.Dir(path)) != workersDir) {
				return nil
			}
			data, err := os.ReadFile(path) //nolint:gosec // a file under a directory of the test's
			if err != nil {
				return nil //nolint:nilerr // as above
			}
			if i := bytes.Index(data, []byte(marker)); i >= 0 {
				text := data[i:]
				if len(text) > maxReport {
					text = text[:maxReport]
				}
				out = append(out, report{path, text})
			}
			return nil
		})
	}
	return out
}
