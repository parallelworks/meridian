// Package racetest finds the races that processes a test starts report,
// under the race detector, which nothing else sees: a worker that the
// local backend starts writes its standard error to a file, and meridian
// host's goes to the ssh session. A process that finds a race reports it
// on its standard error, or in a file GORACE's log_path names, and exits
// 66, which nobody reads either. Check reads both kinds of file, and
// fails the test with each report.
package racetest

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// marker begins each report the race detector writes.
const marker = "WARNING: DATA RACE"

// logName is the base of the files that Env's log_path names: the race
// detector adds the process's ID.
const logName = "race"

// maxReport is how much of a report Check shows.
const maxReport = 8 << 10

// Watch has t check the files under dirs once it ends (Check), after the
// cleanups registered after this call, and before those registered
// before it, as those of t.TempDir: so a test calls it once it has made
// dirs, and before it registers what stops the processes it starts.
func Watch(t testing.TB, dirs ...string) {
	t.Helper()
	t.Cleanup(func() { Check(t, dirs...) })
}

// Check fails t with each race report in the files under dirs that the
// processes a test starts write them to: their standard error, kept in a
// file whose name ends in .log, as the local backend keeps a worker's, and
// the files of Env's log_path. It does nothing in a binary built without
// the race detector, which the processes a test starts as itself aren't
// built with either.
func Check(t testing.TB, dirs ...string) {
	t.Helper()
	if !Enabled {
		return
	}
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			// A directory that is gone, or that the test made unreadable, has
			// no report Check can read.
			if err != nil || !d.Type().IsRegular() {
				return nil //nolint:nilerr // see above
			}
			if name := d.Name(); !strings.HasSuffix(name, ".log") && !strings.HasPrefix(name, logName+".") {
				return nil
			}
			data, err := os.ReadFile(path) //nolint:gosec // a file under a directory of the test's
			if err != nil {
				return nil //nolint:nilerr // as above
			}
			if i := bytes.Index(data, []byte(marker)); i >= 0 {
				report := data[i:]
				if len(report) > maxReport {
					report = report[:maxReport]
				}
				t.Errorf("a process the test started reported a race, in %s:\n%s", path, report)
			}
			return nil
		})
	}
}

// Env returns the GORACE variable, as NAME=value, for a process a test
// starts whose standard error doesn't go to a file Check reads: the
// test's own GORACE, with log_path in dir, which Check reads too. dir
// mustn't hold a space, which GORACE takes to end the path.
func Env(dir string) string {
	return "GORACE=" + strings.TrimSpace(os.Getenv("GORACE")+" log_path="+filepath.Join(dir, logName))
}
