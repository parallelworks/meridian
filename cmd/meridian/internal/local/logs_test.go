package local

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/workerapi"
)

var stepLog = coordinator.LogKey{Run: "r1", Job: "build#2", Attempt: 1, Part: workerapi.LogStep, Step: 3, Try: 1}

type appendCall struct {
	offset  int64
	data    string
	end     bool
	length  int64
	ended   bool
	wantGap bool
}

func newLogs(t *testing.T, dir string) *Logs {
	t.Helper()
	return newStoreLogs(t, dir, "store-a")
}

// newStoreLogs returns the Logs of the store storeID in dir.
func newStoreLogs(t *testing.T, dir, storeID string) *Logs {
	t.Helper()
	l, err := NewLogs(dir, storeID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	return l
}

func TestLogsAppend(t *testing.T) {
	l := newLogs(t, t.TempDir())
	for i, c := range []appendCall{
		{offset: 0, data: "hello ", length: 6},
		{offset: 6, data: "world", length: 11},
		// Repeated after a dropped connection: stored once.
		{offset: 6, data: "world", length: 11},
		// Partly repeated: only what is new is stored.
		{offset: 8, data: "rld!\n", length: 13},
		// Past the end.
		{offset: 20, data: "x", length: 13, wantGap: true},
		{offset: 13, data: "", end: true, length: 13, ended: true},
		// The end, repeated.
		{offset: 13, data: "", end: true, length: 13, ended: true},
		{offset: 11, data: "!\n", length: 13, ended: true},
		// Past the end of a log that has ended.
		{offset: 13, data: "more", length: 13, ended: true, wantGap: true},
		{offset: 12, data: "\nmore", length: 13, ended: true, wantGap: true},
	} {
		length, ended, err := l.Append(t.Context(), stepLog, c.offset, []byte(c.data), c.end)
		if gap := errors.Is(err, coordinator.ErrLogGap); gap != c.wantGap || (err != nil && !gap) {
			t.Fatalf("append %d: err = %v, want a gap: %t", i, err, c.wantGap)
		}
		if length != c.length || ended != c.ended {
			t.Errorf("append %d = %d, %t, want %d, %t", i, length, ended, c.length, c.ended)
		}
	}
	data, ended, err := l.Read(t.Context(), stepLog, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world!\n" || !ended {
		t.Errorf("Read = %q, %t", data, ended)
	}
}

func TestLogsEndWithData(t *testing.T) {
	l := newLogs(t, t.TempDir())
	if length, ended, err := l.Append(t.Context(), stepLog, 0, []byte("all of it"), true); err != nil || length != 9 || !ended {
		t.Fatalf("Append = %d, %t, %v", length, ended, err)
	}
}

func TestLogsRead(t *testing.T) {
	l := newLogs(t, t.TempDir())
	data, ended, err := l.Read(t.Context(), stepLog, 0, 10)
	if err != nil || len(data) != 0 || ended {
		t.Errorf("Read of a log that hasn't begun = %q, %t, %v", data, ended, err)
	}
	if _, _, err := l.Append(t.Context(), stepLog, 0, []byte("0123456789"), false); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		offset int64
		limit  int
		want   string
	}{
		{0, 4, "0123"},
		{4, 4, "4567"},
		{8, 4, "89"},
		{10, 4, ""},
		{12, 4, ""},
		{0, 0, ""},
		{0, math.MaxInt, "0123456789"},
	} {
		data, ended, err := l.Read(t.Context(), stepLog, c.offset, c.limit)
		if err != nil || string(data) != c.want || ended {
			t.Errorf("Read(%d, %d) = %q, %t, %v, want %q", c.offset, c.limit, data, ended, err, c.want)
		}
	}
	if _, _, err := l.Read(t.Context(), stepLog, -1, 4); err == nil {
		t.Error("Read from -1 succeeded")
	}
}

func TestLogsKeys(t *testing.T) {
	dir := t.TempDir()
	l := newLogs(t, dir)
	keys := []coordinator.LogKey{
		{Run: "r1", Job: "build", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "r1", Job: "build", Attempt: 2, Part: workerapi.LogSetup},
		{Run: "r1", Job: "build", Attempt: 1, Part: workerapi.LogStep, Step: 0, Try: 0},
		{Run: "r1", Job: "build", Attempt: 1, Part: workerapi.LogStep, Step: 0, Try: 1},
		{Run: "r1", Job: "build", Attempt: 1, Part: workerapi.LogCleanup, Step: 0},
		{Run: "r1", Job: "build#1", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "../r1", Job: "../../x", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "r1/..", Job: ".", Attempt: 1, Part: workerapi.LogSetup},
	}
	for i, k := range keys {
		if _, _, err := l.Append(t.Context(), k, 0, []byte{byte('a' + i)}, false); err != nil {
			t.Fatalf("Append(%+v): %v", k, err)
		}
	}
	for i, k := range keys {
		data, _, err := l.Read(t.Context(), k, 0, 10)
		if err != nil || string(data) != string(rune('a'+i)) {
			t.Errorf("Read(%+v) = %q, %v: logs share a file", k, data, err)
		}
	}
	// Every log is under the logs directory, whatever its key.
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasPrefix(path, filepath.Join(dir, LogsDir)) && path != dir {
			t.Errorf("%s is outside the logs directory", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []coordinator.LogKey{
		{Job: "build", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "r1", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "r1", Job: "build", Attempt: 0, Part: workerapi.LogSetup},
		{Run: "r1", Job: "build", Attempt: 1, Part: "trace"},
		{Run: "r1", Job: "build", Attempt: 1, Part: workerapi.LogStep, Step: -1},
	} {
		if _, _, err := l.Append(t.Context(), bad, 0, nil, false); err == nil {
			t.Errorf("Append(%+v) succeeded", bad)
		}
	}
}

// TestLogsStores checks that two stores' logs of runs of the same number
// are apart, as of a store made again, whose runs are numbered from 1
// again, and that a store's ID can name no other directory.
func TestLogsStores(t *testing.T) {
	dir := t.TempDir()
	for i, store := range []string{"store-a", "store-b", "../store-a", "STORE-A"} {
		if _, _, err := newStoreLogs(t, dir, store).Append(t.Context(), stepLog, 0, []byte{byte('a' + i)}, false); err != nil {
			t.Fatal(err)
		}
	}
	for i, store := range []string{"store-a", "store-b", "../store-a", "STORE-A"} {
		data, _, err := newStoreLogs(t, dir, store).Read(t.Context(), stepLog, 0, 10)
		if err != nil || string(data) != string(rune('a'+i)) {
			t.Errorf("the store %s's log is %q, %v: stores share a log", store, data, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != LogsDir {
		t.Errorf("the state directory holds %v, %v", entries, err)
	}
	if _, err := NewLogs(dir, ""); err == nil {
		t.Error("NewLogs without a store's ID succeeded")
	}
}

// TestLogsConcurrentRepeats checks that an upload repeated at once, as by
// a worker that retried while its first try was still being stored, is
// stored once.
func TestLogsConcurrentRepeats(t *testing.T) {
	l := newLogs(t, t.TempDir())
	parts := []string{"one\n", "two\n", "three\n"}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			offset := int64(0)
			for _, p := range parts {
				// Another goroutine may have stored it first: the log has
				// the bytes then, and the length says where they end.
				if _, _, err := l.Append(t.Context(), stepLog, offset, []byte(p), false); err != nil {
					t.Error(err)
				}
				offset += int64(len(p))
			}
		})
	}
	wg.Wait()
	data, _, err := l.Read(t.Context(), stepLog, 0, 100)
	if err != nil || string(data) != "one\ntwo\nthree\n" {
		t.Errorf("Read = %q, %v", data, err)
	}
}

// TestLogNames checks that log names stay within what a filesystem allows
// a name, and stay apart on one that ignores case.
func TestLogNames(t *testing.T) {
	l := newLogs(t, t.TempDir())
	long := strings.Repeat("j", 255)
	keys := []coordinator.LogKey{
		{Run: "r1", Job: long + "#12", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "r1", Job: long + "#13", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "Run", Job: "build", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "run", Job: "build", Attempt: 1, Part: workerapi.LogSetup},
		{Run: "RUN", Job: "Build", Attempt: 1, Part: workerapi.LogSetup},
	}
	seen := map[string]bool{}
	for i, k := range keys {
		if _, _, err := l.Append(t.Context(), k, 0, []byte{byte('a' + i)}, false); err != nil {
			t.Fatalf("Append(%.40v): %v", k, err)
		}
		path, err := l.path(k)
		if err != nil {
			t.Fatal(err)
		}
		folded := strings.ToLower(path)
		if seen[folded] {
			t.Errorf("%s is another log's path on a filesystem that ignores case", path)
		}
		seen[folded] = true
	}
	for i, k := range keys {
		if data, _, err := l.Read(t.Context(), k, 0, 10); err != nil || string(data) != string(rune('a'+i)) {
			t.Errorf("Read(%.40v) = %q, %v", k, data, err)
		}
	}
	for _, s := range []string{long, long + "#1", strings.Repeat("%", 200), strings.Repeat("é", 100)} {
		name := escapeName(s)
		if len(name) > maxName {
			t.Errorf("escapeName gives %d bytes for %.20q", len(name), s)
		}
		if strings.ToLower(name) != name && !strings.Contains(name, "%") {
			t.Errorf("escapeName(%.20q) = %q has upper case", s, name)
		}
	}
	if escapeName(long+"#12") == escapeName(long+"#13") {
		t.Error("two long names that differ at their end are cut to one")
	}
}
