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
	l, err := NewLogs(dir)
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
