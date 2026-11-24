//go:build unix

package batch

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestRecordsFIFO checks that a FIFO where a record, or the job's output,
// would be doesn't hold up reading them: it is opened without waiting for
// a writer, and refused.
func TestRecordsFIFO(t *testing.T) {
	t.Parallel()
	sub := t.TempDir()
	for _, name := range []string{ExitFile, OutFile} {
		if err := syscall.Mkfifo(filepath.Join(sub, name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan records, 1)
	go func() {
		r := readRecords(sub)
		_ = lastLine(sub)
		done <- r
	}()
	select {
	case r := <-done:
		if r.exit != nil {
			t.Errorf("a FIFO read as an exit record: %+v", r.exit)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reading a submission's records waited on a FIFO")
	}
}
