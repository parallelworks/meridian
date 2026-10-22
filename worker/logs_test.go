package worker

import (
	"io"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/workerapi"
)

// TestSpoolShrinks checks that a log's file holds what the coordinator
// hasn't stored, and little more, while its step goes on printing: once
// the bytes sent are many times those not, the rest are copied to a file
// of their own, and once all are sent, the file is emptied.
func TestSpoolShrinks(t *testing.T) {
	t.Parallel()
	s := newSpool(t.TempDir(), 1<<30)
	defer s.close()
	l := s.open(logKey{workerapi.LogStep, 0, 0})
	size := func() int64 {
		t.Helper()
		info, err := l.file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		return info.Size()
	}
	write := func(text string) {
		t.Helper()
		if _, err := io.WriteString(l, text); err != nil {
			t.Fatal(err)
		}
	}
	write(strings.Repeat("a", 90<<10) + strings.Repeat("b", 10<<10))
	s.stored(l, 10<<10, false)
	if got := size(); got != 100<<10 {
		t.Fatalf("with a tenth sent, the file holds %d bytes, want all %d", got, 100<<10)
	}
	s.stored(l, 90<<10, false)
	if got := size(); got != 10<<10 {
		t.Fatalf("with 90 KiB sent, the file holds %d bytes, want the 10 KiB unsent", got)
	}
	write("c")
	u, _ := s.next()
	data := make([]byte, u.n)
	if _, err := u.file.ReadAt(data, u.at); err != nil {
		t.Fatal(err)
	}
	if want := strings.Repeat("b", 10<<10) + "c"; u.offset != 90<<10 || string(data) != want {
		t.Fatalf("the next upload is %d bytes from %d, %q…, want the unsent bytes", len(data), u.offset, data[:min(len(data), 8)])
	}
	s.stored(l, 100<<10+1, false)
	if got := size(); got != 0 {
		t.Errorf("with every byte sent, the file holds %d bytes", got)
	}
}
