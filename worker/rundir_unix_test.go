//go:build unix

package worker

import (
	"io/fs"
	"os"
	"syscall"
	"testing"

	"github.com/parallelworks/meridian/problems"
)

// ownedBy is a directory's info, with what the system says of it, as
// Sys gives it, replaced by st.
type ownedBy struct {
	fs.FileInfo
	st *syscall.Stat_t
}

func (i ownedBy) Sys() any {
	if i.st == nil {
		return nil
	}
	return i.st
}

// TestCheckRunDirOwner checks that checkRunDir refuses a directory of a
// run's tree that another user owns, root among them, whatever its mode,
// as run_dir_not_owned, and one whose owner the system doesn't say as
// owner_unknown; and that it passes the user's own.
func TestCheckRunDirOwner(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRunDir(dir, info); err != nil {
		t.Errorf("the user's own directory: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("the system says %T of a directory", info.Sys())
	}
	others := []int{os.Getuid() + 1}
	if os.Getuid() != 0 {
		others = append(others, 0)
	}
	for _, uid := range others {
		owned := *st
		owned.Uid = uint32(uid)
		checkRefused(t, checkRunDir(dir, ownedBy{info, &owned}), problems.RunDirNotOwned, dir, "")
	}
	checkRefused(t, checkRunDir(dir, ownedBy{info, nil}), problems.OwnerUnknown, dir, "")
}
