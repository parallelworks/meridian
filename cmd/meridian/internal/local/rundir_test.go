package local

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// TestMakeRunDir checks that a run's directory is made new, readable only
// by its user, and that one that is there, a link among them, is refused,
// before the run is stored and after.
func TestMakeRunDir(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "runs")
	if err := EnsureRunsDir(parent); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(parent); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("the runs' directory is %v, %v", info, err)
	}
	dir := filepath.Join(parent, RunDirName("0123abcd", "7"))
	if filepath.Base(dir) != "7-0123abcd" {
		t.Errorf("the run's directory is named %s", filepath.Base(dir))
	}
	if err := CheckRunDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := MakeRunDir(dir); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("the run's directory is %v, %v", info, err)
	}
	kindtest.Want(t, CheckRunDir(dir), problems.RunDirExists)
	kindtest.Want(t, MakeRunDir(dir), problems.RunDirExists)

	// A link, even to nothing, is something there, which isn't followed.
	link := filepath.Join(parent, "link")
	if err := os.Symlink(filepath.Join(parent, "elsewhere"), link); err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, CheckRunDir(link), problems.RunDirExists)
	kindtest.Want(t, MakeRunDir(link), problems.RunDirExists)
	if _, err := os.Lstat(filepath.Join(parent, "elsewhere")); err == nil {
		t.Error("MakeRunDir made a link's target")
	}

	// A directory whose parent isn't there can't be made.
	kindtest.Want(t, MakeRunDir(filepath.Join(parent, "none", "run")), problems.RunDirNotMade)
}

// TestRunDirUnsafeWay checks that a run's directory is refused under a
// directory that others can change, who could replace it once it is made.
func TestRunDirUnsafeWay(t *testing.T) {
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	run := filepath.Join(shared, "run")
	kindtest.Want(t, CheckRunDir(run), problems.PathUnsafe)
	kindtest.Want(t, MakeRunDir(run), problems.PathUnsafe)
	kindtest.Want(t, EnsureRunsDir(filepath.Join(shared, "runs")), problems.PathUnsafe)
	if _, err := os.Lstat(run); err == nil {
		t.Error("MakeRunDir made a directory under one others can change")
	}
}

func TestRunsDir(t *testing.T) {
	if got, want := RunsDir("/tmp"), "/tmp/meridian-"+strconv.Itoa(os.Getuid()); got != want {
		t.Errorf("RunsDir = %s, want %s", got, want)
	}
	if got := RunDirName("STORE", "../1"); got != "%2E%2E%2F1-%53%54%4F%52%45" {
		t.Errorf("RunDirName escapes to %s", got)
	}
}
