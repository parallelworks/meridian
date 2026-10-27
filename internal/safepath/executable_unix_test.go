//go:build unix

package safepath

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// TestCheckExecutable checks what a file run with what other users
// mustn't have must be: a regular file of the user's or root's that no
// other user can change, on a way no other user can change; and that
// OpenExecutable gives such a file open, and the problem with any other.
func TestCheckExecutable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(path string, perm fs.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, perm); err != nil {
			t.Fatal(err)
		}
	}
	safe, changeable := filepath.Join(dir, "safe"), filepath.Join(dir, "changeable")
	write(safe, 0o755)
	write(changeable, 0o775)
	shared := filepath.Join(dir, "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	inShared := filepath.Join(shared, "cmd")
	write(inShared, 0o755)

	if err := CheckExecutable(safe); err != nil {
		t.Errorf("a safe file: %v", err)
	}
	f, info, err := OpenExecutable(safe, safe)
	if err != nil || f == nil || !info.Mode().IsRegular() {
		t.Errorf("OpenExecutable of a safe file = %v, %v, %v", f, info, err)
	}
	if f != nil {
		f.Close()
	}
	for path, want := range map[string]*problems.Kind{
		changeable: problems.FileChangeable,
		inShared:   problems.PathUnsafe,
		dir:        problems.FileNotRegular,
	} {
		err := CheckExecutable(path)
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != want.Code() {
			t.Errorf("CheckExecutable(%s) = %v, want %s", path, err, want.Code())
		}
	}
	if err := CheckExecutable(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a file that isn't there: %v", err)
	}
	// Root's file, in root's directory, is safe to run, as a system's
	// command is.
	if _, err := os.Stat("/bin/sh"); err == nil {
		sh, err := filepath.EvalSymlinks("/bin/sh")
		if err != nil {
			t.Fatal(err)
		}
		if err := CheckExecutable(sh); err != nil {
			t.Errorf("CheckExecutable(%s) = %v", sh, err)
		}
	}
}
