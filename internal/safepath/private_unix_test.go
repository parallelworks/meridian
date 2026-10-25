//go:build unix

package safepath

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/problems"
)

// ownerless is a file's info on a system that doesn't say who owns it.
type ownerless struct{ mode fs.FileMode }

func (o ownerless) Name() string       { return "" }
func (o ownerless) Size() int64        { return 0 }
func (o ownerless) Mode() fs.FileMode  { return o.mode }
func (o ownerless) ModTime() time.Time { return time.Time{} }
func (o ownerless) IsDir() bool        { return o.mode.IsDir() }
func (o ownerless) Sys() any           { return nil }

// TestCheckInfo checks what a directory or a file of the user's must be:
// the user's, and as closed to other users as its access says; and that
// one whose owner the system doesn't say is refused.
func TestCheckInfo(t *testing.T) {
	t.Parallel()
	me := uint32(os.Getuid()) //nolint:gosec // a user ID, which fits
	other := me + 1
	file := func(perm fs.FileMode, uid uint32) fakeInfo { return fakeInfo{perm, uid} }
	for _, tc := range []struct {
		name   string
		info   fs.FileInfo
		dir    bool
		access Access
		kind   *problems.Kind // nil for none
	}{
		{name: "private directory", info: fakeDir(0o700, me), dir: true, access: Private},
		{name: "directory others can read", info: fakeDir(0o755, me), dir: true, access: Private, kind: problems.DirectoryNotPrivate},
		{name: "directory others can read but not change", info: fakeDir(0o755, me), dir: true, access: Unchangeable},
		{name: "directory others can change", info: fakeDir(0o775, me), dir: true, access: Unchangeable, kind: problems.DirectoryNotPrivate},
		{name: "another's directory", info: fakeDir(0o700, other), dir: true, access: Private, kind: problems.DirectoryNotOwned},
		{name: "a file for a directory", info: file(0o600, me), dir: true, access: Private, kind: problems.NotADirectory},
		{name: "directory without an owner", info: ownerless{fs.ModeDir | 0o700}, dir: true, access: Private, kind: problems.OwnerUnknown},
		{name: "private file", info: file(0o600, me), access: Private},
		{name: "file others can read", info: file(0o640, me), access: Private, kind: problems.FileNotPrivate},
		{name: "file others can read but not change", info: file(0o644, me), access: Unchangeable},
		{name: "file others can change", info: file(0o664, me), access: Unchangeable, kind: problems.FileChangeable},
		{name: "another's file", info: file(0o600, other), access: Private, kind: problems.FileNotOwned},
		{name: "link", info: file(fs.ModeSymlink|0o777, me), access: Private, kind: problems.FileNotRegular},
		{name: "file without an owner", info: ownerless{0o600}, access: Private, kind: problems.OwnerUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := CheckFileInfo
			if tc.dir {
				check = CheckDirInfo
			}
			err := check("/p", tc.info, tc.access)
			if tc.kind == nil {
				if err != nil {
					t.Error(err)
				}
				return
			}
			if params := wantProblem(t, err, tc.kind); params["path"] != "/p" {
				t.Errorf("the problem's params are %v", params)
			}
		})
	}
}

// TestPathCheckerOwnerUnknown checks that a way whose directories' owners
// the system doesn't say is refused, rather than taken to be safe.
func TestPathCheckerOwnerUnknown(t *testing.T) {
	t.Parallel()
	pc := checker{
		uid:      user,
		lstat:    func(string) (fs.FileInfo, error) { return ownerless{fs.ModeDir | 0o755}, nil },
		readlink: func(string) (string, error) { return "", fs.ErrInvalid },
		unmapped: func(int) bool { return false },
	}
	if params := wantProblem(t, pc.check("/home/u/x", false), problems.OwnerUnknown); params["path"] != "/" {
		t.Errorf("the problem's params are %v", params)
	}
}

// TestEnsureDir checks that EnsureDir makes a directory, and those on the
// way to it, only its user's, and refuses one others can change the way
// to before it makes anything; and that a directory can be a link to one
// elsewhere.
func TestEnsureDir(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dir := filepath.Join(base, "a", "b")
	if err := EnsureDir(dir, Private); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{dir, filepath.Dir(dir)} {
		if info, err := os.Stat(d); err != nil || info.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v", d, info, err)
		}
	}
	if err := EnsureDir(dir, Private); err != nil {
		t.Errorf("again: %v", err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(link, Private); err != nil {
		t.Errorf("a link to a private directory: %v", err)
	}
	shared := filepath.Join(base, "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil { //nolint:gosec // a directory others can change, which EnsureDir refuses
		t.Fatal(err)
	}
	if params := wantProblem(t, EnsureDir(filepath.Join(shared, "x"), Private), problems.PathUnsafe); params["dir"] != shared {
		t.Errorf("the problem's params are %v", params)
	}
	if _, err := os.Stat(filepath.Join(shared, "x")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("EnsureDir made a directory it refused: %v", err)
	}
	if params := wantProblem(t, CheckDir(shared, Unchangeable), problems.DirectoryNotPrivate); params["mode"] != "0777" {
		t.Errorf("the problem's params are %v", params)
	}
}

// TestReadFile checks that ReadFile reads a file of the user's whole, and
// refuses one others can use more than its access allows, one too large,
// a symbolic link, and one in a directory others can change.
func TestReadFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name, data string, perm fs.FileMode) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, perm); err != nil {
			t.Fatal(err)
		}
		return p
	}
	private := write("private", "secret", 0o600)
	if data, err := ReadFile(private, Private, 6); err != nil || string(data) != "secret" {
		t.Errorf("got %q, %v", data, err)
	}
	wantProblem(t, func() error { _, err := ReadFile(private, Private, 5); return err }(), problems.FileOverLimit)
	readable := write("readable", "x", 0o644)
	if _, err := ReadFile(readable, Unchangeable, 10); err != nil {
		t.Errorf("a file others can read but not change: %v", err)
	}
	wantProblem(t, func() error { _, err := ReadFile(readable, Private, 10); return err }(), problems.FileNotPrivate)
	link := filepath.Join(dir, "link")
	if err := os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	wantProblem(t, func() error { _, err := ReadFile(link, Private, 10); return err }(), problems.FileNotRegular)
	if _, err := ReadFile(filepath.Join(dir, "missing"), Private, 10); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a file that isn't there: %v", err)
	}
	shared := filepath.Join(dir, "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	inShared := filepath.Join(shared, "f")
	if err := os.WriteFile(inShared, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil { //nolint:gosec // a directory others can change, which ReadFile refuses
		t.Fatal(err)
	}
	if params := wantProblem(t, func() error { _, err := ReadFile(inShared, Private, 10); return err }(), problems.PathUnsafe); !strings.HasPrefix(params["dir"].(string), shared) {
		t.Errorf("the problem's params are %v", params)
	}
}

// TestReadFileDotDot checks that a .. after a symbolic link in a path
// names what Check checked, the directory before the link, not the
// parent of the link's target, where the kernel would take it, which
// Check never saw.
func TestReadFileDotDot(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	deep := filepath.Join(base, "other", "deep")
	if err := os.MkdirAll(deep, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "f"), []byte("checked"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "other", "f"), []byte("unchecked"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(deep, filepath.Join(base, "link")); err != nil {
		t.Fatal(err)
	}
	data, err := ReadFile(filepath.Join(base, "link")+"/../f", Private, 20)
	if err != nil || string(data) != "checked" {
		t.Errorf("got %q, %v", data, err)
	}
}
