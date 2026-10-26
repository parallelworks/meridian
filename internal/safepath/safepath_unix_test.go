//go:build unix

package safepath

import (
	"io/fs"
	"maps"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// fakeInfo is a file's info as lstat gives it, with its owner.
type fakeInfo struct {
	mode fs.FileMode
	uid  uint32
}

func (f fakeInfo) Name() string       { return "" }
func (f fakeInfo) Size() int64        { return 0 }
func (f fakeInfo) Mode() fs.FileMode  { return f.mode }
func (f fakeInfo) ModTime() time.Time { return time.Time{} }
func (f fakeInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeInfo) Sys() any           { return &syscall.Stat_t{Uid: f.uid} }

// The users of the tests: root, the user Meridian runs as, another, and
// nobody, the ID a user namespace shows for an owner it doesn't map.
const (
	root    = 0
	user    = 1000
	mallory = 2000
	nobody  = 65534
)

// fakeFS is a filesystem of fakeInfos and symbolic links' targets.
type fakeFS struct {
	infos map[string]fakeInfo
	links map[string]string
	// namespace says that nobody is an owner a user namespace hides.
	namespace bool
}

func (f fakeFS) checker() checker {
	return checker{
		uid: user,
		lstat: func(p string) (fs.FileInfo, error) {
			if info, ok := f.infos[p]; ok {
				return info, nil
			}
			return nil, fs.ErrNotExist
		},
		readlink: func(p string) (string, error) {
			if t, ok := f.links[p]; ok {
				return t, nil
			}
			return "", fs.ErrInvalid
		},
		unmapped: func(uid int) bool { return uid == nobody && f.namespace },
	}
}

func fakeDir(perm fs.FileMode, uid uint32) fakeInfo { return fakeInfo{fs.ModeDir | perm, uid} }

// base is a filesystem whose / and /home are root's, and /home/u the
// user's, /tmp and /var/tmp sticky, and /var/tmp/alice the user's.
func base() map[string]fakeInfo {
	return map[string]fakeInfo{
		"/":              fakeDir(0o755, root),
		"/home":          fakeDir(0o755, root),
		"/home/u":        fakeDir(0o700, user),
		"/tmp":           fakeDir(0o777|fs.ModeSticky, root),
		"/var":           fakeDir(0o755, root),
		"/var/tmp":       fakeDir(0o777|fs.ModeSticky, root),
		"/var/tmp/alice": fakeDir(0o700, user),
	}
}

// TestPathChecker checks the walk on the way to a path as OpenSSH's
// safe_path does, with owners a test can't give a real file.
func TestPathChecker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		add    map[string]fakeInfo
		links  map[string]string
		path   string
		leaf   bool
		unsafe string // the directory or link the problem names, or "" for none
	}{
		{name: "home", path: "/home/u/.local/state/meridian"},
		{name: "home itself", path: "/home/u", leaf: true},
		{name: "sticky, the user's", path: "/var/tmp/alice/meridian"},
		{name: "sticky, to be created", path: "/var/tmp/new/meridian"},
		{
			name: "sticky, another's", add: map[string]fakeInfo{"/var/tmp/mallory": fakeDir(0o755, mallory)},
			path: "/var/tmp/mallory/meridian", unsafe: "/var/tmp/mallory",
		},
		{
			name: "sticky, root's", add: map[string]fakeInfo{"/var/tmp/shared": fakeDir(0o755, root)},
			path: "/var/tmp/shared/x",
		},
		{
			name: "open, not sticky", add: map[string]fakeInfo{"/home/u/shared": fakeDir(0o777, user)},
			path: "/home/u/shared/meridian", unsafe: "/home/u/shared",
		},
		{
			name: "group writable", add: map[string]fakeInfo{"/home/u/team": fakeDir(0o775, user)},
			path: "/home/u/team/meridian", unsafe: "/home/u/team",
		},
		{
			name: "another's, closed", add: map[string]fakeInfo{"/home/m": fakeDir(0o755, mallory)},
			path: "/home/m/meridian", unsafe: "/home/m",
		},
		{
			name: "leaf open", add: map[string]fakeInfo{"/home/u/config": fakeDir(0o777, user)},
			path: "/home/u/config", leaf: true, unsafe: "/home/u/config",
		},
		{
			name: "link of the user's", add: map[string]fakeInfo{"/home/u/state": {fs.ModeSymlink | 0o777, user}, "/scratch": fakeDir(0o755, root), "/scratch/u": fakeDir(0o700, user)},
			links: map[string]string{"/home/u/state": "/scratch/u"},
			path:  "/home/u/state/meridian",
		},
		{
			name: "link to an open directory", add: map[string]fakeInfo{"/home/u/state": {fs.ModeSymlink | 0o777, user}, "/scratch": fakeDir(0o777, root)},
			links: map[string]string{"/home/u/state": "/scratch/u"},
			path:  "/home/u/state/meridian", unsafe: "/scratch",
		},
		{
			name: "link of another's in a sticky directory", add: map[string]fakeInfo{"/tmp/m": {fs.ModeSymlink | 0o777, mallory}},
			links: map[string]string{"/tmp/m": "/home/u"},
			path:  "/tmp/m/meridian", unsafe: "/tmp/m",
		},
		{
			name: "relative link", add: map[string]fakeInfo{"/home/u/s": {fs.ModeSymlink | 0o777, user}, "/home/u/real": fakeDir(0o700, user)},
			links: map[string]string{"/home/u/s": "../u/real"},
			path:  "/home/u/s/meridian",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			infos := base()
			maps.Copy(infos, tc.add)
			err := fakeFS{infos, tc.links, false}.checker().check(tc.path, tc.leaf)
			if tc.unsafe == "" {
				if err != nil {
					t.Errorf("check(%s): %v", tc.path, err)
				}
				return
			}
			params := kindtest.Want(t, err, problems.PathUnsafe).Params
			if params["dir"] != tc.unsafe || params["path"] != tc.path {
				t.Errorf("check(%s) names %v, want %s", tc.path, params, tc.unsafe)
			}
		})
	}
}

func TestPathCheckerLinkLoop(t *testing.T) {
	infos := base()
	infos["/home/u/a"] = fakeInfo{fs.ModeSymlink | 0o777, user}
	err := fakeFS{infos, map[string]string{"/home/u/a": "/home/u/a"}, false}.checker().check("/home/u/a/x", false)
	kindtest.Want(t, err, problems.PathLinksTooMany)
}

// TestPathCheckerUnmapped checks that, in a user namespace that doesn't map
// root, where / is nobody's, every path is refused, as one whose owner is
// hidden rather than one another user owns, and that a directory nobody
// owns outside such a namespace is just another user's.
func TestPathCheckerUnmapped(t *testing.T) {
	infos := base()
	infos["/"] = fakeDir(0o755, nobody)
	infos["/home"] = fakeDir(0o755, nobody)
	err := fakeFS{infos, nil, true}.checker().check("/home/u/.local/state/meridian", false)
	if params := kindtest.Want(t, err, problems.OwnerUnmapped).Params; params["dir"] != "/" {
		t.Errorf("the problem's params are %v", params)
	}
	err = fakeFS{infos, nil, false}.checker().check("/home/u/.local/state/meridian", false)
	kindtest.Want(t, err, problems.PathUnsafe)
}
