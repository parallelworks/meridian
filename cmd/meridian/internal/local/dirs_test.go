package local

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

// lookup reads vars as os.LookupEnv reads the environment.
func lookup(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestLocate(t *testing.T) {
	if runtime.GOOS == "darwin" {
		d, err := Locate(env(map[string]string{"HOME": "/Users/u", "XDG_STATE_HOME": "/x"}))
		if err != nil {
			t.Fatal(err)
		}
		want := Dirs{Config: "/Users/u/Library/Application Support/Meridian", State: "/Users/u/Library/Application Support/Meridian", Cache: "/Users/u/Library/Caches/Meridian"}
		if d != want {
			t.Errorf("Locate = %+v, want %+v", d, want)
		}
		return
	}
	for _, tc := range []struct {
		name string
		env  map[string]string
		want Dirs
	}{
		{
			name: "home",
			env:  map[string]string{"HOME": "/home/u"},
			want: Dirs{Config: "/home/u/.config/meridian", State: "/home/u/.local/state/meridian", Cache: "/home/u/.cache/meridian"},
		},
		{
			name: "xdg",
			env:  map[string]string{"HOME": "/home/u", "XDG_CONFIG_HOME": "/c", "XDG_STATE_HOME": "/s", "XDG_CACHE_HOME": "/k"},
			want: Dirs{Config: "/c/meridian", State: "/s/meridian", Cache: "/k/meridian"},
		},
		{
			// The XDG specification says a relative path is ignored.
			name: "relative xdg",
			env:  map[string]string{"HOME": "/home/u", "XDG_STATE_HOME": "state"},
			want: Dirs{Config: "/home/u/.config/meridian", State: "/home/u/.local/state/meridian", Cache: "/home/u/.cache/meridian"},
		},
		{
			name: "meridian",
			env:  map[string]string{"XDG_STATE_HOME": "/s", EnvConfigDir: "/a/", EnvStateDir: "/var/tmp/u/m", EnvCacheDir: "/b"},
			want: Dirs{Config: "/a", State: "/var/tmp/u/m", Cache: "/b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Locate(env(tc.env))
			if err != nil {
				t.Fatal(err)
			}
			if d != tc.want {
				t.Errorf("Locate = %+v, want %+v", d, tc.want)
			}
		})
	}
}

func TestLocateRefuses(t *testing.T) {
	for name, tc := range map[string]struct {
		vars map[string]string
		kind *problems.Kind
	}{
		"no home":           {map[string]string{}, problems.HomeUnknown},
		"relative home":     {map[string]string{"HOME": "home"}, problems.HomeUnknown},
		"relative override": {map[string]string{"HOME": "/home/u", EnvStateDir: "state"}, problems.DirectoryVariableRelative},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Locate(env(tc.vars))
			kindtest.Want(t, err, tc.kind)
		})
	}
}

func TestEnsureState(t *testing.T) {
	d := Dirs{State: filepath.Join(t.TempDir(), "a", "state")}
	if err := d.EnsureState(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{d.State, filepath.Dir(d.State)} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("%s has mode %04o, want 0700", dir, perm)
		}
	}
	// Again, with it there.
	if err := d.EnsureState(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureStateRefusesOpenDirectory(t *testing.T) {
	d := Dirs{State: t.TempDir()}
	if err := os.Chmod(d.State, 0o755); err != nil {
		t.Fatal(err)
	}
	p := kindtest.Want(t, d.EnsureState(), problems.DirectoryNotPrivate)
	if p.Params["path"] != d.State || p.Params["mode"] != "0755" {
		t.Errorf("the problem's params are %v", p.Params)
	}
}

func TestEnsureStateRefusesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, (Dirs{State: path}).EnsureState(), problems.DirectoryUnavailable)
}

func TestEnsureStateFollowsLink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "local")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "state")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := (Dirs{State: link}).EnsureState(); err != nil {
		t.Errorf("EnsureState of a link to a private directory: %v", err)
	}
}

func TestDatabase(t *testing.T) {
	d := Dirs{State: filepath.Join(t.TempDir(), "state")}
	path, err := d.Database()
	if err != nil {
		t.Fatalf("Database on a local disk: %v", err)
	}
	if want := filepath.Join(d.State, DatabaseFile); path != want {
		t.Errorf("Database = %s, want %s", path, want)
	}
}

func TestDatabaseRefusesNetworkFilesystem(t *testing.T) {
	detect := detectNetworkFS
	t.Cleanup(func() { detectNetworkFS = detect })
	detectNetworkFS = func(string) (string, bool, error) { return "nfs4", true, nil }
	d := Dirs{State: filepath.Join(t.TempDir(), "state")}
	_, err := d.Database()
	p := kindtest.Want(t, err, problems.NetworkFilesystem)
	for _, want := range []string{d.State, "nfs4", EnvStateDir} {
		if !strings.Contains(p.Detail, want) {
			t.Errorf("Database's problem %q doesn't say %q", p.Detail, want)
		}
	}
}

// TestEnsureStateRefusesOpenParent checks that a state directory under a
// directory others can change is refused, before Meridian creates
// anything in it: they could replace it once it was checked.
func TestEnsureStateRefusesOpenParent(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatal(err)
	}
	d := Dirs{State: filepath.Join(parent, "meridian")}
	p := kindtest.Want(t, d.EnsureState(), problems.PathUnsafe)
	if p.Params["dir"] != parent {
		t.Errorf("the unsafe directory is %v, want %s", p.Params["dir"], parent)
	}
	if _, err := os.Stat(d.State); err == nil {
		t.Error("EnsureState created the directory before refusing it")
	}
}

// TestEnsureStateInStickyDirectory checks that a directory of the user's in
// one whose sticky bit keeps others from removing it, as /tmp is, is safe.
func TestEnsureStateInStickyDirectory(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "tmp")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	if err := (Dirs{State: filepath.Join(parent, "meridian")}).EnsureState(); err != nil {
		t.Errorf("EnsureState in a sticky directory: %v", err)
	}
}

func TestDatabaseMountNotFound(t *testing.T) {
	detect := detectNetworkFS
	t.Cleanup(func() { detectNetworkFS = detect })
	detectNetworkFS = func(string) (string, bool, error) { return "", false, errNoMount }
	_, err := (Dirs{State: filepath.Join(t.TempDir(), "state")}).Database()
	kindtest.Want(t, err, problems.MountNotFound)
}
