package local

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
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
	for name, vars := range map[string]map[string]string{
		"no home":           {},
		"relative home":     {"HOME": "home"},
		"relative override": {"HOME": "/home/u", EnvStateDir: "state"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Locate(env(vars)); err == nil {
				t.Error("Locate succeeded")
			}
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
	err := d.EnsureState()
	if err == nil || !strings.Contains(err.Error(), "chmod 700 "+d.State) {
		t.Errorf("EnsureState = %v, want an error that says to chmod 700", err)
	}
}

func TestEnsureStateRefusesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (Dirs{State: path}).EnsureState(); err == nil {
		t.Error("EnsureState succeeded for a file")
	}
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
	if err == nil {
		t.Fatal("Database succeeded on a network filesystem")
	}
	for _, want := range []string{d.State, "nfs4", EnvStateDir + "="} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Database's error %q doesn't say %q", err, want)
		}
	}
}
