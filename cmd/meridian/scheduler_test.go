//go:build linux || darwin || freebsd

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/backend/slurm"
	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// TestSchedulerRoutes checks that the slurm backend's driver routes a
// member by its login node: one with a host, which a later release
// reaches over SSH, isn't built yet, in every call; one without is this
// machine's, as are the members whose settings don't decode, whose calls
// say so. Where the home the cluster's nodes share can't be told, a
// member of this machine can't run a job.
func TestSchedulerRoutes(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	if err := os.MkdirAll(env[local.EnvStateDir], 0o700); err != nil {
		t.Fatal(err)
	}
	c := &cli{lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok }}
	d, _, err := slurmDriver(c, local.NewKeys(env[local.EnvStateDir]), nil)
	if err != nil {
		t.Fatal(err)
	}
	remote := &backend.Member{Name: "far", Backend: "slurm", Settings: jsontext.Value(`{"host":"login1","partition":"debug"}`)}
	fit, err := d.Probe(t.Context(), remote, backend.Demand{})
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, fit.Problem, problems.BackendNotBuilt)
	// A caller is refused whatever the member's login node.
	fit, err = d.Probe(t.Context(), remote, backend.Demand{CallsWorkflow: true})
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, fit.Problem, problems.SchedulerCallerRefused)
	_, err = d.Place(t.Context(), remote, backend.PlaceRequest{Attempt: "a1"})
	kindtest.Want(t, err, problems.BackendNotBuilt)
	_, err = d.MemberKey(remote)
	kindtest.Want(t, err, problems.BackendNotBuilt)
	_, err = d.Forward(t.Context(), remote)
	kindtest.Want(t, err, problems.BackendNotBuilt)
	_, err = d.Check(t.Context(), remote, backend.Placement{})
	kindtest.Want(t, err, problems.BackendNotBuilt)

	here := &backend.Member{Name: "near", Backend: "slurm", Settings: jsontext.Value(`{"partition":"debug"}`)}
	key, err := d.MemberKey(here)
	if err != nil || key[:6] != "batch-" {
		t.Errorf("the key of a member of this machine is %q, %v", key, err)
	}
	if e, err := d.Endpoint(t.Context(), here); err != nil || filepath.Dir(e.AddressFile) != filepath.Join(env["HOME"], ".local", "state", "meridian", "batch", "relays") {
		t.Errorf("the endpoint of a member of this machine is %+v, %v", e, err)
	}
	bad := &backend.Member{Name: "bad", Backend: "slurm", Settings: jsontext.Value(`{"host":7}`)}
	fit, err = d.Probe(t.Context(), bad, backend.Demand{})
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, fit.Problem, problems.MemberSettingInvalid)

	homeless := &cli{lookupEnv: func(name string) (string, bool) { return "", false }}
	d, _, err = slurmDriver(homeless, local.NewKeys(env[local.EnvStateDir]), nil)
	if err != nil {
		t.Fatal(err)
	}
	fit, err = d.Probe(t.Context(), here, backend.Demand{})
	if err != nil {
		t.Fatal(err)
	}
	kindtest.Want(t, fit.Problem, problems.HomeUnknown)
}

// TestSchedulerHome checks where the slurm backend keeps what compute
// nodes read: in XDG's state and cache directories, or under the home,
// whatever MERIDIAN_STATE_DIR and MERIDIAN_CACHE_DIR say of this
// machine's own.
func TestSchedulerHome(t *testing.T) {
	parallel(t)
	for _, tc := range []struct {
		env          map[string]string
		state, cache string
	}{
		{map[string]string{"HOME": "/home/u", local.EnvStateDir: "/tmp/state", local.EnvCacheDir: "/tmp/cache"}, "/home/u/.local/state/meridian", "/home/u/.cache/meridian"},
		{map[string]string{"HOME": "/home/u", "XDG_STATE_HOME": "/shared/state", "XDG_CACHE_HOME": "/shared/cache"}, "/shared/state/meridian", "/shared/cache/meridian"},
		{map[string]string{"HOME": "/home/u", "XDG_STATE_HOME": "state"}, "/home/u/.local/state/meridian", "/home/u/.cache/meridian"},
	} {
		state, cache, err := schedulerHome(func(name string) string { return tc.env[name] })
		if err != nil || state != tc.state || cache != tc.cache {
			t.Errorf("%v: %s, %s, %v", tc.env, state, cache, err)
		}
	}
	_, _, err := schedulerHome(func(name string) string { return map[string]string{"HOME": "home"}[name] })
	if p := kindtest.Want(t, err, problems.HomeUnknown); p.Params["variable"] != "XDG_STATE_HOME" {
		t.Errorf("the problem's params are %v", p.Params)
	}
	_, _, err = schedulerHome(func(name string) string { return map[string]string{"HOME": "home", "XDG_STATE_HOME": "/s"}[name] })
	if p := kindtest.Want(t, err, problems.HomeUnknown); p.Params["variable"] != "XDG_CACHE_HOME" {
		t.Errorf("the problem's params are %v", p.Params)
	}
}

// TestSchedulerRunDir checks the directory of a run whose job a slurm
// member runs: in the runs' directory in the home the cluster's nodes
// share, named by the run's and the store's IDs, as meridian host names
// one; a child's in its root's, which is made if it isn't there.
func TestSchedulerRunDir(t *testing.T) {
	parallel(t)
	runs := filepath.Join(t.TempDir(), "runs")
	r := &schedulerRuns{dir: runs}
	if _, err := r.runDir(backend.PlaceRequest{Run: "1"}); err == nil {
		t.Error("a run's directory was chosen before the store's ID was known")
	}
	r.store = "s1"
	dir, err := r.runDir(backend.PlaceRequest{Run: "1"})
	if err != nil || dir != filepath.Join(runs, local.RunDirName("s1", "1")) {
		t.Errorf("a root's directory is %s, %v", dir, err)
	}
	if info, err := os.Lstat(runs); err != nil || info.Mode() != fs.ModeDir|0o700 {
		t.Errorf("the runs' directory: %v, %v", info, err)
	}
	dir, err = r.runDir(backend.PlaceRequest{Run: "2", Root: "1", Path: "subworkflows/main/step_0"})
	root := filepath.Join(runs, local.RunDirName("s1", "1"))
	if err != nil || dir != filepath.Join(root, "subworkflows", "main", "step_0") {
		t.Errorf("a child's directory is %s, %v", dir, err)
	}
	if info, err := os.Lstat(root); err != nil || !info.IsDir() {
		t.Errorf("the root's directory: %v", err)
	}
	if _, err := r.runDir(backend.PlaceRequest{Run: "2", Root: "1", Path: "../x"}); err == nil {
		t.Error("a child outside its root's directory was given one")
	}
}

// TestSharedBinary checks the binary a scheduler's job runs: the file
// this process runs, copied into the cache, by its digest, readable by
// the user alone, once, and again where the copy holds another binary; a
// copy other users can change isn't run.
func TestSharedBinary(t *testing.T) {
	parallel(t)
	dir := t.TempDir()
	self := filepath.Join(dir, "self")
	content := []byte("#!/bin/sh\necho meridian\n")
	if err := os.WriteFile(self, content, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	b := &sharedBinary{dir: filepath.Join(dir, "cache", "bin"), self: func() (string, string, string, error) { return self, self, digest, nil }}
	path, got, err := b.get()
	want := filepath.Join(dir, "cache", "bin", hex.EncodeToString(sum[:]), "meridian")
	if err != nil || path != want || got != digest {
		t.Fatalf("the shared binary is %s, %s, %v", path, got, err)
	}
	copied, err := os.Lstat(path)
	if err != nil || copied.Mode() != 0o700 {
		t.Fatalf("the copy: %v, %v", copied, err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != string(content) {
		t.Errorf("the copy holds %q, %v", data, err)
	}
	if _, _, err := b.get(); err != nil {
		t.Fatal(err)
	}
	if again, err := os.Lstat(path); err != nil || !os.SameFile(again, copied) {
		t.Error("the copy was made again")
	}
	// A copy that holds another binary is replaced, by another process's
	// check too.
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho other\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	b2 := &sharedBinary{dir: b.dir, self: b.self}
	if _, _, err := b2.get(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != string(content) {
		t.Errorf("the copy holds %q, %v", data, err)
	}
	if err := os.Chmod(path, 0o777); err != nil {
		t.Fatal(err)
	}
	_, _, err = b.get()
	kindtest.Want(t, err, problems.WorkerBinaryUnsafe)
	// A copy another process linked into place first is checked, not
	// replaced.
	other := filepath.Join(dir, "other")
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "meridian"), content, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := copyBinary(self, other, digest); err != nil {
		t.Errorf("a copy over another's whole copy: %v", err)
	}
	if err := os.WriteFile(filepath.Join(other, "meridian"), []byte("not it"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := copyBinary(self, other, digest); err == nil {
		t.Error("a copy over another binary was taken")
	}
}

// TestSchedulerSettings checks the settings the slurm backend's members
// take, as the groups file is read too: a decoder of its own, slurm's.
func TestSchedulerSettings(t *testing.T) {
	parallel(t)
	if _, err := new(slurm.Scheduler).Settings("m", jsontext.Value(`{"relay":{"network":"10.0.0.0/8"}}`)); err != nil {
		t.Error(err)
	}
}
