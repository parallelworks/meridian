package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// configDir returns a config directory holding files, by name, each
// readable only by the user.
func configDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func load(t *testing.T, files map[string]string, vars map[string]string) *Config {
	t.Helper()
	c, err := LoadConfig(configDir(t, files), lookup(vars))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBuiltInLocalGroup(t *testing.T) {
	c := load(t, nil, nil)
	g, err := c.Group(t.Context(), LocalGroup)
	if err != nil {
		t.Fatal(err)
	}
	want := &backend.Group{Name: "local", Members: []backend.Member{{Name: "host", Backend: "local", Isolation: "host"}}}
	if !reflect.DeepEqual(g, want) {
		t.Errorf("the local group is %+v, want %+v", g, want)
	}
	if got := c.GroupNames(); !reflect.DeepEqual(got, []string{"local"}) {
		t.Errorf("GroupNames = %q, want only local", got)
	}
}

func TestGroupsFile(t *testing.T) {
	c := load(t, map[string]string{GroupsFile: `
gpu:
  members:
    - name: box
      backend: ssh
      settings: {port: 22, host: gpu1.example.com, options: {a: [1, "x"]}}
      isolation: podman
      resources: {cpu: 500m, memory: 1.5Gi, gpu: 2}
      max-walltime: 1d
      margin: 300
      default-timeout: 90s
    - name: cluster
      backend: slurm
      resources: {cpu: 4}
local:
  members:
    - {name: podman, backend: local, isolation: podman}
`}, nil)
	g, err := c.Group(t.Context(), "gpu")
	if err != nil {
		t.Fatal(err)
	}
	want := &backend.Group{Name: "gpu", Members: []backend.Member{
		{
			Name: "box", Backend: "ssh", Isolation: "podman",
			Settings:           []byte(`{"host":"gpu1.example.com","options":{"a":[1,"x"]},"port":22}`),
			Resources:          backend.Resources{MilliCPU: 500, Memory: 3 << 29, GPU: 2},
			MaxWalltimeSeconds: 86400, MarginSeconds: 300, DefaultTimeoutSeconds: 90,
		},
		{Name: "cluster", Backend: "slurm", Resources: backend.Resources{MilliCPU: 4000}},
	}}
	if !reflect.DeepEqual(g, want) {
		t.Errorf("group gpu is\n%+v, want\n%+v", g, want)
	}
	// A group the file defines replaces the built-in one.
	local, err := c.Group(t.Context(), "local")
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Members) != 1 || local.Members[0].Isolation != "podman" {
		t.Errorf("the file's local group is %+v", local)
	}
	// Changing what Group gives changes nothing in the config.
	g.Members[0].Settings[0] = 'x'
	g.Members[1].Name = "changed"
	again, _ := c.Group(t.Context(), "gpu")
	if !reflect.DeepEqual(again, want) {
		t.Errorf("a change to what Group gave reached the config: %+v", again)
	}
}

func TestGroupsFileRefuses(t *testing.T) {
	for name, tc := range map[string]struct {
		text string
		kind *problems.Kind
		line int
	}{
		"unknown key":       {"g:\n  members:\n    - {name: a, backend: local, colour: red}\n", problems.ConfigKeyUnknown, 3},
		"unknown group key": {"g:\n  members: []\n  priority: 1\n", problems.ConfigKeyUnknown, 3},
		"no members":        {"g:\n  members: []\n", problems.GroupEmpty, 1},
		"members not list":  {"g:\n  members: {name: a}\n", problems.ConfigWrongType, 2},
		"member not map":    {"g:\n  members:\n    - a\n", problems.ConfigWrongType, 3},
		"no name":           {"g:\n  members:\n    - {backend: local}\n", problems.MemberInvalid, 3},
		"bad backend":       {"g:\n  members:\n    - {name: a, backend: docker}\n", problems.MemberInvalid, 3},
		"two names":         {"g:\n  members:\n    - {name: a, backend: local}\n    - {name: a, backend: ssh}\n", problems.MemberRepeated, 4},
		"group name":        {"-g:\n  members:\n    - {name: a, backend: local}\n", problems.GroupNameInvalid, 1},
		"isolation":         {"g:\n  members:\n    - {name: a, backend: local, isolation: Pod Man}\n", problems.MemberInvalid, 3},
		"cpu":               {"g:\n  members:\n    - {name: a, backend: local, resources: {cpu: lots}}\n", problems.MemberInvalid, 3},
		"cpu hex":           {"g:\n  members:\n    - {name: a, backend: local, resources: {cpu: 0x10}}\n", problems.MemberInvalid, 3},
		"memory":            {"g:\n  members:\n    - {name: a, backend: local, resources: {memory: 8GB}}\n", problems.MemberInvalid, 3},
		"gpu":               {"g:\n  members:\n    - {name: a, backend: local, resources: {gpu: -1}}\n", problems.MemberInvalid, 3},
		"gpu text":          {"g:\n  members:\n    - {name: a, backend: local, resources: {gpu: one}}\n", problems.ConfigWrongType, 3},
		"walltime":          {"g:\n  members:\n    - {name: a, backend: local, max-walltime: 1.5s}\n", problems.MemberInvalid, 3},
		"walltime in parts": {"g:\n  members:\n    - {name: a, backend: local, max-walltime: 1h30m}\n", problems.MemberInvalid, 3},
		"margin of 0":       {"g:\n  members:\n    - {name: a, backend: local, margin: 0}\n", problems.MemberInvalid, 3},
		"settings":          {"g:\n  members:\n    - {name: a, backend: ssh, settings: [host]}\n", problems.ConfigWrongType, 3},
		"settings nan":      {"g:\n  members:\n    - {name: a, backend: ssh, settings: {weight: .nan}}\n", problems.MemberSettingsNotJSON, 3},
		"settings repeated": {"g:\n  members:\n    - {name: a, backend: ssh, settings: {host: a, host: b}}\n", problems.ConfigYAMLSyntax, 3},
		"repeated group":    {"g:\n  members:\n    - {name: a, backend: local}\ng:\n  members:\n    - {name: b, backend: local}\n", problems.ConfigKeyRepeated, 4},
		"repeated key":      {"g:\n  members:\n    - {name: a, backend: local, name: b}\n", problems.ConfigKeyRepeated, 3},
		"not yaml":          {"g: [", problems.ConfigYAMLSyntax, 0},
		"not a mapping":     {"- g\n", problems.ConfigNotMapping, 1},
		"two documents":     {"g:\n  members:\n    - {name: a, backend: local}\n---\nh: {}\n", problems.ConfigExtraDocument, 4},
	} {
		t.Run(name, func(t *testing.T) {
			dir := configDir(t, map[string]string{GroupsFile: tc.text})
			_, err := LoadConfig(dir, lookup(nil))
			p := kindtest.Want(t, err, tc.kind)
			if p.Params[problems.ParamFile] != filepath.Join(dir, GroupsFile) {
				t.Errorf("the problem's file is %v", p.Params[problems.ParamFile])
			}
			if line, _ := p.Params[problems.ParamLine].(int); line != tc.line {
				t.Errorf("the problem is at line %d, want %d", line, tc.line)
			}
		})
	}
}

func TestSourcesFile(t *testing.T) {
	c := load(t, map[string]string{SourcesFile: "gitlab-servers:\n  - git.example.com\n  - Git.Example.org\n  - git_1\n"}, nil)
	if got, want := c.GitLabServers(), []string{"git.example.com", "git.example.org", "git_1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("GitLabServers = %q, want %q", got, want)
	}
	for text, want := range map[string][]string{"": nil, "gitlab-servers:\n": nil, "gitlab-servers: []\n": nil} {
		if got := load(t, map[string]string{SourcesFile: text}, nil).GitLabServers(); len(got) != len(want) {
			t.Errorf("%q: GitLabServers = %q", text, got)
		}
	}
	for name, tc := range map[string]struct {
		text string
		kind *problems.Kind
		line int
	}{
		"unknown key":   {"github-servers: [a.example.com]\n", problems.ConfigKeyUnknown, 1},
		"not a list":    {"gitlab-servers: a.example.com\n", problems.ConfigWrongType, 1},
		"not text":      {"gitlab-servers:\n  - {host: a}\n", problems.ConfigWrongType, 2},
		"a URL":         {"gitlab-servers:\n  - https://a.example.com\n", problems.GitLabServerInvalid, 2},
		"with userinfo": {"gitlab-servers:\n  - me@a.example.com\n", problems.GitLabServerInvalid, 2},
		"with a path":   {"gitlab-servers:\n  - a.example.com/x\n", problems.GitLabServerInvalid, 2},
		"an option":     {"gitlab-servers:\n  - -a.example.com\n", problems.GitLabServerInvalid, 2},
		// As a $host names a server, which ACTIVATE reads without one.
		"with a port":     {"gitlab-servers:\n  - a.example.com:8443\n", problems.GitLabServerInvalid, 2},
		"repeated key":    {"gitlab-servers: []\ngitlab-servers: []\n", problems.ConfigKeyRepeated, 2},
		"not a mapping":   {"- a.example.com\n", problems.ConfigNotMapping, 1},
		"an empty server": {"gitlab-servers:\n  - ''\n", problems.GitLabServerInvalid, 2},
	} {
		t.Run(name, func(t *testing.T) {
			dir := configDir(t, map[string]string{SourcesFile: tc.text})
			_, err := LoadConfig(dir, lookup(nil))
			p := kindtest.Want(t, err, tc.kind)
			if line, _ := p.Params[problems.ParamLine].(int); line != tc.line || p.Params[problems.ParamFile] != filepath.Join(dir, SourcesFile) {
				t.Errorf("the problem is at %v:%d, want line %d", p.Params[problems.ParamFile], line, tc.line)
			}
		})
	}
}

func TestConfigFilesOthersCanChange(t *testing.T) {
	for _, tc := range []struct {
		file string
		mode os.FileMode
		kind *problems.Kind
	}{
		{GroupsFile, 0o644, nil},
		{GroupsFile, 0o664, problems.FileChangeable},
		{GroupsFile, 0o646, problems.FileChangeable},
		{VariablesFile, 0o600, nil},
		{VariablesFile, 0o640, problems.FileNotPrivate},
		{VariablesFile, 0o604, problems.FileNotPrivate},
		{SourcesFile, 0o644, nil},
		{SourcesFile, 0o664, problems.FileChangeable},
		{ClustersFile, 0o644, nil},
		{ClustersFile, 0o664, problems.FileChangeable},
		{ClustersFile, 0o646, problems.FileChangeable},
	} {
		dir := configDir(t, map[string]string{tc.file: "{}\n"})
		path := filepath.Join(dir, tc.file)
		if err := os.Chmod(path, tc.mode); err != nil {
			t.Fatal(err)
		}
		_, err := LoadConfig(dir, lookup(nil))
		if tc.kind == nil {
			if err != nil {
				t.Errorf("LoadConfig with %s at %04o: %v", tc.file, tc.mode, err)
			}
			continue
		}
		p := kindtest.Want(t, err, tc.kind)
		if p.Params["path"] != path || p.Params["mode"] != fmt.Sprintf("%04o", tc.mode) {
			t.Errorf("the problem's params are %v", p.Params)
		}
	}
}

// TestConfigDirOthersCanChange checks that a config directory others can
// change is refused, since they could replace the files in it.
func TestConfigDirOthersCanChange(t *testing.T) {
	dir := configDir(t, map[string]string{GroupsFile: "{}\n"})
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(dir, lookup(nil))
	kindtest.Want(t, err, problems.PathUnsafe)
}

// TestConfigFileLink checks that a config file isn't read through a
// symbolic link at its path, while the config directory can be a link.
func TestConfigFileLink(t *testing.T) {
	var target string
	for _, name := range []string{GroupsFile, SourcesFile, ClustersFile} {
		target = configDir(t, map[string]string{name: "{}\n"})
		dir := t.TempDir()
		if err := os.Symlink(filepath.Join(target, name), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
		_, err := LoadConfig(dir, lookup(nil))
		kindtest.Want(t, err, problems.FileNotRegular)
	}
	link := filepath.Join(t.TempDir(), "config")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(link, lookup(nil)); err != nil {
		t.Errorf("a config directory that is a link: %v", err)
	}
}

func TestConfigFIFO(t *testing.T) {
	dir := t.TempDir()
	if err := mkfifo(filepath.Join(dir, GroupsFile)); err != nil {
		t.Skipf("can't make a FIFO: %v", err)
	}
	_, err := LoadConfig(dir, lookup(nil))
	kindtest.Want(t, err, problems.FileNotRegular)
}

func TestPlatformGroupsNotFound(t *testing.T) {
	c := load(t, nil, nil)
	if _, err := c.Group(t.Context(), "gpu"); !errors.Is(err, compile.ErrNotFound) {
		t.Errorf("Group of an unknown group: %v", err)
	}
	if _, err := c.Environment(t.Context(), plan.Environment{Cluster: "c", Name: "e"}); !errors.Is(err, compile.ErrNotFound) {
		t.Errorf("Environment: %v", err)
	}
	if _, err := c.ComputeTarget(t.Context(), plan.ComputeTarget{Mode: "login"}); !errors.Is(err, compile.ErrNotFound) {
		t.Errorf("ComputeTarget: %v", err)
	}
}

func TestResolveInput(t *testing.T) {
	c := load(t, map[string]string{GroupsFile: "gpu:\n  members:\n    - {name: a, backend: ssh}\n"}, nil)
	for _, name := range []string{"gpu", "local"} {
		if v, err := c.ResolveInput(t.Context(), "execution-groups", nil, name); err != nil || v != name {
			t.Errorf("ResolveInput(%s) = %v, %v", name, v, err)
		}
	}
	_, err := c.ResolveInput(t.Context(), "execution-groups", nil, "cpu")
	p := kindtest.Want(t, err, problems.ExecutionGroupUnknown)
	if p.Params["value"] != `"cpu"` || p.Params["groups"] != `"gpu", "local"` {
		t.Errorf("ResolveInput of an unknown group: %v, want the groups listed", p.Params)
	}
	_, err = c.ResolveInput(t.Context(), "execution-groups", nil, 3)
	if p := kindtest.Want(t, err, problems.ExecutionGroupUnknown); p.Params["value"] != "3" {
		t.Errorf("ResolveInput of a number: %v", p.Params)
	}
	if _, err := c.ResolveInput(t.Context(), "region", nil, "us-east-1"); !errors.Is(err, compile.ErrInputType) {
		t.Errorf("ResolveInput of a type it doesn't provide: %v", err)
	}
}

// TestResolveInputPassesNamesThrough checks that each input type whose
// value is a name, a scheduler's, a Kubernetes cluster's or an
// organization group's, gives its value as it was given, as format 1
// does: a name, "", or a mapping, as a run passes the value it has to a
// workflow it calls; and refuses any other value as input_not_text, as
// format 1 does.
func TestResolveInputPassesNamesThrough(t *testing.T) {
	c := load(t, nil, nil)
	mapping := map[string]any{"name": "debug"}
	for _, typ := range []string{
		"slurm-partitions", "slurm-accounts", "slurm-qos", "pbs-queues",
		"kubernetes-clusters", "kubernetes-namespaces", "kubernetes-pvc",
		"organization-groups",
	} {
		for _, v := range []any{"debug", "", "gpu,debug", mapping} {
			if got, err := c.ResolveInput(t.Context(), typ, nil, v); err != nil || !reflect.DeepEqual(got, v) {
				t.Errorf("ResolveInput(%s, %v) = %v, %v, want it as it is", typ, v, got, err)
			}
		}
		for v, shown := range map[any]string{int64(3): "3", true: "true"} {
			_, err := c.ResolveInput(t.Context(), typ, nil, v)
			if p := kindtest.Want(t, err, problems.InputNotText); p.Params["value"] != shown {
				t.Errorf("ResolveInput(%s, %v): %v", typ, v, p.Params)
			}
		}
		_, err := c.ResolveInput(t.Context(), typ, nil, []any{"debug"})
		if p := kindtest.Want(t, err, problems.InputNotText); p.Params["value"] != `["debug"]` {
			t.Errorf("ResolveInput(%s) of a list: %v", typ, p.Params)
		}
	}
}

func TestCancelledContext(t *testing.T) {
	c := load(t, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Group(ctx, LocalGroup); err == nil {
		t.Error("Group answered a cancelled context")
	}
	if _, err := c.Read(ctx, []string{"org"}, false); err == nil {
		t.Error("Read answered a cancelled context")
	}
}
