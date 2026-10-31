package local

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

const clustersText = `
hpc:
  host: hpc-login
  scheduler: slurm
lab:
  host: 10.1.2.3
gpu.box:
  host: gpu_1.example.com
  scheduler: pbs
`

func TestClustersFile(t *testing.T) {
	c := load(t, map[string]string{ClustersFile: clustersText}, nil)
	if got, want := c.ClusterNames(), []string{"gpu.box", "hpc", "lab"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ClusterNames = %q, want %q", got, want)
	}
	// Its name as its id, which a session's target reads; no user or
	// namespace, which name the owner's platform account; and, without a
	// scheduler, "", as format 1 writes a cluster's that has none.
	for name, want := range map[string]map[string]any{
		"hpc": {
			"$type": "computeResource", "type": "existing", "provider": "existing", "id": "hpc", "name": "hpc",
			"ip": "hpc-login", "schedulerType": "slurm",
		},
		"lab": {
			"$type": "computeResource", "type": "existing", "provider": "existing", "id": "lab", "name": "lab",
			"ip": "10.1.2.3", "schedulerType": "",
		},
		"gpu.box": {
			"$type": "computeResource", "type": "existing", "provider": "existing", "id": "gpu.box", "name": "gpu.box",
			"ip": "gpu_1.example.com", "schedulerType": "pbs",
		},
	} {
		got, err := c.ResolveInput(t.Context(), "compute-clusters", nil, name)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("ResolveInput(%s) = %v, %v, want %v", name, got, err, want)
		}
	}
	// What ResolveInput gives is the caller's: changing it changes nothing
	// in the config.
	got, _ := c.ResolveInput(t.Context(), "compute-clusters", nil, "hpc")
	got.(map[string]any)["ip"] = "elsewhere"
	again, _ := c.ResolveInput(t.Context(), "compute-clusters", nil, "hpc")
	if ip := again.(map[string]any)["ip"]; ip != "hpc-login" {
		t.Errorf("a change to what ResolveInput gave reached the config: ip %v", ip)
	}
	for _, text := range []string{"", "{}\n", "~\n"} {
		if names := load(t, map[string]string{ClustersFile: text}, nil).ClusterNames(); len(names) != 0 {
			t.Errorf("%q: ClusterNames = %q", text, names)
		}
	}
}

// TestClustersExample checks that the example file in the docs is a
// clusters file that gives what its comments say.
func TestClustersExample(t *testing.T) {
	text, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "docs", "examples", ClustersFile))
	if err != nil {
		t.Fatal(err)
	}
	c := load(t, map[string]string{ClustersFile: string(text)}, nil)
	got, err := c.ResolveInput(t.Context(), "compute-clusters", nil, "hpc")
	want := map[string]any{
		"$type": "computeResource", "type": "existing", "provider": "existing", "id": "hpc", "name": "hpc",
		"ip": "hpc-login", "schedulerType": "slurm",
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("the example's hpc = %v, %v, want %v", got, err, want)
	}
	if names := c.ClusterNames(); !reflect.DeepEqual(names, []string{"hpc", "lab"}) {
		t.Errorf("the example's clusters are %q", names)
	}
}

func TestClustersFileRefuses(t *testing.T) {
	for name, tc := range map[string]struct {
		text string
		kind *problems.Kind
		line int
	}{
		"unknown key":      {"hpc:\n  host: a\n  port: 22\n", problems.ConfigKeyUnknown, 3},
		"a user":           {"hpc:\n  host: a\n  user: alice\n", problems.ConfigKeyUnknown, 3},
		"no host":          {"hpc:\n  scheduler: slurm\n", problems.ClusterInvalid, 1},
		"empty":            {"hpc: {}\n", problems.ClusterInvalid, 1},
		"not a mapping":    {"hpc: login1\n", problems.ConfigWrongType, 1},
		"host not text":    {"hpc:\n  host: [a, b]\n", problems.ConfigWrongType, 2},
		"host null":        {"hpc:\n  host:\n", problems.ConfigWrongType, 2},
		"host empty":       {"hpc:\n  host: ''\n", problems.ClusterInvalid, 2},
		"host option":      {"hpc:\n  host: -oProxyCommand=x\n", problems.ClusterInvalid, 2},
		"host with space":  {"hpc:\n  host: a b\n", problems.ClusterInvalid, 2},
		"host with a user": {"hpc:\n  host: alice@a\n", problems.ClusterInvalid, 2},
		"host a URL":       {"hpc:\n  host: ssh://a\n", problems.ClusterInvalid, 2},
		"host shell":       {"hpc:\n  host: a;b\n", problems.ClusterInvalid, 2},
		// A workflow passes ip to ssh and scp as it is, which take no port in
		// it: a port goes in an ssh_config alias.
		"a port":         {"hpc:\n  host: a:2222\n", problems.ClusterInvalid, 2},
		"bare IPv6":      {"hpc:\n  host: 'fe80::1'\n", problems.ClusterInvalid, 2},
		"scheduler":      {"hpc:\n  host: a\n  scheduler: lsf\n", problems.ClusterInvalid, 3},
		"scheduler case": {"hpc:\n  host: a\n  scheduler: Slurm\n", problems.ClusterInvalid, 3},
		"name":           {"-hpc:\n  host: a\n", problems.ClusterNameInvalid, 1},
		"workspace":      {"workspace:\n  host: a\n", problems.ClusterNameInvalid, 1},
		"user-workspace": {"user-workspace:\n  host: a\n", problems.ClusterNameInvalid, 1},
		// A session's target localhost is the workspace's, as format 1 reads
		// it, and a cluster's id is its name.
		"localhost":     {"localhost:\n  host: a\n", problems.ClusterNameInvalid, 1},
		"repeated name": {"hpc:\n  host: a\nhpc:\n  host: b\n", problems.ConfigKeyRepeated, 3},
		"repeated key":  {"hpc:\n  host: a\n  host: b\n", problems.ConfigKeyRepeated, 3},
		"not yaml":      {"hpc: [", problems.ConfigYAMLSyntax, 0},
		"a list":        {"- hpc\n", problems.ConfigNotMapping, 1},
		"two documents": {"hpc:\n  host: a\n---\nlab: {}\n", problems.ConfigExtraDocument, 3},
	} {
		t.Run(name, func(t *testing.T) {
			dir := configDir(t, map[string]string{ClustersFile: tc.text})
			_, err := LoadConfig(dir, lookup(nil))
			p := kindtest.Want(t, err, tc.kind)
			if p.Params[problems.ParamFile] != filepath.Join(dir, ClustersFile) {
				t.Errorf("the problem's file is %v", p.Params[problems.ParamFile])
			}
			if line, _ := p.Params[problems.ParamLine].(int); line != tc.line {
				t.Errorf("the problem is at line %d, want %d", line, tc.line)
			}
		})
	}
}

// TestClusterInvalidParams checks what a cluster's problem names: the
// cluster, the key, and the value, as Show writes it.
func TestClusterInvalidParams(t *testing.T) {
	_, err := LoadConfig(configDir(t, map[string]string{ClustersFile: "hpc:\n  host: a\n  scheduler: lsf\n"}), lookup(nil))
	p := kindtest.Want(t, err, problems.ClusterInvalid)
	if p.Params["cluster"] != "hpc" || p.Params["key"] != "scheduler" || p.Params["value"] != `"lsf"` {
		t.Errorf("the problem's params are %v", p.Params)
	}
	_, err = LoadConfig(configDir(t, map[string]string{ClustersFile: "hpc:\n  scheduler: slurm\n"}), lookup(nil))
	p = kindtest.Want(t, err, problems.ClusterInvalid)
	if p.Params["key"] != "host" || p.Params["value"] != `""` {
		t.Errorf("a cluster without a host: %v", p.Params)
	}
}

func TestResolveClusters(t *testing.T) {
	c := load(t, map[string]string{ClustersFile: clustersText}, nil)
	workspace := map[string]any{"type": "computeResource", "id": "user-workspace", "name": "user-workspace", "ip": ""}
	// As format 1 resolves a name, whatever the input's include-workspace,
	// provider and csp say: they choose only what a form lists, and a name
	// given otherwise, or a default the form can't list, is resolved all the
	// same.
	for _, definition := range []map[string]any{
		nil,
		{"include-workspace": false},
		{"include-workspace": true},
		{"include-workspace": "${{ inputs.local }}"},
		{"provider": []any{"aws-slurm"}},
		{"provider": []any{}},
		{"csp": "aws"},
	} {
		for _, name := range []string{"workspace", "user-workspace"} {
			got, err := c.ResolveInput(t.Context(), "compute-clusters", definition, name)
			if err != nil || !reflect.DeepEqual(got, workspace) {
				t.Errorf("ResolveInput(%s) with %v = %v, %v", name, definition, got, err)
			}
		}
		got, err := c.ResolveInput(t.Context(), "compute-clusters", definition, "hpc")
		if err != nil || got.(map[string]any)["ip"] != "hpc-login" {
			t.Errorf("ResolveInput(hpc) with %v = %v, %v", definition, got, err)
		}
	}
	// "" stays "", and what isn't a name stays as it is: as the details a
	// run passes on to a workflow it calls, which it resolved already.
	details := map[string]any{"name": "hpc", "ip": "elsewhere"}
	for _, v := range []any{"", details, 3.0, true} {
		if got, err := c.ResolveInput(t.Context(), "compute-clusters", nil, v); err != nil || !reflect.DeepEqual(got, v) {
			t.Errorf("ResolveInput(%v) = %v, %v, want it as it is", v, got, err)
		}
	}
	// A list resolves each of its names, and keeps the rest of it as it is,
	// where format 1 makes each of them null (docs/design.md, Bug fixes to
	// format 1).
	got, err := c.ResolveInput(t.Context(), "compute-clusters", nil, []any{"lab", "", details, "workspace", nil})
	if err != nil {
		t.Fatal(err)
	}
	list := got.([]any)
	if len(list) != 5 || list[0].(map[string]any)["ip"] != "10.1.2.3" || list[1] != "" ||
		!reflect.DeepEqual(list[2], details) || !reflect.DeepEqual(list[3], workspace) || list[4] != nil {
		t.Errorf("ResolveInput of a list = %v", got)
	}
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"unknown", "cpu"},
		{"an unknown name in a list", []any{"hpc", "cpu"}},
		{"a pw URI", "pw://alice/hpc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.ResolveInput(t.Context(), "compute-clusters", nil, tc.value)
			p := kindtest.Want(t, err, problems.ClusterUnknown)
			if p.Params["clusters"] != `"gpu.box", "hpc", "lab"` {
				t.Errorf("the clusters listed are %v", p.Params["clusters"])
			}
		})
	}
}

// TestResolveClustersWithoutFile checks that without a clusters file a
// name is ClustersNone, which names the value, and the workspace is
// resolved all the same, as the local group needs no config either.
func TestResolveClustersWithoutFile(t *testing.T) {
	c := load(t, nil, nil)
	_, err := c.ResolveInput(t.Context(), "compute-clusters", nil, "hpc")
	if p := kindtest.Want(t, err, problems.ClustersNone); p.Params["value"] != `"hpc"` {
		t.Errorf("the problem's params are %v", p.Params)
	}
	if got, err := c.ResolveInput(t.Context(), "compute-clusters", nil, "workspace"); err != nil || got.(map[string]any)["id"] != "user-workspace" {
		t.Errorf("ResolveInput of the workspace = %v, %v", got, err)
	}
}

// TestResolveComputeResources checks that a compute-resources input gives
// what a compute-clusters one does, as format 1 gives a cluster that
// either names the same details: the workspace, a cluster, a list of
// them, and what isn't a name, as it is; and that a name that is no
// cluster here, as a Kubernetes cluster's, which format 1's form lists
// for a compute-resources input too, is the problem compute-clusters
// gives, with the file and without it.
func TestResolveComputeResources(t *testing.T) {
	details := map[string]any{"name": "hpc", "ip": "elsewhere"}
	values := []any{
		"hpc", "lab", "gpu.box", "workspace", "user-workspace", "", "k8s", details,
		[]any{"lab", "", details, "workspace", nil},
		[]any{"hpc", "k8s"},
	}
	for name, files := range map[string]map[string]string{"with the file": {ClustersFile: clustersText}, "without it": nil} {
		c := load(t, files, nil)
		for _, v := range values {
			want, wantErr := c.ResolveInput(t.Context(), "compute-clusters", nil, v)
			got, err := c.ResolveInput(t.Context(), "compute-resources", map[string]any{"include-workspace": false}, v)
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(err, wantErr) {
				t.Errorf("%s: ResolveInput(%v) as compute-resources = %v, %v, want %v, %v as compute-clusters", name, v, got, err, want, wantErr)
			}
		}
	}
	c := load(t, map[string]string{ClustersFile: clustersText}, nil)
	if got, err := c.ResolveInput(t.Context(), "compute-resources", nil, "hpc"); err != nil || got.(map[string]any)["ip"] != "hpc-login" {
		t.Errorf("ResolveInput(hpc) as compute-resources = %v, %v", got, err)
	}
	_, err := c.ResolveInput(t.Context(), "compute-resources", nil, "k8s")
	kindtest.Want(t, err, problems.ClusterUnknown)
	_, err = load(t, nil, nil).ResolveInput(t.Context(), "compute-resources", nil, "k8s")
	kindtest.Want(t, err, problems.ClustersNone)
}

// clusterWorkflow is a format 1 workflow whose job runs on the cluster its
// input names, over SSH, as the workflows we know of write it.
const clusterWorkflow = `
on:
  execute:
    inputs:
      cluster:
        type: compute-clusters
        label: Cluster
        %s
      partition:
        type: string
        default: ${{ inputs.cluster.schedulerType == 'slurm' && 'batch' || 'none' }}
jobs:
  main:
    ssh:
      remoteHost: ${{ inputs.cluster.ip }}
    steps:
      - run: echo ${{ inputs.cluster.name }}
`

// createAndReady creates and starts the run of workflow with submitted, as
// meridian run does with c, and readies its job.
func createAndReady(t *testing.T, c *Config, workflow string, submitted map[string]any) (*compile.Created, *compile.StartResult, *plan.ReadyJob) {
	t.Helper()
	opts := compile.Options{Inputs: c, Variables: c, Groups: c}
	created, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(workflow), submitted, opts)
	if err != nil || diags.HasErrors() {
		t.Fatalf("Create: %v %v", diags, err)
	}
	res, diags, err := compile.Start(t.Context(), created.Plan, compile.StartInput{RunID: "1", Variables: c, Record: created.Record, Inputs: c})
	if err != nil || diags.HasErrors() {
		t.Fatalf("Start: %v %v", diags, err)
	}
	rr, diags, err := created.Plan.Ready(t.Context(), "main", plan.ReadyInput{Run: res.Run})
	if err != nil || diags.HasErrors() || rr.Job == nil {
		t.Fatalf("Ready: %+v %v %v", rr, diags, err)
	}
	return created, res, rr.Job
}

// TestClusterRunsOnItsHost checks a run whose job's ssh: reads the
// cluster's ip, through Create, Start and Ready: the job's target is the
// cluster's host, and what else reads the cluster reads its details.
func TestClusterRunsOnItsHost(t *testing.T) {
	c := load(t, map[string]string{ClustersFile: clustersText}, nil)
	for name, host := range map[string]string{"hpc": "hpc-login", "lab": "10.1.2.3"} {
		_, res, rj := createAndReady(t, c, strings.Replace(clusterWorkflow, "%s", "", 1), map[string]any{"cluster": name})
		inline := rj.Target.Inline
		if inline == nil || inline.Backend != "ssh" {
			t.Fatalf("%s: the job's target is %+v, want an inline ssh group", name, rj.Target)
		}
		if ssh, _ := inline.SSH.Get().(map[string]any); ssh["remoteHost"] != host {
			t.Errorf("%s: the job's ssh: is %v, want remoteHost %s", name, ssh, host)
		}
		inputs, _ := res.Run.Inputs.Get().(map[string]any)
		partition := map[string]string{"hpc": "batch", "lab": "none"}[name]
		if inputs["partition"] != partition {
			t.Errorf("%s: the default that reads the cluster's schedulerType gave %v, want %s", name, inputs["partition"], partition)
		}
		if slices.Contains(rj.PrivateFields, "target") {
			t.Errorf("%s: a target made from a name the person gave is private", name)
		}
	}
}

// TestClusterFromVariableIsPrivate checks that a cluster that a variable
// names is private, as what is made from a variable is: the plan doesn't
// hold its host, the run's values keep the input private, and Ready marks
// the job's target private, which makes its placement private
// (coordinator.placementPrivate), whose problems are masked.
func TestClusterFromVariableIsPrivate(t *testing.T) {
	c := load(t, map[string]string{ClustersFile: clustersText, VariablesFile: "vars:\n  cluster: hpc\n"}, nil)
	workflow := strings.Replace(clusterWorkflow, "%s", "default: ${{ vars.cluster }}", 1)
	created, res, rj := createAndReady(t, c, workflow, map[string]any{})
	encoded, err := json.Marshal(created.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "hpc-login") {
		t.Error("the plan holds the cluster's host")
	}
	private := false
	for _, path := range res.Run.PrivatePaths() {
		private = private || slices.Equal(path, []string{"inputs", "cluster"})
	}
	if !private {
		t.Errorf("the run's private values are %v, without inputs.cluster", res.Run.PrivatePaths())
	}
	if !slices.Contains(rj.PrivateFields, "target") {
		t.Errorf("the job's private fields are %v, without its target", rj.PrivateFields)
	}
	if ssh, _ := rj.Target.Inline.SSH.Get().(map[string]any); ssh["remoteHost"] != "hpc-login" {
		t.Errorf("the job's ssh: is %v", ssh)
	}
}

// TestClusterUnknownAtCreate checks that the compiler reports a name the
// clusters file doesn't have at the input, with the input's name.
func TestClusterUnknownAtCreate(t *testing.T) {
	c := load(t, map[string]string{ClustersFile: clustersText}, nil)
	_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(strings.Replace(clusterWorkflow, "%s", "", 1)),
		map[string]any{"cluster": "cpu"}, compile.Options{Inputs: c, Variables: c, Groups: c})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, d := range diags {
		if d.Code == problems.ClusterUnknown.Code() {
			found = true
			if d.Params["input"] == nil || d.Params["clusters"] != `"gpu.box", "hpc", "lab"` {
				t.Errorf("the problem's params are %v", d.Params)
			}
		}
	}
	if !found {
		t.Errorf("Create of an unknown cluster: %v, want %s", diags, problems.ClusterUnknown.Code())
	}
}
