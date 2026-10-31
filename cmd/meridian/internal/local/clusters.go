package local

import (
	"maps"
	"regexp"
	"slices"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
)

// A cluster is one of the clusters file's: the SSH host a job whose ssh:
// reads the cluster's ip reaches it at, and, optionally, its scheduler.
type cluster struct {
	host, scheduler string
}

// clusterKeys are the keys of a cluster in the clusters file.
var clusterKeys = []string{"host", "scheduler"}

// schedulers are the schedulers a cluster can name, as format 1 writes a
// cluster's schedulerType.
var schedulers = []string{"slurm", "pbs"}

// clusterHostPattern is a cluster's host: an alias that ssh resolves, as
// ssh -G does, or a host name or an address, as format 1 gives an existing
// cluster's ip, which workflows pass to ssh and scp as it is. So it has no
// port, and can't start with -, which ssh would read as an option, nor hold
// space or a character a shell would read, as a workflow's script can put
// it in a command line. A host that needs a port, a user to log in as, a
// jump host or an IPv6 address is an alias the user's ssh_config defines.
var clusterHostPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`)

// The names under which format 1 resolves the workspace, whatever the
// input: the machine its runs start on, which is this one, where a run
// starts.
const (
	workspaceName      = "user-workspace"
	workspaceShortName = "workspace"
)

// localhostName is the name no cluster can have besides the workspace's:
// a cluster's name is its id (cluster.value), and format 1's
// update-session reads the target localhost as the workspace, as it reads
// user-workspace.
const localhostName = "localhost"

// loadClusters reads the clusters file at path: each cluster by its name,
// with its host, and its scheduler, if it gives one. Other users may read
// it, as they may the groups file, which names hosts too, but not change
// it, since that would send the user's jobs to a host of their choosing.
func (c *Config) loadClusters(path string) error {
	data, err := readConfig(path, safepath.Unchangeable)
	if err != nil || data == nil {
		return err
	}
	f := configFile{path}
	root, err := f.parse(data)
	if err != nil || root == nil {
		return err
	}
	clusters, err := f.pairs(root, nil)
	if err != nil {
		return err
	}
	for _, p := range clusters {
		name := p.key.Value
		if !groupNamePattern.MatchString(name) || name == workspaceName || name == workspaceShortName || name == localhostName {
			return f.problem(problems.ClusterNameInvalid, p.key, problems.Params{"cluster": name})
		}
		cl, err := f.cluster(name, p.key, p.value)
		if err != nil {
			return err
		}
		c.clusters[name] = cl
	}
	return nil
}

// cluster reads the cluster named name, whose key is key and which n
// holds.
func (f configFile) cluster(name string, key, n *yaml.Node) (cluster, error) {
	var cl cluster
	if err := f.mapping(name, n); err != nil {
		return cl, err
	}
	pairs, err := f.pairs(n, clusterKeys)
	if err != nil {
		return cl, err
	}
	invalid := func(at *yaml.Node, key, value string) error {
		return f.problem(problems.ClusterInvalid, at, problems.Params{"cluster": name, "key": key, "value": problems.Show(value)})
	}
	for _, p := range pairs {
		k, v := p.key.Value, p.value
		text, err := f.text(k, v)
		if err != nil {
			return cl, err
		}
		var ok bool
		if k == "host" {
			cl.host, ok = text, clusterHostPattern.MatchString(text)
		} else {
			cl.scheduler, ok = text, slices.Contains(schedulers, text)
		}
		if !ok {
			return cl, invalid(v, k, text)
		}
	}
	if cl.host == "" {
		return cl, invalid(key, "host", "")
	}
	return cl, nil
}

// ClusterNames returns the names of the clusters file's clusters, in
// order.
func (c *Config) ClusterNames() []string { return slices.Sorted(maps.Keys(c.clusters)) }

// ClusterHost returns the host of the clusters file's cluster named name,
// which is its id too, and whether the file has it.
func (c *Config) ClusterHost(name string) (string, bool) {
	cl, ok := c.clusters[name]
	return cl.host, ok
}

// resolveClusters gives the value of a compute-clusters input, or a
// compute-resources one, as format 1 resolves one: the workspace's name,
// or a cluster's of the clusters file, or each of a list of them, becomes
// its details, whatever the input's definition says, as its
// include-workspace, provider and csp only choose what a form lists. "",
// and any other value, as the details a run passes to a workflow it
// calls, are kept as they are, in a list too, since only a name is
// resolved. Another name is problems.ClusterUnknown, which lists the
// clusters, or problems.ClustersNone when the file has none.
func (c *Config) resolveClusters(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return c.resolveCluster(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			name, ok := item.(string)
			if !ok {
				out[i] = item
				continue
			}
			r, err := c.resolveCluster(name)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	}
	return value, nil
}

// resolveCluster gives the details of the cluster name names, or "" for "".
func (c *Config) resolveCluster(name string) (any, error) {
	switch cl, ok := c.clusters[name]; {
	case name == "":
		return name, nil
	case name == workspaceName || name == workspaceShortName:
		return workspaceValue(), nil
	case ok:
		return cl.value(name), nil
	case len(c.clusters) == 0:
		return nil, problems.ClustersNone.InputProblem(problems.Params{"value": problems.Show(name)})
	}
	return nil, problems.ClusterUnknown.InputProblem(problems.Params{"value": problems.Show(name), "clusters": problems.QuoteAll(c.ClusterNames())})
}

// value returns the details of the cluster named name, as format 1 gives
// an existing cluster's, for what a cluster of the clusters file can know
// of them: its name, which is also its id, as the workspace's is, and
// which a session's target reads (parallelworks/update-session's target:
// ${{ inputs.<cluster>.id }}); its host as its ip, which a job's ssh: reads
// as its remoteHost; its scheduler as its schedulerType, "" for none, as
// format 1 writes a cluster's that has none; and existing as its type and
// its provider, as format 1 names a cluster that a platform reaches but
// didn't make. What names a platform's own record of it, its pw:// uri, and
// its owner's platform account, its user and its namespace, it has none of,
// and leaves out.
func (cl cluster) value(name string) map[string]any {
	return map[string]any{
		"$type":         "computeResource",
		"type":          "existing",
		"provider":      "existing",
		"id":            name,
		"name":          name,
		"ip":            cl.host,
		"schedulerType": cl.scheduler,
	}
}

// workspaceValue returns the details of the workspace, as format 1 gives
// them: the machine the run starts on, this one, whose ip is "", with
// which format 1 runs a job whose ssh: reads it as its remoteHost there,
// without SSH. It leaves out the user, the namespace and the uri, which
// name the person's account on a platform, which a run here has none of.
func workspaceValue() map[string]any {
	return map[string]any{"type": "computeResource", "id": workspaceName, "name": workspaceName, "ip": ""}
}
