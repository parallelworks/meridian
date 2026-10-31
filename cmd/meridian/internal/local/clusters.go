package local

import (
	"maps"
	"regexp"
	"slices"
	"strings"

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
// compute-resources one, whose definition is def, as format 1 resolves
// one: a cluster's pw:// URI (resolveCluster), or with multi true, each of
// a list of them, of the shape the bug fix to format 1 checks
// (resolveEach), becomes the cluster's details, whatever the input's
// include-workspace, provider and csp say, as they only choose what a form
// lists. "" and null are no value, and are kept. A called workflow's
// input, called, also takes a cluster's details, a mapping, as its caller
// passes those it has, which are kept as they are; a run's own takes only
// the URI, as a platform's form gives it, so a mapping there, or any other
// value of a list, is problems.ClusterURIRequired.
func (c *Config) resolveClusters(def map[string]any, value any, called bool) (any, error) {
	return resolveEach(value, def["multi"] == true, func(v any) (any, error) {
		switch v := v.(type) {
		case nil:
			return nil, nil
		case string:
			if v == "" {
				return v, nil
			}
			return c.resolveCluster(v)
		case map[string]any:
			if called {
				return v, nil
			}
			name, _ := v["name"].(string)
			return nil, c.uriRequired(v, name)
		}
		return nil, c.uriRequired(v, "")
	})
}

// resolveCluster gives the details of the cluster ref names, by its URI:
// pw://, the login name of the user who runs meridian, a slash and a
// cluster's name in the clusters file, as pw://alice/hpc. The workspace,
// this machine, is pw://<login>/user-workspace, and keeps the reserved
// names format 1 resolves it by (isWorkspace). A name alone, and
// pw://name, which a platform reads as an organization's cluster, which a
// run here has none of, are problems.ClusterURIRequired, which gives the
// URI to name the cluster by.
// Another user's URI, or one of no cluster, is problems.ClusterUnknown,
// which lists the clusters' URIs, or problems.ClustersNone when the file
// has none.
func (c *Config) resolveCluster(ref string) (any, error) {
	login, err := c.loginName()
	if err != nil {
		return nil, err
	}
	rest, isURI := strings.CutPrefix(ref, pwPrefix)
	owner, name, userScoped := strings.Cut(rest, "/")
	switch {
	case isWorkspace(ref), isURI && !userScoped && isWorkspace(rest), isURI && owner == login && isWorkspace(name):
		return workspaceValue(login), nil
	case !isURI && groupNamePattern.MatchString(ref):
		return nil, c.uriRequired(ref, ref)
	case isURI && !userScoped && groupNamePattern.MatchString(rest):
		return nil, c.uriRequired(ref, rest)
	case isURI && owner == login:
		if cl, ok := c.clusters[name]; ok {
			return cl.value(login, name), nil
		}
	}
	if len(c.clusters) == 0 {
		return nil, problems.ClustersNone.InputProblem(problems.Params{"value": problems.Show(ref)})
	}
	uris := make([]string, 0, len(c.clusters))
	for _, name := range c.ClusterNames() {
		uris = append(uris, clusterURI(login, name))
	}
	return nil, problems.ClusterUnknown.InputProblem(problems.Params{"value": problems.Show(ref), "clusters": problems.QuoteAll(uris)})
}

// isWorkspace reports whether name is one of the workspace's reserved
// names, workspace and user-workspace, by which format 1 resolves the
// workspace alone, as pw://name, and as pw://<user>/name, where here
// <user> is the login name.
func isWorkspace(name string) bool { return name == workspaceName || name == workspaceShortName }

// uriRequired is problems.ClusterURIRequired of v, a value that names a
// cluster otherwise than by its URI, with the URI of the cluster named
// name, the workspace's for its reserved names, or a URI with <name> in
// the name's place, for none.
func (c *Config) uriRequired(v any, name string) error {
	login, err := c.loginName()
	if err != nil {
		return err
	}
	switch name {
	case workspaceShortName:
		name = workspaceName
	case "":
		name = "<name>"
	}
	return problems.ClusterURIRequired.InputProblem(problems.Params{"value": problems.Show(v), "uri": clusterURI(login, name)})
}

// clusterURI returns the URI of the cluster named name, of the user whose
// login name is login.
func clusterURI(login, name string) string { return pwPrefix + login + "/" + name }

// loginName returns the login name of the user who runs meridian, which
// names their clusters' URIs; or, for a user who has none,
// problems.ClusterLoginUnknown, which says why, as no later try changes
// it.
func (c *Config) loginName() (string, error) {
	if c.user.Err != nil {
		return "", problems.ClusterLoginUnknown.InputProblem(problems.Params{"reason": c.user.Err.Error()})
	}
	return c.user.Name, nil
}

// value returns the details of the cluster named name, of the user whose
// login name is login, as format 1 gives an existing cluster's, for what a
// cluster of the clusters file can know of them: its name, which is also
// its id, as the workspace's is, and which a session's target reads
// (parallelworks/update-session's target: ${{ inputs.<cluster>.id }}); its
// host as its ip, which a job's ssh: reads as its remoteHost; its
// scheduler as its schedulerType, "" for none, as format 1 writes a
// cluster's that has none; existing as its type and its provider, as
// format 1 names a cluster that a platform reaches but didn't make; and
// its URI, and the login name as its user and its namespace, as format 1
// gives the owner's account on the platform.
func (cl cluster) value(login, name string) map[string]any {
	return map[string]any{
		"$type":         "computeResource",
		"type":          "existing",
		"provider":      "existing",
		"id":            name,
		"name":          name,
		"ip":            cl.host,
		"schedulerType": cl.scheduler,
		"uri":           clusterURI(login, name),
		"user":          login,
		"namespace":     login,
	}
}

// workspaceValue returns the details of the workspace, of the user whose
// login name is login, as format 1 gives them: the machine the run starts
// on, this one, whose ip is "", with which format 1 runs a job whose ssh:
// reads it as its remoteHost there, without SSH; and its URI, and the
// login name as its user and its namespace.
func workspaceValue(login string) map[string]any {
	return map[string]any{
		"type": "computeResource", "id": workspaceName, "name": workspaceName, "ip": "",
		"uri": clusterURI(login, workspaceName), "user": login, "namespace": login,
	}
}
