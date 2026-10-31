package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// The files in the config directory.
const (
	GroupsFile    = "groups.yaml"
	VariablesFile = "variables.yaml"
	SourcesFile   = "sources.yaml"
	ClustersFile  = "clusters.yaml"
)

// maxConfigSize is the largest config file, or file holding a variable's
// value, that Config reads.
const maxConfigSize = 1 << 20

// LocalGroup is the name of the built-in group, which needs no config: one
// member, this machine, whose jobs run on the host as the user, with no
// container, as its isolation says. A groups file can define a group of
// the same name in its place, such as one whose member runs jobs in
// rootless containers.
const LocalGroup = "local"

// localGroup is the built-in local group.
func localGroup() *backend.Group {
	return &backend.Group{Name: LocalGroup, Members: []backend.Member{{
		Name:    "host",
		Backend: "local",
		// Named rather than left to the local backend's default, a rootless
		// container, so that the built-in group says what it does.
		Isolation: "host",
	}}}
}

// Config is the user's config: their execution groups, variables, sources
// and clusters, from the config directory. It provides the Variables and
// Groups the compiler and the coordinator read, the input types that name
// what it holds, and what Sources reaches.
type Config struct {
	groups    map[string]*backend.Group
	variables map[string]variable
	servers   []string
	workflows Workflows
	clusters  map[string]cluster
	lookupEnv func(string) (string, bool)
}

var (
	_ compile.Variables     = (*Config)(nil)
	_ compile.Groups        = (*Config)(nil)
	_ compile.InputResolver = (*Config)(nil)
)

// LoadConfig reads the groups file, the variables file, the sources file
// and the clusters file in dir, any of which can be missing, and checks
// them. lookupEnv reads the environment variables that hold variables'
// values, when they are read, and HOME, for a path that starts with ~/, and
// says whether each is set, as os.LookupEnv does: one set to "" gives "".
//
// The directory, and every directory on the way to it, must belong to the
// user or to root, and only they may change it. None of the files may be
// changeable by another user, who could otherwise send the user's jobs to
// a host of their choosing, give them values, or have Sources reach a
// server of their choosing with the user's credentials. The variables
// file must be readable only by the user, since people keep secrets in
// plain variables too. None is read through a symbolic link: the
// directory can be one, and is checked through it (safepath.ReadFile).
func LoadConfig(dir string, lookupEnv func(string) (string, bool)) (*Config, error) {
	c := &Config{lookupEnv: lookupEnv, groups: map[string]*backend.Group{}, variables: map[string]variable{}, clusters: map[string]cluster{}}
	// A directory that isn't there holds no config, and nothing on the
	// way to it is read.
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		if err := safepath.Check(dir, true); err != nil {
			return nil, err
		}
		if err := c.loadGroups(filepath.Join(dir, GroupsFile)); err != nil {
			return nil, err
		}
		if err := c.loadVariables(dir, filepath.Join(dir, VariablesFile)); err != nil {
			return nil, err
		}
		if err := c.loadSources(filepath.Join(dir, SourcesFile)); err != nil {
			return nil, err
		}
		if err := c.loadClusters(filepath.Join(dir, ClustersFile)); err != nil {
			return nil, err
		}
	}
	if _, ok := c.groups[LocalGroup]; !ok {
		c.groups[LocalGroup] = localGroup()
	}
	return c, nil
}

// readConfig reads the config file at path, which other users may use no
// more than access allows (safepath.ReadFile), or returns nil when there
// is none.
func readConfig(path string, access safepath.Access) ([]byte, error) {
	data, err := safepath.ReadFile(path, access, maxConfigSize)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, unreadable(path, err)
	}
	return data, nil
}

// unreadable returns err, from opening the file at path, as a problem:
// itself when it is one, and FileUnreadable, with what the operating
// system said, otherwise.
func unreadable(path string, err error) error {
	if problems.IsProblem(err) {
		return err
	}
	return problems.FileUnreadable.Problem(problems.Params{"path": path, "reason": err.Error()})
}

// A configFile is a config file as it is read: its path, for the problems
// it has.
type configFile struct {
	path string
}

// problem returns a problem of kind k with params, at n, or at the file
// when n is nil.
func (f configFile) problem(k *problems.Kind, n *yaml.Node, params problems.Params) error {
	if params == nil {
		params = problems.Params{}
	}
	params[problems.ParamFile] = f.path
	if n != nil && n.Line > 0 {
		params[problems.ParamLine] = n.Line
		params[problems.ParamColumn] = n.Column
	}
	return k.Problem(params)
}

// parse reads data, one YAML document, and returns the mapping it is, or
// nil for an empty one.
func (f configFile) parse(data []byte) (*yaml.Node, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		return nil, f.problem(problems.ConfigYAMLSyntax, nil, problems.Params{"reason": err.Error()})
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, f.problem(problems.ConfigExtraDocument, &extra, nil)
	}
	if len(doc.Content) == 0 {
		return nil, nil
	}
	root := deref(doc.Content[0])
	switch {
	case root.Kind == yaml.ScalarNode && root.Tag == "!!null":
		return nil, nil
	case root.Kind != yaml.MappingNode:
		return nil, f.problem(problems.ConfigNotMapping, root, nil)
	}
	return root, nil
}

// deref follows n, if it is an alias, to what it names.
func deref(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	return n
}

// A pair is a key of a mapping and its value.
type pair struct {
	key, value *yaml.Node
}

// pairs returns the keys and values of n, a mapping, refusing a key given
// twice, and, when allowed isn't nil, a key it doesn't list.
func (f configFile) pairs(n *yaml.Node, allowed []string) ([]pair, error) {
	seen := map[string]*yaml.Node{}
	var out []pair
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], deref(n.Content[i+1])
		if first, ok := seen[k.Value]; ok {
			return nil, f.problem(problems.ConfigKeyRepeated, k, problems.Params{"key": k.Value, "first_line": first.Line})
		}
		seen[k.Value] = k
		if allowed != nil && !slices.Contains(allowed, k.Value) {
			return nil, f.problem(problems.ConfigKeyUnknown, k, problems.Params{"key": k.Value, "keys": problems.QuoteAll(allowed)})
		}
		out = append(out, pair{k, v})
	}
	return out, nil
}

// mapping checks that n, key's value, is a mapping.
func (f configFile) mapping(key string, n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return f.problem(problems.ConfigWrongType, n, problems.Params{"key": key, "want": "mapping"})
	}
	return nil
}

// text returns n, key's value, which must be text: a scalar, which YAML
// may read as a number or a boolean, as it is written.
func (f configFile) text(key string, n *yaml.Node) (string, error) {
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" {
		return "", f.problem(problems.ConfigWrongType, n, problems.Params{"key": key, "want": "text"})
	}
	return n.Value, nil
}

// The name of a group, a member or a variable can be letters, digits, _
// and -, and . too in a group's or a member's, but never first.
var (
	groupNamePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	variableNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
	isolationPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
)

// backends are the backends a member can name.
var backends = []string{"local", "ssh", "kubernetes", "slurm", "pbs"}

// The keys a group, a member and its resources have.
var (
	groupKeys     = []string{"members"}
	memberKeys    = []string{"name", "backend", "settings", "isolation", "resources", "max-walltime", "margin", "default-timeout"}
	resourcesKeys = []string{"cpu", "memory", "gpu"}
)

// loadGroups reads the groups file at path: each group by its name, with
// its members in order.
func (c *Config) loadGroups(path string) error {
	data, err := readConfig(path, safepath.Unchangeable)
	if err != nil || data == nil {
		return err
	}
	f := configFile{path}
	root, err := f.parse(data)
	if err != nil || root == nil {
		return err
	}
	groups, err := f.pairs(root, nil)
	if err != nil {
		return err
	}
	for _, g := range groups {
		group, err := f.group(g.key, g.value)
		if err != nil {
			return err
		}
		c.groups[group.Name] = group
	}
	return nil
}

// group reads the group key names, which n holds.
func (f configFile) group(key, n *yaml.Node) (*backend.Group, error) {
	name := key.Value
	if !groupNamePattern.MatchString(name) {
		return nil, f.problem(problems.GroupNameInvalid, key, problems.Params{"group": name})
	}
	if err := f.mapping(name, n); err != nil {
		return nil, err
	}
	pairs, err := f.pairs(n, groupKeys)
	if err != nil {
		return nil, err
	}
	g := &backend.Group{Name: name}
	var members *yaml.Node
	for _, p := range pairs {
		members = p.value
	}
	if members == nil || members.Kind == yaml.SequenceNode && len(members.Content) == 0 {
		return nil, f.problem(problems.GroupEmpty, key, problems.Params{"group": name})
	}
	if members.Kind != yaml.SequenceNode {
		return nil, f.problem(problems.ConfigWrongType, members, problems.Params{"key": "members", "want": "list"})
	}
	seen := map[string]bool{}
	for i, mn := range members.Content {
		m, err := f.member(name, i, deref(mn))
		if err != nil {
			return nil, err
		}
		if seen[m.Name] {
			return nil, f.problem(problems.MemberRepeated, mn, problems.Params{"group": name, "member": m.Name})
		}
		seen[m.Name] = true
		g.Members = append(g.Members, m)
	}
	return g, nil
}

// member reads the member at index i of the group named group, which n
// holds, and returns it as the coordinator reads it.
func (f configFile) member(group string, i int, n *yaml.Node) (backend.Member, error) {
	var m backend.Member
	if err := f.mapping(fmt.Sprintf("members[%d]", i+1), n); err != nil {
		return m, err
	}
	pairs, err := f.pairs(n, memberKeys)
	if err != nil {
		return m, err
	}
	invalid := func(at *yaml.Node, key, value string) error {
		return f.problem(problems.MemberInvalid, at, problems.Params{"group": group, "key": key, "value": problems.Show(value)})
	}
	for _, p := range pairs {
		key, v := p.key.Value, p.value
		switch key {
		case "settings":
			if err := f.mapping(key, v); err != nil {
				return m, err
			}
			var settings map[string]any
			if err := v.Decode(&settings); err != nil {
				return m, f.problem(problems.ConfigYAMLSyntax, v, problems.Params{"reason": err.Error()})
			}
			if m.Settings, err = settingsJSON(settings); err != nil {
				return m, f.problem(problems.MemberSettingsNotJSON, v, problems.Params{"group": group})
			}
		case "resources":
			if err := f.resources(group, v, &m.Resources); err != nil {
				return m, err
			}
		default:
			text, err := f.text(key, v)
			if err != nil {
				return m, err
			}
			switch key {
			case "name":
				m.Name = text
			case "backend":
				m.Backend = text
			case "isolation":
				m.Isolation = text
			default:
				seconds, ok := parseSeconds(text)
				if !ok {
					return m, invalid(v, key, text)
				}
				switch key {
				case "max-walltime":
					m.MaxWalltimeSeconds = seconds
				case "margin":
					m.MarginSeconds = seconds
				default:
					m.DefaultTimeoutSeconds = seconds
				}
			}
			switch {
			case key == "name" && !groupNamePattern.MatchString(text),
				key == "backend" && !slices.Contains(backends, text),
				key == "isolation" && !isolationPattern.MatchString(text):
				return m, invalid(v, key, text)
			}
		}
	}
	switch {
	case m.Name == "":
		return m, invalid(n, "name", "")
	case m.Backend == "":
		return m, invalid(n, "backend", "")
	}
	return m, nil
}

// resources reads a member's resources, which n holds, into r.
func (f configFile) resources(group string, n *yaml.Node, r *backend.Resources) error {
	if err := f.mapping("resources", n); err != nil {
		return err
	}
	pairs, err := f.pairs(n, resourcesKeys)
	if err != nil {
		return err
	}
	for _, p := range pairs {
		key, v := p.key.Value, p.value
		text, err := f.text(key, v)
		if err != nil {
			return err
		}
		invalid := f.problem(problems.MemberInvalid, v, problems.Params{"group": group, "key": key, "value": problems.Show(text)})
		var ok bool
		switch key {
		case "cpu":
			if r.MilliCPU, ok = parseCPU(text); !ok {
				return invalid
			}
		case "memory":
			if r.Memory, ok = parseMemory(text); !ok {
				return invalid
			}
		default:
			if v.Tag != "!!int" {
				return f.problem(problems.ConfigWrongType, v, problems.Params{"key": key, "want": "number"})
			}
			if r.GPU, ok = parseGPU(text); !ok {
				return invalid
			}
		}
	}
	return nil
}

// sourcesKeys are the keys of the sources file, and mappingKeys those of a
// mapping of its workflows.
var (
	sourcesKeys = []string{"gitlab-servers", "workflows"}
	mappingKeys = []string{"uses", "yaml", "host"}
)

// loadSources reads the sources file at path: the GitLab servers, besides
// gitlab.com, that a workflow's $host can name, each a host name with no
// port, as a $host names it (validHost); and the workflows a platform
// stores that the user maps to workflows in Git (Workflows).
func (c *Config) loadSources(path string) error {
	data, err := readConfig(path, safepath.Unchangeable)
	if err != nil || data == nil {
		return err
	}
	f := configFile{path}
	root, err := f.parse(data)
	if err != nil || root == nil {
		return err
	}
	keys, err := f.pairs(root, sourcesKeys)
	if err != nil {
		return err
	}
	// The workflows are read once the servers are, which their mappings
	// to GitLab can name.
	var mappings *yaml.Node
	for _, kv := range keys {
		key, list := kv.key.Value, kv.value
		if list.Kind == yaml.ScalarNode && list.Tag == "!!null" {
			continue
		}
		if key == "workflows" {
			mappings = list
			continue
		}
		if list.Kind != yaml.SequenceNode {
			return f.problem(problems.ConfigWrongType, list, problems.Params{"key": key, "want": "list"})
		}
		for _, n := range list.Content {
			n = deref(n)
			server, err := f.text(key, n)
			if err != nil {
				return err
			}
			if !validHost(server) {
				return f.problem(problems.GitLabServerInvalid, n, problems.Params{"server": server})
			}
			c.servers = append(c.servers, strings.ToLower(server))
		}
	}
	if mappings != nil {
		c.workflows, err = f.workflows(mappings, c.servers)
	}
	return err
}

// workflows reads the sources file's workflows, which n holds: each key,
// which names a workflow a platform stores (MappingKey), maps it to a
// workflow in Git, its uses, yaml and host, as a step names one, on
// gitlab.com or one of servers (MappingInvalid). Two keys that name the
// same workflow, as Normal reads them, are refused, as two of one key are.
func (f configFile) workflows(n *yaml.Node, servers []string) (Workflows, error) {
	if err := f.mapping("workflows", n); err != nil {
		return nil, err
	}
	pairs, err := f.pairs(n, nil)
	if err != nil {
		return nil, err
	}
	out := Workflows{}
	firsts := map[string]*yaml.Node{}
	for _, p := range pairs {
		key := p.key.Value
		normal, slug, ok := MappingKey(key)
		if !ok {
			return nil, f.problem(problems.WorkflowMappingKeyInvalid, p.key, problems.Params{"key": key})
		}
		if first, ok := firsts[normal]; ok {
			return nil, f.problem(problems.WorkflowMappingRepeated, p.key, problems.Params{"key": key, "first": first.Value, "first_line": first.Line})
		}
		firsts[normal] = p.key
		if err := f.mapping(key, p.value); err != nil {
			return nil, err
		}
		fields, err := f.pairs(p.value, mappingKeys)
		if err != nil {
			return nil, err
		}
		var to plan.SourceRef
		for _, field := range fields {
			text, err := f.text(field.key.Value, field.value)
			if err != nil {
				return nil, err
			}
			switch field.key.Value {
			case "uses":
				to.Uses = text
			case "yaml":
				to.Path = text
			default:
				to.Host = text
			}
		}
		if what := MappingInvalid(to, slug, servers); what != "" {
			return nil, f.problem(problems.WorkflowMappingInvalid, p.value, problems.Params{"key": key, "what": what})
		}
		out[normal] = to
	}
	return out, nil
}

// GitLabServers returns the GitLab servers, besides gitlab.com, that the
// sources file registers, which a workflow's $host can name (NewSources).
func (c *Config) GitLabServers() []string { return slices.Clone(c.servers) }

// Workflows returns the workflows a platform stores that the sources file
// maps to workflows in Git (NewSources).
func (c *Config) Workflows() Workflows { return maps.Clone(c.workflows) }

// GroupNames returns the names of the execution groups, in order.
func (c *Config) GroupNames() []string { return slices.Sorted(maps.Keys(c.groups)) }

// Group returns the execution group named name, or compile.ErrNotFound.
func (c *Config) Group(ctx context.Context, name string) (*backend.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g, ok := c.groups[name]
	if !ok {
		return nil, compile.ErrNotFound
	}
	out := &backend.Group{Name: g.Name, Members: slices.Clone(g.Members)}
	for i := range out.Members {
		out.Members[i].Settings = slices.Clone(out.Members[i].Settings)
	}
	return out, nil
}

// Environment returns compile.ErrNotFound: only a platform knows its
// scheduler environments.
func (c *Config) Environment(ctx context.Context, _ plan.Environment) (*backend.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, compile.ErrNotFound
}

// ComputeTarget returns compile.ErrNotFound: only a platform knows its
// compute targets.
func (c *Config) ComputeTarget(ctx context.Context, _ plan.ComputeTarget) (*backend.Group, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, compile.ErrNotFound
}

// ResolveInput provides these input types, and no other: the other types
// that name a platform's resources, as its regions and its other
// Kubernetes objects, come later.
//
//   - execution-groups, whose value is the name of one of the config's
//     groups. A value that names none of them is
//     problems.ExecutionGroupUnknown.
//   - compute-clusters, whose value names one of the clusters of the
//     clusters file, or the workspace, and gives its details
//     (resolveClusters).
//   - compute-resources, which format 1 gives as it gives
//     compute-clusters: its form lists Kubernetes clusters beside the
//     clusters, and a cluster it names has the same details. Here it
//     names a cluster as compute-clusters does. It names no Kubernetes
//     cluster here, so a name that isn't one of the clusters is the
//     problem compute-clusters gives.
//   - Those whose value is a name, which the run reads as it was given,
//     as format 1 gives it: a scheduler's partitions, accounts, QoS and
//     queues, Kubernetes clusters, their namespaces and volume claims, and
//     an organization's groups (passedThrough).
//   - bucket, whose value is a bucket's pw://user/name URI, and gives its
//     details, as far as a run here knows them (resolveBucket).
//
// The compiler reports a problem with the input's name.
func (c *Config) ResolveInput(ctx context.Context, typ string, def map[string]any, value any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch typ {
	case "execution-groups":
	case "compute-clusters", "compute-resources":
		return c.resolveClusters(def, value)
	case "bucket":
		return resolveBucket(def, value)
	default:
		if slices.Contains(passedThrough, typ) {
			return passThrough(value)
		}
		return nil, compile.ErrInputType
	}
	name, ok := value.(string)
	if _, known := c.groups[name]; !ok || !known {
		return nil, problems.ExecutionGroupUnknown.InputProblem(problems.Params{"value": problems.Show(value), "groups": problems.QuoteAll(c.GroupNames())})
	}
	return name, nil
}

// resolveEach gives the value of an input whose value names a platform's
// resource, of the shape its type takes: with multi, a list, whose items
// item gives; and otherwise text, which one gives, or a mapping, the
// details a run passes to a workflow it calls, which is kept as it is. ""
// is no value, list or not, and is kept. Any other value is
// problems.InputNotList, with multi, or problems.InputNotText. Format 1's
// run creation checked no shape against multi, so a value of another one
// reached the run as it was given, or resolved: a cluster's or a bucket's
// details read from it were empty, and a name was its JSON text. Checking
// it is a bug fix to format 1, which ACTIVATE takes too (docs/design.md,
// Bug fixes to format 1).
//
// The types differ in two ways, which the callers keep. Only
// compute-clusters, compute-resources and bucket read multi; every other
// type takes text or a mapping whatever multi says. And a list's items
// differ: a cluster list keeps what isn't a name, as details a caller
// resolved already, where a bucket list takes only URIs, but for the
// mappings kept here (docs/design.md, Interfaces).
func resolveEach(value any, multi bool, one func(string) (any, error), item func(any) (any, error)) (any, error) {
	if value == "" {
		return value, nil
	}
	if multi {
		list, ok := value.([]any)
		if !ok {
			return nil, problems.InputNotList.InputProblem(problems.Params{"value": problems.Show(value)})
		}
		out := make([]any, len(list))
		for i, it := range list {
			v, err := item(it)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	switch v := value.(type) {
	case string:
		return one(v)
	case map[string]any:
		return v, nil
	}
	return nil, problems.InputNotText.InputProblem(problems.Params{"value": problems.Show(value)})
}

// expandHome turns a path that starts with ~/ into one in the home
// directory, and a relative one into one in dir. It reports false for a
// path that starts with ~/ when HOME isn't an absolute path.
func expandHome(path, dir string, lookupEnv func(string) (string, bool)) (string, bool) {
	if rest, ok := strings.CutPrefix(path, "~/"); ok {
		home, _ := lookupEnv("HOME")
		if !filepath.IsAbs(home) {
			return "", false
		}
		return filepath.Join(home, rest), true
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), true
	}
	return filepath.Join(dir, path), true
}
