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
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// The files in the config directory.
const (
	GroupsFile    = "groups.yaml"
	VariablesFile = "variables.yaml"
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

// Config is the user's config: their execution groups and variables, from
// the config directory. It provides the Variables and Groups the compiler
// and the coordinator read, and the input types that name what it holds.
type Config struct {
	groups    map[string]*backend.Group
	variables map[string]variable
	getenv    func(string) string
}

var (
	_ compile.Variables     = (*Config)(nil)
	_ compile.Groups        = (*Config)(nil)
	_ compile.InputResolver = (*Config)(nil)
)

// LoadConfig reads the groups file and the variables file in dir, either
// of which can be missing, and checks them. getenv reads the environment
// variables that hold variables' values, when they are read, and HOME, for
// a path that starts with ~/.
//
// Neither file may be changeable by another user, who could otherwise send
// the user's jobs to a host of their choosing, or give them values. The
// variables file must be readable only by the user, since people keep
// secrets in plain variables too.
func LoadConfig(dir string, getenv func(string) string) (*Config, error) {
	c := &Config{getenv: getenv, groups: map[string]*backend.Group{}, variables: map[string]variable{}}
	if err := c.loadGroups(filepath.Join(dir, GroupsFile)); err != nil {
		return nil, err
	}
	if _, ok := c.groups[LocalGroup]; !ok {
		c.groups[LocalGroup] = localGroup()
	}
	if err := c.loadVariables(dir, filepath.Join(dir, VariablesFile)); err != nil {
		return nil, err
	}
	return c, nil
}

// readConfig reads the config file at path, after check checks the file it
// opened, or returns nil when there is none.
func readConfig(path string, check func(fs.FileInfo) error) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // the user's own config file, which check checks

	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := check(info); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, maxConfigSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxConfigSize {
		return nil, fmt.Errorf("%s is larger than %d bytes", path, maxConfigSize)
	}
	return data, nil
}

// decodeStrict decodes the YAML document data into v, refusing keys v
// doesn't have. An empty document leaves v as it is.
func decodeStrict(path string, data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// namePattern is what the name of a group, a member or a variable can be:
// letters, digits, _ and -, and . too in a group's or a member's, but
// never first.
var (
	groupNamePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	variableNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
	isolationPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
)

// backends are the backends a member can name.
var backends = []string{"local", "ssh", "kubernetes", "slurm", "pbs"}

// groupsFile is the groups file: each group by its name.
type groupsFile map[string]struct {
	Members []memberFile `yaml:"members"`
}

// memberFile is one member of a group, as the groups file writes it.
type memberFile struct {
	Name      string         `yaml:"name"`
	Backend   string         `yaml:"backend"`
	Settings  map[string]any `yaml:"settings"`
	Isolation string         `yaml:"isolation"`
	Resources struct {
		CPU    string `yaml:"cpu"`
		Memory string `yaml:"memory"`
		GPU    int    `yaml:"gpu"`
	} `yaml:"resources"`
	MaxWalltime    string `yaml:"max-walltime"`
	Margin         string `yaml:"margin"`
	DefaultTimeout string `yaml:"default-timeout"`
}

// loadGroups reads the groups file at path.
func (c *Config) loadGroups(path string) error {
	data, err := readConfig(path, func(info fs.FileInfo) error { return checkUnchangeable(path, "groups file", info) })
	if err != nil || data == nil {
		return err
	}
	var file groupsFile
	if err := decodeStrict(path, data, &file); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(file)) {
		g, err := group(name, file[name].Members)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		c.groups[name] = g
	}
	return nil
}

// group checks the group name and its members.
func group(name string, members []memberFile) (*backend.Group, error) {
	if !groupNamePattern.MatchString(name) {
		return nil, fmt.Errorf("a group is named %q: a group's name has letters, digits, _, - and ., and starts with a letter or digit", name)
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("group %q has no members", name)
	}
	g := &backend.Group{Name: name}
	seen := map[string]bool{}
	for i, mf := range members {
		m, err := member(mf)
		if err != nil {
			return nil, fmt.Errorf("group %q, member %d: %w", name, i+1, err)
		}
		if seen[m.Name] {
			return nil, fmt.Errorf("group %q has two members named %q", name, m.Name)
		}
		seen[m.Name] = true
		g.Members = append(g.Members, m)
	}
	return g, nil
}

// member checks a member as the groups file writes it, and returns it as
// the coordinator reads it.
func member(mf memberFile) (backend.Member, error) {
	m := backend.Member{Name: mf.Name, Backend: mf.Backend, Isolation: mf.Isolation}
	switch {
	case !groupNamePattern.MatchString(m.Name):
		return m, fmt.Errorf("its name is %q: a member's name has letters, digits, _, - and ., and starts with a letter or digit", m.Name)
	case !slices.Contains(backends, m.Backend):
		return m, fmt.Errorf("its backend is %q, not one of %s", m.Backend, problems.QuoteAll(backends))
	case m.Isolation != "" && !isolationPattern.MatchString(m.Isolation):
		return m, fmt.Errorf("its isolation is %q: it names a container runtime, such as podman, or host", m.Isolation)
	case mf.Resources.GPU < 0:
		return m, fmt.Errorf("its resources' gpu is %d, less than 0", mf.Resources.GPU)
	}
	var err error
	if mf.Settings != nil {
		if m.Settings, err = settingsJSON(mf.Settings); err != nil {
			return m, fmt.Errorf("its settings: %w", err)
		}
	}
	if mf.Resources.CPU != "" {
		if m.Resources.MilliCPU, err = parseCPU(mf.Resources.CPU); err != nil {
			return m, fmt.Errorf("its resources' cpu: %w", err)
		}
	}
	if mf.Resources.Memory != "" {
		if m.Resources.Memory, err = parseMemory(mf.Resources.Memory); err != nil {
			return m, fmt.Errorf("its resources' memory: %w", err)
		}
	}
	m.Resources.GPU = mf.Resources.GPU
	for _, d := range []struct {
		key  string
		text string
		into *int64
	}{
		{"max-walltime", mf.MaxWalltime, &m.MaxWalltimeSeconds},
		{"margin", mf.Margin, &m.MarginSeconds},
		{"default-timeout", mf.DefaultTimeout, &m.DefaultTimeoutSeconds},
	} {
		if d.text == "" {
			continue
		}
		if *d.into, err = parseSeconds(d.text); err != nil {
			return m, fmt.Errorf("its %s: %w", d.key, err)
		}
	}
	return m, nil
}

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

// ResolveInput provides the execution-groups input type, whose value is the
// name of one of the config's groups, and no other: the input types that
// name a scheduler's partitions, accounts and queues come later.
func (c *Config) ResolveInput(typ string, _ map[string]any, value any) (any, error) {
	if typ != "execution-groups" {
		return nil, compile.ErrInputType
	}
	name, ok := value.(string)
	if !ok {
		return nil, errors.New("an execution group is given by its name, which is text")
	}
	if _, ok := c.groups[name]; !ok {
		return nil, fmt.Errorf("no execution group is named %q; the groups are %s", name, problems.QuoteAll(c.GroupNames()))
	}
	return name, nil
}

// expandHome turns a path that starts with ~/ into one in the home
// directory, and a relative one into one in dir.
func expandHome(path, dir string, getenv func(string) string) (string, error) {
	if rest, ok := strings.CutPrefix(path, "~/"); ok {
		home := getenv("HOME")
		if !filepath.IsAbs(home) {
			return "", fmt.Errorf("%s starts with ~/, and HOME isn't set to an absolute path", path)
		}
		return filepath.Join(home, rest), nil
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	return filepath.Join(dir, path), nil
}
