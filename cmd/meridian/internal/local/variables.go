package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
)

// The contexts a variable is in: organization variables, read as
// org.<name>, and the user's, read as vars.<name>.
const (
	contextOrg  = "org"
	contextVars = "vars"
)

// A variable is one of the variables file's: its value, or where its value
// is, and whether it is a secret.
//
// In the file, a plain variable is its value. Any variable can instead be
// a mapping, which gives its value, or the environment variable or the
// file that holds it, and says whether it is a secret:
//
//	org:
//	  region: us-east-1
//	  hf_token: {secret: true, env: HF_TOKEN}
//	vars:
//	  deploy_key: {secret: true, file: ~/.config/meridian/deploy_key}
//	  build: {env: BUILD_NUMBER}
//
// A secret names where its value is, and is never in the file. Its value
// is read only when a run asks for it, as it starts. A file's value has a
// newline at its end, if there is one, removed.
type variable struct {
	value  string
	env    string
	file   string
	secret bool
	// read is whether the file gave the variable, which YAML's decoder
	// leaves false for a null, which it doesn't give UnmarshalYAML.
	read bool
}

// UnmarshalYAML reads a variable as the variables file writes one.
func (v *variable) UnmarshalYAML(n *yaml.Node) error {
	v.read = true
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return fmt.Errorf("line %d: a variable has no value", n.Line)
		}
		v.value = n.Value
		return nil
	case yaml.MappingNode:
	default:
		return fmt.Errorf("line %d: a variable is its value, or a mapping with value, env or file, and secret", n.Line)
	}
	sources := 0
	for i := 0; i+1 < len(n.Content); i += 2 {
		key, val := n.Content[i], n.Content[i+1]
		if key.Value == "secret" {
			if err := val.Decode(&v.secret); err != nil {
				return fmt.Errorf("line %d: secret is true or false", val.Line)
			}
			continue
		}
		if val.Kind != yaml.ScalarNode || val.Tag == "!!null" {
			return fmt.Errorf("line %d: %s is text", val.Line, key.Value)
		}
		switch key.Value {
		case "value":
			v.value = val.Value
		case "env":
			v.env = val.Value
		case "file":
			v.file = val.Value
		default:
			return fmt.Errorf("line %d: a variable has no key %q: it has value, env, file and secret", key.Line, key.Value)
		}
		sources++
	}
	switch {
	case sources != 1:
		return fmt.Errorf("line %d: a variable has one of value, env and file", n.Line)
	case v.secret && v.env == "" && v.file == "":
		return fmt.Errorf("line %d: a secret variable names the environment variable (env) or the file (file) that holds its value, rather than hold it in this file", n.Line)
	}
	return nil
}

// variablesFile is the variables file.
type variablesFile struct {
	Org  map[string]variable `yaml:"org"`
	Vars map[string]variable `yaml:"vars"`
}

// loadVariables reads the variables file at path, in dir.
func (c *Config) loadVariables(dir, path string) error {
	// Variables can hold secrets, as people keep them in plain variables
	// too, so only the user may read the file.
	data, err := readConfig(path, func(info fs.FileInfo) error { return checkPrivateInfo(path, "variables file", info) })
	if err != nil || data == nil {
		return err
	}
	var file variablesFile
	if err := decodeStrict(path, data, &file); err != nil {
		return err
	}
	for context, vars := range map[string]map[string]variable{contextOrg: file.Org, contextVars: file.Vars} {
		for name, v := range vars {
			if !variableNamePattern.MatchString(name) {
				return fmt.Errorf("%s: a variable in %s is named %q: a variable's name has letters, digits, _ and -, and starts with a letter or _", path, context, name)
			}
			if !v.read {
				return fmt.Errorf("%s: %s.%s has no value", path, context, name)
			}
			if v.file != "" {
				if v.file, err = expandHome(v.file, dir, c.getenv); err != nil {
					return fmt.Errorf("%s: %s.%s: %w", path, context, name, err)
				}
			}
			c.variables[context+"."+name] = v
		}
	}
	return nil
}

// Read returns the variables that names name, by their names as the plan
// writes them, org.<name> or vars.<name>, with org or vars alone for every
// variable of that context. A variable the file doesn't have is left out.
// A secret's value is read from where the file says only when secrets is
// true; otherwise it is left out.
func (c *Config) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := map[string]compile.Variable{}
	for _, name := range names {
		context, key, one := strings.Cut(name, ".")
		if context != contextOrg && context != contextVars {
			return nil, fmt.Errorf("%q isn't a variable's name, which is org.<name> or vars.<name>", name)
		}
		var full []string
		if one {
			if _, ok := c.variables[name]; ok {
				full = []string{context + "." + key}
			}
		} else {
			for k := range c.variables {
				if strings.HasPrefix(k, context+".") {
					full = append(full, k)
				}
			}
		}
		for _, k := range full {
			v := c.variables[k]
			got := compile.Variable{Secret: v.secret}
			if !v.secret || secrets {
				value, err := c.resolve(v)
				if err != nil {
					return nil, fmt.Errorf("variable %s: %w", k, err)
				}
				got.Value = value
			}
			out[k] = got
		}
	}
	return out, nil
}

// Contexts returns the org and vars contexts as the compiler reads them
// when a run is created, in compile.Options.Contexts: each plain
// variable's value, and each secret as an expr.Unknown, since the compiler
// reads no secret's value then.
func (c *Config) Contexts(ctx context.Context) (map[string]any, error) {
	vars, err := c.Read(ctx, []string{contextOrg, contextVars}, false)
	if err != nil {
		return nil, err
	}
	org, user := map[string]any{}, map[string]any{}
	out := map[string]any{contextOrg: org, contextVars: user}
	for k, v := range vars {
		context, name, _ := strings.Cut(k, ".")
		m := org
		if context == contextVars {
			m = user
		}
		if v.Secret {
			m[name] = expr.Unknown{Name: context}
		} else {
			m[name] = v.Value
		}
	}
	return out, nil
}

// resolve returns v's value: the file's, or the environment variable's or
// the file's that it names.
func (c *Config) resolve(v variable) (string, error) {
	switch {
	case v.env != "":
		value, ok := lookupEnv(c.getenv, v.env)
		if !ok {
			return "", fmt.Errorf("its value is in the environment variable %s, which isn't set", v.env)
		}
		return value, nil
	case v.file != "":
		value, err := readValueFile(v.file)
		if err != nil {
			return "", fmt.Errorf("its value is in %s, which can't be read: %w", v.file, err)
		}
		return value, nil
	}
	return v.value, nil
}

// lookupEnv reads the environment variable name with getenv, and says
// whether it is set: an empty value counts as not set.
func lookupEnv(getenv func(string) string, name string) (string, bool) {
	v := getenv(name)
	return v, v != ""
}

// readValueFile reads a variable's value from the file at path, without
// the newline at its end.
func readValueFile(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // the file the user's variables file names
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("it isn't a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxConfigSize+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxConfigSize {
		return "", fmt.Errorf("it is larger than %d bytes", maxConfigSize)
	}
	s := strings.TrimSuffix(string(data), "\n")
	return strings.TrimSuffix(s, "\r"), nil
}
