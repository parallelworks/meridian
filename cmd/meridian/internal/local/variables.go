package local

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

// The contexts a variable is in: organization variables, read as
// org.<name>, and the user's, read as vars.<name>.
const (
	contextOrg  = "org"
	contextVars = "vars"
)

// variableKeys are the keys of a variable written as a mapping.
var variableKeys = []string{"value", "env", "file", "secret"}

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
// A secret names where its value is, and is never in the file. A value in
// an environment variable or a file is read only when a run asks for it,
// as it starts. A file's value has a newline at its end, if there is one,
// removed. A file that holds a value must belong to the user, and no other
// user may change it, nor read it if it holds a secret's.
type variable struct {
	// name is the variable's name as the plan writes it, as org.region.
	name   string
	value  string
	env    string
	file   string
	secret bool
}

// loadVariables reads the variables file at path, in dir.
func (c *Config) loadVariables(dir, path string) error {
	// Variables can hold secrets, as people keep them in plain variables
	// too, so only the user may read the file.
	data, err := readConfig(path, func(info fs.FileInfo) error { return checkPrivateInfo(path, info) })
	if err != nil || data == nil {
		return err
	}
	f := configFile{path}
	root, err := f.parse(data)
	if err != nil || root == nil {
		return err
	}
	contexts, err := f.pairs(root, []string{contextOrg, contextVars})
	if err != nil {
		return err
	}
	for _, ctx := range contexts {
		context := ctx.key.Value
		if ctx.value.Kind == yaml.ScalarNode && ctx.value.Tag == "!!null" {
			continue
		}
		if err := f.mapping(context, ctx.value); err != nil {
			return err
		}
		vars, err := f.pairs(ctx.value, nil)
		if err != nil {
			return err
		}
		for _, p := range vars {
			if !variableNamePattern.MatchString(p.key.Value) {
				return f.problem(problems.VariableNameInvalid, p.key, problems.Params{"context": context, "name": p.key.Value})
			}
			v, err := f.variable(context+"."+p.key.Value, p.key, p.value, dir, c.getenv)
			if err != nil {
				return err
			}
			c.variables[v.name] = v
		}
	}
	return nil
}

// variable reads the variable name, whose key and value n are, as the
// variables file writes one.
func (f configFile) variable(name string, key, n *yaml.Node, dir string, getenv func(string) string) (variable, error) {
	v := variable{name: name}
	at := func(k *problems.Kind, n *yaml.Node) error {
		return f.problem(k, n, problems.Params{"variable": name})
	}
	switch {
	case n.Kind == yaml.ScalarNode && n.Tag == "!!null":
		return v, at(problems.VariableNoValue, key)
	case n.Kind == yaml.ScalarNode:
		v.value = n.Value
		return v, nil
	case n.Kind != yaml.MappingNode:
		return v, at(problems.VariableFormInvalid, n)
	}
	pairs, err := f.pairs(n, variableKeys)
	if err != nil {
		return v, err
	}
	sources := 0
	for _, p := range pairs {
		k := p.key.Value
		if k == "secret" {
			if p.value.Tag != "!!bool" || p.value.Decode(&v.secret) != nil {
				return v, f.problem(problems.ConfigWrongType, p.value, problems.Params{"key": k, "want": "boolean"})
			}
			continue
		}
		text, err := f.text(k, p.value)
		if err != nil {
			return v, err
		}
		switch k {
		case "value":
			v.value = text
		case "env":
			v.env = text
		default:
			file, ok := expandHome(text, dir, getenv)
			if !ok {
				return v, f.problem(problems.VariableFileHome, p.value, problems.Params{"variable": name, "file": text})
			}
			v.file = file
		}
		sources++
	}
	switch {
	case sources != 1:
		return v, at(problems.VariableSources, n)
	case v.secret && v.env == "" && v.file == "":
		return v, at(problems.VariableSecretInline, n)
	}
	return v, nil
}

// Read returns the variables that names name, by their names as the plan
// writes them, org.<name> or vars.<name>, with org or vars alone for every
// variable of that context. A variable the file doesn't have is left out.
// A secret's value is read from where the file says only when secrets is
// true; otherwise it is left out. A value that can't be read is a problem
// that says why; a name that is neither org's nor vars' is the caller's
// mistake.
func (c *Config) Read(ctx context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := map[string]compile.Variable{}
	for _, name := range names {
		context, _, one := strings.Cut(name, ".")
		if context != contextOrg && context != contextVars {
			return nil, fmt.Errorf("local: %q isn't a variable's name, which is org.<name> or vars.<name>", name)
		}
		var full []string
		if one {
			if _, ok := c.variables[name]; ok {
				full = []string{name}
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
					return nil, err
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
// reads no secret's value then. A plain variable whose value can't be read
// is unknown too, so that only a run that reads it fails, when it starts.
func (c *Config) Contexts(ctx context.Context) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	org, user := map[string]any{}, map[string]any{}
	out := map[string]any{contextOrg: org, contextVars: user}
	for k, v := range c.variables {
		context, name, _ := strings.Cut(k, ".")
		m := org
		if context == contextVars {
			m = user
		}
		var value any = expr.Unknown{Name: context}
		if !v.secret {
			if s, err := c.resolve(v); err == nil {
				value = s
			}
		}
		m[name] = value
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
			return "", problems.VariableEnvUnset.Problem(problems.Params{"variable": v.name, "env": v.env})
		}
		return value, nil
	case v.file != "":
		return readValueFile(v)
	}
	return v.value, nil
}

// lookupEnv reads the environment variable name with getenv, and says
// whether it is set: an empty value counts as not set.
func lookupEnv(getenv func(string) string, name string) (string, bool) {
	v := getenv(name)
	return v, v != ""
}

// readValueFile reads v's value from its file, without the newline at its
// end. The file must belong to the user, and no other user may change it,
// nor read it when it holds a secret, as the file opened says.
func readValueFile(v variable) (string, error) {
	f, err := openRegular(v.file, func(info fs.FileInfo) error {
		if v.secret {
			return checkPrivateInfo(v.file, info)
		}
		return checkUnchangeable(v.file, info)
	})
	if err != nil {
		if isProblem(err) {
			return "", err
		}
		return "", problems.VariableFileUnreadable.Problem(problems.Params{"variable": v.name, "file": v.file, "reason": err.Error()})
	}
	defer f.Close()
	data, err := readLimited(f, v.file, maxConfigSize)
	if err != nil {
		return "", err
	}
	s := strings.TrimSuffix(string(data), "\n")
	return strings.TrimSuffix(s, "\r"), nil
}
