package local

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/problems"
)

const variables = `
org:
  region: us-east-1
  port: 8080
  hf_token: {secret: true, env: HF_TOKEN}
  key: {secret: true, file: key.txt}
vars:
  build: {env: BUILD}
  plain: {value: "text"}
  home_file: {file: ~/value}
`

func variablesConfig(t *testing.T, vars map[string]string) *Config {
	t.Helper()
	dir := configDir(t, map[string]string{VariablesFile: variables, "key.txt": "s3cret\n"})
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "value"), []byte("from home\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	all := map[string]string{"HOME": home}
	maps.Copy(all, vars)
	c, err := LoadConfig(dir, env(all))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestReadVariables(t *testing.T) {
	c := variablesConfig(t, map[string]string{"HF_TOKEN": "hf_abc", "BUILD": "42"})
	for _, tc := range []struct {
		names   []string
		secrets bool
		want    map[string]compile.Variable
	}{
		{
			names: []string{"org.region", "vars.build", "org.missing", "vars.region"},
			want: map[string]compile.Variable{
				"org.region": {Value: "us-east-1"},
				"vars.build": {Value: "42"},
			},
		},
		{
			// A secret's value is left out when secrets is false.
			names: []string{"org"},
			want: map[string]compile.Variable{
				"org.region":   {Value: "us-east-1"},
				"org.port":     {Value: "8080"},
				"org.hf_token": {Secret: true},
				"org.key":      {Secret: true},
			},
		},
		{
			names:   []string{"org.hf_token", "org.key"},
			secrets: true,
			want: map[string]compile.Variable{
				"org.hf_token": {Value: "hf_abc", Secret: true},
				"org.key":      {Value: "s3cret", Secret: true},
			},
		},
		{
			names: []string{"vars"},
			want: map[string]compile.Variable{
				"vars.build":     {Value: "42"},
				"vars.plain":     {Value: "text"},
				"vars.home_file": {Value: "from home"},
			},
		},
		{names: nil, want: map[string]compile.Variable{}},
	} {
		got, err := c.Read(t.Context(), tc.names, tc.secrets)
		if err != nil {
			t.Fatalf("Read(%q, %t): %v", tc.names, tc.secrets, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Read(%q, %t) = %v, want %v", tc.names, tc.secrets, got, tc.want)
		}
	}
}

// TestReadSecretsLazily checks that a secret's value is read only when it is
// asked for: until then, where it is can be missing.
func TestReadSecretsLazily(t *testing.T) {
	c := variablesConfig(t, map[string]string{"BUILD": "1"})
	if err := os.Remove(c.variables["org.key"].file); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(t.Context(), []string{"org", "vars"}, false); err != nil {
		t.Fatalf("Read without secrets: %v", err)
	}
	_, err := c.Read(t.Context(), []string{"org.hf_token"}, true)
	if err == nil || !strings.Contains(err.Error(), "HF_TOKEN") {
		t.Errorf("Read of a secret whose environment variable isn't set: %v", err)
	}
	_, err = c.Read(t.Context(), []string{"org.key"}, true)
	if err == nil || !strings.Contains(err.Error(), "key.txt") {
		t.Errorf("Read of a secret whose file is missing: %v", err)
	}
}

func TestReadPlainFromMissingEnv(t *testing.T) {
	c := variablesConfig(t, nil)
	if _, err := c.Read(t.Context(), []string{"vars.build"}, false); err == nil {
		t.Error("Read of a plain variable whose environment variable isn't set succeeded")
	}
}

func TestReadRefusesOtherNames(t *testing.T) {
	c := variablesConfig(t, nil)
	for _, name := range []string{"env.HOME", "inputs.x", "", "orgs"} {
		if _, err := c.Read(t.Context(), []string{name}, false); err == nil {
			t.Errorf("Read(%q) succeeded", name)
		}
	}
}

func TestContexts(t *testing.T) {
	c := variablesConfig(t, map[string]string{"BUILD": "7"})
	got, err := c.Contexts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"org": map[string]any{
			"region": "us-east-1", "port": "8080",
			"hf_token": expr.Unknown{Name: "org"}, "key": expr.Unknown{Name: "org"},
		},
		"vars": map[string]any{"build": "7", "plain": "text", "home_file": "from home"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Contexts = %v, want %v", got, want)
	}
}

func TestContextsWithoutFile(t *testing.T) {
	got, err := load(t, nil, nil).Contexts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"org": map[string]any{}, "vars": map[string]any{}}; !reflect.DeepEqual(got, want) {
		t.Errorf("Contexts = %v, want %v", got, want)
	}
}

func TestVariablesFileRefuses(t *testing.T) {
	for name, tc := range map[string]struct {
		text string
		kind *problems.Kind
	}{
		"secret in the file": {"org:\n  t: {secret: true, value: x}\n", problems.VariableSecretInline},
		"secret alone":       {"org:\n  t: {secret: true}\n", problems.VariableSources},
		"two sources":        {"org:\n  t: {env: A, file: b}\n", problems.VariableSources},
		"no value":           {"org:\n  t:\n", problems.VariableNoValue},
		"unknown key":        {"org:\n  t: {env: A, from: b}\n", problems.ConfigKeyUnknown},
		"repeated key":       {"org:\n  t: {secret: true, file: f, secret: false}\n", problems.ConfigKeyRepeated},
		"repeated variable":  {"org:\n  t: a\n  t: b\n", problems.ConfigKeyRepeated},
		"list":               {"org:\n  t: [a]\n", problems.VariableFormInvalid},
		"secret not bool":    {"org:\n  t: {secret: maybe, env: A}\n", problems.ConfigWrongType},
		"env not text":       {"org:\n  t: {env: [A]}\n", problems.ConfigWrongType},
		"context not map":    {"org: [a]\n", problems.ConfigWrongType},
		"unknown context":    {"users:\n  t: x\n", problems.ConfigKeyUnknown},
		"name":               {"vars:\n  a.b: x\n", problems.VariableNameInvalid},
		"name start":         {"vars:\n  1a: x\n", problems.VariableNameInvalid},
		"relative home":      {"vars:\n  a: {file: ~/x}\n", problems.VariableFileHome},
		"two documents":      {"vars: {}\n---\norg: {}\n", problems.ConfigExtraDocument},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConfig(configDir(t, map[string]string{VariablesFile: tc.text}), env(map[string]string{"HOME": "home"}))
			wantProblem(t, err, tc.kind)
		})
	}
}

// TestVariableFileModes checks that a variable's value is read only from a
// file that is the user's and that no one else can change, nor read when
// it holds a secret's, as the file opened says.
func TestVariableFileModes(t *testing.T) {
	for _, tc := range []struct {
		secret bool
		mode   os.FileMode
		kind   *problems.Kind
	}{
		{true, 0o600, nil},
		{true, 0o640, problems.FileNotPrivate},
		{true, 0o666, problems.FileNotPrivate},
		{false, 0o644, nil},
		{false, 0o664, problems.FileChangeable},
		{false, 0o666, problems.FileChangeable},
	} {
		dir := configDir(t, map[string]string{
			VariablesFile: fmt.Sprintf("org:\n  v: {secret: %t, file: v.txt}\n", tc.secret),
			"v.txt":       "value\n",
		})
		if err := os.Chmod(filepath.Join(dir, "v.txt"), tc.mode); err != nil {
			t.Fatal(err)
		}
		c, err := LoadConfig(dir, env(nil))
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Read(t.Context(), []string{"org.v"}, true)
		if tc.kind == nil {
			if err != nil || got["org.v"].Value != "value" {
				t.Errorf("secret %t at %04o: %v, %v", tc.secret, tc.mode, got, err)
			}
			continue
		}
		wantProblem(t, err, tc.kind)
	}
}

// TestContextsUnreadable checks that a plain variable whose value can't be
// read is unknown when a run is created, so that only a run that reads it
// fails, as it starts.
func TestContextsUnreadable(t *testing.T) {
	c := variablesConfig(t, nil)
	got, err := c.Contexts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if v := got["vars"].(map[string]any)["build"]; v != (expr.Unknown{Name: "vars"}) {
		t.Errorf("vars.build, whose environment variable isn't set, is %v", v)
	}
	_, err = c.Read(t.Context(), []string{"vars.build"}, false)
	if p := wantProblem(t, err, problems.VariableEnvUnset); p.Params["env"] != "BUILD" || p.Params["variable"] != "vars.build" {
		t.Errorf("the problem's params are %v", p.Params)
	}
}

// TestVariableFileDirectory checks that a value is read only from a file
// whose directories no one else can change: one who could would swap it
// for a link to one of the user's own files.
func TestVariableFileDirectory(t *testing.T) {
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "v.txt"), []byte("v"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatal(err)
	}
	dir := configDir(t, map[string]string{VariablesFile: "org:\n  v: {secret: true, file: " + filepath.Join(shared, "v.txt") + "}\n"})
	c, err := LoadConfig(dir, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Read(t.Context(), []string{"org.v"}, true)
	if p := wantProblem(t, err, problems.PathUnsafe); p.Params["dir"] != shared {
		t.Errorf("the problem's params are %v", p.Params)
	}
}
