package local

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/expr"
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
	for name, text := range map[string]string{
		"secret in the file": "org:\n  t: {secret: true, value: x}\n",
		"secret alone":       "org:\n  t: {secret: true}\n",
		"two sources":        "org:\n  t: {env: A, file: b}\n",
		"no value":           "org:\n  t:\n",
		"unknown key":        "org:\n  t: {env: A, from: b}\n",
		"list":               "org:\n  t: [a]\n",
		"secret not bool":    "org:\n  t: {secret: maybe, env: A}\n",
		"env not text":       "org:\n  t: {env: [A]}\n",
		"unknown context":    "users:\n  t: x\n",
		"name":               "vars:\n  a.b: x\n",
		"name start":         "vars:\n  1a: x\n",
		"relative home":      "vars:\n  a: {file: ~/x}\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConfig(configDir(t, map[string]string{VariablesFile: text}), env(map[string]string{"HOME": "home"}))
			if err == nil {
				t.Fatal("LoadConfig succeeded")
			}
		})
	}
}
