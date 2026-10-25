package main

import (
	"flag"
	"slices"
	"strings"
	"testing"
)

// result is what one run of the command did.
type result struct {
	stdout, stderr string
	code           int
}

// runCLI runs the command with args, the environment variables vars and
// stdin as its standard input.
func runCLI(t *testing.T, vars map[string]string, stdin string, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	c := &cli{
		stdin:     strings.NewReader(stdin),
		stdout:    &stdout,
		stderr:    &stderr,
		lookupEnv: func(name string) (string, bool) { v, ok := vars[name]; return v, ok },
	}
	code := run(t.Context(), c, args)
	return result{stdout.String(), stderr.String(), code}
}

func TestUsage(t *testing.T) {
	r := runCLI(t, nil, "")
	if r.code != exitError || r.stdout != "" || !strings.Contains(r.stderr, "usage: meridian <command>") {
		t.Errorf("meridian with no command = %+v", r)
	}
	for _, args := range [][]string{{"help"}, {"-h"}, {"--help"}} {
		r := runCLI(t, nil, "", args...)
		if r.code != exitOK || r.stderr != "" {
			t.Errorf("meridian %s = %+v", args, r)
		}
		for _, cmd := range commands {
			if !strings.Contains(r.stdout, "\n  "+cmd.name) {
				t.Errorf("meridian %s doesn't list %s:\n%s", args, cmd.name, r.stdout)
			}
		}
	}
}

// TestCommandsAreTheDesigns checks that the command has the commands
// docs/design.md's table names, in its order.
func TestCommandsAreTheDesigns(t *testing.T) {
	var names []string
	for _, cmd := range commands {
		names = append(names, cmd.name)
	}
	want := []string{"init", "run", "validate", "migrate", "status", "logs", "cancel", "resume", "rerun", "serve", "worker", "relay", "version"}
	if !slices.Equal(names, want) {
		t.Errorf("the commands are %q, want %q", names, want)
	}
}

func TestNotBuilt(t *testing.T) {
	for _, cmd := range commands {
		if cmd.run != nil {
			continue
		}
		r := runCLI(t, nil, "", cmd.name, "x")
		if r.code != exitError || r.stdout != "" || !strings.Contains(r.stderr, cmd.name+" isn't built yet") {
			t.Errorf("meridian %s = %+v", cmd.name, r)
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	r := runCLI(t, nil, "", "valdate", "w.yaml")
	if want := "meridian: error: There is no command \"valdate\". Did you mean \"validate\"? (command_unknown)\n"; r.code != exitError || r.stderr != want {
		t.Errorf("meridian valdate = %+v, want %q", r, want)
	}
	r = runCLI(t, nil, "", "frobnicate")
	if r.code != exitError || strings.Contains(r.stderr, "did you mean") {
		t.Errorf("meridian frobnicate = %+v", r)
	}
}

func TestHelpCommand(t *testing.T) {
	r := runCLI(t, nil, "", "help", "validate")
	if r.code != exitOK || !strings.Contains(r.stdout, "usage: meridian validate <workflow> [options]") || !strings.Contains(r.stdout, "-inputs file") {
		t.Errorf("meridian help validate = %+v", r)
	}
	r = runCLI(t, nil, "", "validate", "-h")
	if r.code != exitOK || !strings.Contains(r.stdout, "usage: meridian validate") || r.stderr != "" {
		t.Errorf("meridian validate -h = %+v", r)
	}
	r = runCLI(t, nil, "", "validate", "--lang", "ja")
	if r.code != exitError || r.stdout != "" || r.stderr != "meridian: error: The command meridian validate has no option -lang. (flag_unknown)\n" {
		t.Errorf("meridian validate --lang ja = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "version")
	if r.code != exitOK || strings.Contains(r.stdout, "options:") {
		t.Errorf("meridian help version = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "run")
	if r.code != exitOK || !strings.Contains(r.stdout, "It isn't built yet.") {
		t.Errorf("meridian help run = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "nope")
	if r.code != exitError {
		t.Errorf("meridian help nope = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "run", "validate")
	if r.code != exitError {
		t.Errorf("meridian help run validate = %+v", r)
	}
}

func TestVersion(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}} {
		r := runCLI(t, nil, "", args...)
		if r.code != exitOK {
			t.Fatalf("meridian %s = %+v", args, r)
		}
		for _, want := range []string{"meridian ", "go:", "fips 140-3 module:", "plan version:"} {
			if !strings.Contains(r.stdout, want) {
				t.Errorf("meridian %s doesn't say %q:\n%s", args, want, r.stdout)
			}
		}
	}
	if r := runCLI(t, nil, "", "version", "extra"); r.code != exitError {
		t.Errorf("meridian version extra = %+v", r)
	}
}

func TestParseFlags(t *testing.T) {
	for _, tc := range []struct {
		args       []string
		positional []string
		input      []string
		format     string
	}{
		{args: []string{"w.yaml"}, positional: []string{"w.yaml"}, format: "text"},
		{args: []string{"w.yaml", "--format", "json", "-input", "a=1"}, positional: []string{"w.yaml"}, input: []string{"a=1"}, format: "json"},
		{args: []string{"-input=a=1", "w.yaml", "x", "--input", "b=2"}, positional: []string{"w.yaml", "x"}, input: []string{"a=1", "b=2"}, format: "text"},
		{args: []string{"--format=json", "--", "-w.yaml", "--input", "c=3"}, positional: []string{"-w.yaml", "--input", "c=3"}, format: "json"},
		{args: []string{"a", "--", "--b"}, positional: []string{"a", "--b"}, format: "text"},
		{args: nil, format: "text"},
	} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		var input []string
		fs.Func("input", "", func(s string) error { input = append(input, s); return nil })
		format := fs.String("format", "text", "")
		positional, err := parseFlags(fs, tc.args)
		if err != nil {
			t.Fatalf("parseFlags(%q): %v", tc.args, err)
		}
		if !slices.Equal(positional, tc.positional) || !slices.Equal(input, tc.input) || *format != tc.format {
			t.Errorf("parseFlags(%q) = %q, input %q, format %s; want %q, %q, %s", tc.args, positional, input, *format, tc.positional, tc.input, tc.format)
		}
	}
}

// TestUsageProblems checks that each problem with the command's arguments
// is a kind, with its code, on standard error.
func TestUsageProblems(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code string
	}{
		{nil, "command_missing"},
		{[]string{"frobnicate"}, "command_unknown"},
		{[]string{"run", "w.yaml"}, "command_not_built"},
		{[]string{"help", "a", "b"}, "arguments_invalid"},
		{[]string{"version", "extra"}, "arguments_invalid"},
		{[]string{"validate"}, "arguments_invalid"},
		{[]string{"validate", "--lang", "ja", "w.yaml"}, "flag_unknown"},
		{[]string{"validate", "w.yaml", "--format"}, "flag_needs_value"},
		{[]string{"validate", "--format", "xml", "w.yaml"}, "flag_value_invalid"},
		{[]string{"validate", "--input", "count", "w.yaml"}, "input_flag_unsplit"},
		{[]string{"validate", "---format=json", "w.yaml"}, "flag_syntax"},
	} {
		r := runCLI(t, nil, "", tc.args...)
		if r.code != exitError || r.stdout != "" || !strings.HasPrefix(r.stderr, "meridian: error: ") || !strings.Contains(r.stderr, "("+tc.code+")\n") {
			t.Errorf("meridian %q = %+v, want %s", tc.args, r, tc.code)
		}
	}
}
