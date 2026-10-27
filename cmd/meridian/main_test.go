package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

// TestFIPSRequired checks that no command runs, help and the worker
// included, unless the process's cryptography runs in FIPS 140-3 mode,
// enforced, as GODEBUG could otherwise turn it off.
//
// The test process itself must be built with the validated module
// (GOFIPS140=certified, as make sets it) for the commands of the other
// tests to run.
func TestFIPSRequired(t *testing.T) {
	for _, tc := range []struct{ mode, module, code string }{
		{"off", "v1.0.0", "fips_not_enforced"},
		{"on", "v1.0.0", "fips_not_enforced"},
		{"only", "latest", "fips_module_not_validated"},
	} {
		for _, args := range [][]string{{"version"}, {"help"}, {"worker"}, {"validate", "workflow.yaml"}} {
			var stdout, stderr strings.Builder
			c := &cli{
				stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr,
				lookupEnv: func(string) (string, bool) { return "", false },
				fips:      func() (string, string) { return tc.mode, tc.module },
			}
			code := run(t.Context(), c, args)
			if code != exitError || stdout.Len() > 0 || !strings.Contains(stderr.String(), "("+tc.code+")") {
				t.Errorf("meridian %s with FIPS 140-3 %s, module %s: %d, %q, %q", args, tc.mode, tc.module, code, stdout.String(), stderr.String())
			}
		}
	}
	if r := runCLI(t, nil, "", "version"); r.code != exitOK {
		t.Errorf("meridian version in this process, whose mode is enforced: %+v", r)
	}
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
			if listed := strings.Contains(r.stdout, "\n  "+cmd.name+" "); listed == cmd.hidden {
				t.Errorf("meridian %s lists %s %v, hidden %v:\n%s", args, cmd.name, listed, cmd.hidden, r.stdout)
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
	want := []string{"init", "run", "validate", "migrate", "status", "logs", "cancel", "resume", "rerun", "serve", "worker", "host", "relay", "version"}
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

// TestFail checks how the command writes an error that isn't a problem: a
// cancelled context is interrupted, which only an interrupt causes, and
// any other is unexpected_error, with its text as the reason, as text and
// as problem details.
func TestFail(t *testing.T) {
	for _, tc := range []struct {
		err  error
		json bool
		want string
	}{
		{errors.New("the disk is gone"), false, "meridian: error: "},
		{errors.New("the disk is gone"), true, `"code":"unexpected_error"`},
		{fmt.Errorf("reading: %w", context.Canceled), false, "(interrupted)"},
	} {
		var stderr strings.Builder
		c := &cli{stderr: &stderr, json: tc.json}
		if code := c.fail(tc.err); code != exitError || !strings.Contains(stderr.String(), tc.want) {
			t.Errorf("fail(%v) = %d, %q, want %q in it", tc.err, code, stderr.String(), tc.want)
		}
		if !errors.Is(tc.err, context.Canceled) && !strings.Contains(stderr.String(), "the disk is gone") {
			t.Errorf("fail(%v) wrote %q, without the reason", tc.err, stderr.String())
		}
	}
	var stderr strings.Builder
	(&cli{stderr: &stderr}).fail(errors.New("the disk is gone"))
	if !strings.Contains(stderr.String(), "(unexpected_error)") {
		t.Errorf("fail wrote %q, want unexpected_error", stderr.String())
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
	if r.code != exitOK || !strings.Contains(r.stdout, "usage: meridian run <workflow>") || !strings.Contains(r.stdout, "-run-dir") {
		t.Errorf("meridian help run = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "rerun")
	if r.code != exitOK || !strings.Contains(r.stdout, "It isn't built yet.") {
		t.Errorf("meridian help rerun = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "cancel")
	if r.code != exitOK || !strings.Contains(r.stdout, "usage: meridian cancel <run>") || !strings.Contains(r.stdout, "coordinates the run itself") || !strings.Contains(r.stdout, "-f") ||
		!strings.Contains(r.stdout, "meridian resume\n<run> sees it to its end") || !strings.Contains(r.stdout, "It exits 2 for a run the store hasn't") {
		t.Errorf("meridian help cancel = %+v", r)
	}
	r = runCLI(t, nil, "", "help", "resume")
	if r.code != exitOK || !strings.Contains(r.stdout, "usage: meridian resume <run>") || !strings.Contains(r.stdout, "-logs") {
		t.Errorf("meridian help resume = %+v", r)
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
		for _, want := range []string{"meridian ", "go:", "fips 140-3 module:", "plan version:", "worker API version:"} {
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
		{[]string{"rerun", "1"}, "command_not_built"},
		{[]string{"cancel"}, "arguments_invalid"},
		{[]string{"cancel", "1", "2"}, "arguments_invalid"},
		{[]string{"cancel", "--format", "xml", "1"}, "flag_value_invalid"},
		{[]string{"resume"}, "arguments_invalid"},
		{[]string{"logs"}, "arguments_invalid"},
		{[]string{"logs", "1", "build", "more"}, "arguments_invalid"},
		{[]string{"status", "1", "2"}, "arguments_invalid"},
		{[]string{"status", "-n", "0"}, "flag_value_invalid"},
		{[]string{"status", "--format", "xml"}, "flag_value_invalid"},
		{[]string{"run"}, "arguments_invalid"},
		{[]string{"run", "--group", "w.yaml"}, "arguments_invalid"},
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
