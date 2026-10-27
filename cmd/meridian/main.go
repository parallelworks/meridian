// Command meridian compiles workflows and runs them on this machine, or on
// the execution groups the user's config names (docs/design.md, The
// meridian command).
//
// Usage:
//
//	meridian <command> [arguments]
//
// Run meridian help for its commands. Its exit status is 0 when it did
// what it was asked, 1 when a workflow it was given has errors, or a run it
// ran failed or was cancelled, and 2 when it couldn't do what it was asked,
// such as for a command it doesn't know, or when it was interrupted, which
// leaves a run it ran running. Every problem it reports is one of the
// problems package's kinds, with its code.
package main

import (
	"context"
	jsonv2 "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/internal/fipsmode"
	"github.com/parallelworks/meridian/internal/hint"
	"github.com/parallelworks/meridian/problems"
)

// The command's exit statuses.
const (
	// exitOK is a command that did what it was asked.
	exitOK = 0
	// exitInvalid is a workflow with errors, or a run that failed or was
	// cancelled.
	exitInvalid = 1
	// exitError is a command that couldn't do what it was asked.
	exitError = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// The first interrupt cancels ctx, which stops what the command waits
	// on; a second, which is no longer caught, stops the process at once.
	context.AfterFunc(ctx, stop)
	code := run(ctx, &cli{stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr, lookupEnv: os.LookupEnv}, os.Args[1:])
	stop()
	os.Exit(code)
}

// A cli is what a command reads and writes besides its arguments, which a
// test gives in place of the process's.
type cli struct {
	stdin          io.Reader
	stdout, stderr io.Writer
	// lookupEnv reads an environment variable, and says whether it is
	// set, as os.LookupEnv does.
	lookupEnv func(string) (string, bool)
	// json says to write problems as RFC 9457 problem details, one a line,
	// as validate --format json does.
	json bool
	// fips returns how the process's cryptography runs and its FIPS 140-3
	// module's version, which a test gives in place of the process's
	// (fipsmode.Mode and Module) for nil.
	fips func() (mode, module string)
	// signals and interactive are what a test gives meridian run in place
	// of the process's: the interrupts it gets, nil for the process's
	// (notify); and whether a person can answer it, nil for whether its
	// standard input and error are a terminal.
	signals     chan os.Signal
	interactive *bool
}

// getenv reads an environment variable, or "" if it isn't set.
func (c *cli) getenv(name string) string {
	v, _ := c.lookupEnv(name)
	return v
}

// fail writes the problem err is, or wraps, as compilers write an error,
// or as RFC 9457 problem details when c.json is set, and returns
// exitError. A cancelled context is problems.Interrupted, since
// only an interrupt cancels the command's, and any other error that isn't
// a problem problems.UnexpectedError, with its text as the reason.
func (c *cli) fail(err error) int {
	p, ok := errors.AsType[*problem.Problem](err)
	switch {
	case ok:
	case errors.Is(err, context.Canceled):
		p = problems.Interrupted.Problem(nil)
	default:
		p = problems.UnexpectedError.Problem(problems.Params{"reason": err.Error()})
	}
	if c.json {
		data, err := jsonv2.Marshal(p)
		if err == nil {
			fmt.Fprintf(c.stderr, "%s\n", data)
			return exitError
		}
	}
	d := problems.Diagnostic{Type: p.Type, Code: p.Code, Detail: p.Detail, Params: p.Params}
	line := d.String()
	if d.File() == "" {
		line = "meridian: " + line
	}
	fmt.Fprintln(c.stderr, line)
	return exitError
}

// failf writes a problem of kind k with params, and returns exitError.
func (c *cli) failf(k *problems.Kind, params problems.Params) int {
	return c.fail(k.Problem(params))
}

// A command is one of meridian's commands.
type command struct {
	name    string
	args    string
	summary string
	// run runs the command with its arguments, which it parses with fs,
	// and returns its exit status. It is nil for a command that isn't built
	// yet.
	run func(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int
}

// commands are meridian's commands, in the order help lists them. init
// fills it in, since some of them look it up.
var commands []command

func init() {
	commands = []command{
		{name: "init", summary: "Writes a new workflow in the latest format."},
		{name: "run", args: "<workflow>", summary: "Compiles and runs a workflow, coordinating it in the foreground.", run: runRun},
		{name: "validate", args: "<workflow>", summary: "Compiles without running, and reports every error.", run: runValidate},
		{name: "migrate", args: "<workflow>", summary: "Rewrites a workflow in the latest format."},
		{name: "status", args: "[run]", summary: "Shows runs, or one run's jobs."},
		{name: "logs", args: "<run> [job]", summary: "Shows or follows logs."},
		{name: "cancel", args: "<run>", summary: "Cancels a run."},
		{name: "resume", args: "<run>", summary: "Picks up a run whose coordinator stopped."},
		{name: "rerun", args: "<run>", summary: "Runs a run again, or only its failed jobs."},
		{name: "serve", summary: "Runs a coordinator that keeps running."},
		{name: "worker", summary: "Runs one attempt of a job. Started by backends, not by people.", run: runWorker},
		{name: "relay", summary: "Forwards workers' connections to a coordinator. Started by backends, not by people."},
		{name: "version", summary: "Shows Meridian's version.", run: runVersion},
	}
}

// run runs the command args name, and returns its exit status. No command
// runs unless FIPS 140-3 mode is enforced, with the validated module
// (fipsmode): go.mod's godebug line is only the binary's default, which
// GODEBUG overrides, a worker can be started by hand, or over SSH, with
// any environment, and a plain go build compiles the module from source.
func run(ctx context.Context, c *cli, args []string) int {
	mode, module := fipsmode.Mode(), fipsmode.Module()
	if c.fips != nil {
		mode, module = c.fips()
	}
	if err := fipsmode.Check(mode, module); err != nil {
		return c.fail(err)
	}
	if len(args) == 0 {
		code := c.failf(problems.CommandMissing, nil)
		usage(c.stderr)
		return code
	}
	name, rest := args[0], args[1:]
	switch name {
	case "help", "-h", "-help", "--help":
		return help(ctx, c, rest)
	case "-version", "--version":
		name = "version"
	}
	cmd, ok := lookup(name)
	if !ok {
		return unknown(c, name)
	}
	if cmd.run == nil {
		return c.failf(problems.CommandNotBuilt, problems.Params{"command": name})
	}
	return cmd.run(ctx, c, flagSet(cmd), rest)
}

// lookup returns the command named name.
func lookup(name string) (command, bool) {
	for _, cmd := range commands {
		if cmd.name == name {
			return cmd, true
		}
	}
	return command{}, false
}

// unknown says that meridian has no command named name, and suggests one
// it does have.
func unknown(c *cli, name string) int {
	names := make([]string, len(commands))
	for i, cmd := range commands {
		names[i] = cmd.name
	}
	s, ok := hint.Suggest(name, names)
	return c.failf(problems.CommandUnknown, problems.WithSuggestion(problems.Params{"command": name}, s, ok))
}

// help shows the commands, or one command's usage.
func help(ctx context.Context, c *cli, args []string) int {
	switch len(args) {
	case 0:
		usage(c.stdout)
		return exitOK
	case 1:
		cmd, ok := lookup(args[0])
		if !ok {
			return unknown(c, args[0])
		}
		if cmd.run == nil {
			fmt.Fprintf(c.stdout, "usage: meridian %s\n\n%s\n\nIt isn't built yet.\n", strings.TrimSpace(cmd.name+" "+cmd.args), cmd.summary)
			return exitOK
		}
		return cmd.run(ctx, c, flagSet(cmd), []string{"-h"})
	default:
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "help"})
	}
}

// usage writes the commands.
func usage(w io.Writer) {
	fmt.Fprint(w, "Meridian compiles and runs workflows.\n\nusage: meridian <command> [arguments]\n\ncommands:\n")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, cmd := range commands {
		state := ""
		if cmd.run == nil {
			state = " (not built yet)"
		}
		fmt.Fprintf(tw, "  %s\t%s%s\n", strings.TrimSpace(cmd.name+" "+cmd.args), cmd.summary, state)
	}
	tw.Flush()
	fmt.Fprint(w, "\nRun meridian help <command> for a command's options.\n")
}

// parseFlags parses args with fs, allowing flags among the positional
// arguments, as in meridian validate workflow.yaml --input n=1, and
// returns the positional arguments. Every argument after -- is
// positional. It returns flag.ErrHelp for -h.
func parseFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		// Parse stops at the first argument that isn't a flag, or after --,
		// which it removes; what follows -- is positional.
		if n := len(args) - len(rest); n > 0 && args[n-1] == "--" {
			return append(positional, rest...), nil
		}
		if len(rest) == 0 {
			return positional, nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

// flagSet returns a flag set for cmd, which writes nothing itself:
// flagError reports what is wrong with its arguments.
func flagSet(cmd command) *flag.FlagSet {
	fs := flag.NewFlagSet(cmd.name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// The errors package flag gives, which flagError tells apart.
var (
	flagUnknownRE = regexp.MustCompile(`^flag provided but not defined: -+(.*)$`)
	flagNeedsRE   = regexp.MustCompile(`^flag needs an argument: -+(.*)$`)
	flagValueRE   = regexp.MustCompile(`^invalid (?:boolean )?value "(.*)" for flag -+([^:]*): `)
	flagSyntaxRE  = regexp.MustCompile(`^bad flag syntax: (.*)$`)
)

// flagError returns the exit status of a command whose arguments fs
// couldn't parse: exitOK for -h, having written its usage to standard
// output, and exitError otherwise, having written what was wrong.
func flagError(c *cli, fs *flag.FlagSet, err error) int {
	name := fs.Name()
	if !errors.Is(err, flag.ErrHelp) {
		msg := err.Error()
		switch {
		case flagUnknownRE.MatchString(msg):
			return c.failf(problems.FlagUnknown, problems.Params{"command": name, "flag": flagUnknownRE.FindStringSubmatch(msg)[1]})
		case flagNeedsRE.MatchString(msg):
			return c.failf(problems.FlagNeedsValue, problems.Params{"command": name, "flag": flagNeedsRE.FindStringSubmatch(msg)[1]})
		case flagValueRE.MatchString(msg):
			m := flagValueRE.FindStringSubmatch(msg)
			return c.failf(problems.FlagValueInvalid, problems.Params{"command": name, "flag": m[2], "value": m[1]})
		case flagSyntaxRE.MatchString(msg):
			return c.failf(problems.FlagSyntax, problems.Params{"command": name, "argument": flagSyntaxRE.FindStringSubmatch(msg)[1]})
		}
		return c.fail(err)
	}
	cmd, _ := lookup(name)
	fmt.Fprintf(c.stdout, "usage: meridian %s [options]\n\n%s\n", strings.TrimSpace(cmd.name+" "+cmd.args), cmd.summary)
	var options strings.Builder
	fs.SetOutput(&options)
	fs.PrintDefaults()
	fs.SetOutput(io.Discard)
	if options.Len() > 0 {
		fmt.Fprintf(c.stdout, "\noptions:\n%s", options.String())
	}
	return exitOK
}
