// Command meridian compiles workflows and runs them on this machine, or on
// the execution groups the user's config names (docs/design.md, The
// meridian command).
//
// Usage:
//
//	meridian <command> [arguments]
//
// Run meridian help for its commands. Its exit status is 0 when it did
// what it was asked, 1 when a workflow it was given has errors, and 2 when
// it couldn't do what it was asked, such as for a command it doesn't know.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/parallelworks/meridian/internal/hint"
)

// The command's exit statuses.
const (
	// exitOK is a command that did what it was asked.
	exitOK = 0
	// exitInvalid is a workflow with errors.
	exitInvalid = 1
	// exitError is a command that couldn't do what it was asked.
	exitError = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, &cli{stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr, getenv: os.Getenv}, os.Args[1:])
	stop()
	os.Exit(code)
}

// A cli is what a command reads and writes besides its arguments, which a
// test gives in place of the process's.
type cli struct {
	stdin          io.Reader
	stdout, stderr io.Writer
	getenv         func(string) string
}

// errorf writes a message saying why the command couldn't do what it was
// asked, and returns exitError.
func (c *cli) errorf(format string, args ...any) int {
	fmt.Fprintf(c.stderr, "meridian: "+format+"\n", args...)
	return exitError
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
		{name: "run", args: "<workflow>", summary: "Compiles and runs a workflow, coordinating it in the foreground."},
		{name: "validate", args: "<workflow>", summary: "Compiles without running, and reports every error.", run: runValidate},
		{name: "migrate", args: "<workflow>", summary: "Rewrites a workflow in the latest format."},
		{name: "status", args: "[run]", summary: "Shows runs, or one run's jobs."},
		{name: "logs", args: "<run> [job]", summary: "Shows or follows logs."},
		{name: "cancel", args: "<run>", summary: "Cancels a run."},
		{name: "resume", args: "<run>", summary: "Picks up a run whose coordinator stopped."},
		{name: "rerun", args: "<run>", summary: "Runs a run again, or only its failed jobs."},
		{name: "serve", summary: "Runs a coordinator that keeps running."},
		{name: "worker", summary: "Runs one attempt of a job. Started by backends, not by people."},
		{name: "relay", summary: "Forwards workers' connections to a coordinator. Started by backends, not by people."},
		{name: "version", summary: "Shows Meridian's version.", run: runVersion},
	}
}

// run runs the command args name, and returns its exit status.
func run(ctx context.Context, c *cli, args []string) int {
	if len(args) == 0 {
		usage(c.stderr)
		return exitError
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
		return c.errorf("%s isn't built yet: docs/design.md, under The meridian command, says what it will do", name)
	}
	return cmd.run(ctx, c, flagSet(c, cmd), rest)
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
	if s, ok := hint.Suggest(name, names); ok {
		return c.errorf("there is no command %q; did you mean %q? Run meridian help for the commands", name, s)
	}
	return c.errorf("there is no command %q; run meridian help for the commands", name)
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
		return cmd.run(ctx, c, flagSet(c, cmd), []string{"-h"})
	default:
		return c.errorf("help takes one command, or none")
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
// positional. It returns flag.ErrHelp for -h, having written the usage.
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

// flagSet returns a flag set for cmd, which writes what is wrong with its
// arguments to c's standard error.
func flagSet(c *cli, cmd command) *flag.FlagSet {
	fs := flag.NewFlagSet(cmd.name, flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	// flagError writes the usage, where it belongs.
	fs.Usage = func() {}
	return fs
}

// flagError returns the exit status of a command whose arguments fs
// couldn't parse, having written its usage to standard output for -h, and
// where to find it to standard error otherwise, after what fs wrote of
// what was wrong.
func flagError(c *cli, fs *flag.FlagSet, err error) int {
	if !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(c.stderr, "Run meridian help %s for its options.\n", fs.Name())
		return exitError
	}
	cmd, _ := lookup(fs.Name())
	fmt.Fprintf(c.stdout, "usage: meridian %s [options]\n\n%s\n", strings.TrimSpace(cmd.name+" "+cmd.args), cmd.summary)
	var options strings.Builder
	fs.SetOutput(&options)
	fs.PrintDefaults()
	fs.SetOutput(c.stderr)
	if options.Len() > 0 {
		fmt.Fprintf(c.stdout, "\noptions:\n%s", options.String())
	}
	return exitOK
}
