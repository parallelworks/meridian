package main

import (
	"context"
	"crypto/fips140"
	"flag"
	"fmt"
	"runtime"
	"runtime/debug"
	"text/tabwriter"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// runVersion shows Meridian's version: its module's, the commit it was
// built from, the Go release and FIPS 140-3 module it was built with, and
// the newest plan version it reads and version of the worker API it speaks.
func runVersion(_ context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) > 0 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "version"})
	}
	version, commit := buildVersion()
	fips := "not in FIPS 140-3 mode"
	switch {
	case fips140.Enforced():
		fips = fips140.Version() + ", enforced"
	case fips140.Enabled():
		fips = fips140.Version() + ", enabled"
	}
	fmt.Fprintf(c.stdout, "meridian %s\n", version)
	tw := tabwriter.NewWriter(c.stdout, 0, 0, 1, ' ', 0)
	if commit != "" {
		fmt.Fprintf(tw, "  commit:\t%s\n", commit)
	}
	fmt.Fprintf(tw, "  go:\t%s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(tw, "  fips 140-3 module:\t%s\n", fips)
	fmt.Fprintf(tw, "  plan version:\t%d\n", plan.Version)
	fmt.Fprintf(tw, "  worker API version:\t%d\n", workerapi.Version)
	tw.Flush()
	return exitOK
}

// buildVersion returns Meridian's version, its module's, and the commit it
// was built from, if the build says.
func buildVersion() (version, commit string) {
	version = "(devel)"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version, ""
	}
	if info.Main.Version != "" {
		version = info.Main.Version
	}
	modified := false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			commit = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if commit != "" && modified {
		commit += " (modified)"
	}
	return version, commit
}
