package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/worker"
	"github.com/parallelworks/meridian/workerapi"
)

// runWorker is meridian worker, which a backend starts to run one attempt
// of a job (worker.Run). It takes no arguments: where the coordinator is,
// and the attempt's bootstrap token, come from its environment, as
// workerapi.EnvCoordinator, EnvKeySHA256, and EnvBootstrapToken or
// EnvBootstrapTokenFile, never from its command line, where other users
// could read the token. It logs what it does as JSON on standard error.
func runWorker(ctx context.Context, c *cli, fs *flag.FlagSet, args []string) int {
	positional, err := parseFlags(fs, args)
	if err != nil {
		return flagError(c, fs, err)
	}
	if len(positional) > 0 {
		return c.failf(problems.ArgumentsInvalid, problems.Params{"command": "worker"})
	}
	version, _ := buildVersion()
	cfg := worker.Config{
		Worker: workerapi.Worker{Version: version},
		Log:    slog.New(slog.NewJSONHandler(c.stderr, nil)),
	}
	if err := worker.Run(ctx, cfg); err != nil {
		return c.fail(err)
	}
	return exitOK
}
