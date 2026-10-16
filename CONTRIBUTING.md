# Contributing

Thanks for helping build Meridian. The rules that apply to every change are in
[AGENTS.md](AGENTS.md); this file covers the mechanics.

## Contributor license agreement

Contributions from outside Parallel Works need a signed contributor license
agreement before they can be merged.

## Local setup

Meridian needs Go, the version in `go.mod`, which the `go` command fetches
unless `GOTOOLCHAIN=local`. Development tools are pinned in `tools/go.mod` and
run with `go tool`, so there is nothing else to install.

```sh
make check  # tidiness, lint, vulnerabilities, tests and a static build
make fmt    # format Go code
```

## Pull requests

- Branch from `canary` and open the PR against `canary`.
- Write the PR title as a [Conventional Commit](https://www.conventionalcommits.org)
  (`feat(compiler): ...`, `fix(ssh): ...`, `docs: ...`). PRs are squash-merged,
  so the title becomes the commit message.

## Testing

```sh
make test                      # everything, as CI runs it
go test -run TestName ./...    # one test
```

Tests run in FIPS 140-3 mode: `GODEBUG=fips140=only` comes from `go.mod`, and
`make` builds against the certified module (`GOFIPS140=certified`), as releases
are. A plain `go test` uses the toolchain's newer module instead, so use `make`
to match CI. If a test fails with a FIPS error, the fix is an approved
algorithm, not a different flag.
