# Contributing

Thanks for helping build Meridian. The rules that apply to every change are in
[AGENTS.md](AGENTS.md); this file covers the mechanics.

## Contributor license agreement

Contributions from outside Parallel Works need a signed contributor license
agreement before they can be merged.

## Local setup

Meridian needs Go, the version in `go.mod`, which the `go` command fetches
unless `GOTOOLCHAIN=local`. Development tools are pinned in `tools/go.mod` and
run with `go tool`, so there is nothing else to install, except a C compiler
on Linux for `make test-race`.

```sh
make check  # tidiness, lint, vulnerabilities, cgo, tests and a static build
make fmt    # format Go code
```

## Pull requests

- Branch from `canary` and open the PR against `canary`.
- Write the PR title as a [Conventional Commit](https://www.conventionalcommits.org)
  (`feat(compiler): ...`, `fix(ssh): ...`, `docs: ...`). PRs are squash-merged,
  so the title becomes the commit message.

## Testing

```sh
make test                      # all tests
make test-race                 # all tests, with the race detector
make test-openssh              # the ssh backend's tests, against OpenSSH's sshd
GOFIPS140=certified go test -run TestName ./...    # one test
```

The ssh backend's tests run against an SSH server in the test's process.
`make test-openssh` runs them against OpenSSH's sshd too, as CI does: each
test starts the sshd that `SSHD` names, `/usr/sbin/sshd` by default, as you,
on a port of the loopback interface, with keys and a config of its own.

Tests run in FIPS 140-3 mode: `GODEBUG=fips140=only` comes from `go.mod`, and
`make` builds against the certified module (`GOFIPS140=certified`), as releases
are. `make` also builds without cgo (`CGO_ENABLED=0`), except for the race
detector, which needs cgo on Linux; nothing built with it ships. A plain
`go test` uses the toolchain's newer FIPS module, and cgo when a C compiler is
installed, so use `make` to match CI. The `meridian` command, and the worker,
refuse to run with a FIPS module that isn't the validated one, so the tests
of `cmd/meridian` fail under a plain `go test`: set `GOFIPS140=certified`, as
`make` does, or `go env -w GOFIPS140=certified` once. If a test fails with a
FIPS error, the fix is an approved algorithm, not a different flag.
