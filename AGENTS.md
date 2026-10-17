# Contributing to Meridian

- Every change goes through a pull request against `canary`.
- Commit messages and PR titles follow [Conventional Commits](https://www.conventionalcommits.org)
  (`feat(compiler): ...`, `fix(ssh): ...`, `docs: ...`). PRs are squash-merged, so the
  title becomes the commit.
- The workflow format is the contract. A change to it is an addition; a change to what
  an existing workflow means needs a new `format:` number. Everything behind the format
  is free to be redesigned.
- Code that comes from ACTIVATE is a starting point, not the design. Keep it only where
  it is still the best way to do the job.
- Prefer the standard library. A new dependency needs a reason in its PR.
- cgo is not allowed, in Meridian or in its dependencies: every binary is a pure-Go
  static build (`CGO_ENABLED=0`), and `make cgo-check` enforces it. The race
  detector's tests are the one exception, because the detector needs cgo on Linux.
- Cryptography comes from the standard library, or from `golang.org/x/crypto` where the
  standard library has no equivalent, and uses FIPS-approved algorithms only. Tests run
  with `GODEBUG=fips140=only`, set in `go.mod`, against the certified module
  (`GOFIPS140=certified`). If a test fails with a FIPS error, change the algorithm, not
  the flag.
- Never fall back to weaker isolation or security. When a requirement can't be met, such
  as a missing container runtime or a key the environment can't provide, fail with an
  error that says why.
- Everything here is meant to be public: write nothing that names customers or describes
  unannounced Parallel Works plans.
