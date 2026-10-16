# Meridian

**Durable workflows, from a laptop to a supercomputer.**

Meridian runs YAML workflows in a format modeled on GitHub Actions: jobs made
of steps, with dependencies, matrices, inputs, outputs and `${{ }}`
expressions. One static binary runs a workflow on your machine, across hosts
it reaches over SSH, or through a Slurm or PBS scheduler, with no account,
database server or network service to set up. The same workflow file runs
unchanged on
[Parallel Works ACTIVATE](https://parallelworks.com), which is built on Meridian.

```yaml
on:
  execute:
    inputs:
      steps:
        label: Time steps
        type: number
        default: 1000

jobs:
  build:
    steps:
      - run: make
  simulate:
    needs: [build]
    strategy:
      matrix:
        mesh: [coarse, fine]
    steps:
      - run: ./simulate --mesh ${{ matrix.mesh }} --steps ${{ inputs.steps }}
  report:
    needs: [simulate]
    steps:
      - run: ./report
```

```console
$ meridian run ./workflow.yaml
```

## Goals

1. **Nothing to install but Meridian.** `meridian run ./workflow.yaml` runs a
   workflow with no account, database server or network service.
2. **One workflow format.** A workflow runs unchanged on Meridian and on
   ACTIVATE. Only what stands behind it differs: where inputs, secrets and
   capacity come from.
3. **Durable on a small cluster.** From one host or a login node, Meridian runs
   jobs across SSH hosts or a Slurm or PBS scheduler, and a run survives
   Meridian restarting, a host rebooting and the network dropping.
4. **A library first.** Meridian is a set of Go packages, and the `meridian`
   command is a thin program on top of them. ACTIVATE embeds the same packages
   and plugs its platform features, such as sessions, organization variables
   and shared capacity, into Meridian's interfaces.
5. **CI/CD.** Meridian grows into a CI/CD system, first for secure and HPC
   environments: triggers, reporting to GitHub and GitLab, artifacts, caches
   and concurrency.

## Principles

- **The workflow format is the contract.** Existing workflows keep working.
  New keys are additions; a change to what an existing workflow means waits
  for a new `format:` number, which a workflow opts into. A workflow without
  `format:` is format 1, and every release runs every earlier format.
  Everything behind the format, from the compiler to the scheduler and the
  executor, is free to change.
- **Self-describing.** A workflow file carries its own name, description and
  images, inline or referenced from files beside it, so a workflow in a Git
  repository needs nothing else to be listed, shared or run.
- **Strict.** A workflow that needs something the current environment can't
  provide, such as a key only a platform understands, fails to compile. There
  is no flag to run it anyway.
- **Durable.** A run is stored before anything executes. Each attempt of a job
  holds a lease, and a worker that has lost its lease can no longer report a
  result. Execution is at least once: steps are not assumed idempotent, so
  retries are explicit. State lives in
  [hopper](https://github.com/parallelworks/hopper): SQLite on one machine,
  Postgres when several share it.
- **Secure by default.** Jobs run in rootless containers, except under a batch
  scheduler, where they run on the host it allocates. A missing container
  runtime is an error, never a fallback to the host. For untrusted code, a
  group can run each job in a microVM: Kata Containers with Cloud Hypervisor on
  Linux, Apple's container framework on macOS. Workers only connect out and
  receive a credential scoped to one attempt, and secrets are masked in logs.
- **FIPS 140-3.** Releases are built with Go's validated FIPS 140-3
  cryptographic module (`GOFIPS140=certified`) and run under
  `GODEBUG=fips140=only`, SSH included.
- **Works disconnected.** Meridian never calls home. Images come from the
  registry you name and `uses:` from your own Git server.
- **Auditable.** Every attempt records the image, executor and workflow
  revision it ran. Releases are signed with keys that verify offline, and ship
  an SBOM and SLSA provenance.
- **Fast.** As little as possible stands between a job becoming ready and its
  first step running. Compile time, scheduling latency and throughput are
  benchmarked, and the benchmarks gate releases.
- **Standards, not inventions.** JSON Schema for the workflow format, OCI
  images, OpenTelemetry traces and metrics, and RFC 9457 problem details for
  errors.
- **Idiomatic Go.** The standard library first; `context`, `log/slog` and
  iterators; small interfaces and few dependencies.
- **One implementation.** Editors and web UIs validate workflows and evaluate
  expressions with a TypeScript package built from the same Go source, as
  WebAssembly, so what they accept is what runs.

## Where jobs run

A job runs on an execution group: a named source of capacity with one backend
and the resources it offers. A job names its group and the resources it needs.

| Backend | Runs a job | Isolated by default in |
| --- | --- | --- |
| `local` | on this machine | a rootless container |
| `ssh` | on hosts reached over SSH | a rootless container |
| `kubernetes` | as a pod | a rootless container |
| `slurm`, `pbs` | as a batch job submitted from a login node | the host |

A job that names no image runs in `ghcr.io/parallelworks/meridian-base`.

## License

AGPL-3.0. Contributions require a contributor license agreement.
