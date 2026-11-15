# Slurm's output, captured

What Slurm's commands print, and their exit statuses, on real one-node
clusters, which the backend's parsers, and the fake cluster of
`internal/schedtest`, follow. Each directory is one release:

| Directory | Release | Image | Accounting | cgroups |
| --- | --- | --- | --- | --- |
| `21.08.5` | Ubuntu's `slurm-wlm` 21.08.5 | `ubuntu@sha256:076e7bf82319619f28dd0aa9c02ab34b055ad37100395b4f49a2ffba04fb5e11` (22.04) | none | `proctrack/linuxproc` |
| `23.11.4` | Ubuntu's `slurm-wlm` 23.11.4 | `ubuntu@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852` (24.04) | none | v2 |
| `23.11.4-accounting` | the same | the same | `slurmdbd`, MariaDB | v2 |
| `24.11.5` | Debian's `slurm-wlm` 24.11.5 | `debian@sha256:9c6e381421d0f48d1b8133e573f33c185ee16cbf505eda9c4a8a4791b25ea8a6` (trixie) | none | v2 |

## Capturing them again

From `capture/`, as a user who runs Docker:

```sh
./run.sh ubuntu-22.04 ../21.08.5 noacct
./run.sh ubuntu-24.04 ../23.11.4 noacct
./run.sh ubuntu-24.04 ../23.11.4-accounting acct
./run.sh debian-trixie ../24.11.5 noacct
```

`run.sh` builds the image `Dockerfile.<name>` describes, from the base
image above, pinned by its digest, with the distribution's packages as
its mirrors have them that day (`dpkg.txt` says which were installed);
runs `capture.sh` in a container of it, `--privileged --init --hostname
n1`; and copies what it captured out. `capture.sh` starts munged,
slurmctld and slurmd as root, with the `slurm.conf` beside the output
(`cgroup.conf` too where cgroups were used), runs each command as the user
`alice`, uid 1001, and writes:

- `commands.txt`, each capture's command line;
- `NN-name.stdout`, `.stderr` and `.exit`, what it printed and its exit
  status, and `.stdin`, its standard input; a file that would be empty
  was left out;
- `JOBS.txt`, what each job was;
- `05-job-env.txt` and `05-job-cgroup.txt`, a batch job's environment and
  its `/proc/self/cgroup`;
- `NOTES.txt`, where something didn't go as the script meant.

`./run-auth.sh ubuntu-22.04 ../21.08.5-auth` and `./run-auth.sh
debian-trixie ../24.11.5-auth` capture the token cases (below).

Nothing in them was edited. These were captured on 2026-11-06.

## What they show

- `--nodes=2 --ntasks=1` gets one node: sbatch says it can't run 1
  process on 2 nodes (`06j`, `06l`); `--ntasks-per-node=1` keeps two
  (`06k`, `06m`).
- squeue's short options fail under `POSIXLY_CORRECT` (`11c`), and its
  long ones with `=` don't (`11d`).
- A hidden partition, and its jobs, are listed only with `--all` (`03`,
  `03b`, `10`, `10b`).
- 23.11 without accounting says `InvalidAccount` of a job that waits for
  CPUs, and then runs it (`23.11.4/07a`).
- A drained node's job pends for `ReqNodeNotAvail`, not
  `PartitionNodeLimit` (`16b`). A down node's wasn't captured.
- scancel says nothing, and exits 0, of a job that runs, has ended, or
  that the cluster doesn't know (`06g`, `14a`–`14c`).

## Tokens and munge

`capture/capture-auth.sh`, which `capture/run-auth.sh` runs as `run.sh`
runs `capture.sh`, captures `21.08.5-auth` and `24.11.5-auth`: a cluster
whose controller also takes JWTs (`AuthAltTypes=auth/jwt`) where the
release's packages have `auth_jwt.so`, as Ubuntu 22.04's do and Debian
trixie's don't. A job prints its environment with a token's value as
`<token>`, and `commands.txt` writes the token `$TOK`, so no token is
kept. They show:

- `--export=ALL` gives a job sbatch's environment, a token among it
  (`01`, `03`).
- Any `--export` with a variable after `ALL` copies every `SLURM_`
  variable of sbatch's back over what it says, a token among them, and
  has the job read the user's login environment, `SLURM_GET_USER_ENV=1`
  (`02`, `04`): Slurm's `env_merge_filter`. No `--export` keeps a token
  out of a job.
- squeue takes a token (`05`).
- Without munge, sbatch and squeue can't authenticate (`06`, `07`), but
  for a token (`08`).

So the backend never gives sbatch a token, and a login node where sbatch
can't do without one is `scheduler_token_required`.

Debian 12's 22.05.8 isn't here: its slurmd needs systemd, over D-Bus, to
make the cgroup v2 scope of its steps, which a container without systemd
doesn't have.
