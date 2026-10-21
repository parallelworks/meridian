# Stories

These are things people need to do that [the design](design.md) doesn't cover
yet. Each story says what someone wants, what they do today, how Meridian
could do it, and what is still open. When a story is decided, its design moves
into the design document and the story leaves this one.

Most proposals add keys to the workflow format. Each is an addition, so format
1 gains it without a new number ([The workflow format](design.md#the-workflow-format)).

## Running existing code on an HPC site

**Story.** Someone has a Python script or a Jupyter notebook that works on
their laptop. They want it to run on an HPC site with one command, without
writing a workflow, copying files by hand or rebuilding their environment
there.

**Today.** The code has to be copied to the cluster, and its environment
rebuilt there by hand, before a workflow can run it.

**Proposal.**

```console
$ meridian run train.py --group hpc --gpus 1 --time 2h -- --epochs 10
$ meridian run analysis.ipynb --group hpc --cpus 32 -p alpha=0.5
$ meridian notebook analysis.ipynb --group hpc
```

- **The code.** Meridian sends the project: the Git repository the file is
  in, or its directory. It leaves out what `.gitignore` excludes and sends
  files by content hash, so a second run sends only what changed. A project
  over a size limit is refused, with a message to keep its data on the site.
- **The environment** is built on the site from what the project declares, in
  this order: `uv.lock` or `pyproject.toml`, `requirements.txt`,
  `environment.yml` or `pixi.toml`, or an image named with `--image`. Python
  environments are built with uv and conda ones with pixi, with no root and no
  container build on a login node. Packages are resolved for the site's
  operating system, CPU and CUDA, not the laptop's. The environment is cached
  on the member by the hash of its lockfile, so the next run starts in
  seconds.
- **Flags** set the job's `resources:`, its `timeout:`, which is its walltime,
  and its group. Fallback between the group's members works as for any job.
- **Notebooks** run every cell in order, and the executed notebook comes back
  with its outputs. Parameters use papermill's convention, a cell tagged
  `parameters`. `meridian notebook` instead starts Jupyter in an allocation
  and opens it in the local browser through Meridian's connection to the
  member, until the browser closes or the walltime ends.
- **Results.** Logs stream to the terminal, and the command can detach like
  `meridian run`. Files the code writes inside the project come back when the
  run ends, or with `meridian fetch <run>`.
- **Saving it.** `--save run.yaml` writes the workflow Meridian generated: a
  format 1 workflow like any other, to version, share and extend. Its
  environment and notebook steps are built-in [actions](#reusable-steps), such
  as `parallelworks/python-env` and `parallelworks/notebook`, so the file
  stays short.

**Open.**

- The project when the file isn't in a Git repository, and the size limit.
- Code that reads files outside the project. Meridian doesn't rewrite code,
  so it can only list the files it sent.
- Languages other than Python. Their environments are built the way
  [other languages'](#other-languages) tasks are.

## Many small tasks on HPC

**Story.** A parameter study runs 10,000 cases of a few minutes each, or
100,000 of a few seconds. Submitting each as its own scheduler job floods the
scheduler, can exceed the site's limits on jobs, and makes every case wait in
the queue on its own.

**Today.** Meridian places each matrix instance as its own job
([Placement](design.md#placement)), so a large matrix becomes as many
scheduler jobs.

**Proposal.** A Slurm or PBS member runs a batch of instances in either of two
ways, and Meridian supports both.

- **Job arrays.** Instances of a job that go to the same member are submitted
  as one array (`sbatch --array`, `qsub -J`), split at the site's
  `MaxArraySize`. On Slurm, `max-parallel` becomes the array's `%` limit.
  - Each array task starts a worker that claims its instance by array index.
    The array's name carries the batch's ID, and the coordinator records
    which attempt each index runs, so placement stays idempotent and fencing
    still holds.
  - An instance that fails before it starts is placed again on its own,
    possibly on another member.
  - Each task is still a scheduler job. It is accounted for separately and
    shares fairly with other users, but it waits in the queue on its own.
- **Allocations that run many tasks.** Meridian requests an allocation, such
  as 4 nodes for 4 hours, and starts a worker in it that claims instances
  until none are left. Then the allocation ends.
  - On Slurm, each task runs as a job step (`srun --exact`), so Slurm gives
    it its own cores and GPUs and accounts for it. On PBS, the worker pins
    each task's cores and sets `CUDA_VISIBLE_DEVICES` itself.
  - A worker only takes a task whose `timeout:` fits in what is left of the
    allocation's walltime.
  - There is one queue wait for thousands of tasks, and tasks of a few
    seconds are cheap. The cost is cores left idle while the last tasks
    finish, and accounting per allocation rather than per task.
- **Who chooses.** A member's configuration says which of the two the site
  allows or prefers. Meridian chooses per batch, from the number of instances
  and the job's `timeout:`: arrays for tasks of minutes to hours, an
  allocation for many short ones. A job can override the choice in
  `strategy:`. A single instance is placed as a job of its own, as now.

**Pools.** An allocation that runs many tasks is a pool: workers that keep
running on an execution group and claim work as it comes. The
[Python API](#code-that-is-the-workflow) runs its tasks on the same pools. A
pool's worker holds a pool credential, valid while the allocation lasts, that
can claim only the attempts placed in that pool. An attempt's credential still
covers one attempt ([Starting a worker](design.md#starting-a-worker)).

**Open.**

- The key a job overrides the choice with, and its options.
- The thresholds between arrays and allocations, and whether one pool can
  serve several runs of the same user.
- Job retries in a pool: whether a lost instance waits for the pool or is
  placed on its own.

## Workflows written in code

Some people want to write workflows in a programming language rather than
YAML. Some of them want code that writes a workflow file, and some want their
program to be the workflow.

### Code that builds a workflow

**Story.** A team generates its workflows: the same pipeline for twenty
instruments, or jobs built from a table. They want types, functions, loops and
tests, and a workflow file like any other at the end.

**Proposal.** Libraries for Python and TypeScript, with types generated from
the format's JSON Schema, that build a workflow and write it out. A JSON
document is also YAML, so Meridian reads what they write as it is. The file is
still the contract, so nothing after it knows how it was made.

### Code that is the workflow

**Story.** Researchers write Python with Parsl, Dask or Ray: functions that
return futures, run in parallel, and depend on each other through their
arguments. They want those functions to run on their HPC allocations, across
sites, with the records and recovery that Meridian's runs have.

**Today.** They start each framework's workers on each cluster themselves,
connect the workers back to their program across firewalls, and tear them
down afterwards.

**Proposal.** A Python API in Meridian itself, rather than adapters for each
framework:

```python
import meridian as mr

@mr.task(cpus=4, memory="8Gi", timeout="10m")
def simulate(case: dict) -> float: ...

@mr.task(gpus=1, timeout="2h")
def train(scores: list[float]) -> str: ...

@mr.task()
def solve(case_dir: str):
    return mr.sh(f"./solver -i {case_dir}/input.dat")

with mr.run("sweep", group="hpc"):
    scores = simulate.map(cases)  # many small tasks
    model = train(scores)         # a future passed as an argument is a dependency
    print(model.result())
```

- **A task is a job.** `mr.run` is a Meridian run, and each call of a task
  adds a job to it. The job has the same records, placement, fallback,
  [pools and arrays](#many-small-tasks-on-hpc), and `meridian status`, `logs`
  and `cancel` as a job from a workflow file. What differs is when jobs are
  added: the program adds them while the run is going, through the driver
  API, instead of the compiler emitting them all up front. Each job is checked
  as it is added, as a compiled one is: its resources, its group and the
  run's limits.
- **Durable without replay.** Each task has a key: a hash of its function and
  arguments, or one given with `key=`. Running the program again with the
  same run name returns the stored results of tasks that finished and submits
  only the rest. The program can stop, or its laptop sleep, while tasks run;
  their results wait in the run.
- **Where the program runs.** On the laptop, with its tasks on HPC: their
  workers connect out to Meridian as any worker does, so nothing has to accept
  connections. Or on the site, with `meridian run driver.py --group hpc`
  ([Running existing code](#running-existing-code-on-an-hpc-site)). In a
  notebook, functions defined in a cell are sent by value with cloudpickle.
- **The environment** is built as it is for
  [existing code](#running-existing-code-on-an-hpc-site), which also gives the
  program and its tasks the same Python and packages, as pickling needs.
- **Data.** Arguments and small results are stored with the task, capped as a
  job's outputs are. Large results are written to files in the run directory
  and returned as references, and `mr.File` names a file to copy to a task's
  member. A program only loads results that its own run produced.
- **Workflows and tasks mix.** `mr.workflow("github/org/repo/sim.yaml@<sha>")`
  returns a callable whose result is a future of the workflow's outputs. A
  workflow step can run a program whose tasks join that step's run.
- **Not for fine-grained work.** Each task is a stored record and at least one
  process, so tasks should take a second or more. Task throughput joins the
  benchmarks that gate releases ([Operations](design.md#operations)). Dask
  arrays and dataframes belong inside a task, which can run a Dask cluster
  across its own node.
- **Checked as it goes.** A run that a program builds isn't checked as a whole
  before it starts, only each task as it is added. Workflow files stay the way
  to define something reviewed and reused; programs are for exploration and
  for logic that depends on results.

Porting from Parsl is mostly renaming: apps become tasks, a `bash_app` becomes
`mr.sh`, and futures are passed as dependencies the same way. Adapters for
Parsl, Dask or Ray can come later, if people ask for them.

### Other languages

What depends on a language lives in that language's library. The worker stays
a Go binary that runs commands.

- **Shared, and specified once in Meridian:**
  - **The driver API**: HTTP with JSON, versioned like
    [the worker API](design.md#the-worker-api) and described in OpenAPI. It
    adds tasks in batches, waits for results, cancels, and calls workflows.
  - **The task protocol**: how a worker runs a task. It runs a command with
    the task's function, arguments and key in a file, and reads the result
    from a file, as steps write `$OUTPUTS` now.
  - **Shell tasks** are commands, so a library for a new language can start
    with the API alone.
  - **A conformance suite for libraries**, run against a local Meridian, so
    that every language behaves the same.
- **Written for each language:**
  - a client for the driver API, mostly generated from its OpenAPI
    description;
  - a task runner, such as `python -m meridian.task`, that loads a function
    in the task's environment, runs it and writes its result;
  - an environment recipe: `uv.lock` for Python, and `pnpm-lock.yaml`,
    `package-lock.json` or `bun.lock` for TypeScript, with a pinned runtime
    fetched where a site has none.
- **How functions travel** differs. Python sends a function by value.
  TypeScript can't send closures, so a task is an exported, top-level function,
  found by name in the project Meridian sends, and the program's own code runs
  in a guarded `main`, so that loading the module on a worker doesn't run the
  program again. Go can compile the user's program for the site into one
  static binary that is both the program and its task runner.
- **Encoding.** Arguments and results are JSON by default, with large data in
  files passed by reference, which also lets one language call tasks written
  in another. When both ends use the same language, a library can use that
  language's own encoding, such as pickle, tagged with a content type.

Python comes first, for research and HPC users, then TypeScript. R
(`renv.lock`) and Julia (`Manifest.toml`) need only a task runner and an
environment recipe.

**Open.**

- How long a run that a program builds stays open for new tasks, and what ends
  it.
- What a task's key covers, so that a change to a function's code makes its
  stored results stale.
- How the plan encodes jobs added after the run starts
  ([The plan](design.md#the-plan)).
- Whether the libraries live in this repository, so that the API, the
  libraries and their conformance suite change together, or in their own.

## Decisions

**Story.** A run should stop and ask. A person checks the mesh before the
expensive simulation, chooses which candidate designs go on, or approves a
deployment.

**Today.** A workflow can only wait inside a step, polling for a file or a
service. The step holds its worker, and on Slurm or PBS an allocation, for as
long as it waits, and nothing offers a person a choice.

**Proposal.** A job that asks a question instead of running steps:

```yaml
jobs:
  review:
    needs: [mesh]
    decision:
      prompt: The mesh has ${{ needs.mesh.outputs.cells }} cells. Run the full simulation?
      inputs:
        proceed:
          type: boolean
          label: Run it
        nodes:
          type: number
          step: 1
          min: 1
          default: 8
    timeout: 2d
  simulate:
    needs: [review]
    if: ${{ needs.review.outputs.proceed }}
```

- **Waiting costs nothing.** A decision job is stored, with no worker and no
  allocation, and survives restarts like any job. It adds a `waiting` state
  between `ready` and the job's end
  ([A job's lifecycle](design.md#a-jobs-lifecycle)).
- **The question is made of inputs.** They use the same definitions as
  `on.execute.inputs`, so a platform can show them with the form it uses to
  start a run, and an answer is checked the way inputs are when a run starts.
  The answers become the job's outputs.
- **Answering.** Through the API, with
  `meridian decide <run> review proceed=true nodes=16`, or at a prompt when
  `meridian run` is in a terminal. The run records who answered, when, and the
  reason they gave.
- **No answer is assumed.** When the job's `timeout:` passes, it fails, and
  the workflow decides what follows with `needs.review.result`.
- **Notifications.** The Events interface announces a job that is waiting, so
  a platform can send an email or a chat message
  ([Interfaces](design.md#interfaces)).

**Open.**

- The key's name: `decision:`, `ask:` or `await:`.
- Whether hopper can hold a job that only an outside answer completes, or this
  is something hopper doesn't have yet ([On hopper](design.md#on-hopper)).
- Who may answer. Meridian has no user accounts, so a platform decides, but a
  workflow may need to ask for someone other than the person who started the
  run.

## Decisions by agents

**Story.** An AI agent reviews results and decides whether a run converged,
which candidates continue, or whether to try other parameters, and a person
can overrule it.

**Proposal.** A [decision](#decisions) with a different answerer. The workflow
says what is decided and which answers are allowed. Whoever runs it chooses
who answers, as they choose execution groups: a person, an agent, a policy
service, or an agent whose answer a person can overrule.

- **An agent is bounded by the inputs.** It can only give answers that the
  decision's inputs allow, checked as any answer is.
- **The record says who answered**: that it was an agent, which one, and the
  reason it gave.
- **Failure goes to a person.** When an agent errors or takes too long, the
  decision goes to a person or fails, as configured, and never to a default
  answer.
- **An agent that does work is a step.** Writing code or proposing the next
  parameters is a step like any other, and needs nothing new.

**Open.** The interface that routes a decision to whoever answers it, and how
an agent gets what it needs, such as the run's outputs and logs, without being
given more than that.

## Waiting on outside events

**Story.** Process a dataset once the instrument has written all of it.
Continue once an external system approves. Start when a provider publishes new
data.

**Today.** A step polls, holding its worker or allocation while it waits.

**Proposal.**

- **Answered from outside.** A [decision](#decisions) that a service answers
  through the API, with a credential that can answer that decision and
  nothing else.
- **Checked from inside.** A job that runs a short check until it succeeds:

  ```yaml
    data-ready:
      wait:
        run: test -f /data/run42/DONE
        every: 5m
      timeout: 2d
  ```

  Each check runs as a short step on the member, with the member's isolation
  and outside any allocation: on a Slurm or PBS member, that is the login
  node. Nothing is held between checks. Airflow calls this a sensor in
  reschedule mode.

**Open.** How checks run on Kubernetes members, and the shortest interval a
member allows.

## Loops

**Story.** An optimization proposes a batch of cases, evaluates them, and
repeats until it converges, up to 20 times. Each iteration starts from the
results of the one before.

**Today.** Workflows loop by failing. A called workflow exits 1 to ask for
another iteration, and a step's `retry:` runs it again, so each iteration
looks like a failure, and retries mean something else.

**Proposal.** A [job that calls a workflow](#reusable-workflows) can repeat
it:

```yaml
  optimize:
    uses: ./iteration.yaml
    with:
      state: ${{ loop.previous.outputs.state || inputs.initial }}
    repeat:
      until: ${{ loop.outputs.status == 'CONVERGED' }}
      max: 20
```

Each iteration is a child run with its own records. `loop.index`,
`loop.previous` and `loop.outputs` are known for each iteration, and the job's
outputs are the last iteration's. A program written with the
[Python API](#code-that-is-the-workflow) loops in Python.

**Open.** Whether reaching `max` without meeting `until` is a failure, and the
context's name.

## Smart routing

**Story.** A job should go where it will start soonest, cost least, or find
its data, judged by what is known about each site's queues and allocations
right now.

**Today.** Placement ranks a group's members by whether they can start the job
now, have capacity for it, or would queue it, and then by the group's order
([Placement](design.md#placement)).

**Proposal.** A Placement interface that ranks a job's eligible members and
gives a reason for each.

- Meridian's ranking stays the default. A platform can plug in its own, from
  measured queue waits, allocation balances, prices or where data is, or from
  an AI model.
- A ranking can only reorder the members that the group allows and that
  passed the filter. It can't add a member or skip the filter.
- Its reasons go in the attempt's record ([Operations](design.md#operations)).
- When it fails or is slow, Meridian uses its own order.
- The same predictions can answer how long a job may wait in a queue before it
  moves to another member ([Open questions](design.md#open-questions)).

**Open.** What the interface is told about the job and the members, and how
long Meridian waits for an answer.

## Reusable workflows

**Story.** Teams build workflows from other workflows: a submitter, a data
transfer, a session. They want to pass data back from what they call, catch
mistakes in what they pass to it, and know which version ran.

**Today.** A step calls a workflow with `uses:`. About 400 steps in public
Parallel Works workflows do, and none of them reads outputs from the workflow
it calls, since a workflow has none: results pass through files in the run
directory. `with:` takes any keys. References to repositories name a branch,
such as `@main`, so two runs of a workflow can run different code.

**Proposal.**

- **Outputs.** A workflow can declare top-level `outputs:`, mapped from its
  jobs' outputs, and a step that calls it reads them as `steps.<id>.outputs`.
- **Checked `with:`.** The compiler loads a workflow that a step calls when
  the run is created
  ([Workflows a step calls](design.md#workflows-a-step-calls)), so it can
  check `with:` against that workflow's inputs. An unknown key or a wrong
  type is an error at its line.
- **Jobs that call workflows.** A job can have a `uses:` of its own. The
  called workflow's jobs join the run's graph and are placed like the
  caller's, and no worker waits for them. This answers steps that run a
  workflow ([Open questions](design.md#open-questions)).

**Open.** Whether a reference to a branch is a warning, given how many format
1 workflows use one, and how the called workflow's jobs are named in the
caller's run.

## Reusable steps

**Story.** Several workflows repeat the same steps: staging data in, setting
up an environment, registering a service. Their authors want to write those
steps once, with inputs and outputs, and use them inside a job.

**Today.** Built-in actions, such as `parallelworks/checkout`, are reusable
steps, but a workflow can't define its own. A workflow that a step calls runs
jobs of its own rather than steps in the caller's job.

**Proposal.** An action: a file with inputs, outputs and steps that runs
inside the job that uses it, as GitHub's composite actions do.

```yaml
# actions/stage-in/action.yaml
inputs:
  source:
    type: string
outputs:
  path: ${{ steps.copy.outputs.path }}
steps:
  - id: copy
    run: ./stage-in.sh "${{ inputs.source }}"
```

```yaml
      - id: stage
        uses: ./actions/stage-in
        with:
          source: s3://bucket/data
```

- **In the caller's job.** An action's steps run on the same worker, in the
  same container and run directory. The compiler inlines them into the plan,
  so they cost nothing at run time, and an error in one points at the
  action's file and line.
- **Where it comes from.** A path relative to the file that uses it, or a
  reference to a repository, pinned as a
  [called workflow](#reusable-workflows) is. The worker checks out an action's
  files at its commit, so its steps can run the scripts that come with it.
- **Built-in actions** that need no platform can be written as actions
  themselves.

**Where paths resolve.** Meridian resolves every `uses:` through the Sources
interface, relative to the source of the file that contains it. An action in a
Git repository resolves its own paths in that repository, whoever calls it. A
platform that stores a workflow as a single document, with no files around
it, has no directory to resolve a path against, and the same is true of the
workflow's [metadata](design.md#the-workflow-format) paths. There, a relative
path is an error that suggests a reference the platform can resolve, such as
an action it stores by name, unless the platform stores a workflow's files
with it.

**Open.** The action file's name, and how a reference tells an action from a
workflow.

## Triggers

**Story.** Run the regression suite every night at 02:00 local time. Process
each new dataset as it arrives. Build the report when the ingest run
succeeds.

**Today.** Format 1 has one trigger, `on: execute`: a person or a program
starts the run.

**Proposal.**

```yaml
on:
  execute:
    inputs: {}
  schedule:
    - cron: "0 2 * * *"
      timezone: America/Chicago
      inputs:
        suite: full
```

- **Schedules.** `on.schedule` lists cron expressions, each with a time zone
  and the inputs to run with.
- **Other runs.** `on.run-completed` starts a run when a named workflow's run
  ends with a given result, with that run's outputs available as inputs.
- **Platform events**, such as a push or a new object in storage, come through
  a Triggers interface.
- **Who starts them.** `meridian serve` runs schedules, with hopper's cron. A
  platform maps its own events.
- **The record.** A triggered run records its trigger and the revision of the
  workflow it ran. When the previous run is still going, the workflow's
  [limits across runs](#limits-across-runs) decide what happens.

**Open.** Whose identity a triggered run uses, and which revision of the
workflow it runs: the one current when the trigger was set up, or the newest.

## Limits across runs

**Story.** Only one deployment to production at a time, with a newer one
replacing one that is waiting. At most three of a person's sweeps on an
allocation at once. Ten solver jobs at a time across every run, because the
site has ten licenses.

**Today.** Each run is scheduled on its own, and nothing limits runs against
each other.

**Proposal.**

- **`concurrency:`** on a workflow or a job names a group, built with an
  expression, a `max`, and what happens to a run or job that would exceed it:
  it waits, it replaces the one that is waiting, or it cancels the one that is
  running.

  ```yaml
  concurrency:
    group: deploy-${{ inputs.environment }}
    max: 1
    exceeded: replace-waiting
  ```

- **Licenses are resources.** `resources: { licenses: { solver: 1 } }` becomes
  Slurm's `--licenses` where the site defines that license, and Meridian
  counts it otherwise.
- **hopper's global concurrency limits** enforce both.

**Open.** The names, and how a limit holds across runs that different
coordinators serve.

## Artifacts, files between members, and caches

**Story.** Keep the plots and the final mesh after the run, and download them
later. Give a job on one cluster the files that a job on another cluster
wrote. Reuse a 40-minute Spack build across runs.

**Today.** Files stay in the run directory on the member where they were
written ([Running steps](design.md#running-steps)). Jobs on members without a
shared filesystem don't see each other's files, and Meridian keeps nothing
from a run but its logs and outputs.

**Proposal.**

- **Artifacts.** A job's `artifacts:` lists paths to keep, and for how long.
  They are stored through an Artifacts interface, in Meridian's state
  directory locally or in a platform's storage, and their digests go in the
  attempt's record. `meridian fetch` downloads them.
- **Files between members.** A job can name artifacts of the jobs it needs,
  and Meridian copies them to its member before it starts. This answers files
  between members ([Open questions](design.md#open-questions)).
- **Caches.** `cache:` saves and restores a path under a key made from a name
  and the hashes of files that the worker reads, as GitHub's cache does, and
  keeps it on the member's own filesystem, so nothing crosses sites. The
  environments for [existing code](#running-existing-code-on-an-hpc-site) are
  caches.

**Open.** Retention and size limits, and whether users of a site can share a
cache.

## Debugging

**Story.** A job failed on the cluster at its third step. Its author wants a
shell in the same container, environment and directory to see why, and then
to run just that job again with a fixed input.

**Today.** `meridian rerun` reruns a run, or its failed jobs, with the same
plan ([Reruns](design.md#reruns)). Nothing opens a shell where a job ran.

**Proposal.**

- **`meridian debug <run> <job>`** runs the job again as a new attempt, up to
  the step that failed, and holds it there. Or, with `--hold-on-failure`, a
  run keeps a failed job's container or allocation for a set time. Either way,
  a shell opens through the worker's connection to the coordinator, so nothing
  listens on the host. Locally, only the run's user can open one, a platform
  decides who else may, and each shell is recorded.
- **Rerunning with a change** makes a new run from the old one. The jobs it
  doesn't rerun keep their outcomes and outputs, and the new run records what
  changed.

**Open.** Shells on Slurm and PBS sites that don't allow interactive use of
allocations, and how long a held allocation may last.

## Order

1. Reusable workflows and steps. The compiler comes next, and workflows
   already depend on calling each other.
2. Many small tasks, and the pools they share with the Python API.
3. Decisions, for people, agents and services.
4. Running existing code, then the Python API and its driver API.
5. The Placement interface, as a hook, with better rankings later.
6. Triggers, limits across runs, loops, artifacts and caches, and debugging.

## What these add

| Where | Additions |
| --- | --- |
| Format 1 | Actions; a workflow's `outputs:`; a job's `uses:` and `repeat:`; jobs that are a `decision:` or a `wait:`; `on.schedule` and `on.run-completed`; `concurrency:`; licenses in `resources:`; `artifacts:` and `cache:`; the choice between arrays and allocations in `strategy:` |
| Interfaces | Placement, Triggers, Artifacts, and one that routes decisions |
| APIs | The driver API and the task protocol; answering decisions |
| Workers | Job arrays; job steps in allocations; pool credentials |
| hopper | A job that something outside completes; claims from pools |
| Commands | `meridian run` for scripts and notebooks; `meridian notebook`, `decide`, `fetch` and `debug` |
| Libraries | Python, then TypeScript: building workflows, and the Python API |
