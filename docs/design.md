# Meridian design

This document describes how Meridian turns a workflow file into jobs that run
and report back. The workflow format itself is defined by its JSON Schema and
its conformance suite; this document explains what Meridian does with it. A
pull request that makes any part of it wrong updates it.

## Terms

| Term | Meaning |
| --- | --- |
| Workflow | A YAML file in the workflow format. |
| Run | One execution of a workflow with a set of inputs. |
| Job | Steps that run in order, in one place. A matrix job has one instance per combination of its matrix values. |
| Step | A command (`run:`), or another workflow or a built-in action (`uses:`). |
| Attempt | One try at running a job instance. Most job instances have one. |
| Phase | One of the five points in a run at which Meridian evaluates what the workflow leaves to it: Create, Start, Ready, Step and End. |
| Execution group | A named source of capacity: an ordered list of members. |
| Member | One place a group can run jobs: a backend, its settings and the resources it offers. |
| Backend | How a member starts work: `local`, `ssh`, `kubernetes`, `slurm` or `pbs`. |
| Plan | The compiled form of a run: typed, checked, holding no secret, and stored with the run. |
| Secret record | What a run needs that its plan can't hold: what was submitted, passwords among it, what the plan withholds of the input definitions and what Create decided of them, and what Start read and made that is private, the variables among it; encrypted under a data key of the run's own, bound to the run, and kept only until the run ends (`plan.Record`). |
| Coordinator | The part of Meridian that schedules jobs, places them on groups and hears from workers. |
| Worker | The process that runs one attempt's steps and reports on them: the `meridian` binary itself. |

Meridian is not a hosted service, and has no web UI or user accounts; a
platform that embeds Meridian provides those. Running GitHub Actions workflow
files unchanged is not a goal: the format is modeled on GitHub Actions but is
its own.

## Overview

```text
 workflow.yaml + inputs
          │
          ▼
   compile: Create ◀──── resolvers: inputs, variables, groups, sources, …
          │
          ▼
  plan + secret record ───▶ store (hopper, on SQLite or Postgres)
                                ▲
                                │
                           coordinator ──── backend ────▶ starts a worker
                                ▲                              │
                                └──────── worker API ◀─────────┘
                                                        runs the steps
```

The compiler creates a run's plan, the coordinator starts the run and
readies each job, and a worker runs a job's steps and gives its outputs:
the five [phases](#phases).

Meridian is a set of Go packages:

| Package | Does |
| --- | --- |
| `format` | Loads workflow files and validates them against the schema of their format. |
| `expr` | Parses, checks and evaluates `${{ }}` expressions. |
| `compile` | Create and Start: turns a workflow and its inputs into a plan, and completes a run's values as it starts, through resolver interfaces. |
| `plan` | The plan's types, encoding and schema, matrix expansion, and the functions of the phases after Start. |
| `coordinator` | The coordinator, which stores runs, schedules and places their jobs, and serves the worker API, and the interfaces through which it reaches what differs between a laptop and a platform: keys and logs; the timing of its leases; how it encrypts a run's secret record ([The secret record](#the-secret-record)); its store, hopper on SQLite with Meridian's own tables beside it, for runs, their secret records, the texts of the workflows they call, jobs' results, attempts with their leases, fences, events and controls, each run's events in order, and the endpoints workers reach it through; and its server of the worker API ([The worker API](#the-worker-api)). The coordinator creates runs, starts them, places their jobs' attempts on their groups' members, matrices among them, ends them, and lets whoever shows a run follow its events and logs ([Following a run](#following-a-run)) ([The coordinator](#the-coordinator)). |
| `worker` | Runs one attempt of a job and reports on it. |
| `workerapi` | The worker API's paths, requests, responses and problems, which the coordinator serves and the worker calls. |
| `backend` | The interfaces of execution groups and their members, and of what starts workers on a member. |
| `cmd/meridian` | The command. |

The backends that start workers (`local`, `ssh`, `kubernetes`, `slurm`,
`pbs`) and the drivers of container runtimes (Podman, Docker, Apptainer,
Apple's `container`) will be packages of their own; only `local`, which
runs workers on this machine, and `ssh`, which runs them on hosts it
reaches over SSH, are built yet
([Backends and isolation](#backends-and-isolation)).

The `meridian` command wires the packages together with local resolvers.
It runs a coordinator in its own process, with a SQLite store: today it
validates a workflow (`meridian validate`), runs one, its jobs on this
machine with the `local` backend or on hosts with the `ssh` backend
(`meridian run`), shows runs and their logs (`meridian status` and
`logs`), takes up a run whose command stopped (`meridian resume`), cancels
one (`meridian cancel`), runs one attempt of a job as a worker (`meridian
worker`), and runs the local backend on a host for the `ssh` backend
(`meridian host`), and its other commands say they aren't built yet
([The `meridian` command](#the-meridian-command)). A
platform wires them with its own resolvers, Postgres, whose store isn't
built yet, and as many coordinator replicas as it needs. Nothing in the
packages knows which of the two it is running in.

## The workflow format

The format is defined by a JSON Schema (2020-12) per format number, kept in
this repository's `format/schema`. The schemas are under the MIT license
rather than Meridian's AGPL, so that any tool can use them.
Format 1 is the format Parallel Works ACTIVATE runs.

- **Format numbers.** A workflow may set a top-level integer `format:`.
  Without it, a workflow is format 1. A new number is introduced only for a
  change to what an existing workflow means, never for an addition, and every
  release runs every earlier format. Breaking changes are collected into
  rare new numbers rather than given one each. Tools that create a workflow,
  such as `meridian init` and editor templates, write the latest number into
  it.
- **Bug fixes.** A fix for behaviour that is plainly a bug can change what a
  format means without a new number, when a scan of the workflows we know of
  shows that none rely on it. ACTIVATE takes the same fix at the same time,
  so that a format means one thing wherever it runs.
- **Translation.** Each format after the first is defined by how it differs
  from the one before, and comes with a translation from it. The compiler
  translates an older workflow into the current format right after
  validating it, so everything after that, from the rest of the compiler to
  the plan, coordinator and worker, sees one format's keys. A feature a newer
  format removes survives in older workflows as its translation: if a later
  format drops `ssh:`, a format 1 job's `ssh:` becomes an inline execution
  group. Translations chain, so a new format adds one, from the format
  before it. Expressions keep their own format's language: the parser reads
  each in its workflow's format, and evaluation follows the format the plan
  records.
- **Migration.** `meridian migrate` applies the same translations to a
  workflow file, keeping its comments, and says what it can't change in the
  file, such as an execution group to configure.
- **Deprecations.** A key can be deprecated within a format. It keeps its
  meaning, the schema marks it `deprecated`, so editors flag it, and the
  compiler warns. Only a new format number removes it. The schema marks no
  key yet, and the compiler doesn't yet warn about the first,
  [`ssh:`](#ssh-is-deprecated).
- **Additions.** Format 1 gains keys and functions without a new number when
  they change no workflow that was valid before: a key the schema didn't
  allow, or a call that was a syntax error. A word that is text today is
  different. Giving it a meaning, as a new context such as `job`, a new
  property such as `needs.<job>.result` or a new word operator does, changes
  what a workflow that has the word means. So it is made as a bug fix is,
  with a scan of the known workflows and a conformance case, and ACTIVATE
  takes it too, or it waits for a new format. Format 1's additions are:
  - `format:` itself;
  - workflow metadata (below);
  - `execution-group:` in `runs-on:`, beside format 1's `environment:`, the
    `execution-groups` input that chooses a group, and `resources:`, which
    says what a job needs from it;
  - `needs.*` in `strategy:` and `runs-on:`, for jobs sized from what earlier
    jobs found;
  - `needs.<job>.result`, a dependency's outcome, and the `job` context
    ([Contexts](#contexts));
  - `map()` and `filter()`, with their lambdas
    ([The language](#the-language));
  - a job's `retry:`, which can rerun a job whose worker was lost;
  - a group's `flatten: true`, which puts its inputs' values beside its own
    ([Inputs](#inputs)), as ACTIVATE runs it, though ACTIVATE's schema
    doesn't allow the key: Meridian's format dropped it, with a warning, as
    a key the schema doesn't define, and none of the 218 known workflows
    has it.
- **Stricter than ACTIVATE.** Where ACTIVATE runs a workflow whose meaning
  would depend on something it shouldn't, Meridian refuses it rather than
  guess. Each such place comes with an addition, or with a bug fix ACTIVATE
  takes too, and no known workflow has one:
  - a group's `flatten` is `true` or `false` written out. An expression
    there is a schema error: ACTIVATE reads it as evaluated where it fills
    the defaults, as text that isn't `true` where it resolves resources,
    redacts passwords and fills rows, and its form as `true`, so where the
    group's inputs' values are would depend on which reads it;
  - two inputs whose values have one key, where a group with
    `flatten: true` puts one beside the other, the group's own key among
    them, are an error (`flattened_name_taken`). ACTIVATE fills such a
    value from both definitions in the order the file writes them, and an
    ignored group drops the other's value with its own, which a workflow's
    meaning shouldn't depend on. Wizard steps whose inputs share a key are
    run as they were. None of the 218 known workflows has a group with
    `flatten`, so none has either;
  - an expression in `sessions:` or `links:` that reads an input whose
    value reads `env`, `needs`, `steps`, `job` or `matrix`, as its default
    does, is an error as the run is created (`context_not_known`), whatever
    was submitted for the input ([Bug fixes to format 1](#bug-fixes-to-format-1)).
    ACTIVATE refuses it only while the input's value is still that
    expression, and reads the input's value where one was given, as a form
    gives what it can evaluate: whether a run is created would depend on
    what was typed. An input whose default reads `needs`, `job` or
    `matrix` is an error on its own, which fails the run as it starts
    (`context_not_known`, at its default), where ACTIVATE gives the
    expression's text. None of the 16 known workflows with `sessions:`
    writes an expression in them, and none has `links:`.
- **Metadata.** A workflow can carry `name`, `description`, `icon` and
  `thumbnail`. `description` is a string or `{file: <path>}`; `icon` and
  `thumbnail` are paths. Paths are relative to the workflow file and may not
  leave its directory tree, so a workflow in a Git repository is complete and
  can't read files outside it. The compiler doesn't check that yet.
- **Strictness.** A workflow that needs something the environment can't
  provide fails to compile: an input type that names platform resources, a
  `uses:` that names a workflow with no Sources resolver, an execution
  group, an environment or a compute target a job's `runs-on:` writes
  out with no Groups resolver, sessions without a session provider. The
  error names the key and what would provide it (`resolver_missing`).
  There is no flag to run the workflow anyway. `compile.Compile`, which
  an editor checks a workflow as it is written with, reaches no resolver
  that takes a context, and so checks none of the last three, and takes an
  input of a platform's type as it was given, unchecked, but where there is
  no Inputs resolver at all.
- **What ACTIVATE ignores.** ACTIVATE has never checked workflows against
  the schema, so workflows that run there can have keys the schema doesn't
  define, which ACTIVATE ignores. In format 1, such a key is a warning, and
  is ignored. So is a repeated key, whose last value is used, and a second
  YAML document in a file. A name the schema doesn't allow, such as a job's,
  is still an error. A later format can make these warnings errors.
- **A job's `cleanup:` list.** ACTIVATE's ingress reads one more key that
  neither its schema nor Meridian's has: a job's `cleanup:`, a list of
  steps that it appends to the job's, after the copies that run its
  steps' `cleanup:`s, linked to the job rather than to a step (`.job`).
  Like those copies, they run once the job's steps have, through its
  cancellation and without a timeout, with each entry's own `cleanup:`
  after them, run as such a copy. Unlike them, an entry is a whole step:
  it keeps its `id`, its `uses:` and `with:`, which a copy drops, and its
  `retry:`, which a copy ignores, and runs whether or not any of the
  job's steps ran, where a copy runs only if its step did. Meridian warns
  of it as an unknown key and ignores it, as it does every key the schema
  doesn't define, so a workflow that has one runs without its cleanup.
  None of the 218 known workflows
  ([Bug fixes to format 1](#bug-fixes-to-format-1)) has one. Whether
  format 1 gains it as an addition, with ACTIVATE's meaning, or ACTIVATE
  drops it, is a decision for later.
- **Where Meridian still runs format 1 otherwise than ACTIVATE**, besides
  the additions, the stricter places and the keys ACTIVATE ignores above,
  and the [bug fixes](#bug-fixes-to-format-1), each is described where it
  applies:
  - the path a job not yet readied takes as its run stops, where ACTIVATE
    reads its `if:` by its text, or as far as it evaluates it, and a job
    between attempts as its run stops
    ([Cancellation, timeouts and cleanup](#cancellation-timeouts-and-cleanup));
  - when `early-cancel: any-job-failed` hears of a step that failed on a
    try that isn't known to be its last, and a job whose `if:` fails to
    evaluate (the same);
  - the order in which a matrix's instances take their turns under
    `max-parallel` ([Matrices](#matrices));
  - the `uses:` references ACTIVATE reads otherwise
    ([Interfaces](#interfaces));
  - where checkout's repository is read, with whose credentials, and when,
    which transports it may use and that it holds no credential, and what
    its paths mean; cancel-jobs' cancels of each other; and the actions
    only a platform provides ([Built-in actions](#built-in-actions));
  - what isn't built yet of the built-in actions, which fails the step
    that needs it when it runs: a whole checkout of a repository with
    submodules, and a cancel of another run's jobs
    ([Built-in actions](#built-in-actions));
  - what isn't built yet of checkout's revisions, which fails the run as
    it is created: one that format 1's whole checkout takes, as `git
    checkout` does, that isn't a branch, a tag or a full commit, such as
    a short commit or `HEAD~1`, which none of the 148 known checkouts
    names ([Built-in actions](#built-in-actions));
  - a workflow a step calls, which is a run of its own, can name groups
    with `runs-on:`, and its caller's job too; a required input of one
    that an expression in its caller's `with:` leaves without a value, or
    whose `optional:`, `hidden:` or `ignore:` is an expression, fails the
    step that calls it when it runs, where ACTIVATE refuses the run as it
    is created, if the expression reads only what is known then; and what
    isn't built yet of one, which fails the step
    that calls it when it runs: a directory outside its root's run
    directory, and a value of its `env:` that reads its own name
    ([Workflows a step calls](#workflows-a-step-calls));
  - a step's `clean-on-retry:`, which never cleans its own run's
    directory, nor one that holds it, where format 1 spares only the
    root's ([Failures and retries](#failures-and-retries)), and, for a
    caller on a host, never cleans its child's directory on the machine
    the run started on ([Workflows a step calls](#workflows-a-step-calls));
  - the limits on outputs, and a FIFO in a step's `$OUTPUTS`
    ([Outputs](#outputs));
  - how a job's `ssh:` reaches its host, where its run's directory is
    there, and what an outage or a cancel does to its steps
    ([The `ssh` backend](#the-ssh-backend)).

### Conformance

The conformance suite pins the format's meaning. Each case is a workflow,
or an expression, with the inputs and contexts it is given, and what it
loads, resolves or compiles to, the problems it has, or both. Cases come
from ACTIVATE's own tests, from public workflows and from Meridian. They
live in `conformance/`, in areas, and each package runs the cases of its
own: `format` for loading and validation, `expressions` for what
expressions evaluate to, `inputs` for a run's input values, `compile` for
plans and the problems with jobs and expressions, `start` for a run's
values as it starts and its check of the run's shape, `matrices` for
matrix expansion, and `contexts` for the shape of each context in the
phases after Start. Cases for runs come with what runs them, with the
outcomes and outputs they must end with, status functions and step
retries among them.

Behavior that workflows rely on is kept, even where it is surprising. Behavior
no workflow could rely on, such as a crash or a silently wrong result, is
fixed, and its case records the fix.

### Bug fixes to format 1

Each of these changes what format 1 means where it was plainly a bug, and
ACTIVATE takes the same fix. Each counts the same workflows, as every
count of the known workflows in this document does: each YAML file of the
known repositories that holds `jobs:` and parses to a mapping whose `jobs`
is a mapping, but for Meridian's own copies of them and the files under
`.git` and `.github`, and for other CI systems' configs, which hold
`jobs:` too but no `on:` with `execute`, as CircleCI's
`.circleci/config.yml` and Travis CI's `.travis.yml` do, counted once by
the SHA-256 hash of its content: 218 files.

- **A matrix job's output** is the instance's with the highest index, not
  the last to finish's. None of the 14 matrix jobs has `outputs:`, and
  nothing reads a matrix job's outputs.
- **A matrix instance's `needs.<its job>.outputs` are its own**: those the
  instance's own steps wrote, as a job reads its own outputs, where
  ACTIVATE read those of the instances of the job that had finished before
  it, merged, so that what it read depended on timing. The jobs that need
  the matrix job still read the instance's with the highest index.
  ACTIVATE takes the same fix by reading the instance's own outputs, as it
  reads its own steps. The 14 matrix jobs have 24 reads of `needs`, and
  none names its own job: all 262 reads of a job's own outputs by name,
  `needs.<its job>.outputs.<name>`, in 66 jobs of 48 workflows, are in
  jobs without a matrix.
- **An `if:` whose value is null is false**, as `docs/expressions.md` says
  conditions go by truthiness, where format 1 ran the job or step. So is
  an `if:` written empty, which is null: format 1 ran its job or step too,
  and Meridian refused it as the wrong type. None of the 329 `if:`s is
  written empty. All 7 that are a bare reference read a boolean input, and
  none of those inputs is hidden or ignored, so none is null: each has a
  default but one known workflow's boolean input, which ACTIVATE fills
  with `false` when it is left out, as Meridian does.
- **A duration or a count that isn't one is a problem**, where format 1
  ignored a timeout it couldn't read and fell back to a retry's defaults:
  an expression's value for a job's or a step's `timeout:`, a retry's
  `interval:` or `timeout:` (`duration_invalid`), or `max-retries:`
  (`count_invalid`). A literal one is a schema error already. A step's
  `timeout:` and `retry:` read `env` as its other values do, with the
  variables the worker defines: its `timeout:` for each try, and its
  `retry:` as its first try is evaluated, where format 1 left an
  expression there that read `env` as its text, and so ignored it. 11
  workflows take retry values from expressions, of inputs whose defaults
  read as one and of outputs a step wrote, none takes a timeout from one,
  and none of the 18 step retries reads `env`. In detail: `+5m` is a
  duration, as in format 1;
  null, `""` and `0` without a unit are the default, as format 1 reads a
  value it has none of, so a retry waits 5 seconds and a step's timeout is
  none; `0s` is 0, which for a job's or a step's timeout is none, where
  format 1 timed out at once; a negative duration, such as `-5m`, which
  format 1 timed out at once or ignored, is a problem; `max-retries:` text
  must be a whole number, with space around it or not, so `3.5` is a
  problem where format 1 retried 10 times, and ` 3` is 3; a number's
  fraction is rounded down in `max-retries:` and a retry's `interval:` and
  `timeout:`, as in format 1, so a retry's `0.5` is its default; and a
  job's or a step's `timeout:` of a number with a fraction is a problem,
  where format 1 ignored it.
- **A matrix key whose values aren't a list, and an `include` or `exclude`
  entry that isn't a mapping**, are problems (`matrix_not_list`,
  `matrix_entry_not_mapping`), where format 1 dropped them silently. All
  14 strategies give each key a list, written out, from a list input or
  from `range`.
- **A job's `outputs:` read `env`**: the workflow's `env:`, the job's, and
  the variables the worker defines, where format 1 left an expression that
  read it as its text. ACTIVATE takes the same fix by passing the job's
  `env` to the parse of its outputs. None of the 8 jobs with `outputs:`
  reads `env` in them.
- **`matrix.<key>` in a job without a matrix is null**, where ACTIVATE gave
  the text `matrix.<key>` when no job of the workflow had a `strategy:`,
  and null when another job had one, so that what a job read depended on
  the workflow's other jobs. The compiler warns of it (`no_matrix`).
  ACTIVATE takes the same fix by always passing a matrix, empty for a job
  without one, where the run is created. No known workflow reads `matrix`
  in a job without a matrix.
- **A value of an `env:` that reads itself, or values of one that read each
  other, read what is under the `env:`**: for a job's, the workflow's
  `env:`, and for a step's, the job's over that, where format 1 recursed
  until it failed. Values that read others of the same `env:` are
  evaluated in the order they read each other, as in format 1. None of the
  122 `env:` blocks has a value that reads another of its own block.
- **A step's `cleanup:` reads `steps.<its own id>` as its step wrote it,
  and so do the cleanups after it and the job's `outputs:`, for a step
  with an `id` and a `cleanup:`**, where ACTIVATE's copy of the step that
  runs the cleanup kept the step's `id`, and so emptied its outputs before
  the cleanup, the cleanups after it, which run in reverse order, and the
  job's `outputs:` read them. ACTIVATE takes the same fix by leaving the
  `id` out of the copy. None of the 155 `cleanup:`s is on a step with an
  `id`, and none reads `steps`.
- **`needs.<job>.steps.<id>.outputs.<name>` reads the step of `<job>`**, the
  job it names, where ACTIVATE read the step with that `id` of the job the
  expression is in, whatever job it named. All 37 known reads name their
  own job, so they read the same step either way. ACTIVATE takes the same
  fix by reading the job the reference names.
- **The platform's credential is kept out of the `env` context**, a fix for
  security: format 1 gives a step's process `PW_API_KEY`, and gave its
  expressions it in `env` too, so a value an expression made from it could
  reach a log, an output or the run's state. Meridian's `env` holds only
  the variables format 1 defines but it (`plan.IsFormatVariable`), and the
  worker gives the key only to the step's process. No known expression
  reads `env.PW_API_KEY`. ACTIVATE takes the same fix by leaving it out of
  the env map its parser reads, while keeping it in the step's process
  environment.
- **`sessions:` and `links:` read the inputs' values as the run has
  them**, defaults filled in and ignored inputs dropped, as the workflow's
  `env:` and its jobs read them, where ACTIVATE evaluated both with the
  inputs as they were submitted, before their defaults were filled in, so
  that a session or a link read an input given no value as null, and an
  ignored input's value. Meridian checks `links:` but doesn't make them
  yet; it will read them so. ACTIVATE takes the same fix by evaluating
  both with the values it fills in. Of the 218 known workflows, 16 have
  `sessions:`, none writes an expression in them, and none has `links:`.
- **An expression in `sessions:` or `links:` can't read `env`, `needs`,
  `steps`, `job` or `matrix`**, directly or through an input whose value
  reads one, as one whose default reads `env` does: it is an error as the
  run is created (`context_not_known`), as in the workflow's `env:`. The
  sessions are made, and the links shown, before any job runs; a job's
  `env` holds its own `env:` and what the worker sets as each attempt
  starts besides the workflow's, so what the workflow's alone gives is a
  value no job reads. ACTIVATE left such an expression for the worker to
  evaluate, which neither reaches, so the session or the link was made
  with the expression's text, and takes the same fix by refusing it as the
  run is created. None of the 16 workflows with `sessions:` writes an
  expression in them, and none has `links:`.
- **An `env:` value is text in the `env` context**, as it is in the step's
  process: one whose expression gives null is `""`, and one that gives a
  number, a boolean, a list or a mapping is its text, as a template writes
  it, where ACTIVATE's `env` context held what the expression gave, so
  that an expression could read null or a number where the step's process
  read `""` or the number's text. ACTIVATE takes the same fix by keeping
  an `env:` value's text, as it exports it, in the env map its parser
  reads. Of the 219 values of the 122 `env:` blocks, none is written as
  other than text, 139 are an expression whole, which can give other than
  text, and no expression reads a value any of them defines: the known
  workflows read them in the shell. A job's `runs-on:` that is an `env:`
  value holding a mapping names the group its JSON text names, where
  ACTIVATE refuses it; none of the known workflows' jobs has a
  `runs-on:`.
- **An ignored group with `flatten: true` drops its inputs' values in a
  group that flattens or a wizard step that flattens too**, as it does
  anywhere else, where ACTIVATE kept them: the outer container copied back
  to the run's values only its own inputs' keys, so what the inner group
  dropped stayed in them, and its form, which drops them the same way,
  sends them, their defaults when untouched. An ignored group's inputs reaching the run is plainly a
  bug. ACTIVATE takes the same fix by dropping, from the outer values, the
  keys the inner group dropped. None of the 218 known workflows has a group
  with `flatten`, which format 1 adds
  ([The workflow format](#the-workflow-format)).
- **A `compute-clusters` input given a list keeps what in it isn't a
  name**, where ACTIVATE made each item that isn't text, or is `""`, null:
  so the details of clusters that a workflow passes to one it calls, which
  it resolved already, reached the called workflow as `[null]`. Only a
  name is resolved; the rest of a list is kept as it is, as a value that
  isn't a list is. ACTIVATE takes the same fix. None of the 211
  `compute-clusters` inputs of the known workflows is `multi`, the form
  that takes a list.
- **A `parallelworks/cancel-jobs` of a job that hasn't started yet is
  kept**: the job ends cancelled where it waits, on the jobs it needs or
  to be placed, and never starts
  ([Built-in actions](#built-in-actions)), where ACTIVATE marked it as
  cancelling, which only a job that runs heeds, and the job's start
  overwrote the mark, so that it ran once its needs had ended. The jobs
  that need it read it as cancelled, as they read a running job that is
  cancelled: one without a cancel-aware `if:` is skipped because its
  needs failed, and the run fails, where it could succeed before. Of the
  215 known cancel-jobs steps, 2 cancel jobs that can't have started yet,
  since they wait on a job that still runs, both in one workflow, whose
  jobs so ran though it meant them not to; no job needs either of them,
  so no other job changes. In the other 213, the job cancelled waits on
  no job its canceller doesn't, or the canceller waits, polling, for it to
  run, so that whether it had started was a race at most, which no
  workflow can rely on. ACTIVATE takes the same fix by ending a job marked
  as cancelling as it would start it, or as it waits for its turn.
- **`vars.*` and `org.*` read every variable of their context**, as
  `inputs.*` reads every input's value: a list of the variables' values, in
  the order of their names, with the rest of the path read from each, as in
  `org.*.region`. ACTIVATE read a variable named `*`, which no variable's
  name can be, and so gave `""` whatever the variables were. A secret
  variable read so is one of the run's secrets, as one read by its name
  is, and what an expression makes of it is masked
  ([Variables and secrets](#variables-and-secrets)). ACTIVATE takes the
  same fix by reading each variable for a `*`. It masks the text of every
  secret variable the run has, however it was read, so one read so is
  masked as one read by its name, and its form leaves `vars.*` to its
  backend, which has the secret variables' values, when the user has one.
  None of the 10,080 expressions of the 218 known workflows reads `vars.*`
  or `org.*`, nor has a `*` in a reference to any context: 19, in 6
  workflows, read an organization variable by its name, and none reads
  `vars`.

## Phases

A run is evaluated at five points, its phases. Each phase is one function of
Meridian's packages, which computes what the workflow leaves to that point
from what is known by then. The coordinator and the worker only gather a
phase's inputs and keep what it gives, so every implementation, whether
`meridian`, a platform or an editor's WebAssembly, computes the same thing.

| Phase | When | Function | Contexts it adds | Evaluates |
| --- | --- | --- | --- | --- |
| Create | The run is created | `compile.Create`, wherever the run is created | `inputs`; `org` and `vars`, for the run's shape; `remote`; `sessions` | The input values it can, and what decides the run's shape: an input's `hidden`, `ignore` and `optional`, and a job's `needs` |
| Start | The run starts | `compile.Start`, in the coordinator | `org` and `vars`, read once for the run, secret ones included; passwords; the workflow's `env` | The input values again, from what was submitted, with everything known; the workflow's `env:`, `timeout:` and `sessions:`, and matrices whose values are known; the run's shape again |
| Ready | A job's dependencies have finished | `plan.Plan.Ready`, in the coordinator | `needs` and the status functions, then `matrix` | A matrix that reads `needs`, then a job's `if:`, `runs-on:`, `resources:`, `retry:`, `timeout:` and `ssh:` |
| Step | A job starts on its worker, for its `env:`, each of its steps is about to run, and each step's `cleanup:` once they have | `plan.ReadyJob.JobEnv`, once, then `plan.ReadyJob.Step` and `plan.ReadyJob.Cleanup`, in the worker | `job`; the job's and the step's `env`; `steps` | The job's `env:`, then, for each step, the job's `working-directory:`, where the run's directory is known (`env.PW_PARENT_JOB_DIR`), and the step's `if:`, `run:`, `with:`, `env:` and the rest of the step, or its `cleanup:` |
| End | A job's steps have run | `plan.ReadyJob.End`, in the worker | | The job's `outputs:` |

Each phase reads the contexts of the phases before it too.

- **Pure.** Create and Start reach files, networks and platforms only
  through resolvers ([Interfaces](#interfaces)), and Ready, Step and End
  reach nothing. Given the same inputs and resolver answers, a phase gives
  the same result, byte for byte.
- **One scope.** One function, `plan.NewScope`, builds the contexts an
  expression reads in each phase, and the compiler, coordinator, worker and
  WebAssembly package all use it, so a context has one shape everywhere
  ([Contexts](#contexts)). One table, `plan.Phase.Knows`, says which
  contexts each phase knows: `NewScope` builds a phase's contexts by it,
  and the compiler checks what an expression reads with it.
- **What each gives.** Create gives the plan, the run's secret record and its
  diagnostics ([Compiling](#compiling)). Start gives the run's completed
  values, or fails the run ([Starting a run](#starting-a-run)). Ready gives
  a job's evaluated values, or skips it, and a matrix's instances. Step
  gives a step's evaluated values, and End the job's outputs.
- **What is kept.** Start's results depend on what the run read as it
  started, so the coordinator keeps them: in the run's secret record when
  evaluating them read a variable or a password, and as run state otherwise
  ([Run data](#run-data)). Ready, Step and End depend only on what is kept
  already, so what they evaluate is given to whoever needs it, and only what
  they decide is kept: that a job is skipped, a matrix's instances, a job's
  outcome and outputs.

The five phases are built, and so are matrix expansion, the scope builder
and the masker. The worker calls Step and End, and the coordinator Start
and Ready ([The coordinator](#the-coordinator)); `meridian validate
--start` calls Start too.

### Contexts

What an expression can read is its contexts, each a mapping:

| Context | Holds | Known from |
| --- | --- | --- |
| `inputs` | The run's input values ([Inputs](#inputs)): a group's under its name, a list's as rows. In input definitions, `inputs.x.type` and the like read `x`'s definition, and `[index]` is the index of a list's row. | Create; Start completes it |
| `org`, `vars` | Organization and user variables, by name, as strings. `var` is another name for `vars`. | Create, for the run's shape; Start, for everything else |
| `remote` | Where a fetched workflow came from, as strings, such as `remote.repo` and `remote.branch`. | Create |
| `sessions` | The workflow's `sessions:`, by name, as strings, as the session provider gives them. | Create |
| `env` | The workflow's `env:`, then the job's over it, then the step's over that, and on the worker the variables format 1 defines, such as `PW_JOB_DIR`, over all, as strings. Each reads the ones under it, and the values of its own that don't read `env`; the workflow's reads none. | Start, the workflow's; Step, the job's, the step's and the worker's |
| `needs` | For each job this one needs: `needs.<job>.result`, its outcome; `needs.<job>.outputs.<name>`; and `needs.<job>.steps.<id>.outputs.<name>`, as strings. In the job's own steps and outputs, the job itself, or a matrix job's instance, whose outputs are those its steps without an `id` wrote so far. | Ready |
| `matrix` | The job instance's matrix values. | Ready, once the matrix is expanded |
| `job` | The job instance: `job.name`, the job's name; `job.key`, the instance's, as in `build#2`; `job.attempt`, from 1; and `job.group` and `job.member`, where it was placed. | Step |
| `steps` | `steps.<id>.outputs.<name>`, for each step that ran before this one, as strings. | Step |

A name that a context doesn't have reads as `""` in `needs`, `steps`, `env`,
`org`, `vars`, `remote` and `sessions`, whose values are strings, and as null
in `inputs`, `matrix` and `job`.

Reading a context in a phase before it is known is an error when the run is
created, such as `steps` in a job's `if:`, or `needs` in the workflow's
`env:`. A job's `strategy:` is what is expanded, so it can't read `matrix`.
Input definitions are evaluated at Create and Start, so they read only the
contexts those phases know ([Inputs](#inputs)). The workflow's `env:` and
`timeout:`, which Start evaluates, read only what Create knows, and so no
`env`: the workflow's `env:` is what `env` holds, and format 1 never read
`env` in the workflow's `timeout:`, where ACTIVATE leaves an expression
that reads it as its text, and so ignores the timeout.

The status functions are `success()`, `failure()`, `always()` and
`cancelled()` (also spelled `canceled()`), known from Ready on. A job's
default `if:` is `success()`, and an `if:` that calls a status function
replaces it: one that doesn't is true only when `success()` is too. An
`if:` whose value is the word `always` means `always()`: it is the older
spelling, which the compiler warns about, and a later format can drop it.
Otherwise an `if:` goes by its value's truthiness
([The language](#the-language)), so one written empty, which is null, is
false.

A job fails, for `failure()`, when a job it needs failed, or was skipped
because a job it needs had failed in turn, and is cancelled, for
`cancelled()`, when a job it needs ended cancelled, as in format 1: a
cancel of the run alone doesn't make `cancelled()` true for a job
([Cancellation, timeouts and cleanup](#cancellation-timeouts-and-cleanup)).
A job its own `if:` skipped counts as a success, so a job that needs an
optional one still runs. So a failure reaches the
jobs below it through those it skips: a job whose `if:` the jobs it needs
made false, because one failed or was cancelled, is skipped with that
reason, which the jobs that need it read as a failure. A step fails, for
`failure()`, when a step before it failed, as its job counts it, and is
cancelled when its job is being cancelled or a step before it was
cancelled, as `early-cancel:` stops one: as in format 1, that cancels the
rest of the job, whose later steps run only if they outlive a cancel, and
the job ends cancelled unless a step of it failed.

`needs.<job>.result` is a dependency's outcome: `success`, `failure`,
`cancelled` or `skipped`. For a matrix job it is `failure` if any instance
failed, otherwise `cancelled` if any was cancelled, `skipped` if all were
skipped, and `success` otherwise. With it, a job can run only when another
failed, as a fallback the workflow controls:

```yaml
jobs:
  primary:
    steps:
      - run: ./deploy.sh primary
  secondary:
    needs: [primary]
    if: ${{ always() && needs.primary.result == 'failure' }}
    steps:
      - run: ./deploy.sh secondary
```

`job` and `needs.<job>.result` change what an expression that reads them
means in format 1, so they follow the rule for such additions
([The workflow format](#the-workflow-format)).

## Compiling

Compiling is the Create phase. The compiler turns a workflow, its inputs and
its resolvers into a plan, in order:

1. **Load.** Parse the YAML, keeping the source position of every node.
   YAML aliases and merge keys are refused, as ACTIVATE refuses them, and
   the file's size is bounded.
2. **Validate** against the schema of the workflow's format.
3. **Translate** a workflow in an older format into the current one.
4. **Resolve** through the resolver interfaces: check input values against
   their declarations and apply defaults, look up execution groups, check
   the workflow's sessions, and resolve, pin and load the workflows that
   the run's own steps' `uses:` name, and those they call in turn: the
   run's whole tree, checked for cycles, depth and size
   ([Workflows a step calls](#workflows-a-step-calls)).
5. **Read matrices**, without expanding them: the keys a job's matrix
   writes out, which what reads `matrix` must name, and the jobs and
   outputs it reads. The compiler doesn't check a matrix's `include`,
   `exclude` or size: Start expands a matrix whose values are known by
   then, and fails the run if it can't be expanded, and Ready the rest
   ([Matrices](#matrices)). Compile each step's `cleanup:`, which runs at
   the end of its job.
6. **Check expressions.** Parse every `${{ }}`, evaluate what is already
   known, and check every reference that is left, with its position:
   - Reading a context where it isn't known yet is an error, such as `steps`
     in a job's `if:`, or `needs` in the workflow's `env:`
     ([Contexts](#contexts)). So is a relative reference outside input
     definitions, since the plan holds no other part of the workflow for it
     to read.
   - Reading a job's outputs is an error unless the job needs it, or is the
     job itself. A job's outputs are whatever its steps write, so their names
     aren't checked.
   - In format 1, reading an input, matrix key or session that doesn't
     exist is a warning: ACTIVATE reads it as null or `""` and runs, and a
     workflow a step calls can read inputs its caller passes without
     defining them. Format 1 reads `steps` only as
     `steps.<id>.outputs.<name>`. A step reading one of a step that doesn't
     exist or doesn't run before it is an error, since ACTIVATE fails the
     step. In a step's `cleanup:`, which reads it as `""`, it is a warning,
     and in a job's `outputs:` too: they read it as `""` once a step of
     the job failed or was cancelled, but once its steps succeeded, fail
     the job, as ACTIVATE fails it (`output_not_written`). Any other
     reference to `steps`, such as
     `steps.build`, `steps.build.outputs` or `steps.build.result`, whatever
     step it names, is a warning (`steps_read_whole`, `steps_unknown_key`),
     and reads as its own text, as format 1 reads a word it doesn't know.
     No known workflow has one: of the 218 that
     [Bug fixes to format 1](#bug-fixes-to-format-1) counts, one reads
     `steps` other than through `needs.<job>.steps`, as
     `steps.<id>.outputs.<name>` in a job's `outputs:`, of a step that
     has no `id`, which is a warning (`output_undefined_step`), and fails
     the job once its steps succeeded, in ACTIVATE as in Meridian. So with
     `needs`, which format 1 reads only as `needs.<job>.outputs.<name>` and
     `needs.<job>.steps.<id>.outputs.<name>`, and Meridian as
     `needs.<job>.result` too: any other reference to it, such as
     `needs.build`, `needs.build.outputs`, `needs.build.steps.compile` or
     `needs.build.status`, whatever job it names, is a warning
     (`needs_read_whole`, `needs_unknown_key`), and reads as its own text.
     The known workflows have 839 references to a job's outputs by name and
     37 to its steps', and none of any other form. A plan holds an
     expression's tree, not its text, so a reference read as its own text
     is made again from its path: one written with dots is as written, and
     a key in brackets is written `['key']`, or `[key]` when it had no
     quotes, which can differ from how it was written, where ACTIVATE gives
     the reference exactly as written.
   - Reading a variable that isn't a secret, or defining an input that takes
     text rather than a password, whose name looks like a secret's is a
     warning: a plain variable's value isn't masked, and a text input's is
     kept with the run. A name looks like a secret's when one of its words,
     split at anything that isn't a letter or a digit, where letters and
     digits meet, and at changes of case, as the masker splits a key
     (`internal/words`), is `token`,
     `password`, `passwd`, `pwd`, `passphrase`, `secret`, `apikey`,
     `privatekey`, `accesskey`, `credential` or `credentials`, or two in a
     row are `api key`, `private key` or `access key`, so `HF_TOKEN` and
     `apiKey` do and `tokenizer` doesn't. A variable is warned about once,
     where it is first read.
7. **Emit** the plan, and beside it the run's secret record as it starts
   (`plan.Record`), which holds what the plan doesn't: what was submitted,
   as it was given, passwords among it; the parts of the input definitions
   that can hold a password; what each decision of the run's shape that
   read a variable or a password decided; and which parts of what was
   submitted mean the expression a form made them from. Beside the input
   values that read no variable and no password, the plan holds what Start
   needs to resolve the rest again as Create did: the input definitions,
   where the parts it withholds are (`Run.Withheld`), and where each
   decision of the run's shape that read a variable or a password is
   (`Run.Decisions` and a job's `NeedsExpr`).

The compiler does no I/O of its own: files, networks and platforms are
reached only through resolvers, which get the context the compiler is
given, so cancellation, deadlines and traces reach them. It is
deterministic, so the same workflow, inputs and resolver answers give a
byte-identical plan. That is also what lets it build to WebAssembly
unchanged (see [Editors and web UIs](#editors-and-web-uis)). `Create`
takes a context, which it reaches Inputs, Variables, Groups, Sources and
Sessions with; `Compile`, which gives the plan alone, doesn't, and reaches
none of them, as if they weren't there.

Errors and warnings are reported together rather than one at a time.
Warnings, such as a deprecated key, don't stop a run, and are kept with it,
so that they reach whoever runs the workflow and not only its author.

Each one is a problem of a kind the `problems` package defines, built on
the problem details that Parallel Works' services share
(`github.com/parallelworks/foundation/problem`):

- **A stable code per message**, such as `unknown_key`, which is part of the
  contract: a code and its params are never renamed. Its type URI,
  `/problems/workflow/<code>`, resolves to a page that says why it happens
  and how to fix it. Checks every API shares, such as a text's length or a
  number's range, use foundation's own rules, such as `too_long`.
- **Params**, the values its message shows. Data from the workflow or the
  run, such as a key or a value, is shown as data (a quoted name, compact
  JSON), so that only the words around it are translated. A value that an
  organization or user variable or a `password` input gave, or that was
  made from one, is never shown: a variable can hold a secret, and a
  password is one, so a param that would show it is `***`, and a
  suggestion near it is left out. Nor is a value given for a group, a list
  or a wizard step with a password among its inputs.
- **Its message in every language** the catalog has, written in ICU
  MessageFormat: English, Spanish, Japanese, Korean and Chinese, the
  languages of foundation's own catalogs. A diagnostic's detail is its
  message in English; a platform writes it in the reader's language from the
  code and params.
- **Where it is**: a JSON Pointer into the workflow, and the file, line and
  column among its params. The kind decides whether it is an error or a
  warning.

A diagnostic is a foundation `FieldError`, so over HTTP a workflow's errors
are an RFC 9457 validation problem, one entry each, and its warnings travel
the same way beside a successful result. A plan keeps its warnings' codes
and params but not their text, so each reader sees them in their own
language.

A workflow, or the values submitted for a run, can have far more problems
of one kind than anyone reads, such as a value for each of a hundred
thousand inputs the workflow doesn't define, and a plan keeps its
warnings. So the compiler lists the first 100 problems of each kind, in
order, and one more, `problems_not_listed`, where the first it leaves out
is, counts the rest. A run's unknown inputs past the first 100 aren't
looked at for a suggestion either.

Limits on a run's size are Meridian's own, which whoever runs it sets in
the compiler's options (`Options.Limits`): the workflow file's size, 100 KiB
by default, as in ACTIVATE; the number of jobs; a matrix's instances, 256 by
default; the tree of workflows that `uses:` calls, as in ACTIVATE, in
`Options.Calls`: how deep it nests, 4 levels below the run by default,
how many distinct workflows it holds, 100, and calls, 1,000, and how much
text, 1 MiB; and the nets on what a
group of templates builds and does ([The language](#the-language)). The
defaults are high enough for every workflow in the conformance suite. An
expression's depth, `range`, and what one template builds and does are the
language's limits, the same wherever a workflow runs. A plan's JSON nests at
most 10,000 levels, as deep as its encoding reads, so a value nested deeper
is an error. The plan records the limits that the phases after Create
apply, such as a matrix's instances, so every coordinator replica, and
every rerun, applies the same ones (`Run.Limits`). Today the file's size
and the nets are fixed, and of the others `Options.Limits` has only a
matrix's instances, and `Options.Calls` the tree's.

### Inputs

The compiler checks a run's input values against the workflow's input
definitions, under `on.execute.inputs`, and fills in what is missing, as
ACTIVATE's launch form and its backend do together:

- **Defaults.** An input with no value, or with `""` when `""` isn't one of
  its options, takes its default. A null submitted for an input, or an
  input in a group, is its value, as ACTIVATE keeps it: it takes no
  default, and what reads it reads null; only a key not submitted, or
  `""`, takes one. A group submitted as null takes its inputs' defaults, and
  in a list's row null is no value, as ACTIVATE fills in a row's. A default
  of `""` gives no value. A
  default that is an expression is evaluated with the other inputs' values,
  defaults included, in whatever order they need; defaults that read each
  other are an error. A default reads what its paths can reach, however
  they are written: a `*` or a slice of a list reads its rows, `rows[00]`
  the row `rows[0]` does, and `rows[1:][0]` the row `rows[1]`. An index of
  a slice that counts from the list's end, as `rows[-1:][0]` is, reads any
  of the rows, since which one depends on how many there are.
- **Syntax.** Every expression in the input definitions must parse, in a
  default, an option, a label or anywhere else, whether or not the run
  evaluates it, and in a key the format doesn't define, which is a warning
  and is otherwise ignored, too: ACTIVATE evaluates every string there. A
  syntax error is reported once, where it is.
- **What a form sends.** A form evaluates each default it can and sends
  what it gives, and sends the text of an expression it can't evaluate
  itself, such as a default that reads a secret. A submitted value that is
  exactly the text of an input's default means the default. So does a
  string in a submitted mapping or list that is the text of an expression
  in the same place in its default, and a choice that is the text of one of
  its options' values, which means that option. Any other value holding
  `${{ }}` is data. A submitted value in the place of such an expression
  that can read a variable or a password, or that one of the options can
  have given, can be made from them, so it is read as one of them from the
  start (Variables and Passwords, below). Once the values are filled in, it
  means the expression if it is what the expression gives, the same JSON,
  a number or a boolean the same as its text, as `8` and `"8"` are: as the
  run starts, it is what the expression gives then, so a variable that
  changed since is read as it is. Which values mean their expressions says
  something of a variable's value, so the run's secret record keeps it
  (`plan.Record.Means`), and the plan holds such a value no more than it
  holds one typed over the expression. It means an option written in the
  workflow if it is one. Anything else there can be what the form gave
  before the person changed what it read, what it gave a row they moved,
  what it evaluated otherwise than Meridian does, or what the person typed
  over it, which the compiler can't tell apart, so the plan doesn't hold
  it, and the run has it as it was given. Where the expression can read a
  password or a secret variable, it is one of the run's secrets, which can
  be made from one or not, and is masked as what is made from a secret is
  rather than as a secret's own text; where it can read only variables
  that aren't secrets, it is private, as their values are, and isn't
  masked. A list's default rows count for any of its rows, since a person
  can delete, add and move them. What can read a variable or a password,
  and what a password or a secret variable, is found from the definitions
  before anything is evaluated, following relative references to the
  definitions they read. A default that reads its own input, which a form
  doesn't fill in, gives what the compiler gives it. A submitted value that
  an expression which reads it gives back, as a default that reads its own
  input does, or one that reads another input whose default reads the
  first, isn't what made it, however the two compare: it is kept as what a
  person typed over the expression is.
- **Lists.** A list's default is the rows it starts with. Each row, given or
  a default, is a group of the template's fields, which take their defaults
  as inputs do, in the same order as the rest; in a field's default,
  `[index]` is its row's index. A row's fields aren't checked against their
  types, but for a password field, which is checked as a `password`
  input is, and their conditions aren't evaluated. In a list or a repeated
  step with a password among its rows' inputs, a row, or a group or list
  of a row on the way to a password, that isn't the shape its inputs say
  can hold a password where the compiler can't find it, so it is kept as a
  secret, as it was given; the plan holds null in place of a row.
- **Conditions.** `hidden`, `ignore` and `optional` are evaluated once the
  values are complete, and are true when they are true or the text `true`.
  One that reads what isn't known until the run starts, such as a secret,
  is an error, and so is one that reads an input whose value isn't known
  until then because a part of it reads such a thing. An input whose value
  reads only variables and passwords, which the compiler knows, can be
  read.
  An ignored input has no value, even one that was submitted: a condition
  evaluated once it is ignored reads it as null, a list with rows too, and
  a group read whole leaves it out. An
  input with no value and no default is required unless it is optional, or
  hidden or in a hidden group; a boolean with none is `false`. A submitted
  null is a value, so an input given null isn't required, and a boolean
  given null stays null. Every missing input is reported, not only the
  first.
- **Containers.** A group's values are under its name, unless it has
  `flatten: true`: then its inputs' values are beside the group's, under
  their own names, as ACTIVATE runs it, in a list's row, a wizard step or
  another group as well, and the group has none of its own. A group's
  value, and a wizard step's whose inputs' values are under its name, is a
  mapping in the run's values, though none of its inputs has a value, or
  it was given null, as ACTIVATE fills in a run's values, so the jobs read
  `{}`. One that is ignored, or in one that is, has none, and so has one in
  a list's row none of whose inputs has a value, as ACTIVATE fills in a
  row. The input definitions and the workflow's `env:` read a group whole
  as the jobs do, as the values its inputs have, without the inputs that
  have none, as ACTIVATE's do with what a launch form sends, which gives
  an untouched group's booleans as `false`; an input in it read alone with
  no value reads null, or `false` for a boolean, as above, and so does a
  row's input that a lambda reads through its parameter, as `r.b` in
  `map(inputs.l, r => r.b)` does, while the row read whole, as `toJSON(r)`
  reads it, leaves the input out, as a list's rows read whole do. A group
  given no value at all, as only a request made without a form can be,
  reads in ACTIVATE's input definitions as its definition, where
  Meridian's reads `{}`, among the differences of incomplete values below.
  A value given at
  its key, of any shape, is kept, as ACTIVATE keeps it, with a warning
  (`flattened_group_value`), and none of its inputs reads it; it is kept as
  a secret if the group has a password among its inputs, which the group's
  inputs sent under its name, misspelt or as text, can hold. Ignoring the
  group drops its inputs' values and that one, and hiding it hides its
  inputs, as for any group, wherever it is: in another group that flattens
  or a wizard step that flattens too, where ACTIVATE kept its inputs' values
  ([Bug fixes to format 1](#bug-fixes-to-format-1)).
  `inputs.<group>.<input>` doesn't read such an input, which the compiler
  warns of in a job (`flattened_input_ref`), as it does for a flattened
  wizard step's. A wizard step's inputs have their values beside the step's,
  unless `$meta.wizard.flatten` is `false`. A wizard step with `multi: true`
  repeats, as many times as the person running the workflow adds it: its
  value is a list under its name, one row for each time, whatever `flatten`
  says, as a form keeps it, and each row is a group of the step's inputs, as
  a list's row is of its template's fields. A repeated step given no value
  has none. A header has no value.
- **Types.** Format 1 reads every input value as it was submitted, as
  ACTIVATE does, and converts none: its defaults, conditions, `env:` and
  steps read a number sent as text as that text, so `"8" + 1` is `"81"`, and
  the text `"true"` isn't `true`, nor is `"false"` false; a submitted null
  stays null, as Defaults says; and a submitted number is read as ACTIVATE
  reads a JSON number, as a 64-bit float, so a whole number past 2^53 is the
  float nearest it: 9007199254740993 is 9007199254740992 wherever it is
  read. ACTIVATE's run form sends a number field's value as a JSON number
  and a checkbox's as a JSON boolean, and a JSON caller writes the types
  itself; a client that has only text converts by the field's type, as
  `meridian validate`'s `-input` does ([The `meridian`
  command](#the-meridian-command)). The checks take what they took when
  Meridian converted: text that reads as a number passes a number input's
  type and its `min` and `max`, and the text true or false, in any case and
  with space around it, a boolean's, each read for the check and kept as it
  was submitted. A value submitted as another JSON type than its input's,
  text for a number or a boolean, or a number or a boolean for a text input,
  is a warning (`input_read_as_submitted`), which names the input, its type
  and the JSON type it was given, never the value, and changes nothing else.
  A later format can refuse such a value. An input of a platform's type is
  what the Inputs resolver gives for it, as a cluster's details for its
  name, from the moment its value is known: the value submitted, a field's
  of a list's rows among them, its default, or what its default gave. So
  everything that reads it reads what the resolver gave: the other inputs'
  defaults and conditions, as `hidden: ${{ inputs.cluster.schedulerType !=
  'slurm' }}`, the workflow's `env:` and the jobs, at Create and at Start,
  as ACTIVATE resolves a run's resources before it evaluates any of its
  expressions. ACTIVATE resolves a `compute-clusters` or `bucket` input's
  name, and leaves every other type's value as it is, which the resolver of
  a platform that runs format 1 as ACTIVATE does does too. A value the
  resolver can't resolve is an error, as it fails ACTIVATE's run, even for
  an input that is ignored, as ACTIVATE resolves before it decides what is
  ignored. ACTIVATE gives a `compute-clusters` or `bucket` input given null,
  or a value that isn't text, its default. Meridian keeps a null, as for
  every input, and doesn't ask the resolver for it, and asks the resolver
  for any other value as it was given, as `5`. ACTIVATE asks for a default
  that is an expression unevaluated, which names no resource and fails the
  run, where Meridian asks for what the default gives. None of the 218 known
  workflows has a `compute-clusters` or `bucket` input whose default is an
  expression. Start asks the resolver again ([Starting a
  run](#starting-a-run)). What the resolver gives can't be split by the
  parts of the value that made it, so a value of which a part isn't the
  plan's, as a part typed over a part of a default that reads a variable, is
  held whole: private, or a secret if a part read one, out of the plan's
  values. The local resolver gives back what it is given. Anything else that
  doesn't fit the input's type is an error, as is a number outside its `min`
  and `max`, or a choice that isn't one of a dropdown's options when they
  are written in the workflow. A value for an input the workflow doesn't
  define is a warning that suggests the nearest name, and is kept, as
  ACTIVATE keeps it. One whose name is a password field's, wherever the
  workflow defines it, as a repeated step's password sent beside the step
  is, or is close to a password input's there, as a misspelling is, is kept
  as a password: it is one of the run's secrets, and not in the plan. So is
  one whose name can be a secret's, as one sent for a password field that
  was renamed can: it looks like one, by the words that make an input's name
  look like a secret's ([Compiling](#compiling)), or one of its words is a
  letter from one of the longer of them, `password`, `passwd`, `passphrase`,
  `secret` and `credential`, as `pasword` is; not from `token`, whose
  neighbours, such as `tokens` and `taken`, are common words. That errs
  toward a secret, such as `passed`, which is kept as one, and still given
  to the run. So is a mapping or list whose name is close to an input's with
  a password among its inputs, or that holds a key named as a password field
  or that can be a secret's, as a flattened wizard step's inputs sent under
  the step's name do. A row of a list or a repeated step is checked the same
  way, against the inputs its rows have, and a value there for one they
  don't have is reported by its path, as `creds.0.pasword`, and kept as a
  password by the same rules, by that path, as a password field of a row is:
  among the secrets, and not in the plan's values. A key of a list's default
  rows, written in the workflow, that isn't one of its rows' inputs is a
  warning where it is written (`default_row_unknown_input`), once, and the
  row keeps it; a launch form sends the default rows back, so a submitted
  row's key that a default row has isn't reported again, and a value a
  default row has at that key isn't taken for a password by its name, as
  `api_token: none` written in the workflow isn't. A row's inputs are those
  of every list at its list's path, where a list and a wizard step's list
  share one. A value that isn't JSON, such as NaN or text that isn't UTF-8,
  is an error, whether it was submitted or a default gave it, as `0/0` gives
  NaN and `1/0` an infinity: ACTIVATE can't encode such a run's values, and
  fails it as it starts. One in a list's rows is reported at the row's
  field, and so is one in what the compiler knows of a value that waits for
  Start.
- **Platform types.** Input types that name a platform's resources, such as
  clusters, Slurm partitions and buckets, need an Inputs resolver, which
  checks the value and gives the run the resource's details. Without one, an
  input of such a type that has a value is an error. Format 1 gives a run
  the details of a cluster and of a bucket for its name; every other type's
  value is the name as it was given, text or a mapping, which nothing checks
  as the run is created: a platform's form lists the names to choose from,
  as a cluster's partitions, and the scheduler, or the command the workflow
  gives the name to, checks it as the run uses it. The meridian command's
  resolver gives these names so ([Interfaces](#interfaces)).
- **What waits for Start.** A default that reads something not known when
  the run is created, such as `env` or a secret variable, isn't evaluated
  until then: its input isn't among the plan's values, and Start, which
  resolves the inputs again with everything known, evaluates it, and checks
  it as Create checks the others: against its input's type, whether it is
  required, and with the Inputs resolver for a platform type
  ([Starting a run](#starting-a-run)). An input's value waits whole, even if
  only part of it reads such a thing: a part of a mapping or list default, a
  form's text, or a row's default. So what reads any part of it, a part the
  compiler has too, isn't known until then either: a condition that reads
  it is an error, and a default that reads it waits too, and so does one
  that reads that default. A row's default that reads another field of its
  row reads it as it is, as a part of the same value. The secret variables
  it reads are among the run's secrets.
- **What definitions read.** Input definitions are evaluated at Create and
  Start, so they read `inputs`, `org`, `vars`, `remote`, `sessions` and the
  workflow's `env`, and nothing later: reading `needs`, `steps`, `matrix`,
  `job` or a status function in one is an error. Start evaluates the
  workflow's `env:` with the inputs, in the order they need, so a default
  can read `env` and an `env:` value an input; a default and an `env:` value
  that read each other are an error (`values_read_each_other`), as two
  defaults that do are. A default reads the workflow's `env:` alone, not
  the variables the worker defines for a job, such as `PW_PARENT_JOB_DIR`,
  which it reads as `""`, where ACTIVATE evaluates an input expression
  that reads `env` where it is read, with the env there. No known workflow
  reads `env` in an input expression. Today the compiler accepts a later
  context in a definition, and leaves its input to Start, which reports it
  (`context_not_known`).
- **Variables.** A plan never holds an organization or user variable's
  value, secret or not ([The plan](#the-plan)). An input whose default
  reads `org` or `vars`, or a part of whose value reads one, isn't among
  the plan's values in the same way, all of it, and Start evaluates it
  again; so does a submitted value that is what such an expression gives,
  as a form sends it; any other submitted value in its place is private,
  or one of the run's secrets where the variable is one (What a form
  sends). The compiler
  evaluates it all the same, since what decides the run's shape reads its
  value: the inputs' `hidden`, `ignore` and `optional`, and the jobs'
  `needs`. It checks it against its input's type too, but a problem with
  it doesn't show the value.
- **Passwords.** A plan never holds a `password` input's value, wherever
  the input is: on its own, in a group or a wizard step, or a field of a
  list's or a repeated step's rows. The value is one of the run's secrets,
  which the plan names by its path in the values, such as
  `inputs.creds.0.pass` for the field `pass` of the list `creds`'s first
  row, and it is checked against its type, but a problem with it doesn't
  show it. What decides the run's shape reads its value, as it reads a
  variable's. A value made from a password counts as the password, as one
  made from a variable counts as the variable: an input whose default
  reads one isn't among the plan's values, and Start evaluates it again,
  and so does a submitted value that is what such a default gives, as a
  form sends it; any other submitted value in its place is one of the
  run's secrets. A list, a group or a wizard step with a password among its
  inputs whose value, or one of whose rows, is evaluated at Start is one of
  the run's secrets as a whole, as which of its values are passwords isn't
  known until then.

ACTIVATE differs where its values can be incomplete, as they are from its
API: it evaluates conditions and defaults against the submitted values
before it fills in defaults, so a condition can read an input's definition
in place of its default. It doesn't check values against their types. Its
form keeps a repeated step's value as rows, as Meridian does, but its
backend fills in the step's inputs as an ordinary step's, and drops the
rows.

## Starting a run

Start completes the run's values with what only the start has: the
variables, read once for the run, the run's passwords, and what the
platform says of its resources then. It is `compile.Start`, and the
coordinator runs it as the run's first hopper job, which every other job of
the run waits for ([On hopper](#on-hopper)).

Start doesn't resume what Create left. It runs the compiler's own input
resolution again, from what was submitted, which the run's secret record
holds, with everything known: Start is Create with the variables and the
passwords known, so what it gives is what Create would have given had it
known them, in the same order and by the same code. The plan holds nothing
for Start to resume, so what it can't hold, a variable's value or what was
made from one, it doesn't hold in another form either.

1. **Read.** Read the variables the plan lists through the Variables
   interface, once, secret ones included: by name, or a context's whole
   when the plan names it alone. The plan lists every variable the input
   definitions read, as Start can evaluate any of them. The interface reads
   a variable by its context, a dot and its name as it is, which can hold
   dots of its own, as `vars.my.token`; the plan writes the same name
   `vars.my~1token` (`plan.JoinPath`). A variable the interface gives as a
   secret is one, and so is one the plan names as one though the interface
   no longer does. Put the parts of the input definitions the plan
   withholds back in their places, from the secret record
   (`plan.Record.Withheld`).
2. **Resolve the inputs again.** Resolve what was submitted
   (`plan.Record.Submitted`) against the input definitions as Create does
   ([Inputs](#inputs)): defaults filled in in the order they read each
   other, conditions applied in the inputs' order, reading the inputs as
   they were given, and each value checked against its input's type, with
   the Inputs resolver for a platform's type. Every variable and password
   is known now, so:
   - A value a form made from an expression that reads a variable or a
     password, which Create decided means the expression
     (`plan.Record.Means`, [What a form sends](#inputs)), is what the
     expression gives now, so a variable that changed since the run was
     created is read as it is. Start evaluates it in the value's place, as
     a form's text for the expression, in the order the values read each
     other, so that what reads the value reads what it is, and each is
     evaluated once; values that mean expressions that read each other
     read each other as they were submitted. What reads one through a
     relative reference doesn't wait for it, so where the input
     definitions make one, and the expression gives another value than
     was submitted, Start resolves the inputs once more with it in the
     value's place. Any other value in such a place, which Create decided
     was typed, is what was typed, and so is one whose expression fails
     now.
   - A default, or such a value, that reads an `env:` value waits for what
     the `env:` value reads, and Start evaluates the `env:` value then,
     before what reads it, so inputs and `env:` values that read each other
     in turn are resolved in one round, in the order they read each other.
     An `env:` value that gives otherwise once the inputs are resolved, as
     one that reads an input a condition drops does, Start evaluates only
     then, and resolves the inputs again with it. The rounds share the
     budget of what Start evaluates, and each after the first counts
     toward it the work of resolving every input, row field and `env:`
     value again, so rounds that go on, one for each link of a chain of
     such values, fail the run with `expressions_too_much_work` in a few
     seconds at most, rather than outlast whatever deadline the
     coordinator gives Start.
   - Every condition decides, and every input it ignores is dropped, those
     whose `ignore:` read a variable or a password among them, which Create
     held out of the plan whatever it decided. Whether each input is
     required is checked.
   - An input of a platform's type is checked with the Inputs resolver
     again, which gives what the platform says of the resource as the run
     starts, as Start reads the variables as it starts. Everything that
     reads the value reads what the resolver gives, as at Create
     ([Inputs](#inputs)). The secret record doesn't keep what the resolver
     gave Create: the run reads a resource as it is when it starts, as a
     rerun does, and Create's answer only checked that the value names one.
     A value, or a part of one, that means the default a form made it from
     is what the default gives as the run starts, and the resolver is asked
     for that, never for what was submitted, whether or not anything reads
     the input: a variable that names another resource since the run was
     created names that one, which the resolver can refuse, masked.
     A value the resolver refuses is the run's problem, as at Create;
     one it can't answer for, as when the platform can't be reached, is
     Start's error, which the coordinator starts the run again after, as
     for every resolver.
   - What Create reported as a warning isn't reported again.

   A relative reference reads the input definitions; one that climbs out
   of them reads nothing, where Create read the rest of the workflow,
   which no known workflow does: each of theirs reads a key of the input
   it is in, as `.hidden` does, or, with `..`, of an input beside it.
3. **Evaluate the workflow's `env:`** with the inputs resolved: an input the
   run ignored reads as having none, as the jobs read it, and one of a
   platform's type as the Inputs resolver gave it. A default that reads
   `env` waits for it: Start resolves the inputs again with the `env:`
   values it has evaluated, until a round evaluates nothing more. What is
   left then reads itself, or each other (`values_read_each_other`), or a
   context Start doesn't know, as `needs` (`context_not_known`). No known
   workflow reads `env` in an input's definition.
4. **Check the record fits the plan.** A value the resolution kept out of
   the plan, as a password or a private value, that the plan doesn't name
   means the secret record isn't the one Create gave with the plan, and so
   does a value the plan holds that what was submitted, resolved again,
   doesn't give, or a value for an input the workflow doesn't define that
   the plan doesn't hold: each is an error (`compile.ErrMismatch`). A
   password input's value is one the plan names whenever the run has it.
   Start checks these once it has checked the shape, as a run whose shape
   changed gives other values, and fails as it is. Before it reads anything,
   it checks that the record names the plan, by its digest
   (`plan.Record.PlanDigest`, `plan.Digest`), and that the record's withheld
   parts and decisions are those the plan names, no more and no fewer. What
   the plan doesn't hold isn't checked: an input of a platform's type, which
   the resolver gives again and can give otherwise, and one whose definition
   reads what it gives, a password, a value given in place of a default that
   reads a variable, and which values mean their expressions. So the digest
   binds a record to its plan, and not to its run: a record of another run
   of the same plan starts with what it gives, and the coordinator keeps
   each run's record with its plan. A value the plan names that the record
   doesn't hold, as a password, is the run's problem, `input_required`, as
   it is when a run is created without it.
5. **Check the shape.** Compare each decision of the run's shape that read
   a variable or a password as the run was created, an input's `hidden`,
   `ignore` and `optional` (`Run.Decisions`), with what the secret record
   says it decided (`plan.Record.Decisions`), and each job's `needs:` that
   read one (`Job.NeedsExpr`), which names the same jobs in whatever order,
   with the plan's graph. If one differs, the run fails with
   `run_shape_changed`. Every value Start gives is of one reading, its
   own, so what the check keeps is what the plan shows of Create's: the
   jobs' graph, and that the run that starts is the run that was created.
   A decision of an input in a group the run ignored isn't made, nor
   checked: the group's is.
6. **Evaluate the workflow's `timeout:`.** ACTIVATE leaves as text a
   workflow's `timeout:` or a `strategy:` that reads `env`, and evaluates a
   `strategy:` without the user's variables, so that `vars.X` reads `""`
   there, where Meridian evaluates both with them; a `timeout:` reads
   `vars` in ACTIVATE's final pass as Meridian's does. No known workflow
   has either.
7. **Expand** each matrix whose values are known by now, written out or
   read from inputs or variables ([Matrices](#matrices)), so a matrix that
   can't be expanded fails the run before any job runs.
8. **Make the sessions** of the workflow's `sessions:`, through the
   Sessions interface, under the names Create gave them, once everything
   else has passed, so a run that fails to start makes none. Start
   evaluates them with the run's values, as the phases after it read them,
   and tells the provider what evaluating them read: what is made from a
   variable or a password is private, and from a secret masked as what is
   made from one is. One that fails to evaluate fails the run, as an
   `env:` value does, without showing what it read. Create only checks
   them, as they are written, since a run that is created may never start.
   Making them again for a run that has them makes nothing more, and a
   rerun, whose sessions ended with the run, makes them again. A run with
   sessions can't start without a session provider (`resolver_missing`).
   What the provider refuses, or fails with, of sessions that read a
   variable or a password shows nothing of them: a refusal's params are
   masked, and any other error is Start's, saying only that the provider
   couldn't make them ([Bug fixes to format 1](#bug-fixes-to-format-1)
   says how `sessions:` read the inputs and `env`).
9. **Keep** the variables it read in the secret record, and what it
   evaluated there or as run state, by whether evaluating it read a
   variable or a password ([Phases](#phases)), and its diagnostics with the
   run, all in the transaction that ends its hopper job. What Start made
   from a secret is a secret, masked as what is made from one rather than
   as a secret's own text (`RunValues.Derived`), so that an `env:` value
   that says whether a secret is set, `true`, masks no `true` in the logs;
   and from a variable or a password private, by its name
   (`RunValues.Secrets` and `Private`), beside what the plan names so. A
   variable that was plain as the run was created and is a secret as it
   starts makes every private value the plan names masked from then on as
   made from a secret, since the plan doesn't say which variable each was
   given in the place of. `StartResult.Keep` is the one place that splits
   what Start gives: it puts what the secret record holds in it, the
   private half of `RunValues.Split`, the timeout when it is private, and
   the matrices whose instances are, whole; and returns what run state
   holds (`StartState`): the rest, with a private matrix's instances by key
   and index alone, without its `fail-fast` and `max-parallel`, which read
   what its values read (`plan.Expansion.Split`).

Start's problems are the run's, at the JSON Pointer of what they are about,
with no file position, as Ready's are. Given the same plan, secret record
and resolver answers, it gives the same result, byte for byte.

Start's error, when it can't finish for a reason that isn't the run's,
says what the coordinator does next:

- **A plan or a record that doesn't fit** wraps `compile.ErrMismatch`:
  starting again gives the same error, so the coordinator fails the run,
  with `run_record_mismatch`.
- **A plan of another release's version** wraps `plan.ErrVersion`: a
  replica of a release that reads it starts it
  ([Plans across versions](#plans-across-versions)), and one that doesn't
  leaves the run as it is, saying so in its log (`run_plan_newer`).
- **Any other**, a resolver that couldn't answer, as the Inputs
  resolver's, which wraps `compile.ErrUnanswered`, or a try that ran out
  of time, the coordinator starts the run again after, waiting longer each
  time, from 2 seconds to 2 minutes: 8 times, in about 4 minutes, after
  which it fails the run with `run_start_unanswered`. Its log says which
  kind of try failed, never what the error says, which can quote a
  secret. A try cut short because the coordinator stopped doesn't count.

Decoding the plan or the secret record before Start fails alike: with
`plan.ErrVersion` for another release's version, or for a plan whose
trees name what a newer release's format has, and with
`plan.ErrMalformed` for what no release encoded, or this one can't read
as its own version, which decoding again gives too, so the coordinator
fails the run, with `run_malformed`.

What Start gave that run state holds, the coordinator stores encoded with
the plan's version (`compile.EncodeStartState`), and reads back as
strictly as a plan, in the transaction that seals the secret record again
with the rest and completes Start's hopper job.

A coordinator that dies during Start leaves its job to another replica,
which starts it again from the beginning: nothing reads Start's results
until they are stored, and a Start that two replicas run at once stores
its results once. A run's Start can be in a hopper queue of the run's
own, which only the coordinator that created the run works, as `meridian
run`'s is, so that Start reads a variable whose value comes from the
environment from the one the command ran in; its job is then left to the
next process that works that queue. The coordinator that created the run
holds its Start, under a lock on a file beside the store's database, named
for the run, until the run has started, or the process ends: a run whose
creator ended before its Start stays created until `meridian resume` of
the run, which takes the hold over, works the queue, and one whose creator
lives is left to it ([Status, logs, resume and cancel](#status-logs-resume-and-cancel)).
A rerun runs Start again ([Reruns](#reruns)).

## Run data

A run's data is of three kinds, kept apart by who may read them:

| Kind | Holds | Read by | Kept |
| --- | --- | --- | --- |
| [The plan](#the-plan) | What the run does, as the workflow says it, with no secret and no variable's value | Whoever can see the run | With the run |
| [The secret record](#the-secret-record) | What the run was submitted, passwords among it, the variables it read as it started, and what it made from them | The coordinator, and each attempt the part its job reads | Until the run ends |
| [Run state](#run-state) | Jobs, attempts, outcomes, outputs and diagnostics | Whoever can see the run | With the run |

What Meridian makes from a variable or a password is kept only in the
secret record, or sealed under its data key beside it, as a private
placement is ([Run state](#run-state)), so the plan and run state can be
shown to whoever can see the run. A job's outputs and logs hold what its
steps write, and Meridian masks only secrets in them
([Outputs](#outputs)), so a plain variable's value that a step writes
stays as it is
([Variables and secrets](#variables-and-secrets)).

### The plan

The plan is typed Go, encoded as JSON and stored with the run, and doesn't
change once the run is created. A coordinator schedules from the plan and
the secret record alone, and a run resumed by another process or release
continues from the same plan.

It holds:

- **The run:** the workflow's source and revision, its format and metadata,
  the Meridian release that compiled it (`Run.Release`, `plan.Release`:
  the module's version in the build, or `(devel)` for a build that isn't a
  release), which says only which build made the plan, so the plan's
  digest leaves it out ([Reruns](#reruns)), the input values known when the
  run was created that read no variable and no password, the workflow's
  `env`, the run's timeout, `remote` and `sessions`, where the decisions of
  its shape that read a variable or a password are, and the secrets and
  variables Start reads. A revision names the exact text that ran: a Git
  commit, or the SHA-256 digest of the text when no commit has it, as for
  a file with uncommitted changes or a workflow a platform stores.
- **The input definitions**, `on.execute.inputs` as the workflow writes
  them, with their expressions parsed and no values, which Start resolves
  the inputs again with, as Create did. They leave out what can hold a
  password, which the secret record holds and the plan names by its JSON
  Pointer (`Run.Withheld`): each password input's default, a password
  field's of a list's rows among them, and the default of each list whose
  rows have a password input. A person who keeps a password's default runs
  with it as the run's password, which the plan never holds.
- **Jobs**, one per job: dependencies (on all instances of a job, or `:any`
  of them), its `strategy:`, its `if:`, group, resources and `retry:`,
  steps, outputs, timeout and `env`. The secrets and variables a job reads
  follow from its expressions, which `plan.Job.Reads` finds, so the plan
  doesn't list them again.
- **Steps:** what they run (a command, a workflow by its pinned reference,
  or a built-in action), and their `if:`, `env`, `with:`, shell, working
  directory, `retry:`, timeout, `ignore-errors`, `early-cancel` and cleanup.

Expressions are stored parsed, not as strings, with their source kept for
messages: each string in the workflow that holds a `${{ }}` is stored as its
text and the trees of its expressions. What reads a context known only
later, such as `needs`, stays an expression for its phase to evaluate.

Everything else is data, and stays data. What a run was given, such as its
inputs' values and a matrix instance's, is stored as JSON that is never
evaluated. In the workflow's own values, a mapping whose only key starts
with `$`, as a stored expression's does, is stored marked as data. So nothing
a workflow or a run's data holds can become an expression when a plan is
read back, and an explicit `null`, such as a step's `working-directory:
null`, stays distinct from a value that isn't there.

The plan holds no secret values, nor any value made from one. It names the
secrets the run reads, for Start, and `plan.Job.Reads` finds each job's,
for its attempts, among them: the secret variables an expression reads, each of a context's when it reads
every variable of the context, as `vars.*` does; and passwords, by their
paths in the inputs' values, `password` inputs and the password fields of
lists' rows, lists whose rows hold one and are evaluated at Start, and
submitted values that can hold one, such as what a form made from one,
which are kept as passwords are ([Inputs](#inputs)). Of those, it names the
submitted values in the place of what is made from a secret apart, as
derived, since they can be made from a secret or not: they are masked as
what is made from a secret is, rather than as a secret's own text. Their
values are in what the secret record holds of what was submitted. It
names, apart again, the submitted values that can hold a variable's value
that isn't a secret, but no password, which are private, as that value is:
the secret record holds them, and nothing masks them.

A value is named by its path: its context, then the keys that lead to it
from there, a list's items by their indexes, joined with dots, as in
`inputs.creds.0.pass` and `vars.token` (`plan.JoinPath`). A key's own `.`
and `~` are written `~1` and `~0`, as JSON Pointer writes `/` and `~`, so
that a key with a dot, which a submitted value's can have, names its value
too: the key `db.password` of the inputs is `inputs.db~1password`. The
masker finds each secret by its name (`plan.SplitPath`).

Nor does the plan hold the value of an organization or user variable, secret
or not, or a value made from one. People keep secrets in plain variables
too, and a plan is kept with every run's record, where whoever can see the
run can read it. So an input whose value reads a variable, or a part of
whose value does, isn't among the plan's values, all of it, and Start
resolves it again from the definitions; a matrix that reads one stays an
expression, as its job's strategy. The plan names the variables the run
reads that aren't secrets, for Start and for each job, as `org.<name>` and
`vars.<name>`, found in every expression the run evaluates: those it holds,
and each of the input definitions', which Start resolves the inputs again
with. A read of each variable of a context, as `vars.*` is, names the
context, `org` or `vars`. (In format 1, `vars` alone is a word, not a read
of the context.)

What decides the run's shape still reads variables when the run is created:
an input's `hidden`, `ignore` and `optional`, and a job's `needs`. The plan
holds the run's shape, which inputs the run has and which jobs each job
waits for, and, for each decision that read a variable or a password, where
it is, or the job's `needs:` expression; Start decides them again to check
that nothing changed ([Starting a run](#starting-a-run)). What each
condition decided is in the secret record, not the plan: a workflow could
otherwise spell a password out into the plan a bit at a time, one `hidden:`
that tests a character of it for each. Nor does what such an `ignore:`
decided show in which inputs have values: the plan holds an input whose
`ignore:` read a variable or a password, and each input in a group whose
did, out of its values, as private, or as derived when the `ignore:` read an
input, whatever it decided, and Start drops it if the `ignore:` decides so
again. The conditions in such a group are each a decision, whatever they
read, so the plan names the same ones either way; a later condition that
reads such an input reads what a variable or a password decided, and is one
too; and whether an input such a decision decides about is required, Start
decides, so creating the run doesn't fail for one value and not another.
Which inputs the run ignored the plan doesn't name. What a `needs:` decided
shows in the jobs' graph, which the plan has to hold: a `needs:` that reads
an input such an `ignore:` ignored reads it as ignored, both as the run is
created and as it starts, so the graph says what the `ignore:` decided
there, as it says what a `needs:` that read a password itself decided, while
the input's value stays out of the plan. So does a matrix that reads such an
input, which Start expands, in the number of its instances, which are run
state, while their values are private.

Nor does the plan say whether a value given in the place of a default that
reads a variable is what the default gives: it holds such a value out of
its values whatever it is, and the secret record keeps which ones are
(`plan.Record.Means`). So a plan's bytes don't depend on a variable's
value, nor on a password's, but for the jobs' graph and the number of a
matrix's instances above. Create run again on what was submitted, with the
same resolver answers, gives the same plan.

A source's location names the workflow to whoever can see the run, so it
says nothing of the machine the run was created on: a platform gives its
own name for the workflow, and `meridian` gives the workflow's path in its
Git repository, or its file's name outside one.

### The secret record

The secret record holds what a run needs that its plan and its run state
can't: what the run was given, and what it made from a variable or a
password. It is one type, `plan.Record`, which the coordinator keeps for
the run in one place, encoded as a plan is (`plan.EncodeRecord` and
`plan.DecodeRecord`), with the plan's version: a release reads the records
of the plans it reads, and refuses one with a key it doesn't know, one
that names no version, and one whose submitted values aren't a mapping
(`plan.ErrMalformed`). Each phase adds its part:

- **Create** (`compile.Created.Record`): what was submitted, as it was
  given, passwords among it (`Submitted`); the parts of the input
  definitions that can hold a password (`Withheld`); what each condition
  of the run's shape that read a variable or a password decided
  (`Decisions`); which parts of what was submitted mean the expression a
  form made them from, as Create decided, with the expression's place
  (`Means`); and the digest of the plan it made with them (`PlanDigest`),
  which Start checks, and which leaves out the release that compiled the
  plan ([Reruns](#reruns)). Start resolves the inputs again from these.
- **Start** (`compile.StartResult.Keep`): the variables it read, plain
  and secret, and the private half of the run's values (`Run`, as
  `RunValues.Split` gives it); the run's timeout, when it is private
  (`TimeoutSeconds`); and each matrix whose instances are private
  (`Expansions`), which Ready adds to for a matrix it expands.
- **Each job, as it ends** (`plan.JobResult.Split`): its private outputs
  and its steps' outputs that hold a secret (`Jobs`), by the key of the job
  or of its matrix instance, which the jobs that need it read
  ([Outputs](#outputs)). Each job's is sealed as a part of its own, stored
  beside the job, so that a job's end seals and writes only its own.

The coordinator encrypts and stores it.

- **Encrypted.** The coordinator stores it beside the run, in the same
  database, encrypted with AES-256-GCM under a data key of the run's own:
  32 bytes from `crypto/rand`, made when the record is first stored, which
  the Keys interface wraps and the store keeps beside the record. Keys is a
  platform's key management service, or locally a key file only the user
  can read. The record's encoding is encrypted with a random nonce
  (`cipher.NewGCMWithRandomNonce`), after a byte that says how it was
  sealed, so that a later way can be told from this one. A later version
  is read before the record is authenticated, and leaves the run to a
  later release without asking Keys: one byte written to the store can so
  leave a run waiting, which costs only what deleting the record would.
  Each time the coordinator adds to the record, as Ready does a matrix it
  expands, it encrypts the whole record again under the same data key,
  with a new nonce, at most 2^24 times for a run (`record_reseal_limit`),
  which keeps the chance that two of the data key's nonces are the same
  below 2^-48. Parts kept apart from the record take nothing from that
  bound (below). Neither the data key nor what the record holds is ever in
  an error, and nothing decrypted is kept between calls. Both are cleared
  once used, as far as Go lets them be: not AES's key schedule or GCM's
  hash key, inside the cipher, the buffer the record is encoded in, which
  the JSON encoder keeps for reuse, or the record's strings
  (`coordinator/internal/seal`).
- **Bound to its run.** The record's associated data is
  `meridian/record/1`, the store's ID, the run's and the record's
  generation, written in decimal, each after a NUL byte, and Keys wraps the
  data key for the two IDs joined with a `/`. Neither ID can hold a NUL or
  a `/`, so neither reads two ways. The store's ID is random, made once
  when the store is, so a run ID that a store reuses, as one whose runs are
  numbered would after its database is replaced, can't open another
  store's record. That binding is mandatory, and fails closed: a record
  that doesn't decrypt as its run's fails the run, with
  `record_unreadable`. It is what ties a record to its run, as the plan's
  digest (`PlanDigest`) ties it only to its plan: two runs of one workflow
  with the same plain inputs have the same plan, and Start can't tell
  their records apart. A problem from Keys, such as a wrapped key that
  doesn't unwrap for the run, or the local key file gone
  (`key_file_missing`), fails the run too; any other error from Keys is
  tried again.
- **The latest only.** A record's generation is 1 when the run is created,
  and one more each time it is encrypted again. The coordinator keeps the
  current generation with the run's own state, apart from the record, and
  opens the record only as that generation, so a record that a later one
  replaced, put back in the store, fails the run as one changed does. A
  store restored whole, the run's state with its record, is the store as
  it was, which this can't tell apart.
- **Parts apart from it.** What the record holds of each job (`Jobs`) is
  kept as a part of its own, beside the job, so that a job's end seals and
  writes only its own: it neither opens the record nor holds the store's
  lock while Keys unwraps the data key. Any other secret the coordinator
  keeps apart from the record is sealed the same way, with a purpose of
  its own. A part is encrypted with AES-256-GCM and a random nonce under a
  key of its own, which HKDF-SHA256 (NIST SP 800-56C, inside Go's FIPS
  140-3 module) derives from the run's data key, with no salt and the info
  of the part's purpose, the store's ID, the run's, the part's ID and its
  generation, joined with NUL bytes; the info is the part's associated
  data too. Its purpose is a versioned label, as `meridian/job-record/1`,
  and its ID its own among the run's parts of that purpose, as a job's
  key. So a part opens only as the latest generation of its own purpose,
  ID, run and store: never as the record, as another job's part, as a part
  of another purpose, or as one of its own older generations. A part's
  generation is 1 when it is first stored, and the coordinator keeps it
  with what the part is of, apart from the part, as it keeps the record's,
  and stores a part only at the generation after the one it read. Each
  derived key seals one generation of one part, once, or again only for a
  write that raced another, so the parts, however many a run's jobs and
  attempts make, take nothing from the bound on the data key's nonces
  (`seal.Sealer.SealPart`).
- **Digests keyed, or none.** The store keeps nothing made from what can
  hold a secret where it needn't: of an attempt's end, which holds the
  job's outputs, it keeps no digest ([The worker API](#the-worker-api)).
  Where it must know a value again, as a request for an action, which
  holds a step's `with:`, is answered once, it keeps an HMAC-SHA256 of
  it, never a plain SHA-256, against which whoever holds the store's file
  could test a guess at a short secret. The HMAC's key is one that
  HKDF-SHA256 derives from the run's data key, with no salt and the info
  of the digest's purpose, a versioned label, as
  `meridian/action-digest/1`, the store's ID, the run's, and the digest's
  own ID among the run's of that purpose, as an action's attempt, step and
  try, joined with NUL bytes (`seal.Sealer.Digest`). So the key is never
  in the store, only the data key wrapped, and every replica derives it
  again through Keys, after a restart and in `meridian resume` too. It
  goes with the data key as the run ends, after which no one can check a
  digest of the run, the coordinator included, and nothing needs to: a
  request that a digest answers once is one of an attempt that still
  holds its job. A key of the store's own, which Keys would wrap once,
  would outlive every run, and so let a copy of the database, with that
  key, test guesses at what runs long ended held, which the record's
  erasure is there to prevent. Earlier releases kept plain SHA-256
  digests of both, of ends and of actions' requests. The store erases
  them as it is migrated, each time a process opens it, and each time a
  run ends, so that those an earlier release still running on the
  database goes on writing don't stay; an index of the rows that hold one
  keeps each erase to them. An action whose digest an earlier release
  kept can't be checked, and takes any request for its try as a repeat:
  the attempt's worker, which alone holds its credential, sends one.
- **Read in parts.** The coordinator reads it for Start and Ready, and gives
  each attempt, when it is claimed, the values its own job reads
  ([Variables and secrets](#variables-and-secrets)).
- **Deleted when the run ends**, in the transaction that ends it. Nothing
  in it is needed once the run has ended. A rerun is the stored plan and
  what is submitted again, from which Create makes the rerun's record
  again ([Reruns](#reruns)). Each of
  the record's parts is either what a person or a form gives, the
  workflow's own text (`Withheld`, which the pinned workflow holds), or
  what Create and Start make again from them.

  On any store, the record and its parts are sealed under the run's data
  key, and the data key, wrapped by Keys, is deleted with them. Without
  the key that wraps it, which Keys holds, a copy of the database can't
  open what of them remains. That key outlives the run, so deleting the
  wrapped data key isn't shredding: a copy of the database taken before
  the run ended, or a store that kept the deleted rows, opens the record
  with it. Locally the key is `secret-record.key`, in the state directory
  beside the database, `meridian.db`, so back up the state directory
  without the key file; while a run goes on, a step the `local` backend
  runs can read both ([Security](#security)).

  On SQLite, deleted means erased from the store's files too, not only
  from its tables, so that someone who later has both a copy of the
  database and the key that wrapped the record's data key can't open it:
  the store writes with `secure_delete` on, which overwrites with zeros
  what it deletes or replaces, the sealed record, its jobs' parts and its
  wrapped key among them, rather than leave it in free space. Once the
  transaction that ends the run commits, the store checkpoints the
  write-ahead log into the database and truncates it, so the log keeps no
  copy either. If a reader or a writer in another process holds the log,
  that checkpoint can't truncate it, and the next one does: when the next
  run ends, when the store is next opened, or in hopper's maintenance. A
  backup of the database taken before the run ended holds the record,
  sealed, as it was.
- **Stored, not held in memory.** Coordinator replicas keep no state between
  calls, and `meridian resume` picks a run up in another process, so a run's
  secrets live where its state does, and the store holds them only
  encrypted.

### Run state

Run state is the store's: hopper's jobs, and Meridian's rows beside them
([On hopper](#on-hopper)).

- **The run's** holds its plan, with the plan's version and digest beside
  it, so a replica tells whether it reads the run without decoding it; the
  run's diagnostics; its state and outcome; its stop record, once a cancel
  or its timeout stops it
  ([Cancellation, timeouts and cleanup](#cancellation-timeouts-and-cleanup));
  and the secret record's generation, apart from the record itself
  ([The secret record](#the-secret-record)).
- **Each job instance's** holds its state, its outcome, the part of its
  result that run state holds (`plan.JobResult.Split`), a matrix job's
  instances (`plan.Expansion.Split`), what it was readied with
  ([A job's lifecycle](#a-jobs-lifecycle)), and the generation of its part
  of the secret record.
- **Each attempt's** holds its lease and fencing number
  ([Leases and fencing](#leases-and-fencing)), the hashes of its bootstrap
  token and its credential, and its audit record
  ([Operations](#operations)).
- **The run's events**, each change to it in order, which every process
  that follows the run reads, whichever process made the change: its
  start; a job ready, placing, running and finished; each step's start and
  end, as the attempt's worker reported them; an attempt's end, however it
  ended; the run's stop, once a cancel or its timeout stops it; and the
  run's end. Each is added in the transaction that makes the change,
  numbered after the run's last, and names the row that
  changed rather than copying it, so an event shows only what that row
  shows: never a private placement's member, which only the record holds
  ([Following a run](#following-a-run)).

It holds no secret that Meridian can recognize: a job's outputs are checked
against the secrets its attempt received before they leave the worker
([Outputs](#outputs)), and diagnostics never show a value made from a
variable or a password. Nor does where a job runs, when its `runs-on:`,
`resources:`, `retry:` or `timeout:` reads one: Ready says which of them
did (`plan.ReadyJob.PrivateFields`, and `SecretFields` for a secret), and
the coordinator keeps them out of run state: Ready gives them again from
the secret record whenever the coordinator needs them, as the record holds
the run's timeout when that reads one, and the placement made from them,
the member chosen and why each other wasn't, and the backend's own name for
the placement, which can say where it is, as a pod's name can, is in the
attempt's row, sealed as the secret record's parts are: the member, before
its backend is asked, so that a coordinator that stops then finds the
placement again, and then the backend's name too, sealed again as the
part's next generation once the backend answers. Run state holds
only the digest of the worker binary the
placement started, which says the attempt is placed. What such an attempt
reports that can name the member, the problem it ends with and the
diagnostics of its end and of its steps' events, is stored with what it
shows of values masked (`problems.MaskProblem`, `problems.MaskDiagnostics`).

### Plans across versions

A plan has one reader, as a workflow does. A release reads every plan
version up to its own, and upgrades an older plan in memory as it decodes
it, as the compiler translates an older format, so everything past decoding
sees one version.

- **Every change gets a version.** A plan's version rises with each change
  to its encoding, an addition among them; an addition's upgrade changes
  nothing. Each version's encoding is described by a JSON Schema, kept in
  this repository, so the plan is a contract a platform can read too. The
  trees of stored expressions are part of it, and so are the types the
  worker API's bodies carry of a run: its values, the job Ready
  evaluated, and the results of the jobs it needs. The API's own bodies
  have a version of their own ([The worker API](#the-worker-api)).
- **A plan is of the lowest version that holds it.** Create writes a plan
  at the lowest version that has what it uses (`Plan.MinVersion`), and the
  run's secret record and start state at its plan's, so that a release
  that reads only an earlier version runs every run that version holds,
  and only a run that uses an addition needs a newer release, and newer
  workers. Version 2 adds the pins of a run's called workflows' steps
  (`Plan.Calls`), the variables the tree reads (`Run.TreeVariables`), each
  pin's remote context (`Source.Remote`), and a child run's link to its
  step (`Run.Parent`), with what its secret record holds of the step
  (`Record.Inherited`); and the pins of `parallelworks/checkout` steps, a
  run's own (`Step.Checkout`) and its called workflows'
  (`Plan.Checkouts`). So a run that calls no workflow, and checks out
  nothing, is of version 1. A release reads every version up to its own,
  each by its own schemas, under `plan/schema/<version>/`, and refuses
  what a later version added in an earlier one's plan
  (`plan.ErrMalformed`).
- **The version says who can read it.** The run's row keeps its plan's
  version, so a replica tells whether it reads a run without decoding its
  plan. A replica claims only runs whose version it reads, so during a
  rolling upgrade old replicas leave newer runs alone. A release still
  refuses a plan with a key it doesn't know, rather than run it without
  that part, and one that names no version, which is no release's
  (`plan.ErrMalformed`). Keys match exactly, and a key given twice is
  refused.
- **Decoding is as strict as the schema.** What the schema refuses,
  decoding refuses, a plan's, a ready job's, a secret record's and the
  worker API's bodies' alike, as a job's result a worker reports: a member
  that is always written missing, null where none is written, an integer
  past the bounds its Go type has in every build, and what a member holds
  that the schema says it can't, such as an empty name or an outcome that
  isn't one. Decoding refuses more only where a schema can't say it: an
  integer written as `2.0` or `-0.0`, which JSON Schema can't tell from
  `2` and `0`, which job comes before another, and what a tree names that
  the release doesn't know.
- **What an expression means is its format's.** A stored tree is read the
  same in every version, and evaluated by the rules of the format the plan
  records. A format can gain a context, an operator or a function, or a
  function's arguments, without a new plan version, so a newer release
  can write a tree, of a version an older one reads, that names what the
  older one doesn't know. The older release refuses that plan, or that
  ready job, as one of a newer release (`plan.ErrVersion`, not
  `plan.ErrMalformed`), and leaves its run to a replica that reads it,
  rather than fail the run. A worker refuses such a job as one it can't
  read, and the job is placed again.
- **The release.** The plan records the Meridian release that compiled it,
  for whoever reads the run, but its digest leaves it out, as a rerun's
  comparison does ([Reruns](#reruns)): the same plan from another build is
  the same plan.

A run's secret record is versioned with its plan, and encoded as it is
(`plan.Record`): a release reads the records of the plans it reads, and the
rules above are the record's too.

Version 1 is frozen: every change to the plan's encoding from here on
raises the version, an addition among them. Its schemas are in
`plan/schema/1` (`plan.Schemas`): a plan's (`plan.schema.json`), whose
definitions hold the other types'; a ready job's, a secret record's, a
run's values' and a job's result's, which name them; and the worker API's
bodies' for runs of the version (`workerapi.schema.json`), which refer to
them. They refer to each other by their names, so they resolve wherever
the directory is. Version 2's are in `plan/schema/2`: it holds what child
runs add ([Workflows a step calls](#workflows-a-step-calls)), which goes
on adding to it until a release writes it, which freezes it as version 1
is. The bodies' schema follows the API's own version rather than the
plan's: each plan version's describes the bodies of the API's newest
version alike, and says of a member a later API version adds which version
that is. Tests check that every encoded type's members are what its schema
describes, that golden files setting every member, and what Meridian
encodes for every conformance case, fit the schemas, that decoding agrees
with the schemas on each member of the goldens and the bodies left out,
null, or a number past its bounds, and that every member has the version
that gave it, the plan's in plan's since table and the API's in the worker
API's, so that a member added later can't go in without a version. A value
is named one way throughout, by its path with its context
(`plan.JoinPath`), as in `inputs.region`, in the plan, the secret record
and a run's values.

## Expressions

An expression is evaluated in a phase, which decides what it can read
([Phases](#phases), [Contexts](#contexts)). This section says what an
expression means.

### The language

Format 1's expressions are the ones ACTIVATE evaluates, as its documentation
describes them: JavaScript's operators, with its rules for comparing and
converting values. The `expr` package parses them into a tree, which is what
the plan stores, and the conformance suite's `expressions` area pins what
each one means. [Expressions](expressions.md) gives their grammar, limits and
conversions in full. A later format can change the language: the parser
reads each expression in its workflow's format.

- **Values** are JSON's: null, booleans, numbers, strings, lists and
  mappings. Numbers are 64-bit floats, as in JavaScript.
- **Literals** are numbers, strings in single, double or back quotes, `true`,
  `false`, `null`, and lists and mappings written as JSON.
- **References** read a context by path: `inputs.cluster.ip`,
  `inputs['my-key']`, `inputs.hosts[0]`, the slice `inputs.hosts[1:3]`, and
  `inputs.groups.*.name` for each group's name. A path can follow a group or
  a call too: `fromJSON(x).*.name`. A path that leads nowhere is
  null, except in the contexts that hold strings, where a missing name is
  `""` ([Contexts](#contexts)). A string that holds JSON is read as its
  value, since outputs are strings.
- **Words.** A word that is none of these is a string: `inputs.mode == debug`
  compares with `"debug"`. The compiler warns about a word that looks like a
  reference, such as `input.mode`, and about a word that decides a
  condition, since a word is never false: `if: ${{ !completed }}` never runs.
  For a word that was once one of ACTIVATE's `if:` keywords, the warning
  names the function that replaced it. A hyphen is a minus sign, so the
  compiler also warns about a reference that subtracts a word with nothing
  between them, such as `inputs.pw-user`, which is NaN; a name with a hyphen
  in it is read in brackets, `inputs['pw-user']`.
- **Operators**, from loosest to tightest: `? :`; `??`; `||` and `or`; `&&`
  and `and`; the comparisons `==`, `!=`, `===`, `!==`, `<`, `>`, `<=` and
  `>=`, with the word operators `in`, `includes`, `startsWith`, `endsWith`
  and `get`; `range`; `+` and `-`; `*`, `/`, `//`, `%` and `mod`; `**`, `^`
  and `exp`, which group from the right; and `split` and `join`. As in
  JavaScript, `??` can't be mixed with `||` or `&&` without parentheses. The
  unary `!` and `-`, and the word operators `length`, `upper`, `lower`,
  `trim`, `keys`, `values`, `abs`, `floor`, `ceil`, `round`, `fromJSON` and
  `range`, bind tightest.
- **Functions** are GitHub Actions' `contains`, `startsWith`, `endsWith`,
  `format`, `join`, `toJSON` and `fromJSON`, with its meanings, except that
  `contains` also finds a key in a mapping, plus the status functions and
  `case(condition, value, ..., default)`. A word operator gives what its
  function gives for the same arguments: `in` and `includes` are `contains`,
  and `startsWith`, `endsWith` and `join` are the functions of those names.
- **Lambdas.** `map(list, x => expr)` makes a list of what its lambda gives
  for each item, and `filter(list, x => cond)` a list of the items it gives a
  true value for. With `(x, i) =>` the lambda reads the index too, or the
  key in a mapping; both go through a mapping's values in the order of its
  keys. `filter` goes by the language's truthiness, so it keeps the text
  `"false"`: compare, as in `h => h.enabled == true`, to keep only `true`. A
  parameter is known only in its lambda's body, and can't take a name that a
  word already means something by, such as a context's.
- **Strings with expressions in them.** A string that is one expression and
  nothing else has the expression's value, of any type. Otherwise each
  expression is replaced by its value's text: a string as it is, null as
  nothing, and anything else as JSON. Null is nothing wherever a string is
  made of it, as in `'a' + null`. A mapping's keys are in sorted order here,
  and wherever else their order shows. A `${{` without its `}}` is a syntax
  error.
- **The workflow around an expression.** A relative reference reads the
  workflow near the expression: `.hidden` is the key `hidden` beside it, and
  each further dot, or `../`, goes up a level. A word that reads as a number,
  such as `.5`, is the number, as in JavaScript, so a key named with digits
  is read in brackets: `.[5]`. `inputs.x.type` reads input `x`'s definition,
  as its value has no type, and in a list input's template, `[index]` is the
  index of the item.

Only expressions written in the workflow are evaluated. A value an
expression reads, such as an input's value, a variable or an output, is data
even if it holds `${{ }}`, and so is a string an expression builds. An
input's default is part of the workflow, so a default that is an expression
is evaluated.

Evaluation is pure: an expression can't read files, start processes or reach
a network. Depth, work and the size of a result are bounded, so a workflow
can't make the compiler or coordinator do unbounded work: an expression's
tree nests at most 200 levels, `range` makes at most 10,000 numbers, none
beyond ±2^53, and a template, a `${{ }}` alone or text with `${{ }}` in it,
builds at most 8 MiB of strings and lists and does at most 32 MiB of work.
Work is counted in bytes, as what a template builds is: 8 for each step,
such as an operand, an operator, a segment of a path or a run of a lambda,
the size of each value an operation such as `==` or `keys` reads, and what
the template builds. These limits are the language's, and
[Expressions](expressions.md#limits) says how each operation counts toward
them. They depend only on the expression and the sizes of the values it
reads, so every implementation of the format counts the same, and a
workflow passes or fails them wherever it runs. A template doesn't count
what a template it reads builds or does, such as an input's default: that
has its own count.

Meridian adds a safety net of its own, which isn't part of the format: the
templates it evaluates together as a group build at most 64 MiB between
them, and do at most 256 MiB of work, eight templates at their limit. A
group is all of what decides a run's shape when it is created, its input
defaults and conditions and its jobs' `needs:`, or every template in one
value, such as a step's `with:`. The net bounds what a group does
however many templates it has, and each of these caps, for every group, is
at least one template's limit, so a template on its own that keeps to the
language's limits never reaches it. A group that passes one fails with
`expressions_build_too_much` or `expressions_too_much_work`, which say the
group did too much together, rather than with an expression's error. What a
template has read or built counts toward its group even when the template
then fails its own limits, so that templates that each fail after reading a
large value can't together do more than the group may, and a template that
starts once its group has passed a cap fails before it does anything. A
string that a template reads as the JSON it holds is counted before it is
parsed, so one the template can't afford to read isn't.

The net bounds time too, as a backstop that work within the limits never
reaches. The time is the caller's: a group's templates stop evaluating when
the context of the phase that evaluates them is done, and a group evaluated
without a deadline spends at most 10 seconds of wall-clock time, together.
The work count prices each step and each read by what it costs, so a
template at its limit takes well under a second, and a group at its own a
few seconds. The cap isn't a limit a workflow is written to, since how long
evaluating takes depends on the machine. The evaluator reads the clock as
each template starts, one that another reads among them, and every 256
steps its group takes, each node it evaluates and each run of a lambda, and
a group that passes the cap fails with `expressions_too_slow`. Today the cap
is 10 seconds, whatever the caller's deadline.

A secret's own text is masked wherever it appears in logs and outputs,
however short, and so is the text of what an expression made from it,
under the rules of [Variables and secrets](#variables-and-secrets): an
evaluation can say whether it read a value its scope marks
(`Template.EvalRead`), and one that read a secret is marked whatever it
made of it, whether `${{ vars.token }}`, `${{ vars.token * 2 }}` or a
condition the secret decided. The mark is conservative: anything a template
reads from a secret marks all of what it gives, and each `${{ }}` in it
that read one marks what that expression gave (`Template.EvalParts`). What
is masked is text, so a boolean or a number a secret decided isn't.

## Interfaces

Everything that differs between a laptop and a platform is behind an
interface, and the compiler, coordinator and worker see only the interface:

| Interface | Provides | Local implementation |
| --- | --- | --- |
| Inputs | Input types beyond the built-in ones: what a value of one names, as a cluster's details for its name | `execution-groups`, the user's groups by name; `compute-clusters`, the clusters of `clusters.yaml` by name, and the workspace, this machine; a scheduler's partitions, accounts, QoS and queues, Kubernetes clusters, namespaces and volume claims, and an organization's groups, each the name as it is given, as format 1 gives it; the platform's other resources, as its regions, come later |
| Variables | Organization and user variables, plain or secret, by name | A variables file in Meridian's config directory, in which a secret names the environment variable or file that holds its value |
| Keys | The key that wraps each run's secret record's data key, for the run, by its store's ID and its own | A key file in Meridian's state directory, which only the user can read |
| Groups | Execution groups | The user's config |
| Sources | Workflows that `uses:` names, and the repositories `parallelworks/checkout` names, pinned and read by their pins | Git, through the `git` command, 2.29 or later |
| Trees | The trees of the commits checkouts were pinned to, which the coordinator serves to their steps' workers | The copies of the repositories that Sources pinned them from, in Meridian's cache directory |
| Sessions | Interactive sessions a workflow serves, which it makes and updates | None |
| Logs | Storing and reading step logs | Files in Meridian's state directory |

A name the local Inputs gives as it was given means what it means on a
platform: a Kubernetes cluster's is the name the platform knows it by. A
step that gives one to the platform's own CLI, as `pw kube auth` does in
every known workflow with such an input, needs that CLI installed and
signed in to the platform as it runs, standalone as on the platform:
Meridian gives the step the name, and doesn't stand in for that CLI.

The compiler's resolvers, Inputs, Variables, Groups, Sources and Sessions,
are interfaces in `compile`, the coordinator's, Keys, Trees and Logs, in
`coordinator`, and the backends' `Driver` in `backend`. Built-in actions
have no interface of their own: each goes through the one that owns what
it acts on ([Built-in actions](#built-in-actions)). Whoever shows a run
as it happens, as the command's progress display does, follows its events
and its logs (`Coordinator.Follow`, below), rather than being told of
them. A resolver may be
missing. Then the workflow features that need it fail to compile, with an
error saying which interface was missing; without Trees, a checkout's
step fails as it runs, saying so (`resolver_missing`).

Resolvers get the caller's context, so cancellation, deadlines and traces
reach them. They report what is wrong as a problem, a kind and its params
([Compiling](#compiling)), rather than as text, so that a platform's reason
reaches each reader in their own language. Create reads the variables
through Variables, checks sessions with Sessions, the execution groups and
environments a job's `runs-on:` writes out with Groups, and pins the
workflows `uses:` names with Sources; Start reads the variables again and
makes the sessions. A caller without a Variables or a Sessions resolver,
such as an editor or a test, can give the variables and the sessions
context as mappings, in `Options.Contexts`, as it gives `remote`. A
resolver refuses what it is given with a problem of one of the workflow's
kinds of error, as the Inputs resolver refuses a value with
`input_rejected` and its reason, or a kind of its own, which is the run's
problem; any other error says it couldn't answer, and Create or Start
returns it, so the coordinator can try again. The value the Inputs resolver
is asked for can be private, as one made from a variable or a password, so
it is held apart: any problem it gives refuses the value, one of a kind
that isn't the workflow's error, as a warning's, being `input_rejected`
with the problem's text as its reason, masked as any refusal of a private
value is; and for any other error, Create and Start return one that wraps
`compile.ErrUnanswered` and names the input, with the resolver's text only
where the value can't be private, and nothing else of the resolver's
error, so that it can't be taken for a plan that doesn't fit
(`compile.ErrMismatch`). A resolver's error that says it couldn't answer
mustn't quote the value all the same.

A uses: names a workflow by what format 1 writes (`plan.SourceRef`), as
ACTIVATE reads it: the `uses:` itself, as
`github/<owner>/<repository>@<revision>`, with nothing after the
repository's name, or `gitlab/<group>/<project>@<revision>`, where every
name after `gitlab/` is the project's path, its subgroups among it; and in
the step's `with:`, `$yaml`, the workflow's file in the repository,
`workflow.yaml` unless it names another, and `$host`, the GitLab server that
holds it, `gitlab.com` unless it names another. As ACTIVATE reads them
(`SourceRef.Normal`), space after a `uses:`, and on GitLab after `gitlab/`
and around `$host`, goes; the revision is what follows the first `@`, and
isn't empty; one slash and then `.git` after the repository's name go; a
GitHub owner's name has letters, digits and hyphens, a repository's, group's
or project's dots and underscores too, and a revision slashes too, and no
`..`; a `$host` is a DNS name of one label or more, with no port, in any
case; a `$yaml` that starts with a slash is from the repository's root, as
ACTIVATE fetches the file, which drops every slash before it on GitHub and
one on GitLab; and a GitHub repository's `$host`, or a `$yaml` or `$host`
that isn't text, is read as if it weren't there, which the schema refuses,
as ACTIVATE's does; and a `$yaml` with `..` among its names, which would
leave the repository, names no workflow, as ACTIVATE refuses it on GitHub
and finds no such file on GitLab. Create checks a reference by these rules
(`SourceRef.Check`) before it asks Sources, and with no Sources, so that
every Sources, a platform's as the local one, reads only a reference
format 1 takes, and none decides what format 1 means: what is wrong with a
`$yaml` or a `$host` is reported at it (`source_with_invalid`), and with
the rest of a reference at the `uses:` (`source_ref_invalid`). A Sources
can refuse more, with the same kinds. Besides what format 1 refuses, the
local Sources refuses, for git and its cache, a name, a revision or a
server that starts with a hyphen, which git could take for an option; a
revision git refuses as a branch's or a tag's name, one with a name in it
that is empty, starts with a dot or ends in `.lock`, or that ends with a
dot, which ACTIVATE takes and then finds on no server; a group's name that
ends in `.git` or `.lock`, or starts with a dot, which would be where
another repository's copy, its lock or a copy being made is in the cache,
where a repository's name, as `.github`, can start with one; and a `$yaml`
with an empty name or `.` in it, or a control character, `./` among them,
whose reading by ACTIVATE can't be confirmed.
Two steps whose references read the same are pinned once, to one commit,
however they write it. A pinned workflow keeps its reference beside its
commit and digest (`Source.Ref`), so Sources reads it again by it, and gives
the `remote` context a child run reads from it, `remote.repo`,
`remote.branch` and `remote.yaml`, and its `Location` names its server, its
repository, its revision after an `@` and, after a colon, its file, as
`gitlab.com/group/project@main:workflow.yaml`. Git reaches a server with the
credentials of whoever runs it, so a Sources reaches only the GitLab servers
registered where the workflow runs, as the schema's "registered GitLab
server" says, and never one that only a workflow names
(`source_server_unknown`). ACTIVATE requires gitlab.com to be registered
too; `meridian` always reaches gitlab.com.

Where ACTIVATE reads a `uses:` otherwise, no known workflow depends on it:

- **Expressions.** ACTIVATE evaluates a `uses:`, `$yaml` and `$host` with
  the run's inputs before it reads them. Meridian pins the workflow as the
  run is created, so each must be written out (`source_not_known`); a
  `uses:` that is an expression whole the schema refuses. 0 known.
- **Short commits.** git fetches a commit by its full SHA, of 40 or 64
  characters, where ACTIVATE's GitHub and GitLab APIs take a short one too:
  `@abc1234` is `source_revision_not_found`, whose message says so. 0
  known.
- **The remote context.** A child run named without an `@` has no
  `remote.branch`, where ACTIVATE gives the name of the repository's
  default branch, and none has `remote.thumbnail`, the step's
  `$thumbnail`. No known workflow reads `remote`.
- **Space before a `uses:`.** Both read a `uses:` with space before
  `github/` or `gitlab/` as no reference to a repository: ACTIVATE refuses
  its prefix, and Meridian takes it for a built-in action. 0 known.

## Durable execution

### On hopper

Durable state is [hopper](https://github.com/parallelworks/hopper)'s, and
Meridian's own tables beside hopper's, in the same database and in the same
transactions: SQLite, through hopper's pure-Go driver, for `meridian`, and
Postgres for a platform, which isn't built yet. hopper holds a run's jobs,
and schedules and retries them; Meridian's tables hold what hopper doesn't
model, the plan, the secret record, the texts of the workflows the run's
tree pins, each once by its digest, whatever runs pin it, and the state of
each job and each attempt ([Run state](#run-state)). Meridian uses only what hopper has:
workflows of jobs with dependency edges, jobs that snooze until a time
without using an attempt, and jobs that complete in their caller's
transaction.

- **A run is a hopper workflow.** One transaction gives the run its ID,
  stores its plan, its secret record, sealed, the texts of the workflows
  its tree pins, and its state, and inserts the workflow, so a run is
  stored before anything executes, and either all of it is stored or none
  of it is. The workflow's first job is Start
  ([Starting a run](#starting-a-run)), which every other job waits for, and
  its last the run's finisher, which waits for every job, and for a root
  every run of its tree ([Workflows a step calls](#workflows-a-step-calls)),
  decides the run's outcome, and deletes the secret record in the
  transaction that ends the run.
- **A pinned text is kept by its digest alone.** The store keeps each text
  once, whatever runs pin it, and which runs pin it; where a run found it
  is its plan's, so a run that pins the same bytes from elsewhere, as a
  public copy of a private repository's workflow, learns nothing of where
  another found them. The texts stay as long as their run does, as its
  plan does, rather than end with it as the secret record does: they hold
  no variable's or password's value, and a rerun, which runs the plan's
  pins, can read them. Nothing deletes a run yet; what comes to will
  delete the run's links to its texts with it, and then each text no run
  pins.
- **Each job has a supervisor**, a hopper job that waits for its needs'
  supervisors, so `needs:` becomes hopper's dependency edges. The
  supervisor drives its job from Ready to its end: it readies the job,
  places its attempts and waits on them, and ends the job. It waits by
  snoozing until its next deadline, such as a lease's, and a worker's report
  that changes the job, stored by whichever replica the worker reached,
  wakes it at once. Before it snoozes, it reads again whether anything
  woke it meanwhile, and looks again if so; hopper applies the snooze once
  it has returned, so a wake that commits in between finds it still
  running, and is tried again, briefly apart, until hopper no longer runs
  it: a few times as the waker waits, and then in the background, further
  apart, for up to 30 seconds, since hopper records a snooze in batches,
  and late on a loaded host. So is a wake of a marker of a need on any
  instance ([Matrices](#matrices)). Each instance of a matrix job
  has a supervisor of its own, which the job's inserts
  ([Matrices](#matrices)).
- **Meridian decides what a dependency's outcome means.** Its edges let a
  dependent run whatever its dependencies' outcomes were. A job ends with
  Meridian's outcome (success, failure, cancelled or skipped), stored with
  its outputs in Meridian's tables, where they stay as long as the run, as
  its reruns need them: hopper keeps a finished job's result only in its
  history, which its retention prunes. The coordinator evaluates the
  dependent's `if:` against it. hopper cancels nothing on Meridian's
  behalf.
- **`needs: <job>:any`** waits for any one instance of a matrix job to
  succeed or be skipped by its own `if:`, or for every instance to end. A
  marker job stands for it, which ends then, and which the dependent's
  supervisor waits for with a plain edge, so hopper needs no edge of
  another kind.
- **A supervisor's own error**, a store's or a bug of the coordinator's,
  hopper retries, with backoff and a bound. A job whose supervisor hopper
  gives up on fails, with `coordinator_job_failed`, and the run goes on to
  its end: the first job that needs it fails it as that job is readied, or
  else the finisher does. Either withdraws its attempt that hadn't ended,
  and has the attempt's driver cancel its placement.

### A job's lifecycle

```text
pending ──▶ ready ──▶ placing ──▶ running ──▶ succeeded | failed | cancelled | skipped
 (needs)              (backend)   (worker)
```

- **pending:** waiting for its dependencies.
- **ready:** its dependencies have finished. The coordinator runs Ready: it
  expands a matrix that reads `needs`, and evaluates the job's `if:`, so a
  false `if:` skips it here. Otherwise it evaluates the rest of what was left
  for this point, such as a `runs-on:` that uses `needs`, and picks a member
  of the job's group ([Placement](#placement)). Ready's errors fail the
  job, and its warnings, as `job_ssh_host_empty`, are kept with the job,
  which runs. The coordinator runs Ready
  again whenever it needs the job, as when it places it and each time a
  worker claims it, with what the job was readied with kept: whether the
  run had been cancelled then, and which instance each `:any` need took. So
  every claim of an attempt gets the same job, byte for byte.
- **placing:** the member's backend is starting a worker: a scheduler job
  waiting in a queue, a pod being scheduled, a process starting over SSH. No
  user code runs yet, and the time this takes doesn't count against any
  lease.
- **running:** a worker has claimed the attempt and holds its lease.
- **The end:** the worker reports the outcome, or the coordinator records
  one when the worker is lost or the job is cancelled.

### Matrices

One function expands a matrix, `plan.Expand`, which takes a job's
`strategy:` and the contexts it reads, and gives the job's instances. The
matrix's keys other than `include` and `exclude`, in the order of their
names, as ACTIVATE orders them, give every combination of their values, the
first key changing slowest. Each `exclude` entry removes the combinations
that have all its values. Each `include` entry then adds its values to every
combination whose matrix values it matches without changing one, or, when
it fits none, is a combination of its own, as in GitHub Actions. ACTIVATE
differs for an `include` entry that names no matrix key: it makes it a
combination of its own, where GitHub Actions adds it to every combination.
Format 1 keeps ACTIVATE's for now. Whether to take GitHub Actions' as a bug
fix ([The workflow format](#the-workflow-format)) is open: of the 218
known workflows ([Bug fixes to format 1](#bug-fixes-to-format-1)), none
has an `include` entry that names no matrix key, nor any `include` at all. An entry also matches the combinations earlier entries made, and
two values match when they are equal, or a string and a number or a
boolean whose text it is, as in ACTIVATE. Each instance's key is the job's
name, `#` and its index from 0, as in `build#2`.

- **Where.** The coordinator expands every matrix: at Start, one whose
  values are known by then, written out or read from inputs or variables,
  and at Ready, one that reads `needs`. The compiler expands none, and
  checks only that the jobs and outputs a matrix reads exist, and that
  what reads `matrix` names a key a matrix written out has; it keeps every
  `strategy:` in the plan. Checking a matrix whose values are known with
  `plan.Expand` as the run is created, which would refuse such a run
  rather than fail it as it starts, isn't built.
- **A matrix job's `if:`** that doesn't read `matrix` is evaluated before
  the matrix is expanded, so a job it skips has no instances. One that
  reads `matrix` is evaluated for each instance: format 1 allows it, as
  GitHub Actions doesn't, and the compiler accepts it.
- **One supervisor, and one for each instance.** A matrix job has one
  supervisor ([On hopper](#on-hopper)), whichever phase expanded it. When
  it is ready, its supervisor inserts a supervisor for each instance, and
  ends the job once they have all ended, with their combined outcome and
  outputs (`plan.Combine`). Jobs that need it wait for all its instances.
  So a matrix written out and one sized from earlier results run the same
  way.
- **`fail-fast` and `max-parallel`** belong to the strategy: the coordinator
  cancels the remaining instances when one whose steps ran fails or is
  cancelled, as format 1 does, though not when one failed before its first
  step, as when its setup did; and places no more than `max-parallel` at a
  time, giving the
  turns in the order of the instances' indexes, where format 1 gives them
  in an order that is random. Every instance that hasn't ended is told
  to cancel its job, with the reason `fail-fast`, whatever its `if:`, as a
  run's cancel stops a job: one whose worker runs, and one not yet placed,
  which is placed still, in its turn under `max-parallel`, with the cancel
  in its first control, which its worker reads before its first step. So
  as in format 1, which starts such an instance with its job already
  cancelled, it runs only its steps that outlive a cancel, and its
  cleanups. The strategy is evaluated as one value, so they
  are as private as the matrix's values: of a matrix whose instances are
  private, the coordinator reads them from the secret record, as run state
  doesn't hold them (`plan.Expansion.Split`).
- **Outcome and outputs.** The job's outcome is its instances' combined, as
  `needs.<job>.result` gives it ([Contexts](#contexts)). Its outputs are
  its instances', by name, and an output that more than one instance sets
  has the value of the one with the highest index, so it doesn't depend on
  which finished last, as it does in GitHub Actions, and did in format 1.
  No workflow can rely on which instance finishes last, so this is made as
  a bug fix ([Bug fixes to format 1](#bug-fixes-to-format-1)). In an
  instance's own steps and outputs, `needs.<job>` is the instance, whose
  outputs are those its own steps wrote, another such fix. As format 1
  reads each instance of a job its dependents need, the job fails for them
  when an instance was skipped because its needs failed, whatever the rest
  did, and is cancelled for them, for `cancelled()`, when an instance was
  cancelled, though another failed.
- **`needs: <job>:any`** waits for any one instance, and `needs.<job>` is
  then the first instance to succeed, or to be skipped by its own `if:`, or
  else, once every instance has ended, a failed one, as format 1 waits
  ([On hopper](#on-hopper)). Its outputs are those of every instance that
  had ended by then, failed ones among them, an output that more than one
  set the highest index's, as format 1 merges them; its result is the
  chosen instance's.
- **Size.** A matrix has at least one instance and at most 256 by default,
  a limit of Meridian's ([Compiling](#compiling)), not the format's. A
  matrix with no instance, or more than the limit, fails where it is
  expanded: at Start, which fails the run, for one whose values are known
  by then, and at Ready, which fails the job, for the rest.

### Workflows a step calls

A step whose `uses:` names a workflow runs it as a run of its own, a child
of the step's run, with its own number, plan, secret record and jobs, which
a coordinator places and follows as it does any run's, and a hopper
workflow of its own. Format 1 runs it inside its caller's step, from a copy
of it embedded in its root run as the root was created; what the workflow
sees of that, its inputs, its environment, its identity, its directory and
how its end becomes its step's, is format 1's, as below. A `uses:` names a
workflow when it starts with `github/`, `gitlab/`, `marketplace/` or
`workflow/`. ACTIVATE also runs a repository's URL as a workflow, and
refuses any other prefix, where Meridian's schema refuses a URL and takes
any other `uses:` for a built-in action; no known workflow has either. The
known workflows make 270 calls, in 146 files, nested at most 3 levels
below their root.

Of this, what is built is the whole tree's pinning as the root is created,
with its limits, its pins in the plan (`Plan.Calls`), each with its remote
context (`Source.Remote`), its texts in the store, beside the run, and the
variables the tree reads (`Run.TreeVariables`), and the check of the
called workflows' required inputs; what a child's Create and Start take
from its step (`compile.Options.Parent`): its inputs, its environment, and
its link to its step and to its root (`Run.Parent`), with what of them is
private kept in its secret record (`Record.Inherited`), and what Step says
of a calling step's `with:` and environment that is private
(`ReadyStep.WithPrivate` and the rest); and the coordinator's creation of
a step's try's child, linked to it (`createChild`), in its directory, in
its caller's job's group, but not yet on its member, with its root's
variables, and the root's finisher, which waits for every run of its tree;
and the worker API's paths for a child, served, and what a child's
attempt's claim has of its tree, which its worker reads; and the worker's
run of a calling step: it makes the child's directory, and cleans it as
`clean-on-retry:` says on the machine the run started on, never on a
host, never following a symbolic link, asks for the child, waits for it,
stops it as the step is stopped, and ends the step as below; the
coordinator's stop of a step's children, in the transaction
that ends its attempt, however it ends, and in the one that stops its run,
or its job, as the plan says of its `if:`; the caller's attempt parked
while it waits; and what people see of a tree, below. A calling step
whose own `ssh:` names a host is refused (`step_ssh_not_supported`), and
stays so ([Steps on another host](#steps-on-another-host)): format 1
connects to the host a calling step's `ssh:` names, its own or its
job's, fails the try if it can't, and gives the child the host's paths
in its environment, but runs the child where its caller runs, as it does
for the 244 known calls whose job's `ssh:` names a host, which Meridian
runs as any other; none of the known calls has an `ssh:` of its own.

- **The whole tree is pinned when the root is created.** The compiler
  resolves each workflow the run's own steps name through the Sources
  interface, pins it to a commit and the SHA-256 digest of its text
  (`Step.Source`), and loads and validates it; and then, for each pinned
  workflow, the workflows its own steps name, and so on, breadth-first,
  each reference once for the whole tree (`SourceRef.Normal`), whatever a
  step's `if:`, as format 1 fetches and checks every called workflow as
  its root is created. Each `with:`'s `$yaml` and `$host`, which say where
  the workflow is, must be written out (`source_not_known`); an empty
  `$yaml` means `workflow.yaml`. The root's plan holds every pin in the
  tree (`Plan.Calls`), by the digest of the text that names it and the
  step's place in that text, so that a child's Create reads its own steps'
  pins rather than resolve them, and the store keeps each pinned text, by
  its digest, so that a child is created from the text its root was
  created with, as format 1's copy is, without a fetch, by any replica,
  `resume` among them. A run and its reruns so run the same code
  throughout, even where a reference names a branch.
- **What fails the root's Create**, as it fails format 1's: a workflow that
  can't be resolved, loaded or validated; one more than 4 levels below
  the root (`workflow_nested_too_deep`); one already on its own path of
  calls (`workflow_calls_itself`); more than 100 distinct workflows, or
  1,000 calls, in the tree (`workflows_too_many`), or more than 1 MiB of
  their text (`workflows_too_large`); and a required input of a called
  workflow that its caller's `with:` leaves out (`child_input_missing`),
  whatever the step's `if:`: one with no `default:`, that isn't a
  boolean, and that isn't optional, nor hidden or ignored, nor in a group
  that is, to which `with:` gives no value, or `""` where no option is
  `""`. That is as far as the text says: what an expression in `with:`
  gives, and an input whose `optional:`, `hidden:` or `ignore:` is an
  expression, the child's Create checks as the step runs
  (`input_required`), where ACTIVATE evaluates, as the root is created,
  each that reads only what is known then, as the run's inputs, and takes
  one that reads what is known only as jobs run, as `needs` or `env`, for
  a value. Of the known workflows' 270 calls, none of the 262 whose
  workflow is one known file leaves out an input that is required
  whatever the run's values; 27 of them leave out one that such an
  expression decides, and so may one more, whose workflow is a
  marketplace workflow that 4 known files could be. Each is reported at
  the `uses:` of the root's own step that the path of calls starts at, in
  the root's file, since whoever runs the root may not own the workflows
  it calls; a cycle, once whatever paths reach it, a workflow too deep,
  and an input left out, once for each step that leaves it out, name the
  chain of calls from the root; what is wrong with a called workflow's
  own step, as a reference that names no workflow, is reported in that
  workflow's file, at the step. As in format 1, a workflow called on two
  paths is walked on each, its calls counted on each, and its text once,
  the root's own not counted. These limits are format 1's, and Meridian's
  own (`Options.Calls`), as its other limits are, so whoever runs a
  workflow can raise them; a child's Create, which reads its pins from
  its root's plan, checks them no more, so the plan doesn't record them.
- **The worker runs the step, and waits for the child.** As for any step,
  Step evaluates the step's `with:` on the worker, in the caller's scope,
  on each try, as format 1 does; the worker asks the coordinator for the
  child of the step's try, which creates and starts it, linked to the
  step's try, and waits for it to end, heartbeating. While it waits, its
  attempt is parked: it keeps its lease, but lends its member's capacity
  to the child's jobs, which need it, so that callers that wait don't
  starve their own children.
  - An attempt is parked for its child's jobs, and theirs, as long as the
    child runs: placing a job, the coordinator reads the job's run's
    callers from the store's links, the attempt whose step called the run
    and its own run's caller, up to the root, each by its ID, a few at
    most. So the worker says nothing more of it, and every replica sees
    the same, after a restart too.
  - Placement tells each member's driver the job's callers
    (`backend.Demand.Parked`). `local`, and `ssh` through the host's
    `local`, leave what they asked for out of what their member runs, so
    a member that runs only the job's callers starts it now. A caller's
    room so goes to its own descendants alone, which end before it goes
    on, never to another job, which would still hold it then.
  - Neither backend queues a job: a member past its capacity starts one
    too, ranked after one that has room. So on them a caller that waits
    no longer makes its member look busy to its own descendants' jobs.
    Which member a child's job runs on is still the group's order among
    those as soon to start it: keeping it on its caller's member, where
    the caller's files are, isn't built.
  - A caller stays counted parked while its child runs, even once a poll
    for the child's end, or a stop of it, has failed for good and failed
    the step (`child_request_failed`), so that the job's later steps may
    run meanwhile, as their `if:` lets them. Its room is lent to its own
    descendants alone even then, which share it with those steps.
  - A batch scheduler can't give back a waiting job's allocation, and its
    driver ignores the callers: there a caller holds its member while it
    waits, and a child whose jobs queue behind it waits for it until the
    caller's walltime or timeout ends it, an open question before a
    scheduler backend runs children
    ([Open questions](#open-questions)).

  The worker API's paths for a child, to ask for it, which a repeat
  answers with the same child, to wait for its end, and to stop it, raise
  the API's own version, not the plan's: a coordinator refuses a worker
  of an older one, at its claim, for a job that calls a workflow
  (`worker_api_too_old`).
- **Its inputs** are the step's `with:`, but its `$yaml`, `$host` and
  `$thumbnail`, as what was submitted to it: defaults are filled in and
  required inputs checked, as for any run, and, as in format 1, a value of
  another type than its input's is read as it is given, as Create reads
  any run's, and one for an input it doesn't define is kept. Step names
  the parts of `with:` that a template made from a variable or a password,
  and those it made from a secret, by their paths, all of a part a
  template gives (`ReadyStep.WithPrivate`, `ReadyStep.WithSecrets`); the
  child's plan holds none of them, nor what reads one, as an input's
  default can: its secret record holds them, in what was submitted, as it
  holds a password, and its values name them private, and those made from
  a secret secrets, masked as what is made from one is
  (`RunValues.Derived`), as in the caller's. The strings the caller's
  worker masks are kept in the child's secret record too, for its jobs to
  mask, as format 1 has a child mask its caller's secrets. A child whose
  Create fails, as for an input that a `with:` made of expressions leaves
  out, fails its step (`child_not_created`), with the child's problems,
  masked as the step's are.
- **Its environment** is its calling step's, as in format 1: the caller's
  workflow, job and step `env:` and the variables format 1 defines, under
  the child's own `env:`, whose expressions read it, and its own other
  values, in the order they read each other, and its jobs' and steps' over
  that, the variables the child's worker defines over all; so a variable
  the child's worker doesn't define, as `PW_MATRIX_INDEX` for a job that
  isn't a matrix's, is its caller's, and so are the step's own, as
  `OUTPUTS`, which the child's worker sets over for each step of its own,
  and the retry counters, which a step of the child's without a `retry:`
  reads as its caller's, as in format 1. Step names the variables of the
  step's environment made from a variable or a password, and from a secret
  (`ReadyStep.EnvPrivate`, `ReadyStep.EnvSecrets`), which the child's plan
  doesn't hold: its secret record does, and its values name them private,
  or secrets, as for its inputs. A value of the child's own `env:` that
  reads its own name, which reads the caller's in format 1, isn't built
  yet: the child fails as it starts, as one whose values read each other
  does (`values_read_each_other`); no known workflow has one. The caller's
  `matrix` values and `app` aren't passed to it.
- **Its identity** is its root's, as format 1 gives a child:
  `PW_JOB_ID`, `PW_JOB_NUMBER`, `PW_RUN_SLUG` and `PW_PARENT_NAME` are the
  root run's, and `PW_PARENT_JOB_DIR` is the root's run directory, which
  the known workflows read 215 times; `PW_JOB_DIR` is the child's
  directory, and `PW_MATRIX_INDEX` its caller's. `PW_WORKFLOW_NAME` is
  format 1's name for the call, from its `uses:`: `github.`, the owner, a
  dot and the repository, for one in a GitHub repository, which two of its
  `$yaml` files share; `gitlab.`, the server and the project's path, with
  dots for its slashes, for one in a GitLab project; the slug, for
  `marketplace/`; and the saved workflow's name, for `workflow/`
  (`SourceRef.CallName`). The child's plan links it to its step's try,
  and to its root, by run ID, with how far below the root it is
  (`Run.Parent`), which its attempts' identity is given from.
- **Its directory** is format 1's: the step's own `working-directory:`,
  relative to the caller's run directory (`PW_JOB_DIR`), or, absolute, as
  it is; or, for a step without one, or whose `working-directory:` is
  null, the caller's run directory's `subworkflows/<job>/step_<n>/`, whose
  workflows read files their children leave there (`plan.ChildDir`).
  `<job>` is the calling job's name, or, for an instance of a matrix job,
  its name, a hyphen and the instance's index, from 0, in the order the
  combinations are made (`build-0`), and `<n>` the step's place in its
  job's `steps:` as written, from 0, counting steps that are skipped. The
  job's `working-directory:` doesn't move it, as it doesn't in format 1:
  22 of the 270 known calls are in a job with one, and have none of their
  own. A step's `working-directory:` that puts the child outside its
  root's run directory, an absolute one elsewhere or a relative one that
  leaves it with `..`, isn't built yet, where format 1 runs the child
  there: anywhere on the machine the run started on, and under the home
  on a host. A child's directory isn't made private, as a root's is,
  since what holds it is, which one elsewhere wouldn't be. The step fails
  when it runs, as its child would be created (`child_dir_outside`), never
  as the root is created, since an `if:` may skip it; none of the 270
  known calls sets `working-directory:`.
- **Its directory is in the same place on each machine**, as format 1
  lays a tree out. Format 1 prepares a subworkflow on the machine the run
  started on, even for a step on a host, resolving the step's
  `working-directory:` there, and maps each directory of the tree to the
  same place under its jobs directory on each host its jobs run on. So a
  child's run directory is in its root's on the coordinator's machine,
  and a working directory that a worker on a host gives inside its root's
  run directory there, as one `PW_JOB_DIR` or `PW_PARENT_JOB_DIR` gives,
  is the same place in the root's run directory on the coordinator's
  machine. A child's job that runs on a host runs in the same place in
  its root's run directory there, which `meridian host` makes if none of
  the root's jobs ran on the host (`backend.PlaceRequest.Path`), and
  reads the host's as `PW_JOB_DIR` and `PW_PARENT_JOB_DIR`. Every try of
  a step's child shares its directory, and a later step on that host
  finds what the child wrote there: 5 known calls read their child's
  files so, and 3 more through a nested call, and 3 of them resume, on a
  step's next try, from what its last try left there. Every one of the
  263 known calls whose workflow is known has a child with jobs on a
  host. A child's job on another host than its caller's has its own copy
  of the directory there, without its caller's files, as in format 1.
- **The calling step's worker makes it**, where the step runs, and cleans
  it before the step's next try as its `clean-on-retry:` says, but only
  on the machine the run started on: a worker on a host never cleans it,
  as format 1 never cleans a host's copy of a directory, which is where
  those 3 calls' tries leave what the next resumes from
  (`workerapi.Claim.RunDirOnMember`, [Failures and
  retries](#failures-and-retries)). Format 1 cleans its copy on the
  machine the run started on even for a caller on a host, where its
  child's jobs that run there keep their files; a worker on a host can't
  reach that copy, so Meridian leaves it, and none of the 9 known calls on
  a host with a `retry:` has a child with a job anywhere but on a host.
  The worker makes it, and cleans it, a directory at a time from its
  root's run directory, and never follows a symbolic link on the way,
  which a step of the run could leave there to put the child elsewhere:
  the step fails instead (`child_dir_unusable`), and so does the child's
  job, as its worker makes it again (`run_dir_unusable`).
- **Its variables** are its root's: the root's Start reads the variables
  every workflow in the tree reads (`Run.TreeVariables`, which the root's
  Create finds in each pinned workflow's expressions), once, into the
  root's secret record, and a child's Create and Start read them from
  there, never from Variables, as format 1 reads them once for the whole
  tree. So a child sees what its root saw, and any replica creates and
  starts it, `resume` among them, its Start in the queue every coordinator
  works. The root's record is kept until every run in its tree has ended:
  the root's finisher waits for them, and the end of each wakes it.
- **Its sessions.** A child's `sessions:` makes its own sessions, as its
  run's. Its `parallelworks/update-session` asks the provider as any
  run's does, which is told the child's tree's root and the root's
  `permissions:`, never the child's own, so that it authorizes the step
  as format 1 does a tree's, whose sessions it makes with the root's run,
  and whose token is the root's ([Built-in actions](#built-in-actions)).
- **Its groups.** A job of a child, and a job that calls one, may name
  groups with `runs-on:`, which format 1 refuses in both as not supported
  yet, so no workflow format 1 runs has either: a difference that takes
  more. A child's job that names none runs in its caller's job's group,
  on its member where it can, where its caller's files are. Of this, the
  group is built: the child runs in the group its caller's attempt was
  placed in, or its caller's run's default group where that placement is
  private, which no caller format 1 runs has; its caller's member isn't
  built yet, and its jobs are placed on any member of the group. A caller
  on the host its format 1 `ssh:` names is in no group a name finds, so
  its child runs in its run's default group: format 1 runs a
  subworkflow's jobs where their own `ssh:` says, or else on the machine
  the run started on, never on its caller's host because the caller ran
  there.
- **Its end is its step's**, as format 1 maps it, from the child's jobs
  and its stop, rather than from its outcome:
  - a child that was cancelled, or whose caller's step stopped it, ends
    its step cancelled, whatever the step's `ignore-errors:`;
  - one with a job that failed, or was skipped because a job it needs
    failed, fails its step, which succeeds where its `ignore-errors:` is
    true and no try is left; a job of it cancelled by itself, as by
    `parallelworks/cancel-jobs`, doesn't count;
  - any other succeeds.

  A step whose child was cancelled from within it, or by a person's
  `meridian cancel` of the child, while its job goes on, counts as a
  success for its job: the job's later steps run, `success()` is true for
  them and `cancelled()` and `failure()` false, the job and the jobs that
  need it see a success, and its step's own end says cancelled, as in
  format 1 ([Cancellation, timeouts and cleanup](#cancellation-timeouts-and-cleanup)).
  A child's own `timeout:` fails it, and its step.
- **No outputs come back**, as in format 1: a calling step writes none,
  and an expression that reads one fails as a read of an output that
  isn't there does. Workflows pass what a child makes back through
  files: no known workflow reads a child's output, nor gives a calling
  step an `id`.
- **Stopping it.** A calling step that is stopped stops its child, and
  waits for the child to end, its cleanups among it, as format 1's call
  returns only once its child has; the child's jobs end as any run's do
  when it is stopped. The step's worker stops it, and the coordinator does
  too, as it tells the step's job to stop, in the same transaction: the
  children of the steps a stop of the job stops, those whose `if:` doesn't
  outlive a cancel, for the stop's reason; and those of a step that does
  run on, as the step does.
  - A run's cancel stops a step that doesn't outlive it, and its child
    with `cancelled`; the step ends cancelled.
  - The cancel of the step's job, as by `parallelworks/cancel-jobs`, stops
    it with `cancelled`; the step and its job end cancelled.
  - `early-cancel: any-job-failed`, which 215 of the 270 known calls set,
    stops it with `cancelled`; the step ends cancelled and, as in format 1,
    its job goes on.
  - The step's `timeout:`, and its `retry:`'s timeout for a try, stop it
    with `timeout`. As in format 1, a step without a `retry:` past its
    `timeout:` ends cancelled, not failed, and its job goes on. A try past
    its `retry:`'s timeout ends as a command's that ran out of time does:
    the step is tried again, and its last try fails, or succeeds if its
    `ignore-errors:` says so. A step with a `retry:`, whose tries have a
    timeout of their own, past its own `timeout:` fails too, and isn't
    tried again, as in format 1, which takes that deadline for its tries'.
  - The job's timeout, or the run's, stops it with `timeout`, and, as in
    format 1, the step and its job end cancelled, not failed, with the
    reason `timeout`, without marking the run for
    `early-cancel: any-job-failed`: the one place where a job's own
    timeout doesn't fail it. A job that needs it is skipped as after one
    that failed.
  - An attempt that ends, without its worker, as lost, withdrawn or
    superseded, or with it, stops the children of its steps that haven't
    ended with `cancelled`, in the transaction that ends it, whatever ends
    it: nothing waits for them any more. Its job's next attempt runs its
    steps again, with new children. A worker that is only cut off keeps
    its children: asked again, the coordinator gives it the same.
  - A root run doesn't end while a run of its tree hasn't, which it waits
    for, so its tree ends before it: a stop of the root, a cancel or its
    `timeout:`, stops the children of its steps it stops, and so on down
    its tree, and its end waits for theirs.
- **Each try is a child of its own.** A step's `retry:` runs the whole
  child again, evaluating its `with:` again, as in format 1: a child for
  each try, linked to it. A rerun, which is Meridian's own, makes new
  children from the plan's pins.
- **What people see.** A child is a run, with its own number, shown under
  the step that called it:
  - `meridian run`, `resume` and `cancel -f`, and `meridian logs`, follow
    the whole tree: a child's events and logs, from its caller's
    `child.created` on, each after the step's try that called it and a
    `›`, as `14:02:08 build step 1 › run 7 created` and `build step 1 ›
    main step 0| testing`, and the child's end, before the root's; and as
    JSON lines with the child's `run` and `callers`, the steps' tries that
    called it from the root down, each with its `run`, `job`, `step` and
    `try`. The parent's events say when its step's try's child was created
    and how it ended (`child.created`, `child.ended`, with the `child`'s
    ID), the latter while the parent hasn't ended;
  - `meridian status` lists root runs, each with how many runs its tree
    has below it, and `--all` every run, a child with what called it;
    `meridian status <run>` shows the child of each step's try that called
    one, and a child shows its parent, job, step and try, and its root;
  - `meridian logs <run> [job]` shows a child's logs under its step, a
    job's name its children's too, and the child's own, as a root's, when
    it names the child;
  - `meridian cancel <run>` of a root stops its tree with it; of a child,
    that child only: its step ends cancelled, and its job goes on.
- **Without a Sources resolver**, a `uses:` that names a workflow fails to
  compile (`resolver_missing`, [The workflow format](#the-workflow-format)).
  A `uses:` names a workflow when it starts with `github/`, `gitlab/`,
  `marketplace/` or `workflow/`; any other names a built-in action.
- **What a later format can change**, which format 1 fixes, and no
  workflow format 1 runs can rely on otherwise: a calling step that gets
  its child's outputs; a step `timeout:`, and a job's or a run's, that
  fails a calling step, and its job, rather than cancelling them; a cancel
  from within a child that stops its caller's job too; a type check of a
  `with:` against its child's inputs, and a refusal of a key the child
  doesn't define; and a name for a matrix instance's directory that can't
  be a job's, as `build-0` can.

### Steps on another host

A format 1 step's `ssh:` is its own connection: it is evaluated for each
try, so a `remoteHost` that indexes a list by the retry counter fails
over between hosts, and a step without one has its job's. Format 1
connects from the platform, evaluates the step's `if:`, `ssh:`, `retry:`
and `timeout:` before it connects, and the rest, its `env:`, `run:`,
`with:` and `working-directory:`, after it, with the variables it
defines, `PW_JOB_DIR`, `PW_PARENT_JOB_DIR`, `OUTPUTS` and
`PW_STEP_SUMMARY`, naming the host's paths: the host's copy of the job's
directory, and a job's `working-directory:` under the runs' directory
made the host's too. It copies the file the step writes its outputs to,
the job's shared one for a step without an `id`, to the host before the
step, and back over its own after it, only after a try that succeeded. A
step's `cleanup:` is a copy of the step, its `ssh:` with it, which is
evaluated again as the cleanup runs, and which outlives its job's cancel.

Of the known workflows, 37 steps, in 14 files, have an `ssh:` of their
own: 28 in jobs without one, which run where the run started, 10 of them
checkouts that a later job on the host reads relative to its directory,
and 6 of them steps that append outputs to their job's file, which later
jobs read; 1 that names its job's host again, which runs where its job
runs; and 8 that reach a compute node through their job's host, as a
jump host, after actions only a platform provides ([Built-in
actions](#built-in-actions)). Every one of the 28 reads its host from
the run's inputs. None calls a workflow, or has a `retry:`, an
`ignore-errors:` or a `working-directory:` of its own.

Meridian runs such a step as a run of its own, which a worker on the
step's host runs, as a job's is, rather than over a connection of the
job's worker's, which would leave a process on the host that no
placement records, that nothing could check or stop once the worker had
gone, and that a checkout couldn't be written safely by. Version 3 of the
worker API brings it: in an attempt of an earlier version, as a worker
or a coordinator of an older release serves, such a step fails
(`step_ssh_not_supported`), as one whose `ssh:` is null, or gives no
`remoteHost`, in a job on a host does (`step_off_job_host`), and so
does, whatever the version, a step that calls a workflow.

- **A step run.** A step's try whose `ssh:` names a host other than its
  job's, compared as text by its `remoteHost`, `remoteUser`,
  `jumpNodeHost` and `jumpNodeUser` ([Running steps](#running-steps)), is
  a child of the try, as a workflow a step calls is, of a kind of its
  own: it has no plan of its own, but names its caller's plan, by its
  digest, and the job and the step in it, from which whatever reads a
  run's steps, as the worker API's actions and the checkout's pin, reads
  the step; it has one job, the caller's, which Ready readies as the
  caller's was, with the caller's inputs, needs and matrix values, and
  whose target is where the step's `ssh:` puts the try. A step whose
  `ssh:` is null, or gives no `remoteHost`, in a job on a host, is a step
  run where its job would run without its `ssh:`, the run's default
  group. So the coordinator makes a step run only for a step of the plan
  with an `ssh:` of its own, null among them, in a job without `ssh:` or
  whose `ssh:` names a host, and never for one that calls a workflow, nor
  for one of a step run's, nor for one that uses an action the
  coordinator performs but checkout: the coordinator does its work
  wherever its `ssh:` names, so it runs in its job, as it does today. A
  cleanup whose `ssh:` puts it elsewhere is a step run of its own, as its
  job's cleanups run.
- **The coordinator evaluates where it runs**, from the plan, with what
  Ready readies the job with, the run's inputs and variables, the outputs
  of the jobs it needs and its matrix values, and the try's retry
  counter, as format 1 resolves a step's host before it connects; the
  caller's worker's evaluation must agree, or the step run isn't made. So
  a host never chooses where the coordinator connects with the user's
  keys, and a host that a variable or a password names is placed
  privately. Its tries come in order, each once the one before has
  failed, so the retry counter is the try's. A step whose `ssh:` reads
  what only the job's worker knows, as its earlier steps' outputs, its
  own job's outputs through `needs`, a job of `needs` that only
  evaluating it names, the job's status, or `env` but the retry counter,
  which it has only with a `retry:`, fails
  (`step_ssh_reads_worker`); of the known workflows, only the 8 steps
  that reach a compute node do, which read their own job's steps'
  outputs, after actions only a platform provides.
- **It runs where its caller's run's jobs on that host run**, in the
  host's copy of the caller's run directory ([Workflows a step
  calls](#workflows-a-step-calls)), so a checkout there is where they find
  it. The coordinator places it through the `ssh` backend, as it places
  a job whose `ssh:` names that host, so no key leaves the machine the
  run started on, and no host needs one for another; a jump host is a
  hop of the coordinator's connection, as a job's is.
- **Its worker evaluates the step** from the plan, as format 1 does once
  it has connected: with its own `PW_*`, over the state of the caller's
  job at the try, which the caller's worker gives, the earlier steps'
  outcomes and outputs, the job's status and the retry counters, and
  which the coordinator keeps sealed in the step run's secret record, as
  a calling step's private values are. It evaluates the job's `env:`
  again, with its own paths, as format 1 gives the step the job's
  environment with its paths made the host's. It doesn't evaluate the
  step's `if:` again, nor its cleanup's, nor heed its `ssh:`, which the
  caller's worker did, as format 1 does before it connects. It runs that
  one step, never the job's others, and none once
  it is told to stop, whatever its `if:` says, nor whether its caller's
  job is being cancelled, which is only the step's `cancelled()`. It
  reports the step's exit status and its outcome as it ran, which its
  caller's `retry:` and `ignore-errors:` then apply to. It masks what the
  caller's worker masked at the try, as a workflow a step calls masks its
  caller's secrets, and names an output that holds what it masks a
  secret; the caller's worker masks each output named private or secret
  as a secret of its job's, at least. Its `OUTPUTS` starts empty, and
  what the step
  writes there comes back to the caller's worker, as the values it
  reads, which appends them to the file the step would have written, the
  job's for a step without an `id`, as escaped `name=value` lines, and
  reads it as it does a step's of its own, under the job's cap: as it
  reads that file, a line it can't parse is ignored, and a name written
  twice has one value, so the values are what the step's lines would
  give. A value's escaping can make its line longer than format 1 reads
  one, which would keep the whole file from being read: then none of them
  is appended, and the step fails (`step_run_output_line_too_long`). A
  try that fails gives none, as in format 1. Of the caller's job,
  its worker gives only what format 1 passes between steps, the earlier
  steps' outcomes and outputs, whether the job is being cancelled, the
  step's retry counter, and whether its `if:` gave the word `always`,
  which reads an output nothing wrote as empty, never a variable of the
  environment or a path; and the coordinator keeps of the outputs only
  those the step reads, as the plan says, since one can hold what was
  made from a secret, and gives the job's retry counter and attempt as the
  caller's claim has them. The step run's claim has the step it runs
  whole, at its own index, the steps before it by their `id` alone, which
  its expressions read as `steps.<id>`, and none after it, or, for a
  cleanup's, every step so but that one, of which it has what its
  `cleanup:` reads; and of the run's values and its needs' outputs only
  what that step and the job's `env:` read, never what another step
  reads.
- **The caller waits for it**, parked, as for a workflow a step calls;
  stops it when the step's try is stopped; and ends the try as it ended,
  its `retry:` and `ignore-errors:` applying as to any try. Two jobs,
  each with a step on the other's host, don't wait on each other, since
  neither backend queues a job. The coordinator stops it in the
  transaction that ends the caller's attempt, however that ends, and in
  the one that stops its run, or its job, as for a workflow a step
  calls, but for one of a step that outlives that stop, as its `if:`
  says, and a cleanup's, which no stop of its job or its run stops; the
  end of the caller's attempt stops them all. A stop that reaches a step
  run, as these, its step's own, its step's timeout or a person's
  cancel, reaches its worker in the same transaction, whatever its
  step's `if:` says, and a step run stopped before it was placed is
  never placed. A try that its timeout stops, whose step run's host can't
  be reached, ends only once the step run's lease lapses, as a workflow a
  step calls does. Its paths are version 3's of the worker API; an
  attempt of an older version fails such a step, as now.

Where format 1 does otherwise, none of it in the known workflows:

- **A placement for each try.** Meridian places a worker on the host for
  each try, a second or two once the host has the binary, where format 1
  reuses its connection. A host that can't be reached fails the try
  after the backend's tries (`ssh_unreachable`), as format 1's connection
  fails it after its own, and the step's `retry:` applies in both.
- **`OUTPUTS` starts empty on the host**, so the step doesn't read what
  its job's earlier steps wrote there, and a step that writes it with
  `>` doesn't erase that, as format 1's copy back does. All 9 of the
  known steps' writes to it append.
- **A calling step's own `ssh:`** is refused ([Workflows a step
  calls](#workflows-a-step-calls)).
- **Its log** is the step run's, which its calling step's shows, as a
  child's, where format 1 writes it in the step's own.
- **An action the coordinator performs, with an `ssh:` of its own**, as
  cancel-jobs or update-session, runs in its job, which connects to no
  host, where format 1 connects to the host first, and fails the try if
  it can't.
- **A step whose `ssh:` reads what only its job's worker knows** fails
  (`step_ssh_reads_worker`), where format 1 runs it there. The 8 known
  steps that do never get that far: they come after actions only a
  platform provides.
- **A step whose `ssh:` is null, or gives no `remoteHost`, in a job on a
  host** runs in the run's default group, where format 1 runs it where
  the job would run without its `ssh:`, its `runs-on:` among them, which
  a job on a host doesn't keep once its `ssh:` names one.

### Built-in actions

A step whose `uses:` is `parallelworks/` and one of five names runs one of
format 1's built-in actions, with its `with:`; the schema refuses any other
name under `parallelworks/`. The worker evaluates the step as it does a
`run:` step, its `with:`, `if:`, `retry:`, `timeout:` and `cleanup:` among
it, and does the action, or asks the coordinator to, in place of a
command. Each action goes through the interface that owns what it acts
on, rather than an interface of actions: a repository is Sources', the
tree of a commit pinned from it Trees', a session Sessions', and the run's
jobs the coordinator's own.

| Action | Acts on | Done by | Without a platform |
| --- | --- | --- | --- |
| `parallelworks/checkout` | A Git repository | The coordinator, which pins it through Sources as the run is created, and sends its tree, read through Trees, to the worker, which writes it | Works, with Git on the coordinator's host, as Sources needs |
| `parallelworks/cancel-jobs` | Jobs of the step's run | The coordinator, asked by the worker | Works |
| `parallelworks/update-session` | A session the run may update, as the provider decides | The coordinator, through Sessions | The step fails (`resolver_missing`), as there is no local Sessions |
| `parallelworks/scheduler-agent`, `parallelworks/wait-for-agent` | A platform's agent on a cluster's compute node, which the first starts through the cluster's scheduler with the platform's command-line tool and credentials, and the second waits for | A platform | The step fails (`action_needs_platform`) |

None of them is a no-op where nothing provides it: a step that succeeded
would say that a session points at the run, or that an agent runs, where
none does. A step that uses an action nothing provides fails when it
runs, and only then: one only a platform provides with
`action_needs_platform`, which names it, and update-session, where
nothing provides Sessions, with `resolver_missing`. It fails as an action
that fails does in format 1, as a command that fails does: its `retry:`
tries it again, its `ignore-errors:` lets its last try succeed, and its
`cleanup:` runs. An `if:` can skip the step, or its job, as the known
workflows that hold the agent actions do, each of their 8 steps in a job
whose `if:` reads an input that defaults to false, which every known call
of them leaves false. That differs from a feature whose interface is
missing, which fails to compile ([Interfaces](#interfaces)): every run
makes its `sessions:`, but a step may never run. The worker fails a step
that uses scheduler-agent or wait-for-agent with `action_needs_platform`
already, and the coordinator does cancel-jobs, update-session and
checkout.

- **checkout.**
  - **Pinned as the run is created.** Create pins each checkout's `repo`
    and `branch` to a commit through Sources, as it pins the workflows
    `uses:` names, so that a run and its reruns check out the same tree,
    and a branch that is gone fails the run before anything runs, even
    under an `if:` that is false, as a `uses:` that can't be resolved
    does. Both must be written out (`checkout_not_known`), so that the
    coordinator, not the worker, chooses what is fetched, with whose
    credentials. ACTIVATE reads the branch when the step runs. All 148
    known checkouts write both out. A `repo` or `branch` written out as
    `""` pins nothing, and the step checks out nothing and succeeds, as
    format 1's does. The branch is a branch, a tag or a full commit;
    another revision that format 1's whole checkout takes, as `git
    checkout` does, such as a short commit or `HEAD~1`, isn't built yet,
    and fails the run as it is created (`source_revision_not_found`). No
    known checkout names one. The local Sources refuses a revision git
    could take for an option, or refuses as a branch's or a tag's name,
    as it does a `uses:`'s, without asking git
    (`checkout_revision_invalid`). A full commit the copy doesn't hold is
    fetched by itself, which the server must allow, as Git's protocol
    version 2 does. The pin is the step's in the plan
    (`Step.Checkout`), and the pins of the checkouts of the workflows a
    run calls, down the tree, are its root's (`Plan.Checkouts`), by the
    digest of their workflow's text and the step's place in it, as calls'
    are, so that a child run's Create reads them rather than pin them
    again; each repository and revision is asked once for the tree. A
    plan that holds a pin is of version 2. A written-out `sparse_checkout`
    entry is checked as the run is created (below); one an expression
    gives, as the step runs. The local Sources keeps a copy of each
    repository by the SHA-256 digest of its URL as the step writes it, and
    pins a full commit it holds without the network.
  - **Fetched by the coordinator.** Git runs on the coordinator's host,
    through Sources: with the user's own Git configuration, credential
    helpers and SSH agent where `meridian` runs, and a platform's linked
    accounts on a platform; and never prompting. A `repo` is any
    `https://` or `ssh://` URL, or an scp-like `user@host:path`, with an
    IPv6 address in brackets too, on any host, as format 1 takes; a
    platform's Sources may take fewer. Whatever URL Git ends up at,
    through a redirect or the user's `insteadOf`, it fetches only over
    https and ssh (`protocol.allow`), for `uses:` workflows too, unless
    the user's own configuration allows another transport by name. Three
    are refused as the run is created (`checkout_repo_invalid`),
    differences from format 1: another transport, as `file://` or `ext::`,
    so that a workflow can't have the coordinator read its host's files, or
    run a command there, or `git://` or `http://`, which have no TLS or
    SSH, so that nothing fetched would be authenticated or protected; a
    URL that holds a credential, as `user:token@`, which would put a
    secret in the plan; and, over ssh, a host or a user that holds what
    ssh would read as more than a name, by the rules OpenSSH 10.1 and
    later checks its own command line with, as Meridian checks an `ssh:`'s
    ([Connections](#connections)). They are checked as Git gives them
    to ssh, with a URL's percent-escapes decoded, and an address's
    brackets and a port taken out, and as ssh splits the user from the
    host, at the last `@`. ssh puts them in the commands of the user's ssh
    config, as a `ProxyCommand`, that a shell runs, and an older ssh takes
    them. None of the known workflows' checkouts names one. Git is the
    `git` command, not a client in Go
    ([FIPS 140-3](#fips-140-3)). ACTIVATE runs `git` on the step's host,
    with that host's credentials, and through the platform, with the run's
    owner's linked account, where that fails. The worker's host needs no
    Git, no credentials and no way to the repository's server, which a
    cluster's login node with an old Git, or without outbound access,
    hasn't.
  - **Written by the worker.** The worker asks the coordinator for the
    step's action, as for cancel-jobs, and the coordinator answers from
    the step's pin, never from what the worker says: it fails the step for
    a tree it can't serve, and warns of a path that names nothing. Once it
    succeeds, the worker asks for the tree with the same request
    ([The worker API](#the-worker-api)), which the coordinator streams as a
    tar of the commit's files, links and directories, whose files come to
    1 GiB at most (`workerapi.MaxCheckoutBytes`, `checkout_too_large`),
    in a million entries at most (`workerapi.MaxCheckoutEntries`,
    `checkout_too_many`), so that no tree uses up a host's inodes. The
    tree request is the action's own, unchanged; another is refused as
    the worker's bug, which fails the step rather than halt the attempt.
    The worker writes the tree through an `os.Root` on the step's working
    directory joined with `path:`, or on `path:` as it is where it is
    absolute, as format 1 uses it, so that no member of it, nor a link
    already there, writes outside it. It merges, as ACTIVATE's copy does:
    a file or a link replaces a file or a link of the same name, written
    under a name of its own and renamed over it, so that it never writes
    through a link, or into a file another name links to, and the rest is
    kept; a directory where the tree has a file fails the step
    (`checkout_write_failed`). A link where the tree has a directory, as
    one in a member's leading directories, is replaced by a directory, as
    git checks out over one: no write follows a link, one an earlier step
    left, into `.git` or elsewhere, or one the tree holds, as a system
    whose names ignore case reads a link `A` and a directory `a/` of one
    tree as one. It writes only directories, files and symbolic links, by
    names that stay inside the directory, none of them `.git`, nor a link
    to one, in any case (`checkout_unsafe`).
    The coordinator reads the commit from its copy of the repository, and
    fetches it by itself if the copy hasn't it, trying 5 times, 2, 4, 8
    and 16 seconds apart, as ACTIVATE tries a clone
    (`checkout_unavailable`); the step's `retry:` applies on top, and the
    worker asks again for a stream cut short. A step whose `repo` or
    `branch` is `""` checks out nothing, and succeeds, as in format 1.
    The files are the blobs as committed, with no `.gitattributes`
    applied: no line ending converted, and no filter, such as Git LFS's
    smudge, run, as a clone on the step's host could. The tree is built
    from the commit's own list of files, not by `git archive`, which
    applies `export-ignore` and `export-subst`, as a clone doesn't.
  - **Paths.** A `sparse_checkout` entry is a literal path from the
    repository's root; a glob or a `!`, which ACTIVATE can read as a
    pattern, is refused (`checkout_path_invalid`). Every known entry, in
    the 134 known sparse checkouts, means the same either way. An entry
    that matches nothing warns (`checkout_path_not_found`). A whole
    checkout of a repository with submodules, which format 1 clones with
    them, isn't built yet: its step fails when it runs
    (`checkout_submodules`), and it can come later. A sparse one leaves
    them empty, as format 1's does, and a submodule a sparse entry names
    is an empty directory. None of the known repositories checked
    out has submodules.
- **cancel-jobs.** The coordinator does it, in one transaction for each
  request, and answers a repeat of it as it answered it:
  - Each name in `jobs:` names a job of the step's own run. One that names
    no job, or a matrix job, whose instances format 1 names otherwise,
    warns (`cancel_jobs_job_unknown`) and does nothing, as in format 1.
    A cancel of another run's jobs, which format 1 makes with `slug`,
    `workflow` and `run`, or `run` alone, isn't built yet: its step fails
    when it runs (`cancel_jobs_other_run`), never as the run is created,
    since an `if:` may skip it; no known workflow makes one. What format
    1 reads as the step's own run is its own run: a negative `run`, and a
    `workflow` and `run`, or a `run` alone, that name it; a Meridian run is
    named by its workflow's `name:` and its ID, and has no slug. A
    `workflow` without a `run`, or with a negative one, fails the step, as
    in format 1 (`cancel_jobs_run_missing`). With no `jobs:`, or an empty
    one, the step cancels its own run, as a person does, and succeeds. A
    `jobs:` that isn't a list of names, a `slug` or `workflow` that isn't
    a string, or a `run` that isn't a whole number, fails the step
    (`wrong_type`), as format 1 refuses it too; null, and `""` for a
    `slug` or `workflow`, are as if not given.
  - **From a child run**, as format 1's from a subworkflow, the step's own
    run is named as its root is, by the root's workflow and number, as
    format 1 names a subworkflow's run, which is its root's: a `workflow`
    and `run` that name the root cancel the root's jobs, or with no
    `jobs:` the root, which stops its tree as a person's cancel does; a
    `run` alone that names the root, or nothing, cancels the child's own,
    as format 1's does. A child's own number, which format 1 doesn't know,
    names another run, which fails the step.
  - A job that has ended, or is cancelled already, is left as it is. One
    that runs stops as a cancel stops it
    ([Cancellation, timeouts and cleanup](#cancellation-timeouts-and-cleanup)),
    one placed and not yet claimed is claimed with its cancel, and one that
    hasn't started ends cancelled and never starts, a
    [bug fix](#bug-fixes-to-format-1). One whose worker is lost once it
    is cancelled isn't placed again, and ends cancelled; an attempt of it
    made after the cancel is told it from the start.
  - **Of two jobs that cancel each other at once, exactly one is
    cancelled:** a step whose own job is cancelled already, and that
    doesn't outlive a cancel, cancels nothing, and ends cancelled. Each
    cancel is a transaction of the store's, which run one at a time, so
    the second sees the first's. In format 1 it is a race, whose outcome
    no workflow can rely on; exactly one cancelled is one of the outcomes
    that race can have, made certain. 196 of the 215 known cancels are of
    jobs that cancel each other.
  - A step that cancels its own job succeeds, and its job then stops as a
    cancelled job does. ACTIVATE can fail such a step instead in a job
    that runs over `ssh:`, as it copies the step's outputs back once the
    job is cancelled; no workflow can rely on that.
  - A job cancelled this way doesn't fail the run, as in format 1, but the
    jobs that need it are skipped, which does, as a job they need that
    failed does.
- **update-session** updates the session its `name` names, as text,
  through Sessions (`Sessions.Update`), with the rest of its `with:` as
  it was evaluated, and whether that read a variable or a password, which
  the provider keeps private, or a secret, which it masks. A name is any
  session's: `${{ sessions.<key> }}` gives one of the run's own, as format
  1's does, but Meridian doesn't hold a step to those. The provider
  decides whether the run may update the session, as format 1's platform
  decides whether the run's token may. It is told the run, its tree's
  root, and the root's `permissions:` to decide by: format 1 gates the top
  workflow's alone, and a child runs with its root's token, so a workflow
  a step calls that asks for more than its root did is given none of it.
  - A run whose tree's `permissions:` hold `'*'` acts as the person who
    runs it, and may update any session of their own, as they could
    themself through the platform's API. One only shared with them it may
    not: sharing gives a session's use, never its administration.
  - Any other may update only a session of its own tree, which a run of
    the tree made, as format 1 makes a tree's sessions with its root's
    run, and only one of the person who runs it.

  The coordinator asks the provider before it stores the step's answer,
  and asks it nothing for a step that its job's cancel, or its
  `early-cancel:`, stops. A refusal of the provider's, as of a session the
  run may not update, fails the step, as a command that fails does, so
  its `retry:` tries it again; a provider that can't answer is asked
  again, as the worker repeats its request. Where nothing provides
  Sessions, as with `meridian run`, the step fails when it runs
  (`resolver_missing`, with the interface Sessions), as a feature whose
  interface is missing fails, but when the step runs, not as the run is
  created; a run with `sessions:` can't start there anyway, and all 15
  known update-session steps are in workflows that have them.
- **scheduler-agent and wait-for-agent** need a platform's agent, its
  command-line tool and its credentials, which Meridian has none of, so the
  step fails (`action_needs_platform`). A platform that embeds Meridian can
  provide them, an addition. Meridian's own way to run on a compute node is
  a scheduler backend, which places a worker there.
- **The worker API.** The worker asks the coordinator to do a step's
  cancel-jobs or update-session, and for its checkout's tree, through
  paths that are additions to the worker API, which raise the API's own
  version, not the plan's; a checkout's pin is the plan's.

### Failures and retries

Meridian never runs a step again unless the workflow asks it to. Steps are
not assumed idempotent.

- **Before the first step starts**, a failure retries automatically, with
  backoff and a bound: placement failed, the scheduler rejected the job,
  the image couldn't be pulled, or the worker never connected. None of the
  workflow's code has run, so the retry can go to another member of the
  job's group ([Placement](#placement)). A job is placed again after 5
  seconds, then 30, then 2 minutes, and its fourth such failure fails it,
  or its second with a problem of the same kind, with the last one's
  problem.
  A backend that can't answer, as when a scheduler's command fails, is
  asked again after a second, twice as long each time up to a minute, ten
  times, after which that counts as such a failure.
- **A run's Start that can't finish**, as when a resolver can't answer, is
  tried again after 2 seconds, twice as long each time up to 2 minutes,
  eight times, about four minutes in all, after which the run fails
  ([Starting a run](#starting-a-run)).
- **The worker records that the first step is starting**, and waits for the
  coordinator to store that, before it runs it. Whether user code may have
  run is therefore always known.
- **Once a step has started**, a lost worker fails the job, with the reason
  `worker-lost`, unless the job's `retry:` lists that reason
  ([Job retries](#job-retries)).
- **A step's `retry:`** reruns that step on the same worker when it fails,
  as in format 1. It can't help when the worker itself is lost. Its
  `clean-on-retry:` is true unless the workflow says otherwise, as in
  format 1, or an expression, which Step reads as format 1 does: true or
  false, text or a number that reads as one, and true for anything else
  (`plan.ReadyStepRetry.CleanOnRetry`). It cleans only a `uses:` step's
  directory before its next try, and only on the machine the run started
  on, by a caller that runs there: never a host's copy, as in format 1,
  nor, for a caller on a host, the copy where the run started, which
  format 1 cleans ([Workflows a step calls](#workflows-a-step-calls)).
  It never cleans the caller's run directory, or one that holds it, and
  so never the root run's, which holds the caller's, as format 1 never
  does; a run's logs aren't in a run's directory, so format 1's log
  directory has nothing to match. That is stricter than format 1 in one
  place: format 1 spares only the root's directory, and would clean, for
  a call in a child, a `working-directory:` that names the child's own
  directory, with what its steps keep there for its later steps, which
  no workflow could mean. That differs only for such a step, and none of
  the 270 known calls sets a `working-directory:`.
- **Batch schedulers don't rerun jobs either.** Jobs are submitted with
  `--no-requeue` (Slurm) or `-r n` (PBS), so the scheduler doesn't rerun one
  behind Meridian's back. That holds for jobs with a `retry:` too: Meridian
  places their new attempts itself, so counting, backoff and fencing work
  the same on every backend.
- **A worker that stops heartbeating is checked with its backend** before it
  is declared lost: `squeue` or `qstat`, the pod's status, the process on the
  SSH host. So a worker that is only cut off from the coordinator isn't
  mistaken for one that is dead ([Leases and fencing](#leases-and-fencing)).

### Job retries

A job's `retry:` says when Meridian may run the whole job again, from its
first step, as a new attempt:

```yaml
jobs:
  simulate:
    runs-on:
      execution-group: ${{ inputs.group }}
    retry:
      reasons: [worker-lost]
      max-retries: 2
      interval: 5m
    steps:
      - run: ./simulate --resume
```

- **`reasons`** lists why an attempt may be run again, and is required, so a
  job's `retry:` can't be mistaken for one that reruns failed steps. Its
  values are the reasons Meridian records when an attempt ends. The only one
  for now is
  `worker-lost`: the worker was lost after a step started, for example
  because its node failed or the job was preempted.
- **`max-retries` and `interval`.** A job runs again at most `max-retries`
  times, 3 by default. The first new attempt waits `interval`, 1 minute by
  default, and each one after it twice as long as the one before, up to an
  hour. A job's `retry:` is Meridian's addition, so these defaults are its
  own, and can still change. There is no per-attempt `timeout`, since a job
  has its own.
- **The new attempt is placed again**, so it can go to another member of
  the job's group.
- **What the lost attempt wrote stays** in the run directory, so a job that
  checkpoints can resume, on a member that shares that directory.
- **The attempt number** is in `PW_WORKFLOW_JOB_CURRENT_RETRY` and
  `PW_WORKFLOW_JOB_MAX_RETRIES`, set when a job has a `retry:` and named
  after format 1's variables for a step's retries, so a step can tell that
  it is resuming.
- **Only the workflow can ask for it.** Whether a job is safe to run twice
  depends on its code, so neither a group nor a command-line flag can turn
  it on.

### Reruns

There is no retry for a whole run. It would rerun jobs that had succeeded,
and a failure that ends a run would usually happen again.

A person reruns a run with `meridian rerun`, which isn't built yet, either
all of it or only its failed jobs and the jobs that depend on them. A rerun
uses the run's stored plan, so it runs the same revision, with the same
workflows pinned, and the jobs it doesn't rerun keep their outcomes and
outputs.

A rerun is the stored plan and what is submitted again. The run's secret
record was deleted when it ended ([The secret record](#the-secret-record)),
so the person rerunning it submits its inputs again, passwords among them,
as its launch form does. Create runs again on what is submitted, with the
workflow the plan pins, and gives the rerun's secret record; the plan it
gives must be the stored one, as a plan doesn't depend on a variable's or a
password's value ([The plan](#the-plan)): one that differs, but for the
jobs' graph, which Start checks, the variables' names, whose kinds can
have changed, and the release that compiled it, means what was submitted
isn't the run's, and the rerun is refused. The release is another whenever
the build is, even of one commit, whose version a build without its VCS
information, or with uncommitted changes, writes otherwise, and whenever a
coordinator is upgraded; so the plan's digest, by which the rerun's record
names the stored plan, leaves it out (`plan.Digest`), as the comparison
does. Start then starts it as it starts any run: it reads each variable's
value as it is then ([Variables and secrets](#variables-and-secrets)), and
the platform's resources as they are then. A rerun whose shape would now
differ fails with `run_shape_changed`. Each attempt of a job, and each retry
of a step, keeps its own logs, so earlier attempts can still be read.

Whether a platform should keep what was submitted, so that a rerun needn't
ask for it again, is open ([Open questions](#open-questions)).

### Variables and secrets

A run reads organization and user variables through the Variables
interface, by name, plain or secret, and keeps their values only in its
secret record ([Run data](#run-data)).

- **At Create**, the compiler reads only the plain variables read by what
  it evaluates: what decides the run's shape, and the values of inputs that
  read one, which it checks against their types. It reads no secret
  variable's value; what reads one waits for Start. It asks the Variables
  interface for the variables the workflow's expressions read, by name, or
  a context's whole for one that reads every variable of it, and a plain
  one whose value can't be read fails the run then.
- **Once at Start**, the coordinator reads every variable the plan lists, a
  context's whole when the plan names it alone, and keeps them in the
  secret record, so every expression of the run reads the same values. A
  variable is a secret if the Variables interface says so as the run
  starts, or said so as it was created: one that became a secret in
  between is masked, and one that stopped being one is masked still.
- **Text.** A variable's value is text. One the compiler is given that
  isn't, in `Options.Contexts`, reads as `""`, as one that isn't set does,
  and the compiler warns of it (`variable_not_text`); the Variables
  interface gives only text.
- **Each job gets its own.** An attempt receives only the inputs,
  variables and secrets its own job's expressions read, directly or
  through an input made from one, when it is claimed, which `plan.Job.Reads`
  finds from the expressions its worker evaluates: its `env:`, its steps
  and its `outputs:`. A read of each variable of a context, as `vars.*` is,
  gives all of them. Every attempt receives the workflow's `env:`, which
  every step's environment holds. Of the jobs it needs, it receives each
  one's outcome, and only the outputs, and steps' outputs, that the same
  expressions read, which `plan.Job.ReadsNeeds` finds: format 1 reads a
  job only as `needs.<job>.outputs.<name>` and
  `needs.<job>.steps.<id>.outputs.<name>`, and Meridian as
  `needs.<job>.result` too, and any other reference as its own text, so
  the grant is exact; a `*` in the place of a name gives each. So its
  worker masks only those of the needs' secrets that the job reads.
- **Declared variables.** Format 1's `needs.organizationVariables` lists
  the organization variables a workflow requires: as in ACTIVATE, a run
  fails when it is created if the Variables interface doesn't have one,
  naming each one missing. Its `needs.userVariables` lists the user
  variables a workflow asks to read: as in ACTIVATE, a secret user variable
  a workflow doesn't list isn't given to it, and reads as `""`, and the
  compiler warns where it is read. A platform can ask the person running a
  workflow to approve the variables it lists.
- **A rerun reads them again.** It sees each variable's value as it is
  then, not as it was when the run first started. If the Variables
  interface keeps versions of a variable, a run could record the versions
  it read, so that a rerun reads the same ones.
- **Secrets are masked; plain variables aren't.** A secret variable's
  value is masked in logs, and so is a value an expression made from one,
  which its evaluation marks. A plain variable's value isn't masked: a
  variable that holds a secret should be a secret variable, and the
  compiler warns when a plain variable's name, such as `HF_TOKEN`, looks
  like a secret's.
- **What is masked is text.** The masker is given strings: a secret's own
  text, however short, unless it is empty, which is never masked, and that
  of a secret that is a number or a boolean, as a password submitted as
  one is, as an expression writes it, `8` or `true`, so that a password
  `true` masks every `true` in the logs, as never showing a secret is the
  rule, and as a step's output that holds a secret's text holds a secret
  ([Outputs](#outputs)), any output with `true` in it is then kept as one,
  and the jobs that need it mask `true` too; and the
  text a template made from one, and that each `${{ }}` in it that read
  one gave, so that a field of a structured secret, such as
  `${{ fromJSON(vars.creds).token }}`, is masked wherever a command prints
  it, and not only within the command. A secret in one value of a step's
  `with:` masks only what that value's template gave. A string made from a
  secret is masked when it is at least 8 bytes, or when it is a credential
  field of a structured secret: a string, or a number's text, of at least
  4 bytes, but `true`, `false` or `null`, that a secret's text, read as
  JSON, holds as the value of a key a credential is kept under, or as an
  item of a list that is, but not in a mapping under it, which has keys of
  its own: `{"pin":1234}` holds `1234`, and `{"token":{"type":"bearer"}}`
  nothing. Such a key is one whose words, split as an input's name is for
  the warning about secret-looking names ([Compiling](#compiling)), at
  anything that isn't a letter or a digit, where letters and digits meet,
  and at changes of case, whatever their case, hold a credential word,
  `password`, `passwd`, `pass`, `passphrase`, `passcode`, `pwd`, `pw`,
  `secret`, `token`, `pin`, `credential` or `cred`, or the plurals
  `passwords`, `tokens`, `credentials` or `creds`, whose value holds them
  only as a list's items; or `key` or `keys` after a qualifier, `api`,
  `private`, `secret`, `access`, `client`, `signing`, `encryption`,
  `master`, `ssh`, `account` or `service`; or a word written without
  breaks that is one of these after a head such as `api`, `access`,
  `auth`, `client` or `db`. A key whose last word describes a credential
  rather than holding one, `name`, `id`, `type`, `url`, `uri`, `endpoint`,
  `path`, `file`, `alias`, `prefix`, `policy`, `length`, `version`,
  `schema`, `region`, `arn`, `format`, `algorithm`, `expiry`, `ttl`,
  `scope` or `provider`, or whose last word is `key` without a qualifier,
  isn't one, and nor is `secrets`, which names a section in
  docker-compose and GitHub Actions; but a mapping under `secrets`,
  `credentials` or `creds` holds credentials as its own values, one level
  down, as `{"secrets":{"github":"ghp_…"}}` does, where
  `{"secrets":{"db":{"file":"./db.txt"}}}` and `{"secrets":"inherit"}`
  hold none. So `password`, `api_key`,
  `accessToken`, `password2`, `OAuth2Token`, `apikey` and `clientsecret`
  are, and `key`, `sortKey`, `KeyName`, `keyFile`, `tokenUrl`,
  `secret_name`, `token-key`, `keyboard`, `monkey` and `passport`
  aren't. Each credential field of a secret's own text is masked however
  it is made, so a password that a step's script takes out of a secret's
  JSON, as `jq` does, is masked in its logs. What masks a job's secrets'
  own texts is built once for each attempt (`plan.JobState.JobEnv`), as a
  large secret's takes long to build, and found again for a state whose
  secrets differ, as another run's or attempt's do. It is never
  given another short one, such as a `yes` that a secret decided, or a
  database's name or a region a structured secret holds, which common text
  holds too, nor the text of a boolean, a number or null that one decided,
  such as whether a token is set, which would mask every `yes` or `true`
  in the job's logs. A job output made from a secret, of whatever type, is
  private and a secret instead ([Outputs](#outputs)).
- **What a run was given in a secret's place is masked as what is made
  from one.** A value the run was given in the place of what is made from a
  password or a secret variable, as one a person typed over such a
  default is, is a secret the plan names as derived (`plan.Run.Derived`,
  `plan.RunValues.Derived`): it can be made from a secret or not, so the
  masker is given it as it is given what an expression makes from a
  secret, from 8 bytes or as a credential field, and never as a secret's
  own text, and it isn't a secret's text whose credential fields are
  masked. What an expression makes from it is a secret, as what is made
  from any secret is. A value the run was given in the place of what is
  made from a variable that isn't a secret (`plan.Run.Private`,
  `plan.RunValues.Private`) is private, as the variable's value is: the
  phases read it as private, and nothing masks it.
- **A secret is masked as it can be shown.** Its shown forms are masked
  too: without the line breaks it ends with; as JSON escapes it in a
  string, as Go's `encoding/json/v2` writes it, as `encoding/json` writes
  it with `<`, `>` and `&` escaped, with U+2028 and U+2029 escaped, and
  with every character outside ASCII escaped, as Python's `json.dumps`
  writes it; and, in logs, each of its lines on its own, as a key printed
  a line at a time shows, as GitHub Actions masks a secret of more than one
  line line by line. A line is masked on its own only when it is at least
  8 bytes, with a letter or a digit, and isn't `true`, `false` or `null`,
  so that a JSON or YAML secret's `},` or `true` lines mask nothing else.
- **What holds a secret.** A step's output holds a secret when it holds
  what the job's logs mask, as it can show, but for a line of a secret,
  which plain text can hold too: a secret's own text, or a string made
  from one that is masked. A job's later steps read such an output as a
  secret, and it, and an output made from it, keep their values, but are
  private and secrets ([Outputs](#outputs)).
- **What a script does with a value can't be followed.** A secret that a
  step's script transforms, such as by encoding it in base64, isn't known
  to come from the secret, and isn't masked, but for a credential field a
  script takes out of a structured secret, which is masked whatever made
  it.
- **A job masks only its own secrets.** A job's worker masks the secrets
  its attempt received, which are those its expressions read. A secret
  another job wrote to the run directory, which the job's steps can read
  too, isn't masked in its logs: giving every attempt every secret, so that
  it could, would give each one more than it reads.
- **Diagnostics never quote a variable's value.** Warnings and errors kept
  with a run show `***` in place of a value that came from a variable.
- **Permissions.** Format 1's `permissions:` lists what the run's token for
  a platform's API may do, `'*'` for all that the person running it may.
  Meridian keeps it in the plan and gives it to the platform, which issues
  the token and can ask the person to approve it. Meridian has no API of
  its own for such a token to reach, so locally it has no effect.

Today the compiler lists the secrets and variables of the run as a whole,
and checks neither declared list.

### Leases and fencing

A running attempt holds a lease of its own, with a fencing number that
grows with every attempt: the job's next, which the transaction that makes
the attempt takes. Both are in Meridian's row for the attempt, not
hopper's, whose leases belong to the client that claimed a job, where an
attempt's must outlive the replica that placed it. The worker renews the
lease with heartbeats, through whichever coordinator replica it reaches,
and every write it makes (step events, logs, outputs, the result) carries
the attempt's credential, which names the attempt, and is stored only
while the attempt holds the job's fencing number and hasn't ended, which
the write's own transaction checks. A write from a superseded attempt
fails (`fenced`), and the worker that made it stops its job; a repeat of
the result the attempt already stored, as a worker sends after a dropped
connection, is answered as the first was. A heartbeat fails too once the
result is stored, so a worker that has sent its result takes the answer
to it, repeated until it has one, as saying how the attempt ended, not a
heartbeat's.

The coordinator's numbers are the same for every job; only tests change
them:

| | | |
| --- | --- | --- |
| Lease | 60 s | How long a lease lasts after a heartbeat. |
| Heartbeat | 10 s | How often a worker renews its lease: a fifth of the lease at most. |
| Margin | 15 s | How long past a lapsed lease the coordinator waits before it starts a self-fencing job's next attempt. |
| Grace | 60 s | How long after a replica takes over, or its host wakes, before it counts a lapsed lease. |

Fencing numbers keep a superseded attempt's reports out of the store, but
not what it writes elsewhere, such as checkpoints in the run directory.
Only a job whose `retry:` lists `worker-lost` can have a second attempt
while the first may still be running, cut off. So its workers fence
themselves too, as a Kubernetes controller that can't renew its leader
lease stops leading:

- **The worker stops the job before its lease lapses**, so that every
  process the attempt started is dead by then. Counted from when it sent
  the last heartbeat that succeeded, on whichever of its monotonic and wall
  clocks has gone further, since the monotonic clock stops while a host is
  suspended: from six tenths of a lease period without a renewal it may
  start stopping them, with SIGTERM, and by nine tenths it has sent every
  process group the attempt started SIGKILL, and stopped waiting for them,
  as `workerapi.Claim.SelfFencing` says: with the coordinator's 60-second
  lease, SIGTERM at 37 seconds, SIGKILL by 49, and no more waiting by 54.
  It shortens the grace period of a cancellation in progress to do so. A
  process that left its group, as with `setsid`, isn't its to stop. It
  then stops renewing the lease, and sends what it has until the lease
  lapses, when its credential stops working. It reads the wall clock at
  least every second, so once a host that was suspended past the lease
  resumes, a step can run for up to about a second more.
- **Its heartbeats** come often enough to try three times before it would
  start stopping the job, more often than the coordinator asks if need
  be, and it gives up on one that hasn't been answered in a third of the
  time then left, to send another, so that one heartbeat held by a
  connection that is half open doesn't stop the job: every 10 seconds,
  each given 9, and one that fails tried again within a period, so at
  least three tries before 37 seconds.
- **The coordinator waits** until the lease has lapsed and the margin
  passed, 75 seconds after the last renewal by the store's clock, before
  it starts a new attempt: 21 seconds after the worker stopped waiting,
  which covers clocks that run at different rates, the second a resumed
  host's worker can take to notice, and the time a commit takes. Neither
  side reads the other's clock; each only measures the period. The
  store's clock never goes back, for any of its coordinators: it is the
  database host's, or the latest time one of them recorded, if that is
  later, so a host clock stepped back stops it until it catches up,
  rather than let a coordinator that started since take a lease another
  renewed for older than it is. A clock stepped back further than the
  grace, as a host set to an old date, or one bad reading, is a reset:
  the store's clock takes the host's again, brings each time a run, a
  job, an attempt or an endpoint recorded ahead of it back to it, so that
  a lease renewed then lapses a lease period after the reset, starts the
  grace for every coordinator, and wakes the supervisor of every readied
  job of a running run, which may have been put off by the clock that was
  ahead.
- **The cost** is that such a job starts stopping when it is cut off from
  every coordinator replica for about six tenths of a lease period, 37
  seconds, even when its host is fine. Its new attempt resumes it, as its
  `retry:` asks.

Other jobs keep running while they are cut off, since nothing runs them
again: their workers spool reports and deliver them when they reconnect
([Running steps](#running-steps)). Their credential lasts until the
attempt ends, however long ago its lease lapsed.

A worker is lost only when its backend says it is, or, for a job that
fences itself, once its lease has lapsed and the margin passed. When the
lease of an attempt that doesn't fence itself lapses, the coordinator asks
the attempt's backend, and again every heartbeat period while the backend
says the worker is running or queued, or can't decide, as over an SSH
connection that is down, which counts as not lost. A backend that can't
decide for a cause that won't pass by itself, as a host key that
known_hosts doesn't have, or that has changed, says what it is, as a
problem: that counts as not lost for five minutes from the first such
check since the backend last decided, or the attempt's lease was last
renewed, as the worker that comes back does, which the store records with
the attempt, whichever replica checked, time for a person to see the
problem and fix it; and then the attempt is lost, with the problem, which
its end, its events, and its job's diagnostics in `meridian status`, show. Once the backend says
the worker has ended without completing its attempt, the attempt ends
with the reason `worker-lost` if a step had started, and otherwise as a
failure before its first step, which is placed again
([Failures and retries](#failures-and-retries)); or with `timeout` when a
scheduler ended it at its time limit, and `out-of-memory` when a scheduler
or the kernel ended it for its memory. The backend of an attempt whose
member its group no longer has can't be asked: the attempt is lost once
the grace has passed since its lease lapsed, or since it was placed if no
worker claimed it, with `worker-lost` once a step has started and
otherwise as a failure before its first step (`member_gone`). A
coordinator keeps the member an attempt was placed on for a minute before
it asks the group again, so a member removed from its group can be taken
for one it has, or one added back for one it doesn't, for up to a minute.

When a replica takes over what workers connect through
([Connections](#connections)), as `meridian run` or `resume` does after
another stopped, and when a replica sees its wall clock jump more than 10
seconds past its monotonic clock, as when a laptop wakes, no replica of
the store counts a lapsed lease until the grace has passed: the store
records when the grace began. Jobs keep running while Meridian
is down, but for those that fence themselves, and a cut-off worker tries
again at least every 30 seconds, so it reaches the replica at least twice
within the grace: a laptop waking up or `meridian run` restarting doesn't
make them all look lost.

### Cancellation, timeouts and cleanup

A run stops early when a person cancels it, or when it runs past its own
`timeout:`, and does what format 1 does in each case. Either way, what
happens to a job depends on whether its `if:` is cancel-aware: whether it
calls `always()` or `cancelled()`, negates `success()` or `failure()`, or
is, or evaluates to, the word `always`, as a step's can be (below).
Whether it evaluates to `always` is known at once, from the run's values,
for an `if:` format 1 evaluates before the job's needs have ended, and
for any other only once they have (`plan.Plan.CancelPath`).

- **Stopping a run is stored first**: the stop record, when and why, in
  the run's row, in one transaction that also tells every running attempt
  of a job whose `if:` isn't cancel-aware, through its control poll; then
  the supervisor of every readied job, and of every matrix instance, is
  woken, and one still waiting for its job's needs readies the job, as
  stopped, once they have ended. A placement of such a job that no worker
  has claimed yet is told too, and isn't withdrawn: the job was readied,
  so its needs had all ended, and format 1 would be running it. Its worker
  reads the stop before its first step, so the job runs only its cleanups
  and the steps that outlive a cancel, as format 1's would: after a
  cancel, its other steps end cancelled; after the run's timeout, they end
  as format 1's do when they start under the passed deadline, failing but
  for those that outlive a cancel and a last try that ignores its errors.
  With a backend that queues jobs, a stopped run so ends only once such a
  job's worker has claimed it and stopped, or its backend says the worker
  has ended. A running attempt that is stopped is waited for while it
  renews its lease, since its cleanups have no timeout, and its placement
  is withdrawn (`scancel`, `qdel`, deleting the pod, signalling the
  process) if its lease lapses, once the grace in which no lapsed lease
  counts has passed. A job between attempts when the run stops, readied
  before the stop, as one waiting to be placed again after a failure
  before its first step, or after its worker was lost, or for a backend
  to answer, or one whose attempt a supervisor that ended chose a place
  for but never placed, isn't placed again, unless it outlives the
  stop: it has no placement, so it ends cancelled after a cancel, and
  failed, with the reason `timeout`, after the run's timeout. Format 1
  would start it under the passed deadline, which would run its steps
  that outlive a cancel; here they don't run. No attempt is made once the
  run has stopped since the job's supervisor looked: it looks again.
  `meridian cancel` stores the record whether or not a coordinator is
  running, and when none is, it coordinates the run itself until it has
  ended, so that the placements the stop tells are told, or withdrawn
  ([Status, logs, resume and cancel](#status-logs-resume-and-cancel)).
- **A running job, when a person cancels the run**, has the step it is
  running stopped, if its `if:` isn't cancel-aware: SIGTERM, then SIGKILL
  after a grace period. The step and the job end cancelled, and the job's
  later steps are skipped, but for those whose `if:` outlives a cancel;
  the cleanup of each step that ran still runs (below). A cancel that
  comes between steps, as format 1's does, cancels the next step that
  comes to run whose `if:` passes, with `cancelled()` still false: it ends
  cancelled without starting, and its cleanup runs. Only a step that
  ended cancelled cancels the job, so one whose steps after the cancel
  all outlive it ends as they did.
- **A job whose `if:` is cancel-aware runs on**, as format 1 runs it apart
  from the run: neither a cancel nor the run's timeout stops it, whether
  it is running, or placed and not yet claimed, which is then claimed and
  runs. It ends as its steps do: neither stops it, though its own
  timeout still does.
- **A job not yet readied**, still waiting for its needs, takes one of
  three paths, by its `if:` (`plan.Plan.CancelPath`):
  - **One that is cancel-aware** waits until its needs have ended. Its
    `if:` is then evaluated with the status of its needs alone, as a
    job's always is: `cancelled()` is true when a job it needs ended
    cancelled, and `failure()` when one failed, or was skipped because
    its own needs failed. If it is true, the job runs, and runs on as
    above.
  - **One that calls a status function, or reads `env`, an output or the
    result of a job it needs, or an input whose value reads `env`**, and
    isn't cancel-aware, waits until its needs have ended, then ends
    cancelled, unless its `if:` then evaluates to the word `always`, when
    it runs on as above. These are the `if:`s format 1 leaves until the
    needs have ended, and those that read Meridian's `needs.<job>.result`.
    A reference to `needs` that format 1 reads as its own text, such as
    `needs.build` alone, isn't one.
  - **Any other**, a job with no `if:` among them, ends at once, skipped
    because its needs failed, as format 1 ends a job whose dependency
    failed, unless its `if:` evaluates to the word `always` with the run's
    values, when it is cancel-aware, and takes the first path. A job that
    needs none was readied when the run started, so it never takes this
    path.
  - **Where format 1 takes another path**, which a later format may
    settle:
    - ACTIVATE finds the `if:`s it leaves until the needs have ended by
      matching their text, so text that reads like an output, as in
      `contains('needs.dep.outputs.x', inputs.y)`, puts an `if:` on the
      second path there. Meridian reads the expression, and keeps it on the
      third.
    - ACTIVATE leaves an `if:` that reads an input made from `env` until
      the needs have ended only when evaluating it reaches that input as
      an operand: `inputs.go || inputs.fromenv == 'x'`, with `go` true,
      stops short of it, and a group read whole as an operand that isn't
      text, as in `inputs.group && true`, doesn't count. Meridian reads the
      expression, and puts both on the second path.
    - A password input's default is in the run's secret record, not the
      plan (`Run.Withheld`), so Meridian doesn't follow a reference through
      one to `env`, and the `if:` keeps the path it has otherwise.
    - An `if:` that reads `matrix`, of a matrix job Ready has yet to
      expand, is taken as not evaluating to `always`. ACTIVATE expands
      every matrix before the run starts, so it never meets this case.
- **A matrix instance waiting for its turn** under `max-parallel` ends
  cancelled at once, whatever its `if:`, as format 1 ends one waiting for
  its turn, after the run's timeout too, with the reason `timeout`.
- **The run's outcome after a cancel** is cancelled, whatever its jobs did
  after it, an `always()` job that succeeds among them. It comes from the
  stop record, as format 1's comes from its record of an external
  cancellation, rather than from the jobs.
- **The run's own timeout isn't a cancel.** A job of the run's own waits
  for it, and then stores the stop record with the reason `timeout`. When
  the timeout is private, that job wakes at least every minute rather than
  at the timeout, so that hopper's schedule doesn't show it. The step
  running in a job whose `if:` isn't cancel-aware then fails with the
  reason `timeout`, and so does its job; a cancel-aware job runs on, as
  after a cancel. The job's worker is told to stop it with the reason
  `timeout`, which it takes as the job's own timeout, below. Jobs not yet
  readied take the same three paths as after a cancel. The run's outcome
  then comes from its jobs, as format 1's does: failure if one failed, as
  a stopped one has, or was skipped because its needs failed, with the
  reason `timeout`, which Meridian adds since format 1 records none; and
  success when nothing failed, as when only cancel-aware jobs were left
  and they succeeded.
- **A job's own timeout** ends only that job, and the worker enforces it.
  The job isn't cancelled: the step it was running fails, and so does one
  that comes to run after it, so `failure()` is true after it and
  `cancelled()` false, even if a cancel follows, and the job fails with
  the reason `timeout`, as in format 1, whose context keeps the reason it
  ended with first. That is so when the timeout fails a step: one that
  comes once no step is left that it stops, as during a last step that
  outlives a cancel, or the cleanups, leaves the job ending as its steps
  did. A step with `ignore-errors:` whose last try, with no retry left, a
  deadline stops, its own, the job's or the run's, succeeds, as in format
  1, and so does one with no retry left that comes to run after the
  job's or the run's deadline has passed. An earlier try that the job's
  or the run's deadline stops fails, and isn't tried again, as does a
  step waiting to try again when it passes. A job whose deadline stopped
  only steps that succeed so fails no step, and ends as its steps did,
  with no reason. The run goes on: the jobs that need it are skipped
  because their needs failed, and those whose `if:` calls `failure()`
  run. The coordinator enforces it too, as a backstop for a
  worker that doesn't: once the job's timeout, the worker's own time to
  stop its steps, a heartbeat period and the margin have passed since the
  attempt was claimed, without its end, it tells the worker to stop the
  job, with the reason `timeout`, and withdraws its placement once its
  lease lapses.
- **A step's timeouts:** the worker enforces them. As in format 1, a
  step's `timeout:`, and its `retry:`'s, bound each of its tries, and a try
  past the step's own `timeout:` isn't tried again.
- **A step whose `if:` is cancel-aware**, one that calls `always()` or
  `cancelled()`, negates `success()` or `failure()`, or is the word
  `always`, outlives its job's cancellation and timeout, as in format 1: it
  runs once they have happened, and they don't stop it once it runs, nor
  does its own `timeout:`, though its `retry:`'s timeout, its
  `early-cancel:` and the attempt ending without the worker do. Step marks
  it (`plan.ReadyStep.OutlivesCancel`). Format 1 dropping such a step's own
  `timeout:` is a candidate for a later fix.
- **Once a job is stopping**, cancelled or past its timeout, its other
  steps that haven't started don't run. The first of them that comes to
  run whose `if:` isn't cancel-aware, as one whose `if:` passes with
  `cancelled()` still false, ends cancelled, or failed after a timeout,
  without starting, and its cleanup runs; those after it are skipped, as
  `success()` is then false. Format 1 starts such a step and stops it at
  once, so no workflow can rely on its running.
- **A later cancellation that gives less time**, or the attempt ending
  without the worker, makes SIGKILL sooner. When an attempt ends without
  its worker, every process group the attempt started is stopped, those of
  steps that have ended and left processes behind among them. So that no
  other group can be given such a group's number, the worker keeps the
  group's leader, the step's shell, unreaped, on Linux, while the group
  has a process left: `ps` shows it as defunct, and `kill -0` on its
  process ID succeeds, until the processes the step left behind have ended
  or the attempt ends.
- **`early-cancel: any-job-failed`** stops a step when another job of the run
  fails, or a step of its own job did, as in format 1. Format 1 counts a
  job as failed from the moment one of its steps fails, after its last
  try, while the job's later steps that outlive it and its cleanups still
  run; and from the moment the job fails otherwise, as when its `if:`, its
  setup or its outputs fail. The coordinator tells the step's worker so:
  the first failure marks the run, in the transaction that stores it, and
  every running attempt's control poll then says so, as a new attempt's
  first one does. A failed step's events mark it as they are stored: at
  once for a cleanup, which is never tried again, and for a try its
  worker says was its step's last, as a worker of the worker API's
  version 2 does. Of a worker of version 1, which doesn't say, a try that
  the step's `retry:`, written out, says is its last marks it at once,
  and another once the next event shows the step wasn't tried again. A
  job that ends failed marks it as it ends. An attempt that ends for a
  reason its job's `retry:` lists, as one whose worker was lost, which
  another attempt may follow, doesn't mark it: the job marks it once its
  last attempt has ended, failed. Format 1 doesn't count one failure: a job whose
  `if:` fails to evaluate, which it fails without saying so, where
  Meridian fails it through Ready's diagnostics, and marks the run. The
  step ends cancelled,
  which cancels the rest of its job, as in format 1: a later step runs only
  if it outlives a cancel, and the job ends cancelled unless a step of it
  failed (`plan.JobState.Cancelled`). It doesn't cut short the wait between
  a step's tries; a cancellation and the job's timeout do, and the step ends
  cancelled, or failed after a timeout. A `uses:` step whose child run was
  cancelled from within it while its job goes on counts as a success, as
  format 1 goes on with the job after one, and cleans it up.
- **A step's `cleanup:` command** runs at the end of its job if the step
  ran, in reverse order, as a step of its own named `POST` and the step's
  name, which outlives a cancel and has no timeout or retry of its own, as
  format 1 runs a copy of the step. The step's `if:` is evaluated again,
  with the job's status as it ends and without `success()`, so a step with
  no `if:` is cleaned up whatever the job's outcome, and one whose `if:` is
  false then isn't: 37 of the 155 `cleanup:`s of the known workflows are
  on a step with an `if:`, and none of those calls a status function. It is
  evaluated as it runs (`plan.ReadyJob.Cleanup`), with every step's
  outputs, its own and later ones' among them, and reads one that nothing
  wrote as `""`. What it writes to `$OUTPUTS` is the job's, over what its
  steps wrote. A cleanup that fails fails the job, as a step does: the
  cleanups after it read `failure()` as true, and the job's `outputs:`
  read an output that nothing wrote as `""`. A step whose `if:` fails to
  evaluate didn't run, as format 1 counts it, and its cleanup is skipped;
  the step fails its job. A cleanup that is skipped has a finished event
  that says so, as a skipped step has, and as format 1 shows its copy of
  the step. A worker whose attempt ended without it, because another
  superseded it or its lease lapsed, runs none: another attempt may be
  running, and a cleanup could remove what it is using.

### Outputs

A step writes outputs to the file named by `$OUTPUTS`, a line `name=value`
each. As in format 1, a step with an `id` has a file of its own, whose
outputs are its `steps.<id>.outputs`, and the steps without one share the
job's. A file with a line of 64 KiB or longer is read as holding none, and
the step's event warns of it. Each byte of a name or a value that isn't
UTF-8 is replaced with U+FFFD, as format 1 stores outputs, with
`encoding/json`, which replaces it so, and the step's event warns of it:
JSON, as the worker API's is, holds only UTF-8. A step's log keeps the bytes
it printed as they are. A symbolic link a step puts there is
followed, as format 1 follows it, but what is read must be a regular file,
opened without waiting, so that a FIFO there fails the step rather than make
the worker wait forever, as it makes format 1 wait, which is a bug; format 1
is to fail it too. A job's outputs are capped at 1 MB and a run's at 50 MB,
a limit of Meridian's, which format 1 doesn't have: the worker reads no more
than 1 MiB of a step's file, counting each byte that isn't UTF-8 as the
3 bytes of the U+FFFD that replaces it, and a step whose file is larger
fails, but for
one without an `id` whose job's shared file was already larger, which can't
add to it. A job's outputs are format 1's: what its steps wrote, the later
step's where two wrote the same name, what its cleanups wrote over that, and
its `outputs:` over all. The worker reports what each step wrote as it wrote
it, and End decides which outputs are private and which hold a secret; an
event shows a step's outputs masked as its logs are.

An output keeps its value, as format 1 passes it to the jobs that need the
job, whatever it was made from: masking it would change what those jobs
read. Instead:

- **An output that holds a secret** is private and a secret: one made from
  a secret, or that holds a secret's text as the job's logs mask it, but
  for a line of one, and so is a step's output that holds a secret's text.
  End names it, with no warning, since passing a token to the jobs that
  need it is common and safe so; the coordinator keeps its value only in
  the secret record, never in run state; and the
  jobs that need the job read it as a secret, so what they make of it is
  masked in their logs and problems, and an output they make of it holds a
  secret too. Logs and problems mask the secret's text in it, as they do
  everywhere. An output that holds a secret's own text, or a credential
  field of one, however short, reaches the jobs that need the job as such:
  they mask it however short, count it among the secrets' own texts, and
  find an output of theirs that holds it, as a dependent's step that writes
  a 4-digit PIN it was given does. One that holds only what was made from a
  secret, such as its length, End names as derived (`EndResult.Derived`,
  `JobResult.Derived`), and the jobs that need the job mask it as what is
  made from a secret is. That End counts an output as holding a secret when
  it holds a string made from one costs only privacy, never a changed
  value. GitHub Actions leaves such an output out.
- **An output made from an organization or user variable or a password**,
  and not from a secret, isn't masked, since a plain variable isn't, but
  is private: End names it, the coordinator keeps its value only in the
  secret record, never in run state, and the jobs that need the job
  receive it with their needs and read it as private, so what they make of
  it is kept out of run state too.

When a job is claimed, it receives its dependencies' outcomes and outputs
with it, rather than reading each one back: the coordinator joins what run
state holds of each dependency's result with what the secret record holds
(`plan.JoinJobResult`), and gives the job those its expressions read
([Variables and secrets](#variables-and-secrets)).

## The coordinator

The coordinator is a hopper client whose handlers keep no state between
calls: each reads the job's state, acts, and stores what it did. Any replica
can therefore handle any job, and a replica that dies loses nothing. What a
replica keeps is a cache of the plans it has decoded, which never change.

- **Supervisors drive jobs**, one for each job and each matrix instance,
  as hopper jobs that any replica works ([On hopper](#on-hopper)). A
  worker's report that changes a job, stored by the replica the worker
  reached, wakes the job's supervisor, wherever it runs.
- **Placement is idempotent.** A placement carries its attempt's ID (in a
  Slurm or PBS job name, a pod label, the local backend's placement file),
  and before placing, the coordinator looks for one that already exists. A
  coordinator that crashed between submitting and recording never submits
  twice: a supervisor that another replica rescues finds the placement, and
  the job's fencing number keeps a second one from reporting.
- **It places jobs** on the members of their groups
  ([Placement](#placement)), within each member's capacity and priorities.
- **It serves the worker API** (below).
- **Every change to a run** adds an event to the run's events in the
  transaction that makes it, so whoever follows the run, in any process,
  sees one ordered stream.

Locally, `meridian run` creates the run, coordinates it in the foreground,
with SQLite, and exits when the run ends.

- **Every `meridian run` is a replica**, and so is every `meridian resume`.
  One at a time holds the socket workers connect to, under a lock on a file
  beside the store's database, and serves every run's workers; the others
  work the store's jobs too, and follow their own run. When the holder
  exits, one that waits for the lock takes the socket over, and the
  workers reconnect to it.
- **Start runs where the run was made.** Only the `meridian run` that
  created a run works its Start, or a `meridian resume` of it once that has
  gone, so the variables it reads from environment variables are read from
  the shell that ran it.
- **Interrupting it.** In a terminal, Ctrl-C asks whether to cancel the
  run, leave it running, or keep watching it. Otherwise, as when a script
  runs it, an interrupt leaves the run running: `meridian run` detaches
  from it rather than cancel it, its workers keep running and hold their
  reports, and `meridian resume` picks the run back up. `meridian cancel`
  cancels.

`meridian serve`, which isn't built yet, is a coordinator that keeps
running, for example on a cluster's login node, and `meridian run` hands
its run to one if it finds one using the same store.

## Execution groups

An execution group is a named, ordered list of members. Each member is one
place jobs can run: a backend with its settings, such as an SSH host, a
Slurm partition and account, or a Kubernetes namespace, and the resources it
offers. A job names one group, usually through an input, and the group
decides where it runs. A group with several members is how a job falls back
from one system to another without the workflow naming them.

Groups come through the Groups interface: locally from the user's config
directory ([The `meridian` command](#the-meridian-command)), on a platform
from the platform.

### Naming a group

A job names a group with `runs-on: { execution-group: … }`. Format 1 has two
older ways to say where a job runs, and each becomes an inline group with
one member:

- **`runs-on: { environment: … }`** names a platform's scheduler
  environment by cluster and name, with its scheduling parameters, or takes
  it from a `compute-target` input. Only a platform's Groups resolver
  knows its environments, so elsewhere a workflow that uses one fails to
  compile, naming the key.
- **`ssh:`** names a host ([`ssh:` is deprecated](#ssh-is-deprecated)).

### Placement

When a job is ready, the coordinator places it on one member of its group:

1. **Filter.** Members that are unhealthy, or can't offer the job's
   `resources:`, are left out. A job that no member could ever run fails
   when it is placed, with each member's reason. Refusing it when the run
   is created, from the resources each member declares, isn't built.
2. **Rank.** The rest are ranked by whether they can start the job now,
   then whether they have capacity for it, then whether it would queue, and
   in the group's order within each.
3. **Record.** The attempt records the member chosen and why each other
   member wasn't. The member is recorded before its backend is asked, so a
   supervisor that ends before it records the placement finds it there; why
   each other member wasn't is recorded only for a job no member can run, so
   far.

A job moves to another member only when none of its code has run, or when
the workflow asked for it:

- **A failure before the first step starts** places the job again, on a
  member it hasn't failed on when there is one. When two attempts have
  failed for the same reason, on two members where its group has them,
  the job fails: the job, not the members, is then the likelier cause.
- **A step that fails** fails the job where it is. The failure is the
  workflow's own, and another member would only run its code again.
- **After a lost worker**, the job runs again only if its `retry:` asks
  ([Job retries](#job-retries)). The new attempt is placed the same way.
- **A job waiting in a queue** stays there. Moving it would be safe, since
  nothing has run, but when to give up on a queue is an open question.

## Workers

A worker is the `meridian` binary run as `meridian worker`. Each attempt
records the exact version and SHA-256 digest of the worker that ran it, and
a worker never upgrades itself.

### Starting a worker

The backend puts the binary where the job runs (copying it over SSH,
verified by its digest, or using one on a shared filesystem), then starts it
with three things:

- the coordinator's address;
- the SHA-256 hash of the coordinator's TLS public key, for a connection that
  goes over TLS;
- a bootstrap token: random and single-use, valid for this one attempt until
  the attempt is claimed or its placement withdrawn, however long a
  scheduler's queue holds the job, and stored only as a hash. It travels on
  a socket the worker inherits, in the job's environment, or in a file only
  the user can read, on a path no other user can change, never on a command
  line, where other users could see it in `ps`. The local backend gives it
  on the socket, where no other process of the user's can read it first,
  as one could a file: the worker says there that it has kept the user's
  other processes out of its memory, and only then is the token written to
  it. On macOS it never travels in the environment, and a file is removed
  as soon as the worker has read it: any process of the user can read the
  environment of any other of theirs there, as `ps -E` does
  (`bootstrap_token_in_environment`). Wherever it travels, the user's own
  processes can read it until it is claimed, from the file until the
  worker removes it, or, on Linux and FreeBSD, from the environment until
  the worker has hardened itself, the first thing it does; it is good for
  one claim, which the worker makes at once.

The coordinator looks for the attempt's placement before it makes the
token, and makes one only for a placement that doesn't exist yet. When the
worker claims the attempt, it presents the bootstrap token with a
credential it makes itself, which is the attempt's credential from then on,
so the credential never travels from the coordinator. A backend says the
digest of the binary it started, which the coordinator records with the
placement, and the coordinator refuses a claim from a worker whose digest
is another, before it sends anything of the run
(`worker_digest_mismatch`): the attempt fails before its first step, and
is placed again. A first claim before the placement is recorded, as from
a worker that claims before the call that started it returns, waits up to
two seconds for it to be recorded, and is then asked to come again (503),
never taken. The digest catches a binary replaced
between the placement and the claim; it isn't authentication, since
whoever holds the token can say any digest.

### The worker API

The API is HTTP with JSON bodies, under `/v1/`, with errors as RFC 9457
problem details, and a version of its own (below). Meridian's coordinator
and a platform that embeds Meridian serve the same API.

- **Claim:** present the bootstrap token, the worker's credential, and the
  newest versions of the API and of the plan the worker speaks and reads,
  and receive the version of the API the attempt is served at, and the job
  as Ready evaluated it, encoded as its plan is; the values of its own
  job's secrets and variables, and of the inputs Start completed that it
  reads; and its dependencies' outcomes, and the outputs of theirs that it
  reads.
- **Heartbeat:** renew the lease.
- **Control:** a long poll that delivers cancellation and `early-cancel`.
  A stop asked before the claim applies from the job's first step, as
  format 1 starts a job whose context has already ended. From version 2,
  the claim's answer holds the control as it is then, which the worker
  does as it asks before anything of the job runs, and its polls go on
  from it, so a slow coordinator can't let the first step start. A worker
  of version 1 reads it with a first poll, answered at once, before the
  job's first step, and a coordinator that doesn't answer that poll
  within a few seconds leaves the stop to the polls after.
- **Step events:** a step started, or finished with its exit code.
- **Logs:** each step's output, uploaded as a stream with byte offsets, so
  an interrupted upload resumes without duplicating or losing lines. An
  attempt's logs keep 1 GiB at most, all of them together; the log that
  reaches it keeps its bytes to there, then a line that says the rest
  wasn't kept (`log_truncated`), and ends. A replica reserves the limit
  1 MiB at a time, in one write, and spends it as uploads come, so a
  chatty step doesn't take a write for each upload. The store counts what
  every replica reserves, and gives each only what the limit has left, so
  the logs never keep more than it, whichever replicas serve the uploads.
  A replica spends what it reserved even once another has run the limit
  out, so a log after the one that reached it can still keep bytes, of
  what a replica that serves it holds; otherwise it keeps only the line.
  Every replica answers more of a log the limit ended as stored, since
  the worker was told the bytes before it were. What a replica
  reserved and didn't spend goes unused, so the logs can keep less than
  the limit: up to 1 MiB less for each replica that still holds some of
  it when the attempt's uploads stop, and for each time a replica lets go
  what it holds, after ten minutes without an upload.
- **Child runs:** ask for the child run of a workflow a step's try calls
  (`/v1/child`), with the step's `with:` and environment as Step evaluated
  them, which parts of them are private or hold a secret, and the strings
  the worker masks for the step, which a repeat answers with the same
  child, and a child that wasn't made with why, as `child_not_created`
  with its own problems; wait for its end (`/v1/child/end`), held as a
  control is, with how the child run ended; and stop it, with why
  (`/v1/child/stop`) ([Workflows a step calls](#workflows-a-step-calls)).
  The coordinator reads which workflow the step calls, and whether it
  outlives its job's cancellation, from the run's plan, never from the
  request. A child's attempt's claim says what it has of its tree: its
  root's ID and directory, the call's name, and the strings its caller's
  worker masked, which each of its job's phases counts as secrets' own
  texts. Version 3 adds them. The coordinator serves them, and the worker
  makes them for a step that calls a workflow. Such a job needs version 3:
  the coordinator refuses it to an earlier worker, and a later worker, in
  an attempt an earlier coordinator serves, fails the step
  (`uses_not_supported`) (Never a silent downgrade, below).
- **Step runs:** ask for the step run of a step's try, or of its
  `cleanup:` (`/v1/steprun`), with the step's `ssh:` as the try
  evaluated it, the state of its job at the try, a result for each step
  before the try's, or every step, for a cleanup's, and the strings the
  worker masks for the step, 10,000 of them, of 1 MiB in all, at most,
  which a repeat answers with the same step run, and one that wasn't made
  ended, with why; wait for its end (`/v1/steprun/end`), as for a child's, with how
  it ended, its step's exit status and, once it succeeded, what its step
  wrote to `OUTPUTS`, 1 MiB of it at most, as a worker reads, which the
  coordinator refuses more of as the step run's attempt ends; and stop it
  (`/v1/steprun/stop`). Each
  names the step run by the step, the try and whether it is the
  cleanup's, of the attempt that asks, so an attempt reaches only its
  own, and never a workflow its step calls ([Steps on another
  host](#steps-on-another-host)). A step run's attempt's claim has the
  try of the step it runs and that state, and its job has that step, the
  steps before it by their `id` alone, and no `outputs:`. Version 3 adds
  them, and such a job needs it, as one that calls a workflow does.
- **Actions:** ask the coordinator to do a step's cancel-jobs,
  update-session or checkout, from version 3
  ([Built-in actions](#built-in-actions)). The request has the step's
  `with:` as Step evaluated it, and its answer says how the action ended:
  success or failure, with its problems. The coordinator reads which
  action it is from the plan, never from the request. It does the action
  once for a step's try, and keeps the answer, with a keyed digest of the
  whole request, an HMAC-SHA256 ([The secret record](#the-secret-record)),
  never the `with:` itself, which can hold a secret, nor a plain digest of
  it: a repeat is given the answer, and another request for the same try
  is fenced. The answers outlive their attempt in the store, which enforces
  no foreign keys, so nothing deletes them with it: whatever comes to
  delete attempts, as a retention of runs, deletes their answers too. A
  step whose `if:` doesn't outlive its job's cancellation, as the
  coordinator reads it from the plan (`plan.OutlivesCancel`), never from
  the worker, of an attempt whose control asks to cancel the job, is
  answered cancelled, without the
  action, as though the worker had read the control first; so is a step
  whose `early-cancel` is `any-job-failed`, once a job of the run has
  failed, with no reason, as that stops it. Another's answer says the
  job's cancel too, as one that cancelled the step's own job, so that the
  worker heeds it before the next step. The worker waits for
  the answer through a stop's grace period, as a command has until
  SIGKILL: the step ends as the answer says, and the stop applies from
  the step after; past it, the step ends as the stop has it, though
  the coordinator may have done the action. Such a job needs version 3,
  as one that calls a workflow does (Child runs, above). The request
  also says what evaluating `with:` read, which update-session's provider
  is given. The worker fails a step that uses an action only a platform
  provides (`action_needs_platform`).
- **Checkout's tree:** once a checkout's action succeeded, the same
  request reads its tree (`/v1/checkout`, of version 3), which the
  coordinator serves only for that request, as a tar stream of the
  commit the step was pinned to, read from the plan: another request for
  the try, and one of a try whose action wasn't answered success, is
  refused as the worker's bug, 400, which fails the step rather than
  halt the attempt. A stream cut short ends without the end of its
  chunked body, and the worker asks again, writing the tree whole again
  over what it wrote of it. The answer may take as long as its most bytes
  take at the server's slowest rate, 1 MiB a second.
- **Complete:** the job's result, as End gave it: its outcome, its outputs
  and its steps', and which of them are private or hold a secret
  (`plan.JobResult`). The same end again, as a worker sends after a
  dropped connection, is answered as the first was, and so is any other
  end of an attempt that completed, which changes nothing: the end stored
  first is the attempt's. The coordinator keeps nothing made from an end
  to tell a repeat from another end, since an end can hold a secret, as
  an output, which a digest of it would let whoever holds the store test
  a guess at ([The secret record](#the-secret-record)); and it needn't,
  since the attempt's worker, which alone holds its credential, sends one
  end. Another coordinator that keeps what tells them apart may fence
  another end instead, as the conformance suite allows. An end of an
  attempt that ended otherwise, as withdrawn, lost or superseded, is
  fenced.

Every write is idempotent and carries a sequence number, so a worker
retrying after a dropped connection can't apply a report twice. A write
from a superseded attempt fails with a `fenced` problem.

The attempt credential is an opaque random token, stored as a hash, that
lasts until the attempt ends, so that a worker cut off from the coordinator
can still deliver what it spooled. For a job that fences itself it lasts
only while heartbeats renew it, and lapses with the lease. It lets its
holder do one thing: report on its own attempt. A step that reads it gains
nothing in that attempt that its job couldn't already do. That holds
where steps and their worker run as the same user only as far as the
user's own files and processes allow: with the `local` backend, a step
is the user, and can read everything the user can, other runs' secrets
among them ([Security](#security)).

The API has a version of its own, apart from the plan's. The plan's
version says what a job means; the API's says what a worker and a
coordinator say to each other.

- **One number.** The API's version is one integer, and every change to
  the API after a release has written its version raises it: a path, a
  member of a body, or a kind of event, added. A version no release has
  written yet stays open, as a plan version does until a release writes
  it: what is added before a release joins the newest version, and the
  release that first writes it freezes it. Version 2 added a failed try's
  event saying whether it was its step's last, and the attempt's control
  in the claim's answer; version 3, the request for a built-in action the
  coordinator performs, with what evaluating its `with:` read. Neither
  side reads a member it doesn't know, so a major and a minor number,
  which tell an addition a reader can skip from one it can't, would say
  nothing more. The paths stay under `/v1/`: a version is served beside
  every version before it, not in place of them.
- **Each side speaks every version up to its own.** A worker says, as it
  claims, the newest version it speaks; a worker of a release from before
  the API had a version says none, and speaks version 1. The coordinator
  serves the attempt at the lower of that and its own, says which in the
  claim's answer, and keeps it with the attempt, and both sides speak
  that version until the attempt ends: a worker newer than the
  coordinator speaks the coordinator's version, and an older one its own.
  A member a later version adds is left out when it is empty, and each
  side sends it only in an attempt whose version has it, so each reads
  the other's bodies strictly, refusing a member it doesn't know. Each
  side sends and reads every body of an attempt through one check, which
  refuses a member later than the attempt's version
  (`workerapi.CheckVersion`), from a table of the version that added each
  member. A path a later version adds, a worker calls only in such an
  attempt, and the coordinator answers it in another as one the API
  doesn't have.
- **Never a silent downgrade.** An attempt needs the oldest version that
  has everything its job uses, and the coordinator refuses a worker whose
  version is older before it sends anything of the run
  (`worker_api_too_old`), rather than run the job without that part. The
  attempt fails before its first step, and is placed again, as for
  `plan_too_new`. A job with a step whose `uses:` names a workflow, or an
  action the coordinator performs (checkout, cancel-jobs or
  update-session), or whose own `ssh:` would make it a step run, as the
  plan says without evaluating it ([Steps on another
  host](#steps-on-another-host)), and every job of a child run, whose
  claim holds what masks what it inherits, needs version 3; any other,
  version 1. A later
  worker, in an attempt an earlier coordinator serves at an earlier
  version, fails such a step (`uses_not_supported`), rather than run it
  otherwise. The same problem fails, as it runs, a step that has no pin:
  a checkout of a run created before checkouts were pinned, which the
  coordinator fails, and a call of a workflow of one created before calls
  were, which the worker fails.
- **What a body carries of a run is the plan's.** The run's values, the
  job Ready evaluated and the results of the jobs it needs are encoded at
  the run's plan version, and a change to one of their types raises the
  plan's version, not the API's
  ([Plans across versions](#plans-across-versions)). A worker says, as it
  claims, the newest plan version it reads too, and the coordinator
  refuses one that doesn't read the run's before it sends anything of the
  run (`plan_too_new`).
- **Replicas.** A replica answers a worker only for a run whose plan
  version it reads, as it claims only such runs, and only for an attempt
  whose API version it speaks; otherwise it asks the worker to try again,
  which reaches another. So it does for a path or a method under `/v1/`
  that it doesn't have, of an attempt claimed at a later version, which
  may have it, where it answers another's as one the API doesn't have.
  What needs neither version it answers first: a credential it doesn't
  take, or that has lapsed, and a heartbeat, events, a log upload or a
  claim of an attempt that no longer holds its job, which is fenced. So a
  worker superseded during a rolling upgrade, or after one is rolled
  back, stops, whichever replicas are left. The claim's request comes
  before the worker knows either version, so it never changes.
- **The schema.** The bodies' JSON Schema is beside each plan version's
  schemas (`plan/schema/<version>/workerapi.schema.json`), since it refers
  to them for what a body carries of a run. It describes the bodies of the
  API's newest version, and says of a member a later version adds which
  version that is, so a body of an earlier version is one without it. Its
  cost is that an addition to the API changes each plan version's copy
  alike. A test gives every member of every body the API version that
  added it, and fails for a member without one, one added after the
  first that isn't left out when empty, or one added to the claim's
  request.

What the API means is pinned by a suite, `workerapitest.Conformance`, that
plays a worker against a coordinator over its Unix socket: the claim and
its credential, heartbeats, control polls, events, logs, actions and the
attempt's end, with each refusal, fencing, strict decoding, and unknown paths and
methods. Every coordinator passes it, Meridian's and a platform's, and so
does `workerapitest.Fake`, the coordinator the worker's own tests run
against, so neither side can drift from the other.

### Connections

Workers only connect out, and nothing listens on a host where jobs run.

| Backend | The worker reaches the coordinator through |
| --- | --- |
| `local` | A Unix socket in a directory only the user can open. |
| `ssh` | A Unix socket on the host that the coordinator forwards over its SSH connection, so no port opens on either side. |
| `slurm`, `pbs` | TLS 1.3 to the coordinator, through `meridian relay` on the login node, checked against its key's hash. |
| `kubernetes` | TLS 1.3 to the coordinator's address, checked the same way. |

A coordinator outside a cluster drives Slurm or PBS over SSH to the login
node. There it starts `meridian relay`, the same binary, which accepts
workers' connections and forwards them to the coordinator over SSH, still
encrypted: TLS ends at the coordinator, not at the relay, and the
coordinator's replicas share one key, so the hash a worker is started with
holds whichever replica it reaches. Compute nodes reach the login node but
not the coordinator's machine, and the coordinator's machine opens no port.

The coordinator connects to a host over SSH as the user would with `ssh`
(`backend/ssh/internal/sshconn`), which the `ssh` backend and the login
nodes' backends build on:

- **What a workflow names is checked first.** A format 1 `ssh:`'s
  `remoteHost`, `remoteUser`, `jumpNodeHost` and `jumpNodeUser` usually
  come from its inputs, and reach `ssh -G`, `ssh-keygen` and the user's
  `ProxyCommand`, which a shell runs with `%h`, `%n` and `%r` in it. So
  before any of them runs, Meridian refuses a host or a user that could
  be read as more than a name (`ssh_target_unsafe`), by the rules
  OpenSSH 10.1 and later checks its own command line with
  (`valid_hostname`, `valid_ruser`), whichever `ssh` is installed: an
  older one doesn't check, or, before 10.1, takes a user's control
  characters, so a newline in one starts a line of `ssh -G`'s output of
  its own. The message shows the value with its control characters
  escaped. `ssh -G` is given the host, its port and its user, once they
  are checked, never a jump host, which Meridian connects through itself,
  resolving it with an `ssh -G` of its own, for its own user; a hop of
  the config's `ProxyJump` whose host or user wouldn't pass the same check
  is `ssh_proxy_jump_invalid`. A host that names a user (`user@host`), is
  a URL (`ssh://…`) or a list of hosts (`a,b`), which `ssh`'s command
  line takes but a format 1 `ssh:` doesn't mean, is
  `ssh_host_form_unsupported`, rather than a host `ssh -G` would resolve
  as another: none of the known workflows writes one.
- **The commands Meridian runs to connect**, `ssh`, `ssh-keygen` and the
  shell a `ProxyCommand` runs in, are checked as the local backend checks
  a command it runs: a regular file of the user's or root's that no other
  user can change, nor any directory on the way to it, since a directory
  another user can change can come first on `PATH`. One that isn't safe
  doesn't run (`ssh_command_unsafe`).
- **The user's ssh_config**, as `ssh -G` resolves it, with `Match`,
  `Include` and its defaults: the host's `HostName`, `Port`, `User`,
  `IdentityFile`, `IdentitiesOnly`, `IdentityAgent`, `ProxyJump` (any
  chain of hosts, each one's key checked) and `ProxyCommand` (run as
  OpenSSH runs it, `$SHELL -c "exec <command>"`, or `/bin/sh` without
  `SHELL`, its `%h %p %r %n %u %d` expanded), its known_hosts files,
  `HostKeyAlias` and `RevokedHostKeys`, `ConnectTimeout` and
  `ServerAliveInterval`. A format 1 `ssh:`'s `remoteUser` is over its
  `User`, as `ssh -l` is, so a `Match user`, and `%r`, which `ssh`
  expands in `UserKnownHostsFile` and Meridian in `IdentityFile`, see
  `remoteUser`, as they would with `ssh`; and its `jumpNodeHost` is over
  the config's `ProxyJump` and `ProxyCommand`, as `ssh -J` is. A
  `remoteHost` of the form `host:port` is where to connect, as format 1
  takes it, without the config's `HostName`, `ProxyJump` or
  `ProxyCommand`. A config `ssh` can't read is `ssh_config_unreadable`,
  and output of `ssh -G`'s that prints a keyword Meridian reads more than
  once, but `IdentityFile`, or for another host or port than it asked
  for, isn't trusted (`ssh_config_output_unexpected`); a `ProxyCommand`
  with another token isn't run (`ssh_proxy_command_token`), and
  `ProxyJump`s nested more than 8 deep, as those that lead back to
  themselves are, aren't followed (`ssh_proxy_jump_too_deep`). Without an
  `ssh` client, OpenSSH's defaults apply, with a note
  (`ssh_config_not_read`), but only where there is no config for one to
  read, neither the user's nor the system's: what one says can't be
  ignored (`ssh_client_missing`).
- **Logging in**, with the keys of the user's agent, then the key files
  the config names, then the user's other key pairs in `~/.ssh`, a
  private key beside its `.pub`, as format 1 finds them, unless
  `IdentitiesOnly`. Meridian never asks for a passphrase, so a key file
  that needs one counts only if the agent holds its key, and never logs
  in with a password. A host that accepts none is `ssh_auth_failed`,
  which names no key.
- **One connection to each host**, which every use of it shares: a
  keepalive every 15 seconds closes one whose host doesn't answer within
  10, as a network that silently drops a connection leaves it, long
  before TCP would notice. It is dialed again after a backoff, from a
  second, doubling, up to 30, and at most 8 sessions run on it at once,
  under sshd's default `MaxSessions`, 10. A session sshd refuses is
  asked for again, since a refusal says nothing of whether the
  connection is alive.

One replica at a time holds what a member's workers connect through, such
as the forwarded socket or the relay, under a lease in the store, and
serves every attempt that reaches it there, of whichever job: any replica
can serve any job. It closes the connection only when it gives the lease
up: as it stops, or once no attempt has needed the member for five
minutes, neither one that hasn't ended nor a placement, so that a
long-lived coordinator doesn't keep one on every member it ever served,
while a member between one job and the next keeps it, and so does a
placement on it, however long it takes. The next attempt on the member
takes the lease again, as idle: no worker of the member's can have waited
for it, so it starts no grace, unless another process placed an attempt
on the member meanwhile, and lapsed leases go on counting however often
members come and go ([Leases and fencing](#leases-and-fencing)). Taking a
lease at a start or a takeover still starts one. A host's pooled SSH
connection isn't let go yet. A replica that takes the lease over,
once its holder has stopped renewing it, sets the connection up again at
the same address, so the
member's running workers reconnect to it. Locally, the socket is held
under a lock on a file beside the store's database,
which the system releases when its holder exits, however it exits, and
the store records which process holds it and since when, from which the
grace after a takeover is counted ([Leases and fencing](#leases-and-fencing)).

On a Unix socket, the coordinator asks the system who each connection is
from before it reads a request, as the worker asks who serves its socket
([Running steps](#running-steps)), and serves only its own user, or root.
A platform's coordinator that serves several users from one account maps
the connection's user to the one it acts for, and serves each only their
own runs.

### Running steps

- **The run directory.** Format 1 gives each run a directory (`PW_JOB_DIR`),
  which is also where steps run by default. Jobs of a run on the same host,
  or on a shared filesystem, see the same directory. Containers mount it at
  the same path, so a path one job writes is valid in the next. Jobs on
  members that share no filesystem see different directories, and Meridian
  doesn't copy files between them.
- **The environment.** Steps get the environment variables format 1 defines,
  such as `OUTPUTS`, `PW_JOB_DIR` and `PW_JOB_ID`. Variables that only a
  platform can fill, such as an API key, are set only when one supplies them,
  in the claim (`workerapi.Claim.Env`), which can mark one secret, to be
  masked however short, and which the phases count as a secret's own text,
  as they count the run's (`plan.JobState.Secrets`): an output that holds
  one is a secret to the jobs that need the job. They go over the environment the workflow gives the step, as in
  format 1, and the worker's own over them. Without a platform, `PW_JOB_ID`
  is the run's ID and `PW_PARENT_JOB_DIR` its directory; a child run's are
  its root's ([Workflows a step calls](#workflows-a-step-calls)).
- **The shell** is bash, or sh, as `shell:` says: format 1 runs no other.
- **The env context on the worker.** As in format 1, `env` reads the
  workflow's `env:`, the job's over it, the step's over that, and the
  variables the worker defines over all, such as `PW_PARENT_JOB_DIR` and
  `PW_WORKFLOW_STEP_CURRENT_RETRY` (`plan.JobState.Env`). A value of a job's
  or a step's `env:` that reads others of it reads their values, evaluated
  in the order they read each other, as in format 1; one that reads
  itself, and values that read each other, read what is under the `env:`
  ([Bug fixes to format 1](#bug-fixes-to-format-1)). The job's `env:` is
  evaluated once, as the job starts (`plan.ReadyJob.JobEnv`), and its steps
  and outputs read what it gave (`plan.JobState.JobEnv`); it and the job's
  `working-directory:` read only the variables format 1 defines then, not
  `OUTPUTS` or the retry counters, so they are the same for each step. A credential, such as the platform's API
  key that format 1 gives a step as `PW_API_KEY`, is in the step's
  environment but never in the `env` context: the plan reads only the
  variables format 1 defines (`plan.IsFormatVariable`), whatever the worker
  passes, and none of the workflows we know of reads one in an expression.
  A job's `working-directory:` is evaluated on the worker, where the run
  directory is known: 24 jobs in 4 known workflows read
  `env.PW_PARENT_JOB_DIR` in it. The compiler
  warns about `env.<name>` that nothing defines where it is read.
- **Each try is evaluated.** A step is evaluated again for each try of its
  `retry:`, so that what reads `env.PW_WORKFLOW_STEP_CURRENT_RETRY` reads the
  try's, as format 1's failover does: 8 known workflows read it in a retried
  step's `with:`. Its `if:` and `retry:` are those of its first try: a
  later try runs whatever its `if:` would give now, and carries over
  whether it outlives a cancel and reads outputs as its first did. How
  many more times a step may run, `PW_WORKFLOW_STEP_MAX_RETRIES`, is known
  only once Step has read its `retry:`, so the worker evaluates the first
  try of a step with one again, with it, as format 1 gives it from the
  first. A try that can't start, as one whose evaluation fails, fails its
  step, which isn't tried again.
- **Outputs that nothing wrote.** As in format 1, a job's output or a
  step's that nothing wrote fails the expression that reads it
  (`output_not_written`) where a job or a step is about to run, and in a
  job's `outputs:` once its steps succeeded, so that a command such as
  `rm -rf ${{ needs.prep.outputs.dir }}/*` never runs with an empty path;
  so does a step's output read by an `id` that none of the job's steps
  has, as `steps.<id>.outputs.<name>` there or in
  `needs.<job>.steps.<id>.outputs.<name>`. An
  `if:` reads one as `""`, and so do a step whose `if:` is the word
  `always`, a job's own values and its strategy when its `if:` is, a
  step's `cleanup:`, and a job's `outputs:` once a step of it failed or was
  cancelled. A step with an `id` writes outputs of its own, which
  `steps.<id>` reads; the job's outputs are those its steps without one
  write, then those its cleanups write, with its `outputs:` over them.
- **A host of a step's own.** A format 1 `ssh:` on a job runs its steps on
  the host it names, and a step's runs it on its own: `ssh: null` runs the
  step where the job runs without its `ssh:`, and an empty or null
  `remoteHost` runs it there too, with a note in its log, as format 1 does
  (`ssh_host_empty`). A step whose `ssh:` names a host of its own is to
  run there as a run of its own, and so is such a step in a job on a host
  ([Steps on another host](#steps-on-another-host)); until that is built, a
  worker on the host a job's `ssh:` names fails such a step
  (`step_off_job_host`) rather than run it on the wrong host, and any
  worker fails one whose `ssh:` names a host of its own
  (`step_ssh_not_supported`), but for one whose `ssh:` names the same host
  and login as its job's, as a step that repeats its job's `remoteHost`
  does, which runs where its job runs: the same `remoteHost`,
  `remoteUser`, `jumpNodeHost` and `jumpNodeUser`, by which format 1 keys
  the connections it reuses, with
  null and none as `""`, whatever else it says, such as its
  `disconnect-timeout`. `host:22` isn't `host`, as format 1 connects to
  `host:port` as it is and reads `host` through the user's `ssh_config`.
  All 536 jobs' `ssh:` mappings in the known workflows read `remoteHost`
  from an input, and 29 of the 37 steps' do.
- **A step that outlives its job's cancelling.** A step whose `if:` can be
  true once its job is stopping, the word `always`, `always()`,
  `cancelled()`, `!success()` or `!failure()`, runs even once the job is
  being cancelled or has run past its timeout, and runs its course, as in
  format 1 (`plan.ReadyStep.OutlivesCancel`).
- **A working directory is used as it is.** One that keeps a shell
  variable, such as `$HOME`, fails the step, as in format 1, since no shell
  expands it.
- **Containers.** On a member that runs containers, the worker runs on the
  host, outside the container. It creates the job's container, runs each
  step in it with the runtime's `exec`, and removes the container when the
  job ends.
- **A step ends when its output closes**, as in format 1, not when its
  shell exits: what it starts in the background and leaves writing to its
  output is still the step's. It runs in a process group of its own, which
  is what SIGTERM and SIGKILL stop. The group is sent SIGTERM once,
  however many things stop it at once, as a halt stops a step both as a
  step and as one of the attempt's groups: a second SIGTERM would run a
  step's trap again, and stop what the first runs, as format 1's single
  SIGTERM doesn't.
- **Logs** are masked on the worker, before they leave the host, and so are
  outputs ([Outputs](#outputs)). Once an upload has sent all a log had,
  the worker starts the next no sooner than 100 ms after that one
  started, but sends a log's end at once, and more of a log at once
  after an upload as large as one can be: a step that prints a line at a
  time has its lines sent together, rather than an upload, and the
  coordinator's work for it, for nearly each line.
- **While the coordinator is unreachable**, a worker keeps running its
  steps, spools logs to disk up to a limit, keeps events in memory, and
  delivers them in order when it reconnects, unless its job fences itself
  ([Leases and fencing](#leases-and-fencing)). Each try of a log's upload
  sends what the log has by then, up to an upload's 1 MiB, and its end
  once it has ended, not what the first try had; and each try of a report
  of events sends the events queued by then, up to a request's 64 events
  or 4 MiB of their outputs, which the coordinator stores but for those
  it has. So once the coordinator is back, one upload per log sends what
  that log has, and one report what the worker has of its events, as a
  job that fences itself needs before its lease lapses, rather than the
  rest, and a log's end, a round trip later. Past the limit, or once the
  disk is full, a log keeps nothing more until there is room, and the
  step's event says how much it lost. A log's file holds what the
  coordinator hasn't stored, and of what it has, less than the limit, and
  at most four times as much, or 16 KiB, but while the worker copies what
  is unsent to a new file, when it holds both.
- **What the worker keeps** of its own, its spooled logs, its steps' scripts
  and `$OUTPUTS` files, is in a directory only its user can use, which it
  holds open and makes every file in through that, whose name starts with a
  dot, so that `rm -rf "$TMPDIR"/*` passes it by. It is outside the run's
  directory, as format 1 keeps its scripts and `$OUTPUTS` outside
  `PW_JOB_DIR`, since a step can remove that, as `cleanup: rm -r
  ${PW_JOB_DIR}` does in workflows we know of, and on NFS a directory can't
  be removed while a file in it is held open. It is in a directory no other
  user can change, on a path no other user can change (as OpenSSH checks the
  way to a user's keys), so that no other user can take its name once it is
  removed: `meridian/worker` in the user's cache directory, on disk, which
  the worker makes; otherwise `XDG_RUNTIME_DIR`, when it is set and no other
  user can change it, which is in memory, so the worker spools at most 64
  MiB while its directory is there; and only otherwise the system's
  temporary directory, with a warning in the worker's log. Any process of
  the user, the job's steps among them, or a cleaner of temporary files, can
  still remove it. The worker then never trusts what is at its path again:
  it makes a new directory, with a new name, which it checks is a directory
  of its user's that no one else can use, in the same parent, which it makes
  and checks again if it made it, or else in the next it would have used;
  and a step whose `$OUTPUTS` went as it ran has its event say so
  (`outputs_removed`), as what it wrote there after is lost. A problem the
  worker has with its own files fails a step, never the attempt, and one
  with the directory at first ends the worker before it claims the attempt.
- **When it is stopped from outside**, as by its scheduler, it stops the
  job's steps, and sends what it has for a while once they have stopped,
  so that a step in its grace period keeps what it printed last: the
  attempt's end too, if the steps had ended.
- **It talks only to its coordinator.** Over a Unix socket, it asks the
  system which user serves the socket, and sends a token only to its own
  user's, or root's: `SO_PEERCRED` says on Linux, and `LOCAL_PEERCRED` on
  macOS and FreeBSD. Over TLS, it checks the coordinator's key. It follows
  no redirect.
- **It keeps its credential out of reach.** First of all, before it reads
  its bootstrap token, it keeps the user's other processes, the job's steps
  among them, from reading its memory, or tracing what it sends: on Linux
  it makes itself not dumpable, which also keeps them from reading its
  environment in `/proc`; on FreeBSD it turns tracing of itself off
  (`procctl`), core dumps and its environment included; and on macOS it
  refuses debuggers (`PT_DENY_ATTACH`), where reading another process's
  memory any other way takes root or a debugger the user has authorised,
  and sets the size of its core file to 0, as macOS's default is, which
  the job's steps start with too, and can raise. macOS still shows the
  worker's environment to the user's other processes, so the worker there
  takes no token from it. Its credential, which it makes, is never in its
  environment or arguments. On any other system the worker doesn't
  run (`worker_system_unsupported`), rather than run a job with less:
  NetBSD and OpenBSD say who serves a socket, but give a process no way to
  keep the user's other processes from tracing it, and OpenBSD's `ktrace`
  records what a process it traces writes, whatever `kern.global_ptrace`
  says.

Today the worker runs steps only on the host. A job whose steps are to run
in a container fails before its first step, with a problem that says so,
rather than run them on the host, and a step that `uses:` a built-in
action the worker doesn't run, or runs on another host with `ssh:`, fails,
as does one whose `ssh:` is null, or gives no `remoteHost`, on a worker
that a job's `ssh:` placed on a host, until steps on another host are
built ([Steps on another host](#steps-on-another-host)); one that uses an
action only a platform provides fails saying so (`action_needs_platform`, [Built-in
actions](#built-in-actions)). A step that `uses:` a workflow runs it as a
run of its own ([Workflows a step calls](#workflows-a-step-calls)), whose
directory its `clean-on-retry:` cleans on the machine the run started
on, never on a host. Its events are kept in memory
while the coordinator is unreachable, since each is small but for a step's
outputs, which are capped.

## Backends and isolation

| Backend | Starts a worker | Default isolation |
| --- | --- | --- |
| `local` | As a process detached from the terminal | A rootless container |
| `ssh` | Over SSH, detached so it outlives the connection: in a session of its own, or, where logind ends a session's processes with it and the user lingers, in a scope of their service manager (`systemd-run --user --scope`) | A rootless container |
| `kubernetes` | As a pod, through the Kubernetes API | A rootless container: the pod, run as a non-root user |
| `slurm`, `pbs` | As a batch job, submitted on the login node | The host |

A member can choose stronger isolation, such as a microVM, or name host
isolation explicitly. It never gets weaker isolation than it asked for: when
a member's runtime is missing, jobs fail there with an error that says so.
Until Meridian drives container runtimes, a member that doesn't name host
isolation, as the built-in `local` group's does, runs no job: a job that no
other member can take fails, with that member's reason among the rest,
rather than run on the host.

Today the `local` backend (`backend/local`) and the `ssh` backend
(`backend/ssh`), which runs the local backend on its host through
`meridian host` ([The `ssh` backend](#the-ssh-backend)), are built, and
both run a member's jobs on the host only. A member of either names
`isolation: host`, as the built-in `local` group's member does, and as the
member a format 1 `ssh:` makes does. One that names a runtime, or none,
which is a rootless container, has `isolation_not_built`, so placement
leaves it out rather than run its jobs on the host. A member of another
backend has `backend_not_built` (`backend.NotBuilt`).

The `local` backend starts `meridian worker` from the binary that places
it, once it has checked that no other user can change that binary, or the
way to it (`worker_binary_unsafe`). The worker starts in `/`, in a session
of its own, so that the terminal's Ctrl-C and SIGHUP don't reach it, with
the user's environment but its `MERIDIAN_` variables and those that the
user's secret variables are read from (`env:`), which the worker gives
its steps: a job reads a secret only as its run gives it, masked in its
logs, and a step that printed its environment would show the others in
the clear. A plain variable's environment variable stays, since its value
isn't a secret, and steps may read it. It gets its
bootstrap token on a socket it inherits, once it has said there that it
is hardened (`workerapi.EnvBootstrapTokenFD`), and the token is in no
file. One window is known, and left: from `exec` until the worker hardens
itself, which it does as soon as Go's runtime has started, about 300 µs
on Linux, the new process is dumpable, so a process of the same user that
already knows its ID, where `kernel.yama.ptrace_scope` is 1 or less, could
copy its descriptor 3 with `pidfd_getfd` and race it for the token. A file
was readable for the whole of the worker's start. Closing the window would
need the child made non-dumpable before `exec`, which takes cgo, and
Meridian builds without it. On Linux the backend starts the file it runs itself, and reports
that file's digest, which the worker reports too, even if an upgrade has
taken its path since. It checks the file, and the way to it, at every
placement. On Linux, where that file is the one the process runs, which
nothing can write while it runs, it hashes it again only once it has
changed: the digest is kept by the file's device, inode, size and the times
its data and its status last changed, as `fstat` gives them, and only once
its status last changed a second before. A process can set back the time a
file's data changed, but not its status's, which any change sets to the
time it happens. Elsewhere it hashes the file at every placement, since a
file system's times can be as coarse as two seconds, as FAT's are. A placement's file in the state directory says
which process the worker is, by its ID, its start time and the boot it
started in, which no change to the clock moves (Linux's
`/proc/<pid>/stat` and `boot_id`, macOS's `p_starttime` and
`kern.bootsessionuuid`, FreeBSD's `ki_start` less `kern.boottime`, and
`kern.boot_id`), so that any process of the user's finds, checks and
cancels it, and none takes a later process with the same ID for it. One
process at a time places an attempt. Cancel sends SIGTERM, and SIGKILL if
the worker still runs 60 seconds later, to that process only, through a
pidfd on Linux; a worker killed so can't stop its steps' process groups.
The coordinator has the backend forget a placement once it has recorded
how its attempt ended, and its worker has exited, which it does soon after
(`Driver.Forget`): the placement's files and the worker's log go. A
placement no coordinator had the backend forget, as when the one that
placed it stopped first, the next process that places a worker removes
once its worker ended more than a day before. Which placements' workers
may still run, and asked for what, the backend reads from `live/`, a file
for each, not from every placement kept. A member offers what it declares, or else
the host's CPUs and memory, and no GPU.

On another host, the `ssh` backend runs the same backend through `meridian
host <operation>`, a hidden command of the same binary that does one of
the Driver's calls, or `hello`, which says the host's system, its user's
home, its machine ID, how it starts a worker detached, the binary it runs
with its digest, and its directories. Its protocol is JSON lines on its
standard input and output, versioned (`backend/hostproto`): it hardens
itself, as `meridian run` does, says it is ready, and only then reads the
request, which for `place` carries the bootstrap token, so the token
reaches only a hardened process, and never its arguments or environment.
The coordinator runs it without the session's `GODEBUG`, as it runs a
worker, and is given none of a member's settings, which name the host and
its logins. One that stops before it answers, saying why on its standard
error, as `meridian` says a problem before it runs a command, as a binary
out of FIPS 140-only mode does, is `ssh_host_helper_failed`.
`place` returns once the worker has taken its token, and a worker that
ends first, as one `systemd-run` couldn't start does, or doesn't ask for
it within a minute, is stopped, and isn't placed
(`worker_token_not_taken`). On the host:
- **Its state** is the state directory's
  `hosts/<machine ID>-<host name>/`, the host's own where a cluster's
  nodes share a home, so that no node takes another's live workers for
  dead by their boot. The machine ID alone would do but where nodes were
  made from one image that held it, as stateless or cloned nodes can be;
  their names tell them apart. A host whose name changes starts afresh,
  without the workers it placed before. On Linux the ID is systemd's, or
  D-Bus's where systemd's file is empty or says `uninitialized`; a host
  without one, or without a name, is `ssh_host_id_unknown`.
- **Its directories**, the state's, the binaries' cache (the cache
  directory's `bin/`) and the runs', are made the user's own, which only
  they can use, and checked as Meridian's own are (`ssh_host_dir_unusable`,
  which names the directory that failed, the one asked for or one on the
  way to it).
- **The sockets' directory**, where the coordinator listens for the host's
  workers, is the runtime directory's `meridian/`, where `XDG_RUNTIME_DIR`
  names one that is only the user's, and short enough for a socket's path
  in it (`hostproto.MaxSocketsDir`); and otherwise one of a random name
  that `meridian host` makes in `/tmp`, and records in its state, so that
  every `hello` gives the same one while it is the user's own. A name in
  `/tmp` that another user could know, as `/tmp/meridian-<uid>`, is one
  they could make first, and keep the user out of.
- **A run's directory** is
  `${XDG_STATE_HOME:-$HOME/.local/state}/meridian/runs/<run>-<store>`, on
  every system, outside `hosts/`, or the `runs/` of the state directory
  that `MERIDIAN_STATE_DIR` names. On macOS too: Meridian's own state
  directory there, `~/Library/Application Support/Meridian`, holds a
  space, which `make`, autotools and an unquoted `$PWD` break on, as a
  run's steps would in it. On a cluster it is in the home its nodes
  share, so the compute nodes that a step's `sbatch`, `qsub` or `srun`
  sends work to see it, and so do the cluster's other login nodes, which a
  reconnect to the same host name can reach. The temporary directory, where a run's directory is on the
  coordinator's own host, is each node's own. `place` chooses it, and
  returns it, and `find` too, as the local backend records it with the
  placement; the coordinator records it with the attempt, sealed where
  the placement is private, and gives it to the worker as it claims the
  attempt, in place of the run's directory on the coordinator's machine.
- **A worker starts detached**: in a session of its own, unless logind runs,
  as its directory of seats says, and ends a session's processes as it
  ends (`KillUserProcesses=yes`, as `loginctl` says). Then it starts in a
  scope of the user's service manager (`systemd-run --user --scope`, in a
  scope whose name is new each time), which keeps the session's
  environment, as a service of the manager wouldn't, if the user lingers;
  and otherwise not at all (`ssh_worker_would_be_killed`, whose page says
  `loginctl enable-linger`), rather than to end with the session. A
  `systemd-run` older than systemd 236, whose scopes don't take
  `--collect`, which lets a scope that failed go, is
  `ssh_systemd_too_old`. Only logind's absence means a session of its
  own: where logind runs, but the `PATH` finds no `loginctl`, the host is
  `ssh_loginctl_missing`, and a `loginctl` that fails otherwise, as one
  that can't reach logind over D-Bus or times out, is an error, which the
  coordinator tries again, but for `show-user`'s failure for a user who
  neither lingers nor is logged in, which says they don't linger. It
  reads logind's setting, not whether `meridian host`'s own session is
  one of logind's, so where sshd doesn't register its sessions with logind
  (`UsePAM no`) it decides as if it did, at the cost of a scope or a
  refusal the host didn't need.
- **The commands it runs**, `loginctl`, `systemd-run` and, on macOS,
  `ioreg`, are found on the `PATH` and run by their absolute paths, once
  they are checked as the worker's binary is: a regular file of the
  user's or root's that no other user can change, nor any directory on the
  way to it. `systemd-run` gets what the worker is given, the socket its
  token comes on among it, and a directory another user can change can
  come first on a `PATH`, as a shared `bin/` a cluster's startup files add
  can; one that isn't safe is `ssh_host_command_unsafe`. The check is
  `safepath.CheckExecutable`, which the `ssh` backend's own commands take
  too. The token's socket so passes through `systemd-run`, which execs
  the worker, before the worker has hardened itself; the token is written
  only once the worker says it is ready, which it says only once
  hardened, so what could read it before is `systemd-run`, which is the
  system's or the user's, as checked. Another process of the same user
  that may trace theirs, as where Yama's `ptrace_scope` is 0, could copy
  the socket out of `systemd-run`, or out of the worker before it hardens,
  and race the worker for the token, as it could with the `local`
  backend's worker.
- **Its environment** is the session's, without `GODEBUG`, which could turn
  off what the worker requires of its cryptography.

A worker passes its steps nothing it inherited but its standard input,
output and error: it marks every other descriptor close-on-exec as it
starts, and a signal it was started ignoring, as `nohup` ignores SIGHUP,
is one its steps get the default handling of.

### Container runtimes

Meridian drives the container runtimes a host already has, through their
command-line tools:

- **Podman**, rootless.
- **Docker**, rootless. A rootful Docker daemon is refused.
- **Apptainer** and **SingularityCE**, which HPC sites often provide.
  Meridian runs them with `--containall`, so a job sees neither the user's
  home directory nor their environment, and gets its own PID and IPC
  namespaces. They share the host's network.
- **Apple's `container`** on macOS, which runs each container in its own
  lightweight VM.

A member names its runtime. One that doesn't uses rootless Podman, or
rootless Docker if the host has no Podman. Apptainer and SingularityCE
isolate less, so they run a member's jobs only when the member names them;
that is how a Slurm or PBS member runs jobs that name an image. MicroVMs on
Linux come from Kata Containers with Cloud Hypervisor, set as the runtime
under Podman, or as a Kubernetes runtime class.

### `ssh:` is deprecated

A format 1 job or step can name a host with `ssh:`. That ties the workflow
to one host, while `runs-on: { execution-group: … }` names capacity that
each user or platform defines for itself, with as many members as it has to
fall back between. So `ssh:` is deprecated in favor of execution groups:

- The schema marks it `deprecated`, and the compiler warns, naming the
  execution group that would replace it. Neither does yet.
- It keeps working in format 1 as it always has: its commands run on that
  host, outside any container, because existing workflows rely on the
  host's files and tools. To Meridian it is an inline execution group with
  the `ssh` backend and host isolation.
- A later format number can remove it.

### The `ssh` backend

A format 1 `ssh:` makes a group of one member of the `ssh` backend, and a
configured group can name hosts too. The coordinator connects to a host
as the user's `ssh` would ([Connections](#connections)), and runs the
local backend there through `meridian host`
([Backends and isolation](#backends-and-isolation)). Besides those:

- **The binary on the host.** A host of the coordinator's system and
  architecture runs the coordinator's own binary. It is copied over the
  connection, once, into the host's cache of binaries by its digest,
  checked against it there before it is kept, and run from there, so a
  cluster whose nodes share a home gets it once. A host of another system
  or architecture runs the same release's binary for it, which the
  coordinator's machine holds only once the user has asked `meridian` to
  fetch it, checked against the release's checksums; Meridian fetches
  nothing by itself, and without it a job there fails, saying how
  (`ssh_worker_binary_missing`). A host where the worker doesn't run, as
  one that isn't Linux, macOS or FreeBSD, is `ssh_host_unsupported`.
- **What runs there.** Every command Meridian runs on a host is
  `/bin/sh -s`, which any login shell, csh's among them, passes to `sh`
  as it is, with its script on its standard input, values in it quoted
  for `sh`. What a script says is a line of JSON, after a random nonce
  the script prints first, so that nothing a login script prints is
  taken for it, and what it reads after its script,
  as the binary being copied, or `meridian host`'s request, is sent once
  it has said it is ready for it: a `sh`, as dash's and BusyBox's, can
  read ahead of the command it runs.
- **Reaching the coordinator.** One replica at a time holds a host's
  connection, under a lease in the store named by a keyed hash of the
  host and its login, so the store never holds in the clear a host that
  came from a variable or a password; locally, the lease is a lock on a
  file beside the database, as the coordinator's socket's is, and the
  hash is keyed by a key derived from the state directory's key file. It
  asks the host to listen on a socket in the host's sockets directory,
  named by the same hash, and serves the worker API on what the host
  forwards, to the attempts placed on that member only, since a forwarded
  connection has no user the system can name: the store records the hash
  with each attempt placed on a host, and a request on a member's socket
  for an attempt of another member, or a request on the coordinator's own
  socket for an attempt on a host, is refused as one with no credential
  is. A connection forwarded over SSH takes no deadlines, so each is
  passed through one that does, and the server's bounds on a request's
  time hold on it too. It first removes a socket that an earlier holder
  left at the path, over which sshd's default, `StreamLocalBindUnlink no`,
  won't listen. A host whose sshd forbids forwarding Unix sockets takes
  no workers (`ssh_forwarding_refused`); Meridian opens no port instead.
  Each `meridian run` or `resume` of a store holds the hosts' sockets
  that no other holds, as the attempts it places need them, and as the
  store's attempts that haven't ended do, which it looks for every few
  seconds; one that another holds, it waits for, and takes over once that
  one stops. An attempt whose member it can't find, as when its group
  can't be read, it looks for again after a wait that doubles, from 10
  seconds to 5 minutes. A process holds a host's socket, its lease and
  its connection until it stops, which `run` and `resume` do once their
  runs have ended; a coordinator that outlives its runs, as a platform's
  does, would let a host's go once no attempt placed there is left, which
  isn't built yet. A host's connection is where the holder's last dial
  reached: where a host's name leads to several nodes, the workers of a
  node it no longer reaches can't reach the coordinator until it does.
- **Which host.** A connection made again can reach another host than the
  last did, as a cluster's login nodes behind one name do, each with
  placements of its own. So a placement's ID begins with a keyed hash of
  the host it was made on, its machine ID and its name, as `hello` says
  them, and it is checked, cancelled or forgotten only on that host: a
  check of it over a connection to another is unknown
  (`ssh_placement_elsewhere`), for five minutes at most, as for any
  problem a check gives ([Leases and fencing](#leases-and-fencing)), since
  the node it is on may be gone, or out of the name's rotation, and a
  cancel waits. Nor is another host's having no placement of
  an attempt taken for the attempt's having none, where the coordinator
  has placed or found it on another since it started.
- **Outages.** A connection that drops is dialed again, and the socket
  forwarded again at the same path, so the host's workers reconnect
  without being told; meanwhile they run their steps and hold their
  reports, as any worker cut off does. A check of a placement over a
  connection that is down, stalls, or whose sessions sshd refuses is
  unknown, so a job on a host is never lost to an outage, and a cancel
  waits for the connection too. A check that can't be made for a cause
  that won't pass by itself, as a host key that known_hosts doesn't have
  or that has changed, a `meridian host` that stops before it answers
  (`ssh_host_helper_failed`), or one of another release
  (`ssh_host_release_mismatch`), is unknown for five minutes at most. A replica that
  takes a host's connection over, as `meridian resume` does once the
  process that held it has stopped, finds the attempts placed on it in
  the store, by the hash they were recorded with, readies their jobs
  again for the members' settings, which the store doesn't hold, and
  forwards their sockets again; the grace after a takeover keeps their
  leases from counting as lapsed meanwhile
  ([Leases and fencing](#leases-and-fencing)).
- **A host that can't be reached** as a job is placed is tried four times,
  as format 1 tries it, each try bounded by the host's `ConnectTimeout`,
  or 30 seconds, as in format 1. Format 1 waits 2, 4 and 8 seconds
  between the tries; Meridian waits about 1, 2 and 4, as it does before
  it dials any host again that it failed to reach. Then its member can't
  run the job (`ssh_unreachable`), and the job fails
  (`placement_impossible`), as format 1's step does. A job counts the
  failures it sees: its own tries, and those made for the jobs waiting on
  the host with it, since its first look, so that every job waiting on a
  host that doesn't answer gives up after the four failures it has seen,
  together, as format 1's steps do, rather than one after another. The
  tries are spread over the looks of the jobs' supervisors, never made in
  one: a host that doesn't answer must not hold the workers that every
  run's supervisors share. A probe makes one try at most, and waits 5
  seconds at most for it, the try going on without it; while the host is
  being tried, or before the wait after a try that failed has passed, a
  probe, or a check, answers at once that the host hasn't been tried for
  the job, with how many tries of it have failed in all
  (`backend.FailedError`), and the supervisor asks again when it will be
  tried, counting the failures since its previous look
  (`backend.Demand.Tries`). A job's first look counts none but its own, so
  a job that comes to a host just after other jobs' tries gave up on it
  tries it again, and runs there if it is back. The coordinator keeps
  what each job's latest look saw in memory, until the job is placed,
  placing it fails, or it ends; one that restarts counts from the next
  look. A look that counts many failures at once, as one whose supervisor
  stalled, takes the job no further than one short of the coordinator's
  limit on looks that couldn't answer, which is more than four, so that
  the job fails with `ssh_unreachable`, not `placement_unanswered`. A
  dial is bounded in all: `ssh -G` by twice the connect timeout, and the
  whole by twice the host's connect timeout for it and each hop there can
  be, 5 minutes at least; one that reaches it has failed. A cancel the
  driver can't make for now is made again in the background.
- **Format 1's keys.** `remoteUser` and `jumpNodeUser` are the user's
  config's `User` for the host where they're empty, or else the local
  user; format 1's "your pw username" is the local user here. A
  `remoteHost` of the form `host:port` is where to connect, as format 1
  takes it, and `jumpNodeHost` is a host to connect through, its key
  checked against its own entries in known_hosts. `remoteHost:
  localhost` is SSH to this machine, like any host's, which needs an sshd
  here, as ACTIVATE's workspace has one.
- **`disconnect-timeout`**, how long format 1 lets a step's connection be
  lost before it fails the step, 300 seconds by default, isn't honoured:
  a step keeps running through an outage, which is what a worker on the
  host is for, and a laptop that sleeps through it fails no remote step.
  Every step runs as a negative one asks. Format 1 reads one as a number
  of seconds, or a whole number with one of `d`, `h`, `m` or `s` after
  it, so `-1`, `-5m` and `-2d` are negative. The compiler warns, where the
  workflow writes it, of one that isn't negative, and of one an
  expression gives, whose value it doesn't know yet
  (`ssh_disconnect_timeout_ignored`); none of the known workflows sets
  it.
- **A job whose `ssh:` gives no `remoteHost`**, empty or null, runs as it
  would without its `ssh:`, where its `runs-on:` says, or on the run's
  default group, as format 1 runs it, and as a step's does
  ([Running steps](#running-steps)): Ready gives it no inline group.
  Format 1 notes it in the log of each step that runs so; Meridian keeps
  Ready's warning with the job (`job_ssh_host_empty`), once however often
  the job is readied, which its status and its end show, however it ends.
  The common case is a `remoteHost` that reads a `compute-clusters`
  input's `ip`, which is `""` for the workspace, this machine.

Where format 1, as ACTIVATE runs it, does otherwise:

- **What a host needs.** Meridian runs a worker of its own on the host,
  so the host must be Linux, macOS or FreeBSD (`ssh_host_unsupported`),
  of an architecture whose binary of the same release the coordinator's
  machine holds (`ssh_worker_binary_missing`), and its sshd must forward
  Unix sockets to the coordinator (`ssh_forwarding_refused`). ACTIVATE
  runs each step's script through the host's shell, over its connection,
  and needs none of them.
- **What a host keeps.** Meridian's binary, in the host's cache by its
  digest, and Meridian's state, under `~/.local/state/meridian`: its
  workers' placements, and the runs' directories. ACTIVATE keeps the
  job's directory alone.
- **A step on another host than its job's**, as a step's `ssh:` that
  names one, fails (`step_ssh_not_supported`) until it runs there as a run
  of its own ([Steps on another host](#steps-on-another-host)), which
  differs from ACTIVATE's run of it as that section says. ACTIVATE runs
  it there, but for a step that calls a workflow, whose child it runs
  where its caller runs, which Meridian refuses.
- **A step off its job's host.** In a job on a host, a step whose `ssh:`
  is null, or gives no `remoteHost`, would run where its job runs without
  its `ssh:`, the machine the run started on, which a worker on the host
  can't reach, so it fails (`step_off_job_host`) until it runs there as a
  run of its own. ACTIVATE runs it on that machine. None of the known
  workflows has one.
- **An unreachable host** is tried again after about 1, 2 and 4 seconds,
  where ACTIVATE waits 2, 4 and 8.
- **Host keys.** Meridian checks every host's key against known_hosts,
  `localhost`'s too, and an unknown key fails: it never trusts one on
  first use. So `remoteHost: localhost` needs this machine's key in
  known_hosts, as `ssh localhost` does. ACTIVATE checks host keys as its
  platform's host-key policy says, against the keys the platform keeps.
- **Logging in.** Meridian logs in with the user's agent and the key files
  their config names too, so a key with a passphrase works through the
  agent. ACTIVATE offers only key files beside a `.pub` that need no
  passphrase.
- **Algorithms.** Only those FIPS 140-3 approves
  ([FIPS 140-3](#fips-140-3)), so a host that offers none of them can't
  be used (`ssh_algorithms_unapproved`), as an sshd that offers only
  chacha20-poly1305 and curve25519 can't.
- **The user's config** is read whole, through `ssh -G`, so `Match`,
  `Include` and `ProxyJump` apply, as with `ssh`. ACTIVATE reads part of
  it, and for a jump host only its name and port.
- **A run's directory on a host** is in Meridian's state directory there,
  `~/.local/state/meridian/runs` by default, `localhost`'s too, not
  ACTIVATE's `~/pw/jobs/<workflow>/<number>`, or for `localhost` the same
  under `~/remote_pw`, which a few known workflows derive paths from. A
  child run's is in its root's there, in the same place as on the
  coordinator's machine, as in ACTIVATE
  ([Workflows a step calls](#workflows-a-step-calls)).
- **Outages.** Steps run through one, and their reports arrive after it.
  ACTIVATE fails a step once its connection has been lost for
  `disconnect-timeout`, and leaves its process running.
- **A `disconnect-timeout` format 1 can't read**, such as `1h30m`, `1.5h`
  or `300ms`, fails the step in ACTIVATE. Meridian ignores the key,
  whatever its value, and runs the step, with a warning
  (`ssh_disconnect_timeout_invalid`).
- **A job whose `ssh:` gives no `remoteHost`** has a warning, kept with
  the job, which its status and its end show (`job_ssh_host_empty`).
  ACTIVATE notes it in the log of each step that runs so.
- **Cancelling during an outage** reaches the worker once the connection
  is back. ACTIVATE tries for 10 seconds, and leaves the process running.
- **One worker runs a job's steps**, so they share its session, and a
  background process a step leaves is stopped with the step, as on a
  local member. ACTIVATE starts each step in a session of its own.
- **A host that kills a session's processes as it ends** takes no workers
  unless the user lingers (`ssh_worker_would_be_killed`). ACTIVATE's steps
  there end with their session.

### Images

A job that names no image runs in `ghcr.io/parallelworks/meridian-base`,
which this repository builds. An image's tag is resolved to a digest when
the job is placed, and the digest is what the attempt records and runs.

### Resources

`resources:` says what a job needs: CPUs, memory and GPUs. A job fits a
member that offers at least that. On Slurm and PBS, resources become
scheduler options, and `runs-on:` can add scheduler parameters of its own.

A job's time limit is its `timeout:`. There is no separate walltime:

- **The walltime Meridian requests** is the job's `timeout:` plus a margin
  the member sets, for starting the worker, pulling the image, running
  `cleanup:` commands and reporting. The worker enforces the timeout itself,
  so the job ends as timed out, with its cleanup run and its logs delivered,
  before the scheduler would end it.
- **A job without a `timeout:`** gets the member's default, or else the
  partition's. Either way, the worker reads the allocation's end time from
  the scheduler and stops the job before it.
- **A job the scheduler ends at its time limit timed out:** its worker
  wasn't lost. When the scheduler reports that it ended a job at its limit,
  such as Slurm's `TIMEOUT` state, the attempt's reason is `timeout`, so a
  `retry:` that lists `worker-lost` doesn't run it again. A node failure or
  a preemption is still `worker-lost` ([Job retries](#job-retries)).
- **Placement** leaves out a member whose longest allowed walltime is
  shorter than the one the job would request.
- **A format 1 job that sets its time limit through `runs-on:`** keeps it.
  The compiler warns when that limit is shorter than the job's `timeout:`
  and the margin.

## Security

- **Who is trusted.** A workflow runs with the privileges of the user who
  started it. Code that `uses:` brings in is as trusted as where it comes
  from, so references to it should pin a commit. One that names a branch is
  pinned when the run is created, so the run and its reruns run the same
  code. Untrusted code belongs in a group whose members use microVMs.
  GitHub serves a commit from a repository's whole fork network, so
  `github/<owner>/<repo>@<commit>` can name a commit that only a fork of
  the repository has: pin a commit you have seen in the repository itself.
- **Workers are trusted with one attempt.** A worker's credential covers
  one attempt and expires, and the coordinator checks every write against
  the attempt's lease.
- **Other users on a shared host** can't reach Meridian's state: the state
  directory is readable only by its user, sockets live in it, a listening
  coordinator requires TLS and a token, and tokens never appear on command
  lines.
- **The user's own processes can.** With the `local` backend, a workflow's
  steps run as the user who started it, so they can read what the user can:
  the state directory's key file, `secret-record.key`, and its database,
  with which they can open the secret record of every run that hasn't ended,
  passwords, secret variables and private outputs of other runs and other
  jobs among them. That comes with running steps as the user, and
  per-attempt least privilege, each attempt given only what its job reads,
  holds against a step only where steps run apart from the state, as in a
  container, or a group whose members use microVMs. What Meridian does
  close: `meridian run` and `resume`, which hold runs' secret records in
  their memory, keep the user's other processes out of it, as the worker
  does (not dumpable on Linux, `PT_DENY_ATTACH` on macOS, `procctl` on
  FreeBSD), and refuse to run where they can't
  (`coordinator_harden_failed`, `coordinator_system_unsupported`); and a
  worker's environment, and so its steps', leaves out the environment
  variables that the user's secret variables are read from.
- **SSH host keys are checked** against the user's `known_hosts`, the
  files their ssh_config names, by the host's `HostKeyAlias`, or else its
  name and port, as OpenSSH looks them up, with `@cert-authority` and
  `@revoked` lines. An unknown key is an error (`ssh_host_key_unknown`)
  until the user accepts it explicitly, as `ssh` offers to; Meridian never
  accepts a key on first use by itself, whatever `StrictHostKeyChecking`
  says. A key that differs from the one of its type that known_hosts has
  is `ssh_host_key_changed`, and one it revokes `ssh_host_key_revoked`,
  as is one that the file the config's `RevokedHostKeys` names revokes, a
  list of keys or a KRL, which `ssh-keygen -Q` checks, or, for a
  certificate, whose key or authority it revokes. Where that file can't
  be read, or a KRL checked, no key of the host's is trusted
  (`ssh_revoked_host_keys_unreadable`), as OpenSSH trusts none. A config
  with a `KnownHostsCommand` for the host is refused
  (`ssh_known_hosts_command`): Meridian doesn't run it, and what it
  prints could revoke the host's key, so Meridian fails closed rather than
  check the key against part of what the user trusts.
  The host is asked first for a key of a type known_hosts has for it, as
  OpenSSH asks, so a host with keys of several types presents one the
  user has. A host certificate passes only if an authority known_hosts
  names for the host signed it, neither it nor its authority is revoked,
  and it is valid for the host now (`ssh_host_certificate_invalid`); its
  key is checked as a plain key only when no authority known_hosts names
  signed it, as OpenSSH falls back. A hashed line, whose host is an
  HMAC-SHA1 of it, is matched by OpenSSH's `ssh-keygen -F`, since FIPS
  140-3 doesn't let Meridian compute HMAC-SHA1. Without `ssh-keygen`, or
  when it fails, no key is checked against a file that has hashed lines
  (`ssh_known_hosts_unmatched`): one of them could revoke it.
- **Secrets** stay out of the plan and run state. A run's passwords and
  secret variables are in its secret record, encrypted under a data key of
  the run's own, until the run ends, and the record opens only as the
  latest of its own run, in its own store
  ([The secret record](#the-secret-record)).
  Each attempt receives only those its own job reads, when it claims, and
  they are masked in its logs and outputs.
- **Variables** stay out of the plan and run state too, secret or not, since
  people keep secrets in plain variables. A run reads them as it starts and
  keeps them in its secret record while it runs, and its diagnostics never
  quote one ([Variables and secrets](#variables-and-secrets)).
- **Untrusted input is bounded:** workflow size, YAML aliases, expressions,
  outputs and log volume.

### FIPS 140-3

Releases are built with Go's FIPS 140-3 cryptographic module
(`GOFIPS140=certified`) and run under `GODEBUG=fips140=only`, which turns
the use of a non-approved algorithm into an error. The binary makes that
its default, but a default is all it is: the `GODEBUG` environment
variable overrides it, and a worker can be started by hand, or over SSH,
with any environment. And a build without `GOFIPS140` compiles the
module from Go's source, as the version `latest`, which isn't the
validated module in any mode. So no `meridian` command runs, the worker
above all, unless the process's cryptography runs in FIPS 140-3 mode,
enforced (`fips_not_enforced`), with a validated module
(`fips_module_not_validated`); `worker.Run` and `coordinator.Open`
check both too, whatever program calls them, as every entry point of
Meridian's must (`internal/fipsmode`).

- **TLS 1.3**, through `crypto/tls`, with the algorithms FIPS mode allows.
  The coordinator's certificate uses an ECDSA P-256 key.
- **SSH**, through `golang.org/x/crypto/ssh`, with the algorithms it can run
  in FIPS 140-only mode pinned: key exchange with ECDH on P-256, P-384 and
  P-521; the ciphers aes128-ctr, aes192-ctr and aes256-ctr; the MACs
  hmac-sha2-256 and hmac-sha2-512, and their encrypt-then-MAC forms; and
  host keys and certificates of ECDSA, Ed25519 and RSA signing with
  SHA-2. User keys are the same, RSA of 2048 bits or more. The library
  drops what FIPS mode doesn't approve, but still offers AES-GCM, which it
  builds with an arbitrary nonce, and the ML-KEM hybrid key exchange,
  whose X25519 isn't approved, both of which fail in FIPS 140-only mode,
  so its defaults aren't used. A server that offers none of a list's
  can't be used (`ssh_algorithms_unapproved`), as one that offers only
  curve25519, chacha20-poly1305 or the Diffie-Hellman groups can't.
- **Secret records** are encrypted with AES-256-GCM, through `crypto/cipher`
  with random nonces (`cipher.NewGCMWithRandomNonce`), the way the module
  approves, each under a data key of its run's own from `crypto/rand`, and
  bound by its associated data to its store, its run and its generation.
  A platform's Keys interface wraps the data keys with its own key
  management, and the local one with AES-256-GCM under a key in a file.
  The local key file's key is HKDF-SHA256's input too, for the keys
  derived from it (`local.Keys.Derive`), as the one that keys the hash
  naming a host's lease and socket: one key used both to wrap and to
  derive, each derived key under a label of its own. A later version of
  the key file could wrap with a key derived from it as well, keeping the
  file's own key for deriving alone.
- **Tokens** come from `crypto/rand` and are stored as SHA-256 hashes;
  digests are SHA-256, and a digest of what can hold a secret, as an
  action's request, is an HMAC-SHA256 under a key that HKDF-SHA256
  derives from its run's data key ([The secret record](#the-secret-record)).
- **External tools** that Meridian runs (`git`, container runtimes,
  schedulers) run in processes of their own, outside Meridian's module.
  Their TLS and SSH are the host's cryptography, which a FIPS host
  configures for the whole system; Git's own hashing is below.
- **Git** is the `git` command, on the coordinator's host, for the
  workflows `uses:` names and the repositories `parallelworks/checkout`
  names, not a Git client in Go. GitHub and GitLab name a repository's
  objects by their SHA-1 digests, so a client computes SHA-1 to check what
  it fetches, which `crypto/sha1` refuses to do under `fips140=only`. A
  client in Go needs a SHA-1 of its own, outside the module, in
  Meridian's process: go-git's is `sha1cd`, and go-git brings more
  cryptography from outside the standard library and `golang.org/x/crypto`
  with it, much of it not FIPS-approved. The command hashes with its own
  SHA-1 too, but in a process of its own, outside Meridian's module, which
  is why it is acceptable; its TLS and SSH are the host's, as above.
  Meridian's own digests, of a pinned workflow's text among them, are
  SHA-256, in the module.

Every binary is a pure-Go, static build (`CGO_ENABLED=0`). On macOS, Go
binaries still link `libSystem`, as Apple requires.

## Operations

- **Logs:** `log/slog`, as JSON for services and as text for the command.
- **Telemetry:** OpenTelemetry traces (a run, its jobs, their attempts and
  steps, with the trace context passed to workers) and metrics (queue time,
  placement time, run time, attempts by outcome). Exporting them is opt-in;
  by default Meridian sends nothing anywhere.
- **The audit record:** each attempt records the plan digest, the workflow's
  source and revision, the image digest and container runtime, the worker's
  version and digest, the member and why it was chosen, the backend and its
  placement ID (such as a Slurm job ID), its timestamps, and its outcome and
  reason.
- **Retention:** runs, logs and hopper's history are kept for a configured
  time. A job's outputs are kept with its run. A run's secret record is
  deleted when the run ends.
- **Benchmarks:** compile time for large workflows, the time Meridian adds
  between a job becoming ready and its placement, and job throughput on
  SQLite and Postgres. Their thresholds live in the repository and gate
  releases.

## The `meridian` command

| Command | Does |
| --- | --- |
| `meridian init` | Writes a new workflow in the latest format. |
| `meridian run <workflow>` | Compiles and runs a workflow, coordinating it in the foreground. |
| `meridian validate <workflow>` | Compiles without running, and reports every error. |
| `meridian migrate <workflow>` | Rewrites a workflow in the latest format. |
| `meridian status [run]` | Shows runs, or one run's jobs. |
| `meridian logs <run> [job]` | Shows or follows logs. |
| `meridian cancel <run>` | Cancels a run. |
| `meridian resume <run>` | Picks up a run whose coordinator stopped. |
| `meridian rerun <run>` | Runs a run again, or only its failed jobs. |
| `meridian serve` | Runs a coordinator that keeps running. |
| `meridian worker`, `meridian host`, `meridian relay` | Started by backends, not by people. |

`meridian init`, `migrate`, `rerun`, `serve` and `relay` aren't built yet,
and each says so.

A run's ID is a number, one more than the last its store gave, which people
type, as in `meridian logs 42`, and which the run's steps see as
`PW_JOB_ID`. The store's own ID, which is random, keeps a number that a new
store gives again apart from an old store's
([The secret record](#the-secret-record)).

A run's directory, `PW_JOB_DIR`, is temporary: a directory of its own under
the system's temporary directory, in one of the user's that only they can
use, which Meridian checks as it checks its own. Meridian makes it once the
run is stored, and makes it new, so that a run never shares one with an
earlier run of the same number. A step can remove it, as `cleanup: rm -r
${PW_JOB_DIR}` does in workflows we know of, so it isn't in Meridian's
state directory, and a reboot can empty it, so what a run keeps belongs
elsewhere. Its name is the run's number and the store's ID. `--run-dir`
names another, which mustn't be there yet.

`meridian run` exits 0 when the run succeeded; 1 when the workflow has
errors, or the run failed or was cancelled; and 2 when Meridian couldn't do
what it was asked, and when it leaves the run running, as when it is
interrupted ([The coordinator](#the-coordinator)).


Execution groups, variables, clusters and the GitLab servers a workflow can
name live in Meridian's config directory, and `--group` picks a group. A
built-in `local` group needs no config: one member, this machine, which
names host isolation explicitly, and which a group of the same name in
the config replaces. Groups and clusters are never read from the repository
a workflow comes from, so cloning a workflow can't add a host to yours. It
can still send its jobs to one you defined, by naming it: a `runs-on:` that
names one of your groups, or a `compute-clusters` input whose default is
`hpc`, names your own group or cluster. And a format 1 `ssh:` names its
host itself.

- **`groups.yaml`** holds each group by name, with its members in order:
  each one's backend, its settings, its isolation, the resources it offers
  (`cpu: 4`, `memory: 8Gi`, `gpu: 1`), and its walltime limit, margin and
  default timeout.
- **`variables.yaml`** holds `org` and `vars`. A plain variable is its
  value, and any variable can instead name an environment variable or a
  file that holds it; an environment variable set to `""` gives `""`, as
  a platform's variable whose value is empty does. A secret one is marked
  `secret: true`, and always names where its value is, which is read only
  as a run starts.
- **`sources.yaml`** holds `gitlab-servers`, the GitLab servers besides
  gitlab.com that a step's `with: $host` can name, each a host name with
  no port, as a `$host` names it: `git.example.com`. Git reaches them with
  the user's credentials, so a workflow can't name a server of its own,
  and at HTTPS's port, or at another that Git's own `url.<base>.insteadOf`
  rewrites the server's address to.
- **`clusters.yaml`** holds the clusters a `compute-clusters` input can
  name, each by its name, with its `host`, and, if it gives one, its
  `scheduler`, `slurm` or `pbs`
  ([`docs/examples/clusters.yaml`](examples/clusters.yaml)). The host is
  an alias that ssh resolves, as `ssh -G` does, or a host name or an
  address, without a port: format 1 gives an existing cluster's `ip` so,
  and 21 of the known workflows run `ssh` or `scp` with it in a script. A
  host that needs more, such as a port, a user to log in as, a jump host
  or an IPv6 address, is an alias the user's `ssh_config` defines, as
  format 1 takes a login from the config's `User`. A host can't start
  with `-` or hold a space, as a workflow's script can put it in a
  command line. A name is letters, digits, `_`, `-` and `.`, as a group's.

  An input that names a cluster sees what format 1 gives for an existing
  cluster's details, for what a cluster here can know of them: `name`;
  its host as `ip`, which 189 of the 196 workflows we know of that have
  such an input read, as a job's `ssh:` `remoteHost` above all; its
  scheduler as `schedulerType`, or `""` for none, as format 1 writes a
  cluster's that has none; `existing` as its `type` and its `provider`, as
  format 1 names a cluster that a platform reaches but didn't make, which
  73 of those workflows read to decide what else to ask for; and `$type`
  `computeResource`. What names a platform's own record of the cluster,
  its `id` and its `pw://` `uri`, and its owner's platform account, its
  `user` and its `namespace`, it leaves out: an existing cluster without
  the account fields, a shape format 1 doesn't give, where the 5 reads of
  them in the known workflows feed the platform's own CLI. The workspace,
  `workspace` or `user-workspace`, is the machine a run starts on, this
  one, and needs no config: `id` and `name` `user-workspace`, and `ip`
  `""`, as format 1 gives it, without the person's platform account.

  A name resolves whatever the input's definition says, as format 1
  resolves it: `include-workspace`, `provider` and `csp` choose only what
  a form lists, and a name given otherwise, or a default the form can't
  list, is resolved all the same. Another name is `cluster_unknown`, which
  lists the clusters, or `clusters_none` when there are none, as without
  a clusters file. A list of names gives each one's details, and what in
  it isn't a name, as the details a run passes to a workflow it calls, is
  kept as it is ([Bug fixes to format 1](#bug-fixes-to-format-1)).

  A cluster's host is in the run's values in the clear, as the rest of an
  input's value is, which whoever can see the run sees, as on a platform.
  Where a variable or a password names the cluster, its details are
  private, as what is made from one is: the plan doesn't hold them, the
  run's secret record does, and the job whose `ssh:` reads them has a
  private target, so its placement is private, and the problems of its
  placement are masked.

Meridian refuses any of these files if another user can change it, and the
variables file if another user can read it, since people keep secrets in
plain variables too. A file that holds a variable's value must be the
user's, and only they may change it, or read it when it holds a secret's,
and the directories on the way to it are checked as Meridian's own are.
Meridian reads none of these files, nor its key, through a symbolic link at
the file's own path, so that what it reads is the file the path names: a
directory on the way to one, the config directory among them, can be a link,
and is checked through it.

The database, the key that wraps secret records, logs and the socket that
workers on the machine connect to live in Meridian's state directory, which
only its user can read. SQLite can't lock reliably on a network filesystem,
which is where home directories on clusters often are, so Meridian refuses
to open a database there, as on any FUSE filesystem and in any folder a
virtual machine shares with its host, whose locks it can't tell hold, as
the store's SQLite driver does: fuse-overlayfs, the usual root of a
rootless Podman container, among them. It says how to choose a local
path: one that only the user can change, and that lasts, since it holds
the database and the key, such as one that `mktemp -d -p /var/tmp` makes,
but not one in `/tmp`, which a reboot can empty. On Linux the
directories are the XDG base directories' `meridian`, as
`~/.config/meridian` and `~/.local/state/meridian`; on macOS both are
`~/Library/Application Support/Meridian`. `MERIDIAN_CONFIG_DIR`,
`MERIDIAN_STATE_DIR` and `MERIDIAN_CACHE_DIR` choose others, and the cache
directory holds copies of the repositories that `uses:` names. As OpenSSH
checks the directories on the way to a user's keys, Meridian checks those
on the way to each of its own: each must belong to the user or to root,
and no one else may change it, unless its sticky bit, as `/tmp`'s, keeps
others from removing what follows it, which must be the user's. In a user
namespace that doesn't map root, as `unshare --user`, Apptainer, bubblewrap
and some CI sandboxes make, `/` and much else show as owned by the kernel's
overflow ID, and Meridian can't tell who owns them: it refuses them, saying
so, rather than assume that no one else can change them, as it refuses
every path on a system that doesn't say who owns a file. So Meridian's own
directories and config must be used outside such a namespace, or in one
that maps root. A worker in one can't use its cache directory or
`XDG_RUNTIME_DIR` either: it keeps its own files in the system's temporary
directory, as it does wherever neither is safe, with a warning in its log
that says why, and refuses a bootstrap token's file, which its backend can
give it in its environment instead.

Whoever can write the coordinator's database can claim an attempt, and
with it the private values its job reads, so the coordinator's store
checks the database's path itself as it opens it, whoever chose the path,
before SQLite opens anything. The way to the database's directory is
checked as above, and the directory must be the user's, with no access
for others. The database, and each file SQLite keeps beside it (its
write-ahead log, the log's index and a rollback journal), must be, where
it is there, a regular file of the user's, not a link, with no access for
others. The store makes the database readable and writable only by its
user, so the files SQLite makes beside it are too.

The command's Sources, which reads the workflows that `uses:` names, runs
the `git` command on `PATH`, with the user's own Git configuration and
credentials. It needs Git 2.29 or later, the first whose `git fetch`
takes `--no-write-fetch-head`, which Sources passes so that a fetch leaves
no `FETCH_HEAD` in its copy of the repository; an older Git fails the
fetch, and a missing one is `git_unavailable`. Nothing else the command
does needs Git.

Every problem the command reports is a kind, with its code, its params and
its message in each language, as a workflow's are: its own, such as a
config file another user can change, are in the problems package's
`command` registry, at `/problems/command/<code>`.

`meridian validate` writes each error and warning as compilers do,
`file:line:column: severity: message (code)`, or with `--format json` as
an API sends them ([Compiling](#compiling)): a workflow with errors is an
RFC 9457 validation problem, with its warnings beside them. It exits 0
for a workflow without errors, 1 for one with, and 2 when it couldn't
compile it. It creates the run as `meridian run` would, with the user's
variables and groups, and pins the workflows `uses:` names through the
command's Sources, so it reports an execution group that isn't in the
config, a workflow that isn't there or doesn't load, and sessions, which
nothing provides here. With `--start`, it starts the run too, as far as
it can without running anything: it reads every variable the run reads,
secret ones too, evaluates what waits for the start, checks it and the
run's shape, and expands the matrices known then, and reports what would
fail the run as it starts, where the workflow writes it.

`meridian validate` takes the run's input values as `--inputs`, a JSON
file, whose values it sends as the file writes them, and as `--input
path=value`, which sets one input over them, converted by its input's
type by the rules of ACTIVATE's `pw workflows run --input`: a boolean
takes `true` or `false`, exactly; a number a JSON number, so `007`,
`0x10`, `+1`, `.5` and `1.` are errors, and so is one too large for 64
bits; a list, a multi-dropdown, a repeated wizard step, a group, a wizard
step, a compute target and a `multi` compute-clusters, compute-resources
or bucket input JSON of their shape; every text type, and an input the
workflow doesn't define, the text as it is; a dropdown the text, unless
no option's value is that text and an option is a number or a boolean the
text reads as, when it takes that option's value, among the list an
`option-key` picks by the values sent, or every list; and a header is an
error. A path is an input's name, or a group's and its input's, as
`group.name`; the inputs of a group with `flatten: true` and of a
flattened wizard step are set by their own names, and naming the group or
the step is an error, as a path through a list's rows is: a list is set
whole. A problem with an `-input` names its path, never its
value, which can be a password.

### Following a run

`meridian run` shows its run as it goes, by following it
(`Coordinator.Follow`): the run's events, from its first, in order, among
them its stop, `run.stopping`, with why, `cancelled` or `timeout`, once a
person cancels it or it runs past its `timeout:`; and its steps' logs as
their workers store them.

- **Which logs there are.** A log that isn't there yet can't be told from
  one that never will be, so the events say which logs to read: an
  attempt's claim begins its setup log, and each step's, retry's or
  cleanup's start begins its own. A worker reports its attempt's end only
  once every log is stored, so each log of an attempt that completed ends
  whole. One whose attempt ended otherwise, as when its worker was lost, is
  shown as far as it was stored, and said to be cut short (`log_cut`).
- **In order.** An event that ends a step, an attempt or a job is shown
  once its logs, as far as they are stored, are; and the run's end last,
  once every log is. A worker sends its events and its logs apart, so a
  step's end can be stored before the last of its log: it is held back,
  with the events after it, until the step's log has ended, for 2 seconds
  at most after it was stored, by the store's clock, and the log is then
  followed on. The worker isn't made to wait for its logs, which would
  count against a job's timeout, a round trip a step.
- **Promptly.** It reads the run's events again as soon as a transaction
  of its own process adds one, and every 200 milliseconds for those
  another process adds, and for the logs, which aren't in the store.
- **As text or JSON.** `--format text`, the default, writes a line a
  person reads for each event, with its time, and each line of a step's
  log after the job and step it is of, as `build step 0| compiling`, and
  those of the runs its steps call after the step that called each, as
  `build step 0 › main step 1| testing`; the run's problems, and the
  command's, go to standard error as compilers write them. `--format json`
  writes a JSON object a line, each with `version`, now 1, which a later
  release can add members and kinds of line to, but never changes the
  meaning of: one for the run's creation, one for each event, one for each
  line of a log, setup logs too, its last with `ended` or `cut`, and one
  for a run left running, each of a run a step called with its `run` and
  its `callers`; and the command's problems as RFC 9457 problem details.
  `--logs=false` shows no logs.
- **Nothing secret.** Logs and a step's problems are shown as the worker
  masked them, and an attempt's problems as the coordinator stored them,
  masked where it ran is private; a private placement's member is never
  shown, nor anything else only the run's secret record holds.

### Status, logs, resume and cancel

- **`meridian status`** lists the store's last runs, the newest first, 20
  unless `-n` says: each one's state, created, running or ended, how it
  ended, when it was created and its workflow's location. Children of a
  run, the runs of the workflows its steps call, are listed under their
  root, as a count of the runs below it, and with `--all` as runs of their
  own, each with what called it ([Workflows a step
  calls](#workflows-a-step-calls)). `meridian status <run>` shows one run,
  its directory and its times, and what called a child, a line for each of
  its jobs, waiting, ready, placing, running or ended, with how it ended,
  how many attempts it had and the member its last ran on, none for a
  private placement, a line for each run its steps called, and then the
  run's problems and its jobs'. `--format json` writes a JSON object a
  line, of the same `version` as `meridian run`'s: one for each run, or
  one for the run, one for each job, and one for each run its steps
  called, a `child`, with its `parent`. A state directory without a store
  has no runs.
- **`meridian logs <run> [job]`** shows the run's steps' logs as `meridian
  run` does, the run's events saying which logs there are, and those of
  the runs its steps called under the steps: as far as they are stored,
  and with `-f` as they are stored, until the run ends. A job's name shows
  each of its instances' logs, and an instance's key that one's, with
  the runs their steps called.
- **Reading changes nothing.** `status` and `logs` read the store without
  changing it, not even its schema, which every other command brings up to
  its release's as it opens the store, so that a newer release's `status`
  doesn't change a store that an earlier release, still running a
  workflow, uses. They read a store whose schema is at their own release's
  version only, and refuse an earlier one (`database_too_old`), which
  `meridian run` or `resume` of their release brings up to date once the
  earlier release is done with it, as a later one (`database_too_new`).
- **`meridian resume <run>`** takes a run up again as one more replica,
  as when the `meridian run` that coordinated it was killed, or left it
  running: it works the run's Start if that hasn't run, and no process
  that lives holds it, as the `meridian run` that created it does until
  the run has started, reading variables from its own environment, and
  holds the Start itself as it does; it holds the socket once no other
  process does,
  and shows the run, from its first event, until it ends, exiting as
  `meridian run` does. What a killed process held of the run's work, its
  jobs' supervision and its end, another replica takes over once its
  lease, hopper's, lapses, in 15 seconds; the run's workers, cut off
  meanwhile, hold their reports and logs and send them once a process
  holds the socket again. Before it takes up a run that hasn't ended, it
  checks the run's directory as `meridian run` did once it made it, as the
  run's steps will run in it: one that is no longer the user's own private
  directory refuses the run, and a run that hasn't started, so that nothing
  has run in it, ends failed with what is wrong. A run that has started,
  which it refuses, it doesn't stop: the store's other replicas, as the
  `meridian run` that coordinated it, and the run's workers go on with it,
  until `meridian cancel` stops it. A run whose plan only a newer release
  reads is refused (`run_plan_newer`) and left as it is.
- **`meridian cancel <run>`** stores the run's stop, as Ctrl-C's cancel in
  `meridian run` does (above, under Cancellation, timeouts and cleanup),
  and says so: `run 1 stopping (cancelled)`, or, as JSON, a `run.stopping`
  line. Every `meridian run` and `resume` of the store works its runs, and
  holds the socket or waits for it, so one that holds it sees the run to
  its end, and `meridian cancel` exits at once. When none does, as when the
  run's `meridian run` was killed, or left it running, `meridian cancel`
  coordinates the run itself, as `resume` does, once it has checked the
  run's directory as `resume` does, until it has ended, and says how it
  ended, so that its workers hear of the stop, and the
  placements of those that don't are withdrawn once their leases lapse.
  `-f` shows the run, as `meridian run` does, until it ends, either way.
  When the process that coordinates the run exits before it has ended,
  with no other waiting to take its place, the run waits, stopped, for a
  coordinator: `meridian resume <run>` sees such a cancel through. A run
  that hadn't started ends cancelled at once; one that had ended already is
  said to have, as JSON with its `run.finished` line. It exits 0 once the
  run is stopped, or has ended cancelled, or had ended already. A run it
  coordinates whose first stop was its timeout, which the cancel changes
  nothing of, ends as that has it, and it exits as `meridian run` would. It
  exits 2 for a run the store hasn't (`run_not_found`), or whose directory
  is no longer private, which it leaves stopped without coordinating it,
  as for any problem of its own, and when an interrupt leaves the run it
  coordinates running. `meridian status` shows a run
  that has been stopped, and hasn't ended, as `stopping`, with why.

## Editors and web UIs

Editors and web UIs validate workflows, expand matrices and evaluate
expressions with a TypeScript package built from the same Go code, as
WebAssembly: the phases' own functions, so what they accept is what runs.
The package exports typed functions rather than setting globals.

The package also edits workflows. An edit sets, adds or removes the value at
a JSON Pointer and leaves every other byte of the file as it was, so comments
and formatting survive. `meridian migrate` changes files the same way, and a
platform that stores a workflow's text can change it without losing what its
author wrote.

## Open questions

Larger features that aren't designed yet are proposed in
[Stories](stories.md).

- **A built-in container runtime for Linux.** A runtime in pure Go is
  possible: the worker can start a job's process in new namespaces itself,
  and be the container's init so that nothing has to join a running
  container. It would let an SSH host run jobs with nothing installed but
  Meridian. Against it: container runtimes are security-critical code, and
  hosts would still need `newuidmap`, a network helper and, on some
  distributions, an AppArmor profile. Revisit once driving Podman and Docker
  works.
- **Steps that run a workflow.** A job made only of calls to workflows
  could run on the coordinator, with no worker; 28 known jobs are. The
  worker runs every calling step, and waits for the child run, parked
  ([Workflows a step calls](#workflows-a-step-calls)): on `local` and
  `ssh` it holds its lease, and lends its member's capacity to its own
  descendants, but on a batch scheduler a waiting job can't give back its
  allocation, which the child's jobs may need, and a child whose jobs
  queue behind their caller waits for it until the caller's walltime or
  timeout ends it. Decide before a scheduler backend runs children.
  Format 1's answer is to run the calling job off the scheduler, on the
  cluster's controller, through its `ssh:` hop, so that it holds no
  allocation while it waits. Others: run a child's jobs within the
  caller's allocation; run call-only jobs on the coordinator, which only
  28 of the 267 calling jobs are; or give the caller's job back and place
  it again once the child ends, which can't go on mid-job for the 239
  calling jobs that share files, env or outputs with their other steps.
- **The Kubernetes client.** `client-go` is a large dependency for a few API
  calls; plain HTTP against the API may be enough.
- **How long a job may wait in a queue** before Meridian withdraws it and
  places it on another member, and whether the job or the group sets that.
  An interactive session and an overnight batch job in the same group would
  want different limits.
- **Files between members.** A job on another member doesn't see the run
  directory's files. Staging them, or declaring which files a job needs,
  would let jobs of one run use members without a shared filesystem.
- **What a rerun asks for again.** A rerun needs what was submitted, passwords
  among it, as the run's secret record is deleted when the run ends
  ([Reruns](#reruns)). Should a platform keep what was submitted, so a rerun
  needn't ask again? It would keep the run's passwords past its end, in a
  store of its own, where Meridian keeps none.
