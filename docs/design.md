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
| `coordinator` | The interfaces through which the coordinator, which will store runs, schedule and place jobs, and serve the worker API, reaches what differs between a laptop and a platform: keys, logs and events; the timing of its leases; how it encrypts a run's secret record ([The secret record](#the-secret-record)); and its store, hopper on SQLite with Meridian's own tables beside it, for runs, their secret records, jobs' results, attempts with their leases and fences, and the endpoints workers reach it through. The rest of the coordinator isn't built yet ([The coordinator](#the-coordinator)). |
| `worker` | Runs one attempt of a job and reports on it. |
| `workerapi` | The worker API's paths, requests, responses and problems, which the coordinator serves and the worker calls. |
| `backend` | The interfaces of execution groups and their members, and of what starts workers on a member. |
| `cmd/meridian` | The command. |

The backends that start workers (`local`, `ssh`, `kubernetes`, `slurm`,
`pbs`) and the drivers of container runtimes (Podman, Docker, Apptainer,
Apple's `container`) will be packages of their own; none is built yet
([Backends and isolation](#backends-and-isolation)).

The `meridian` command wires the packages together with local resolvers.
It will run a coordinator in its own process, with a SQLite store; today
it validates a workflow (`meridian validate`) and runs one attempt of a
job as a worker (`meridian worker`), and its other commands say they
aren't built yet ([The `meridian` command](#the-meridian-command)). A
platform wires them with its own resolvers, Postgres and as many
coordinator replicas as it needs. Nothing in the packages knows which of
the two it is running in.

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
    a key the schema doesn't define, and none of the 221 known workflows
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
    run as they were. None of the 221 known workflows has a group with
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
  None of the 221 known workflows
  ([Bug fixes to format 1](#bug-fixes-to-format-1)) has one. Whether
  format 1 gains it as an addition, with ACTIVATE's meaning, or ACTIVATE
  drops it, is a decision for later.

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
`.git` and `.github`, counted once by the SHA-256 hash of its content: 221
files.

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
  default but sl_core's `push_to_gh`, which ACTIVATE fills with `false`
  when it is left out, as Meridian does.
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
  124 `env:` blocks has a value that reads another of its own block.
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
  both with the values it fills in. Of the 221 known workflows, 16 have
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
  reads. Of the 221 values of the 124 `env:` blocks, 2 are written as other
  than text and 139 are an expression whole, which can give other than
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
  keys the inner group dropped. None of the 221 known workflows has a group
  with `flatten`, which format 1 adds
  ([The workflow format](#the-workflow-format)).

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
and the masker. The worker calls Step and End; the coordinator, which calls
Start and Ready, is designed here, and until it is built only
`meridian validate --start` calls Start.

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
   the run's own steps' `uses:` name. A child run pins the workflows its
   own steps name as it is created; pinning a run's descendants all at
   once, with cycle detection and a depth limit, isn't built yet
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
     No known workflow has one: of the 221 that
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
default; how deep workflows that `uses:` calls nest; and the nets on what a
group of templates builds and does ([The language](#the-language)). The
defaults are high enough for every workflow in the conformance suite. An
expression's depth, `range`, and what one template builds and does are the
language's limits, the same wherever a workflow runs. A plan's JSON nests at
most 10,000 levels, as deep as its encoding reads, so a value nested deeper
is an error. The plan records the limits that the phases after Create
apply, such as a matrix's instances, so every coordinator replica, and
every rerun, applies the same ones (`Run.Limits`). Today the file's size
and the nets are fixed, and of the others `Options.Limits` has only a
matrix's instances.

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
  run, where Meridian asks for what the default gives. None of the 221 known
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
  input of such a type that has a value is an error.
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
  starting again gives the same error, so the coordinator fails the run.
- **A plan of another release's version** wraps `plan.ErrVersion`: a
  replica of a release that reads it starts it
  ([Plans across versions](#plans-across-versions)).
- **Any other**, a resolver that couldn't answer, as the Inputs
  resolver's, which wraps `compile.ErrUnanswered`, or the context's end,
  the coordinator starts the run again after.

Decoding the plan or the secret record before Start fails alike: with
`plan.ErrVersion` for another release's version, or for a plan whose
trees name what a newer release's format has, and with
`plan.ErrMalformed` for what no release encoded, or this one can't read
as its own version, which decoding again gives too, so the coordinator
fails the run.

A coordinator that dies during Start leaves its job to another replica,
which starts it again from the beginning: nothing reads Start's results
until they are stored. A rerun runs Start again ([Reruns](#reruns)).

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
  ([Outputs](#outputs)).

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
  Each time the coordinator adds a part, as a job's as the job ends, it
  encrypts the whole record again under the same data key, with a new
  nonce, at most 2^24 times for a run (`record_reseal_limit`), which keeps
  the chance that two of the data key's nonces are the same below 2^-48.
  Neither the data key nor what the record holds is ever in an error, and
  nothing decrypted is kept between calls. Both are cleared once used, as
  far as Go lets them be: not AES's key schedule or GCM's hash key, inside
  the cipher, the buffer the record is encoded in, which the JSON encoder
  keeps for reuse, or the record's strings (`coordinator/internal/seal`).
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

  Deleted means erased from the store's files, not only from its tables,
  so that someone who later has both a copy of the database and the key
  that wrapped the record's data key can't open it. On SQLite, the store
  writes with `secure_delete` on, which overwrites with zeros what it
  deletes or replaces, the sealed record and its wrapped key among it,
  rather than leave it in free space. Once the transaction that ends the
  run commits, the store checkpoints the write-ahead log into the
  database and truncates it, so the log keeps no copy either. If a reader
  or a writer in another process holds the log, that checkpoint can't
  truncate it, and the next one does: when the next run ends, when the
  store is next opened, or in hopper's maintenance. A backup of the database taken
  before the run ended holds the record, sealed, as it was.
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
  instances (`plan.Expansion.Split`), and what it was readied with
  ([A job's lifecycle](#a-jobs-lifecycle)).
- **Each attempt's** holds its lease and fencing number
  ([Leases and fencing](#leases-and-fencing)), the hashes of its bootstrap
  token and its credential, and its audit record
  ([Operations](#operations)).
- **The run's events**, each change to it in order, which every process
  that follows the run reads, whichever process made the change.

It holds no secret that Meridian can recognize: a job's outputs are checked
against the secrets its attempt received before they leave the worker
([Outputs](#outputs)), and diagnostics never show a value made from a
variable or a password. Nor does where a job runs, when its `runs-on:`,
`resources:`, `retry:` or `timeout:` reads one: Ready says which of them
did (`plan.ReadyJob.PrivateFields`, and `SecretFields` for a secret), and
the coordinator keeps them out of run state: Ready gives them again from
the secret record whenever the coordinator needs them, as the record holds
the run's timeout when that reads one, and the placement made from them,
the member chosen and why each other wasn't, is in the attempt's row,
sealed under the run's data key as the secret record is.

### Plans across versions

A plan has one reader, as a workflow does. A release reads every plan
version up to its own, and upgrades an older plan in memory as it decodes
it, as the compiler translates an older format, so everything past decoding
sees one version.

- **Every change gets a version.** A plan's version rises with each change
  to its encoding, an addition among them; an addition's upgrade changes
  nothing. Each version's encoding is described by a JSON Schema, kept in
  this repository, so the plan is a contract a platform can read too. The
  trees of stored expressions are part of it, and so are the worker API's
  bodies, which carry a run's values and the job Ready evaluated
  ([The worker API](#the-worker-api)).
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

Version 1 is frozen: every change to the plan's encoding, or to the worker
API's bodies, from here on raises the version, an addition among them. Its
schemas are in `plan/schema/1` (`plan.Schemas`): a plan's
(`plan.schema.json`), whose definitions hold the other types'; a ready
job's, a secret record's, a run's values' and a job's result's, which name
them; and the worker API's bodies' (`workerapi.schema.json`), which refer
to them. They refer to each other by their names, so they resolve wherever
the directory is. Tests check that every encoded type's members are what
its schema describes, that golden files setting every member, and what
Meridian encodes for every conformance case, fit the schemas, that
decoding agrees with the schemas on each member of the goldens and the
bodies left out, null, or a number past its bounds, and that every member
has the version that gave it, in plan's and the worker API's since tables,
so that a member added later can't go in without a version. A value is
named one way throughout, by its path with its context (`plan.JoinPath`),
as in `inputs.region`, in the plan, the secret record and a run's values.

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
| Inputs | Input types beyond the built-in ones: what a value of one names, as a cluster's details for its name | `execution-groups`, the user's groups by name; a scheduler's partitions, accounts and queues come later |
| Variables | Organization and user variables, plain or secret, by name | A variables file in Meridian's config directory, in which a secret names the environment variable or file that holds its value |
| Keys | The key that wraps each run's secret record's data key, for the run, by its store's ID and its own | A key file in Meridian's state directory, which only the user can read |
| Groups | Execution groups | The user's config |
| Sources | Workflows that `uses:` names, pinned and read by their pins | Git, through the `git` command, 2.29 or later |
| Actions | Built-in actions, which no interface provides yet: the worker refuses a step that `uses:` one (`uses_not_supported`) | `parallelworks/checkout`, and those that need no platform, once built |
| Sessions | Interactive sessions a workflow serves | None |
| Logs | Storing and reading step logs | Files in Meridian's state directory |
| Events | Run, job and step events as they happen | The command's progress display |

The compiler's resolvers, Inputs, Variables, Groups, Sources and Sessions,
are interfaces in `compile`, the coordinator's, Keys, Logs and Events, in
`coordinator`, and the backends' `Driver` in `backend`. A resolver may be
missing. Then the workflow features that need it fail to compile, with an
error saying which interface was missing.

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
Postgres for a platform. hopper holds a run's jobs, and schedules and
retries them; Meridian's tables hold what hopper doesn't model, the plan,
the secret record, and the state of each job and each attempt
([Run state](#run-state)). Meridian uses only what hopper has: workflows of
jobs with dependency edges, jobs that snooze until a time without using an
attempt, and jobs that complete in their caller's transaction.

- **A run is a hopper workflow.** One transaction gives the run its ID,
  stores its plan, its secret record, sealed, and its state, and inserts
  the workflow, so a run is stored before anything executes, and either all
  of it is stored or none of it is. The workflow's first job is Start
  ([Starting a run](#starting-a-run)), which every other job waits for, and
  its last the run's finisher, which waits for every job, decides the
  run's outcome, and deletes the secret record in the transaction that
  ends the run.
- **Each job has a supervisor**, a hopper job that waits for its needs'
  supervisors, so `needs:` becomes hopper's dependency edges. The
  supervisor drives its job from Ready to its end: it readies the job,
  places its attempts and waits on them, and ends the job. It waits by
  snoozing until its next deadline, such as a lease's, and a worker's report
  that changes the job, stored by whichever replica the worker reached,
  wakes it at once. Before it snoozes, it checks, in the transaction that
  snoozes it, that nothing woke it meanwhile. Each instance of a matrix job
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
  gives up on fails, and the run goes on to its end.

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
  of the job's group ([Placement](#placement)). The coordinator runs Ready
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
fix ([The workflow format](#the-workflow-format)) is open: of the 221
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
  cancels the remaining instances when one fails, and places no more than
  `max-parallel` at a time. The strategy is evaluated as one value, so they
  are as private as the matrix's values: of a matrix whose instances are
  private, the coordinator reads them from the secret record, as run state
  doesn't hold them (`plan.Expansion.Split`).
- **Outcome and outputs.** The job's outcome is its instances' combined, as
  `needs.<job>.result` gives it ([Contexts](#contexts)). Its outputs are
  its instances', by name, and an output that more than one instance sets
  has the value of the one with the highest index, so it doesn't depend on
  which finished last, as it does in ACTIVATE and GitHub Actions. No
  workflow can rely on which instance finishes last, so this is made as a
  bug fix ([Bug fixes to format 1](#bug-fixes-to-format-1)). In an
  instance's own steps and outputs, `needs.<job>` is the instance, whose
  outputs are those its own steps wrote, another such fix. As format 1
  reads each instance of a job its dependents need, the job fails for them
  when an instance was skipped because its needs failed, whatever the rest
  did, and is cancelled for them, for `cancelled()`, when an instance was
  cancelled, though another failed.
- **`needs: <job>:any`** waits for any one instance, and `needs.<job>` is
  then the first instance to succeed, or to be skipped by its own `if:`, or
  else, once every instance has ended, a failed one, as format 1 waits
  ([On hopper](#on-hopper)).
- **Size.** A matrix has at least one instance and at most 256 by default,
  a limit of Meridian's ([Compiling](#compiling)), not the format's. A
  matrix with no instance, or more than the limit, fails where it is
  expanded: at Start, which fails the run, for one whose values are known
  by then, and at Ready, which fails the job, for the rest.

### Workflows a step calls

A step whose `uses:` names a workflow runs it as a run of its own, a child
of the step's run, with its own plan, secret record and jobs. A `uses:` names
a workflow when it starts with `github/`, `gitlab/`, `marketplace/` or
`workflow/`. ACTIVATE also runs a repository's URL as a workflow, and
refuses any other prefix, where Meridian's schema refuses a URL and takes
any other `uses:` for a built-in action; no known workflow has either.

- **Pinned when the run is created.** The compiler resolves each workflow
  the run's own steps name through the Sources interface, each reference
  once, pins it to a commit and the SHA-256 digest of its text
  (`Step.Source`), and loads and validates it. The plan holds every pinned
  reference, not a compiled workflow, so the run and its reruns run the
  same code throughout, even where a reference names a branch. Its `with:`
  `$yaml` and `$host`, which say where the workflow is, must be written out
  (`source_not_known`); an empty `$yaml` means `workflow.yaml`. The
  workflows a pinned workflow calls in turn are pinned by the child run's
  own Create, as it is created, so a cycle, or calls nested too deep, is
  found only then. Pinning a run's descendants all at once, with cycle
  detection and a depth limit (`Options.Limits`), needs the plan to hold
  their pins, by the pinned workflow and the step that names each, for
  each child run's Create to read rather than resolve; it isn't built.
- **Compiled when the step runs.** The worker evaluates the step's `with:`,
  and the coordinator creates the child run from the pinned workflow with
  those values, through Create and Start, linked to the step. The step ends
  with the child run's outcome.
- **Its own scope.** A child run reads its own inputs, and the variables its
  own Start reads. A value the step passes that was made from a variable or
  a password is kept out of the child's plan, in its secret record, as a
  password is.
- **Without a Sources resolver**, a `uses:` that names a workflow fails to
  compile (`resolver_missing`, [The workflow format](#the-workflow-format)).
  A `uses:` names a workflow when it starts with `github/`, `gitlab/`,
  `marketplace/` or `workflow/`; any other names a built-in action.

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
  (`plan.ReadyStepRetry.CleanOnRetry`). As in format 1, it cleans only a
  `uses:` step's directory before its next try, and never one that is the
  parent run's directory or log directory, or holds either; a worker that
  runs `uses:` steps forces it off for those.
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

A person reruns a run with `meridian rerun`, either all of it or only its
failed jobs and the jobs that depend on them. A rerun uses the run's stored
plan, so it runs the same revision, with the same workflows pinned, and the
jobs it doesn't rerun keep their outcomes and outputs.

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
connection, is answered as the first was.

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
  side reads the other's clock; each only measures the period.
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
connection that is down, which counts as not lost. Once the backend says
the worker has ended without completing its attempt, the attempt ends
with the reason `worker-lost` if a step had started, and otherwise as a
failure before its first step, which is placed again
([Failures and retries](#failures-and-retries)); or with `timeout` when a
scheduler ended it at its time limit, and `out-of-memory` when a scheduler
or the kernel ended it for its memory.

When a replica takes over what workers connect through
([Connections](#connections)), as `meridian run` or `resume` does after
another stopped, and when a replica sees its wall clock jump more than 10
seconds past its monotonic clock, as when a laptop wakes, it counts no
lapsed lease until the grace has passed. Jobs keep running while Meridian
is down, but for those that fence themselves, and a cut-off worker tries
again at least every 30 seconds, so it reaches the replica at least twice
within the grace: a laptop waking up or `meridian run` restarting doesn't
make them all look lost.

### Cancellation, timeouts and cleanup

A run stops early when a person cancels it, or when it runs past its own
`timeout:`, and does what format 1 does in each case.

- **Stopping a run is stored first**: the stop record, when and why, in
  the run's row, in one transaction that also tells every running
  attempt, through its control poll, and wakes every job's supervisor. A
  placement that no worker has claimed yet is withdrawn (`scancel`,
  `qdel`, deleting the pod, signalling the process), and its job ends
  cancelled, whatever its `if:`: the job was readied, so its needs had all
  ended, and format 1 would be running it, and would stop it as below. A
  running attempt is waited for while it renews its lease,
  since its cleanups have no timeout, and withdrawn as a placement is if
  its lease lapses. `meridian cancel` stores the record whether or not a
  coordinator is running, and withdraws placements itself when none is.
- **A running job, when a person cancels the run**, has the step it is
  running stopped: SIGTERM, then SIGKILL after a grace period. The step
  and the job end cancelled, and the job's later steps are skipped, but
  for those whose `if:` outlives a cancel; the cleanup of each step that
  ran still runs (below).
- **A job not yet readied**, still waiting for its needs, takes one of
  three paths, by its `if:`:
  - **One that is cancel-aware**, one that calls `always()` or
    `cancelled()`, negates `success()` or `failure()`, or is the word
    `always`, waits until its needs have ended. Its `if:` is then
    evaluated with the status of its needs alone, as a job's always is:
    `cancelled()` is true when a job it needs ended cancelled, and
    `failure()` when one failed, or was skipped because its own needs
    failed. If it is true, the job runs.
  - **One that calls a status function, or reads `needs`**, and isn't
    cancel-aware, waits until its needs have ended, then ends cancelled,
    without its `if:` being evaluated.
  - **Any other**, a job with no `if:` among them, ends at once, skipped
    because its needs failed, as format 1 ends a job whose dependency
    failed.
- **The run's outcome after a cancel** is cancelled, whatever its jobs did
  after it, an `always()` job that succeeds among them. It comes from the
  stop record, as format 1's comes from its record of an external
  cancellation, rather than from the jobs.
- **The run's own timeout isn't a cancel.** A job of the run's own waits
  for it, and then stores the stop record with the reason `timeout`. When
  the timeout is private, that job wakes at least every minute rather than
  at the timeout, so that hopper's schedule doesn't show it. The step
  running then fails with the reason `timeout`, and so does its job. Jobs
  not yet readied take the same three paths as after a cancel. The
  run's outcome is failure, with the reason `timeout`, which Meridian adds:
  format 1 records no reason.
- **A job's own timeout** ends only that job, and the worker enforces it.
  The job isn't cancelled: the step it was running fails, so `failure()`
  is true after it and `cancelled()` false, and the job fails with the
  reason `timeout`, as in format 1. The run goes on: the jobs that need it
  are skipped because their needs failed, and those whose `if:` calls
  `failure()` run. The coordinator enforces it too, as a backstop for a
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
  steps that haven't started don't run. A step whose `if:` isn't
  cancel-aware is skipped then, as `success()` is false; one that comes to
  run all the same, as when the cancellation arrives as it is readied, ends
  cancelled, or failed after a timeout, without starting. Format 1 starts
  such a step and stops it at once, so no workflow can rely on its running.
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
  fails, or a step of its own job did, as in format 1. The coordinator
  tells the step's worker when another job fails: the first job of the run
  to fail marks the run, in the transaction that ends it, and every running
  attempt's control poll then says so, as a new attempt's first one does.
  The step ends cancelled,
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
  Slurm or PBS job name, a pod label, a process's arguments), and before
  placing, the coordinator looks for one that already exists. A coordinator
  that crashed between submitting and recording never submits twice: a
  supervisor that another replica rescues finds the placement, and the
  job's fencing number keeps a second one from reporting.
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

`meridian serve` is a coordinator that keeps running, for example on a
cluster's login node, and `meridian run` hands its run to one if it finds
one using the same store.

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
   member wasn't.

A job moves to another member only when none of its code has run, or when
the workflow asked for it:

- **A failure before the first step starts** places the job again, on a
  member it hasn't failed on when there is one. When two members have
  failed a job for the same reason, the job fails: the job, not the
  members, is then the likelier cause.
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
  scheduler's queue holds the job, and stored only as a hash. It travels in
  the job's environment or in a file only the user can read, on a path no
  other user can change, never on a command line, where other users could
  see it in `ps`. On macOS it travels only in a file, which the worker
  removes as soon as it has read it: any process of the user can read the
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
so the credential never travels from the coordinator.

### The worker API

The API is HTTP with JSON bodies, versioned in its path (`/v1/`), with
errors as RFC 9457 problem details. Meridian's coordinator and a platform
that embeds Meridian serve the same API.

- **Claim:** present the bootstrap token and the worker's credential, and
  receive the job as Ready evaluated it, encoded as its plan is; the values
  of its own job's secrets and variables, and of the inputs Start completed
  that it reads; and its dependencies' outcomes, and the outputs of theirs
  that it reads.
- **Heartbeat:** renew the lease.
- **Control:** a long poll that delivers cancellation and `early-cancel`.
- **Step events:** a step started, or finished with its exit code.
- **Logs:** each step's output, uploaded as a stream with byte offsets, so
  an interrupted upload resumes without duplicating or losing lines.
- **Child runs:** start the run of a workflow a step calls, with its
  step's `with:`, and wait for its outcome
  ([Workflows a step calls](#workflows-a-step-calls)).
- **Complete:** the job's result, as End gave it: its outcome, its outputs
  and its steps', and which of them are private or hold a secret
  (`plan.JobResult`).

Every write is idempotent and carries a sequence number, so a worker
retrying after a dropped connection can't apply a report twice. A write
from a superseded attempt fails with a `fenced` problem.

The attempt credential is an opaque random token, stored as a hash, that
lasts until the attempt ends, so that a worker cut off from the coordinator
can still deliver what it spooled. For a job that fences itself it lasts
only while heartbeats renew it, and lapses with the lease. It lets its
holder do one thing: report on its own attempt. A step that reads it gains
nothing that its job couldn't already do, so on backends where steps and
their worker run as the same user, no further separation is needed.

The API's bodies are versioned by the run's plan version: a change to a
body is a change to the plan's encoding, and raises the plan's version, as
any other does ([Plans across versions](#plans-across-versions)). A field
added in a later version is left out when it is empty, and each side sends
it only for a run whose version has it, so each side reads the other's
bodies strictly, refusing a field it doesn't know. A worker says, as it
claims, the newest plan version it reads, and the coordinator refuses one
that doesn't read the run's before it sends anything of the run. A replica
answers a worker only for a run whose version it reads, as it claims only
such runs, and otherwise asks it to try again, which reaches another. The
claim's request comes before the worker knows the run's version, so it
never changes.

What the API means is pinned by a suite, `workerapitest.Conformance`, that
plays a worker against a coordinator over its Unix socket: the claim and
its credential, heartbeats, control polls, events, logs and the attempt's
end, with each refusal, fencing, strict decoding, and unknown paths and
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

One replica at a time holds what a member's workers connect through, such
as the forwarded socket or the relay, under a lease in the store, and
serves every attempt that reaches it there, of whichever job: any replica
can serve any job. It closes the connection only when it gives the lease
up. A replica that takes the lease over, once its holder has stopped
renewing it, sets the connection up again at the same address, so the
member's running workers reconnect to it. Locally, the socket in the state
directory is held under a lock on a file beside the store's database,
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
  is the run's ID and `PW_PARENT_JOB_DIR` its directory.
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
  (`ssh_host_empty`). A worker on the host a job's `ssh:` names fails such
  a step (`step_off_job_host`) rather than run it on the wrong host, as it
  fails one whose `ssh:` names a host of its own (`step_ssh_not_supported`),
  but for one whose `ssh:` names the same host and login as its job's, as a
  step that repeats its job's `remoteHost` does, which runs where its job
  runs: the same `remoteHost`, `remoteUser`, `jumpNodeHost` and
  `jumpNodeUser`, by which format 1 keys the connections it reuses, with
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
  is what SIGTERM and SIGKILL stop.
- **Logs** are masked on the worker, before they leave the host, and so are
  outputs ([Outputs](#outputs)).
- **While the coordinator is unreachable**, a worker keeps running its steps,
  spools logs to disk up to a limit, keeps events in memory, and delivers
  them in order when it reconnects, unless its job fences itself
  ([Leases and fencing](#leases-and-fencing)). Past the limit, or once the
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
rather than run them on the host, and a step that `uses:` an action or a
workflow, or runs on another host with `ssh:`, fails, as does one whose
`ssh:` is null, or gives no `remoteHost`, on a worker that a job's `ssh:`
placed on a host. So a step's `clean-on-retry:`, which cleans a `uses:`
step's directory, has nothing to clean yet. Its events are kept
in memory while the coordinator is unreachable, since each is small but for
a step's outputs, which are capped.

## Backends and isolation

| Backend | Starts a worker | Default isolation |
| --- | --- | --- |
| `local` | As a process detached from the terminal | A rootless container |
| `ssh` | Over SSH, detached so it outlives the connection (`systemd-run --user` where there is one) | A rootless container |
| `kubernetes` | As a pod, through the Kubernetes API | A rootless container: the pod, run as a non-root user |
| `slurm`, `pbs` | As a batch job, submitted on the login node | The host |

A member can choose stronger isolation, such as a microVM, or name host
isolation explicitly. It never gets weaker isolation than it asked for: when
a member's runtime is missing, jobs fail there with an error that says so.
Until Meridian drives container runtimes, a member that doesn't name host
isolation, as the built-in `local` group's does, runs no job: a job that no
other member can take fails, with that member's reason among the rest,
rather than run on the host.

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
- **SSH host keys are checked** against the user's `known_hosts`. An unknown
  key is an error until the user accepts it explicitly; Meridian never
  accepts a key on first use by itself.
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
(`fips_module_not_validated`); `worker.Run` checks both too, whatever
program calls it, as every entry point of Meridian's must
(`internal/fipsmode`).

- **TLS 1.3**, through `crypto/tls`, with the algorithms FIPS mode allows.
  The coordinator's certificate uses an ECDSA P-256 key.
- **SSH**, through `golang.org/x/crypto/ssh`, which negotiates only approved
  algorithms in FIPS mode. A server that offers none of them can't be used.
- **Secret records** are encrypted with AES-256-GCM, through `crypto/cipher`
  with random nonces (`cipher.NewGCMWithRandomNonce`), the way the module
  approves, each under a data key of its run's own from `crypto/rand`, and
  bound by its associated data to its store, its run and its generation.
  A platform's Keys interface wraps the data keys with its own key
  management, and the local one with AES-256-GCM under a key in a file.
- **Tokens** come from `crypto/rand` and are stored as SHA-256 hashes;
  digests are SHA-256.
- **External tools** that Meridian runs (`git`, container runtimes,
  schedulers) use the host's cryptography, which a FIPS host configures for
  the whole system.

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
| `meridian worker`, `meridian relay` | Started by backends, not by people. |

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
elsewhere. `--run-dir` names another.

`meridian run` exits 0 when the run succeeded; 1 when the workflow has
errors, or the run failed or was cancelled; and 2 when Meridian couldn't do
what it was asked, and when it leaves the run running, as when it is
interrupted ([The coordinator](#the-coordinator)).

Execution groups, variables and the GitLab servers a workflow can name
live in Meridian's config directory, and `--group` picks a group. A
built-in `local` group needs no config: one member, this machine, which
names host isolation explicitly, and which a group of the same name in
the config replaces. Groups are never read from the repository a workflow
comes from, so cloning a workflow can't send its jobs to your SSH hosts.

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

Meridian refuses any of these files if another user can change it, and the
variables file if another user can read it, since people keep secrets in
plain variables too. A file that holds a variable's value must be the
user's, and only they may change it, or read it when it holds a secret's,
and the directories on the way to it are checked as Meridian's own are.
Meridian reads none of these files, nor its key, through a symbolic link at
the file's own path, so that what it reads is the file the path names: a
directory on the way to one, the config directory among them, can be a link,
and is checked through it.

The database, the key that wraps secret records, and logs live in
Meridian's state directory, which only its user can read. SQLite can't lock
reliably on a network filesystem, which is where home directories on
clusters often are, so Meridian refuses to open a database there and says
how to choose a local path: one that only the user can change, and that
lasts, since it holds the database and the key, such as one that
`mktemp -d -p /var/tmp` makes, but not one in `/tmp`, which a reboot can
empty. On Linux the directories are the XDG base
directories' `meridian`, as `~/.config/meridian` and
`~/.local/state/meridian`; on macOS both are
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
- **Steps that run a workflow.** A job made only of `uses:` steps could run
  on the coordinator, with no worker. In a job that mixes them with `run:`
  steps, the worker waits for the child run
  ([Workflows a step calls](#workflows-a-step-calls)), holding capacity
  while it does.
- **The Kubernetes client.** `client-go` is a large dependency for a few API
  calls; plain HTTP against the API may be enough.
- **Git.** Running the `git` command uses the host's credentials and
  configuration, but its cryptography is outside Meridian's FIPS module. A
  pure Go client would be inside it, at the cost of a large dependency.
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
