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
| Secret record | A run's passwords and the variables it read, encrypted and kept only until the run ends. |
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
  plan + passwords ───▶ store (hopper, on SQLite or Postgres)
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
| `coordinator` | Stores runs, schedules and places jobs, and serves the worker API. |
| `worker` | Runs one attempt of a job and reports on it. |
| `workerapi` | The worker API's paths, requests, responses and problems, which the coordinator serves and the worker calls. |
| `backend` | Execution groups and their members, and the interface that starts workers on a member. |
| `backend/…` | Start workers: `local`, `ssh`, `kubernetes`, `slurm`, `pbs`. |
| `runtime/…` | Drive container runtimes: Podman, Docker, Apptainer, Apple's `container`. |
| `cmd/meridian` | The command. |

The `meridian` command wires the packages together with local resolvers, a
SQLite store and a coordinator in its own process. A platform wires them
with its own resolvers, Postgres and as many coordinator replicas as it
needs. Nothing in the packages knows which of the two it is running in.

## The workflow format

The format is defined by a JSON Schema (2020-12) per format number, kept in
this repository. Format 1 is the format Parallel Works ACTIVATE runs.

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
  - a job's `retry:`, which can rerun a job whose worker was lost.
- **Metadata.** A workflow can carry `name`, `description`, `icon` and
  `thumbnail`. `description` is a string or `{file: <path>}`; `icon` and
  `thumbnail` are paths. Paths are relative to the workflow file and may not
  leave its directory tree, so a workflow in a Git repository is complete and
  can't read files outside it. The compiler doesn't check that yet.
- **Strictness.** A workflow that needs something the environment can't
  provide fails to compile: an input type that names platform resources, a
  `uses:` that names a workflow with no Sources resolver, sessions without a
  session provider. The error names the key and what would provide it.
  There is no flag to run the workflow anyway. The compiler doesn't yet
  refuse the last two.
- **What ACTIVATE ignores.** ACTIVATE has never checked workflows against
  the schema, so workflows that run there can have keys the schema doesn't
  define, which ACTIVATE ignores. In format 1, such a key is a warning, and
  is ignored. So is a repeated key, whose last value is used, and a second
  YAML document in a file. A name the schema doesn't allow, such as a job's,
  is still an error. A later format can make these warnings errors.

### Conformance

The conformance suite pins the format's meaning. Each case is a workflow,
or an expression, with the inputs and contexts it is given, and what it
loads, resolves or compiles to, the problems it has, or both. Cases come
from ACTIVATE's own tests, from public workflows and from Meridian. They
live in `conformance/`, in areas, and each package runs the cases of its
own: `format` for loading and validation, `expressions` for what
expressions evaluate to, `inputs` for a run's input values, and `compile`
for plans and the problems with jobs and expressions. Cases for the later
phases come with them: Start's values and its check of the run's shape,
matrix expansion, the shape of each context in each phase, and runs, with
the outcomes and outputs they must end with, status functions and step
retries among them.

Behavior that workflows rely on is kept, even where it is surprising. Behavior
no workflow could rely on, such as a crash or a silently wrong result, is
fixed, and its case records the fix.

## Phases

A run is evaluated at five points, its phases. Each phase is one function of
Meridian's packages, which computes what the workflow leaves to that point
from what is known by then. The coordinator and the worker only gather a
phase's inputs and keep what it gives, so every implementation, whether
`meridian`, a platform or an editor's WebAssembly, computes the same thing.

| Phase | When | Function | Contexts it adds | Evaluates |
| --- | --- | --- | --- | --- |
| Create | The run is created | `compile.Compile`, wherever the run is created | `inputs`; `org` and `vars`, for the run's shape; `remote`; `sessions` | The input values it can, and what decides the run's shape: an input's `hidden`, `ignore` and `optional`, and a job's `needs` |
| Start | The run starts | `compile.Start`, in the coordinator | `org` and `vars`, read once for the run, secret ones included; passwords; the workflow's `env` | The input values Create left, the workflow's `env:` and `timeout:`, and matrices whose values are known; the run's shape again |
| Ready | A job's dependencies have finished | `plan.Job.Ready`, in the coordinator | `needs` and the status functions, then `matrix` | A matrix that reads `needs`, then a job's `if:`, `runs-on:`, `resources:`, `retry:`, `timeout:`, `working-directory:` and `ssh:` |
| Step | A job starts on its worker, and each of its steps is about to run | `plan.ReadyJob.Step`, in the worker | `job`; the job's and the step's `env`; `steps` | The job's `env:`, then a step's `if:`, `run:`, `with:`, `env:` and the rest of the step |
| End | A job's steps have run | `plan.ReadyJob.End`, in the worker | | The job's `outputs:` |

Each phase reads the contexts of the phases before it too.

- **Pure.** Create and Start reach files, networks and platforms only
  through resolvers ([Interfaces](#interfaces)), and Ready, Step and End
  reach nothing. Given the same inputs and resolver answers, a phase gives
  the same result, byte for byte.
- **One scope.** One function, `plan.NewScope`, builds the contexts an
  expression reads in each phase, and the compiler, coordinator, worker and
  WebAssembly package all use it, so a context has one shape everywhere
  ([Contexts](#contexts)).
- **What each gives.** Create gives the plan, the run's passwords and its
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

Only Create is built. Start, matrix expansion, Ready, Step and End are
designed here, and not built yet.

### Contexts

What an expression can read is its contexts, each a mapping:

| Context | Holds | Known from |
| --- | --- | --- |
| `inputs` | The run's input values ([Inputs](#inputs)): a group's under its name, a list's as rows. In input definitions, `inputs.x.type` and the like read `x`'s definition, and `[index]` is the index of a list's row. | Create; Start completes it |
| `org`, `vars` | Organization and user variables, by name, as strings. `var` is another name for `vars`. | Create, for the run's shape; Start, for everything else |
| `remote` | Where a fetched workflow came from, as strings, such as `remote.repo` and `remote.branch`. | Create |
| `sessions` | The workflow's `sessions:`, by name, as strings, as the session provider gives them. | Create |
| `env` | The workflow's `env:`, then the job's over it, then the step's over that, as strings. Each reads the ones under it, and the workflow's reads none. | Start, the workflow's; Step, the job's and the step's |
| `needs` | For each job this one needs: `needs.<job>.result`, its outcome; `needs.<job>.outputs.<name>`; and `needs.<job>.steps.<id>.outputs.<name>`, as strings. In the job's own steps and outputs, the job itself, whose outputs are its steps' so far. | Ready |
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
contexts those phases know ([Inputs](#inputs)).

The status functions are `success()`, `failure()`, `always()` and
`cancelled()` (also spelled `canceled()`), known from Ready on. A job's
default `if:` is `success()`, and an `if:` that calls a status function
replaces it. An `if:` whose value is the word `always` means `always()`: it
is the older spelling, which the compiler warns about, and a later format
can drop it.

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
   `uses:` names, with cycle detection and a depth limit
   ([Workflows a step calls](#workflows-a-step-calls)).
5. **Check matrices** with the function that expands them, without
   expanding them: their keys, the shape of `include` and `exclude`, and
   their size where their values are known ([Matrices](#matrices)). Link
   each step's `cleanup:` command to the end of its job.
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
     defining them. A step reading a step that doesn't exist or doesn't run
     before it, or a step's key other than `outputs`, is an error, since
     ACTIVATE fails the step; in a job's outputs, which read it as `""`, it
     is a warning.
   - Reading a variable that isn't a secret, or defining an input that takes
     text rather than a password, whose name looks like a secret's is a
     warning: a plain variable's value isn't masked, and a text input's is
     kept with the run. A name looks like a secret's when one of its words,
     split at `_`, `-`, `.`, spaces and changes of case, is `token`,
     `password`, `passwd`, `pwd`, `passphrase`, `secret`, `apikey`,
     `privatekey`, `accesskey`, `credential` or `credentials`, or two in a
     row are `api key`, `private key` or `access key`, so `HF_TOKEN` and
     `apiKey` do and `tokenizer` doesn't. A variable is warned about once,
     where it is first read.
7. **Emit** the plan, and beside it the run's passwords, which the plan
   doesn't hold.

The compiler does no I/O of its own: files, networks and platforms are
reached only through resolvers, which get the context the compiler is
given, so cancellation, deadlines and traces reach them. It is
deterministic, so the same workflow, inputs and resolver answers give a
byte-identical plan. That is also what lets it build to WebAssembly
unchanged (see [Editors and web UIs](#editors-and-web-uis)). Today
`Compile` takes no context, and doesn't return the run's passwords, which
only `ResolveInputs` gives.

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
every rerun, applies the same ones. Today the file's size and the nets are
fixed, there are no others, and the plan records none.

### Inputs

The compiler checks a run's input values against the workflow's input
definitions, under `on.execute.inputs`, and fills in what is missing, as
ACTIVATE's launch form and its backend do together:

- **Defaults.** An input with no value, or with `""` when `""` isn't one of
  its options, takes its default. A default of `""` gives no value. A
  default that is an expression is evaluated with the other inputs' values,
  defaults included, in whatever order they need; defaults that read each
  other are an error. A default reads what its paths can reach, however
  they are written: a `*` or a slice of a list reads its rows, `rows[00]`
  the row `rows[0]` does, and `rows[1:][0]` the row `rows[1]`. An index of
  a slice that counts from the list's end, as `rows[-1:][0]` is, reads any
  of the rows, since which one depends on how many there are.
- **Syntax.** Every expression in the input definitions must parse, in a
  default, an option, a label or anywhere else, whether or not the run
  evaluates it. A syntax error is reported once, where it is.
- **A form's text.** A form sends the text of an expression it can't
  evaluate itself, such as a default that reads a secret. A submitted value
  that is exactly the text of an input's default means the default. So does
  a string in a submitted mapping or list that is the text of an expression
  in the same place in its default, and a choice that is the text of one of
  its options' values, which means that option. Any other value holding
  `${{ }}` is data.
- **Lists.** A list's default is the rows it starts with. Each row, given or
  a default, is a group of the template's fields, which take their defaults
  as inputs do, in the same order as the rest; in a field's default,
  `[index]` is its row's index. A row's fields aren't checked against their
  types, but for a password field, which is checked as a `password`
  input is, and their conditions aren't evaluated.
- **Conditions.** `hidden`, `ignore` and `optional` are evaluated once the
  values are complete, and are true when they are true or the text `true`.
  One that reads what isn't known until the run starts, such as a secret,
  is an error, and so is one that reads an input whose value isn't known
  until then because a part of it reads such a thing. An input whose value
  reads only variables and passwords, which the compiler knows, can be
  read.
  An ignored input has no value, even one that was submitted. An input with
  no value and no default is required unless it is optional, or hidden or in
  a hidden group; a boolean with none is `false`. Every missing input is
  reported, not only the first.
- **Containers.** A group's values are under its name. A wizard step's
  inputs have their values beside the step's, unless `$meta.wizard.flatten`
  is `false`. A wizard step with `multi: true` repeats, as many times as the
  person running the workflow adds it: its value is a list under its name,
  one row for each time, whatever `flatten` says, as a form keeps it, and
  each row is a group of the step's inputs, as a list's row is of its
  template's fields. A repeated step given no value has none. A header has
  no value.
- **Types.** A number or a boolean given as text, such as `"5"` or
  `"true"`, is converted, and a text input given a number or a boolean gets
  its text. Anything else that doesn't fit the input's type is an error, as
  is a number outside its `min` and `max`, or a choice that isn't one of a
  dropdown's options when they are written in the workflow. A value for an
  input the workflow doesn't define is a warning that suggests the nearest
  name, and is kept, as ACTIVATE keeps it. A value that isn't JSON, such
  as NaN or text that isn't UTF-8, is an error, whether it was submitted or
  a default gave it, as `0/0` gives NaN and `1/0` an infinity: ACTIVATE
  can't encode such a run's values, and fails it as it starts. One in a
  list's rows is reported at the row's field.
- **Platform types.** Input types that name a platform's resources, such as
  clusters, Slurm partitions and buckets, need an Inputs resolver, which
  checks the value and gives the run the resource's details. Without one, an
  input of such a type that has a value is an error.
- **What waits for Start.** A default that reads something not known when
  the run is created, such as `env` or a secret variable, stays an
  expression in the plan, beside the input definitions. Start evaluates it,
  then checks it as Create checks the others: against its input's type,
  whether it is required, and with the Inputs resolver for a platform type
  ([Starting a run](#starting-a-run)). An input's value stays whole, even if
  only part of it reads such a thing: a part of a mapping or list default, a
  form's text, or a row's default, whose `[index]` is kept in the plan. The
  secret variables it reads are among the run's secrets.
- **What definitions read.** Input definitions are evaluated at Create and
  Start, so they read `inputs`, `org`, `vars`, `remote`, `sessions` and the
  workflow's `env`, and nothing later: reading `needs`, `steps`, `matrix`,
  `job` or a status function in one is an error. Start evaluates the
  workflow's `env:` with the inputs, in the order they need, so a default
  can read `env` and an `env:` value an input; a default and an `env:` value
  that read each other are an error, as two defaults that do are. Today the
  compiler accepts a later context in a definition, and holds its input for
  a start that can't evaluate it.
- **Variables.** A plan never holds an organization or user variable's
  value, secret or not ([The plan](#the-plan)). A default that reads `org`
  or `vars`, or a part of an input's value that reads one, stays an
  expression in the plan in the same way, its input's whole value, and is
  evaluated at Start. The compiler evaluates it all the same, since what
  decides the run's shape reads its value: the inputs' `hidden`, `ignore`
  and `optional`, and the jobs' `needs`. It checks it against its input's
  type too, but a problem with it doesn't show the value.
- **Passwords.** A plan never holds a `password` input's value, wherever
  the input is: on its own, in a group or a wizard step, or a field of a
  list's or a repeated step's rows. The value is one of the run's secrets,
  which the plan names by its path in the values, such as
  `inputs.creds.0.pass` for the field `pass` of the list `creds`'s first
  row, and it is checked against its type, but a problem with it doesn't
  show it. A value made from a password counts as the password, as one made
  from a variable counts as the variable: a default that reads one stays an
  expression in the plan, and is evaluated at Start. A list held for Start
  holds its rows without their passwords' values, which Start puts in their
  places. A list whose rows are evaluated at Start, with a password field in
  them, is one of the run's secrets as a whole, since the plan can't say
  which of its values are passwords.

ACTIVATE differs where its values can be incomplete, as they are from its
API: it evaluates conditions and defaults against the submitted values
before it fills in defaults, so a condition can read an input's definition
in place of its default. It doesn't check values against their types. Its
form keeps a repeated step's value as rows, as Meridian does, but its
backend fills in the step's inputs as an ordinary step's, and drops the
rows.

## Starting a run

Start completes what the plan leaves to the run's start, with what only the
start has: the variables, read once for the run, and the run's passwords.
It is `compile.Start`, which reuses the compiler's input resolver, and the
coordinator runs it as the run's first hopper job, which every other job of
the run waits for ([On hopper](#on-hopper)).

1. **Read.** Read the variables the plan lists through the Variables
   interface, by name, or a context's whole when the plan names it alone,
   and take the run's passwords from its secret record.
2. **Check the shape.** Evaluate again each decision of the run's shape
   that read a variable: an input's `hidden`, `ignore` and `optional`, and a
   job's `needs`. If one differs from what the plan records, the run fails
   with `run_shape_changed`, rather than run with values from two readings.
3. **Complete the inputs.** Evaluate the inputs the plan holds for Start,
   and the workflow's `env:` and `timeout:`, against the plan's input
   definitions, so relative references, reads of a definition and lists'
   templates work as they do at Create. Each input's value is checked as
   Create checks one: its type, whether it is required, and a platform
   type with the Inputs resolver.
4. **Expand** each matrix whose values are known by now, written out or
   read from inputs or variables ([Matrices](#matrices)), so a matrix that
   can't be expanded fails the run before any job runs.
5. **Make the sessions** of the workflow's `sessions:`, through the
   Sessions interface, under the names Create gave them. Create only
   checks them, since a run that is created may never start, and making
   them again for the same run makes nothing more.
6. **Keep** the variables it read in the secret record, and what it
   evaluated there or as run state, by whether evaluating it read a
   variable or a password ([Phases](#phases)), and its diagnostics with the
   run, all in the transaction that ends its hopper job.

A coordinator that dies during Start leaves its job to another replica,
which starts it again from the beginning: nothing reads Start's results
until they are stored. A rerun runs Start again ([Reruns](#reruns)).

## Run data

A run's data is of three kinds, kept apart by who may read them:

| Kind | Holds | Read by | Kept |
| --- | --- | --- | --- |
| [The plan](#the-plan) | What the run does, as the workflow says it, with no secret and no variable's value | Whoever can see the run | With the run |
| [The secret record](#the-secret-record) | The run's passwords, the variables it read as it started, and what Start made from them | The coordinator, and each attempt the part its job reads | Until the run ends |
| [Run state](#run-state) | Jobs, attempts, outcomes, outputs and diagnostics | Whoever can see the run | With the run |

What Meridian makes from a variable or a password is kept only in the
secret record, so the plan and run state can be shown to whoever can see the
run. A job's outputs and logs hold what its steps write, and Meridian masks
only secrets in them ([Outputs](#outputs)), so a plain variable's value
that a step writes stays as it is
([Variables and secrets](#variables-and-secrets)).

### The plan

The plan is typed Go, encoded as JSON and stored with the run, and doesn't
change once the run is created. A coordinator schedules from the plan and
the secret record alone, and a run resumed by another process or release
continues from the same plan.

It holds:

- **The run:** the workflow's source and revision, its format and metadata,
  the Meridian release that compiled it, the input values known when the
  run was created, the inputs left to Start, the workflow's `env`, the run's
  timeout, `remote` and `sessions`, the decisions of its shape that read a
  variable, and the secrets and variables Start reads. A revision names the
  exact text that ran: a Git commit, or the SHA-256 digest of the text when
  no commit has it, as for a file with uncommitted changes or a workflow a
  platform stores.
- **The input definitions**, `on.execute.inputs` as the workflow writes
  them, with their expressions parsed and no values, so that Start
  evaluates what was left to it as Create would have.
- **Jobs**, one per job: dependencies (on all instances of a job, or `:any`
  of them), its `strategy:`, its `if:`, group, resources and `retry:`,
  steps, outputs, timeout and `env`, and the secrets and variables it reads.
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
secrets the run reads, the run's for Start and each job's for its attempts:
the secret variables an expression reads, each of a context's when it reads
every variable of the context, as `vars.*` does; and passwords, by their
paths in the inputs' values, `password` inputs and the password fields of
lists' rows, and lists whose rows hold one and are evaluated at Start
([Inputs](#inputs)). Their values are in the secret record.

Nor does the plan hold the value of an organization or user variable, secret
or not, or a value made from one. People keep secrets in plain variables
too, and a plan is kept with every run's record, where whoever can see the
run can read it. So what reads a variable stays an expression: an input's
default, as the run's deferred inputs, and a matrix, as its job's strategy.
The plan names the variables the run reads that aren't secrets, for Start
and for each job, as `org.<name>` and `vars.<name>`, found in every
expression it holds; a read of each variable of a context, as `vars.*` is,
names the context, `org` or `vars`. (In format 1, `vars` alone is a word,
not a read of the context.)

What decides the run's shape still reads variables when the run is created:
an input's `hidden`, `ignore` and `optional`, and a job's `needs`. The plan
holds only what they decided, which inputs the run has and which jobs each
job waits for, and Start decides them again to check that nothing changed
([Starting a run](#starting-a-run)). That can say something of a variable's
value, such as that it names a job, which is accepted: the plan has to hold
the run's shape.

A source's location names the workflow to whoever can see the run, so it
says nothing of the machine the run was created on: a platform gives its
own name for the workflow, and `meridian` gives the workflow's path in its
Git repository, or its file's name outside one.

Today the plan doesn't hold the input definitions, `remote`, `sessions`, the
release, the shape's decisions, or each job's secrets and variables.

### The secret record

The secret record holds what a run needs that the plan can't: its
passwords, from Create; the variables it read at Start, plain and secret;
and what Start made from them.

- **Encrypted.** The coordinator stores it beside the run, in the same
  database, encrypted with AES-256-GCM under a data key of its own, which
  the Keys interface wraps: a platform's key management service, or locally
  a key file only the user can read. The run's ID is its associated data,
  so a record can't be moved to another run.
- **Read in parts.** The coordinator reads it for Start and Ready, and gives
  each attempt, when it is claimed, the values its own job reads
  ([Variables and secrets](#variables-and-secrets)).
- **Deleted when the run ends.** A rerun starts again: it reads the
  variables again, and needs the passwords again, given again by the person
  running it or kept by a platform that chooses to.
- **Stored, not held in memory.** Coordinator replicas keep no state between
  calls, and `meridian resume` picks a run up in another process, so a run's
  secrets live where its state does, and the store holds them only
  encrypted.

### Run state

Run state is hopper's: the run's jobs, their attempts, outcomes and outputs,
beside Meridian's run record, which holds the plan and the run's
diagnostics. It holds no secret that Meridian can recognize: a job's
outputs are checked against the secrets its attempt received before they
leave the worker ([Outputs](#outputs)), and diagnostics never show a value
made from a variable or a password.

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
- **The version says who can read it.** The run record keeps its plan's
  version, so a replica tells whether it reads a run without decoding its
  plan. A replica claims only runs whose version it reads, so during a
  rolling upgrade old replicas leave newer runs alone. A release still
  refuses a plan with a key it doesn't know, rather than run it without
  that part. Keys match exactly, and a key given twice is refused.
- **What an expression means is its format's.** A stored tree is read the
  same in every version, and evaluated by the rules of the format the plan
  records.
- **The release.** The plan records the Meridian release that compiled it.

Today a plan is version 1, and has no schema.

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

Values that come from secrets are masked wherever they appear in logs,
including inside strings built from them. So are values made from them: an
evaluation can say whether it read a value its scope marks
(`Template.EvalMarked`), and one that read a secret is marked whatever it
made of it, whether `${{ vars.token }}`, `${{ vars.token * 2 }}` or a
condition the secret decided. The mark is conservative: anything a template
reads from a secret marks all of what it gives.

## Interfaces

Everything that differs between a laptop and a platform is behind an
interface, and the compiler, coordinator and worker see only the interface:

| Interface | Provides | Local implementation |
| --- | --- | --- |
| Inputs | Input types beyond the built-in ones, and their choices | Built-in types, plus Slurm partitions, accounts and QOS, and PBS queues, read from a member's scheduler |
| Variables | Organization and user variables, plain or secret, by name | A variables file in Meridian's config directory, in which a secret names the environment variable or file that holds its value |
| Keys | The key that wraps each run's secret record's data key | A key file in Meridian's state directory, which only the user can read |
| Groups | Execution groups | The user's config |
| Sources | Workflows that `uses:` names, pinned and read by their pins | Git, through the `git` command |
| Actions | Built-in actions | `parallelworks/checkout`, and those that need no platform |
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
reaches each reader in their own language. Today the Inputs resolver
takes no context and returns a Go error, and the compiler uses none of the
others: it is given variables as mappings, in `Options.Contexts`.

## Durable execution

### On hopper

Durable state is [hopper](https://github.com/parallelworks/hopper)'s.
Meridian keeps no scheduling state of its own, and only adds run records
that hopper doesn't model, such as the plan and the secret record, in the
same database.

- **A run is a hopper workflow.** One transaction stores the run record and
  the secret record, with the run's passwords, and inserts the workflow, its
  jobs and their edges, so a run is stored before anything executes, and
  either all of it is stored or none of it is. Its first job is Start
  ([Starting a run](#starting-a-run)), which every other job waits for.
- **Each job is a hopper job**, and `needs:` becomes hopper's dependency
  edges. A matrix job's instances are a hopper workflow of their own
  ([Matrices](#matrices)).
- **Meridian decides what a dependency's outcome means.** Its edges let a
  dependent run whatever its dependencies' outcomes were. A job ends with
  Meridian's outcome (success, failure, cancelled or skipped) stored with its
  outputs, and the coordinator evaluates the dependent's `if:` against it.
  hopper cancels nothing on Meridian's behalf.
- **`needs: <job>:any`** waits for any one instance of a matrix job to
  complete or be skipped. This needs an any-of edge, which hopper doesn't
  have yet.

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
  of the job's group ([Placement](#placement)).
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
Which one format 1 keeps is decided by a scan of the known workflows, as a
bug fix is ([The workflow format](#the-workflow-format)). Each instance's
key is the job's name, `#` and its index from 0, as in `build#2`.

- **Where.** The coordinator expands every matrix: at Start, one whose
  values are known by then, written out or read from inputs or variables,
  and at Ready, one that reads `needs`. The compiler expands none. It checks
  each with the same function where its values are known, and checks that
  the jobs and outputs a matrix reads exist; only the values wait. Today the
  compiler keeps every `strategy:` in the plan, and nothing expands one.
- **A matrix job's `if:`** that doesn't read `matrix` is evaluated before
  the matrix is expanded, so a job it skips has no instances. One that
  reads `matrix` is evaluated for each instance: format 1 allows it, as
  GitHub Actions doesn't, and the compiler accepts it.
- **One hopper job.** A matrix job is one hopper job, whichever phase
  expanded it. When it is ready, the coordinator inserts its instances as a
  hopper workflow of their own, and finishes the job once they have all
  finished, with their combined outcome and outputs. Jobs that need it wait
  for all its instances. So a matrix written out and one sized from earlier
  results run the same way.
- **`fail-fast` and `max-parallel`** belong to the strategy: the coordinator
  cancels the remaining instances when one fails, and places no more than
  `max-parallel` at a time.
- **Outcome and outputs.** The job's outcome is its instances' combined, as
  `needs.<job>.result` gives it ([Contexts](#contexts)). Its outputs are
  its instances', by name, and an output that more than one instance sets
  has the value of the one with the highest index, so it doesn't depend on
  which finished last, as it does in ACTIVATE and GitHub Actions. No
  workflow can rely on which instance finishes last, so this is made as a
  bug fix ([The workflow format](#the-workflow-format)).
- **`needs: <job>:any`** waits for any one instance, and `needs.<job>` is
  then that instance's outcome and outputs ([On hopper](#on-hopper)).
- **Size.** A matrix has at least one instance and at most 256 by default,
  a limit of Meridian's ([Compiling](#compiling)), not the format's. A
  matrix with no instance, or more than the limit, fails where it is
  expanded, or when the run is created if its values are known then.

### Workflows a step calls

A step whose `uses:` names a workflow runs it as a run of its own, a child
of the step's run, with its own plan, secret record and jobs.

- **Pinned when the run is created.** The compiler resolves the workflow
  through the Sources interface, pins it to a commit and the SHA-256 digest
  of its text, and loads and validates it, and the workflows it calls in
  turn, with cycle detection and a depth limit. The plan holds every pinned
  reference, not a compiled workflow, so the run and its reruns run the same
  code throughout, even where a reference names a branch.
- **Compiled when the step runs.** The worker evaluates the step's `with:`,
  and the coordinator creates the child run from the pinned workflow with
  those values, through Create and Start, linked to the step. The step ends
  with the child run's outcome.
- **Its own scope.** A child run reads its own inputs, and the variables its
  own Start reads. A value the step passes that was made from a variable or
  a password is kept out of the child's plan, in its secret record, as a
  password is.
- **Without a Sources resolver**, a `uses:` that names a workflow fails to
  compile ([The workflow format](#the-workflow-format)). Today the compiler
  keeps it in the plan as it is written, without resolving it.

### Failures and retries

Meridian never runs a step again unless the workflow asks it to. Steps are
not assumed idempotent.

- **Before the first step starts**, a failure retries automatically, with
  backoff and a bound: placement failed, the scheduler rejected the job,
  the image couldn't be pulled, or the worker never connected. None of the
  workflow's code has run, so the retry can go to another member of the
  job's group ([Placement](#placement)).
- **The worker records that the first step is starting**, and waits for the
  coordinator to store that, before it runs it. Whether user code may have
  run is therefore always known.
- **Once a step has started**, a lost worker fails the job, with the reason
  `worker-lost`, unless the job's `retry:` lists that reason
  ([Job retries](#job-retries)).
- **A step's `retry:`** reruns that step on the same worker when it fails,
  as in format 1. It can't help when the worker itself is lost.
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
plan, so it runs the same revision, with the same workflows pinned and the
same inputs, and the jobs it doesn't rerun keep their outcomes and outputs.
It starts again: Start reads each variable's value as it is then
([Variables and secrets](#variables-and-secrets)), and needs the run's
passwords again, given again by the person rerunning it or kept by a
platform that chooses to. A rerun whose shape would now differ fails with
`run_shape_changed`. Each attempt of a job, and each retry of a step, keeps
its own logs, so earlier attempts can still be read.

### Variables and secrets

A run reads organization and user variables through the Variables
interface, by name, plain or secret, and keeps their values only in its
secret record ([Run data](#run-data)).

- **At Create**, the compiler reads only the plain variables read by what
  it evaluates: what decides the run's shape, and the values of inputs that
  read one, which it checks against their types. It reads no secret
  variable's value; what reads one waits for Start.
- **Once at Start**, the coordinator reads every variable the plan lists, a
  context's whole when the plan names it alone, and keeps them in the
  secret record, so every expression of the run reads the same values.
- **Each job gets its own.** The plan lists, for each job, the secrets and
  variables its expressions read, directly or through an input made from
  one, and an attempt receives only its own job's when it is claimed. A
  read of each variable of a context, as `vars.*` is, lists all of them.
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
- **What a script does with a value can't be followed.** A secret that a
  step's script transforms, such as by encoding it in base64, isn't known
  to come from the secret, and isn't masked.
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
grows with every attempt. The worker renews it with heartbeats, through
whichever coordinator replica it reaches, and every write it makes (step
events, logs, outputs, the result) carries the attempt's credential, which
names the attempt, and is stored only while the attempt holds the job's
fencing number. A write from a superseded attempt fails, and the worker
that made it stops its job. hopper's leases today belong to the client
that claimed a job; per-job leases are something it doesn't have yet.

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
  as `workerapi.Claim.SelfFencing` says. It shortens the grace period of a
  cancellation in progress to do so. A process that left its group, as
  with `setsid`, isn't its to stop.
- **The coordinator waits** until the lease has lapsed for that period and a
  margin, by the store's clock, before it starts a new attempt. Neither
  side reads the other's clock; each only measures the period.
- **The cost** is that such a job stops when it is cut off from every
  coordinator replica for longer than a lease period, even when its host is
  fine. Its new attempt resumes it, as its `retry:` asks.

Other jobs keep running while they are cut off, since nothing runs them
again: their workers spool reports and deliver them when they reconnect
([Running steps](#running-steps)).

A worker is lost only when its backend says it is, or, for a job that
fences itself, once its lease has lapsed for the period and the margin. A
backend check that can't decide, such as one over an SSH connection that is
down, counts as not lost.

When the coordinator comes back after being down, its workers get a grace
period to reconnect before their attempts count as lost. Jobs keep running
while Meridian is down, but for those that fence themselves, and a laptop
waking up or `meridian run` restarting must not make them all look lost.

### Cancellation, timeouts and cleanup

- **Cancelling a run is stored first.** Jobs that haven't been placed are
  cancelled in the store; workers are told to stop and send each running
  step SIGTERM, then SIGKILL after a grace period; placements in progress
  are withdrawn (`scancel`, `qdel`, deleting the pod).
- **Timeouts:** the worker enforces step and job timeouts. The coordinator
  enforces the run's timeout, and a job's as a backstop. As in format 1, a
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
  As in format 1, a job past its timeout isn't cancelled: `cancelled()` is
  false in its steps.
- **A later cancellation that gives less time**, or the attempt ending
  without the worker, makes SIGKILL sooner. When an attempt ends without
  its worker, every process group the attempt started is stopped, those of
  steps that have ended and left processes behind among them.
- **`early-cancel: any-job-failed`** stops a step when another job of the run
  fails, or a step of its own job did. The coordinator tells the step's
  worker. A step it stops is cancelled, and its job ends cancelled, as in
  format 1. It doesn't cut short the wait between a step's tries; a
  cancellation and the job's timeout do, and the step ends cancelled, or
  failed after a timeout.
- **A step's `cleanup:` command** runs at the end of its job if the step
  ran, whatever the job's outcome, in reverse order. As in format 1, it runs
  as a step of its own, with its step's shell, working directory,
  environment and `ignore-errors:`, but no timeout or retry, and a
  cancellation doesn't stop it; one that fails fails the job. A worker whose
  attempt ended without it, because another superseded it or its lease
  lapsed, runs none: another attempt may be running, and a cleanup could
  remove what it is using.

### Outputs

A step writes outputs to the file named by `$OUTPUTS`, a line `name=value`
each. As in format 1, a step with an `id` has a file of its own, whose
outputs are its `steps.<id>.outputs`, and the steps without one share the
job's, whose outputs are the job's, with its `outputs:` over them; each
step's result holds what it wrote, and End puts the job's together. As in
format 1, a file with a line of 64 KiB or longer is read as holding none,
and the step's event warns of it. A file is read only if it is a regular
file, not through a symbolic link, so that a FIFO there can't make the
worker wait. A job's outputs are capped at 1 MB and a run's at 50 MB, a
limit of Meridian's, which format 1 doesn't have: the worker reads no more
than 1 MiB of a step's file, and a step whose file is larger fails, but for
one without an `id` whose job's shared file was already larger, which can't
add to it. Before the worker reports a job's outputs, it checks them against
the values of the secrets its attempt received, and an output that holds one
is reported with it masked, `***`, as logs are, and a warning that names the
output; GitHub Actions leaves such an output out. When a job is claimed, it
receives its dependencies' outcomes and outputs with it, rather than reading
each one back. hopper keeps finalized jobs' outputs only in its history, so
this, too, needs something hopper doesn't have yet.

## The coordinator

The coordinator is a hopper client whose handlers keep no state between
calls: each reads the job's state, acts, and stores what it did. Any replica
can therefore handle any job, and a replica that dies loses nothing.

- **Placement is idempotent.** A placement carries its attempt's ID (in a
  Slurm or PBS job name, a pod label, a process's arguments), and before
  placing, the coordinator looks for one that already exists. A coordinator
  that crashed between submitting and recording never submits twice.
- **It places jobs** on the members of their groups
  ([Placement](#placement)), within each member's capacity and priorities.
- **It serves the worker API** (below).

Locally, `meridian run` coordinates in the foreground, with SQLite, and exits
when the run ends. If it is interrupted or exits early, it detaches from the
run rather than cancelling it: workers keep running and hold their reports,
and `meridian resume` picks the run back up. `meridian cancel` cancels.
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
   `resources:`, are left out. A job that no member could ever run is
   refused when the run is created, with each member's reason.
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
  the job's environment or in a file only the user can read, never on a
  command line, where other users could see it in `ps`.

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
  that it reads; and its dependencies' outcomes and outputs.
- **Heartbeat:** renew the lease.
- **Control:** a long poll that delivers cancellation and `early-cancel`.
- **Step events:** a step started, or finished with its exit code.
- **Logs:** each step's output, uploaded as a stream with byte offsets, so
  an interrupted upload resumes without duplicating or losing lines.
- **Child runs:** start the run of a workflow a step calls, with its
  step's `with:`, and wait for its outcome
  ([Workflows a step calls](#workflows-a-step-calls)).
- **Complete:** the outcome and the job's outputs.

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
member's running workers reconnect to it.

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
  masked. They go over the environment the workflow gives the step, as in
  format 1, and the worker's own over them. Without a platform, `PW_JOB_ID`
  is the run's ID and `PW_PARENT_JOB_DIR` its directory.
- **The shell** is bash, or sh, as `shell:` says: format 1 runs no other.
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
  spools logs and events to disk up to a limit, and delivers them in order
  when it reconnects, unless its job fences itself
  ([Leases and fencing](#leases-and-fencing)). Past the limit, or once the
  disk is full, a log keeps nothing more until there is room, and the
  step's event says how much it lost. A log's file holds what the
  coordinator hasn't stored, and, besides, at most four times as much, or
  16 KiB, of what it has.
- **What the worker keeps** of its own, its spooled logs, its steps'
  scripts and `$OUTPUTS` files, is in a directory only its user can read,
  under `TMPDIR` by default. A step that removes it, as `rm -rf
  "$TMPDIR"/*` does, removes only their names: the worker makes it again,
  and a problem it has with its own files fails a step, never the attempt.
- **It talks only to its coordinator.** Over a Unix socket, it checks that
  the process at the other end runs as its user, or as root, before it
  sends a token, where the system says (`SO_PEERCRED` on Linux), and
  elsewhere that the socket's directory is its user's and no one else can
  write to it; over TLS, the coordinator's key. It follows no redirect. On
  Linux it makes itself not dumpable, so that the user's other processes
  can't read its credential from its memory.

Today the worker runs steps only on the host. A job whose steps are to run
in a container fails before its first step, with a problem that says so,
rather than run them on the host, and a step that `uses:` an action or a
workflow, or runs on another host with `ssh:`, fails. Its events are kept
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
  secret variables are in its secret record, encrypted, until the run ends.
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
the use of a non-approved algorithm into an error.

- **TLS 1.3**, through `crypto/tls`, with the algorithms FIPS mode allows.
  The coordinator's certificate uses an ECDSA P-256 key.
- **SSH**, through `golang.org/x/crypto/ssh`, which negotiates only approved
  algorithms in FIPS mode. A server that offers none of them can't be used.
- **Secret records** are encrypted with AES-256-GCM, through `crypto/cipher`
  with random nonces (`cipher.NewGCMWithRandomNonce`), the way the module
  approves, under data keys from `crypto/rand`. A platform's Keys interface
  wraps them with its own key management.
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
  time. A job's outputs are kept until every job that depends on it has
  finished. A run's secret record is deleted when the run ends.
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

Execution groups and variables live in Meridian's config directory, and
`--group` picks a group. A built-in `local` group needs no config. Groups
are never read from the repository a workflow comes from, so cloning a
workflow can't send its jobs to your SSH hosts.

The database, the key that wraps secret records, and logs live in
Meridian's state directory. SQLite can't lock reliably on a network
filesystem, which is where home directories on clusters often are, so
Meridian refuses to open a database there and says how to choose a local
path.

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
- **What hopper adds.** Per-job leases with fencing and a grace period after
  a coordinator restart, an any-of edge, dependencies' outputs at claim, and
  a SQLite driver.
