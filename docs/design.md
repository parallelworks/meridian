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
| Execution group | A named source of capacity: an ordered list of members. |
| Member | One place a group can run jobs: a backend, its settings and the resources it offers. |
| Backend | How a member starts work: `local`, `ssh`, `kubernetes`, `slurm` or `pbs`. |
| Plan | The compiled form of a run: typed, checked and stored with the run. |
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
       compile ◀──── resolvers: inputs, secrets, groups, sources, …
          │
          ▼
        plan ───────▶ store (hopper, on SQLite or Postgres)
                            ▲
                            │
                       coordinator ──── backend ────▶ starts a worker
                            ▲                              │
                            └──────── worker API ◀─────────┘
                                                    runs the steps
```

Meridian is a set of Go packages:

| Package | Does |
| --- | --- |
| `format` | Loads workflow files and validates them against the schema of their format. |
| `expr` | Parses, checks and evaluates `${{ }}` expressions. |
| `compile` | Turns a workflow and its inputs into a plan, through resolver interfaces. |
| `plan` | The plan's types and encoding. |
| `coordinator` | Stores runs, schedules and places jobs, and serves the worker API. |
| `worker` | Runs one attempt of a job and reports on it. |
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
  the plan, coordinator and worker, sees one format. A feature a newer
  format removes survives in older workflows as its translation: if a later
  format drops `ssh:`, a format 1 job's `ssh:` becomes an inline execution
  group. Translations chain, so a new format adds one, from the format
  before it.
- **Migration.** `meridian migrate` applies the same translations to a
  workflow file, keeping its comments, and says what it can't change in the
  file, such as an execution group to configure.
- **Deprecations.** A key can be deprecated within a format. It keeps its
  meaning, the schema marks it `deprecated`, so editors flag it, and the
  compiler warns. Only a new format number removes it.
- **Additions.** Format 1 gains keys without a new number, because adding a
  key changes no workflow that was valid before:
  - `format:` itself;
  - workflow metadata (below);
  - `execution-group:` in `runs-on:`, beside format 1's `environment:`, the
    `execution-groups` input that chooses a group, and `resources:`, which
    says what a job needs from it;
  - `needs.*` in `strategy:` and `runs-on:`, for jobs sized from what earlier
    jobs found;
  - `needs.<job>.result`, a dependency's outcome;
  - a job's `retry:`, which can rerun a job whose worker was lost.
- **Metadata.** A workflow can carry `name`, `description`, `icon` and
  `thumbnail`. `description` is a string or `{file: <path>}`; `icon` and
  `thumbnail` are paths. Paths are relative to the workflow file and may not
  leave its directory tree, so a workflow in a Git repository is complete and
  can't read files outside it.
- **Strictness.** A workflow that needs something the environment can't
  provide fails to compile: an input type that names platform resources, a
  `uses:` source with no resolver, sessions without a session provider. The
  error names the key and what would provide it. There is no flag to run the
  workflow anyway.
- **What ACTIVATE ignores.** ACTIVATE has never checked workflows against
  the schema, so workflows that run there can have keys the schema doesn't
  define, which ACTIVATE ignores. In format 1, such a key is a warning, and
  is ignored. So is a repeated key, whose last value is used, and a second
  YAML document in a file. A name the schema doesn't allow, such as a job's,
  is still an error. A later format can make these warnings errors.

### Conformance

The conformance suite pins the format's meaning. Each case is a workflow and
its inputs with either the plan it compiles to or the errors it produces, and
some cases also run, with the outputs and outcomes they must end with. Cases
come from ACTIVATE's own tests, from public workflows and from Meridian, and
cover loading and validation, expressions, matrix expansion, `needs`,
defaults, inputs, status functions and step retries, including retries that
pick another host each time. They live in `conformance/`, and each package
runs the cases of its area.

Behavior that workflows rely on is kept, even where it is surprising. Behavior
no workflow could rely on, such as a crash or a silently wrong result, is
fixed, and its case records the fix.

## Compiling

The compiler turns a workflow, its inputs and its resolvers into a plan, in
order:

1. **Load.** Parse the YAML, keeping the source position of every node.
   YAML aliases and merge keys are refused, as ACTIVATE refuses them, and
   the file's size is bounded.
2. **Validate** against the schema of the workflow's format.
3. **Translate** a workflow in an older format into the current one.
4. **Resolve** through the resolver interfaces: check input values against
   their declarations and apply defaults, look up execution groups, and fetch
   and compile the workflows that `uses:` names, with cycle detection and a
   depth limit.
5. **Expand** matrices whose values are known and read no organization or
   user variable, and link each step's `cleanup:` command to the end of
   its job.
6. **Check expressions.** Parse every `${{ }}`, evaluate what is already
   known, and check every reference that is left, with its position:
   - Reading a context where it isn't known yet is an error, such as `steps`
     in a job's `if:`, or `needs` in the workflow's `env:`. So is a relative
     reference outside input definitions, since the plan doesn't hold the
     workflow it would read.
   - Reading a job's outputs is an error unless the job needs it, or is the
     job itself. A job's outputs are whatever its steps write, so their names
     aren't checked.
   - In format 1, reading an input, step, matrix key or session that doesn't
     exist is a warning: ACTIVATE reads it as null or `""` and runs, and a
     workflow a step calls can read inputs its caller passes without
     defining them. A step reading a step that doesn't run before it is an
     error, since ACTIVATE fails the step.
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
7. **Emit** the plan.

The compiler does no I/O of its own: files, networks and platforms are
reached only through resolvers. It is deterministic, so the same workflow,
inputs and resolver answers give a byte-identical plan. That is also what
lets it build to WebAssembly unchanged (see
[Editors and web UIs](#editors-and-web-uis)).

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

Limits on a run's size (jobs, matrix instances, expression depth, `uses:`
depth) are set by whoever runs Meridian. The defaults are high enough for
every workflow in the conformance suite.

### Inputs

The compiler checks a run's input values against the workflow's input
definitions, under `on.execute.inputs`, and fills in what is missing, as
ACTIVATE's launch form and its backend do together:

- **Defaults.** An input with no value, or with `""` when `""` isn't one of
  its options, takes its default. A default of `""` gives no value. A
  default that is an expression is evaluated with the other inputs' values,
  defaults included, in whatever order they need; defaults that read each
  other are an error.
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
  `"true"`, is converted, and a text input given a number gets its text.
  Anything else that doesn't fit the input's type is an error, as is a
  number outside its `min` and `max`, or a choice that isn't one of a
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
- **What the compiler doesn't have.** A default that reads something not
  known when the run is created, such as `env` or a secret variable, stays an
  expression in the plan, beside the inputs' values, and is evaluated as the
  run starts, then checked against its input's type as the others' values
  are. An input's value stays whole, even if only part of it reads such a
  thing: a part of a mapping or list default, a form's text, or a row's
  default, whose `[index]` is kept in the plan. The secret variables it
  reads are among the run's secrets.
- **Variables.** A plan never holds an organization or user variable's
  value, secret or not ([The plan](#the-plan)). A default that reads `org`
  or `vars`, or an input whose value reads one, stays an expression in the
  plan in the same way, its input's whole value, and is evaluated as the
  run starts. The compiler evaluates it all the same, since what decides
  the run's shape reads its value: the inputs' `hidden`, `ignore` and
  `optional`, and the jobs' `needs`. It checks it against its input's type
  too, but a problem with it doesn't show the value.
- **Passwords.** A plan never holds a `password` input's value, wherever
  the input is: on its own, in a group or a wizard step, or a field of a
  list's or a repeated step's rows. The value is one of the run's secrets,
  which the plan names by its path in the values, such as
  `inputs.creds.0.pass` for the field `pass` of the list `creds`'s first
  row, and it is checked against its type, but a problem with it doesn't
  show it. A value made from a password counts as the password, as one made
  from a variable counts as the variable: a default that reads one stays an
  expression in the plan, and is evaluated as the run starts. A list held
  for the run's start holds its rows without their passwords' values,
  which the run's start puts in their places. A list whose rows are
  evaluated as the run starts, with a password field in them, is one of the
  run's secrets as a whole, since the plan can't say which of its values
  are passwords.

ACTIVATE differs where its values can be incomplete, as they are from its
API: it evaluates conditions and defaults against the submitted values
before it fills in defaults, so a condition can read an input's definition
in place of its default. It doesn't check values against their types. Its
form keeps a repeated step's value as rows, as Meridian does, but its
backend fills in the step's inputs as an ordinary step's, and drops the
rows.

## The plan

The plan is typed Go, encoded as JSON and stored with the run. A coordinator
schedules from the plan alone, and a run resumed by another process or
release continues from the same plan.

It holds:

- **The run:** the workflow's source and revision, its format and metadata,
  the resolved inputs and the defaults evaluated as it starts, workflow
  `env`, the run's timeout, and the secrets and variables it reads. A
  revision names the exact text that ran: a Git commit, or the SHA-256
  digest of the text when no commit has it, as for a file with uncommitted
  changes or a workflow a platform stores.
- **Jobs**, one per job instance: dependencies (on all instances of a job,
  or `:any` of them), matrix values, the job's `if:`, its group, resources
  and `retry:`, steps, outputs, timeout and `env`.
- **Steps:** what they run (a command, a workflow, or a built-in action), and
  their `if:`, `env`, `with:`, shell, working directory, `retry:`, timeout,
  `ignore-errors`, `early-cancel` and cleanup.

Expressions are stored parsed, not as strings, with their source kept for
messages: each string in the workflow that holds a `${{ }}` is stored as its
text and the trees of its expressions. Anything that can't be known until
the run is underway, such as a job's `needs`, stays an expression to
evaluate later.

Everything else is data, and stays data. What a run was given, such as its
inputs' values and a matrix instance's, is stored as JSON that is never
evaluated. In the workflow's own values, a mapping whose only key starts
with `$`, as a stored expression's does, is stored marked as data. So nothing
a workflow or a run's data holds can become an expression when a plan is
read back, and an explicit `null`, such as a step's `working-directory:
null`, stays distinct from a value that isn't there.

Each job instance has a key: the job's name, or for a matrix instance its
name, `#` and its index, as in `build#2`. A matrix whose values come from
`needs` can't be expanded when the run is created, so the plan holds the
job once, with its strategy, and the coordinator expands it when the jobs it
needs have finished. A matrix that reads a variable, or an input whose
value reads one, is held the same way, and expanded as the run starts.

The plan holds no secret values, nor any value made from one. It names the
secrets a run needs, and each attempt receives them when it is claimed:
the secret variables it reads, each of a context's when it reads every
variable of the context, as `vars.*` does; and its passwords, by their
paths in the inputs' values, `password` inputs and the password fields of
lists' rows, and lists whose rows hold one and are evaluated as the run
starts ([Inputs](#inputs)). As the run starts, each password's value is
put in the inputs' values at its path, in those of the inputs evaluated
then too.

Nor does the plan hold the value of an organization or user variable, secret
or not, or a value made from one. People keep secrets in plain variables
too, and a plan is kept with every run's record, where whoever can see the
run can read it. So what reads a variable stays an expression: an input's
default, as the run's deferred inputs, and a matrix, as its job's strategy.
The plan names the variables a run reads that aren't secrets, as
`org.<name>` and `vars.<name>`, found in every expression it holds; a read
of each variable of a context, as `vars.*` is, names the context, `org` or
`vars`. (In format 1, `vars` alone is a word, not a read of the context.)

What decides the run's shape still reads variables when the run is created:
an input's `hidden`, `ignore` and `optional`, and a job's `needs`. The plan
holds only what they decided, which inputs the run has and which jobs each
job waits for. That can say something of a variable's value, such as that
it names a job, which is accepted: the plan has to hold the run's shape.

Plan encodings change the way the format does: by addition. A change that
can't be an addition gets a new plan version, and releases keep reading the
old one until no stored run can still use it. A release refuses a plan with
a key it doesn't know, which a newer release wrote, rather than run it
without that part. Keys match exactly, and a key given twice is refused.

## Expressions

Expressions are evaluated in five places, and what an expression can refer
to depends on where it is evaluated:

| When | Evaluated by | Adds these contexts | Used by |
| --- | --- | --- | --- |
| The run is created | Compiler | `inputs`, `org` and `vars` (organization and user variables), `remote` (the repository the workflow came from), workflow `env`, `sessions` from a session provider | Everything that is known by then and reads no variable, and what decides the run's shape, which can: an input's `hidden`, `ignore` and `optional`, and a job's `needs` |
| The run starts | Coordinator | `org` and `vars`, read once for the run, and the secrets the run needs | Input defaults the compiler leaves to it, such as those that read a variable, a secret or a password, and matrices that read a variable |
| A job's dependencies have finished | Coordinator | `needs`, status functions, `env` | Job `if:`, and `strategy:`, `runs-on:`, `resources:`, `retry:`, `timeout:` and `working-directory:` that use them |
| A step is about to run | Worker | `steps`, `job`, `env` | Step `if:`, `run:`, `with:`, `env:` and the rest of the step |
| A job ends | Worker | | Job `outputs:` |

The compiler reads `org` and `vars` for what decides the run's shape, and
to check the values of inputs whose defaults read them, but a plan holds
none of their values ([The plan](#the-plan)): everything else that reads one
is evaluated once the run is underway, with the values the run read as it
started ([Variables](#variables)).

`matrix` is known once a job's matrix is expanded: when the run is created,
or, for a matrix that reads a variable, when the run starts, or, for a
matrix that uses `needs`, when its dependencies have finished. A job's
`strategy:` is what is expanded, so it can't read `matrix`.

The status functions are `success()`, `failure()`, `always()` and
`cancelled()` (also spelled `canceled()`). A job's default `if:` is
`success()`, and an `if:` that calls a status function replaces it. An
`if:` whose value is the word `always` means `always()`: it is the older
spelling, which the compiler warns about, and a later format can drop it.

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

### The language

Format 1's expressions are the ones ACTIVATE evaluates, as its documentation
describes them: JavaScript's operators, with its rules for comparing and
converting values. The `expr` package parses them into a tree, which is what
the plan stores, and the conformance suite's `expressions` area pins what
each one means. [Expressions](expressions.md) gives their grammar, limits and
conversions in full.

- **Values** are JSON's: null, booleans, numbers, strings, lists and
  mappings. Numbers are 64-bit floats, as in JavaScript.
- **Literals** are numbers, strings in single, double or back quotes, `true`,
  `false`, `null`, and lists and mappings written as JSON.
- **References** read a context by path: `inputs.cluster.ip`,
  `inputs['my-key']`, `inputs.hosts[0]`, the slice `inputs.hosts[1:3]`, and
  `inputs.groups.*.name` for each group's name. A path can follow a group or
  a call too: `fromJSON(x).*.name`. A path that leads nowhere is
  null, except in `env`, `org`, `vars`, `remote`, `sessions` and outputs,
  which hold strings, where a missing name is `""`. A string that holds JSON
  is read as its value, since outputs are strings.
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
group is all of a run's input defaults and conditions, or every template in
one value, such as a step's `with:`. The net bounds what a group does
however many templates it has, and each of these caps, for every group, is
at least one template's limit, so a template on its own that keeps to the
language's limits never reaches it. A group that passes one fails with
`expressions_build_too_much` or `expressions_too_much_work`, which say the
group did too much together, rather than with an expression's error.

The net bounds time too, as a backstop that work within the limits never
reaches: a group's templates spend at most 10 seconds of wall-clock time
evaluating, together. The work count prices each step and each read by
what it costs, so a template at its limit takes well under a second, and a
group at its own a few seconds. The cap isn't a limit a workflow is written
to, since how long evaluating takes depends on the machine. The evaluator
reads the clock every 256 steps its group takes, each node it evaluates and
each run of a lambda, and a group that passes the cap fails with
`expressions_too_slow`.

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
| Secrets | Secret values, by name | Environment variables and files the user names |
| Variables | Organization and user variables | A variables file in Meridian's config directory |
| Groups | Execution groups | The user's config |
| Sources | Workflows that `uses:` names | Git, through the `git` command |
| Actions | Built-in actions | `parallelworks/checkout`, and those that need no platform |
| Sessions | Interactive sessions a workflow serves | None |
| Logs | Storing and reading step logs | Files in Meridian's state directory |
| Events | Run, job and step events as they happen | The command's progress display |

A resolver may be missing. Then the workflow features that need it fail to
compile, with an error saying which interface was missing.

## Durable execution

### On hopper

Durable state is [hopper](https://github.com/parallelworks/hopper)'s.
Meridian keeps no scheduling state of its own, and only adds run records
that hopper doesn't model, such as the plan, in the same database.

- **A run is a hopper workflow.** One transaction stores the run record and
  inserts the workflow, its jobs and their edges, so a run is stored before
  anything executes, and either all of it is stored or none of it is.
- **Each job instance is a hopper job**, and `needs:` becomes hopper's
  dependency edges.
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
- **ready:** its dependencies have finished. The coordinator evaluates its
  `if:`, and a false `if:` skips it here. Otherwise the coordinator evaluates
  whatever was left for this point, such as a `runs-on:` that uses `needs`,
  and picks a member of the job's group ([Placement](#placement)).
- **placing:** the member's backend is starting a worker: a scheduler job
  waiting in a queue, a pod being scheduled, a process starting over SSH. No
  user code runs yet, and the time this takes doesn't count against any
  lease.
- **running:** a worker has claimed the attempt and holds its lease.
- **The end:** the worker reports the outcome, or the coordinator records
  one when the worker is lost or the job is cancelled.

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
  mistaken for one that is dead.

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
- **`max-retries` and `interval`** work as in a step's `retry:`, defaults
  included. There is no per-attempt `timeout`, since a job has its own.
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
plan, so it runs the same revision with the same inputs, and the jobs it
doesn't rerun keep their outcomes and outputs; only what reads a variable
reads its value as it is then ([Variables](#variables)). Each attempt of a
job, and each retry of a step, keeps its own logs, so earlier attempts can
still be read.

### Variables

A run reads organization and user variables through the Variables
interface, and keeps none of their values in its records
([The plan](#the-plan)).

- **Once for each run.** As a run starts, the coordinator reads the
  variables its plan lists, a context's whole when the plan names it alone,
  and holds them in memory for that run. It evaluates the defaults and
  matrices the compiler left to it with them, and each attempt receives
  them when it is claimed, so every expression of the run reads the same
  values.
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

### Leases and fencing

A running attempt holds a lease of its own, with a token and a fencing
number that grows with every attempt. The worker renews it with heartbeats,
through whichever coordinator replica it reaches, and every write it makes
(step events, logs, outputs, the result) carries the token and number. A
write from a superseded attempt fails, and the worker that made it stops its
job. hopper's leases today belong to the client that claimed a job; per-job
leases are something it doesn't have yet.

When the coordinator comes back after being down, its workers get a grace
period to reconnect before their attempts count as lost. Jobs keep running
while Meridian is down, and a laptop waking up or `meridian run` restarting
must not make them all look lost.

### Cancellation, timeouts and cleanup

- **Cancelling a run is stored first.** Jobs that haven't been placed are
  cancelled in the store; workers are told to stop and send each running
  step SIGTERM, then SIGKILL after a grace period; placements in progress
  are withdrawn (`scancel`, `qdel`, deleting the pod).
- **Timeouts:** the worker enforces step and job timeouts. The coordinator
  enforces the run's timeout, and a job's as a backstop.
- **`early-cancel: any-job-failed`** stops a step when another job of the run
  fails. The coordinator tells the step's worker.
- **A step's `cleanup:` command** runs at the end of its job if the step
  ran, whatever the job's outcome, in reverse order.

### Outputs

A step writes outputs to the file named by `$OUTPUTS`. A job's outputs are
capped at 1 MB and a run's at 50 MB. When a job is claimed, it receives its
dependencies' outcomes and outputs with it, rather than reading each one
back. hopper keeps finalized jobs' outputs only in its history, so this,
too, needs something hopper doesn't have yet.

### Matrices from earlier results

A matrix whose values come from `needs` can't be expanded until its
dependencies have finished. It is stored as one hopper job. When that job is
ready, the coordinator expands the matrix, inserts the instances as a hopper
workflow of their own, and finishes the job once they have all finished,
with their combined outcome and outputs. Jobs that need it wait for all its
instances, as they do for any matrix. The compiler still checks that the
jobs and outputs the matrix refers to exist; only the values wait.

A matrix that reads a variable, whose values a plan doesn't hold, is stored
the same way, and the coordinator expands it as the run starts, with the
variables the run read then.

`fail-fast` and `max-parallel` apply to these matrices as to any other: the
coordinator cancels the remaining instances when one fails, and places no
more than `max-parallel` at a time.

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
- a bootstrap token: random, single-use and short-lived, valid for this one
  attempt, and stored only as a hash. It travels in the job's environment or
  in a file only the user can read, never on a command line, where other
  users could see it in `ps`.

The worker exchanges the bootstrap token for the attempt's credential when
it claims the attempt.

### The worker API

The API is HTTP with JSON bodies, versioned in its path (`/v1/`), with
errors as RFC 9457 problem details. Meridian's coordinator and a platform
that embeds Meridian serve the same API.

- **Claim:** exchange the bootstrap token for the attempt's credential, and
  receive the job from the plan, its secrets, the variables the run read as
  it started, and its dependencies' outcomes and outputs.
- **Heartbeat:** renew the lease.
- **Control:** a long poll that delivers cancellation and `early-cancel`.
- **Step events:** a step started, or finished with its exit code.
- **Logs:** each step's output, uploaded as a stream with byte offsets, so
  an interrupted upload resumes without duplicating or losing lines.
- **Complete:** the outcome and the job's outputs.

Every write is idempotent and carries a sequence number, so a worker
retrying after a dropped connection can't apply a report twice. A write
from a superseded attempt fails with a `fenced` problem.

The attempt credential is an opaque random token, stored as a hash, that
expires unless heartbeats renew it. It lets its holder do one thing: report
on its own attempt. A step that reads it gains nothing that its job couldn't
already do, so on backends where steps and their worker run as the same
user, no further separation is needed.

### Connections

Workers only connect out, and nothing listens on a host where jobs run.

| Backend | The worker reaches the coordinator through |
| --- | --- |
| `local` | A Unix socket in a directory only the user can open. |
| `ssh` | A Unix socket on the host that the coordinator forwards over its SSH connection, so no port opens on either side. |
| `slurm`, `pbs` | TLS 1.3 to the coordinator on the login node, checked against its key's hash. |
| `kubernetes` | TLS 1.3 to the coordinator's address, checked the same way. |

A coordinator outside a cluster drives Slurm or PBS over SSH to the login
node. There it starts `meridian relay`, the same binary, which accepts
workers over TLS and forwards their connections to the coordinator over SSH.
Compute nodes reach the login node but not the coordinator's machine, and
the coordinator's machine opens no port.

### Running steps

- **The run directory.** Format 1 gives each run a directory (`PW_JOB_DIR`),
  which is also where steps run by default. Jobs of a run on the same host,
  or on a shared filesystem, see the same directory. Containers mount it at
  the same path, so a path one job writes is valid in the next. Jobs on
  members that share no filesystem see different directories, and Meridian
  doesn't copy files between them.
- **The environment.** Steps get the environment variables format 1 defines,
  such as `OUTPUTS`, `PW_JOB_DIR` and `PW_JOB_ID`. Variables that only a
  platform can fill, such as an API key, are set only when one supplies them.
- **Containers.** On a member that runs containers, the worker runs on the
  host, outside the container. It creates the job's container, runs each
  step in it with the runtime's `exec`, and removes the container when the
  job ends.
- **Logs** are masked on the worker, before they leave the host.
- **While the coordinator is unreachable**, a worker keeps running its steps,
  spools logs and events to disk up to a limit, and delivers them in order
  when it reconnects.

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
  execution group that would replace it.
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
  from, so references to it should pin a commit. Untrusted code belongs in a
  group whose members use microVMs.
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
- **Secrets** stay out of the plan and out of the run's records. Each
  attempt receives the secrets it needs when it claims, and they are masked
  in its logs. Locally, a secret a run will need later, such as the value of
  a `password` input, is kept in a file only the user can read and deleted
  when the run ends.
- **Variables** stay out of the plan and the run's records too, secret or
  not, since people keep secrets in plain variables. A run reads them as it
  starts and holds them in memory while it runs, and its diagnostics never
  quote one ([Variables](#variables)).
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
  finished.
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

The database and logs live in Meridian's state directory. SQLite can't lock
reliably on a network filesystem, which is where home directories on
clusters often are, so Meridian refuses to open a database there and says
how to choose a local path.

## Editors and web UIs

Editors and web UIs validate workflows, expand matrices and evaluate
expressions with a TypeScript package built from the same Go code, as
WebAssembly, so what they accept is what runs. The package exports typed
functions rather than setting globals.

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
  steps, the worker waits for the child run, holding capacity while it does.
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
