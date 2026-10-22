# Conformance suite

These cases pin what workflows mean, in areas:

| Area | Pins |
| --- | --- |
| `format` | Loading a workflow and validating it against its format's schema |
| `expressions` | What `${{ }}` expressions evaluate to |
| `inputs` | A run's input values: defaults, conditions, types and what's required |
| `compile` | The plan a workflow compiles to, and the problems with its jobs and expressions |
| `matrices` | How a job's `strategy:` expands into its instances |
| `contexts` | What each context holds in each phase after the run starts, and what a job's and a step's values evaluate to |

## Format

Each case is a directory, which holds its workflow, `workflow.yaml`, and
`case.yaml`, which says what the workflow must produce:

```yaml
description: >-
  A link names an endpoint or a URL, not both.
source: ACTIVATE's workflow schema tests
expect:
  diagnostics:
    - severity: error
      code: keys_not_allowed_together
      path: /links/docs
      line: 3
      column: 5
```

- `expect.diagnostics` lists every error and warning, in any order. A case
  without any loads cleanly. `code` is the problem's code, which says what is
  wrong without depending on a message's wording or language; Meridian's
  `problems` package lists them. `path` is a JSON Pointer, and `line` and
  `column` are each checked when they are set: `column: 0` says the
  diagnostic is on a line but has no column.
- A diagnostic's `params` are values its message shows, which are part of
  its kind's contract as its code is, such as the name an unknown key's
  warning suggests:

  ```yaml
  - severity: warning
    code: unknown_key
    path: /jobs/main/steps/0/with/local_port
    params: {key: local_port, suggested: "true", suggestion: localPort}
  ```

  The params listed must have these values, and those it doesn't list
  aren't checked. [`problems/kinds.go`](../problems/kinds.go) names each
  kind's params where it defines the kind, and its first comment says what
  they hold. In short:

  - A param is text or a number, as each kind has it: `too_short`'s `min`
    is the number `1`, but `input_above_max`'s `value` and `max` are text,
    `"9"` and `"8"`. A yes or no is the text `"true"` or `"false"`.
  - A name or a key is as it is written, and a list of them is each in
    double quotes, joined with `", "`, as `'"run", "uses"'`.
  - A value from the workflow or the run is its JSON, as `problems.Show`
    writes it: compact, with a mapping's keys in sorted order, `<`, `>` and
    `&` as they are, U+2028 and U+2029 escaped, and cut to 40 characters,
    the last of them `…`. A value from a variable or a password is `***`.
  - Where a diagnostic is, its file, line and column, are params too, which
    a case checks with `line` and `column` rather than `params`.
- `expect.values` lists values the loaded workflow must have, by JSON Pointer.

## Inputs

Each case is a directory like a format case, whose `case.yaml` also gives
the values submitted for a run, and the input values the run must have:

```yaml
description: >-
  An input with ignore true has no value, even one that was submitted.
source: ACTIVATE's tests
inputs:
  regular_field: regular_value
  ignore_field: ignore_value
expect:
  inputs:
    regular_field: regular_value
```

- `expect.inputs` are the run's input values, and `expect.secrets` are the
  values of password inputs, by their path, such as `creds.0.pass`.
- `expect.deferred` are the values that stay expressions until the run is
  underway, such as a default that reads a variable, by their input's path,
  such as `cluster.token`. Each is its text: a list or mapping holds each of
  its expressions as the text it is written as. A deferred value isn't in
  `expect.inputs`.
- A case without errors has exactly the values its `expect` lists, and none
  where it lists none. A run with errors doesn't start, so a case with
  errors checks only the values it lists, such as those of the inputs that
  were given good ones, and not the others.
- `contexts` are what expressions read besides inputs, such as `org` and
  `vars`. `secret-variables` names variables in `vars` that are secrets:
  the compiler doesn't have their values, which are known only once the run
  is underway, so each is read as such whether or not `contexts` gives it a
  value.
- Input types that name a platform's resources take the value they're given
  as it is, as if a platform accepted it, unless `no-platform: true`.

## Compile

Each case is a directory like an inputs case, whose `expect.plan` holds
values the compiled plan must have, by JSON Pointer into its JSON encoding:

```yaml
description: >-
  A need written job:any waits for any one instance of a matrix job.
source: Meridian
expect:
  plan:
    /jobs/0/needs: [{job: probe, any: true}]
```

Diagnostics list every error and warning the workflow compiles with, its
loading's included. A case with errors has no plan.

## Matrices

Each file holds cases of a job's `strategy:` and what it expands to: the
matrix values of each of its instances, in order, or the problem it has.

```yaml
description: >-
  Each exclude entry removes the combinations that have all of its values.
source: ACTIVATE's matrix expansion
contexts:
  inputs: {skip: windows}
cases:
  - strategy:
      matrix:
        os: [linux, windows]
        exclude:
          - {os: "${{ inputs.skip }}"}
    instances:
      - {os: linux}
  - strategy:
      matrix:
        os: []
    error: matrix_empty
    path: /strategy/matrix
```

- `strategy` is the job's `strategy:`, whose strings can hold expressions,
  which read `contexts`: the file's, with those a case gives in place of
  each it names. A strategy is evaluated where the job's dependencies have
  finished, so it can read `needs`.
- A case expects exactly one of `instances`, each instance's matrix values,
  and `error`, the code of the problem the matrix has, with `path`, the JSON
  Pointer of where it is from the job, such as `/strategy/matrix/exclude/0`.
  `fail-fast` and `max-parallel` are checked when they are set.
- `limit` is the most instances a matrix may have, which isn't part of the
  format: whoever runs the workflow sets it. Without it, it is Meridian's
  default, 256.

## Contexts

Each case is a directory like a compile case, whose `case.yaml` also says
what is known of each job, by name, where the phases after the run starts
evaluate it, and what the job's and its steps' values evaluate to there:

```yaml
description: >-
  A step's env: reads the job's.
source: Meridian
jobs:
  main:
    needs:
      build: {result: success, outputs: {group: gpus}}
    steps:
      - {outcome: success, outputs: {PORT: "8080"}}
expect:
  skipped: [/jobs/optional]
  values:
    /jobs/main/steps/1/run: echo 8080
    /jobs/main/outputs/port: "8080"
```

- The run's values are the case's: its `inputs`, as they are submitted, its
  `contexts`, the variables, `remote` and `sessions`, and the workflow's
  `env:`, which holds no expressions, so that the case needs nothing the
  run evaluates as it starts. A variable `secret-variables` names is a
  secret.
- A job's `needs` are the results of the jobs it needs: `result`, its
  outcome; `outputs`; `steps`, its steps' outputs by id; `needs-failed`,
  set for a job skipped because one it needs failed, rather than by its own
  `if:`, or a matrix job one of whose instances was; and `cancelled`, set
  for a matrix job one of whose instances was cancelled. A job's own
  `cancelled` says the run is being cancelled. A matrix job's `index` and
  `matrix` are those of the instance evaluated. `attempt`, from 1, `group`
  and `member` are the job context's. `steps` are how the job's steps
  ended, in order, each with its `outcome` and the `outputs` it wrote.
- Each job is evaluated where its dependencies have finished, and each of
  its steps as it is about to run, up to the one after the last that
  `steps` gives, and its `outputs:` once `steps` gives every step. Its
  steps are evaluated at the job's `try`, from 0, with `env`, the
  variables the worker defines, such as `PW_WORKFLOW_STEP_CURRENT_RETRY`,
  which `env` reads over the workflow's, the job's and the step's.
- `expect.skipped` lists the JSON Pointers of the jobs and steps whose
  `if:` is false, and every other job and step evaluated runs.
- `expect.needs-failed` lists the JSON Pointers of the jobs skipped because
  a job they need failed, which the jobs that need them read as failed.
  Every other job skipped was skipped by its own `if:`.
- `expect.private` lists the JSON Pointers of the job outputs that are
  private: made from a variable or a password and not from a secret, so
  kept only with the run's secrets. No other output is.
- `expect.values` are the values at JSON Pointers into the workflow, as
  their phase gives them: a step's `run`, `name`, `cleanup`,
  `working-directory` and `with`, and `env/<name>`, which is the
  environment the step runs with, its own `env:` or not; a job's
  `outputs/<name>`, each of its outputs, those its steps wrote among them;
  a step's `ssh`, the mapping it runs on a host with, or null when its
  `ssh:` is null; and its `runs-on`, or `runs-on/execution-group`, which
  is the execution group it names.
- `expect.diagnostics` are those of compiling the workflow and of each
  phase together.

## Expressions

Each file holds cases that share a scope: the contexts their expressions
read, and optionally a status for the status functions and indexes for
`[index]`. A file can also hold a workflow, which relative references and
input definitions read.

```yaml
description: >-
  ?? replaces only null.
source: ACTIVATE's tests
scope:
  contexts:
    inputs: {name: Alice, empty: ""}
cases:
  - expr: "inputs.empty ?? 'fallback'"
    value: ""
  - template: "Hello, ${{ inputs.name }}"
    value: Hello, Alice
  - expr: "inputs.name ??"
    error: syntax
    code: expression_ends_early
  - expr: "inputs.name ?? needs.a.outputs.b"
    value: Alice
```

- A case is an `expr`, the text between `${{` and `}}`; a `template`, a string
  with expressions in it; or `at`, the JSON Pointer of a string in the file's
  workflow, which is evaluated where it is.
- It expects exactly one of a `value`, an `error` (`syntax` or `eval`), or
  `unknown`: the context, `status` or `index` the expression reads that the
  scope doesn't have, so that it is evaluated later. A case that expects
  null says `value: null`.
- An error's `code` is its problem's code, as a diagnostic's is, and is
  checked when it is set.
- A case's own `scope` replaces each context it names, and the status and
  indexes if it has them.
- `warnings` lists the byte offsets of an expression's probable mistakes, in
  the expression, or in the template or string for a `template` or `at`, and
  `condition: true` checks it as a condition, such as an `if:`. A case
  without `warnings` doesn't check them.

The Go package here embeds the cases, so that any implementation of the
format can run them. Meridian's packages run the cases of their area in their
tests.

## Sources

Cases come from ACTIVATE's own tests and documentation, from public
workflows, and from Meridian. A workflow copied from a public Parallel Works
repository keeps its license, which its `case.yaml` names along with the
commit it was copied from: [Apache-2.0](licenses/Apache-2.0.txt) or
[MIT](licenses/MIT.txt). Everything else here is under the repository's
license.
