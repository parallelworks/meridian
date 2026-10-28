# Conformance suite

These cases pin what workflows mean, in areas:

| Area | Pins |
| --- | --- |
| `format` | Loading a workflow and validating it against its format's schema |
| `expressions` | What `${{ }}` expressions evaluate to |
| `inputs` | A run's input values: defaults, conditions, types and what's required |
| `compile` | The plan a workflow compiles to, and the problems with its jobs and expressions |
| `start` | A run's values as it starts: the inputs resolved again from what was submitted, the workflow's `env:` and `timeout:`, the matrices expanded then, and the run's shape decided again |
| `matrices` | How a job's `strategy:` expands into its instances |
| `contexts` | What each context holds in each phase after the run starts, and what a job's and a step's values evaluate to |
| `runs` | How a run ends once its jobs have run, their steps on a host: each job's outcome, and what its steps print |

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
  values of password inputs, by their path, such as `creds.0.pass`, with a
  key's own `.` and `~` written `~1` and `~0`, as the plan names them, and of
  the other values kept as secrets, as one a form can have made from a
  password is. `expect.private` are the values kept out of the plan, as a
  variable's value is, that aren't secrets.
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

## Start

Each case is a directory like an inputs case, whose run is created with
its `inputs` and `contexts` and then started, as a coordinator starts it:
with the passwords and the private values creating it kept, and the
variables read again, secret ones too.

```yaml
description: >-
  An input's condition that read a variable as the run was created is
  decided again as it starts.
source: Meridian
contexts:
  org: {ENV: prod}
start-contexts:
  org: {ENV: dev}
expect:
  diagnostics:
    - severity: error
      code: run_shape_changed
      path: /on/execute/inputs/debug/ignore
      params: {key: ignore, name: debug}
```

- `contexts` are what the run reads when it is created, as an inputs
  case's are, and the variables it reads as it starts, those
  `secret-variables` names with the values `contexts` gives them.
  `start-contexts` replaces each context it gives as the run starts, as a
  variable can change before a rerun.
- `expect.diagnostics` are those of creating the run and starting it
  together. A run with errors doesn't start, and has no values to check.
- `expect.inputs` are the run's input values, complete: those the plan
  holds, what the start evaluated, and each password and private value in
  its place. `expect.env` is the workflow's `env:`, evaluated, and
  `expect.timeout` its `timeout:`, in seconds, checked when it is set.
- `expect.run-secrets`, `expect.run-private` and `expect.run-derived` name
  what of the run's values is a secret or made from one, is private, and
  was given in the place of what is made from a secret, as the phases after
  the start read them, by name, as in `inputs.creds.0.pass` and
  `vars.TOKEN`.
- `expect.matrices` are the matrix values of each instance of each matrix
  expanded as the run starts, by job name. A matrix that reads `needs`
  isn't among them: it is expanded once the jobs it needs have finished.
- A case without errors has exactly the values its `expect` lists, and
  none where it lists none.

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
  `cancelled` says the run is being cancelled, or is past its timeout, so
  a job that hadn't started takes the path its `if:` gives it, and the
  steps of one that runs read it, but those of one whose `if:` can be true
  once the run is cancelled, which it doesn't reach. A matrix job's
  `index` and `matrix` are those of the instance evaluated.
  `attempt`, from 1, `group` and `member` are the job context's. `steps`
  are how the job's steps ended, in order, each with its `outcome` and the
  `outputs` it wrote: a step that ended `cancelled` cancels the rest of the
  job.
- Each job is evaluated where its dependencies have finished, and each of
  its steps as it is about to run, up to the one after the last that
  `steps` gives, and once `steps` gives every step, each step's
  `cleanup:` and then its `outputs:`. Its steps are evaluated at the job's
  `try`, from 0, after the first try, with `env`, the variables the worker
  defines, such as `PW_WORKFLOW_STEP_CURRENT_RETRY`, which `env` reads over
  the workflow's, the job's and the step's.
- `expect.skipped` lists the JSON Pointers of the jobs and steps whose
  `if:` is false, and of the `cleanup:` of each step that didn't run or
  whose `if:` is false as the job ends, and every other job, step and
  `cleanup:` evaluated runs, but those `expect.cancelled` lists.
- `expect.needs-failed` lists the JSON Pointers of the jobs skipped because
  a job they need failed, which the jobs that need them read as failed.
  Every other job skipped was skipped by its own `if:`.
- `expect.cancelled` lists the JSON Pointers of the jobs that end cancelled
  without running: a job that hadn't started when the run was cancelled,
  whose `if:` reads what the jobs it needs give, `env`, an input whose
  value reads `env`, or a status function, but can't be true once the run
  is cancelled, nor is the word `always`.
- `expect.private-outputs` lists the JSON Pointers of the job outputs that
  are private: made from a variable, a password or a secret, or holding a
  secret, so kept only with the run's secrets. No other output is.
- `expect.secret-outputs` lists the JSON Pointers of the job outputs that
  hold a secret, made from one or holding one's text, and of the outputs
  of steps with ids that hold one's text, as
  `/jobs/<job>/steps/<index>/outputs/<name>`: each keeps its value, and
  the jobs that need the job read it as a secret. No other output does.
- `expect.values` are the values at JSON Pointers into the workflow, as
  their phase gives them: a step's `run`, `name`, `working-directory` and
  `with`; its `cleanup`, the command it runs once the job's steps have;
  and `env/<name>`, which is the environment the step runs with, its own
  `env:` or not; a job's `outputs/<name>`, each of its outputs, those its
  steps wrote among them; a job's or a step's `ssh`, the mapping it runs
  on a host with, or null when a step's `ssh:` is null; and a job's
  `runs-on`, or `runs-on/execution-group`, which is the execution group it
  names.
- `expect.diagnostics` are those of compiling the workflow and of each
  phase together.

## Runs

Each case is a directory like a compile case, whose run is created with
its `inputs` and `contexts`, started, and run to its end: each of its jobs
runs on a host, as the user, without a container, and its steps print to
their logs.

```yaml
description: >-
  A step's retry: tries it again once it fails.
source: Meridian
expect:
  run:
    outcome: success
    jobs:
      main:
        outcome: success
        logs:
          - {step: 0, try: 0, text: "try 0\n"}
          - {step: 0, try: 1, text: "try 1\n"}
```

- `expect.run.outcome` is the run's outcome, and `expect.run.jobs` how
  each of its jobs ends, by name: every one of them, with its `outcome`,
  and each instance of a matrix job, as its name, `#` and its index, from
  0, as in `build#1`.
- A job's `logs` are some of its logs, each exactly: the log of the try
  `try`, from 0, of the step `step`, from 0, or of its `cleanup:` with
  `cleanup: true`, which is what the command printed, its standard output
  and its standard error together, masked, as the run keeps it: a secret's
  text is `***`. The job's other logs aren't checked.
- A job's directory, and so what a step prints of a path in it, differs
  from one run to the next: a case prints what it checks of one, such as
  its last name, rather than the path.
- `repositories` names the repositories whose workflows the case's steps
  call, each as `github/<owner>/<repository>`. Each holds the files of the
  case's directory, at a commit of its default branch, `main`, which a
  step's `uses:` names with no revision, or as `@main`, and its `with:`'s
  `$yaml` names one of:

  ```yaml
  repositories: [github/conformance/calls]
  ```

  A run a step calls is a run of its own, whose jobs aren't among
  `expect.run.jobs`. A job's `calls` lists every run its steps called, of
  its last attempt, each by its `step` and `try`, from 0, with how it ends
  as `run`, as `expect.run` says how the run ends: its `outcome`, and its
  `jobs`, with their logs and calls in turn:

  ```yaml
  calls:
    - step: 0
      run:
        outcome: success
        jobs:
          greet: {outcome: success}
  ```

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
