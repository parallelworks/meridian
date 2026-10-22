# Conformance suite

These cases pin what workflows mean, in areas:

| Area | Pins |
| --- | --- |
| `format` | Loading a workflow and validating it against its format's schema |
| `expressions` | What `${{ }}` expressions evaluate to |
| `inputs` | A run's input values: defaults, conditions, types and what's required |
| `compile` | The plan a workflow compiles to, and the problems with its jobs and expressions |

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
  values of password inputs, by their path, such as `creds.0.pass`, and of
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
