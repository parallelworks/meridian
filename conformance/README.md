# Conformance suite

These cases pin what workflows mean, in areas:

| Area | Pins |
| --- | --- |
| `format` | Loading a workflow and validating it against its format's schema |
| `expressions` | What `${{ }}` expressions evaluate to |

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
      code: schema
      path: /links/docs
      line: 3
      column: 5
```

- `expect.diagnostics` lists every error and warning, in any order. A case
  without any loads cleanly. `path` is a JSON Pointer, and `line` and `column`
  are checked when they are set.
- `expect.values` lists values the loaded workflow must have, by JSON Pointer.

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
  - expr: "inputs.name ?? needs.a.outputs.b"
    value: Alice
```

- A case is an `expr`, the text between `${{` and `}}`; a `template`, a string
  with expressions in it; or `at`, the JSON Pointer of a string in the file's
  workflow, which is evaluated where it is.
- It expects a `value`, an `error` (`syntax` or `eval`), or `unknown`: the
  context, `status` or `index` the expression reads that the scope doesn't
  have, so that it is evaluated later.
- A case's own `scope` replaces each context it names, and the status and
  indexes if it has them.
- `warnings` lists the byte offsets of an expression's probable mistakes, and
  `condition: true` checks it as a condition, such as an `if:`.

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
