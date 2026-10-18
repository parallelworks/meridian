# Conformance suite

These cases pin what workflows mean. Each case is a directory under an area:

| Area | Pins |
| --- | --- |
| `format` | Loading a workflow and validating it against its format's schema |

A case holds its workflow, `workflow.yaml`, and `case.yaml`, which says what
the workflow must produce:

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

The Go package here embeds the cases, so that any implementation of the
format can run them. Meridian's packages run the cases of their area in their
tests.

## Sources

Cases come from ACTIVATE's own tests, from public workflows, and from
Meridian. A workflow copied from a public Parallel Works repository keeps its
license, which its `case.yaml` names along with the commit it was copied
from: [Apache-2.0](licenses/Apache-2.0.txt) or [MIT](licenses/MIT.txt).
Everything else here is under the repository's license.
