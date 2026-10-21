package compile_test

import (
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A value that nests as deeply as a plan can hold compiles to a plan that
// encodes and decodes, and one that nests a level more is an error: in each
// part of the plan, whose levels around a value count, and whatever makes
// it deep. A mapping whose only key starts with $ counts as two levels, and
// a template as many as its parts' JSON.
func TestValueTooDeep(t *testing.T) {
	flow := func(depth int, open, leaf, close string) string {
		return strings.Repeat(open, depth) + leaf + strings.Repeat(close, depth)
	}
	nestedList := func(depth int) any {
		var v any = int64(1)
		for range depth {
			v = []any{v}
		}
		return v
	}
	// marked mappings, each the only key of the one around it, are most of
	// the levels of a mapping nested to the limit, so that YAML nests only
	// half as deep.
	const marked = 4900
	for name, tc := range map[string]struct {
		workflow  func(depth int) string
		submitted func(depth int) map[string]any
		// max is the deepest that compiles: what is left of MaxDepth once
		// the levels around the value, and those it adds itself, are
		// counted.
		max     int
		kind    *problems.Kind
		pointer string
	}{
		// The plan, its jobs, a job, its steps, a step, with: and k.
		"a step's mappings": {
			workflow: func(depth int) string {
				return "jobs:\n  a:\n    steps:\n      - uses: marketplace/x\n        with: {k: " + flow(marked, "{$a: ", flow(depth, "{a: ", "1", "}"), "}") + "}\n"
			},
			max: plan.MaxDepth - 6 - 2*marked, kind: problems.ValueTooDeep, pointer: "/jobs/a/steps/0/with",
		},
		"a step's mappings whose only key starts with $": {
			workflow: func(depth int) string {
				return "jobs:\n  a:\n    steps:\n      - uses: marketplace/x\n        with: {k: " + flow(depth, "{$a: ", "1", "}") + "}\n"
			},
			max: (plan.MaxDepth - 6) / 2, kind: problems.ValueTooDeep, pointer: "/jobs/a/steps/0/with",
		},
		// The plan, its jobs, a job, its env, and X: a template, its parts,
		// an expression and its tree, a literal.
		"a job's template": {
			workflow: func(depth int) string {
				return "jobs:\n  a:\n    env:\n      X: ${{ " + flow(depth, "[", "1", "]") + " }}\n    steps:\n      - run: echo\n"
			},
			max: plan.MaxDepth - 8, kind: problems.ValueTooDeep, pointer: "/jobs/a/env/X",
		},
		// The plan, its run, its env, and X.
		"the run's template": {
			workflow: func(depth int) string {
				return "env:\n  X: ${{ " + flow(depth, "[", "1", "]") + " }}\njobs:\n  a:\n    steps:\n      - run: echo\n"
			},
			max: plan.MaxDepth - 7, kind: problems.ValueTooDeep, pointer: "/env/X",
		},
		// The plan, its run, its inputs, and l.
		"an input's value": {
			workflow: func(int) string {
				return "on:\n  execute:\n    inputs:\n      l: {type: compute-clusters}\njobs:\n  a:\n    steps:\n      - run: echo\n"
			},
			submitted: func(depth int) map[string]any { return map[string]any{"l": nestedList(depth)} },
			max:       plan.MaxDepth - 3, kind: problems.InputTooDeep, pointer: "/on/execute/inputs",
		},
		// The plan, its run, its deferred inputs, and s: a template, its
		// parts, an expression, ||, a call, its arguments and a literal.
		"an input's default evaluated as the run starts": {
			workflow: func(depth int) string {
				return "on:\n  execute:\n    inputs:\n      s: {type: string, default: '${{ vars.s || contains(" + flow(depth, "[", "1", "]") + ", 1) }}'}\njobs:\n  a:\n    steps:\n      - run: echo\n"
			},
			max: plan.MaxDepth - 10, kind: problems.InputTooDeep, pointer: "/on/execute/inputs",
		},
	} {
		compileAt := func(depth int) (*plan.Plan, problems.Diagnostics) {
			var submitted map[string]any
			if tc.submitted != nil {
				submitted = tc.submitted(depth)
			}
			return compile.Compile("workflow.yaml", []byte(tc.workflow(depth)), submitted, compile.Options{Inputs: passThrough{}})
		}
		p, diags := compileAt(tc.max)
		if p == nil {
			t.Errorf("%s: as deep as a plan holds: %v", name, diags.Errors())
			continue
		}
		data, err := plan.Encode(p)
		if err != nil {
			t.Errorf("%s: as deep as a plan holds: %v", name, err)
			continue
		}
		if _, err := plan.Decode(data); err != nil {
			t.Errorf("%s: as deep as a plan holds: %v", name, err)
		}
		p, diags = compileAt(tc.max + 1)
		errs := diags.Errors()
		if p != nil || len(errs) != 1 || errs[0].Code != tc.kind.Code() || errs[0].Path() != tc.pointer {
			t.Errorf("%s: a level deeper = %v, want only %s at %s", name, errs, tc.kind.Code(), tc.pointer)
		}
	}
}
