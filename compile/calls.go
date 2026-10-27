package compile

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/format"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// CallLimits are Meridian's limits on the tree of workflows that a run's
// uses: steps call, and those call in turn, which are format 1's too.
// Create checks them as it pins the tree; a child run's Create, which reads
// its pins from its root's plan (plan.Plan.Calls), checks them no more, so
// the plan doesn't record them. A zero field is the default.
type CallLimits struct {
	// MaxDepth is how many levels below the run a workflow may be called:
	// DefaultMaxDepth for 0.
	MaxDepth int
	// MaxWorkflows is how many distinct workflows the tree may hold:
	// DefaultMaxWorkflows for 0.
	MaxWorkflows int
	// MaxCalls is how many calls the tree may make, the run's own steps'
	// among them, counted on every path of calls from the run, so that a
	// workflow called from two places counts its own calls twice:
	// DefaultMaxCalls for 0.
	MaxCalls int
	// MaxBytes is how much text, in bytes, the tree's distinct called
	// workflows may hold in all, the run's own not among them, as format 1
	// counts it: DefaultMaxCallBytes for 0.
	MaxBytes int
}

// The defaults of CallLimits, which are format 1's.
const (
	DefaultMaxDepth     = 4
	DefaultMaxWorkflows = 100
	DefaultMaxCalls     = 1000
	DefaultMaxCallBytes = 1 << 20
)

// withDefaults returns l with each zero field its default.
func (l CallLimits) withDefaults() CallLimits {
	return CallLimits{
		MaxDepth:     cmp.Or(l.MaxDepth, DefaultMaxDepth),
		MaxWorkflows: cmp.Or(l.MaxWorkflows, DefaultMaxWorkflows),
		MaxCalls:     cmp.Or(l.MaxCalls, DefaultMaxCalls),
		MaxBytes:     cmp.Or(l.MaxBytes, DefaultMaxCallBytes),
	}
}

// A pin is a workflow that a step's uses: names, resolved once for the
// whole tree, by its reference as plan.SourceRef.Normal reads it: its
// pin, and the workflow, loaded and validated; source is nil for one that
// couldn't be pinned, and doc for one that didn't load, whose problems are
// reported.
type pin struct {
	source *plan.Source
	doc    *format.Document
}

// A tree is the tree of workflows a run calls, as Create pins it: each
// path of calls from the run walked, as format 1 walks it, and each
// reference resolved once.
type tree struct {
	limits CallLimits
	// pins are the workflows pinned, by their references.
	pins map[plan.SourceRef]*pin
	// workflows and bytes count the distinct workflows pinned and their
	// text, and calls the calls on every path.
	workflows, bytes, calls int
	// full is set once a limit is passed: the walk stops there.
	full bool
	// found are the pins of the steps of the workflows the run calls, by
	// their caller's digest and the step's JSON Pointer (plan.Plan.Calls).
	found map[callKey]plan.Source
	// reported holds what was reported once, of what more than one path
	// reaches: a cycle, or a step too deep.
	reported map[string]bool
}

// A callKey is where a step that calls a workflow is: the digest of its
// workflow's text, and its JSON Pointer in it.
type callKey struct{ caller, step string }

// newTree returns a tree with limits, which the run's root, whose own
// reference, if a workflow named it, is root, starts.
func newTree(limits CallLimits) *tree {
	return &tree{
		limits: limits.withDefaults(), pins: map[plan.SourceRef]*pin{},
		found: map[callKey]plan.Source{}, reported: map[string]bool{},
	}
}

// A node is a workflow on a path of calls from the run: its pin, and the
// path to it, from the run's own workflow: each workflow's reference
// (plan.SourceRef.Normal, the run's own one's if a workflow named it), and
// its location, which a problem with the path shows; and root, the uses:
// of the run's own step the path starts at, where a problem with the tree
// below it is reported, in the run's own workflow, which whoever runs it
// can see, if not the workflows it calls.
type node struct {
	pin   *pin
	keys  []plan.SourceRef
	chain []string
	root  []string
}

// depth is how many levels below the run n is.
func (n node) depth() int { return len(n.keys) - 1 }

// walkCalls walks the paths of calls from first, the workflows the run's
// own steps call, breadth-first: it pins the workflows each one's steps
// call, as it pinned theirs (pinRef), checks the tree's limits and cycles,
// and records each step's pin (tree.found). It returns an error, a
// resolver's that couldn't answer, or ctx's.
func (c *compiler) walkCalls(ctx context.Context, t *tree, first []node) error {
	queue := first
	for len(queue) > 0 && !t.full {
		n := queue[0]
		queue = queue[1:]
		if n.pin.doc == nil {
			continue
		}
		next, err := c.callsOf(ctx, t, n)
		if err != nil {
			return err
		}
		queue = append(queue, next...)
	}
	return ctx.Err()
}

// callsOf pins the workflows that the steps of the workflow at n call, in
// the order of their jobs' names and their places, and returns their
// nodes. What is wrong with a step itself is reported in n's workflow, at
// the step; what is wrong with the tree, a cycle, a call too deep, or past
// its limits, at the run's own step the path starts at (node.root), with
// the chain of calls from the run.
func (c *compiler) callsOf(ctx context.Context, t *tree, n node) ([]node, error) {
	rep := &reporter{doc: n.pin.doc}
	defer func() { c.diags = append(c.diags, rep.diags...) }()
	jobs, _ := n.pin.doc.Value["jobs"].(map[string]any)
	var next []node
	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		job, _ := jobs[name].(map[string]any)
		steps, _ := job["steps"].([]any)
		for i, s := range steps {
			step, _ := s.(map[string]any)
			loc := []string{"jobs", name, "steps", strconv.Itoa(i)}
			ref, ok := stepRef(rep, loc, step)
			if !ok {
				continue
			}
			if t.count(&c.reporter, n.root) {
				return nil, nil
			}
			key := ref.Normal()
			if i := slices.Index(n.keys, key); i >= 0 {
				// A workflow already on its own path of calls: the chain
				// from the run, to it again. A cycle is reported once,
				// whatever paths reach it.
				chain := append(slices.Clone(n.chain), n.chain[i])
				if t.once("cycle " + cycleKey(n.chain[i:])) {
					c.reportAt(problems.WorkflowCallsItself, n.root, problems.Params{"chain": problems.QuoteAll(chain)})
				}
				continue
			}
			if n.depth() >= t.limits.MaxDepth {
				if t.once("depth " + jsonptr.Format(n.root) + "\x00" + n.pin.doc.File + "\x00" + jsonptr.Format(loc)) {
					chain := append(slices.Clone(n.chain), ref.Uses)
					c.reportAt(problems.WorkflowNestedTooDeep, n.root, problems.Params{"max": t.limits.MaxDepth, "chain": problems.QuoteAll(chain)})
				}
				continue
			}
			p, err := c.pinRef(ctx, t, rep, loc, n.root, ref)
			if err != nil || t.full {
				return nil, err
			}
			if p.source == nil {
				continue
			}
			t.found[callKey{n.pin.source.Digest, jsonptr.Format(loc)}] = *p.source
			next = append(next, node{pin: p, keys: append(slices.Clone(n.keys), key), chain: append(slices.Clone(n.chain), p.source.Location), root: n.root})
		}
	}
	return next, nil
}

// count counts a call, on the path from the uses: at root, of the run's
// own step, and says whether that passes the tree's limit, which it
// reports there, and which ends the walk.
func (t *tree) count(rep *reporter, root []string) bool {
	if t.calls++; t.calls > t.limits.MaxCalls {
		rep.reportAt(problems.WorkflowsTooMany, root, problems.Params{"max": t.limits.MaxCalls, "what": "calls"})
		t.full = true
	}
	return t.full
}

// cycleKey names the cycle of the workflows at locations, in their order,
// whichever of them it starts at: from the least of them.
func cycleKey(locations []string) string {
	i := slices.Index(locations, slices.Min(locations))
	return strings.Join(slices.Concat(locations[i:], locations[:i]), "\x00")
}

// once says whether key is new to the tree, which it then holds.
func (t *tree) once(key string) bool {
	if t.reported[key] {
		return false
	}
	t.reported[key] = true
	return true
}

// planCalls returns the pins the tree found, in the plan's order: by
// their caller's digest, and then by the step's JSON Pointer.
func (t *tree) planCalls() []plan.Call {
	keys := slices.SortedFunc(maps.Keys(t.found), func(a, b callKey) int {
		return cmp.Or(strings.Compare(a.caller, b.caller), strings.Compare(a.step, b.step))
	})
	var out []plan.Call
	for _, k := range keys {
		out = append(out, plan.Call{Caller: k.caller, Step: k.step, Source: t.found[k]})
	}
	return out
}
