package compile

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// A checkoutKey is a repository and a revision, as a checkout step writes
// them, which the tree pins once, however many steps name them.
type checkoutKey struct{ repo, revision string }

// A checkoutAnswer is what Sources answered of a checkoutKey: the commit,
// or why not.
type checkoutAnswer struct {
	commit string
	err    error
}

// pinCheckout pins the parallelworks/checkout step at loc, in the workflow
// rep reports in, step, as the run is created (docs/design.md, Built-in
// actions): its with:'s repo and branch, which must be written out
// (checkout_not_known), to the commit the branch names now, through
// Sources, once for the tree, unless either is "", for which format 1
// checks out nothing, and the step succeeds; and checks its repo
// (plan.CheckRepo) and its
// sparse_checkout entries that are written out (plan.CheckCheckoutPath),
// wherever the run is created, whatever Sources resolver there is, or none.
// It reports what is wrong, and returns the pin, or nil for none. It
// returns an error, a resolver's that couldn't answer, or ctx's.
func (c *compiler) pinCheckout(ctx context.Context, t *tree, rep *reporter, loc []string, step map[string]any) (*plan.Checkout, error) {
	with, _ := step["with"].(map[string]any)
	if entries, ok := with["sparse_checkout"].([]any); ok {
		for i, e := range entries {
			if path, ok := e.(string); ok && !strings.Contains(path, "${{") {
				if err := plan.CheckCheckoutPath(path); err != nil {
					reportProblem(rep, appendPath(loc, "with", "sparse_checkout", strconv.Itoa(i)), err)
				}
			}
		}
	}
	var written [2]string
	for i, key := range []string{"repo", "branch"} {
		// The schema requires both, as text.
		text, _ := with[key].(string)
		if strings.Contains(text, "${{") {
			// The coordinator, not the worker, chooses what is fetched,
			// with whose credentials, as the run is created.
			rep.reportAt(problems.CheckoutNotKnown, appendPath(loc, "with", key), problems.Params{"key": key})
			return nil, nil
		}
		written[i] = text
	}
	repo, revision := written[0], written[1]
	if repo == "" || revision == "" {
		// Format 1 checks out nothing, and the step succeeds.
		return nil, nil
	}
	if err := plan.CheckRepo(repo); err != nil {
		reportProblem(rep, appendPath(loc, "with", "repo"), err)
		return nil, nil
	}
	if c.opts.Sources == nil {
		rep.reportAt(problems.ResolverMissing, appendPath(loc, "uses"), problems.Params{"key": "uses", "interface": "Sources"})
		return nil, nil
	}
	key := checkoutKey{repo, revision}
	answer, asked := t.checkoutPins[key]
	if !asked {
		answer.commit, answer.err = c.opts.Sources.Pin(ctx, repo, revision)
		t.checkoutPins[key] = answer
	}
	if answer.err != nil {
		// A revision that names no commit is the branch's problem, and
		// what else is wrong the repository's.
		at := appendPath(loc, "with", "repo")
		if k, _, ok := workflowProblem(answer.err); errors.Is(answer.err, ErrNotFound) || ok && k == problems.SourceRevisionNotFound {
			at = appendPath(loc, "with", "branch")
		}
		return nil, resolverError(ctx, rep, answer.err, at, problems.CheckoutNotFound, problems.Params{"repo": plan.ShowRepo(repo), "revision": revision})
	}
	if !commitPattern(answer.commit) {
		return nil, errors.New("compile: Sources.Pin gave " + strconv.Quote(answer.commit) + " as a commit")
	}
	return &plan.Checkout{Repo: repo, Revision: revision, Commit: answer.commit}, nil
}

// reportProblem reports err, a problem of the problems package's kinds, at
// loc in the workflow rep reports in.
func reportProblem(rep *reporter, loc []string, err error) {
	k, params, _ := workflowProblem(err)
	rep.reportAt(k, loc, params)
}

// commitPattern reports whether s is a commit's name in full, as git writes
// it: 40 hexadecimal digits, or 64 for a repository of SHA-256 objects.
func commitPattern(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	return !strings.ContainsFunc(s, func(c rune) bool { return (c < '0' || c > '9') && (c < 'a' || c > 'f') })
}

// isCheckout reports whether step is a parallelworks/checkout step.
func isCheckout(step map[string]any) bool {
	uses, _ := step["uses"].(string)
	return uses == plan.CheckoutUses
}

// planCheckouts returns the pins of the checkout steps of the workflows the
// run calls that the tree found, in the plan's order (planPins).
func (t *tree) planCheckouts() []plan.CheckoutPin {
	return planPins(t.checkouts, func(k callKey, co plan.Checkout) plan.CheckoutPin {
		return plan.CheckoutPin{Caller: k.caller, Step: k.step, Checkout: co}
	})
}
