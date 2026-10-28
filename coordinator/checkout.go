package coordinator

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/store"
	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// Trees gives the trees of the commits that parallelworks/checkout steps
// were pinned to as their runs were created (plan.Checkout), which the
// coordinator serves to the steps' workers (workerapi.PathCheckout). The
// local one reads them from the copies of the repositories that pinned
// them, which hold their commits.
type Trees interface {
	// SurveyTree returns what the tree of commit, of the repository repo,
	// as a checkout step writes it, holds of paths, its sparse_checkout's
	// entries, each a literal path from the repository's root
	// (plan.CheckCheckoutPath), or of the whole tree, for none. It reads
	// the commit from where it has it, and fetches it, if it hasn't, from
	// repo. An error is the commit's that couldn't be read now.
	SurveyTree(ctx context.Context, repo, commit string, paths []string) (TreeSurvey, error)
	// WriteTree writes the tree of commit, of repo, or of paths in it, as
	// SurveyTree read it, to w, as a tar stream: its directories, its files
	// and its symbolic links, with paths from the repository's root, each
	// directory before what it holds, and a submodule as an empty
	// directory. It writes the files as Git holds them, with no attribute
	// of the repository's applied, as a filter or a line ending.
	WriteTree(ctx context.Context, repo, commit string, paths []string, w io.Writer) error
}

// A TreeSurvey is what the tree of a commit holds of a checkout's paths.
type TreeSurvey struct {
	// Size is how many bytes its files hold, together, and Entries how
	// many directories, files, links and submodules it has.
	Size    int64
	Entries int
	// Missing are the indexes of the paths that name nothing in it.
	Missing []int
	// Submodules is set when it holds a submodule.
	Submodules bool
}

// checkoutWaits are how long the coordinator waits before it reads a
// checkout's commit again, after each try that couldn't, as format 1 tries
// a clone again: 5 tries in all.
var checkoutWaits = []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

// checkout answers the action of the parallelworks/checkout step of call's
// attempt, step: whether its tree, of the commit step was pinned to, or of
// its sparse_checkout's paths in it, as the request's with: has them, can
// be served (workerapi.PathCheckout), which the worker then asks for. It
// fails the step for an entry of sparse_checkout an expression gave that
// isn't a literal path (checkout_path_invalid), and for a tree it can't
// serve: one whose commit it couldn't read, though it tried 5 times
// (checkout_unavailable), a whole checkout of a repository with submodules
// (checkout_submodules), and one whose files come to more than
// workerapi.MaxCheckoutBytes (checkout_too_large), or that has more
// entries than workerapi.MaxCheckoutEntries (checkout_too_many). An entry
// that names
// nothing warns (checkout_path_not_found), and the step succeeds. A step
// whose repo or branch is "", which Create pinned nothing for, checks out
// nothing, as in format 1, and succeeds; another without a pin, of a run
// created before checkouts were pinned, fails (uses_not_supported).
//
// It reads the tree outside a transaction, and gives Commit only its
// answer: what a repeat of the request meanwhile reads again is the same.
func (c *Coordinator) checkout(ctx context.Context, call *workerapisrv.ActionCall, step *plan.Step) (workerapi.ActionAnswer, error) {
	answer := func(a workerapi.ActionAnswer) (workerapi.ActionAnswer, error) {
		return call.Commit(ctx, step, func(*store.Tx) (workerapi.ActionAnswer, error) { return a, nil })
	}
	pin := step.Checkout
	with, _ := call.Request.With.Get().(map[string]any)
	at := step.Path + "/with/sparse_checkout"
	paths, wrong := sparsePaths(with["sparse_checkout"], at)
	switch repo, _ := with["repo"].(string); {
	case len(wrong) > 0:
		return answer(actionFailed(wrong...))
	case pin == nil && (repo == "" || with["branch"] == ""):
		// Format 1 checks out nothing, and the step succeeds, which the
		// worker asks for no tree of.
		return answer(workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess})
	case pin == nil:
		// A run created before checkouts were pinned, which nothing can
		// check out by its pin.
		return answer(actionFailed(problems.UsesNotSupported.At("", problems.Position{}, step.Path, problems.Params{"uses": step.Uses})))
	case c.opts.Trees == nil:
		return answer(actionFailed(problems.ResolverMissing.At("", problems.Position{}, step.Path+"/uses", problems.Params{"key": "uses", "interface": "Trees"})))
	}
	survey, err := c.surveyTree(ctx, pin, paths)
	switch {
	case ctx.Err() != nil:
		return workerapi.ActionAnswer{}, ctx.Err()
	case err != nil:
		return answer(actionFailed(problems.CheckoutUnavailable.At("", problems.Position{}, step.Path+"/with/repo",
			problems.Params{"repo": plan.ShowRepo(pin.Repo), "commit": pin.Commit, "reason": problems.EscapeControl(err.Error())})))
	case len(paths) == 0 && survey.Submodules:
		return answer(actionFailed(problems.CheckoutSubmodules.At("", problems.Position{}, step.Path+"/with/repo", problems.Params{"repo": plan.ShowRepo(pin.Repo)})))
	case survey.Size > workerapi.MaxCheckoutBytes:
		return answer(actionFailed(problems.CheckoutTooLarge.At("", problems.Position{}, step.Path, problems.Params{"size": survey.Size, "limit": workerapi.MaxCheckoutBytes})))
	case survey.Entries > workerapi.MaxCheckoutEntries:
		return answer(actionFailed(problems.CheckoutTooMany.At("", problems.Position{}, step.Path, problems.Params{"count": survey.Entries, "limit": workerapi.MaxCheckoutEntries})))
	}
	var warned problems.Diagnostics
	for _, i := range survey.Missing {
		warned = append(warned, problems.CheckoutPathNotFound.At("", problems.Position{}, at+"/"+strconv.Itoa(i), problems.Params{"path": paths[i]}))
	}
	return answer(workerapi.ActionAnswer{Outcome: plan.OutcomeSuccess, Diagnostics: warned})
}

// surveyTree surveys the tree of pin, of paths, with Trees, trying again,
// as checkoutWaits says, while it can't, until ctx is done.
func (c *Coordinator) surveyTree(ctx context.Context, pin *plan.Checkout, paths []string) (TreeSurvey, error) {
	for try := 0; ; try++ {
		survey, err := c.opts.Trees.SurveyTree(ctx, pin.Repo, pin.Commit, paths)
		if err == nil || ctx.Err() != nil || try == len(c.checkoutWaits) {
			return survey, err
		}
		c.log.WarnContext(ctx, "coordinator: a checkout's commit couldn't be read; trying again", "repo", plan.ShowRepo(pin.Repo), "commit", pin.Commit, "error", err)
		timer := time.NewTimer(c.checkoutWaits[try])
		select {
		case <-ctx.Done():
			timer.Stop()
			return TreeSurvey{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// sparsePaths reads a checkout step's with: sparse_checkout, v, as the
// worker evaluated it, at pointer: its entries, or none for null, which
// checks out the whole tree; or the problems of an entry that isn't a
// literal path from the repository's root, and of a value that isn't a
// list of them.
func sparsePaths(v any, pointer string) ([]string, problems.Diagnostics) {
	if v == nil {
		return nil, nil
	}
	entries, ok := v.([]any)
	if !ok {
		return nil, problems.Diagnostics{wrongType(pointer, v, "array")}
	}
	var (
		paths []string
		wrong problems.Diagnostics
	)
	for i, e := range entries {
		at := pointer + "/" + strconv.Itoa(i)
		switch path, ok := e.(string); {
		case !ok:
			wrong = append(wrong, wrongType(at, e, "string"))
		case plan.CheckCheckoutPath(path) != nil:
			wrong = append(wrong, problems.CheckoutPathInvalid.At("", problems.Position{}, at, problems.Params{"path": path}))
		default:
			paths = append(paths, path)
		}
	}
	return paths, wrong
}

// tree writes the tree of the checkout step of call's attempt, step, to w,
// once its action was answered success (workerapisrv.Actions.Tree).
func (c *Coordinator) tree(ctx context.Context, call *workerapisrv.ActionCall, step *plan.Step, w io.Writer) error {
	if step.Checkout == nil || c.opts.Trees == nil {
		return fmt.Errorf("%w: step %d has no tree to serve", workerapisrv.ErrNotAction, call.Request.Step)
	}
	with, _ := call.Request.With.Get().(map[string]any)
	paths, wrong := sparsePaths(with["sparse_checkout"], step.Path+"/with/sparse_checkout")
	if len(wrong) > 0 {
		return fmt.Errorf("%w: step %d's sparse_checkout isn't one its action took", workerapisrv.ErrNotAction, call.Request.Step)
	}
	return c.opts.Trees.WriteTree(ctx, step.Checkout.Repo, step.Checkout.Commit, paths, w)
}
