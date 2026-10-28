package compile_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

const pinnedCommit = "fedcba9876543210fedcba9876543210fedcba98"

// checkoutStep returns a workflow of one job whose steps are steps, each a
// YAML mapping on one line.
func checkoutWorkflow(steps ...string) string {
	var b strings.Builder
	b.WriteString("jobs:\n  a:\n    steps:\n")
	for _, s := range steps {
		b.WriteString("      - " + s + "\n")
	}
	return b.String()
}

// Create pins no checkout whose repo or branch is "", which format 1 checks
// out nothing for, and whose step succeeds, without asking Sources, though
// it checks its sparse_checkout; and the plan of a run that pins nothing
// is of version 1.
func TestCreatePinsNoEmptyCheckout(t *testing.T) {
	src := checkoutWorkflow(
		`{uses: parallelworks/checkout, with: {repo: "", branch: main}}`,
		`{uses: parallelworks/checkout, with: {repo: https://github.com/acme/app.git, branch: ""}}`,
		`{uses: parallelworks/checkout, with: {repo: "", branch: ""}}`,
	)
	s := &testSources{}
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: s})
	if err != nil || c == nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	for i, step := range c.Plan.Jobs[0].Steps {
		if step.Checkout != nil {
			t.Errorf("step %d is pinned as %+v", i, step.Checkout)
		}
	}
	if len(s.Pinned()) != 0 || c.Plan.Version != 1 {
		t.Errorf("Sources pinned %v, and the plan is of version %d", s.Pinned(), c.Plan.Version)
	}

	_, diags, err = compile.Create(t.Context(), "workflow.yaml", []byte(checkoutWorkflow(`{uses: parallelworks/checkout, with: {repo: "", branch: main, sparse_checkout: ["*.go"]}}`)), nil, compile.Options{Sources: s})
	if err != nil || len(diags) != 1 || diags[0].Code != problems.CheckoutPathInvalid.Code() {
		t.Errorf("an empty repo's sparse_checkout wasn't checked: %v, %v", diags, err)
	}
}

// Create pins each parallelworks/checkout step's repo and branch, written
// out, to the commit Sources gives, once for each repository and revision,
// in the step, whatever its if:; and the plan is of version 2.
func TestCreatePinsCheckouts(t *testing.T) {
	src := checkoutWorkflow(
		`{uses: parallelworks/checkout, with: {repo: https://github.com/acme/app.git, branch: main, sparse_checkout: [app, "${{ inputs.x }}"]}}`,
		`{if: false, uses: parallelworks/checkout, with: {repo: https://github.com/acme/app.git, branch: main}}`,
		`{uses: parallelworks/checkout, with: {repo: "git@github.com:acme/tools.git", branch: v2, path: "${{ inputs.dir }}"}}`,
		`{run: echo}`,
	)
	s := &testSources{Commits: map[string]string{
		"https://github.com/acme/app.git@main": pinnedCommit,
		"git@github.com:acme/tools.git@v2":     "0123456789abcdef0123456789abcdef01234567",
	}}
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: s})
	if err != nil || c == nil || diags.HasErrors() {
		t.Fatal(diags, err)
	}
	if want := []string{"https://github.com/acme/app.git@main", "git@github.com:acme/tools.git@v2"}; !slices.Equal(s.Pinned(), want) {
		t.Errorf("pinned %v, want %v", s.Pinned(), want)
	}
	steps := c.Plan.Jobs[0].Steps
	for i, want := range []*plan.Checkout{
		{Repo: "https://github.com/acme/app.git", Revision: "main", Commit: pinnedCommit},
		{Repo: "https://github.com/acme/app.git", Revision: "main", Commit: pinnedCommit},
		{Repo: "git@github.com:acme/tools.git", Revision: "v2", Commit: "0123456789abcdef0123456789abcdef01234567"},
		nil,
	} {
		if got := steps[i].Checkout; (got == nil) != (want == nil) || got != nil && *got != *want {
			t.Errorf("step %d is pinned as %+v, want %+v", i, got, want)
		}
	}
	if c.Plan.Version != 2 || len(c.Plan.Checkouts) != 0 {
		t.Errorf("the plan is of version %d, with the tree's checkouts %v", c.Plan.Version, c.Plan.Checkouts)
	}
}

// Create refuses a checkout it can't pin, at its key, wherever the run is
// created: a repo or a branch that an expression gives (checkout_not_known);
// a repo Meridian doesn't fetch (checkout_repo_invalid); a written-out
// sparse_checkout entry that isn't a literal path (checkout_path_invalid);
// one without a Sources resolver (resolver_missing); and one Sources
// refuses: a revision it doesn't have (checkout_not_found, or its own
// problem, at the branch), or a repository it may not read (at the repo).
// A resolver that couldn't answer fails Create.
func TestCreateRefusesCheckouts(t *testing.T) {
	for _, tc := range []struct {
		name, step string
		sources    compile.Sources
		code       problems.Kind
		at         string
	}{
		{"repo from an expression", `{uses: parallelworks/checkout, with: {repo: "${{ inputs.r }}", branch: main}}`, &testSources{}, *problems.CheckoutNotKnown, "/with/repo"},
		{"branch from an expression", `{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: "${{ inputs.b }}"}}`, &testSources{}, *problems.CheckoutNotKnown, "/with/branch"},
		{"another transport", `{uses: parallelworks/checkout, with: {repo: "file:///srv/r.git", branch: main}}`, &testSources{}, *problems.CheckoutRepoInvalid, "/with/repo"},
		{"a credential", `{uses: parallelworks/checkout, with: {repo: "https://u:t@h/r.git", branch: main}}`, nil, *problems.CheckoutRepoInvalid, "/with/repo"},
		{"a pattern", `{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: main, sparse_checkout: [ok, "*.go"]}}`, &testSources{Commits: map[string]string{"https://h/r.git@main": pinnedCommit}}, *problems.CheckoutPathInvalid, "/with/sparse_checkout/1"},
		{"no Sources", `{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: main}}`, nil, *problems.ResolverMissing, "/uses"},
		{"no such revision", `{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: gone}}`, &testSources{}, *problems.CheckoutNotFound, "/with/branch"},
		{
			"a revision git doesn't find", `{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: gone}}`,
			&testSources{Err: problems.SourceRevisionNotFound.Problem(problems.Params{"repo": "https://h/r.git", "revision": "gone", "reason": "x"}).WithCause(compile.ErrNotFound)},
			*problems.SourceRevisionNotFound, "/with/branch",
		},
		{
			"access denied", `{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: main}}`,
			&testSources{Err: problems.SourceAccessDenied.Problem(problems.Params{"location": "https://h/r.git", "reason": "x"})},
			*problems.SourceAccessDenied, "/with/repo",
		},
	} {
		opts := compile.Options{}
		if tc.sources != nil {
			opts.Sources = tc.sources
		}
		_, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(checkoutWorkflow(tc.step)), nil, opts)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		errs := diags.Errors()
		if len(errs) != 1 || errs[0].Code != tc.code.Code() || errs[0].Pointer != "#/jobs/a/steps/0"+tc.at {
			t.Errorf("%s: %v, want %s at %s", tc.name, errs, tc.code.Code(), tc.at)
		}
	}
	down := errors.New("down")
	src := checkoutWorkflow(`{uses: parallelworks/checkout, with: {repo: https://h/r.git, branch: main}}`)
	if _, _, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: &testSources{Err: down}}); !errors.Is(err, down) {
		t.Errorf("a resolver that couldn't answer: %v", err)
	}
}

// Create pins the checkout steps of the workflows the run calls, down the
// tree, in the plan (plan.Plan.Checkouts), by their workflow's digest and
// their place in it, as it pins their calls, so that a child run's Create
// reads them; and reports what is wrong with one in that workflow's file.
func TestCreatePinsCalledCheckouts(t *testing.T) {
	called := "jobs:\n  b:\n    steps:\n      - uses: parallelworks/checkout\n        with: {repo: https://h/tools.git, branch: main}\n      - uses: parallelworks/checkout\n        with: {repo: 'http://h/bad.git', branch: main}\n"
	s := treeSources(map[string]string{"github/acme/flows@main": called})
	src := "jobs:\n  a:\n    steps:\n      - uses: github/acme/flows@main\n"
	c, diags, err := compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: s})
	if err != nil {
		t.Fatal(err)
	}
	errs := diags.Errors()
	if len(errs) != 1 || errs[0].Code != problems.CheckoutRepoInvalid.Code() || errs[0].Pointer != "#/jobs/b/steps/1/with/repo" || errs[0].Params[problems.ParamFile] == "workflow.yaml" {
		t.Errorf("the problems are %v", errs)
	}
	if c != nil {
		t.Fatalf("a run was created despite %v", errs)
	}
	called = strings.Replace(called, "http://h/bad.git", "https://h/good.git", 1)
	s = treeSources(map[string]string{"github/acme/flows@main": called})
	c, diags, err = compile.Create(t.Context(), "workflow.yaml", []byte(src), nil, compile.Options{Sources: s})
	if err != nil || c == nil {
		t.Fatal(diags, err)
	}
	digest := c.Plan.Jobs[0].Steps[0].Source.Digest
	want := []plan.CheckoutPin{
		{Caller: digest, Step: "/jobs/b/steps/0", Checkout: plan.Checkout{Repo: "https://h/tools.git", Revision: "main", Commit: "89abcdef0123456789abcdef0123456789abcdef"}},
		{Caller: digest, Step: "/jobs/b/steps/1", Checkout: plan.Checkout{Repo: "https://h/good.git", Revision: "main", Commit: "89abcdef0123456789abcdef0123456789abcdef"}},
	}
	if !slices.Equal(c.Plan.Checkouts, want) || c.Plan.Version != 2 {
		t.Errorf("the tree's checkouts are %+v, in a plan of version %d; want %+v", c.Plan.Checkouts, c.Plan.Version, want)
	}
}
