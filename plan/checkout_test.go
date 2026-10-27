package plan_test

import (
	"errors"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// TestFindCheckout checks that a plan's checkouts are in the order of
// their callers and steps, each once, as a plan is decoded, which
// FindCheckout finds them by; and that a plan with one is of version 2.
func TestFindCheckout(t *testing.T) {
	p := samplePlan(t)
	pin := func(caller, step string) plan.CheckoutPin {
		return plan.CheckoutPin{Caller: caller, Step: step, Checkout: plan.Checkout{Repo: "https://example.com/" + caller + step, Revision: "main", Commit: "0123456789abcdef0123456789abcdef01234567"}}
	}
	p.Checkouts = []plan.CheckoutPin{pin("sha256:a", "/jobs/x/steps/0"), pin("sha256:a", "/jobs/x/steps/1"), pin("sha256:b", "/jobs/x/steps/0")}
	p.Version = p.MinVersion()
	if p.Version != 2 {
		t.Errorf("a plan with checkouts is of version %d", p.Version)
	}
	data, err := plan.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	back, err := plan.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if co, found := back.FindCheckout("sha256:a", "/jobs/x/steps/1"); !found || co.Repo != "https://example.com/sha256:a/jobs/x/steps/1" {
		t.Errorf("FindCheckout = %+v, %v", co, found)
	}
	if _, found := back.FindCheckout("sha256:a", "/jobs/x/steps/2"); found {
		t.Error("FindCheckout found a checkout the plan hasn't")
	}
	for name, pins := range map[string][]plan.CheckoutPin{
		"twice":        {pin("sha256:a", "/jobs/x/steps/0"), pin("sha256:a", "/jobs/x/steps/0")},
		"out of order": {pin("sha256:b", "/jobs/x/steps/0"), pin("sha256:a", "/jobs/x/steps/0")},
	} {
		p.Checkouts = pins
		data, err := plan.Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.Decode(data); !errors.Is(err, plan.ErrMalformed) {
			t.Errorf("checkouts %s: %v", name, err)
		}
	}
}

// TestCheckRepo checks which repositories a checkout may name: an https or
// ssh URL, or an scp-like one, on any host, with no credential; and the
// reason each other one is refused for, with any credential masked where
// the problem shows the repository.
func TestCheckRepo(t *testing.T) {
	for _, repo := range []string{
		"https://github.com/acme/app.git",
		"https://git.example.com:8443/team/app",
		"ssh://git@github.com/acme/app.git",
		"ssh://github.com:2222/acme/app",
		"git+ssh://git@host/app",
		"git@github.com:acme/app.git",
		"host.example.com:app",
		"[::1]:app.git",
		"deploy@[2001:db8::1]:team/app.git",
		"ssh://deploy@[2001:db8::1]:2222/team/app",
		"HTTPS://github.com/acme/app",
	} {
		if err := plan.CheckRepo(repo); err != nil {
			t.Errorf("%s: %v", repo, err)
		}
	}
	for _, tc := range []struct {
		repo, reason, shown string
	}{
		{"http://github.com/acme/app.git", plan.RepoTransport, ""},
		{"git://github.com/acme/app.git", plan.RepoTransport, ""},
		{"file:///srv/app.git", plan.RepoTransport, ""},
		{"ext::sh -c touch% /tmp/x", plan.RepoForm, ""},
		{"ext::ssh-helper", plan.RepoTransport, ""},
		{"/srv/app.git", plan.RepoTransport, ""},
		{"./app", plan.RepoTransport, ""},
		{"ftp://host/app", plan.RepoTransport, ""},
		{"https://user:ghp_secret@github.com/acme/app.git", plan.RepoCredential, "https://" + problems.Masked + "@github.com/acme/app.git"},
		{"https://ghp_secret@github.com/acme/app.git", plan.RepoCredential, "https://" + problems.Masked + "@github.com/acme/app.git"},
		{"ssh://git:secret@github.com/acme/app", plan.RepoCredential, "ssh://" + problems.Masked + "@github.com/acme/app"},
		{"git:secret@github.com:acme/app", plan.RepoCredential, problems.Masked + "@github.com:acme/app"},
		{"", plan.RepoForm, ""},
		{"-oProxyCommand=x", plan.RepoForm, ""},
		{"ssh://-oProxyCommand=x/app", plan.RepoForm, ""},
		{"https://github.com/", plan.RepoForm, ""},
		{"https://github.com/acme/ app", plan.RepoForm, ""},
		{"host:", plan.RepoForm, ""},
		{"ssh://-oProxyCommand=x@host/app", plan.RepoForm, "ssh://" + problems.Masked + "@host/app"},
		{"-oProxyCommand=x@[::1]:app", plan.RepoForm, ""},
		{"[]:app", plan.RepoForm, ""},
		{"[-oProxyCommand=x]:app", plan.RepoForm, ""},
		{"deploy@[-oX]:app", plan.RepoForm, ""},
		{"[::1]:", plan.RepoForm, ""},
		{"[::1]app", plan.RepoTransport, ""},
		{"dir/[::1]:app", plan.RepoTransport, ""},
	} {
		p := kindtest.Want(t, plan.CheckRepo(tc.repo), problems.CheckoutRepoInvalid)
		shown := tc.shown
		if shown == "" {
			shown = tc.repo
		}
		if p.Params["reason"] != tc.reason || p.Params["repo"] != shown {
			t.Errorf("%q: refused for %v, shown as %v; want %s, %s", tc.repo, p.Params["reason"], p.Params["repo"], tc.reason, shown)
		}
	}
}

// TestCheckCheckoutPath checks which sparse_checkout entries are literal
// paths from a repository's root.
func TestCheckCheckoutPath(t *testing.T) {
	for _, path := range []string{"app", "workflows/spack-builder/app", "tools/utils/", "a b", "file.txt", ".github/x"} {
		if err := plan.CheckCheckoutPath(path); err != nil {
			t.Errorf("%q: %v", path, err)
		}
	}
	for _, path := range []string{"", "/", "/app", "a//b", "./a", "a/./b", "a/../b", "..", "*.go", "a?", "[ab]", "!a", "a\nb", "a\x00"} {
		kindtest.Want(t, plan.CheckCheckoutPath(path), problems.CheckoutPathInvalid)
	}
}
