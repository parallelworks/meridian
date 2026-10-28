package plan

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/internal/sshname"
	"github.com/parallelworks/meridian/problems"
)

// CheckoutUses is the uses: of a step that checks out a Git repository
// (docs/design.md, Built-in actions).
const CheckoutUses = "parallelworks/checkout"

// A Checkout is a parallelworks/checkout step's repository and revision,
// as its with:'s repo and branch write them out, pinned to the commit the
// revision named as the run was created, so that the run and its reruns
// check out the same tree, however the revision has moved since (Step,
// CheckoutPin). Version 2.
type Checkout struct {
	// Repo is the repository's URL, as with: repo writes it (CheckRepo).
	Repo string `json:"repo"`
	// Revision is the branch, tag or commit that with: branch names.
	Revision string `json:"revision"`
	// Commit is the commit Revision named, in full, as git writes it.
	Commit string `json:"commit"`
}

// A CheckoutPin is the pin of a parallelworks/checkout step of a workflow
// the run calls, as Call is of a step that calls one: by the digest of the
// text of the workflow whose step it is (Source.Digest), and the step's
// JSON Pointer in that text, so that a child run's Create reads its steps'
// pins rather than pin them again. Version 2.
type CheckoutPin struct {
	Caller   string   `json:"caller"`
	Step     string   `json:"step"`
	Checkout Checkout `json:"checkout"`
}

// checkCheckouts checks that the plan's checkouts are in the order of
// their callers and steps, each once, as Checkouts is, since a child run's
// Create finds its steps' pins by them in that order.
func (p *Plan) checkCheckouts() error {
	for i := 1; i < len(p.Checkouts); i++ {
		a, b := p.Checkouts[i-1], p.Checkouts[i]
		switch c := cmp.Or(strings.Compare(a.Caller, b.Caller), strings.Compare(a.Step, b.Step)); {
		case c == 0:
			return fmt.Errorf("the checkout of %s's %s is in the plan twice", b.Caller, b.Step)
		case c > 0:
			return fmt.Errorf("the checkout of %s's %s comes after %s's %s", b.Caller, b.Step, a.Caller, a.Step)
		}
	}
	return nil
}

// FindCheckout returns the pin of the checkout step at the JSON Pointer
// step of the workflow of the digest caller, among those the plan's
// checkouts hold, as a child run's Create pins its own checkouts by.
func (p *Plan) FindCheckout(caller, step string) (Checkout, bool) {
	i, ok := slices.BinarySearchFunc(p.Checkouts, [2]string{caller, step}, func(c CheckoutPin, k [2]string) int {
		return cmp.Or(strings.Compare(c.Caller, k[0]), strings.Compare(c.Step, k[1]))
	})
	if !ok {
		return Checkout{}, false
	}
	return p.Checkouts[i].Checkout, true
}

// The reasons CheckRepo gives for a repository Meridian doesn't fetch, as
// a checkout_repo_invalid problem's reason param.
const (
	// RepoTransport is a URL of another transport than https or ssh: a
	// local path, file://, ext:: and the other remote helpers, which would
	// have the coordinator read its host's files, or run a command there;
	// and git:// and http://, which have no TLS or SSH, so that nothing
	// fetched would be authenticated.
	RepoTransport = "transport"
	// RepoCredential is a URL that holds a credential: an https URL's
	// user and password, or a token alone, before its @, or an ssh URL's
	// password, which would put a secret in the plan.
	RepoCredential = "credential"
	// RepoForm is what git takes for no URL, or for an option.
	RepoForm = "form"
	// RepoName is an ssh URL's, or an scp-like one's, host or user that
	// holds what ssh would read as more than a name (sshname): git gives
	// them to ssh, which puts them in a ProxyCommand or a Match exec of the
	// user's config, which a shell runs. OpenSSH 10.1 and later refuses
	// the same names itself.
	RepoName = "name"
)

// sshSchemes are the schemes git reaches a repository over SSH by.
var sshSchemes = []string{"ssh", "git+ssh", "ssh+git"}

// CheckRepo checks repo, a parallelworks/checkout step's with: repo, as it
// is written, wherever the run is created: a URL git takes over https or
// ssh, as https://host/path, ssh://[user@]host[:port]/path, or
// [user@]host:path, scp-like, with an IPv6 address in brackets, as
// [::1]:path, on any host, as format 1 takes, with no
// credential in it, and, over ssh, a host and a user that ssh would read
// as names alone (sshname), as git decodes them from a URL's
// percent-escapes. Anything else is a checkout_repo_invalid problem, with
// the repository as ShowRepo shows it, and the reason: RepoTransport,
// RepoCredential, RepoForm or RepoName. A Sources resolver can refuse
// more, as a platform's can refuse a host.
func CheckRepo(repo string) error {
	invalid := func(reason string) error {
		return problems.CheckoutRepoInvalid.Problem(problems.Params{"repo": ShowRepo(repo), "reason": reason})
	}
	if repo == "" || strings.ContainsFunc(repo, func(c rune) bool { return c <= ' ' || c == 0x7f }) || strings.HasPrefix(repo, "-") {
		return invalid(RepoForm)
	}
	scheme, rest, isURL := strings.Cut(repo, "://")
	switch {
	case isURL && strings.ContainsAny(scheme, "/:"):
		// A remote helper's transport::address, or a path with :// in it.
		return invalid(RepoTransport)
	case isURL:
		scheme = strings.ToLower(scheme)
		if scheme != "https" && !slices.Contains(sshSchemes, scheme) {
			return invalid(RepoTransport)
		}
		authority, path, _ := strings.Cut(rest, "/")
		user, host, hasUser := strings.Cut(authority, "@")
		if !hasUser {
			host, user = user, ""
		}
		switch {
		case hasUser && (scheme == "https" || strings.Contains(user, ":")):
			return invalid(RepoCredential)
		case host == "" || strings.HasPrefix(host, "-") || strings.HasPrefix(user, "-") || strings.Trim(path, "/") == "":
			return invalid(RepoForm)
		case scheme != "https" && !sshNames(urlDecode(host), urlDecode(user)):
			return invalid(RepoName)
		}
		return nil
	}
	// Without a scheme, git reads [user@]host:path, whose host has no
	// slash before its colon, as an ssh URL, and so [user@][address]:path,
	// with an IPv6 address in brackets; and anything else as a local path,
	// as it does a remote helper's transport::address. A user and a
	// password, user:password@, before it, it would read as a host, but it
	// would put a password in the plan.
	if scpUserinfo(repo) >= 0 {
		return invalid(RepoCredential)
	}
	if user, address, path, ok := scpBracketed(repo); ok {
		if address == "" || strings.HasPrefix(address, "-") || strings.HasPrefix(user, "-") || strings.Trim(path, "/") == "" {
			return invalid(RepoForm)
		}
		if !sshNames(address, user) {
			return invalid(RepoName)
		}
		return nil
	}
	authority, path, scp := strings.Cut(repo, ":")
	if !scp || strings.Contains(authority, "/") || strings.HasPrefix(path, ":") {
		return invalid(RepoTransport)
	}
	user, host, hasUser := strings.Cut(authority, "@")
	if !hasUser {
		host, user = user, ""
	}
	if host == "" || strings.HasPrefix(host, "-") || strings.HasPrefix(user, "-") || strings.Trim(path, "/") == "" {
		return invalid(RepoForm)
	}
	if !sshNames(host, user) {
		return invalid(RepoName)
	}
	return nil
}

// sshNames reports whether host, with its port, if it has one, or an
// address in brackets, and user, of a repository git reaches over ssh,
// are safe to give ssh (sshname).
func sshNames(host, user string) bool {
	return sshname.ValidHost(host) && sshname.ValidUser(user)
}

// urlDecode decodes s, a part of a URL, as git does a URL it reaches over
// ssh before it gives ssh its host and user: each % and two hex digits is
// the byte they write, and anything else is as it is.
func urlDecode(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// ShowRepo returns repo, a checkout's with: repo, as a problem shows it: an
// URL's userinfo, before its authority's @, masked, since it can hold a
// password or a token.
func ShowRepo(repo string) string {
	scheme, rest, isURL := strings.Cut(repo, "://")
	if !isURL {
		if at := scpUserinfo(repo); at >= 0 {
			return problems.Masked + repo[at:]
		}
		return repo
	}
	authority, path, hasPath := strings.Cut(rest, "/")
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return repo
	}
	shown := scheme + "://" + problems.Masked + authority[at:]
	if hasPath {
		shown += "/" + path
	}
	return shown
}

// scpBracketed splits repo, which has no scheme, if it is scp-like with an
// address in brackets, as [user@][address]:path, with no slash before the
// bracket: into its user, its address and its path.
func scpBracketed(repo string) (user, address, path string, ok bool) {
	open := strings.Index(repo, "[")
	if open < 0 || open > 0 && repo[open-1] != '@' || strings.Contains(repo[:open], "/") {
		return "", "", "", false
	}
	address, path, ok = strings.Cut(repo[open+1:], "]:")
	if !ok || strings.ContainsAny(address, "/]") {
		return "", "", "", false
	}
	return strings.TrimSuffix(repo[:open], "@"), address, path, true
}

// scpUserinfo returns where the @ after a user and a password, as
// user:password@, ends them in repo, which has no scheme, or -1 for none:
// its first @, with a colon, and no slash, before it.
func scpUserinfo(repo string) int {
	at := strings.Index(repo, "@")
	if at < 0 || strings.Contains(repo[:at], "/") || !strings.Contains(repo[:at], ":") {
		return -1
	}
	return at
}

// CheckCheckoutPath checks path, an entry of a parallelworks/checkout
// step's with: sparse_checkout: a literal path from the repository's root,
// to a file or a directory, which a slash can end. One that isn't, a
// checkout_path_invalid problem, is one that is empty, starts with a
// slash, has an empty name, . or .. in it, has a glob's *, ? or [ in it,
// or starts with !, which format 1 can read as a pattern, or has a control
// character in it.
func CheckCheckoutPath(path string) error {
	invalid := problems.CheckoutPathInvalid.Problem(problems.Params{"path": path})
	trimmed := strings.TrimSuffix(path, "/")
	if trimmed == "" || strings.HasPrefix(path, "!") || strings.ContainsAny(path, "*?[") ||
		strings.ContainsFunc(path, func(c rune) bool { return c < ' ' || c == 0x7f }) {
		return invalid
	}
	for name := range strings.SplitSeq(trimmed, "/") {
		if name == "" || name == "." || name == ".." {
			return invalid
		}
	}
	return nil
}
