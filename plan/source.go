package plan

import (
	"regexp"
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// The names in a reference to a workflow in Git, as format 1 reads them, as
// ACTIVATE does: a GitHub owner's is letters, digits and hyphens; a GitHub
// repository's, and a GitLab group's or project's, can hold dots and
// underscores too; a revision can hold slashes too; and a server is a DNS
// name of one label or more, with no port, whose labels can hold
// underscores.
var (
	ownerPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)
	namePattern  = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	revPattern   = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
	hostPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$`)
)

// Check returns what is wrong with r as format 1 reads a reference, as
// ACTIVATE refuses it, or nil, so that a reference names a workflow, or
// doesn't, wherever the run is created. It reads a Git repository's, one
// whose uses: starts with github/ or gitlab/, once it is normal (Normal):
// on GitHub, an owner and a repository, and nothing after them; on GitLab,
// a project in a group or more; each of their names as its pattern says,
// and not . or ..; a revision after the first @, if there is one, that
// isn't empty and has no .. in it; a file, $yaml, with no .. among its
// names, which would leave the repository; and on GitLab, a server, $host,
// that is a DNS name. What is wrong with $yaml or $host is
// source_with_invalid, and with the rest source_ref_invalid, each a
// *problem.Problem. It says nothing of any other reference, as one to a
// workflow a platform stores, marketplace/ or workflow/, which is the
// platform's to read. A Sources resolver can refuse more, as git's rules
// for a branch's name.
func (r SourceRef) Check() error {
	norm := r.Normal()
	kind, rest, _ := strings.Cut(norm.Uses, "/")
	if kind != "github" && kind != "gitlab" {
		return nil
	}
	invalid := problems.SourceRefInvalid.Problem(problems.Params{"ref": r.Uses})
	name, rev, hasRev := strings.Cut(rest, "@")
	if hasRev && (!revPattern.MatchString(rev) || strings.Contains(rev, "..")) {
		return invalid
	}
	names := strings.Split(name, "/")
	if kind == "github" && (len(names) != 2 || !ownerPattern.MatchString(names[0])) || len(names) < 2 {
		return invalid
	}
	for _, n := range names {
		if !namePattern.MatchString(n) || n == "." || n == ".." {
			return invalid
		}
	}
	for n := range strings.SplitSeq(norm.Path, "/") {
		if n == ".." {
			return problems.SourceWithInvalid.Problem(problems.Params{"key": "$yaml", "what": "file"})
		}
	}
	if kind == "gitlab" && !ValidHost(norm.Host) {
		return problems.SourceWithInvalid.Problem(problems.Params{"key": "$host", "what": "server"})
	}
	return nil
}

// ValidHost reports whether host names a server as format 1 reads a
// GitLab reference's $host: a DNS name of one label or more, with no port.
func ValidHost(host string) bool {
	return hostPattern.MatchString(host)
}
