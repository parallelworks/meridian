package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// SourcesDir is the name of the directory, in the cache directory, that
// holds the repositories workflows' uses: name.
const SourcesDir = "sources"

// maxSourceSize is the largest workflow file Sources reads. The compiler
// has its own, smaller limit, which it reports as a problem with the
// workflow; this one only keeps a file that is far too large out of
// memory.
const maxSourceSize = 16 << 20

// waitDelay is how long a git command that was cancelled may hold its
// output open, as a helper it started can, before Sources stops waiting.
const waitDelay = time.Second

// Sources reads the workflows that a step's uses: names, from Git
// repositories, by running the git command, which reaches them with the
// user's own Git configuration and credentials. A reference's uses: is, as
// ACTIVATE reads it (plan.SourceRef.Normal, parseRef),
//
//	github/<owner>/<repository>[@<revision>]
//	gitlab/<group>[/<subgroup>...]/<project>[@<revision>]
//
// for a repository on github.com, or a project on gitlab.com, in its
// groups, where revision is a branch, a tag or a commit, the remote's
// default branch by default. The workflow's file in it is the step's with:
// $yaml, workflow.yaml by default; and $host can name the GitLab server
// that holds it, gitlab.com by default, by its name, with no port, which
// must be one the user registered (NewSources): otherwise a workflow could
// have git, with the user's configuration and credentials, reach a server
// of its choosing. A reference to a workflow a platform stores,
// marketplace/... or workflow/..., names one here only where the user maps
// it to one of those (Workflows), which is then read as that one is.
//
// Resolve pins the reference to the commit its revision names now and to
// the SHA-256 digest of the file's text there, and Read reads it again by
// that pin, whatever the revision names by then, and fails if the text
// isn't the digest's. Sources keeps a bare copy of each repository it
// reads in the cache directory, holding each commit it has pinned, so that
// a rerun reads it without the network. On GitHub a commit is served from
// a repository's whole fork network, so a commit that a reference names
// may be one that only a fork has (docs/design.md, Security).
//
// git must be Git 2.29 or later, the first whose fetch takes
// --no-write-fetch-head, which Sources passes so that a fetch leaves no
// FETCH_HEAD in the copy; an older one fails the fetch.
//
// git runs with no hooks, without fetching submodules, checking the
// objects it fetches, and with no automatic maintenance, which would go
// on writing into a copy after git returned. It never asks for a
// password, by a prompt or an askpass program, and its ssh never asks a
// question when the user names no ssh command of their own. It runs in a
// session of its own, with no terminal, so that an ssh command of the
// user's that would ask for a passphrase fails rather than waits. A
// cancelled context stops it and every process it started.
// Sources serializes what it does to one repository's copy, across
// processes too where the system has file locks, and otherwise tries
// again, a few times, a git command another process held a lock against.
//
// The digest is computed here, with the standard library's FIPS 140-3
// module. Git itself names objects by SHA-1 or SHA-256 and reaches remotes
// over TLS or SSH with the host's cryptography, outside Meridian's module,
// which a FIPS host configures for the whole system.
//
// What is wrong with a reference is a problem of one of the problems
// package's kinds, which asking again won't change; one that names no
// workflow, such as a revision that isn't there, wraps compile.ErrNotFound
// too. Any other error is git's, or the system's, such as a network that
// is down, which may not happen again.
type Sources struct {
	dir string
	// git is the git command.
	git string
	// remote returns the URL of the repository repo on host; and repoURL
	// the URL git fetches a checkout's repository from, repo itself.
	remote  func(host, repo string) string
	repoURL func(repo string) string
	// protocols are the transports a fetch may use, whatever URL it ends
	// up at, through a redirect or an insteadOf of the user's: https and
	// ssh, and file for a test's repositories.
	protocols []string
	// servers are the GitLab servers, besides gitlab.com, that a
	// reference's $host can name.
	servers map[string]bool
	// workflows map workflows a platform stores to workflows in Git.
	workflows Workflows

	mu    sync.Mutex
	repos map[string]chan struct{}

	sshMu sync.Mutex
	// sshKnown says whether ssh is known: what git runs as its ssh
	// command, when the user names none, one that never prompts, or ""
	// when the user names their own.
	sshKnown bool
	ssh      string
}

var _ compile.Sources = (*Sources)(nil)

// NewSources returns the Sources whose copies of repositories are in dir,
// the cache directory, which must be readable only by its user, as
// Dirs.EnsureCache makes it, and which reaches the GitLab servers, besides
// gitlab.com, that servers names, as Config.GitLabServers gives them, and
// no others; and which reads the workflows a platform stores that
// workflows maps, as Config.Workflows gives them, and no others.
func NewSources(dir string, servers []string, workflows Workflows) *Sources {
	s := &Sources{
		dir:       filepath.Join(dir, SourcesDir),
		git:       "git",
		remote:    func(host, repo string) string { return "https://" + host + "/" + repo + ".git" },
		repoURL:   func(repo string) string { return repo },
		protocols: []string{"https", "ssh"},
		servers:   map[string]bool{},
		workflows: maps.Clone(workflows),
		repos:     map[string]chan struct{}{},
	}
	for _, server := range servers {
		s.servers[strings.ToLower(server)] = true
	}
	return s
}

// A sourceRef is a reference to a workflow in a Git repository, as
// parseRef reads it.
type sourceRef struct {
	// from is the reference, as Normal reads it, to the workflow a
	// platform stores that the user mapped to this one (Workflows), or "".
	from string
	// kind is github or gitlab, and host its server.
	kind, host string
	// repo is the repository, as owner/name, or a GitLab project's path,
	// as group/subgroup/project.
	repo string
	// path is the workflow's file in the repository.
	path string
	// rev is the revision, or "" for the remote's default branch.
	rev string
}

// hosts are the servers a reference's first name stands for.
var hosts = map[string]string{"github": "github.com", "gitlab": "gitlab.com"}

// platformRefs start the references to workflows a platform stores.
var platformRefs = []string{"marketplace/", "workflow/"}

// onPlatform reports whether uses names a workflow a platform stores.
func onPlatform(uses string) bool {
	return slices.ContainsFunc(platformRefs, func(p string) bool { return strings.HasPrefix(uses, p) })
}

// Workflows map the workflows a platform stores, as sources.yaml's
// workflows do, to workflows in Git repositories, each by its reference,
// as Normal reads it: marketplace/<slug>/<version> or workflow/<name>,
// one workflow; or marketplace/<slug>/{version}, each version of the slug
// that no key of its own names but latest, which is the item's latest as
// its platform decides it, so only a key of its own maps it. The workflow
// in Git is a reference as a step writes one, its uses:, and its $yaml and
// $host as Path and Host, which a slug's mapping can write the version in,
// as {version}: in its Path, and in its uses:'s revision, after its @.
type Workflows map[string]plan.SourceRef

// VersionTemplate is what stands for a marketplace workflow's version in a
// slug's mapping (Workflows).
const VersionTemplate = "{version}"

// versionPattern is a version a slug's mapping takes: letters, digits, .,
// _, + and -, not first a dot, which could name a parent directory or a
// hidden file, nor a hyphen, which git could take for an option.
var versionPattern = regexp.MustCompile(`^[A-Za-z0-9_+][A-Za-z0-9._+-]*$`)

// MappingKey reads key, a key of sources.yaml's workflows: the reference
// to the workflow a platform stores that it maps, as Normal reads it, and
// whether it is a slug's, which maps each of its versions. It reports
// whether key is one: a reference to a marketplace workflow by its slug
// and version, or to a saved workflow by its name, in which nothing holds
// a brace but a slug's whole version, {version}.
func MappingKey(key string) (normal string, slug, ok bool) {
	normal = plan.SourceRef{Uses: key}.Normal().Uses
	if item, isItem := strings.CutPrefix(normal, "marketplace/"); isItem {
		name, version, _ := strings.Cut(item, "/")
		slug = version == VersionTemplate
		return normal, slug, name != "" && !strings.ContainsAny(name, "{}") && (slug || !strings.ContainsAny(version, "{}"))
	}
	name, isSaved := strings.CutPrefix(normal, "workflow/")
	return normal, false, isSaved && name != "" && !strings.ContainsAny(name, "{}")
}

// MappingInvalid returns what of to, a workflow in Git that a key of
// sources.yaml's workflows maps a workflow a platform stores to, a slug's
// if slug, isn't what a step's reference to one is: platform, for another
// workflow a platform stores; template, for a version template where it
// can't be; uses, yaml or host, for what format 1, and these Sources,
// refuse of a reference's uses:, $yaml or $host; or "" for none. A slug's
// mapping is checked with a version in its template's place.
func MappingInvalid(to plan.SourceRef, slug bool) string {
	if onPlatform(to.Uses) {
		return "platform"
	}
	if !strings.HasPrefix(to.Uses, "github/") && !strings.HasPrefix(to.Uses, "gitlab/") {
		return "uses"
	}
	name, rev, _ := strings.Cut(to.Uses, "@")
	if strings.Contains(name, VersionTemplate) || strings.Contains(to.Host, VersionTemplate) ||
		!slug && (strings.Contains(rev, VersionTemplate) || strings.Contains(to.Path, VersionTemplate)) {
		return "template"
	}
	if slug {
		to = withVersion(to, "0")
	}
	_, err := parseRef(to)
	p, ok := errors.AsType[*problem.Problem](err)
	switch {
	case err == nil:
		return ""
	case ok && p.Code == problems.SourceWithInvalid.Code() && p.Params["key"] == "$yaml":
		return "yaml"
	case ok && p.Code == problems.SourceWithInvalid.Code():
		return "host"
	}
	return "uses"
}

// withVersion returns to, a slug's mapping, for version: with version for
// {version} in its Path and in its uses:'s revision.
func withVersion(to plan.SourceRef, version string) plan.SourceRef {
	if name, rev, ok := strings.Cut(to.Uses, "@"); ok {
		to.Uses = name + "@" + strings.ReplaceAll(rev, VersionTemplate, version)
	}
	to.Path = strings.ReplaceAll(to.Path, VersionTemplate, version)
	return to
}

// target returns the workflow in Git that w maps ref, to a workflow a
// platform stores, to, and ref as Normal reads it, which w maps by. One
// that w doesn't map names no workflow here: SourceOnPlatform. Of a slug's
// mapping, the version is the uses:'s, which, as it is the workflow's
// text, must be one versionPattern takes, and give a reference parseRef
// takes, before anything reads it (SourceVersionInvalid); and the latest,
// which only the platform knows, isn't one (SourceLatestNotMapped).
func (w Workflows) target(ref plan.SourceRef) (plan.SourceRef, string, error) {
	from := ref.Normal().Uses
	if to, ok := w[from]; ok {
		return to, from, nil
	}
	if item, ok := strings.CutPrefix(from, "marketplace/"); ok {
		slug, version, _ := strings.Cut(item, "/")
		if to, ok := w["marketplace/"+slug+"/"+VersionTemplate]; ok {
			if version == plan.LatestVersion {
				return plan.SourceRef{}, "", notFound(problems.SourceLatestNotMapped, problems.Params{"ref": ref.Uses, "key": from})
			}
			invalid := notFound(problems.SourceVersionInvalid, problems.Params{"ref": ref.Uses, "version": version})
			if !versionPattern.MatchString(version) {
				return plan.SourceRef{}, "", invalid
			}
			to = withVersion(to, version)
			if _, err := parseRef(to); err != nil {
				return plan.SourceRef{}, "", invalid
			}
			return to, from, nil
		}
	}
	return plan.SourceRef{}, "", notFound(problems.SourceOnPlatform, problems.Params{"ref": ref.Uses})
}

// commitPattern is a full commit's name, as git writes it.
var commitPattern = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)

// validHost reports whether host names a server format 1 takes in $host
// (plan.ValidHost) that doesn't start with a hyphen, which git could take
// for an option.
func validHost(host string) bool {
	return plan.ValidHost(host) && !strings.HasPrefix(host, "-")
}

// validName reports whether name, one of a repository's names that format 1
// takes (plan.SourceRef.Check), doesn't start with a hyphen, which git could
// take for an option.
func validName(name string) bool {
	return !strings.HasPrefix(name, "-")
}

// validRevision reports whether rev, a revision format 1 takes
// (plan.SourceRef.Check), is one git takes as a branch or a tag's name:
// none of its names, between its slashes, is empty, starts with a dot or
// ends in .lock, and it doesn't end with a dot, which git's
// check-ref-format refuses, nor start with a hyphen, which git could take
// for an option. ACTIVATE takes those, which then name no branch or tag on
// the server.
func validRevision(rev string) bool {
	if strings.HasPrefix(rev, "-") || strings.HasSuffix(rev, ".") {
		return false
	}
	for name := range strings.SplitSeq(rev, "/") {
		if name == "" || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".lock") {
			return false
		}
	}
	return true
}

// notFound returns a problem of kind k with params, which names no
// workflow here: it wraps compile.ErrNotFound.
func notFound(k *problems.Kind, params problems.Params) error {
	return k.Problem(params).WithCause(compile.ErrNotFound)
}

// parseRef reads a reference as format 1 does (plan.SourceRef.Check), once
// it is normal (plan.SourceRef.Normal): on GitHub, an owner and a
// repository; on GitLab, a project's path, in a group or more; a revision
// after the first @, if there is one; and the file $yaml names, from the
// repository's root, and the GitLab server $host names. One that isn't a
// Git repository's, such as one to a workflow a platform stores, or that
// format 1 refuses, names no workflow here. Besides what format 1 refuses,
// what is the cache's and git's own: a name or a revision that starts with
// a hyphen, which git could take for an option, a revision git refuses as a
// branch's or a tag's name (validRevision), a group's name that ends in
// .git or .lock or starts with a dot, which the cache keeps beside a
// repository's copy, and a file's path that has an empty name in it, as ./
// does, or a control character: source_with_invalid for $yaml, and
// source_ref_invalid for the rest.
func parseRef(ref plan.SourceRef) (sourceRef, error) {
	uses := ref.Uses
	if onPlatform(uses) {
		return sourceRef{}, notFound(problems.SourceOnPlatform, problems.Params{"ref": uses})
	}
	invalid := func() (sourceRef, error) {
		return sourceRef{}, notFound(problems.SourceRefInvalid, problems.Params{"ref": uses})
	}
	norm := ref.Normal()
	kind, rest, _ := strings.Cut(norm.Uses, "/")
	host, isGit := hosts[kind]
	if !isGit {
		return invalid()
	}
	if err := ref.Check(); err != nil {
		if p, ok := errors.AsType[*problem.Problem](err); ok {
			return sourceRef{}, p.WithCause(compile.ErrNotFound)
		}
		return sourceRef{}, err
	}
	name, rev, hasRev := strings.Cut(rest, "@")
	if hasRev && !validRevision(rev) {
		return invalid()
	}
	repo := strings.Split(name, "/")
	for i, p := range repo {
		if !validName(p) {
			return invalid()
		}
		// Each name but the last is a directory of the cache (open), as a
		// GitLab group's is, beside the copies of repositories, which end
		// in .git, their lock files, in .git.lock, and the copies being
		// made, which start with a dot: one that ends so could be another
		// repository's copy, or where its lock goes, and one that starts
		// with a dot where a copy is being made.
		lower := strings.ToLower(p)
		if i < len(repo)-1 && (strings.HasSuffix(lower, ".git") || strings.HasSuffix(lower, ".lock") || strings.HasPrefix(p, ".")) {
			return invalid()
		}
	}
	for p := range strings.SplitSeq(norm.Path, "/") {
		if p == "" || p == "." || strings.ContainsFunc(p, func(c rune) bool { return c < ' ' || c == 0x7f }) {
			return sourceRef{}, notFound(problems.SourceWithInvalid, problems.Params{"key": "$yaml", "what": "file"})
		}
	}
	if kind == "gitlab" {
		// A GitLab project can be on a server of a platform's own.
		if host = norm.Host; !validHost(host) {
			return sourceRef{}, notFound(problems.SourceWithInvalid, problems.Params{"key": "$host", "what": "server"})
		}
	}
	return sourceRef{kind: kind, host: host, repo: name, path: norm.Path, rev: rev}, nil
}

// parse reads a reference, as parseRef does, a workflow a platform stores
// as the workflow in Git the user maps it to (Workflows), and refuses one on
// a GitLab server the user didn't register.
func (s *Sources) parse(ref plan.SourceRef) (sourceRef, error) {
	var from string
	if onPlatform(ref.Uses) {
		to, normal, err := s.workflows.target(ref)
		if err != nil {
			return sourceRef{}, err
		}
		ref, from = to, normal
	}
	r, err := parseRef(ref)
	if err != nil {
		return sourceRef{}, err
	}
	r.from = from
	if r.host != hosts[r.kind] && !s.servers[r.host] {
		return sourceRef{}, notFound(problems.SourceServerUnknown, problems.Params{"server": r.host})
	}
	return r, nil
}

// location is the reference as a plan's Source names it to whoever can
// see the run: its server and repository, its revision after an @, if it
// has one, and after a colon, the workflow's file, as in
// gitlab.com/group/project@main:dir/workflow.yaml. Neither a repository's
// names nor a revision hold an @ or a colon, so where each part ends is
// plain, a GitLab project in subgroups' too. A workflow a platform stores,
// which the user mapped to this one, is named by both, the one a uses:
// names first, as in marketplace/tools/v1 → github.com/acme/flows:tools.yaml.
func (r sourceRef) location() string {
	loc := r.host + "/" + r.repo
	if r.rev != "" {
		loc += "@" + r.rev
	}
	loc += ":" + r.path
	if r.from != "" {
		return r.from + " → " + loc
	}
	return loc
}

// repoName is the repository as a problem shows it: its server and name.
func (r sourceRef) repoName() string { return r.host + "/" + r.repo }

// remoteContext is what the remote context holds for a workflow at r, as
// ACTIVATE's does: the repository's address, the revision, if r names
// one, and the workflow's file, as remote.repo, remote.branch and
// remote.yaml.
func (r sourceRef) remoteContext() map[string]string {
	remote := map[string]string{"repo": "https://" + r.host + "/" + r.repo, "yaml": r.path}
	if r.rev != "" {
		remote["branch"] = r.rev
	}
	return remote
}

// Resolve finds the workflow ref names now, and pins it to its commit and
// its text's digest.
func (s *Sources) Resolve(ctx context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	r, err := s.parse(ref)
	if err != nil {
		return nil, err
	}
	repo, unlock, err := s.open(ctx, r)
	if err != nil {
		return nil, err
	}
	defer unlock()
	rev := r.rev
	if rev == "" {
		rev = "HEAD"
	}
	commit, err := s.fetch(ctx, repo, r, rev)
	if err != nil {
		return nil, err
	}
	text, err := s.blob(ctx, repo, r, commit)
	if err != nil {
		return nil, err
	}
	return &compile.Workflow{
		Source: plan.Source{Location: r.location(), Revision: commit, Digest: digest(text), Ref: ref},
		Text:   text,
		Remote: r.remoteContext(),
	}, nil
}

// Read reads the workflow that Resolve pinned as source, by its Ref at its
// commit, and checks its text against its digest.
func (s *Sources) Read(ctx context.Context, source plan.Source) (*compile.Workflow, error) {
	if source.Ref.Uses == "" || !commitPattern.MatchString(source.Revision) || source.Digest == "" {
		return nil, problems.SourceNotPinned.Problem(problems.Params{"location": source.Location, "revision": source.Revision})
	}
	r, err := s.parse(source.Ref)
	if err != nil {
		return nil, err
	}
	repo, unlock, err := s.open(ctx, r)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if !s.has(ctx, repo, source.Revision) {
		if _, err := s.fetch(ctx, repo, r, source.Revision); err != nil {
			return nil, err
		}
	}
	text, err := s.blob(ctx, repo, r, source.Revision)
	if err != nil {
		return nil, err
	}
	if d := digest(text); d != source.Digest {
		return nil, problems.SourceChanged.Problem(problems.Params{
			"location": source.Location, "commit": source.Revision, "digest": d, "pinned": source.Digest,
		})
	}
	return &compile.Workflow{Source: source, Text: text, Remote: r.remoteContext()}, nil
}

// digest is the SHA-256 digest of text, as a plan's Source writes it.
func digest(text []byte) string {
	sum := sha256.Sum256(text)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// hooksDir is the name of the empty directory, in the sources directory,
// that git takes its hooks from: none.
const hooksDir = ".hooks"

// open returns the copy of r's repository, creating it if it isn't there,
// locked against the rest of this process, and other processes where the
// system has file locks, until unlock.
func (s *Sources) open(ctx context.Context, r sourceRef) (repo string, unlock func(), err error) {
	// parseRef has checked that each name is one, and not . or .., and
	// that the server is a DNS name.
	owner, name, _ := strings.Cut(r.repo, "/")
	return s.openAt(ctx, filepath.Join(s.dir, r.host, owner, name+".git"))
}

// openAt returns the copy at repo, in the sources directory, creating it if
// it isn't there, locked as open says.
func (s *Sources) openAt(ctx context.Context, repo string) (string, func(), error) {
	if err := os.MkdirAll(filepath.Dir(repo), 0o700); err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(filepath.Join(s.dir, hooksDir), 0o700); err != nil {
		return "", nil, err
	}
	release, err := s.lockRepo(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	unlockFile, err := lockFile(ctx, repo+".lock")
	if err != nil {
		release()
		return "", nil, err
	}
	unlock := func() {
		unlockFile()
		release()
	}
	if _, err := os.Stat(filepath.Join(repo, "HEAD")); err == nil {
		return repo, unlock, nil
	}
	if err := s.create(ctx, repo); err != nil {
		unlock()
		return "", nil, err
	}
	return repo, unlock, nil
}

// create creates the bare repository repo, whole or not at all: in a
// directory of its own, which it then renames to repo, with no template,
// so that it has no hooks or other files a template would give it.
func (s *Sources) create(ctx context.Context, repo string) error {
	tmp, err := os.MkdirTemp(filepath.Dir(repo), "."+filepath.Base(repo)+"-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if _, err := s.run(ctx, "", "", "init", "--bare", "--quiet", "--template=", "--", tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, repo); err != nil {
		// Another process that holds no file lock made it meanwhile.
		if _, statErr := os.Stat(filepath.Join(repo, "HEAD")); statErr == nil {
			return nil
		}
		return err
	}
	return nil
}

// lockRepo locks repo's copy against the rest of this process until the
// function it returns is called, or returns the context's error if it ends
// first.
func (s *Sources) lockRepo(ctx context.Context, repo string) (func(), error) {
	s.mu.Lock()
	ch, ok := s.repos[repo]
	if !ok {
		ch = make(chan struct{}, 1)
		s.repos[repo] = ch
	}
	s.mu.Unlock()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fetch fetches rev of r's repository into repo, keeps its commit, and
// returns it.
func (s *Sources) fetch(ctx context.Context, repo string, r sourceRef, rev string) (string, error) {
	return s.fetchFrom(ctx, repo, s.remote(r.host, r.repo), rev, r.repoName(), r.location())
}

// fetchFrom fetches rev from url into repo, keeps its commit, and returns
// it. name and location name the repository in a problem: as
// source_revision_not_found's repo, and the location of the rest.
func (s *Sources) fetchFrom(ctx context.Context, repo, url, rev, name, location string) (string, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	// Each fetch has a ref of its own, so that two at once never read each
	// other's.
	fetched := "refs/meridian/fetch/" + hex.EncodeToString(nonce[:])
	// Only the transports s allows, whatever the URL a redirect or an
	// insteadOf of the user's makes of url: none that would have git read
	// the host's files, run a command, or fetch without TLS or SSH, unless
	// the user's own configuration allows one by its name
	// (protocol.<name>.allow), which git reads before protocol.allow.
	args := []string{"-c", "protocol.allow=never"}
	for _, p := range s.protocols {
		args = append(args, "-c", "protocol."+p+".allow=always")
	}
	args = append(args,
		// A transfer that slows below a kilobyte a second for a minute is
		// stopped, so that a remote that hangs doesn't hang Meridian when
		// its context has no deadline.
		"-c", "http.lowSpeedLimit=1000", "-c", "http.lowSpeedTime=60",
		"-c", "fetch.fsckObjects=true",
		"fetch", "--quiet", "--no-tags", "--no-write-fetch-head", "--recurse-submodules=no", "--depth=1",
		"--", url, "+"+rev+":"+fetched)
	if _, err := s.run(ctx, repo, "", args...); err != nil {
		gerr, ok := errors.AsType[*gitError](err)
		switch {
		case !ok:
			return "", err
		case gerr.notFound():
			return "", notFound(problems.SourceRevisionNotFound, problems.Params{"repo": name, "revision": rev, "reason": gerr.message()})
		case gerr.denied():
			return "", problems.SourceAccessDenied.Problem(problems.Params{"location": location, "reason": gerr.message()})
		case gerr.malformed():
			return "", problems.SourceObjectsInvalid.Problem(problems.Params{"location": location, "reason": gerr.message()})
		}
		return "", err
	}
	out, err := s.run(ctx, repo, "", "rev-parse", "--verify", fetched+"^{commit}")
	if err != nil {
		if gerr, ok := errors.AsType[*gitError](err); ok {
			return "", notFound(problems.SourceRevisionNotFound, problems.Params{"repo": name, "revision": rev, "reason": gerr.message()})
		}
		return "", err
	}
	commit := strings.TrimSpace(string(out))
	if !commitPattern.MatchString(commit) {
		return "", fmt.Errorf("local: git gave %q as the commit of %s", commit, rev)
	}
	// The commit keeps a ref, so that the copy keeps it for reruns.
	if _, err := s.run(ctx, repo, "", "update-ref", "refs/meridian/pinned/"+commit, commit); err != nil {
		return "", err
	}
	if _, err := s.run(ctx, repo, "", "update-ref", "-d", fetched); err != nil {
		return "", err
	}
	return commit, nil
}

// has says whether repo has the commit.
func (s *Sources) has(ctx context.Context, repo, commit string) bool {
	_, err := s.run(ctx, repo, "", "cat-file", "-e", commit+"^{commit}")
	return err == nil
}

// blob reads r's file at commit in repo.
func (s *Sources) blob(ctx context.Context, repo string, r sourceRef, commit string) ([]byte, error) {
	object := commit + ":" + r.path
	out, err := s.run(ctx, repo, object+"\n", "cat-file", "--batch-check=%(objecttype) %(objectsize)")
	if err != nil {
		return nil, err
	}
	typ, size, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if typ != "blob" {
		return nil, notFound(problems.SourceFileNotFound, problems.Params{"repo": r.repoName(), "path": r.path, "commit": commit})
	}
	if n, err := strconv.ParseInt(size, 10, 64); err != nil || n > maxSourceSize {
		return nil, problems.SourceTooLarge.Problem(problems.Params{"location": r.location(), "commit": commit, "limit": maxSourceSize})
	}
	return s.run(ctx, repo, "", "cat-file", "blob", object)
}

// maxTries is how many times Sources runs a git command that another
// process held a lock against, where the system has no file locks.
const maxTries = 5

// run runs git with args, in repo when it isn't "", with stdin as its
// input, and returns what it writes.
func (s *Sources) run(ctx context.Context, repo, stdin string, args ...string) ([]byte, error) {
	for try := 1; ; try++ {
		out, err := s.runOnce(ctx, repo, stdin, args...)
		gerr, ok := errors.AsType[*gitError](err)
		if !retryContention || !ok || !gerr.contended() || try == maxTries {
			return out, err
		}
		timer := time.NewTimer(time.Duration(try) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// noMaintenance keeps git from starting work that outlives it. A command
// such as fetch ends by starting automatic maintenance, git maintenance
// run --auto, or git gc --auto in an older git, which detaches and goes on
// writing into the copy after git has returned: repacking it, and with
// that rewriting its shallow file, while Sources runs its next command on
// the copy, which then fails, or after Sources has unlocked it. Since Git
// 2.54, maintenance by default repacks a copy as soon as it has two packs
// to merge, and each fetch adds one. maintenance.auto keeps git from
// starting maintenance, and gc.auto keeps an older git from finding any
// to do; the two autoDetach keep whatever still runs in the foreground,
// under Sources' lock.
var noMaintenance = []string{
	"-c", "maintenance.auto=false", "-c", "maintenance.autoDetach=false",
	"-c", "gc.auto=0", "-c", "gc.autoDetach=false",
}

func (s *Sources) runOnce(ctx context.Context, repo, stdin string, args ...string) ([]byte, error) {
	cmd := s.command(ctx, repo, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &limitedWriter{w: &stderr, n: 4 << 10}
	if err := cmd.Run(); err != nil {
		return nil, s.commandError(ctx, args, stderr.String(), err)
	}
	return stdout.Bytes(), nil
}

// command returns the git command that runs args in the copy repo, or
// outside any for "", with the user's configuration and credentials, and
// the ssh command sshFor gives, as one that ends with ctx, and its process
// group with it.
func (s *Sources) command(ctx context.Context, repo string, args ...string) *exec.Cmd {
	ssh := s.sshFor(ctx)
	base := append([]string{"-c", "core.hooksPath=" + filepath.Join(s.dir, hooksDir)}, noMaintenance...)
	if repo != "" {
		base = append(base, "--git-dir="+repo)
	}
	// parseRef has checked every part of a reference that reaches args, and
	// git reads none of it as an option.
	cmd := exec.CommandContext(ctx, s.git, append(base, args...)...) //nolint:gosec // as above
	cmd.Env = gitEnv(os.Environ(), ssh)
	cmd.WaitDelay = waitDelay
	killGroup(cmd)
	return cmd
}

// commandError returns the error of the git command args that failed with
// err, having written stderr: ctx's, once it is done; git_unavailable for
// a git that couldn't be run; or a *gitError.
func (s *Sources) commandError(ctx context.Context, args []string, stderr string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist) {
		return problems.GitUnavailable.Problem(problems.Params{"reason": err.Error()})
	}
	return &gitError{args: args, stderr: stderr, err: err}
}

// batchSSH is the ssh command git runs when the user names none: one that
// never asks a question, which would wait for a terminal a coordinator
// doesn't have, and gives up on a host it can't reach.
const batchSSH = "ssh -o BatchMode=yes -o ConnectTimeout=30"

// sshProbeTimeout bounds asking git for the user's ssh command.
const sshProbeTimeout = 10 * time.Second

// sshFor returns the ssh command git should run, as sshCommand says, which
// it remembers once it knows. A probe that failed is asked again next
// time, and batchSSH, which never prompts, is used meanwhile.
func (s *Sources) sshFor(ctx context.Context) string {
	s.sshMu.Lock()
	defer s.sshMu.Unlock()
	if s.sshKnown {
		return s.ssh
	}
	ssh, ok := s.sshCommand(ctx)
	if ok {
		s.ssh, s.sshKnown = ssh, true
	}
	return ssh
}

// sshCommand returns the ssh command git should run: batchSSH, unless the
// user names their own, with GIT_SSH_COMMAND, GIT_SSH or core.sshCommand,
// which Meridian leaves as it is, and whether it could tell. It asks git
// with a deadline of its own, apart from ctx's end, so that the first
// caller's cancelled context doesn't decide for every later one.
func (s *Sources) sshCommand(ctx context.Context) (string, bool) {
	if os.Getenv("GIT_SSH_COMMAND") != "" || os.Getenv("GIT_SSH") != "" {
		return "", true
	}
	probe, cancel := context.WithTimeout(context.WithoutCancel(ctx), sshProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probe, s.git, "config", "--get", "core.sshCommand") //nolint:gosec // git, with no argument of a workflow's
	cmd.Env = gitEnv(os.Environ(), "")
	cmd.Dir = os.TempDir()
	out, err := cmd.Output()
	if err == nil {
		if strings.TrimSpace(string(out)) != "" {
			return "", true
		}
		return batchSSH, true
	}
	// git config --get exits 1 for a key that isn't set.
	if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 1 {
		return batchSSH, true
	}
	return batchSSH, false
}

// gitKeep are the variables of the user's environment that start with GIT_
// that git gets: those that find their configuration, their credentials,
// their certificates and their ssh command. None points git at another
// repository, another program or another template than the ones it is
// given, or changes what it reads. Every variable that doesn't start with
// GIT_ git gets, as credential helpers, proxies and modules need them,
// but those Meridian sets itself.
var gitKeep = []string{
	"GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM", "GIT_CONFIG_NOSYSTEM",
	"GIT_SSH", "GIT_SSH_COMMAND", "GIT_SSH_VARIANT",
	"GIT_SSL_CAINFO", "GIT_SSL_CAPATH", "GIT_SSL_CERT", "GIT_SSL_KEY", "GIT_SSL_CERT_PASSWORD_PROTECTED",
	"GIT_SSL_VERSION", "GIT_SSL_CIPHER_LIST", "GIT_HTTP_PROXY_AUTHMETHOD", "GIT_HTTP_USER_AGENT",
	"GIT_PROXY_SSL_CERT", "GIT_PROXY_SSL_KEY", "GIT_PROXY_SSL_CAINFO", "GIT_PROXY_SSL_CERT_PASSWORD_PROTECTED",
}

// gitDrop are the variables Meridian takes from no one for git: those it
// sets itself, and SSH_ASKPASS, a graphical askpass program, which
// OpenSSH before 8.4 runs whatever SSH_ASKPASS_REQUIRE says.
var gitDrop = []string{"GIT_TERMINAL_PROMPT", "GIT_ASKPASS", "SSH_ASKPASS", "SSH_ASKPASS_REQUIRE", "LC_ALL", "LANGUAGE"}

// sameVar says whether a and b name the same environment variable: on
// Windows, which looks them up regardless of case, whatever their case.
func sameVar(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// gitVar says whether name starts with GIT_, as sameVar compares names.
func gitVar(name string) bool {
	return len(name) >= 4 && sameVar(name[:4], "GIT_")
}

// gitEnv is the environment git runs in: the user's, but for the
// variables that start with GIT_ that gitKeep doesn't name, and those
// gitDrop names, with no prompt for a password or by an askpass program,
// and ssh as the ssh command when it isn't "". Git's messages are in
// English, so that what they say can be told apart.
func gitEnv(environ []string, ssh string) []string {
	named := func(list []string, name string) bool {
		return slices.ContainsFunc(list, func(v string) bool { return sameVar(v, name) })
	}
	env := make([]string, 0, len(environ)+len(gitDrop)+1)
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case named(gitDrop, name):
		case gitVar(name) && !named(gitKeep, name):
		case ssh != "" && sameVar(name, "GIT_SSH_COMMAND"):
		default:
			env = append(env, kv)
		}
	}
	env = append(env, "GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=", "SSH_ASKPASS_REQUIRE=never", "LC_ALL=C", "LANGUAGE=")
	if ssh != "" {
		env = append(env, "GIT_SSH_COMMAND="+ssh)
	}
	return env
}

// A gitError is a git command that failed.
type gitError struct {
	args   []string
	stderr string
	err    error
}

func (e *gitError) Error() string {
	msg := e.message()
	if msg == "" {
		return fmt.Sprintf("git %s: %v", strings.Join(e.args, " "), e.err)
	}
	return fmt.Sprintf("git %s: %v: %s", strings.Join(e.args, " "), e.err, msg)
}

func (e *gitError) Unwrap() error { return e.err }

// message is what git said, on one line.
func (e *gitError) message() string {
	return redactUserinfo(strings.Join(strings.Fields(e.stderr), " "))
}

// userinfoPattern is a URL's scheme and userinfo, as in
// https://user:password@ or https://token@: everything of the authority
// up to its last @, so that a password with an @ in it goes whole.
var userinfoPattern = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://)[^/?#\s'"]+@`)

// redactUserinfo returns s with the userinfo of each URL in it, which can
// hold a password or a token, as ***. What git says becomes a problem's
// reason, which a run can keep, so a credential in a URL that a user's
// insteadOf rewrites a remote to must never reach it, whether or not git
// leaves it out itself. ssh's user@host, which has no scheme and holds no
// secret, is left as it is.
func redactUserinfo(s string) string {
	return userinfoPattern.ReplaceAllString(s, "${1}"+problems.Masked+"@")
}

// notFound says whether git failed because the repository or the revision
// isn't there.
func (e *gitError) notFound() bool {
	return e.says("couldn't find remote ref", "repository not found", "could not be found",
		"does not appear to be a git repository", "not our ref", "no such remote ref")
}

// denied says whether git failed because the remote refused it: it asked
// for credentials git doesn't have, as a missing or private repository
// does, refused those it gave, or its host's key couldn't be checked. It
// matches only what a remote says: ssh's "Permission denied (publickey)"
// and the like, not a local file's "Permission denied", in Meridian's
// cache. A 403 that mentions a rate limit is the remote asking for less,
// which may not happen again.
func (e *gitError) denied() bool {
	if e.says("rate limit") {
		return false
	}
	return e.says("could not read username", "could not read password", "terminal prompts disabled",
		"permission denied (", "host key verification failed", "authentication failed", "access denied",
		"returned error: 401", "returned error: 403")
}

// malformed says whether git refused objects it fetched, as its fsck
// checks found them.
func (e *gitError) malformed() bool {
	return e.says("fsck error", "error in object", "bad object", "invalid object")
}

// contended says whether git failed because another process held a lock
// on the repository.
func (e *gitError) contended() bool {
	return e.says(".lock': file exists", "shallow file has changed", "could not lock config file", "cannot lock ref")
}

// says says whether git said any of signs, in any case.
func (e *gitError) says(signs ...string) bool {
	s := strings.ToLower(e.stderr)
	return slices.ContainsFunc(signs, func(sign string) bool { return strings.Contains(s, sign) })
}

// A limitedWriter keeps the first n bytes written to it, and drops the
// rest.
type limitedWriter struct {
	w *bytes.Buffer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	keep := min(len(p), l.n)
	if keep > 0 {
		l.w.Write(p[:keep])
		l.n -= keep
	}
	return len(p), nil
}
