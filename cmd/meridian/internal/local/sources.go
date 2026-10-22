package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// SourcesDir is the name of the directory, in the cache directory, that
// holds the repositories workflows' uses: name.
const SourcesDir = "sources"

// maxSourceSize is the largest workflow file Sources reads. The compiler
// has its own, smaller limit, which it reports as a problem with the
// workflow; this one only keeps a file that is far too large out of
// memory.
const maxSourceSize = 16 << 20

// Sources reads the workflows that a step's uses: names, from Git
// repositories, by running the git command, which reaches them with the
// user's own Git configuration and credentials. A reference is
//
//	github/<owner>/<repository>[/<path>][@<revision>]
//	gitlab/<namespace>/<project>[/<path>][@<revision>]
//
// for a repository on github.com or gitlab.com, where path is the
// workflow's file in it, workflow.yaml by default, and revision a branch,
// a tag or a commit, the remote's default branch by default.
//
// Resolve pins the reference to the commit its revision names now and to
// the SHA-256 digest of the file's text there, and Read reads it again by
// that pin, whatever the revision names by then, and fails if the text
// isn't the digest's. Sources keeps a bare copy of each repository it
// reads in the cache directory, holding each commit it has pinned, so that
// a rerun reads it without the network.
//
// The digest is computed here, with the standard library's FIPS 140-3
// module. Git itself names objects by SHA-1 or SHA-256 and reaches remotes
// over TLS or SSH with the host's cryptography, outside Meridian's module,
// which a FIPS host configures for the whole system.
type Sources struct {
	dir string
	// git is the git command.
	git string
	// remote returns the URL of the repository repo on host.
	remote func(host, repo string) string

	seed  maphash.Seed
	locks [16]sync.Mutex
}

var _ compile.Sources = (*Sources)(nil)

// NewSources returns the Sources whose copies of repositories are in dir,
// the cache directory, which must be readable only by its user, as
// Dirs.EnsureCache makes it.
func NewSources(dir string) *Sources {
	return &Sources{
		dir:    filepath.Join(dir, SourcesDir),
		git:    "git",
		remote: func(host, repo string) string { return "https://" + host + "/" + repo + ".git" },
		seed:   maphash.MakeSeed(),
	}
}

// A sourceRef is a reference to a workflow in a Git repository.
type sourceRef struct {
	// kind is github or gitlab, and host its server.
	kind, host string
	// repo is the repository, as owner/name.
	repo string
	// path is the workflow's file in the repository.
	path string
	// rev is the revision, or "" for the remote's default branch.
	rev string
}

// hosts are the servers a reference's first name stands for.
var hosts = map[string]string{"github": "github.com", "gitlab": "gitlab.com"}

var (
	refNamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
	revPattern     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_./-]*$`)
	commitPattern  = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
)

// defaultPath is the workflow's file in a repository when a reference
// names none.
const defaultPath = "workflow.yaml"

// parseRef reads a reference. One that isn't a Git repository's, such as
// one to a workflow a platform stores, or that isn't well formed, names no
// workflow here: it is compile.ErrNotFound.
func parseRef(ref string) (sourceRef, error) {
	notFound := func(why string) (sourceRef, error) {
		return sourceRef{}, fmt.Errorf("%w: %q %s", compile.ErrNotFound, ref, why)
	}
	name, rev, hasRev := strings.CutLast(ref, "@")
	parts := strings.Split(name, "/")
	host, ok := hosts[parts[0]]
	switch {
	case !ok:
		return notFound("isn't a Git repository's workflow: it starts with github/ or gitlab/")
	case len(parts) < 3:
		return notFound("doesn't name a repository: it has an owner and a name")
	}
	r := sourceRef{kind: parts[0], host: host, repo: parts[1] + "/" + parts[2], path: defaultPath, rev: rev}
	for _, p := range parts[1:3] {
		if !refNamePattern.MatchString(p) || p == "." || p == ".." {
			return notFound("doesn't name a repository")
		}
	}
	if len(parts) > 3 {
		for _, p := range parts[3:] {
			if p == "" || p == "." || p == ".." || strings.ContainsFunc(p, func(c rune) bool { return c < ' ' || c == 0x7f }) {
				return notFound("doesn't name a file in the repository")
			}
		}
		r.path = strings.Join(parts[3:], "/")
	}
	if hasRev && (!revPattern.MatchString(rev) || strings.Contains(rev, "..") || strings.Contains(rev, "//") ||
		strings.HasSuffix(rev, "/") || strings.HasSuffix(rev, ".") || strings.HasSuffix(rev, ".lock")) {
		return notFound("doesn't name a revision: one is a branch, a tag or a commit")
	}
	return r, nil
}

// location is the reference as a plan's Source names it: each of its
// parts, with its path and its revision as written, if any.
func (r sourceRef) location() string {
	loc := r.kind + "/" + r.repo + "/" + r.path
	if r.rev != "" {
		loc += "@" + r.rev
	}
	return loc
}

// remoteContext is what the remote context holds for a workflow at r: the
// repository's address and the revision, as remote.repo and
// remote.branch.
func (r sourceRef) remoteContext() map[string]string {
	remote := map[string]string{"repo": "https://" + r.host + "/" + r.repo}
	if r.rev != "" {
		remote["branch"] = r.rev
	}
	return remote
}

// Resolve finds the workflow ref names now, and pins it to its commit and
// its text's digest.
func (s *Sources) Resolve(ctx context.Context, ref string) (*compile.Workflow, error) {
	r, err := parseRef(ref)
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
		Source: plan.Source{Location: r.location(), Revision: commit, Digest: digest(text)},
		Text:   text,
		Remote: r.remoteContext(),
	}, nil
}

// Read reads the workflow that Resolve pinned as source, by its commit,
// and checks its text against its digest.
func (s *Sources) Read(ctx context.Context, source plan.Source) (*compile.Workflow, error) {
	r, err := parseRef(source.Location)
	if err != nil {
		return nil, err
	}
	switch {
	case !commitPattern.MatchString(source.Revision):
		return nil, fmt.Errorf("%s isn't pinned to a commit: its revision is %q", source.Location, source.Revision)
	case source.Digest == "":
		return nil, fmt.Errorf("%s isn't pinned by its text's digest", source.Location)
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
		return nil, fmt.Errorf("%w: %s at %s is %s, not %s as it was pinned", ErrSourceChanged, source.Location, source.Revision, d, source.Digest)
	}
	return &compile.Workflow{Source: source, Text: text, Remote: r.remoteContext()}, nil
}

// ErrSourceChanged is what Read returns for a workflow whose text at its
// commit isn't the text it was pinned to.
var ErrSourceChanged = errors.New("a workflow's text isn't the text it was pinned to")

// digest is the SHA-256 digest of text, as a plan's Source writes it.
func digest(text []byte) string {
	sum := sha256.Sum256(text)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// open returns the copy of r's repository, creating it if it isn't there,
// locked for this process until unlock.
func (s *Sources) open(ctx context.Context, r sourceRef) (repo string, unlock func(), err error) {
	// parseRef has checked that each name is one, and not . or ..
	owner, name, _ := strings.Cut(r.repo, "/")
	repo = filepath.Join(s.dir, r.host, owner, name+".git")
	m := &s.locks[maphash.String(s.seed, repo)%uint64(len(s.locks))]
	m.Lock()
	if _, err := os.Stat(filepath.Join(repo, "HEAD")); err == nil {
		return repo, m.Unlock, nil
	}
	if err := os.MkdirAll(filepath.Dir(repo), 0o700); err != nil {
		m.Unlock()
		return "", nil, err
	}
	if _, err := s.run(ctx, "", nil, "init", "--bare", "--quiet", "--", repo); err != nil {
		m.Unlock()
		return "", nil, err
	}
	return repo, m.Unlock, nil
}

// fetch fetches rev of r's repository into repo, keeps its commit, and
// returns it.
func (s *Sources) fetch(ctx context.Context, repo string, r sourceRef, rev string) (string, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	// Each fetch has a ref of its own, so that two at once, in this process
	// or another, never read each other's.
	fetched := "refs/meridian/fetch/" + hex.EncodeToString(nonce[:])
	url := s.remote(r.host, r.repo)
	if _, err := s.run(ctx, repo, nil, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", "--depth=1",
		"--", url, "+"+rev+":"+fetched); err != nil {
		var gerr *gitError
		if errors.As(err, &gerr) && gerr.notFound() {
			return "", fmt.Errorf("%w: %s has no revision %s: %s", compile.ErrNotFound, url, rev, gerr.message())
		}
		return "", err
	}
	out, err := s.run(ctx, repo, nil, "rev-parse", "--verify", "--quiet", fetched+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("%w: %s's revision %s isn't a commit", compile.ErrNotFound, url, rev)
	}
	commit := strings.TrimSpace(string(out))
	if !commitPattern.MatchString(commit) {
		return "", fmt.Errorf("git gave %q as the commit of %s", commit, rev)
	}
	// The commit keeps a ref, so that the copy keeps it for reruns.
	if _, err := s.run(ctx, repo, nil, "update-ref", "refs/meridian/pinned/"+commit, commit); err != nil {
		return "", err
	}
	if _, err := s.run(ctx, repo, nil, "update-ref", "-d", fetched); err != nil {
		return "", err
	}
	return commit, nil
}

// has says whether repo has the commit.
func (s *Sources) has(ctx context.Context, repo, commit string) bool {
	_, err := s.run(ctx, repo, nil, "cat-file", "-e", commit+"^{commit}")
	return err == nil
}

// blob reads r's file at commit in repo.
func (s *Sources) blob(ctx context.Context, repo string, r sourceRef, commit string) ([]byte, error) {
	object := commit + ":" + r.path
	out, err := s.run(ctx, repo, strings.NewReader(object+"\n"), "cat-file", "--batch-check=%(objecttype) %(objectsize)")
	if err != nil {
		return nil, err
	}
	typ, size, _ := strings.Cut(strings.TrimSpace(string(out)), " ")
	if typ != "blob" {
		return nil, fmt.Errorf("%w: %s has no file %s at %s", compile.ErrNotFound, r.kind+"/"+r.repo, r.path, commit)
	}
	if n, err := strconv.ParseInt(size, 10, 64); err != nil || n > maxSourceSize {
		return nil, fmt.Errorf("%s at %s is larger than %d bytes", r.location(), commit, maxSourceSize)
	}
	return s.run(ctx, repo, nil, "cat-file", "blob", object)
}

// run runs git with args, in repo when it isn't "", with stdin as its
// input, and returns what it writes.
func (s *Sources) run(ctx context.Context, repo string, stdin io.Reader, args ...string) ([]byte, error) {
	if repo != "" {
		args = append([]string{"--git-dir=" + repo}, args...)
	}
	// parseRef has checked every part of a reference that reaches args, and
	// git reads none of it as an option.
	cmd := exec.CommandContext(ctx, s.git, args...) //nolint:gosec // as above

	cmd.Env = gitEnv(os.Environ())
	cmd.Stdin = stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &limitedWriter{w: &stderr, n: 4 << 10}
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &gitError{args: args, stderr: stderr.String(), err: err}
	}
	return stdout.Bytes(), nil
}

// gitEnv is the environment git runs in: the user's, so that their Git
// configuration and credentials reach their repositories, but without what
// would point git at another repository than the one it is given, and
// with no prompt for credentials, which would wait for a terminal that a
// coordinator doesn't have. Git's messages are in English, so that what
// they say can be told apart.
func gitEnv(environ []string) []string {
	drop := []string{
		"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
		"GIT_COMMON_DIR", "GIT_NAMESPACE", "GIT_CEILING_DIRECTORIES", "GIT_DISCOVERY_ACROSS_FILESYSTEM",
		"GIT_TERMINAL_PROMPT", "LC_ALL", "LANGUAGE",
	}
	env := make([]string, 0, len(environ)+3)
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(drop, name) {
			env = append(env, kv)
		}
	}
	return append(env, "GIT_TERMINAL_PROMPT=0", "LC_ALL=C", "LANGUAGE=")
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
	return strings.Join(strings.Fields(e.stderr), " ")
}

// notFound says whether git failed because the repository or the revision
// isn't there.
func (e *gitError) notFound() bool {
	s := strings.ToLower(e.stderr)
	for _, sign := range []string{
		"couldn't find remote ref", "repository not found", "could not be found",
		"does not appear to be a git repository", "not our ref", "no such remote ref",
	} {
		if strings.Contains(s, sign) {
			return true
		}
	}
	return false
}

// A limitedWriter keeps the first n bytes written to it, and drops the
// rest.
type limitedWriter struct {
	w io.Writer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	keep := min(len(p), l.n)
	if keep > 0 {
		if _, err := l.w.Write(p[:keep]); err != nil {
			return 0, err
		}
		l.n -= keep
	}
	return len(p), nil
}
