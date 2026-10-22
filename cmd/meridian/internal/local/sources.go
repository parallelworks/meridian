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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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
// a rerun reads it without the network. On GitHub a commit is served from
// a repository's whole fork network, so a commit that a reference names
// may be one that only a fork has (docs/design.md, Security).
//
// git runs with no hooks, without fetching submodules, checking the
// objects it fetches, and never waiting on a prompt: a password, an
// askpass program, or ssh's questions, unless the user's own ssh command
// asks them. A cancelled context stops it and every process it started.
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
// package's kinds; one that names no workflow wraps compile.ErrNotFound
// too. Any other error is git's, or the system's, which may not happen
// again.
type Sources struct {
	dir string
	// git is the git command.
	git string
	// remote returns the URL of the repository repo on host.
	remote func(host, repo string) string

	mu    sync.Mutex
	repos map[string]chan struct{}

	sshOnce sync.Once
	// ssh is the ssh command git runs, when the user names none: one that
	// never prompts.
	ssh string
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
		repos:  map[string]chan struct{}{},
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

// platformRefs start the references to workflows a platform stores.
var platformRefs = []string{"marketplace/", "workflow/"}

var (
	refNamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
	revPattern     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_./-]*$`)
	commitPattern  = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
)

// defaultPath is the workflow's file in a repository when a reference
// names none.
const defaultPath = "workflow.yaml"

// notFound returns a problem of kind k with params, which names no
// workflow here: it wraps compile.ErrNotFound.
func notFound(k *problems.Kind, params problems.Params) error {
	return k.Problem(params).WithCause(compile.ErrNotFound)
}

// parseRef reads a reference. One that isn't a Git repository's, such as
// one to a workflow a platform stores, or that isn't well formed, names no
// workflow here.
func parseRef(ref string) (sourceRef, error) {
	if slices.ContainsFunc(platformRefs, func(p string) bool { return strings.HasPrefix(ref, p) }) {
		return sourceRef{}, notFound(problems.SourceOnPlatform, problems.Params{"ref": ref})
	}
	invalid := func() (sourceRef, error) {
		return sourceRef{}, notFound(problems.SourceRefInvalid, problems.Params{"ref": ref})
	}
	name, rev, hasRev := strings.CutLast(ref, "@")
	parts := strings.Split(name, "/")
	host, ok := hosts[parts[0]]
	if !ok || len(parts) < 3 {
		return invalid()
	}
	r := sourceRef{kind: parts[0], host: host, repo: parts[1] + "/" + parts[2], path: defaultPath, rev: rev}
	for _, p := range parts[1:3] {
		if !refNamePattern.MatchString(p) || p == "." || p == ".." {
			return invalid()
		}
	}
	if len(parts) > 3 {
		for _, p := range parts[3:] {
			if p == "" || p == "." || p == ".." || strings.ContainsFunc(p, func(c rune) bool { return c < ' ' || c == 0x7f }) {
				return invalid()
			}
		}
		r.path = strings.Join(parts[3:], "/")
	}
	if hasRev && (!revPattern.MatchString(rev) || strings.Contains(rev, "..") || strings.Contains(rev, "//") ||
		strings.HasSuffix(rev, "/") || strings.HasSuffix(rev, ".") || strings.HasSuffix(rev, ".lock")) {
		return invalid()
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

// repoName is the repository as a problem shows it: its server and name.
func (r sourceRef) repoName() string { return r.host + "/" + r.repo }

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
	if !commitPattern.MatchString(source.Revision) || source.Digest == "" {
		return nil, problems.SourceNotPinned.Problem(problems.Params{"location": source.Location, "revision": source.Revision})
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
	// parseRef has checked that each name is one, and not . or ..
	owner, name, _ := strings.Cut(r.repo, "/")
	repo = filepath.Join(s.dir, r.host, owner, name+".git")
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
	unlock = func() {
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
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	// Each fetch has a ref of its own, so that two at once never read each
	// other's.
	fetched := "refs/meridian/fetch/" + hex.EncodeToString(nonce[:])
	url := s.remote(r.host, r.repo)
	if _, err := s.run(ctx, repo, "",
		// A transfer that slows below a kilobyte a second for a minute is
		// stopped, so that a remote that hangs doesn't hang Meridian when
		// its context has no deadline.
		"-c", "http.lowSpeedLimit=1000", "-c", "http.lowSpeedTime=60",
		"-c", "fetch.fsckObjects=true",
		"fetch", "--quiet", "--no-tags", "--no-write-fetch-head", "--recurse-submodules=no", "--depth=1",
		"--", url, "+"+rev+":"+fetched); err != nil {
		if gerr, ok := errors.AsType[*gitError](err); ok && gerr.notFound() {
			return "", notFound(problems.SourceRevisionNotFound, problems.Params{"repo": r.repoName(), "revision": rev, "reason": gerr.message()})
		}
		return "", err
	}
	out, err := s.run(ctx, repo, "", "rev-parse", "--verify", fetched+"^{commit}")
	if err != nil {
		if gerr, ok := errors.AsType[*gitError](err); ok {
			return "", notFound(problems.SourceRevisionNotFound, problems.Params{"repo": r.repoName(), "revision": rev, "reason": gerr.message()})
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

func (s *Sources) runOnce(ctx context.Context, repo, stdin string, args ...string) ([]byte, error) {
	s.sshOnce.Do(func() { s.ssh = s.sshCommand(ctx) })
	base := []string{"-c", "core.hooksPath=" + filepath.Join(s.dir, hooksDir)}
	if repo != "" {
		base = append(base, "--git-dir="+repo)
	}
	// parseRef has checked every part of a reference that reaches args, and
	// git reads none of it as an option.
	cmd := exec.CommandContext(ctx, s.git, append(base, args...)...) //nolint:gosec // as above
	cmd.Env = gitEnv(os.Environ(), s.ssh)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &limitedWriter{w: &stderr, n: 4 << 10}
	cmd.WaitDelay = waitDelay
	killGroup(cmd)
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, exec.ErrNotFound) || errors.Is(err, fs.ErrNotExist) {
			return nil, problems.GitUnavailable.Problem(problems.Params{"reason": err.Error()})
		}
		return nil, &gitError{args: args, stderr: stderr.String(), err: err}
	}
	return stdout.Bytes(), nil
}

// batchSSH is the ssh command git runs when the user names none: one that
// never asks a question, which would wait for a terminal a coordinator
// doesn't have, and gives up on a host it can't reach.
const batchSSH = "ssh -o BatchMode=yes -o ConnectTimeout=30"

// sshCommand returns the ssh command git should run: batchSSH, unless the
// user names their own, with GIT_SSH_COMMAND, GIT_SSH or core.sshCommand,
// which Meridian leaves as it is.
func (s *Sources) sshCommand(ctx context.Context) string {
	if os.Getenv("GIT_SSH_COMMAND") != "" || os.Getenv("GIT_SSH") != "" {
		return ""
	}
	cmd := exec.CommandContext(ctx, s.git, "config", "--get", "core.sshCommand") //nolint:gosec // git, with no argument of a workflow's
	cmd.Env = gitEnv(os.Environ(), "")
	cmd.Dir = os.TempDir()
	if out, err := cmd.Output(); err == nil && strings.TrimSpace(string(out)) != "" {
		return ""
	}
	return batchSSH
}

// gitKeep are the environment variables git gets of the user's: what
// finds their configuration, their credentials and their proxies, and
// what Windows needs to run a program. None points git at another
// repository than the one it is given, or changes what it reads there.
var gitKeep = []string{
	"HOME", "PATH", "USER", "LOGNAME", "TMPDIR", "XDG_CONFIG_HOME",
	"GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM", "GIT_CONFIG_NOSYSTEM",
	"SSH_AUTH_SOCK", "GIT_SSH", "GIT_SSH_COMMAND", "GIT_SSH_VARIANT", "KRB5CCNAME",
	"GIT_SSL_CAINFO", "GIT_SSL_CAPATH", "SSL_CERT_FILE", "SSL_CERT_DIR",
	"http_proxy", "https_proxy", "no_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
	"SYSTEMROOT", "SystemRoot", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PATHEXT", "COMSPEC", "TEMP", "TMP",
	"HOMEDRIVE", "HOMEPATH",
}

// gitEnv is the environment git runs in: those of the user's that gitKeep
// names, with no prompt for a password or by an askpass program, and ssh
// as the ssh command when it isn't "". Git's messages are in English, so
// that what they say can be told apart.
func gitEnv(environ []string, ssh string) []string {
	env := make([]string, 0, len(gitKeep)+6)
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if slices.Contains(gitKeep, name) {
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
	return strings.Join(strings.Fields(e.stderr), " ")
}

// notFound says whether git failed because the repository or the revision
// isn't there.
func (e *gitError) notFound() bool {
	return e.says("couldn't find remote ref", "repository not found", "could not be found",
		"does not appear to be a git repository", "not our ref", "no such remote ref")
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
