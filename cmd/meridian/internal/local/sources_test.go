package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// origin is a repository that Sources fetches from, as it would from
// github.com.
type origin struct {
	t   *testing.T
	dir string
}

// newOrigins returns the directory of test repositories, by host and
// name, and Sources that fetches from them, with its cache in a directory
// of its own.
func newOrigins(t *testing.T) (string, *Sources) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git isn't installed")
	}
	// The user's Git configuration doesn't reach the test's repositories.
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	origins := t.TempDir()
	return origins, sourcesFrom(t, origins)
}

// testServers are the GitLab servers the tests' Sources reach, besides
// gitlab.com.
var testServers = []string{"git.example.com"}

func sourcesFrom(t *testing.T, origins string) *Sources {
	t.Helper()
	s := NewSources(t.TempDir(), testServers)
	s.remote = func(host, repo string) string { return filepath.Join(origins, host, repo) }
	return s
}

// newOrigin makes the repository repo on host in origins, which allows
// fetching any commit a branch reaches, as github.com does.
func newOrigin(t *testing.T, origins, host, repo string) *origin {
	t.Helper()
	o := &origin{t: t, dir: filepath.Join(origins, host, repo)}
	if err := os.MkdirAll(o.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	o.git("init", "--quiet", "--initial-branch=main")
	o.git("config", "uploadpack.allowReachableSHA1InWant", "true")
	return o
}

func (o *origin) git(args ...string) string {
	o.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = o.dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		o.t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// commit writes files, by path, and commits them, and returns the commit.
func (o *origin) commit(files map[string]string) string {
	o.t.Helper()
	for path, text := range files {
		full := filepath.Join(o.dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			o.t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(text), 0o600); err != nil {
			o.t.Fatal(err)
		}
	}
	o.git("add", "--all")
	o.git("commit", "--quiet", "--message=change")
	return o.git("rev-parse", "HEAD")
}

func TestSourcesResolve(t *testing.T) {
	origins, s := newOrigins(t)
	o := newOrigin(t, origins, "github.com", "acme/flows")
	first := o.commit(map[string]string{"workflow.yaml": "first\n"})
	o.git("tag", "v1")
	second := o.commit(map[string]string{"workflow.yaml": "second\n", "sub/flow.yaml": "sub\n"})
	o.git("switch", "--quiet", "--create", "dev")
	dev := o.commit(map[string]string{"sub/flow.yaml": "dev\n"})
	o.git("switch", "--quiet", "main")

	// A project in a subgroup, on a GitLab server of a platform's own.
	g := newOrigin(t, origins, "git.example.com", "team/infra/flows")
	onGitLab := g.commit(map[string]string{"flows/run.yaml": "gitlab\n"})

	for _, tc := range []struct {
		ref      plan.SourceRef
		location string
		commit   string
		text     string
		remote   map[string]string
	}{
		{plan.SourceRef{Uses: "github/acme/flows@main"}, "github.com/acme/flows@main:workflow.yaml", second, "second\n", map[string]string{"repo": "https://github.com/acme/flows", "branch": "main"}},
		{plan.SourceRef{Uses: "github/acme/flows"}, "github.com/acme/flows:workflow.yaml", second, "second\n", map[string]string{"repo": "https://github.com/acme/flows"}},
		{plan.SourceRef{Uses: "github/acme/flows@v1"}, "github.com/acme/flows@v1:workflow.yaml", first, "first\n", map[string]string{"repo": "https://github.com/acme/flows", "branch": "v1"}},
		{plan.SourceRef{Uses: "github/acme/flows@" + first}, "github.com/acme/flows@" + first + ":workflow.yaml", first, "first\n", map[string]string{"repo": "https://github.com/acme/flows", "branch": first}},
		// Format 1 names the file with $yaml.
		{plan.SourceRef{Uses: "github/acme/flows@dev", Path: "sub/flow.yaml"}, "github.com/acme/flows@dev:sub/flow.yaml", dev, "dev\n", map[string]string{"repo": "https://github.com/acme/flows", "branch": "dev"}},
		// As ACTIVATE reads them: with .git or a slash after the name, and
		// space after it.
		{plan.SourceRef{Uses: "github/acme/flows.git@main "}, "github.com/acme/flows@main:workflow.yaml", second, "second\n", map[string]string{"repo": "https://github.com/acme/flows", "branch": "main"}},
		{plan.SourceRef{Uses: "github/acme/flows/"}, "github.com/acme/flows:workflow.yaml", second, "second\n", map[string]string{"repo": "https://github.com/acme/flows"}},
		{plan.SourceRef{Uses: "gitlab/team/infra/flows@main", Path: "flows/run.yaml", Host: "git.example.com"}, "git.example.com/team/infra/flows@main:flows/run.yaml", onGitLab, "gitlab\n", map[string]string{"repo": "https://git.example.com/team/infra/flows", "branch": "main"}},
		{plan.SourceRef{Uses: "gitlab/ team/infra/flows.git/@main", Path: "flows/run.yaml", Host: " Git.Example.com "}, "git.example.com/team/infra/flows@main:flows/run.yaml", onGitLab, "gitlab\n", map[string]string{"repo": "https://git.example.com/team/infra/flows", "branch": "main"}},
	} {
		w, err := s.Resolve(t.Context(), tc.ref)
		if err != nil {
			t.Fatalf("Resolve(%+v): %v", tc.ref, err)
		}
		want := plan.Source{Location: tc.location, Revision: tc.commit, Digest: digest([]byte(tc.text)), Ref: tc.ref}
		if w.Source != want || string(w.Text) != tc.text || !reflect.DeepEqual(w.Remote, tc.remote) {
			t.Errorf("Resolve(%+v) = %+v, %q, %v, want %+v, %q, %v", tc.ref, w.Source, w.Text, w.Remote, want, tc.text, tc.remote)
		}
		// It reads again by its pin, from its Ref, with the same remote.
		again, err := s.Read(t.Context(), w.Source)
		if err != nil || string(again.Text) != tc.text || !reflect.DeepEqual(again.Remote, tc.remote) {
			t.Errorf("Read(%+v) = %v, %v", w.Source, again, err)
		}
	}
}

// TestSourcesGroupIsNoCopy checks that a reference whose group's name is
// where another repository's copy, or its lock file, is in the cache
// leaves that repository readable, before its copy is made and after.
func TestSourcesGroupIsNoCopy(t *testing.T) {
	origins, s := newOrigins(t)
	newOrigin(t, origins, "gitlab.com", "g/sub").commit(map[string]string{"workflow.yaml": "real\n"})
	resolve := func() {
		t.Helper()
		w, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "gitlab/g/sub@main"})
		if err != nil || string(w.Text) != "real\n" {
			t.Fatalf("Resolve of gitlab/g/sub: %v, %v", w, err)
		}
	}
	refs := []plan.SourceRef{
		{Uses: "gitlab/g/sub.git/p@main", Path: "w.yaml"},
		{Uses: "gitlab/g/sub.git.lock/p@main", Path: "w.yaml"},
		{Uses: "gitlab/g/sub.git/refs/heads/x@main", Path: "w.yaml"},
	}
	for _, ref := range refs {
		if _, err := s.Resolve(t.Context(), ref); !errors.Is(err, compile.ErrNotFound) {
			t.Errorf("Resolve(%+v): %v, want ErrNotFound", ref, err)
		}
	}
	resolve()
	for _, ref := range refs {
		_, _ = s.Resolve(t.Context(), ref)
	}
	resolve()
	if _, err := os.Stat(filepath.Join(s.dir, "gitlab.com", "g", "sub.git", "refs", "heads", "x.git")); err == nil {
		t.Error("a reference made a repository in g/sub's copy")
	}
}

// TestSourcesServers checks that a reference's $host reaches only a GitLab
// server the user registered, or gitlab.com, whatever its case, and that
// a pin is read again only from one. A $host has no port, as ACTIVATE
// reads it, and a GitHub repository's names none.
func TestSourcesServers(t *testing.T) {
	origins, s := newOrigins(t)
	newOrigin(t, origins, "git.example.com", "g/p").commit(map[string]string{"w.yaml": "registered\n"})
	newOrigin(t, origins, "gitlab.com", "g/p").commit(map[string]string{"w.yaml": "gitlab\n"})
	newOrigin(t, origins, "other.example.com", "g/p").commit(map[string]string{"w.yaml": "other\n"})
	newOrigin(t, origins, "github.com", "g/p").commit(map[string]string{"w.yaml": "github\n"})
	for host, text := range map[string]string{"Git.Example.com": "registered\n", "gitlab.com": "gitlab\n", "GitLab.com": "gitlab\n", "": "gitlab\n", " ": "gitlab\n"} {
		w, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "gitlab/g/p", Path: "w.yaml", Host: host})
		if err != nil || string(w.Text) != text {
			t.Errorf("Resolve on %q: %v, %v", host, w, err)
		}
	}
	if w, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/g/p", Path: "w.yaml", Host: "other.example.com"}); err != nil || string(w.Text) != "github\n" {
		t.Errorf("Resolve on GitHub, with a $host: %v, %v", w, err)
	}
	for _, host := range []string{"other.example.com", "git.example.org"} {
		ref := plan.SourceRef{Uses: "gitlab/g/p", Path: "w.yaml", Host: host}
		_, err := s.Resolve(t.Context(), ref)
		if !errors.Is(err, compile.ErrNotFound) {
			t.Errorf("Resolve on %s: %v, want ErrNotFound", host, err)
			continue
		}
		if p := wantProblem(t, err, problems.SourceServerUnknown); p.Params["server"] != host {
			t.Errorf("the problem names %v", p.Params["server"])
		}
	}
	for _, host := range []string{"git.example.com:443", "git.example.com:8443"} {
		_, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "gitlab/g/p", Path: "w.yaml", Host: host})
		wantProblem(t, err, problems.SourceRefInvalid)
	}
	// A pin made on a server is read again only while it is registered.
	w, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "gitlab/g/p", Path: "w.yaml", Host: "git.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	unregistered := sourcesFrom(t, origins)
	unregistered.servers = map[string]bool{}
	if _, err := unregistered.Read(t.Context(), w.Source); !errors.Is(err, compile.ErrNotFound) {
		t.Errorf("Read of a pin on a server no longer registered: %v", err)
	}
}

// TestSourcesReadByPin checks that a workflow read by its pin is the text
// it was pinned to, however its branch has moved, even from a copy that
// has never seen it.
func TestSourcesReadByPin(t *testing.T) {
	origins, s := newOrigins(t)
	o := newOrigin(t, origins, "github.com", "acme/flows")
	o.commit(map[string]string{"workflow.yaml": "pinned\n"})
	pinned, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"})
	if err != nil {
		t.Fatal(err)
	}
	o.commit(map[string]string{"workflow.yaml": "moved on\n"})

	for name, src := range map[string]*Sources{"the same copy": s, "a new copy": sourcesFrom(t, origins)} {
		w, err := src.Read(t.Context(), pinned.Source)
		if err != nil {
			t.Fatalf("Read from %s: %v", name, err)
		}
		if string(w.Text) != "pinned\n" || w.Source != pinned.Source || !reflect.DeepEqual(w.Remote, pinned.Remote) {
			t.Errorf("Read from %s = %+v, %q, %v", name, w.Source, w.Text, w.Remote)
		}
	}
	again, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"})
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Text) != "moved on\n" || again.Source.Revision == pinned.Source.Revision {
		t.Errorf("Resolve after the branch moved = %+v, %q", again.Source, again.Text)
	}

	// The copy keeps the pinned commit, so that a rerun reads it without
	// the remote.
	if err := os.RemoveAll(o.dir); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(t.Context(), pinned.Source); err != nil {
		t.Errorf("Read without the remote: %v", err)
	}
}

func TestSourcesReadRefuses(t *testing.T) {
	origins, s := newOrigins(t)
	o := newOrigin(t, origins, "github.com", "acme/flows")
	o.commit(map[string]string{"workflow.yaml": "text\n"})
	w, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"})
	if err != nil {
		t.Fatal(err)
	}
	changed := w.Source
	changed.Digest = digest([]byte("other text\n"))
	_, err = s.Read(t.Context(), changed)
	p := wantProblem(t, err, problems.SourceChanged)
	if p.Params["digest"] != w.Source.Digest || p.Params["pinned"] != changed.Digest || p.Params["commit"] != w.Source.Revision {
		t.Errorf("the problem's params are %v", p.Params)
	}
	for name, src := range map[string]plan.Source{
		"no digest":    {Location: w.Source.Location, Revision: w.Source.Revision},
		"not a commit": {Location: w.Source.Location, Revision: digest([]byte("text\n")), Digest: w.Source.Digest},
		"a branch":     {Location: w.Source.Location, Revision: "main", Digest: w.Source.Digest},
	} {
		_, err := s.Read(t.Context(), src)
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.SourceNotPinned.Code() {
			t.Errorf("Read of a source with %s: %v", name, err)
		}
	}
}

func TestSourcesNotFound(t *testing.T) {
	origins, s := newOrigins(t)
	o := newOrigin(t, origins, "github.com", "acme/flows")
	o.commit(map[string]string{"workflow.yaml": "text\n", "sub/flow.yaml": "sub\n"})
	for ref, kind := range map[plan.SourceRef]*problems.Kind{
		{Uses: "github/acme/flows@missing"}:                    problems.SourceRevisionNotFound,
		{Uses: "github/acme/flows@main", Path: "missing.yaml"}: problems.SourceFileNotFound,
		{Uses: "github/acme/flows@main", Path: "sub"}:          problems.SourceFileNotFound,
		{Uses: "github/acme/none@main"}:                        problems.SourceRevisionNotFound,
		{Uses: "gitlab/acme/flows@main"}:                       problems.SourceRevisionNotFound,
		{Uses: "marketplace/checkout"}:                         problems.SourceOnPlatform,
		{Uses: "workflow/stored"}:                              problems.SourceOnPlatform,
		{Uses: "github/acme"}:                                  problems.SourceRefInvalid,
		// As ACTIVATE reads it, nothing follows a GitHub repository's name:
		// the file is $yaml's.
		{Uses: "github/acme/flows/sub/flow.yaml@main"}: problems.SourceRefInvalid,
	} {
		_, err := s.Resolve(t.Context(), ref)
		if !errors.Is(err, compile.ErrNotFound) {
			t.Errorf("Resolve(%s): %v, want ErrNotFound", ref, err)
			continue
		}
		wantProblem(t, err, kind)
	}
}

func TestParseRef(t *testing.T) {
	for ref, want := range map[plan.SourceRef]sourceRef{
		{Uses: "github/acme/flows"}:                           {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml"},
		{Uses: "github/acme/flows@v1.2"}:                      {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml", rev: "v1.2"},
		{Uses: "gitlab/acme/flows@release", Path: "a/b.yaml"}: {kind: "gitlab", host: "gitlab.com", repo: "acme/flows", path: "a/b.yaml", rev: "release"},
		{Uses: "github/a-b/c_d-e.f@g/h", Path: "x y.yaml"}:    {kind: "github", host: "github.com", repo: "a-b/c_d-e.f", path: "x y.yaml", rev: "g/h"},
		// A repository's name can start with a dot, as .github does.
		{Uses: "github/acme/.github"}:   {kind: "github", host: "github.com", repo: "acme/.github", path: "workflow.yaml"},
		{Uses: "gitlab/g_1/.p@a/b.c"}:   {kind: "gitlab", host: "gitlab.com", repo: "g_1/.p", path: "workflow.yaml", rev: "a/b.c"},
		{Uses: "gitlab/g/p", Host: "x"}: {kind: "gitlab", host: "x", repo: "g/p", path: "workflow.yaml"},
		// As ACTIVATE reads it: one slash, and then .git, after the name,
		// and space after the uses:, and on GitLab, after gitlab/ and
		// around $host, go; $host names no GitHub server, and an empty
		// $yaml means workflow.yaml.
		{Uses: "github/acme/flows.git"}:                       {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml"},
		{Uses: "github/acme/flows/@v1"}:                       {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml", rev: "v1"},
		{Uses: "github/acme/flows.git/"}:                      {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml"},
		{Uses: "github/acme/flows@v1 \t"}:                     {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml", rev: "v1"},
		{Uses: "github/acme/flows", Host: "x.example.com"}:    {kind: "github", host: "github.com", repo: "acme/flows", path: "workflow.yaml"},
		{Uses: "gitlab/ g/p.git@v1 ", Host: " X.Example.COM"}: {kind: "gitlab", host: "x.example.com", repo: "g/p", path: "workflow.yaml", rev: "v1"},
		{Uses: "gitlab/g/p.git.git"}:                          {kind: "gitlab", host: "gitlab.com", repo: "g/p.git", path: "workflow.yaml"},
		// Every name after gitlab/ is the project's path, with or without
		// $yaml.
		{Uses: "gitlab/g/sub/p@main"}: {kind: "gitlab", host: "gitlab.com", repo: "g/sub/p", path: "workflow.yaml", rev: "main"},
		// $yaml names the file, and then a GitLab project can be in a
		// subgroup; $host names its server.
		{Uses: "github/acme/flows@main", Path: "a/b.yaml"}:                          {kind: "github", host: "github.com", repo: "acme/flows", path: "a/b.yaml", rev: "main"},
		{Uses: "gitlab/g/sub/p@main", Path: "w.yaml"}:                               {kind: "gitlab", host: "gitlab.com", repo: "g/sub/p", path: "w.yaml", rev: "main"},
		{Uses: "gitlab/g/p", Path: "w.yaml", Host: "gitlab.example.com"}:            {kind: "gitlab", host: "gitlab.example.com", repo: "g/p", path: "w.yaml"},
		{Uses: "gitlab/g/p", Path: "w.yaml", Host: "git_1.example-.com"}:            {kind: "gitlab", host: "git_1.example-.com", repo: "g/p", path: "w.yaml"},
		{Uses: "gitlab/g/sub/p@v1", Path: "dir/w.yaml", Host: "gitlab.example.com"}: {kind: "gitlab", host: "gitlab.example.com", repo: "g/sub/p", path: "dir/w.yaml", rev: "v1"},
	} {
		got, err := parseRef(ref)
		if err != nil || got != want {
			t.Errorf("parseRef(%+v) = %+v, %v, want %+v", ref, got, err, want)
		}
	}
	// $yaml and $host are checked as the rest of a reference is.
	for _, ref := range []plan.SourceRef{
		{Uses: "github/acme/flows/sub@main", Path: "w.yaml"},
		{Uses: "github/acme/flows", Path: "../w.yaml"},
		{Uses: "github/acme/flows", Path: "/w.yaml"},
		{Uses: "github/acme/flows", Path: "a//w.yaml"},
		{Uses: "github/acme/flows", Path: "w.yaml\n"},
		{Uses: "gitlab/g/p", Host: "-x.example.com"},
		{Uses: "gitlab/g/p", Host: "a.example.com/evil"},
		{Uses: "gitlab/g/p", Host: "user@a.example.com"},
		{Uses: "gitlab/g/p", Host: "a..example.com"},
		{Uses: "gitlab/g/p", Host: "a.example.com."},
		{Uses: "gitlab/g/p", Host: "a.example.com:8443"},
		{Uses: "gitlab/g/../p", Path: "w.yaml"},
		// As ACTIVATE reads them: a GitHub owner's name has no dot or
		// underscore, a GitLab project is in a group, a name has nothing
		// but letters, digits, dots, underscores and hyphens, a revision
		// slashes too, and nothing follows an @ but the revision.
		{Uses: "github/a.b/c"},
		{Uses: "github/a_b/c"},
		{Uses: "gitlab/p"},
		{Uses: "gitlab/p.git"},
		{Uses: "gitlab/g/p//"},
		{Uses: "gitlab/g/p q"},
		{Uses: "github/ acme/flows"},
		{Uses: "github/acme/flows @main"},
		{Uses: "github/acme/flows@a@b"},
		{Uses: "github/acme/flows@a b"},
		{Uses: "github/acme/fl+ows"},
		{Uses: "github/acme/flows@"},
		{Uses: "gitlab/g/p@"},
		{Uses: " github/acme/flows"},
		// A group's name is a directory of the cache, beside the copies of
		// repositories and their lock files, and ends as neither does.
		{Uses: "gitlab/g/sub.git/p@main", Path: "w.yaml"},
		{Uses: "gitlab/g/sub.git.lock/p@main", Path: "w.yaml"},
		{Uses: "gitlab/g/Sub.GIT/p@main", Path: "w.yaml"},
		{Uses: "gitlab/g/sub.git/refs/heads/x@main", Path: "w.yaml"},
		{Uses: "github/acme.git/flows"},
	} {
		if r, err := parseRef(ref); !errors.Is(err, compile.ErrNotFound) {
			t.Errorf("parseRef(%+v) = %+v, %v, want ErrNotFound", ref, r, err)
		}
	}
	// Nothing in a reference reaches git as an option, or names another
	// repository or a file outside it.
	for _, ref := range []string{
		"github/acme/flows@--upload-pack=touch x",
		"github/acme/flows@-b",
		"github/-acme/flows",
		"github/acme/--flows",
		"github/../flows",
		"github/acme/..",
		"github/acme/flows/../x.yaml",
		"github/acme/flows/./x.yaml",
		"github/acme/flows//x.yaml",
		"github/acme/flows/x\n.yaml",
		"github/acme/flows@a..b",
		"github/acme/flows@a:b",
		"github/acme/flows@+main",
		"github/acme/flows@main.lock",
		"github/acme/flows@main/",
		// What git refuses as a branch's or a tag's name, which ACTIVATE
		// takes, to find no such revision.
		"github/acme/flows@a/.b",
		"github/acme/flows@x/./y",
		"github/acme/flows@a.lock/b",
		"github/acme/flows@.a",
		"github/acme/flows@a//b",
		"github/acme/flows@/a",
		"github/acme/flows@a.",
		"github/acme/flows@@{1}",
		"github/acme/flows@x^{tree}",
		"github/acme/flows@",
		"https://github.com/acme/flows",
		"github/acme",
		"",
	} {
		if r, err := parseRef(plan.SourceRef{Uses: ref}); !errors.Is(err, compile.ErrNotFound) {
			t.Errorf("parseRef(%q) = %+v, %v, want ErrNotFound", ref, r, err)
		}
	}
}

func TestGitEnv(t *testing.T) {
	user := []string{
		"HOME=/home/u", "PATH=/bin", "GIT_DIR=/elsewhere", "GIT_WORK_TREE=/w", "GIT_TERMINAL_PROMPT=1", "LC_ALL=fr_FR.UTF-8",
		"GIT_SSH_COMMAND=ssh -i k", "GIT_SHALLOW_FILE=/s", "GIT_REPLACE_REF_BASE=refs/x/", "GIT_NO_REPLACE_OBJECTS=1",
		"GIT_QUARANTINE_PATH=/q", "GIT_PREFIX=p/", "GIT_EXEC_PATH=/x", "GIT_TEMPLATE_DIR=/t", "GIT_ASKPASS=/usr/bin/ksshaskpass",
		"GIT_CONFIG_PARAMETERS='core.hooksPath'='/h'", "GIT_CONFIG_COUNT=1", "GIT_SSL_CERT=/c.pem", "GIT_SSL_KEY=/k.pem",
		"SSH_ASKPASS=/usr/bin/x", "DISPLAY=:0", "HTTPS_PROXY=http://proxy:3128", "DBUS_SESSION_BUS_ADDRESS=unix:path=/b",
		"XDG_RUNTIME_DIR=/run/user/1000", "GH_TOKEN=t", "GITHUB_TOKEN=t", "GCM_CREDENTIAL_STORE=secretservice",
		"LD_LIBRARY_PATH=/opt/lib", "SSH_ASKPASS_REQUIRE=force", "LANGUAGE=fr",
	}
	common := []string{"GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=", "SSH_ASKPASS_REQUIRE=never", "LC_ALL=C", "LANGUAGE="}
	kept := []string{
		"HOME=/home/u", "PATH=/bin", "GIT_SSH_COMMAND=ssh -i k", "GIT_SSL_CERT=/c.pem", "GIT_SSL_KEY=/k.pem",
		"DISPLAY=:0", "HTTPS_PROXY=http://proxy:3128", "DBUS_SESSION_BUS_ADDRESS=unix:path=/b",
		"XDG_RUNTIME_DIR=/run/user/1000", "GH_TOKEN=t", "GITHUB_TOKEN=t", "GCM_CREDENTIAL_STORE=secretservice",
		"LD_LIBRARY_PATH=/opt/lib",
	}
	if env, want := gitEnv(user, ""), slices.Concat(kept, common); !slices.Equal(env, want) {
		t.Errorf("gitEnv = %q, want %q", env, want)
	}
	want := slices.Concat([]string{"HOME=/home/u"}, common, []string{"GIT_SSH_COMMAND=" + batchSSH})
	if env := gitEnv([]string{"HOME=/home/u"}, batchSSH); !slices.Equal(env, want) {
		t.Errorf("gitEnv with an ssh command = %q, want %q", env, want)
	}
}

// TestSSHCommand checks that git's ssh never prompts, unless the user named
// their own ssh command, which Meridian leaves as it is.
func TestSSHCommand(t *testing.T) {
	_, s := newOrigins(t)
	if got, ok := s.sshCommand(t.Context()); got != batchSSH || !ok {
		t.Errorf("sshCommand = %q, %t, want %q", got, ok, batchSSH)
	}
	t.Setenv("GIT_SSH_COMMAND", "ssh -i key")
	if got, ok := s.sshCommand(t.Context()); got != "" || !ok {
		t.Errorf("sshCommand with the user's = %q, %t, want none", got, ok)
	}
	t.Setenv("GIT_SSH_COMMAND", "")
	global := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(global, []byte("[core]\n\tsshCommand = ssh -F mine\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	if got, ok := s.sshCommand(t.Context()); got != "" || !ok {
		t.Errorf("sshCommand with the user's core.sshCommand = %q, %t, want none", got, ok)
	}
	// A context that has ended doesn't decide: the probe has a deadline of
	// its own.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := s.sshFor(ctx); got != "" {
		t.Errorf("sshFor with a cancelled context = %q, want the user's own", got)
	}
	// A probe that fails isn't remembered.
	s2 := sourcesFrom(t, t.TempDir())
	s2.git = filepath.Join(t.TempDir(), "no-git")
	if got := s2.sshFor(t.Context()); got != batchSSH || s2.sshKnown {
		t.Errorf("sshFor without git = %q, remembered %t", got, s2.sshKnown)
	}
}

// TestGitErrorClasses checks how git's messages are told apart.
func TestGitErrorClasses(t *testing.T) {
	for stderr, want := range map[string]string{
		"fatal: couldn't find remote ref nope":                                                                         "not found",
		"remote: Repository not found.\nfatal: repository 'https://github.com/a/b.git/' not found":                     "not found",
		"fatal: could not read Username for 'https://github.com': terminal prompts disabled":                           "denied",
		"git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.":                "denied",
		"error: cannot open '/home/u/.cache/meridian/sources/github.com/a/b.git/FETCH_HEAD': Permission denied":        "other",
		"fatal: Unable to create '/c/b.git/shallow.lock': Permission denied":                                           "other",
		"remote: API rate limit exceeded\nfatal: unable to access 'https://x/': The requested URL returned error: 403": "other",
		"Host key verification failed.\nfatal: Could not read from remote repository.":                                 "denied",
		"remote: HTTP Basic: Access denied\nfatal: Authentication failed for 'https://gitlab.com/a/b.git/'":            "denied",
		"fatal: unable to access 'https://x/': The requested URL returned error: 403":                                  "denied",
		"error: object 1234: badTimezone: invalid author/committer line\nfatal: fsck error in packed object":           "malformed",
		"fatal: unable to access 'https://x/': Could not resolve host: x":                                              "other",
		"error: RPC failed; curl 56 Recv failure: Connection reset by peer":                                            "other",
	} {
		e := &gitError{stderr: stderr}
		got := "other"
		switch {
		case e.notFound():
			got = "not found"
		case e.denied():
			got = "denied"
		case e.malformed():
			got = "malformed"
		}
		if got != want {
			t.Errorf("%q is %s, want %s", stderr, got, want)
		}
	}
}

// TestSourcesAccessDenied checks that a remote that asks for credentials
// git doesn't have, as a private or missing repository on github.com does,
// is refused at once, as a problem that asking again won't change.
func TestSourcesAccessDenied(t *testing.T) {
	_, s := newOrigins(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	s.remote = func(_, repo string) string { return server.URL + "/" + repo + ".git" }
	_, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/private@main"})
	p := wantProblem(t, err, problems.SourceAccessDenied)
	if p.Params["location"] != "github.com/acme/private@main:workflow.yaml" || !strings.Contains(p.Params["reason"].(string), "terminal prompts disabled") {
		t.Errorf("the problem's params are %v", p.Params)
	}
	if errors.Is(err, compile.ErrNotFound) {
		t.Error("access denied is ErrNotFound")
	}
}

// hungRemote returns the address of a server that accepts connections and
// never answers them: a hung network.
func hungRemote(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

// TestSourcesHungRemote checks that a context's deadline stops a fetch from
// a remote that never answers, and git's helpers with it, which would
// otherwise hold its output open.
func TestSourcesHungRemote(t *testing.T) {
	_, s := newOrigins(t)
	addr := hungRemote(t)
	s.remote = func(_, repo string) string { return "http://" + addr + "/" + repo + ".git" }
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := s.Resolve(ctx, plan.SourceRef{Uses: "github/acme/flows@main"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Resolve from a hung remote: %v, want the deadline's error", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("Resolve took %v, past its deadline of a second", took)
	}
}

// TestSourcesHungRemoteBlocksNoOther checks that one repository's hung
// fetch keeps no other repository waiting.
func TestSourcesHungRemoteBlocksNoOther(t *testing.T) {
	origins, s := newOrigins(t)
	addr := hungRemote(t)
	newOrigin(t, origins, "github.com", "good/flows").commit(map[string]string{"workflow.yaml": "ok\n"})
	s.remote = func(host, repo string) string {
		if repo == "acme/flows" {
			return "http://" + addr + "/" + repo + ".git"
		}
		return filepath.Join(origins, host, repo)
	}
	hungCtx, stop := context.WithCancel(t.Context())
	hung := make(chan error, 1)
	go func() {
		_, err := s.Resolve(hungCtx, plan.SourceRef{Uses: "github/acme/flows@main"})
		hung <- err
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := s.Resolve(ctx, plan.SourceRef{Uses: "github/good/flows@main"}); err != nil {
		t.Errorf("Resolve of another repository while one hangs: %v", err)
	}
	stop()
	if err := <-hung; !errors.Is(err, context.Canceled) {
		t.Errorf("the hung Resolve: %v, want its context's error", err)
	}
}

// TestSourcesProcessesShareCache checks that Sources of separate processes,
// which share no lock but their cache's, can each resolve a repository the
// cache has never seen, at once, and leave it usable.
func TestSourcesProcessesShareCache(t *testing.T) {
	origins, _ := newOrigins(t)
	o := newOrigin(t, origins, "github.com", "acme/flows")
	const n = 12
	for i := range n {
		o.commit(map[string]string{"workflow.yaml": strings.Repeat("x", i+1)})
		o.git("tag", fmt.Sprintf("t%d", i))
	}
	cache := t.TempDir()
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			s := NewSources(cache, nil)
			s.remote = func(host, repo string) string { return filepath.Join(origins, host, repo) }
			var w *compile.Workflow
			w, errs[i] = s.Resolve(t.Context(), plan.SourceRef{Uses: fmt.Sprintf("github/acme/flows@t%d", i)})
			if errs[i] == nil && len(w.Text) != i+1 {
				errs[i] = fmt.Errorf("t%d has %d bytes", i, len(w.Text))
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Resolve %d: %v", i, err)
		}
	}
	s := NewSources(cache, nil)
	s.remote = func(host, repo string) string { return filepath.Join(origins, host, repo) }
	if _, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"}); err != nil {
		t.Errorf("Resolve after them: %v", err)
	}
}

// TestSourcesRunNoHooks checks that git runs none of the hooks a copy's
// directory could hold.
func TestSourcesRunNoHooks(t *testing.T) {
	origins, s := newOrigins(t)
	newOrigin(t, origins, "github.com", "acme/flows").commit(map[string]string{"workflow.yaml": "text\n"})
	if _, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"}); err != nil {
		t.Fatal(err)
	}
	ran := filepath.Join(t.TempDir(), "ran")
	hook := filepath.Join(s.dir, "github.com", "acme", "flows.git", "hooks", "reference-transaction")
	if err := os.MkdirAll(filepath.Dir(hook), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ntouch "+ran+"\n"), 0o700); err != nil { //nolint:gosec // a hook that must not run
		t.Fatal(err)
	}
	if _, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ran); err == nil {
		t.Error("git ran a hook in the copy")
	}
	entries, err := os.ReadDir(filepath.Join(s.dir, hooksDir))
	if err != nil || len(entries) != 0 {
		t.Errorf("the hooks directory has %d entries, %v", len(entries), err)
	}
}

// TestSourcesRunNoMaintenance checks that git starts no automatic
// maintenance on a copy, which would go on writing into it after Resolve
// returned, even when the user's configuration asks for it after every
// fetch that leaves two packs: by the geometric strategy, Git 2.54's
// default, and, for a git without it, by git gc --auto's limit on packs.
func TestSourcesRunNoMaintenance(t *testing.T) {
	origins, s := newOrigins(t)
	global := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(global, []byte("[maintenance]\n\tstrategy = geometric\n[gc]\n\tautoPackLimit = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	o := newOrigin(t, origins, "github.com", "acme/flows")
	first := o.commit(map[string]string{"workflow.yaml": "first\n"})
	o.commit(map[string]string{"workflow.yaml": "second\n"})
	repo := filepath.Join(s.dir, "github.com", "acme", "flows.git")
	packs := func() []string {
		t.Helper()
		packs, err := filepath.Glob(filepath.Join(repo, "objects", "pack", "*.pack"))
		if err != nil {
			t.Fatal(err)
		}
		return packs
	}
	if _, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@" + first}); err != nil {
		t.Fatal(err)
	}
	// A fetch keeps the pack it receives, or none in a git that unpacks a
	// small one.
	kept := len(packs())
	if _, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"}); err != nil {
		t.Fatal(err)
	}

	// Nothing still runs on the copy, or writes into it.
	before := treeState(t, repo)
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); {
		if procs := gitProcesses(repo); len(procs) > 0 {
			t.Fatalf("git still runs on the copy after Resolve returned: %q", procs)
		}
		if changed := changedPaths(before, treeState(t, repo)); len(changed) > 0 {
			t.Fatalf("the copy changed after Resolve returned: %q", changed)
		}
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo // the test is that nothing happens for a while, so there is nothing to wait for
	}
	// Nor did maintenance run before Resolve returned: the copy keeps each
	// pack its fetches made, which maintenance would have merged.
	if got := packs(); len(got) != 2*kept {
		t.Errorf("the copy has packs %q, want the %d its fetches made", got, 2*kept)
	}
	for _, name := range []string{"gc.pid", "gc.log", filepath.Join("objects", "maintenance.lock")} {
		if _, err := os.Lstat(filepath.Join(repo, name)); err == nil {
			t.Errorf("the copy has %s, which maintenance leaves", name)
		}
	}
}

// treeState returns each file and directory under dir, with its size and
// the time it last changed.
func treeState(t *testing.T, dir string) map[string]string {
	t.Helper()
	state := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		state[path] = fmt.Sprint(info.Mode(), info.Size(), info.ModTime().UnixNano())
		return nil
	})
	if err != nil {
		t.Fatalf("reading %s, which changed meanwhile: %v", dir, err)
	}
	return state
}

// changedPaths returns the paths that before and after don't have alike.
func changedPaths(before, after map[string]string) []string {
	var changed []string
	for path, state := range after {
		if before[path] != state {
			changed = append(changed, path)
		}
	}
	for path := range before {
		if _, ok := after[path]; !ok {
			changed = append(changed, path)
		}
	}
	slices.Sort(changed)
	return changed
}

// gitProcesses returns the processes, by ID and command line, whose
// environment names repo as git's GIT_DIR, as git does for every command
// it starts, where /proc lists them: none elsewhere.
func gitProcesses(repo string) []string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var procs []string
	for _, e := range entries {
		env, err := os.ReadFile(filepath.Join("/proc", e.Name(), "environ"))
		if err != nil || !slices.Contains(strings.Split(string(env), "\x00"), "GIT_DIR="+repo) {
			continue
		}
		cmdline, _ := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		procs = append(procs, e.Name()+": "+strings.Join(strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00"), " "))
	}
	return procs
}

func TestSourcesGitMissing(t *testing.T) {
	origins, s := newOrigins(t)
	newOrigin(t, origins, "github.com", "acme/flows").commit(map[string]string{"workflow.yaml": "text\n"})
	s.git = filepath.Join(t.TempDir(), "no-git")
	_, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"})
	wantProblem(t, err, problems.GitUnavailable)
}

func TestSourcesCancelled(t *testing.T) {
	origins, s := newOrigins(t)
	newOrigin(t, origins, "github.com", "acme/flows").commit(map[string]string{"workflow.yaml": "text\n"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Resolve(ctx, plan.SourceRef{Uses: "github/acme/flows@main"}); !errors.Is(err, context.Canceled) {
		t.Errorf("Resolve with a cancelled context: %v", err)
	}
}

func TestSourcesCacheIsPrivate(t *testing.T) {
	origins, s := newOrigins(t)
	newOrigin(t, origins, "github.com", "acme/flows").commit(map[string]string{"workflow.yaml": "text\n"})
	if _, err := s.Resolve(t.Context(), plan.SourceRef{Uses: "github/acme/flows@main"}); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{s.dir, filepath.Join(s.dir, "github.com"), filepath.Join(s.dir, "github.com", "acme")} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("%s has mode %04o, want 0700", dir, perm)
		}
	}
}

// TestGitEnvCase checks that the GIT_ variables git doesn't get are left
// out whatever their case on Windows, which looks variables up regardless
// of case, and only as written elsewhere.
func TestGitEnvCase(t *testing.T) {
	env := gitEnv([]string{"Git_Dir=/elsewhere", "git_exec_path=/x", "Ssh_AskPass=/a"}, "")
	if runtime.GOOS == "windows" {
		for _, kv := range env {
			if name, _, _ := strings.Cut(kv, "="); strings.EqualFold(name, "GIT_DIR") || strings.EqualFold(name, "GIT_EXEC_PATH") || strings.EqualFold(name, "SSH_ASKPASS") {
				t.Errorf("gitEnv kept %s", kv)
			}
		}
		return
	}
	for _, want := range []string{"Git_Dir=/elsewhere", "git_exec_path=/x", "Ssh_AskPass=/a"} {
		if !slices.Contains(env, want) {
			t.Errorf("gitEnv left out %s, which isn't one of git's variables here", want)
		}
	}
}

// TestGitMessageRedactsUserinfo checks that a credential in a URL that git
// quotes never reaches a problem's reason.
func TestGitMessageRedactsUserinfo(t *testing.T) {
	for stderr, want := range map[string]string{
		"fatal: unable to access 'https://x-access-token:SECRET123@github.com/a/b.git/': The requested URL returned error: 403": "fatal: unable to access 'https://***@github.com/a/b.git/': The requested URL returned error: 403",
		"fatal: repository 'https://ghp_SECRET123@github.com/a/b.git/' not found":                                               "fatal: repository 'https://***@github.com/a/b.git/' not found",
		"fatal: unable to access 'http://user:p%40ss@host:8080/x': Could not resolve host: host":                                "fatal: unable to access 'http://***@host:8080/x': Could not resolve host: host",
		"fatal: could not read from ssh://deploy:SECRET123@git.example.com/r.git and\nhttps://t@x/y":                            "fatal: could not read from ssh://***@git.example.com/r.git and https://***@x/y",
		"git@github.com: Permission denied (publickey).":                                                                        "git@github.com: Permission denied (publickey).",
		"fatal: unable to access 'https://user:SECRET123@more@host/x': error":                                                   "fatal: unable to access 'https://***@host/x': error",
		"fatal: unable to access 'https://github.com/a/b.git/': Could not resolve host: github.com":                             "fatal: unable to access 'https://github.com/a/b.git/': Could not resolve host: github.com",
	} {
		e := &gitError{args: []string{"fetch"}, stderr: stderr, err: errors.New("exit status 128")}
		if got := e.message(); got != want {
			t.Errorf("message() = %q, want %q", got, want)
		}
		if strings.Contains(e.Error(), "SECRET123") || strings.Contains(e.Error(), "p%40ss") {
			t.Errorf("Error() = %q holds the credential", e.Error())
		}
	}
	p := problems.SourceAccessDenied.Problem(problems.Params{"location": "github.com/a/b@main:workflow.yaml", "reason": (&gitError{stderr: "fatal: unable to access 'https://x:SECRET123@github.com/a/b.git/': The requested URL returned error: 401"}).message()})
	if strings.Contains(p.Detail, "SECRET123") || strings.Contains(fmt.Sprint(p.Params), "SECRET123") {
		t.Errorf("the problem holds the credential: %v", p)
	}
}
