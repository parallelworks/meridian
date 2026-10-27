package local

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// TestSourcesPin checks that Pin pins a checkout's branch, tag or full
// commit to the commit it names now, from the repository its URL names, as
// a step writes it; that a commit it holds is pinned again without the
// network; and that a revision that names nothing, a short commit among
// them, is source_revision_not_found, which names no commit (ErrNotFound).
func TestSourcesPin(t *testing.T) {
	origins, s := newOrigins(t)
	const repo = "https://example.com/acme/app.git"
	o := newOrigin(t, origins, "example.com", "acme/app")
	s.repoURL = func(url string) string {
		if url != repo {
			t.Errorf("fetched %s", url)
		}
		return o.dir
	}
	first := o.commit(map[string]string{"app/main.go": "package main\n"})
	o.git("tag", "v1")
	second := o.commit(map[string]string{"app/main.go": "package main // 2\n"})

	for rev, want := range map[string]string{"main": second, "v1": first, first: first} {
		got, err := s.Pin(t.Context(), repo, rev)
		if err != nil || got != want {
			t.Errorf("Pin %s = %s, %v; want %s", rev, got, err, want)
		}
	}
	for _, rev := range []string{"gone", first[:12], "-x", "a..b."} {
		_, err := s.Pin(t.Context(), repo, rev)
		kindtest.Want(t, err, problems.SourceRevisionNotFound)
		if !errors.Is(err, compile.ErrNotFound) {
			t.Errorf("Pin %s: %v names a commit", rev, err)
		}
	}

	// The commit pinned, without the remote.
	if err := os.RemoveAll(o.dir); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Pin(t.Context(), repo, second); err != nil || got != second {
		t.Errorf("Pin of a commit it holds, without the remote: %s, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(s.dir, checkoutsDir)); err != nil {
		t.Errorf("the copies aren't in the checkouts directory: %v", err)
	}
}

// TestSourcesPinTransports checks that a fetch uses only https and ssh,
// whatever URL git ends up at, as through an insteadOf of the user's that
// makes a URL a path on the host.
func TestSourcesPinTransports(t *testing.T) {
	origins, _ := newOrigins(t)
	o := newOrigin(t, origins, "example.com", "acme/app")
	o.commit(map[string]string{"README.md": "read me\n"})
	s := NewSources(t.TempDir(), nil)
	s.repoURL = func(string) string { return o.dir }
	_, err := s.Pin(t.Context(), "https://example.com/acme/app.git", "main")
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Errorf("a fetch through the file transport: %v", err)
	}
}

// TestSourcesPinRefuses checks that Pin refuses what plan.CheckRepo does,
// before it runs git.
func TestSourcesPinRefuses(t *testing.T) {
	s := NewSources(t.TempDir(), nil)
	s.git = filepath.Join(t.TempDir(), "no-git")
	for _, repo := range []string{"file:///srv/app.git", "https://u:t@github.com/acme/app.git", "/srv/app.git"} {
		_, err := s.Pin(t.Context(), repo, "main")
		kindtest.Want(t, err, problems.CheckoutRepoInvalid)
	}
}
