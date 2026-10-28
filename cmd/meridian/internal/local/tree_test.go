package local

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/problems"
)

// A treeMember is a member of a tree's tar stream, as a test reads it.
type treeMember struct {
	kind byte
	mode int64
	data string
}

// readTree reads the tar stream data, by its members' names, in order.
func readTree(t *testing.T, data []byte) ([]string, map[string]treeMember) {
	t.Helper()
	var names []string
	members := map[string]treeMember{}
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names, members
		}
		if err != nil {
			t.Fatalf("the tree isn't a tar stream: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		m := treeMember{kind: h.Typeflag, mode: h.Mode, data: string(body)}
		if h.Typeflag == tar.TypeSymlink {
			m.data = h.Linkname
		}
		names = append(names, h.Name)
		members[h.Name] = m
	}
}

// treeOrigin returns Sources that fetch the repository repo from an
// origin of its own, and that origin, with a commit of a file, an
// executable file in a directory, a symbolic link, a submodule, and a file
// that an attribute keeps out of an archive, but not out of a clone.
func treeOrigin(t *testing.T, repo string) (*Sources, *origin, string) {
	t.Helper()
	origins, s := newOrigins(t)
	o := newOrigin(t, origins, "example.com", "acme/app")
	s.repoURL = func(string) string { return o.dir }
	first := o.commit(map[string]string{"README.md": "read me\n", "app/main.go": "package main\n", "app/run.sh": "#!/bin/sh\n", "apps.md": "apps\n"})
	if err := os.Chmod(filepath.Join(o.dir, "app/run.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("app/main.go", filepath.Join(o.dir, "link")); err != nil {
		t.Fatal(err)
	}
	o.commit(map[string]string{".gitattributes": "kept.txt export-ignore\n", "kept.txt": "kept\n"})
	// A submodule, which add --all would drop, as its directory isn't
	// there.
	o.git("update-index", "--add", "--cacheinfo", "160000,"+first+",vendor/lib")
	o.git("commit", "--quiet", "--message=submodule")
	commit := o.git("rev-parse", "HEAD")
	if _, err := s.Pin(t.Context(), repo, "main"); err != nil {
		t.Fatal(err)
	}
	return s, o, commit
}

// TestSourcesTree checks what SurveyTree reads of a commit's tree, and
// WriteTree writes of it: all of it, or the paths a sparse checkout names,
// with which of those name nothing; each directory before what it holds;
// a path a sparse entry is only a prefix of, as apps.md is of app, left
// out; a file with its executable bit, and as it is committed, though an
// attribute would keep it out of an archive; a symbolic link with its
// target; and a submodule as an empty directory, which a survey says the
// tree has.
func TestSourcesTree(t *testing.T) {
	const repo = "https://example.com/acme/app.git"
	s, _, commit := treeOrigin(t, repo)

	survey, err := s.SurveyTree(t.Context(), repo, commit, nil)
	if want := int64(len("read me\npackage main\n#!/bin/sh\napps\napp/main.gokept.txt export-ignore\nkept\n")); err != nil || survey.Size != want || survey.Entries != 10 || !survey.Submodules || len(survey.Missing) != 0 {
		t.Errorf("the whole tree's survey is %+v, %v; want %d bytes, with submodules", survey, err, want)
	}
	var buf bytes.Buffer
	if err := s.WriteTree(t.Context(), repo, commit, nil, &buf); err != nil {
		t.Fatal(err)
	}
	names, members := readTree(t, buf.Bytes())
	want := []string{".gitattributes", "README.md", "app/", "app/main.go", "app/run.sh", "apps.md", "kept.txt", "link", "vendor/", "vendor/lib/"}
	if !slices.Equal(names, want) {
		t.Errorf("the whole tree holds %q; want %q", names, want)
	}
	for name, m := range map[string]treeMember{
		"app/":        {tar.TypeDir, 0o755, ""},
		"app/main.go": {tar.TypeReg, 0o644, "package main\n"},
		"app/run.sh":  {tar.TypeReg, 0o755, "#!/bin/sh\n"},
		"kept.txt":    {tar.TypeReg, 0o644, "kept\n"},
		"link":        {tar.TypeSymlink, 0o777, "app/main.go"},
		"vendor/lib/": {tar.TypeDir, 0o755, ""},
	} {
		if got := members[name]; got != m {
			t.Errorf("%s is %+v; want %+v", name, got, m)
		}
	}

	paths := []string{"app/", "README.md", "gone", "vendor/lib"}
	survey, err = s.SurveyTree(t.Context(), repo, commit, paths)
	if want := int64(len("read me\npackage main\n#!/bin/sh\n")); err != nil || survey.Size != want || survey.Entries != 6 || !survey.Submodules || !slices.Equal(survey.Missing, []int{2}) {
		t.Errorf("the sparse tree's survey is %+v, %v; want %d bytes, with submodules, and path 2 missing", survey, err, want)
	}
	buf.Reset()
	if err := s.WriteTree(t.Context(), repo, commit, paths[:3], &buf); err != nil {
		t.Fatal(err)
	}
	if names, _ := readTree(t, buf.Bytes()); !slices.Equal(names, []string{"README.md", "app/", "app/main.go", "app/run.sh"}) {
		t.Errorf("the sparse tree holds %q", names)
	}
	buf.Reset()
	if err := s.WriteTree(t.Context(), repo, commit, []string{"gone"}, &buf); err != nil {
		t.Fatal(err)
	}
	if names, _ := readTree(t, buf.Bytes()); len(names) != 0 {
		t.Errorf("a tree of a path that names nothing holds %q", names)
	}
}

// TestSourcesTreeFetches checks that SurveyTree and WriteTree fetch a
// commit their copy doesn't hold, by itself, and fail without it once the
// repository can't give it.
func TestSourcesTreeFetches(t *testing.T) {
	const repo = "https://example.com/acme/app.git"
	s, o, commit := treeOrigin(t, repo)
	fresh := NewSources(t.TempDir(), nil)
	fresh.repoURL, fresh.protocols = s.repoURL, s.protocols
	var buf bytes.Buffer
	if err := fresh.WriteTree(t.Context(), repo, commit, []string{"README.md"}, &buf); err != nil {
		t.Fatalf("a commit the copy didn't hold wasn't fetched: %v", err)
	}
	if names, _ := readTree(t, buf.Bytes()); !slices.Equal(names, []string{"README.md"}) {
		t.Errorf("the fetched tree holds %q", names)
	}

	if err := os.RemoveAll(o.dir); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SurveyTree(t.Context(), repo, commit, nil); err != nil {
		t.Errorf("a commit the copy holds wasn't read without the repository: %v", err)
	}
	gone := NewSources(t.TempDir(), nil)
	gone.repoURL, gone.protocols = s.repoURL, s.protocols
	if _, err := gone.SurveyTree(t.Context(), repo, commit, nil); err == nil {
		t.Error("a commit neither the copy nor the repository holds was read")
	}
}

// TestSourcesTreeRefuses checks that SurveyTree and WriteTree refuse a
// repository Pin refuses, a commit that isn't one's name in full, and a
// path that isn't a literal one, before they run git.
func TestSourcesTreeRefuses(t *testing.T) {
	s := NewSources(t.TempDir(), nil)
	s.git = filepath.Join(t.TempDir(), "no-git")
	const commit = "0123456789abcdef0123456789abcdef01234567"
	_, err := s.SurveyTree(t.Context(), "file:///srv/app.git", commit, nil)
	kindtest.Want(t, err, problems.CheckoutRepoInvalid)
	err = s.WriteTree(t.Context(), "https://example.com/app.git", commit, []string{"a/*"}, io.Discard)
	kindtest.Want(t, err, problems.CheckoutPathInvalid)
	if _, err := s.SurveyTree(t.Context(), "https://example.com/app.git", "main", nil); err == nil || !strings.Contains(err.Error(), "commit") {
		t.Errorf("a revision that isn't a commit's name was surveyed: %v", err)
	}
}
