package worker

import (
	"archive/tar"
	"bytes"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// A member is one of a test tree's.
type member struct {
	kind     byte
	name     string
	mode     int64
	data     string
	linkname string
}

// tarOf returns the tar stream of members, as the coordinator streams a
// checkout's tree.
func tarOf(t *testing.T, members ...member) []byte {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, m := range members {
		h := &tar.Header{Typeflag: m.kind, Name: m.name, Mode: m.mode, Linkname: m.linkname, Size: int64(len(m.data))}
		if m.kind != tar.TypeReg {
			h.Size = 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(m.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// testTree is a tree of a file, a directory, an executable file in a
// directory not listed before it, and a symbolic link.
func testTree(t *testing.T) []byte {
	return tarOf(t,
		member{kind: tar.TypeReg, name: "README", mode: 0o644, data: "hello\n"},
		member{kind: tar.TypeDir, name: "docs/", mode: 0o755},
		member{kind: tar.TypeReg, name: "bin/run.sh", mode: 0o755, data: "#!/bin/sh\n"},
		member{kind: tar.TypeSymlink, name: "link", mode: 0o777, linkname: "README"},
	)
}

// checkoutStep returns a step that checks out the repository at main into
// path.
func checkoutStep(path string) plan.Step {
	return action(plan.CheckoutUses, map[string]any{"repo": "https://example.com/acme/app.git", "branch": "main", "path": path})
}

// TestCheckout checks that a checkout step asks the coordinator for its
// action, and then, once it succeeded, for its tree, with the same
// request, and writes it into its path: in its working directory, merging
// it with what is there: a file of the same name, and a symbolic link
// where the tree has a file, are replaced, and the rest is kept; with its
// executable bits and its links; and that its next step sees it.
func TestCheckout(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(checkoutStep("src"), command("cat src/README src/link")))
	f.Tree = func(workerapi.ActionRequest) []byte { return testTree(t) }
	src := filepath.Join(f.Claim.RunDir, "src")
	if err := os.MkdirAll(src, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"README": "old\n", "keep.txt": "kept\n", "elsewhere": "not here\n"} {
		if err := os.WriteFile(filepath.Join(src, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A link where the tree has a file is replaced, not written through.
	if err := os.MkdirAll(filepath.Join(src, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../elsewhere", filepath.Join(src, "bin", "run.sh")); err != nil {
		t.Fatal(err)
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success -1", "started 1.0", "finished 1.0 success 0")
	checkLog(t, f, logKey{workerapi.LogStep, 1, 0}, "hello\nhello\n")
	if n := f.Requests()[workerapi.PathCheckout]; n != 1 {
		t.Errorf("the tree was asked for %d times", n)
	}
	for name, want := range map[string]string{"README": "hello\n", "keep.txt": "kept\n", "elsewhere": "not here\n", "bin/run.sh": "#!/bin/sh\n"} {
		if data, err := os.ReadFile(filepath.Join(src, name)); err != nil || string(data) != want {
			t.Errorf("%s holds %q, %v; want %q", name, data, err, want)
		}
	}
	if fi, err := os.Lstat(filepath.Join(src, "bin", "run.sh")); err != nil || !fi.Mode().IsRegular() || fi.Mode()&0o100 == 0 {
		t.Errorf("bin/run.sh is %v, %v; want an executable file", fi, err)
	}
	if target, err := os.Readlink(filepath.Join(src, "link")); err != nil || target != "README" {
		t.Errorf("link is %q, %v", target, err)
	}
	if fi, err := os.Stat(filepath.Join(src, "docs")); err != nil || !fi.IsDir() {
		t.Errorf("docs is %v, %v", fi, err)
	}
	if fi, err := os.Stat(filepath.Join(src, "README")); err != nil || fi.Mode()&0o111 != 0 {
		t.Errorf("README is %v, %v; want a file not executable", fi, err)
	}
}

// TestWriteTreeLinks checks that writeTree writes through no link: a link
// in a member's leading directories, one an earlier step left, into .git
// or outside, or one the tree holds, as a system whose names ignore case
// reads link A and directory a/ of one tree as one, is replaced by a
// directory, as git checks out over one; and that a file replaces a file
// another name links to, rather than write into it, leaving nothing of
// its own name behind.
func TestWriteTreeLinks(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	dir, outside := filepath.Join(base, "dir"), filepath.Join(base, "outside")
	for _, d := range []string{filepath.Join(dir, ".git", "hooks"), filepath.Join(dir, "sub"), outside} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"h": ".git/hooks", "out": outside} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(outside, "secret"), filepath.Join(dir, "README")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	tree := tarOf(t,
		member{kind: tar.TypeReg, name: "h/post-commit", mode: 0o755, data: "planted\n"},
		member{kind: tar.TypeReg, name: "out/x", mode: 0o644, data: "x\n"},
		member{kind: tar.TypeSymlink, name: "A", linkname: "sub"},
		// As the system whose names ignore case reads a/hooks/post-commit.
		member{kind: tar.TypeReg, name: "A/hooks/post-commit", mode: 0o755, data: "cased\n"},
		member{kind: tar.TypeReg, name: "README", mode: 0o644, data: "read me\n"},
	)
	if err := writeTree(root, bytes.NewReader(tree), 1<<20, 100); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"h/post-commit": "planted\n", "out/x": "x\n", "A/hooks/post-commit": "cased\n", "README": "read me\n"} {
		if data, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(data) != want {
			t.Errorf("%s holds %q, %v; want %q", name, data, err, want)
		}
	}
	for _, name := range []string{"h", "out", "A"} {
		if fi, err := os.Lstat(filepath.Join(dir, name)); err != nil || !fi.IsDir() {
			t.Errorf("%s is %v, %v; want a directory in place of the link", name, fi, err)
		}
	}
	for _, gone := range []string{filepath.Join(dir, ".git", "hooks", "post-commit"), filepath.Join(outside, "x"), filepath.Join(dir, "sub", "hooks")} {
		if _, err := os.Lstat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was written through a link: %v", gone, err)
		}
	}
	if data, err := os.ReadFile(filepath.Join(outside, "secret")); err != nil || string(data) != "secret\n" {
		t.Errorf("the file README was a hard link to holds %q, %v", data, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".meridian-checkout-") {
			t.Errorf("%s is left", e.Name())
		}
	}
}

// TestCheckoutNothing checks that a checkout whose repo is "" checks out
// nothing, as in format 1: it asks for its action, and not for a tree.
func TestCheckoutNothing(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(action(plan.CheckoutUses, map[string]any{"repo": "", "branch": "main"})))
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success -1")
	if n := f.Requests()[workerapi.PathCheckout]; n != 0 {
		t.Errorf("a tree was asked for %d times", n)
	}
}

// TestCheckoutAgain checks that a tree whose stream is cut short is asked
// for again, and written whole; and that a checkout whose action failed
// asks for no tree, and fails, as its action did.
func TestCheckoutAgain(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(checkoutStep("src")))
	f.Tree = func(workerapi.ActionRequest) []byte { return testTree(t) }
	var once sync.Once
	f.Fault = func(r *http.Request) workerapitest.Fault {
		cut := workerapitest.Answer
		if r.URL.Path == workerapi.PathCheckout {
			once.Do(func() { cut = workerapitest.Cut })
		}
		return cut
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 success -1")
	if n := f.Requests()[workerapi.PathCheckout]; n != 2 {
		t.Errorf("the tree was asked for %d times", n)
	}
	if data, err := os.ReadFile(filepath.Join(f.Claim.RunDir, "src", "bin", "run.sh")); err != nil || string(data) != "#!/bin/sh\n" {
		t.Errorf("the tree asked for again wrote %q, %v", data, err)
	}

	failed := problems.CheckoutUnavailable.At("", problems.Position{}, "/jobs/build/steps/0/with/repo", problems.Params{"repo": "https://example.com/acme/app.git", "commit": "0123456789abcdef0123456789abcdef01234567", "reason": "gone"})
	g := workerapitest.NewFake(t, newJob(checkoutStep("src")))
	g.Act = func(workerapi.ActionRequest) workerapi.ActionAnswer {
		return workerapi.ActionAnswer{Outcome: plan.OutcomeFailure, Diagnostics: problems.Diagnostics{failed}}
	}
	if err := runWorker(t, g, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, g, "started 0.0", "finished 0.0 failure -1")
	if n := g.Requests()[workerapi.PathCheckout]; n != 0 {
		t.Errorf("the tree of a checkout whose action failed was asked for %d times", n)
	}
}

// TestCheckoutStopped checks that a cancel of its job stops a checkout as
// it reads its tree, at once, and the step ends cancelled.
func TestCheckoutStopped(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(checkoutStep("src")))
	f.Fault = func(r *http.Request) workerapitest.Fault {
		if r.URL.Path != workerapi.PathCheckout {
			return workerapitest.Answer
		}
		go f.SetControl(workerapi.Control{Cancel: true, Reason: plan.ReasonCancelled})
		return workerapitest.Hang
	}
	if err := runWorker(t, f, t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	checkEvents(t, f, "started 0.0", "finished 0.0 cancelled -1")
	checkEnd(t, f, plan.OutcomeCancelled, plan.ReasonCancelled)
}

// TestWriteTree checks what writeTree refuses to write, before it writes
// it: a member whose name leaves the directory, isn't clean, holds a
// backslash, or is .git, whatever its case; a link to .git, whatever its
// case; one that is neither a directory, a file nor a symbolic link;
// files past the limit, and members past theirs; and a write the system
// refuses, as of a file where a directory is.
func TestWriteTree(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		tree   []member
		kind   *problems.Kind
		reason string
	}{
		{"a parent", []member{{kind: tar.TypeReg, name: "../x", mode: 0o644, data: "x"}}, problems.CheckoutUnsafe, unsafeName},
		{"an absolute name", []member{{kind: tar.TypeReg, name: "/tmp/x", mode: 0o644, data: "x"}}, problems.CheckoutUnsafe, unsafeName},
		{"a name not clean", []member{{kind: tar.TypeReg, name: "a/../x", mode: 0o644, data: "x"}}, problems.CheckoutUnsafe, unsafeName},
		{"a .git", []member{{kind: tar.TypeReg, name: ".git/config", mode: 0o644, data: "x"}}, problems.CheckoutUnsafe, unsafeName},
		{"a .GIT deeper", []member{{kind: tar.TypeDir, name: "a/.GIT/", mode: 0o755}}, problems.CheckoutUnsafe, unsafeName},
		{"a backslash", []member{{kind: tar.TypeReg, name: `a\b`, mode: 0o644, data: "x"}}, problems.CheckoutUnsafe, unsafeName},
		{"a link to .git", []member{{kind: tar.TypeSymlink, name: "h", linkname: ".git/hooks"}}, problems.CheckoutUnsafe, unsafeLink},
		{"a link to .Git deeper", []member{{kind: tar.TypeSymlink, name: "h", linkname: "../x/.Git"}}, problems.CheckoutUnsafe, unsafeLink},
		{"a hard link", []member{{kind: tar.TypeReg, name: "a", mode: 0o644}, {kind: tar.TypeLink, name: "b", linkname: "a"}}, problems.CheckoutUnsafe, unsafeType},
		{"a device", []member{{kind: tar.TypeChar, name: "tty", mode: 0o644}}, problems.CheckoutUnsafe, unsafeType},
		{"a fifo", []member{{kind: tar.TypeFifo, name: "pipe", mode: 0o644}}, problems.CheckoutUnsafe, unsafeType},
		{"too large", []member{{kind: tar.TypeReg, name: "a", mode: 0o644, data: "12345"}, {kind: tar.TypeReg, name: "b", mode: 0o644, data: "123456"}}, problems.CheckoutTooLarge, ""},
		{"too many", []member{{kind: tar.TypeDir, name: "a/"}, {kind: tar.TypeDir, name: "b/"}, {kind: tar.TypeDir, name: "c/"}, {kind: tar.TypeDir, name: "d/"}}, problems.CheckoutTooMany, ""},
		{"a file where a directory is", []member{{kind: tar.TypeReg, name: "dir", mode: 0o644, data: "x"}}, problems.CheckoutWriteFailed, ""},
	} {
		base := t.TempDir()
		dir, outside := filepath.Join(base, "dir"), filepath.Join(base, "outside")
		for _, d := range []string{filepath.Join(dir, "dir"), outside} {
			if err := os.MkdirAll(d, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Symlink(outside, filepath.Join(dir, "out")); err != nil {
			t.Fatal(err)
		}
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		err = writeTree(root, bytes.NewReader(tarOf(t, tc.tree...)), 10, 3)
		root.Close()
		var p *treeProblem
		if !errors.As(err, &p) || p.kind != tc.kind || tc.reason != "" && p.params["reason"] != tc.reason {
			t.Errorf("%s: %v", tc.name, err)
		}
		for _, d := range []string{base, outside} {
			if entries, _ := os.ReadDir(d); len(entries) != map[string]int{base: 2, outside: 0}[d] {
				t.Errorf("%s: wrote outside the directory, in %s: %v", tc.name, d, entries)
			}
		}
	}

	// A stream cut short is an error reading it.
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	tree := tarOf(t, member{kind: tar.TypeReg, name: "a", mode: 0o644, data: strings.Repeat("x", 600)})
	var re *treeReadError
	if err := writeTree(root, bytes.NewReader(tree[:700]), 1<<20, 10); !errors.As(err, &re) {
		t.Errorf("a stream cut short: %v", err)
	}
}

// TestTreeProblemsEscaped checks that what a problem of a checkout's tree
// says of the tree, a member's name, or of the system, an error's text,
// has its control characters escaped, as a message's params do.
func TestTreeProblemsEscaped(t *testing.T) {
	t.Parallel()
	for _, err := range []error{
		unsafeMember("../\x1b[2J", unsafeName),
		writeFailed("a\x1b[2J", errors.New("refused\x1b]0;x\x07")),
	} {
		var p *treeProblem
		if !errors.As(err, &p) {
			t.Fatalf("%v isn't a treeProblem", err)
		}
		for name, v := range p.params {
			if s, ok := v.(string); ok && strings.ContainsAny(s, "\x1b\x07") {
				t.Errorf("%s's %s is %q, unescaped", p.kind.Code(), name, s)
			}
		}
	}
}
