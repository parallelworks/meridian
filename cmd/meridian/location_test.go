package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A workflow's location is its path in its repository, whether .git is a
// directory or a worktree's file, and the path is reached through a link
// or not; outside a repository, its file's name; and never the machine's
// directories.
func TestWorkflowLocation(t *testing.T) {
	dir := t.TempDir()
	write := func(path string) string {
		t.Helper()
		path = filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("jobs: {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if err := os.MkdirAll(filepath.Join(dir, "repo", ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	inRepo := write("repo/flows/build/workflow.yaml")
	write("worktree/.git")
	inWorktree := write("worktree/workflow.yaml")
	if err := os.Symlink(filepath.Join(dir, "repo", "flows"), filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"-":        stdinName,
		inRepo:     "flows/build/workflow.yaml",
		inWorktree: "workflow.yaml",
		filepath.Join(dir, "link", "build", "workflow.yaml"): "flows/build/workflow.yaml",
	}
	// Outside a repository, unless the test's own directory is in one.
	if !inGitRepository(dir) {
		cases[write("plain/mine.yaml")] = "mine.yaml"
	}
	for path, want := range cases {
		if got := workflowLocation(path); got != want {
			t.Errorf("workflowLocation(%q) = %q, want %q", path, got, want)
		}
	}
	// A relative path is read from the working directory.
	t.Chdir(filepath.Join(dir, "repo", "flows"))
	if got := workflowLocation(filepath.Join("build", "workflow.yaml")); got != "flows/build/workflow.yaml" {
		t.Errorf("a relative path's location is %q", got)
	}
}

// inGitRepository reports whether dir, or a directory above it, has .git.
func inGitRepository(dir string) bool {
	for {
		if _, err := os.Lstat(filepath.Join(dir, ".git")); err == nil {
			return true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return false
		}
		dir = parent
	}
}
