package main

import (
	"os"
	"path/filepath"
)

// workflowLocation returns what names the workflow at path to whoever can
// see its run (plan.Source.Location): its path in the Git repository that
// holds it, as flows/workflow.yaml, or else its file's name, so that a run
// never records the directories of the machine that created it, which can
// name its user. A workflow read from standard input is stdinName.
func workflowLocation(path string) string {
	if path == "-" {
		return stdinName
	}
	file, err := filepath.Abs(path)
	if err != nil {
		return filepath.Base(path)
	}
	// The repository is the one that holds the file, wherever a symbolic
	// link on the path leads.
	if real, err := filepath.EvalSymlinks(file); err == nil {
		file = real
	}
	for dir := filepath.Dir(file); ; {
		// .git is a directory, or a file in a worktree or a submodule.
		if _, err := os.Lstat(filepath.Join(dir, ".git")); err == nil {
			if rel, err := filepath.Rel(dir, file); err == nil {
				return filepath.ToSlash(rel)
			}
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return filepath.Base(file)
}
