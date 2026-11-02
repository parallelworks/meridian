package conformancetest

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/plan"
)

// digestOf returns the digest of a workflow's text, as a plan's Source has
// it.
func digestOf(text []byte) string {
	sum := sha256.Sum256(text)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Sources gives the workflows a case's steps call, as the conformance
// suite says (conformance.Case.Repositories): each repository the case
// names holds the files of its directory, on its default branch, main.
// A case that names none has none: nil.
func Sources(c conformance.Case) compile.Sources {
	if len(c.Repositories) == 0 {
		return nil
	}
	return caseSources{c}
}

// caseSources are a case's repositories (Sources).
type caseSources struct{ c conformance.Case }

func (s caseSources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	n := ref.Normal()
	repo, rev, _ := strings.Cut(n.Uses, "@")
	if !slices.Contains(s.c.Repositories, repo) || rev != "" && rev != "main" {
		return nil, compile.ErrNotFound
	}
	file := cmp.Or(n.Path, "workflow.yaml")
	text, err := s.c.File(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", compile.ErrNotFound, err)
	}
	digest := digestOf(text)
	owner := strings.TrimPrefix(repo, "github/")
	return &compile.Workflow{
		Source: plan.Source{Location: "github.com/" + owner + "/" + file, Revision: digest, Digest: digest, Ref: ref},
		Text:   text,
		Remote: map[string]string{"repo": "https://github.com/" + owner, "yaml": file, "branch": "main"},
	}, nil
}

func (s caseSources) Read(ctx context.Context, source plan.Source) (*compile.Workflow, error) {
	w, err := s.Resolve(ctx, source.Ref)
	if err != nil {
		return nil, err
	}
	if w.Source.Digest != source.Digest {
		return nil, errors.New("conformancetest: the workflow isn't the text it was pinned to")
	}
	return w, nil
}

// Pin finds the commit of the case's repository repo names, as
// https://github.com/<owner>/<repository>, with or without .git, at its
// branch main, or at that commit itself (Commit), whose tree a
// parallelworks/checkout step checks out (Tree); and none of another.
func (s caseSources) Pin(_ context.Context, repo, revision string) (string, error) {
	commit := Commit(s.c)
	if !Holds(s.c, repo) || revision != "main" && revision != commit {
		return "", compile.ErrNotFound
	}
	return commit, nil
}

// Commit is the commit each of a case's repositories is at, on its branch
// main: the hex of its name's SHA-256, as long as a Git commit's.
func Commit(c conformance.Case) string {
	sum := sha256.Sum256([]byte(c.Name))
	return hex.EncodeToString(sum[:20])
}

// Holds reports whether repo, as a parallelworks/checkout step's with:
// names a repository, https://github.com/<owner>/<repository>, with or
// without .git, is one of the case's (conformance.Case.Repositories).
func Holds(c conformance.Case, repo string) bool {
	rest, ok := strings.CutPrefix(repo, "https://github.com/")
	return ok && slices.Contains(c.Repositories, "github/"+strings.TrimSuffix(rest, ".git"))
}

// Tree returns the tree of the case's repositories at Commit, as a
// parallelworks/checkout step checks one out: the files of the case's
// directory, by their paths in it.
func Tree(c conformance.Case) (map[string][]byte, error) {
	names, err := c.Files()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(names))
	for _, name := range names {
		if out[name], err = c.File(name); err != nil {
			return nil, err
		}
	}
	return out, nil
}
