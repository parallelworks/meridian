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
	sum := sha256.Sum256(text)
	digest := "sha256:" + hex.EncodeToString(sum[:])
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

// Pin finds no commit: a case's repositories hold workflows that its steps
// call, not trees that a parallelworks/checkout step checks out.
func (caseSources) Pin(context.Context, string, string) (string, error) {
	return "", compile.ErrNotFound
}
