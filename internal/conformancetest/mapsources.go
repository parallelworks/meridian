package conformancetest

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/plan"
)

// MapCommit is the commit MapSources pins each workflow it gives to.
const MapCommit = "0123456789abcdef0123456789abcdef01234567"

// MapSources are a test's compile.Sources, of the workflows Texts holds by
// their uses:. Each is at the location uses:path, where path is the file
// its $yaml names, pinned to MapCommit and to its text's digest, with a
// remote context of its own if Remote is set. A repository's revision,
// repo@revision, is pinned to its commit in Commits, or else to Commit. A
// workflow or a revision MapSources doesn't have is Err, or else
// compile.ErrNotFound. MapSources keeps what it was asked to resolve and
// to pin, which Asked and Pinned give.
type MapSources struct {
	Texts   map[string]string
	Remote  bool
	Commits map[string]string
	Commit  string
	Err     error
	// ResolveOnly has Read fail, for a test that a run's Create, which
	// only resolves, never reads.
	ResolveOnly bool

	mu     sync.Mutex
	asked  []plan.SourceRef
	pinned []string
}

var _ compile.Sources = (*MapSources)(nil)

// Resolve gives the workflow ref names.
func (s *MapSources) Resolve(_ context.Context, ref plan.SourceRef) (*compile.Workflow, error) {
	s.mu.Lock()
	s.asked = append(s.asked, ref)
	s.mu.Unlock()
	return s.workflow(ref)
}

// Read gives the workflow source pins, as Resolve gave it, or fails for
// one whose text isn't the digest's, or with ResolveOnly.
func (s *MapSources) Read(_ context.Context, source plan.Source) (*compile.Workflow, error) {
	if s.ResolveOnly {
		return nil, errors.New("conformancetest: not read as the run is created")
	}
	w, err := s.workflow(source.Ref)
	if err != nil {
		return nil, err
	}
	if source.Digest != "" && w.Source.Digest != source.Digest {
		return nil, errors.New("conformancetest: the workflow isn't the text it was pinned to")
	}
	return w, nil
}

// Pin pins repo's revision.
func (s *MapSources) Pin(_ context.Context, repo, revision string) (string, error) {
	s.mu.Lock()
	s.pinned = append(s.pinned, repo+"@"+revision)
	s.mu.Unlock()
	if commit, ok := s.Commits[repo+"@"+revision]; ok {
		return commit, nil
	}
	if s.Commit != "" {
		return s.Commit, nil
	}
	return "", s.notFound()
}

// Asked returns the refs Resolve was asked for, in order.
func (s *MapSources) Asked() []plan.SourceRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.asked)
}

// Pinned returns the revisions Pin was asked for, as repo@revision, in
// order.
func (s *MapSources) Pinned() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.pinned)
}

// Digest returns the digest of the text of the workflow uses names, as a
// plan's Source has it.
func (s *MapSources) Digest(uses string) string {
	return digestOf([]byte(s.Texts[uses]))
}

// workflow returns the workflow ref names.
func (s *MapSources) workflow(ref plan.SourceRef) (*compile.Workflow, error) {
	text, ok := s.Texts[ref.Uses]
	if !ok {
		return nil, s.notFound()
	}
	w := &compile.Workflow{
		Source: plan.Source{Location: ref.Uses + ":" + ref.Path, Revision: MapCommit, Digest: digestOf([]byte(text)), Ref: ref},
		Text:   []byte(text),
	}
	if s.Remote {
		w.Remote = map[string]string{"repo": "https://example.com/" + ref.Uses, "yaml": ref.Path}
	}
	return w, nil
}

// notFound is the error of a workflow or a revision s doesn't have.
func (s *MapSources) notFound() error {
	return cmp.Or(s.Err, error(compile.ErrNotFound))
}
