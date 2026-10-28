package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// checkoutsDir is the name of the directory, in the sources directory,
// that holds the copies of the repositories parallelworks/checkout steps
// name, each by the SHA-256 digest of its URL as the step writes it.
const checkoutsDir = "checkouts"

// Pin pins revision, a branch, a tag or a full commit, of the repository
// repo, as a parallelworks/checkout step's with: writes them out, to the
// commit it names now, which it returns, and keeps in a copy of the
// repository in the cache directory, so that the step's tree is read from
// it, without the network, by that pin. repo is an https or ssh URL, or an
// scp-like one, on any host, as format 1 takes (plan.CheckRepo): git
// reaches it with the user's own configuration and credentials, as it
// does a workflow's repository, and never asks for a password. A revision
// git refuses as a branch's or a tag's name names no commit
// (source_revision_not_found), as one that isn't there does.
func (s *Sources) Pin(ctx context.Context, repo, revision string) (string, error) {
	if err := plan.CheckRepo(repo); err != nil {
		return "", err
	}
	if !validRevision(revision) {
		return "", notFound(problems.SourceRevisionNotFound, problems.Params{"repo": repo, "revision": revision, "reason": "not a branch's, a tag's or a commit's name"})
	}
	dir, unlock, err := s.openAt(ctx, s.checkoutCopy(repo))
	if err != nil {
		return "", err
	}
	defer unlock()
	if commitPattern.MatchString(revision) && s.has(ctx, dir, revision) {
		// A commit names itself, whatever the remote has since.
		return revision, nil
	}
	return s.fetchFrom(ctx, dir, s.repoURL(repo), revision, repo, repo)
}

// checkoutCopy returns where the copy of the repository repo, a checkout's,
// is: by the SHA-256 digest of repo as the step writes it, so that no part
// of it, which a workflow writes, is a name in the cache.
func (s *Sources) checkoutCopy(repo string) string {
	sum := sha256.Sum256([]byte(repo))
	return filepath.Join(s.dir, checkoutsDir, hex.EncodeToString(sum[:])+".git")
}
