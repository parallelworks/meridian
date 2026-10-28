package local

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strconv"
	"strings"

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
// git could take for an option, or refuses as a branch's or a tag's name
// (validRevision), is never asked for (checkout_revision_invalid), and one
// the repository doesn't have names no commit (source_revision_not_found):
// both are the branch's problem (compile.ErrNotFound).
func (s *Sources) Pin(ctx context.Context, repo, revision string) (string, error) {
	if err := plan.CheckRepo(repo); err != nil {
		return "", err
	}
	if !validRevision(revision) {
		return "", notFound(problems.CheckoutRevisionInvalid, problems.Params{"revision": revision})
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

// A TreeSurvey is what the tree of a commit holds of a checkout's paths
// (SurveyTree).
type TreeSurvey struct {
	// Size is how many bytes its files hold, together, its symbolic links'
	// targets among them, and Entries how many entries WriteTree writes of
	// it: its directories, files, links and submodules.
	Size    int64
	Entries int
	// Missing are the indexes of the paths that name nothing in it.
	Missing []int
	// Submodules is set when what it holds of the paths has a submodule.
	Submodules bool
}

// A treeEntry is a file, a symbolic link or a submodule of a commit's
// tree, as ls-tree lists it: its mode, its object, its size, for a blob,
// and its path from the tree's root.
type treeEntry struct {
	mode   uint32
	object string
	size   int64
	path   string
}

// The modes of a tree's entries that aren't files.
const (
	modeSymlink   = 0o120000
	modeSubmodule = 0o160000
)

// maxLinkTarget is the most bytes a symbolic link's target takes that a
// tree's tar holds.
const maxLinkTarget = 4096

// SurveyTree returns what the tree of commit, of the repository repo, as a
// parallelworks/checkout step writes it, holds of paths, its
// sparse_checkout's entries, each a literal path from the repository's
// root, which a slash can end (plan.CheckCheckoutPath), or of all of it,
// for none. It reads the commit from the copy of repo that pinned it
// (Pin), and fetches it from repo again, by itself, if the copy hasn't it.
func (s *Sources) SurveyTree(ctx context.Context, repo, commit string, paths []string) (TreeSurvey, error) {
	_, entries, missing, err := s.treeOf(ctx, repo, commit, paths)
	if err != nil {
		return TreeSurvey{}, err
	}
	survey := TreeSurvey{Missing: missing, Entries: len(entries)}
	dirs := map[string]bool{}
	for _, e := range entries {
		survey.Size += e.size
		survey.Submodules = survey.Submodules || e.mode == modeSubmodule
		for dir := path.Dir(e.path); dir != "." && !dirs[dir]; dir = path.Dir(dir) {
			dirs[dir] = true
		}
	}
	survey.Entries += len(dirs)
	return survey, nil
}

// WriteTree writes what the tree of commit, of repo, holds of paths, as
// SurveyTree reads it, to w, as a tar stream: each directory before what it
// holds, each file with its executable bit, each symbolic link with its
// target, and each submodule as an empty directory, with paths from the
// repository's root. The files are the blobs the repository holds, with
// none of its attributes applied, as a filter or a line ending, which a
// clone would apply.
func (s *Sources) WriteTree(ctx context.Context, repo, commit string, paths []string, w io.Writer) (err error) {
	dir, entries, _, err := s.treeOf(ctx, repo, commit, paths)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	args := []string{"cat-file", "--batch"}
	cmd := s.command(ctx, dir, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &limitedWriter{w: &stderr, n: 4 << 10}
	if err := cmd.Start(); err != nil {
		return s.commandError(ctx, args, "", err)
	}
	defer func() {
		stdin.Close()
		if err != nil {
			cancel()
		}
		if werr := cmd.Wait(); err == nil && werr != nil {
			err = s.commandError(ctx, args, stderr.String(), werr)
		}
	}()
	blobs := &blobReader{in: bufio.NewWriter(stdin), out: bufio.NewReader(stdout)}
	tw := tar.NewWriter(w)
	made := map[string]bool{}
	for _, e := range entries {
		if err := writeParents(tw, made, path.Dir(e.path)); err != nil {
			return err
		}
		switch e.mode {
		case modeSubmodule:
			made[e.path] = true
			err = tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: e.path + "/", Mode: 0o755})
		case modeSymlink:
			var target []byte
			if target, err = blobs.read(e); err == nil {
				err = tw.WriteHeader(&tar.Header{Typeflag: tar.TypeSymlink, Name: e.path, Linkname: string(target), Mode: 0o777})
			}
		default:
			err = blobs.copy(tw, e)
		}
		if err != nil {
			return err
		}
	}
	return tw.Close()
}

// writeParents writes the header of dir, and of each directory it is in,
// that made doesn't hold, to tw, and adds them to it.
func writeParents(tw *tar.Writer, made map[string]bool, dir string) error {
	if dir == "." || made[dir] {
		return nil
	}
	if err := writeParents(tw, made, path.Dir(dir)); err != nil {
		return err
	}
	made[dir] = true
	return tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: dir + "/", Mode: 0o755})
}

// A blobReader reads blobs from a cat-file --batch: it writes each one's
// object to in, and reads it from out.
type blobReader struct {
	in  *bufio.Writer
	out *bufio.Reader
}

// ask asks for e's blob, and returns its size, once it has read the line
// that gives it, which must be the one ls-tree gave.
func (b *blobReader) ask(e treeEntry) (int64, error) {
	if _, err := b.in.WriteString(e.object + "\n"); err != nil {
		return 0, err
	}
	if err := b.in.Flush(); err != nil {
		return 0, err
	}
	line, err := b.out.ReadString('\n')
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != e.object || fields[1] != "blob" {
		return 0, fmt.Errorf("local: cat-file gave %q for the blob of %s", strings.TrimSpace(line), e.path)
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size != e.size {
		return 0, fmt.Errorf("local: cat-file gave %q for the blob of %s, of %d bytes", strings.TrimSpace(line), e.path, e.size)
	}
	return size, nil
}

// end reads the line feed that ends a blob.
func (b *blobReader) end() error {
	c, err := b.out.ReadByte()
	if err == nil && c != '\n' {
		err = errors.New("local: cat-file ended a blob without a line feed")
	}
	return err
}

// copy writes e, a file, to tw: its header, with its executable bit, and
// its blob.
func (b *blobReader) copy(tw *tar.Writer, e treeEntry) error {
	size, err := b.ask(e)
	if err != nil {
		return err
	}
	mode := int64(0o644)
	if e.mode&0o111 != 0 {
		mode = 0o755
	}
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: e.path, Size: size, Mode: mode}); err != nil {
		return err
	}
	if _, err := io.CopyN(tw, b.out, size); err != nil {
		return err
	}
	return b.end()
}

// read reads e's blob, a symbolic link's target, of at most maxLinkTarget
// bytes.
func (b *blobReader) read(e treeEntry) ([]byte, error) {
	size, err := b.ask(e)
	if err != nil {
		return nil, err
	}
	if size > maxLinkTarget {
		return nil, fmt.Errorf("local: the symbolic link %s has a target of %d bytes", e.path, size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(b.out, data); err != nil {
		return nil, err
	}
	return data, b.end()
}

// treeOf returns the copy of repo that holds commit, once it holds it, and
// the files, symbolic links and submodules of commit's tree that paths
// name, or all of them, for none, in the order ls-tree lists them, with
// the indexes of the paths that name none.
func (s *Sources) treeOf(ctx context.Context, repo, commit string, paths []string) (string, []treeEntry, []int, error) {
	if err := plan.CheckRepo(repo); err != nil {
		return "", nil, nil, err
	}
	if !commitPattern.MatchString(commit) {
		return "", nil, nil, fmt.Errorf("local: %q is no commit's name in full", commit)
	}
	for _, p := range paths {
		if err := plan.CheckCheckoutPath(p); err != nil {
			return "", nil, nil, err
		}
	}
	dir, unlock, err := s.openAt(ctx, s.checkoutCopy(repo))
	if err != nil {
		return "", nil, nil, err
	}
	if !s.has(ctx, dir, commit) {
		_, err = s.fetchFrom(ctx, dir, s.repoURL(repo), commit, repo, repo)
	}
	unlock()
	if err != nil {
		return "", nil, nil, err
	}
	out, err := s.run(ctx, dir, "", "ls-tree", "-r", "-l", "-z", "--full-tree", commit)
	if err != nil {
		return "", nil, nil, err
	}
	names := make([]string, len(paths))
	for i, p := range paths {
		names[i] = strings.TrimSuffix(p, "/")
	}
	hit := make([]bool, len(paths))
	var entries []treeEntry
	for rec := range strings.SplitSeq(string(out), "\x00") {
		if rec == "" {
			continue
		}
		e, err := parseTreeEntry(rec)
		if err != nil {
			return "", nil, nil, err
		}
		named := len(paths) == 0
		for i, name := range names {
			if e.path == name || strings.HasPrefix(e.path, name+"/") {
				hit[i], named = true, true
			}
		}
		if named {
			entries = append(entries, e)
		}
	}
	var missing []int
	for i, h := range hit {
		if !h {
			missing = append(missing, i)
		}
	}
	return dir, entries, missing, nil
}

// parseTreeEntry parses a record of ls-tree -r -l -z: its mode, its type,
// its object and its size, a blob's, or -, and, after a tab, its path.
func parseTreeEntry(rec string) (treeEntry, error) {
	invalid := fmt.Errorf("local: ls-tree listed %q", rec)
	meta, name, ok := strings.Cut(rec, "\t")
	fields := strings.Fields(meta)
	if !ok || len(fields) != 4 || name == "" {
		return treeEntry{}, invalid
	}
	mode, err := strconv.ParseUint(fields[0], 8, 32)
	if err != nil {
		return treeEntry{}, invalid
	}
	e := treeEntry{mode: uint32(mode), object: fields[2], path: name}
	switch {
	case fields[1] == "commit" && e.mode == modeSubmodule:
	case fields[1] == "blob" && e.mode != modeSubmodule:
		if e.size, err = strconv.ParseInt(fields[3], 10, 64); err != nil || e.size < 0 {
			return treeEntry{}, invalid
		}
	default:
		return treeEntry{}, invalid
	}
	return e, nil
}
