package worker

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// A treeProblem is what fails a checkout's try as the worker writes its
// tree: a member it doesn't write (checkout_unsafe), one it couldn't
// (checkout_write_failed), or files past the limit (checkout_too_large).
type treeProblem struct {
	kind   *problems.Kind
	params problems.Params
}

func (p *treeProblem) Error() string { return string(p.kind.Code()) }

// A treeReadError is an error reading a checkout's tree from its stream:
// one cut short, which the worker asks for again, or one that isn't a tar
// stream.
type treeReadError struct{ err error }

func (e *treeReadError) Error() string { return "worker: reading a checkout's tree: " + e.err.Error() }
func (e *treeReadError) Unwrap() error { return e.err }

// The reasons of a checkout_unsafe problem.
const (
	unsafeName = "name"
	unsafeType = "type"
	unsafeLink = "link"
)

// writeTree writes the checkout's tree that r streams, a tar stream
// (workerapi.PathCheckout), into root, the step's directory, merging it
// with what is there, as format 1 copies its clone: a directory is made
// if it isn't there; a file or a symbolic link replaces a file or a link
// of the same name, written under a name of its own and renamed over it,
// never through a link or into a file another name links to; and the
// rest is kept. A link where the tree has a directory, as one in a
// member's leading directories, is replaced by a directory, as git checks
// out over one, so that no write follows a link, one an earlier step left
// or one the tree holds, and, on a system whose names ignore case, none
// reaches .git through another case of a link's name. It writes only
// directories, files and symbolic links, each by a name that stays inside
// root, and none named .git, nor a link to one, whatever case: anything
// else is a treeProblem, checkout_unsafe, before anything of it is
// written. It writes limit bytes of files at most (checkout_too_large),
// and entries members (checkout_too_many). An error reading r is a
// treeReadError.
func writeTree(root *os.Root, r io.Reader, limit int64, entries int) error {
	tr := tar.NewReader(r)
	var (
		size    int64
		members int
	)
	for {
		h, err := tr.Next()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case errors.Is(err, tar.ErrInsecurePath) && h != nil:
			return unsafeMember(h.Name, unsafeName)
		case err != nil:
			return &treeReadError{err}
		}
		if h.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		if members++; members > entries {
			return &treeProblem{problems.CheckoutTooMany, problems.Params{"count": members, "limit": entries}}
		}
		name := strings.TrimSuffix(h.Name, "/")
		if !treeName(name) {
			return unsafeMember(h.Name, unsafeName)
		}
		switch h.Typeflag {
		case tar.TypeDir:
			err = makeDirs(root, name)
		case tar.TypeReg:
			if size += h.Size; size > limit || h.Size < 0 {
				return &treeProblem{problems.CheckoutTooLarge, problems.Params{"size": size, "limit": limit}}
			}
			err = writeFile(root, name, h, tr)
		case tar.TypeSymlink:
			if hasGit(h.Linkname) {
				return unsafeMember(name, unsafeLink)
			}
			err = writeLink(root, name, h.Linkname)
		default:
			return unsafeMember(name, unsafeType)
		}
		if err != nil {
			return err
		}
	}
}

// treeName reports whether name, a member's of a checkout's tree, without
// a directory's trailing slash, is one the worker writes: a path relative
// to the step's directory, as it is cleaned, that stays inside it, with no
// part .git, whatever its case, which would make a repository of it, and
// no backslash, which a Windows host would read as a separator.
func treeName(name string) bool {
	return name != "" && path.Clean(name) == name && filepath.IsLocal(name) && !strings.Contains(name, `\`) && !hasGit(name)
}

// hasGit reports whether p, a slash-separated path, has a part .git,
// whatever its case.
func hasGit(p string) bool {
	for part := range strings.SplitSeq(p, "/") {
		if strings.EqualFold(part, ".git") {
			return true
		}
	}
	return false
}

// makeDirs makes dir, a directory of a checkout's tree, and each one it is
// in, in root, as directories: one that is there is kept, and a link there
// is replaced by a directory, never followed, as git checks out over one.
// A file there fails it.
func makeDirs(root *os.Root, dir string) error {
	at := ""
	for part := range strings.SplitSeq(dir, "/") {
		at = path.Join(at, part)
		fi, err := root.Lstat(at)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return writeFailed(at, err)
		case fi.IsDir():
			continue
		case fi.Mode()&fs.ModeSymlink != 0:
			if err := root.Remove(at); err != nil {
				return writeFailed(at, err)
			}
		default:
			return writeFailed(at, &fs.PathError{Op: "mkdir", Path: at, Err: syscall.ENOTDIR})
		}
		if err := root.Mkdir(at, 0o777); err != nil {
			return writeFailed(at, err)
		}
	}
	return nil
}

// writeFile writes the file h of a checkout's tree, as tr reads it, at
// name in root, with its executable bit, as writeAside writes a member.
func writeFile(root *os.Root, name string, h *tar.Header, tr io.Reader) error {
	return writeAside(root, name, func(tmp string) error {
		perm := fs.FileMode(0o666)
		if h.Mode&0o111 != 0 {
			perm = 0o777
		}
		f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
		if err != nil {
			return writeFailed(name, err)
		}
		src := &trackedReader{r: tr}
		_, err = io.CopyN(f, src, h.Size)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		switch {
		case src.err != nil:
			return &treeReadError{src.err}
		case err != nil:
			return writeFailed(name, err)
		}
		return nil
	})
}

// writeLink writes the symbolic link of a checkout's tree, to target, at
// name in root, as writeAside writes a member.
func writeLink(root *os.Root, name, target string) error {
	return writeAside(root, name, func(tmp string) error {
		if err := root.Symlink(target, tmp); err != nil {
			return writeFailed(name, err)
		}
		return nil
	})
}

// writeAside has write write a member of a checkout's tree, name, at a
// name of its own in the same directory, which it makes first (makeDirs),
// and renames it over name: what was there, a file or a link, is
// replaced, never written through, and a file another name links to keeps
// what it held. A directory there stays, as format 1's copy keeps it: the
// member fails. What write wrote is removed if it fails.
func writeAside(root *os.Root, name string, write func(tmp string) error) error {
	dir := path.Dir(name)
	if dir != "." {
		if err := makeDirs(root, dir); err != nil {
			return err
		}
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	tmp := path.Join(dir, ".meridian-checkout-"+hex.EncodeToString(nonce[:]))
	if err := write(tmp); err != nil {
		_ = root.Remove(tmp)
		return err
	}
	if fi, err := root.Lstat(name); err == nil && fi.IsDir() {
		_ = root.Remove(tmp)
		return writeFailed(name, &fs.PathError{Op: "rename", Path: name, Err: syscall.EISDIR})
	}
	if err := root.Rename(tmp, name); err != nil {
		_ = root.Remove(tmp)
		return writeFailed(name, err)
	}
	return nil
}

// A trackedReader reads r, and keeps the error a read of it returned, so
// that a copy from it tells an error reading from one writing.
type trackedReader struct {
	r   io.Reader
	err error
}

func (t *trackedReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		t.err = err
	}
	return n, err
}

// unsafeMember is the problem of a member of a checkout's tree, name, that
// the worker doesn't write, for reason.
func unsafeMember(name, reason string) error {
	return &treeProblem{problems.CheckoutUnsafe, problems.Params{"path": problems.EscapeControl(name), "reason": reason}}
}

// writeFailed is the problem of a member of a checkout's tree, name, that
// the system refused to write, for err.
func writeFailed(name string, err error) error {
	reason := err.Error()
	if pe, ok := errors.AsType[*fs.PathError](err); ok {
		reason = pe.Err.Error()
	}
	return &treeProblem{problems.CheckoutWriteFailed, problems.Params{"path": problems.EscapeControl(name), "reason": problems.EscapeControl(reason)}}
}

// checkoutDir returns the directory a checkout step, readied as rs, writes
// its tree into: its with: path:, in its working directory, or where it is,
// for one that is absolute, as format 1 copies its clone there; or its
// working directory, for none.
func (a *attempt) checkoutDir(rs *plan.ReadyStep, with map[string]any) string {
	dir := a.workingDirectory(rs)
	switch p, _ := with["path"].(string); {
	case p == "":
		return dir
	case filepath.IsAbs(p):
		return p
	default:
		return filepath.Join(dir, p)
	}
}

// checksOut reports whether a checkout step's with:, as Step evaluated it,
// checks anything out: not when its repo or its branch is "", as format 1
// checks out nothing then, and the step succeeds.
func checksOut(with map[string]any) bool {
	repo, _ := with["repo"].(string)
	branch, _ := with["branch"].(string)
	return repo != "" && branch != ""
}

// checkout writes the tree of the parallelworks/checkout step's try key,
// readied as rs, whose action the coordinator answered success: it asks
// the coordinator for it (workerapi.PathCheckout), with req, the action's
// own request, unchanged, which the coordinator serves the tree for only,
// as it asks for any request, and writes it into the step's
// directory (checkoutDir), which it makes if it isn't there, as it makes
// a step's working directory, only its user's, through an
// os.Root of it (writeTree), again whole each time it asks again. A stop
// that stopping asks for ends it at once, as SIGTERM ends format 1's git,
// and it returns the stop's cause. It returns the problems that fail the
// try, and stopHalted once a coordinator that answers that the attempt no
// longer holds its job has halted it. A step whose repo or branch is ""
// checks out nothing (checksOut).
func (a *attempt) checkout(ctx context.Context, key logKey, rs *plan.ReadyStep, req *workerapi.ActionRequest, stopping <-chan stopRequest) (stopCause, problems.Diagnostics) {
	with, _ := rs.With.Get().(map[string]any)
	if !checksOut(with) {
		return noStop, nil
	}
	at := a.job.Steps[key.step].Path
	dir := a.checkoutDir(rs, with)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return noStop, problems.Diagnostics{a.at(problems.CheckoutWriteFailed, at, problems.Params{"path": problems.EscapeControl(dir), "reason": problems.EscapeControl(err.Error())})}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return noStop, problems.Diagnostics{a.at(problems.CheckoutWriteFailed, at, problems.Params{"path": problems.EscapeControl(dir), "reason": problems.EscapeControl(err.Error())})}
	}
	defer root.Close()
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- a.client.tree(tctx, req, func(r io.Reader) error {
			return writeTree(root, r, workerapi.MaxCheckoutBytes, workerapi.MaxCheckoutEntries)
		})
	}()
	select {
	case stop := <-stopping:
		cancel()
		<-done
		return stop.cause, nil
	case err = <-done:
	}
	var tp *treeProblem
	switch {
	case err == nil:
		return noStop, nil
	case errors.Is(err, errFenced) || errors.Is(err, errUnauthorized):
		a.halt(err)
		return stopHalted, nil
	case errors.As(err, &tp):
		return noStop, problems.Diagnostics{a.at(tp.kind, at, tp.params)}
	}
	a.log.Error("a checkout's tree couldn't be read", "step", key.step, "try", key.try, "error", a.mask.Mask(err.Error()))
	return noStop, problems.Diagnostics{a.at(problems.ActionRequestFailed, at, problems.Params{"uses": rs.Uses, "reason": requestFailure(err)})}
}
