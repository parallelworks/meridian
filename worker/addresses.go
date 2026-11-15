package worker

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/meridian/internal/safepath"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
)

// An addressFile is the coordinator's address file
// (workerapi.AddressFile), which the worker reads again before each
// connection it makes, but at most once an every: it is on a filesystem
// the cluster's nodes share, which each request of a worker cut off from
// the coordinator would read otherwise.
type addressFile struct {
	path  string
	every time.Duration
	log   *slog.Logger

	mu   sync.Mutex
	read time.Time
	file *workerapi.AddressFile
	err  error
	// squatted are the addresses at which a server without the
	// coordinator's key answered, the last time each was tried.
	squatted map[string]bool
}

// load returns what the file holds, as it last read it, if that was
// within an every, and otherwise as it is. A file it can't read now it
// logs, once until another error, or none, follows; one it refuses ends
// the worker, which says why.
func (f *addressFile) load() (*workerapi.AddressFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.read.IsZero() && time.Since(f.read) < f.every {
		return f.file, f.err
	}
	last := f.err
	f.file, f.err = readAddressFile(f.path)
	f.read = time.Now()
	_, refused := errors.AsType[*refusedError](f.err)
	if f.err != nil && !refused && (last == nil || last.Error() != f.err.Error()) {
		f.log.Warn("the coordinator's address file can't be read now; trying again", "path", f.path, "error", f.err)
	}
	return f.file, f.err
}

// tried records whether a server without the coordinator's key answered
// at address, and logs it when one does there, once until the address is
// tried again with another answer.
func (f *addressFile) tried(address string, squatted bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if squatted && !f.squatted[address] {
		f.log.Warn("a server at an address in the coordinator's address file doesn't hold the coordinator's key; trying the next",
			"address", address, "path", f.path)
	}
	if f.squatted == nil {
		f.squatted = map[string]bool{}
	}
	if squatted {
		f.squatted[address] = true
	} else {
		delete(f.squatted, address)
	}
}

// readAddressFile reads the address file at path, as the worker reads its
// bootstrap token's (readTokenFile): a file of the user's that only the
// user can read, not through a symbolic link, on a path no other user can
// change. One that isn't, or isn't an address file, is a *refusedError:
// the worker never goes where it says, and doesn't try again. One it
// can't read now, as one that isn't there yet, nor seen there yet on this
// node, or a directory on the way to it that can't be opened, it tries
// again for.
func readAddressFile(path string) (*workerapi.AddressFile, error) {
	data, err := safepath.ReadFile(path, safepath.Private, workerapi.MaxAddressFile)
	switch {
	case err == nil:
	case problems.IsProblem(err) && problemOf(err, problems.DirectoryUnavailable) == nil:
		return nil, &refusedError{err}
	default:
		return nil, err
	}
	f, err := workerapi.UnmarshalAddressFile(data)
	if e, ok := errors.AsType[*workerapi.AddressFileError](err); ok {
		params := problems.Params{"path": path, "has_at": strconv.FormatBool(e.At != ""), "at": e.At}
		return nil, &refusedError{problems.CoordinatorFileInvalid.Problem(params)}
	}
	return f, err
}

// dialFile connects to the coordinator through the address file f: it
// tries the addresses the file names, in order, each for the timing's
// dialEach, TLS's handshake included, asking for the name the file gives,
// and returns the first connection to a server that holds the key whose
// hash is hash. The Transport dials with a context of its own, apart from
// the request's, which only its CloseIdleConnections ends: a request that
// gives up doesn't cut a dial short, which can go on to the last address,
// and leave the connection for the next.
//
// A server there that holds another key is passed by, and the next
// address tried, as the file read again is once none has reached the
// coordinator: TLS has checked the key before the worker sends anything,
// and such a server is most likely at a port another user took after a
// relay ended, before the file was written again.
func (c *client) dialFile(ctx context.Context, f *addressFile, hash []byte) (net.Conn, error) {
	file, err := f.load()
	if err != nil {
		return nil, err
	}
	missed := &missedError{path: f.path}
	for _, address := range file.Addresses {
		conn, err := dialTLS(ctx, address, file.Name, hash, c.timing.dialEach)
		squatted := err != nil && problemOf(err, problems.CoordinatorKeyMismatch) != nil
		f.tried(address, squatted)
		switch {
		case err == nil:
			return conn, nil
		case ctx.Err() != nil:
			return nil, ctx.Err()
		case squatted:
			missed.why = append(missed.why, address+": a server without the coordinator's key")
		default:
			missed.why = append(missed.why, address+": "+err.Error())
		}
	}
	return nil, missed
}

// dialTLS connects to the coordinator at address over TLS 1.3, asking for
// the server name name, and checks that it holds the key whose hash is
// hash, all within each.
func dialTLS(ctx context.Context, address, name string, hash []byte, each time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, each)
	defer cancel()
	cfg := tlsConfig(address, hash)
	cfg.ServerName = name
	d := tls.Dialer{Config: cfg}
	return d.DialContext(ctx, "tcp", address)
}

// A missedError is an address file none of whose addresses reached the
// coordinator, as while it is away, or a relay starts again elsewhere:
// the worker tries again, and reads the file again. It says why each was
// missed, as text, and wraps none of it, so that a server without the
// coordinator's key isn't the coordinator_key_mismatch that, at an address
// the worker was given, ends it.
type missedError struct {
	path string
	why  []string
}

func (e *missedError) Error() string {
	return "worker: no address in " + e.path + " reached the coordinator: " + strings.Join(e.why, "; ")
}
