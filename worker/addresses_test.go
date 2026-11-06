package worker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// relayName is the server name the tests' address files give.
const relayName = "r-test.meridian"

// A tlsServer serves a Fake over TLS 1.3, as the coordinator, or a server
// that holds another key does, and counts the requests it served, and
// the server names TLS's handshakes asked it for.
type tlsServer struct {
	*httptest.Server
	requests atomic.Int32

	mu    sync.Mutex
	names []string
}

// serveTLS serves f over TLS at 127.0.0.1, with httptest's key, which
// every such server shares, as a coordinator's replicas do, or with cert.
func serveTLS(t *testing.T, f *workerapitest.Fake, cert *tls.Certificate) *tlsServer {
	t.Helper()
	s := &tlsServer{}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		f.ServeHTTP(w, r)
	}))
	s.TLS = &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		s.mu.Lock()
		s.names = append(s.names, hello.ServerName)
		s.mu.Unlock()
		return nil, nil
	}}
	if cert != nil {
		s.TLS.Certificates = []tls.Certificate{*cert}
	}
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}

// handshakes returns the server names the server was asked for, one for
// each handshake it began.
func (s *tlsServer) handshakes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.names)
}

// otherKey returns a certificate of a key of its own, as a server at a
// port another user took has.
func otherKey(t *testing.T) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: relayName}, DNSNames: []string{relayName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// writeAddresses writes the address file at path, naming addresses, as a
// relay writes it: whole, renamed into place.
func writeAddresses(t *testing.T, path string, addresses ...string) {
	t.Helper()
	data, err := (&workerapi.AddressFile{Addresses: addresses, Name: relayName, Updated: time.Now().Unix()}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".new", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".new", path); err != nil {
		t.Fatal(err)
	}
}

// throughFile is the environment of a worker that reaches its coordinator
// through the address file at path, whose key is srv's.
func throughFile(t *testing.T, path string, srv *tlsServer) []string {
	t.Helper()
	return []string{workerapi.EnvCoordinatorFile + "=" + path, workerapi.EnvKeySHA256 + "=" + keyHash(t, srv.Server)}
}

// startWorker runs a worker against f, as runWorker does, until it
// returns, which it sends on the channel it returns.
func startWorker(t *testing.T, f *workerapitest.Fake, cfg Config) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runWorker(t, f, t.Context(), cfg) }()
	return done
}

// until waits up to 30 s for cond, and fails the test if it doesn't hold
// by then, or if the worker that done is of returns first.
func until(t *testing.T, what string, done <-chan error, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); !cond(); {
		select {
		case err := <-done:
			t.Fatalf("the worker returned %v before %s", err, what)
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited 30 s for %s", what)
		}
	}
}

// A logCount is a worker's log, as a test's log shows it, in which it
// counts lines.
type logCount struct {
	t  *testing.T
	mu sync.Mutex
	b  strings.Builder
}

func (l *logCount) Write(p []byte) (int, error) {
	l.mu.Lock()
	l.b.Write(p)
	l.mu.Unlock()
	return testWriter{l.t}.Write(p)
}

// logger returns a logger that writes to l.
func (l *logCount) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// lines returns how many lines of the log hold s.
func (l *logCount) lines(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for line := range strings.Lines(l.b.String()) {
		if strings.Contains(line, s) {
			n++
		}
	}
	return n
}

// The worker's warnings about its address file.
const (
	warnUnread   = "address file can't be read now"
	warnSquatter = "doesn't hold the coordinator's key"
)

// workerEnded waits up to 30 s for the worker that done is of to return, and
// returns what it returned.
func workerEnded(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("the worker hasn't returned in 30 s")
		return nil
	}
}

// TestAddressFileMoves checks that a worker that reaches its coordinator
// through an address file goes where the file says once it is written
// again, as after a relay started again elsewhere, mid-job, and asks each
// server for the name the file gives.
func TestAddressFileMoves(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command(`while [ ! -e "$PW_JOB_DIR/go" ]; do sleep 0.05; done; echo moved`)))
	first, second := serveTLS(t, f, nil), serveTLS(t, f, nil)
	path := filepath.Join(t.TempDir(), "coordinator.json")
	writeAddresses(t, path, first.Listener.Addr().String())
	done := startWorker(t, f, Config{Environ: throughFile(t, path, first)})
	until(t, "the step started", done, func() bool { return len(f.Events()) > 0 })
	// The first relay ends, with its connections, and the next listens
	// elsewhere.
	writeAddresses(t, path, second.Listener.Addr().String())
	first.Listener.Close()
	first.CloseClientConnections()
	if err := os.WriteFile(filepath.Join(f.Claim.RunDir, "go"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := workerEnded(t, done); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "moved\n")
	if first.requests.Load() == 0 || second.requests.Load() == 0 {
		t.Errorf("the first server served %d requests, and the second %d", first.requests.Load(), second.requests.Load())
	}
	for _, s := range []*tlsServer{first, second} {
		if names := s.handshakes(); len(names) == 0 || slices.ContainsFunc(names, func(n string) bool { return n != relayName }) {
			t.Errorf("the worker asked for the names %q", names)
		}
	}
}

// TestAddressFileSquatter checks that a server at an address of the file
// that doesn't hold the coordinator's key, as one at a port another user
// took once a relay ended, is passed by: for the next address, and
// until the file is written again, without ending the worker; and that it
// is sent nothing.
func TestAddressFileSquatter(t *testing.T) {
	t.Parallel()
	t.Run("first in the file", func(t *testing.T) {
		t.Parallel()
		f := workerapitest.NewFake(t, newJob(command("echo past it")))
		squatter, coordinator := serveTLS(t, f, otherKey(t)), serveTLS(t, f, nil)
		path := filepath.Join(t.TempDir(), "coordinator.json")
		writeAddresses(t, path, squatter.Listener.Addr().String(), coordinator.Listener.Addr().String())
		if err := runWorker(t, f, t.Context(), Config{Environ: throughFile(t, path, coordinator)}); err != nil {
			t.Fatal(err)
		}
		checkEnd(t, f, plan.OutcomeSuccess, "")
		checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "past it\n")
		if len(squatter.handshakes()) == 0 || squatter.requests.Load() != 0 {
			t.Errorf("the squatter had %d handshakes, and served %d requests", len(squatter.handshakes()), squatter.requests.Load())
		}
	})
	t.Run("until the file is written again", func(t *testing.T) {
		t.Parallel()
		f := workerapitest.NewFake(t, newJob(command("echo past it")))
		squatter, coordinator := serveTLS(t, f, otherKey(t)), serveTLS(t, f, nil)
		path := filepath.Join(t.TempDir(), "coordinator.json")
		writeAddresses(t, path, squatter.Listener.Addr().String())
		logs := &logCount{t: t}
		done := startWorker(t, f, Config{Environ: throughFile(t, path, coordinator), Log: logs.logger()})
		until(t, "the worker tried the squatter again", done, func() bool { return len(squatter.handshakes()) >= 3 })
		// Said once, however often the worker tries the address.
		if n := logs.lines(warnSquatter); n != 1 {
			t.Errorf("the worker warned of the squatter %d times", n)
		}
		writeAddresses(t, path, coordinator.Listener.Addr().String())
		if err := workerEnded(t, done); err != nil {
			t.Fatal(err)
		}
		checkEnd(t, f, plan.OutcomeSuccess, "")
		if squatter.requests.Load() != 0 {
			t.Errorf("the squatter served %d requests", squatter.requests.Load())
		}
	})
}

// TestAddressFileStale checks that a worker whose address file isn't
// there yet, and then names where nothing answers, as a relay that has
// gone does, waits, reading the file again, until it names the
// coordinator.
func TestAddressFileStale(t *testing.T) {
	t.Parallel()
	f := workerapitest.NewFake(t, newJob(command("echo late")))
	coordinator := serveTLS(t, f, nil)
	path := filepath.Join(t.TempDir(), "coordinator.json")
	done := startWorker(t, f, Config{Environ: throughFile(t, path, coordinator)})
	// A file that isn't there yet isn't one the worker gives up on.
	select {
	case err := <-done:
		t.Fatalf("without its address file, the worker returned %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	// A relay that has gone: its port takes connections, as one another
	// listener holds, and closes them.
	gone, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gone.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := gone.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			conn.Close()
		}
	}()
	data, err := (&workerapi.AddressFile{Addresses: []string{gone.Addr().String()}, Name: relayName, Updated: time.Now().Add(-time.Hour).Unix()}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	until(t, "the worker tried the address again", done, func() bool { return accepted.Load() >= 3 })
	writeAddresses(t, path, coordinator.Listener.Addr().String())
	if err := workerEnded(t, done); err != nil {
		t.Fatal(err)
	}
	checkEnd(t, f, plan.OutcomeSuccess, "")
	checkLog(t, f, logKey{workerapi.LogStep, 0, 0}, "late\n")
}

// TestAddressFileRefused checks that a worker refuses an address file that
// other users could have written, or could read, or that isn't one, and
// never tries the coordinator, nor the file, again.
func TestAddressFileRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// write puts the file at path, in dir, a directory of the test's.
		write func(t *testing.T, dir, path, address string) string
		kind  *problems.Kind
		at    string
	}{
		{"others can read it", func(t *testing.T, _, path, address string) string {
			writeAddresses(t, path, address)
			if err := os.Chmod(path, 0o644); err != nil { //nolint:gosec // a file others can read, which the worker refuses
				t.Fatal(err)
			}
			return path
		}, problems.FileNotPrivate, ""},
		{"a symbolic link", func(t *testing.T, dir, path, address string) string {
			writeAddresses(t, path, address)
			link := filepath.Join(dir, "link")
			if err := os.Symlink(path, link); err != nil {
				t.Fatal(err)
			}
			return link
		}, problems.FileNotRegular, ""},
		{"in a directory others can change", func(t *testing.T, dir, _, address string) string {
			open := filepath.Join(dir, "open")
			if err := os.Mkdir(open, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(open, "coordinator.json")
			writeAddresses(t, path, address)
			if err := os.Chmod(open, 0o777); err != nil { //nolint:gosec // a directory others can change, which the worker refuses
				t.Fatal(err)
			}
			return path
		}, problems.PathUnsafe, ""},
		{"not JSON", func(t *testing.T, _, path, _ string) string {
			if err := os.WriteFile(path, []byte("10.1.0.5:40211\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}, problems.CoordinatorFileInvalid, ""},
		{"no address", func(t *testing.T, _, path, _ string) string {
			if err := os.WriteFile(path, []byte(`{"addresses":[],"name":"r","updated":1}`), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}, problems.CoordinatorFileInvalid, "/addresses"},
		{"larger than one", func(t *testing.T, _, path, address string) string {
			data := `{"addresses":["` + address + `"],"name":"r","updated":1,"pad":"` + strings.Repeat("x", workerapi.MaxAddressFile) + `"}`
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}, problems.FileOverLimit, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := workerapitest.NewFake(t, newJob(command("true")))
			coordinator := serveTLS(t, f, nil)
			dir := t.TempDir()
			path := tc.write(t, dir, filepath.Join(dir, "coordinator.json"), coordinator.Listener.Addr().String())
			logs := &logCount{t: t}
			err := runWorker(t, f, t.Context(), Config{Environ: throughFile(t, path, coordinator), Log: logs.logger()})
			// Nor says that it does.
			if n := logs.lines(warnUnread); n != 0 {
				t.Errorf("the worker said %d times that it would try a file it refused again", n)
			}
			p := problemOf(err, tc.kind)
			if p == nil {
				t.Fatalf("got %v, want %s", err, tc.kind.Code())
			}
			if tc.kind == problems.CoordinatorFileInvalid && (p.Params["at"] != tc.at || p.Params["has_at"] != strconv.FormatBool(tc.at != "") || p.Params["path"] != path) {
				t.Errorf("the problem's params are %v", p.Params)
			}
			if _, ok := errors.AsType[*refusedError](err); !ok {
				t.Errorf("%v isn't a refusal", err)
			}
			if f.Claimed() || len(coordinator.handshakes()) != 0 {
				t.Errorf("the worker reached the coordinator: claimed %t, after %d handshakes", f.Claimed(), len(coordinator.handshakes()))
			}
		})
	}
}

// TestTLSGroups checks that the worker offers TLS's key exchanges that the
// coordinator takes, as the coordinator orders them, and no other, X25519
// none, so that its handshake takes no HelloRetryRequest's round trip:
// at an address it was given, as at one its address file names.
func TestTLSGroups(t *testing.T) {
	t.Parallel()
	cert := otherKey(t)
	var (
		mu      sync.Mutex
		offered [][]tls.CurveID
	)
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:       tls.VersionTLS13,
		Certificates:     []tls.Certificate{*cert},
		CurvePreferences: workerapi.TLSGroups,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			mu.Lock()
			offered = append(offered, hello.SupportedCurves)
			mu.Unlock()
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_ = conn.(*tls.Conn).HandshakeContext(t.Context())
			conn.Close()
		}
	}()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	address := l.Addr().String()
	direct := func() (net.Conn, error) {
		d := tls.Dialer{Config: tlsConfig(address, hash[:])}
		return d.DialContext(t.Context(), "tcp", address)
	}
	throughFile := func() (net.Conn, error) { return dialTLS(t.Context(), address, relayName, hash[:], 10*time.Second) }
	for name, dial := range map[string]func() (net.Conn, error){"at its address": direct, "through its address file": throughFile} {
		conn, err := dial()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if conn.(*tls.Conn).ConnectionState().HelloRetryRequest {
			t.Errorf("%s: the handshake took a HelloRetryRequest", name)
		}
		conn.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	for _, curves := range offered {
		if !slices.Equal(curves, workerapi.TLSGroups) || slices.Contains(curves, tls.X25519) || slices.Contains(curves, tls.X25519MLKEM768) {
			t.Errorf("the worker offered %v", curves)
		}
	}
	if len(offered) != 2 {
		t.Errorf("the server had %d ClientHellos", len(offered))
	}
}

// TestAddressFileReread checks that the worker reads its address file
// again at most once a period, however many connections it makes.
func TestAddressFileReread(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "coordinator.json")
	writeAddresses(t, path, "10.1.0.5:1")
	f := &addressFile{path: path, every: time.Hour, log: testLogger(t)}
	read := func() string {
		t.Helper()
		file, err := f.load()
		if err != nil {
			t.Fatal(err)
		}
		return file.Addresses[0]
	}
	if got := read(); got != "10.1.0.5:1" {
		t.Fatalf("read %s", got)
	}
	writeAddresses(t, path, "10.1.0.5:2")
	if got := read(); got != "10.1.0.5:1" {
		t.Errorf("within the period, read %s, as written since", got)
	}
	f.every = 0
	if got := read(); got != "10.1.0.5:2" {
		t.Errorf("after the period, read %s", got)
	}
}
