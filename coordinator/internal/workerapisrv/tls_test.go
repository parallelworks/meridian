package workerapisrv_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/coordinator/internal/workerapisrv"
	"github.com/parallelworks/meridian/internal/fipsmode"
	"github.com/parallelworks/meridian/workerapi"
	"github.com/parallelworks/meridian/workerapi/workerapitest"
)

// newCert returns a certificate of a new ECDSA key on P-256, which signs
// it itself, as the coordinator's is, and the hash of its public key.
func newCert(t testing.TB) (*tls.Certificate, [sha256.Size]byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		Subject:   pkix.Name{CommonName: "test coordinator"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
}

// serveMemberTLS serves srv over TLS with cert, as the listener of the
// member whose key is key, on a port of the loopback interface whose
// connections take no deadlines, as a relay's that reach it through an SSH
// channel don't, until the test ends, and returns its address.
func serveMemberTLS(t *testing.T, srv *workerapisrv.Server, key string, cert *tls.Certificate) string {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- srv.ServeMember(ctx, memberSocket{l}, key, cert) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ServeMember: %v", err)
		}
	})
	return l.Addr().String()
}

// dialTLS makes a TLS connection to address with config, and returns it,
// or the error of its handshake.
func dialTLS(t *testing.T, address string, config *tls.Config) (*tls.Conn, error) {
	t.Helper()
	d := tls.Dialer{Config: config}
	c, err := d.DialContext(t.Context(), "tcp", address)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { c.Close() })
	return c.(*tls.Conn), nil
}

// A member's listener served with a certificate speaks TLS 1.3 and nothing
// else, in FIPS 140-3 mode, enforced, with the algorithms it approves: an
// ECDSA P-256 key, AES-GCM, and ML-KEM with ECDH on P-256, which a client
// that offers X25519's hybrid first gets too, never X25519. It serves
// HTTP/1.1 alone, whatever a client asks for, asks for no client
// certificate, and closes a connection that sends no handshake in the time
// a request's headers have, or that speaks HTTP in the clear.
func TestServeMemberTLS(t *testing.T) {
	parallel(t)
	if mode := fipsmode.Mode(); mode != "only" {
		t.Fatalf("the test runs in FIPS 140-3 mode %s, not only, as go.mod sets it", mode)
	}
	r := newRig(t, rigOptions{configure: func(s *workerapisrv.Server) { workerapisrv.SetTimes(s, 500*time.Millisecond, time.Second) }})
	cert, hash := newCert(t)
	address := serveMemberTLS(t, r.server, "batch-aaaa", cert)

	for _, protos := range [][]string{nil, {"http/1.1"}, {"h2", "http/1.1"}} {
		config := workerapitest.PinnedTLS(hash)
		config.NextProtos = protos
		c, err := dialTLS(t, address, config)
		if err != nil {
			t.Fatalf("a pinned client offering %v: %v", protos, err)
		}
		cs := c.ConnectionState()
		switch {
		case cs.Version != tls.VersionTLS13:
			t.Errorf("the connection is %s", tls.VersionName(cs.Version))
		case cs.CipherSuite != tls.TLS_AES_128_GCM_SHA256 && cs.CipherSuite != tls.TLS_AES_256_GCM_SHA384:
			t.Errorf("the cipher suite is %s", tls.CipherSuiteName(cs.CipherSuite))
		case cs.CurveID != tls.SecP256r1MLKEM768:
			t.Errorf("the key exchange is %s", cs.CurveID)
		case cs.NegotiatedProtocol != "" && cs.NegotiatedProtocol != "http/1.1":
			t.Errorf("offered %v, the server chose %q", protos, cs.NegotiatedProtocol)
		}
		if pub, ok := cs.PeerCertificates[0].PublicKey.(*ecdsa.PublicKey); !ok || pub.Curve != elliptic.P256() {
			t.Errorf("the server's key is %T", cs.PeerCertificates[0].PublicKey)
		}
		// A request on it is answered over HTTP/1.1.
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://coordinator"+workerapi.PathHeartbeat, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := req.Write(c); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(c), req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.Proto != "HTTP/1.1" || resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("a heartbeat without a credential was answered %s %d", resp.Proto, resp.StatusCode)
		}
	}

	refused := map[string]*tls.Config{"TLS 1.2": workerapitest.PinnedTLS(hash), "HTTP/2 alone": workerapitest.PinnedTLS(hash), "X25519's hybrid alone": workerapitest.PinnedTLS(hash)}
	refused["TLS 1.2"].MinVersion, refused["TLS 1.2"].MaxVersion = tls.VersionTLS12, tls.VersionTLS12
	refused["HTTP/2 alone"].NextProtos = []string{"h2"}
	refused["X25519's hybrid alone"].CurvePreferences = []tls.CurveID{tls.X25519MLKEM768}
	for name, config := range refused {
		if _, err := dialTLS(t, address, config); err == nil {
			t.Errorf("a client of %s was served", name)
		}
	}

	// In the clear: refused, 400, as the HTTP server refuses any request to
	// a TLS port, without the API's answer.
	plain, err := new(net.Dialer).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := io.WriteString(plain, "POST "+workerapi.PathHeartbeat+" HTTP/1.1\r\nHost: coordinator\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := plain.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	answer, err := io.ReadAll(plain)
	if status, _, _ := strings.Cut(string(answer), "\r\n"); err != nil || answer != nil && !strings.HasSuffix(status, " 400 Bad Request") {
		t.Errorf("a request in the clear was answered %q, %v", answer, err)
	}

	// No handshake: closed within the time a request's headers have.
	silent, err := new(net.Dialer).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	if err := silent.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := io.ReadAll(silent); err != nil {
		t.Errorf("a connection that sent nothing wasn't closed: %v", err)
	} else if took := time.Since(start); took > 5*time.Second {
		t.Errorf("a connection that sent nothing was closed after %v", took)
	}
}

// A member's listener served without a certificate, as a socket the ssh
// backend forwards, takes no TLS: a client's handshake gets no answer it
// can read.
func TestServeMemberWithoutTLS(t *testing.T) {
	parallel(t)
	r := newRig(t, rigOptions{})
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	done := make(chan error, 1)
	go func() { done <- r.server.ServeMember(ctx, l, "ssh-aaaa", nil) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ServeMember: %v", err)
		}
	})
	_, hash := newCert(t)
	if _, err := dialTLS(t, l.Addr().String(), workerapitest.PinnedTLS(hash)); err == nil {
		t.Error("a TLS client of a listener without a certificate made its handshake")
	}
}

// TestServerLogHandshake checks that a failed TLS handshake, as a port
// scan of a member's listener makes, is logged at Debug, and anything else
// the HTTP server says of a connection at Warn.
func TestServerLogHandshake(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := workerapisrv.ServerLog(t.Context(), log)
	_, _ = io.WriteString(w, "http: TLS handshake error from 10.0.0.9:4000: EOF\n")
	_, _ = io.WriteString(w, "http: Accept error: too many open files\n")
	out := buf.String()
	if !strings.Contains(out, `level=DEBUG msg="http: TLS handshake error from 10.0.0.9:4000: EOF"`) ||
		!strings.Contains(out, `level=WARN msg="http: Accept error: too many open files"`) {
		t.Errorf("the server's words were logged as\n%s", out)
	}
}
