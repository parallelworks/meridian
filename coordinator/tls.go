package coordinator

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"time"
)

// A tlsIdentity is what the coordinator serves TLS with: a certificate of
// its key, and the hash a worker knows that key by (backend.Endpoint).
type tlsIdentity struct {
	cert    *tls.Certificate
	keyHash string
}

// notAfter is the end of the coordinator's certificate: none, as RFC 5280
// says to write it, since a worker checks the key's hash, not the
// certificate, and the key outlives any process.
var notAfter = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)

// newTLSIdentity returns the identity of key, an ECDSA key on P-256: a
// certificate that key signs itself, made now, and the hash of key's
// public key, sha256: and the hex of the SHA-256 digest of its DER
// SubjectPublicKeyInfo. Each process makes its own certificate: replicas
// that share key share its hash, which is all a worker checks.
func newTLSIdentity(key crypto.Signer) (*tlsIdentity, error) {
	if pub, ok := key.Public().(*ecdsa.PublicKey); !ok || pub.Curve != elliptic.P256() {
		return nil, errors.New("coordinator: the TLS key isn't an ECDSA key on P-256")
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		Subject: pkix.Name{CommonName: "meridian coordinator"},
		// An hour back, for a worker's clock that is behind, though none
		// checks it now.
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	return &tlsIdentity{
		cert:    &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf},
		keyHash: "sha256:" + hex.EncodeToString(sum[:]),
	}, nil
}
