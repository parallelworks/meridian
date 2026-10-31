package sessionproxy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"strings"
)

// KeyLabel is the label under which the key of a Keyring is derived from
// the state directory's key, as the ssh backend's key of its members'
// names is: a version of its own, so that a change to what the key makes
// derives another.
const KeyLabel = "meridian session tokens v1"

// KeyLen is how many bytes a Keyring's key has, and SaltLen a session's
// salt.
const (
	KeyLen  = 32
	SaltLen = 16
)

// CodeBits is how many random bits a one-time code has.
const CodeBits = 128

// A Keyring makes what a session's credentials are made of from one key,
// derived under KeyLabel: a session's secret, which its cookie holds, and
// the keyed hash of a one-time code, which is all that is kept of one. Its
// keyed hashes are HMAC-SHA256, which FIPS 140-3 approves, of inputs
// whose fields are each of a fixed length, or led by their length, and
// whose first says what the hash is of, so that no two inputs of different
// fields hash alike.
type Keyring struct{ key []byte }

// NewKeyring returns a Keyring of key, which has KeyLen bytes.
func NewKeyring(key []byte) (*Keyring, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("sessionproxy: a key of %d bytes, not %d", len(key), KeyLen)
	}
	return &Keyring{key: append([]byte(nil), key...)}, nil
}

// Secret returns the secret of the session session of the store store,
// with its salt, salt, of SaltLen bytes, which a browser holds in the
// session's cookie: the same for each, so that any process of the store's
// user makes it again, as a coordinator that serves the session after
// another does, and another once the salt is replaced, which the
// session's reset does.
func (k *Keyring) Secret(store, session string, salt []byte) ([]byte, error) {
	if len(salt) != SaltLen {
		return nil, fmt.Errorf("sessionproxy: a salt of %d bytes, not %d", len(salt), SaltLen)
	}
	return k.mac("secret", []byte(store), []byte(session), salt), nil
}

// CodeMAC returns the keyed hash of the one-time code code, which is all
// that is kept of it.
func (k *Keyring) CodeMAC(code string) []byte { return k.mac("code", []byte(code)) }

// NewCode returns a new one-time code, CodeBits random bits in lowercase
// base32, which a link to a session holds, with its keyed hash (CodeMAC).
func (k *Keyring) NewCode() (code string, mac []byte) {
	b := make([]byte, CodeBits/8)
	_, _ = rand.Read(b) // crypto/rand doesn't fail
	code = strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
	return code, k.CodeMAC(code)
}

// mac returns the HMAC-SHA256 of what, then each field, each led by its
// length, as 8 bytes, big-endian.
func (k *Keyring) mac(what string, fields ...[]byte) []byte {
	h := hmac.New(sha256.New, k.key)
	for _, f := range append([][]byte{[]byte(what)}, fields...) {
		h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(f))))
		h.Write(f)
	}
	return h.Sum(nil)
}
