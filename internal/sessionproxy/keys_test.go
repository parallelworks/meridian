package sessionproxy

import (
	"bytes"
	"regexp"
	"testing"
)

// TestKeyring checks what a session's credentials are made of: a secret
// that the same store, session and salt always give, and that another of
// any of them changes, however their bytes are split between the fields;
// a code of 128 random bits, whose keyed hash isn't a secret's of the same
// bytes; and keys and salts of their lengths alone.
func TestKeyring(t *testing.T) {
	k, err := NewKeyring(bytes.Repeat([]byte{7}, KeyLen))
	if err != nil {
		t.Fatal(err)
	}
	salt := bytes.Repeat([]byte{1}, SaltLen)
	secret := func(store, session string, salt []byte) []byte {
		t.Helper()
		s, err := k.Secret(store, session, salt)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	base := secret("store", "s1", salt)
	if !bytes.Equal(base, secret("store", "s1", salt)) || len(base) != 32 {
		t.Errorf("the secret isn't the same each time, or isn't 32 bytes: %x", base)
	}
	for name, other := range map[string][]byte{
		"another store":   secret("store2", "s1", salt),
		"another session": secret("store", "s2", salt),
		"another salt":    secret("store", "s1", bytes.Repeat([]byte{2}, SaltLen)),
		// Each field is led by its length, so moving bytes between them
		// changes the secret.
		"bytes moved between the fields": secret("stores", "1", salt),
	} {
		if bytes.Equal(base, other) {
			t.Errorf("%s gives the same secret", name)
		}
	}
	if bytes.Equal(k.CodeMAC("store"), secret("store", "", salt)) {
		t.Error("a code's hash is a secret's")
	}
	other, _ := NewKeyring(bytes.Repeat([]byte{8}, KeyLen))
	if s, _ := other.Secret("store", "s1", salt); bytes.Equal(s, base) {
		t.Error("another key gives the same secret")
	}

	seen := map[string]bool{}
	for range 100 {
		code, mac := k.NewCode()
		if !regexp.MustCompile(`^[a-z2-7]{26}$`).MatchString(code) || seen[code] || !bytes.Equal(mac, k.CodeMAC(code)) {
			t.Fatalf("NewCode gave %q, %x", code, mac)
		}
		seen[code] = true
	}

	if _, err := NewKeyring(make([]byte, KeyLen-1)); err == nil {
		t.Error("a short key was taken")
	}
	if _, err := k.Secret("store", "s1", make([]byte, SaltLen-1)); err == nil {
		t.Error("a short salt was taken")
	}
}
