// Package jsonptr reads and writes JSON Pointers (RFC 6901), which name a
// place in JSON values, such as /jobs/build/steps/0.
package jsonptr

import (
	"errors"
	"strings"
)

var (
	escaper = strings.NewReplacer("~", "~0", "/", "~1")
	// unescaper reads ~01 as ~1, as RFC 6901 requires: it replaces in one
	// pass, so ~0's ~ isn't read again.
	unescaper = strings.NewReplacer("~1", "/", "~0", "~")
)

// Format returns the JSON Pointer of a path of reference tokens: keys of
// mappings and indexes of lists.
func Format(tokens []string) string {
	return Append("", tokens...)
}

// Append returns the JSON Pointer of what tokens lead to from pointer.
func Append(pointer string, tokens ...string) string {
	// A token rarely has a ~ or a / to escape, so the pointer is most
	// likely this long.
	n := len(pointer) + len(tokens)
	for _, tok := range tokens {
		n += len(tok)
	}
	var b strings.Builder
	b.Grow(n)
	b.WriteString(pointer)
	for _, tok := range tokens {
		b.WriteByte('/')
		b.WriteString(escaper.Replace(tok))
	}
	return b.String()
}

// Split returns a JSON Pointer's reference tokens. It fails for a pointer
// that isn't "" and doesn't start with /, or has a ~ that isn't ~0 or ~1.
func Split(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	rest, ok := strings.CutPrefix(pointer, "/")
	if !ok {
		return nil, errors.New("jsonptr: a JSON Pointer starts with /")
	}
	tokens := strings.Split(rest, "/")
	for i, tok := range tokens {
		if strings.Count(tok, "~") != strings.Count(tok, "~0")+strings.Count(tok, "~1") {
			return nil, errors.New("jsonptr: a ~ in a JSON Pointer is ~0 or ~1")
		}
		tokens[i] = unescaper.Replace(tok)
	}
	return tokens, nil
}

// Lookup returns the value that tokens lead to in v, JSON values. A token
// reads a list's item only if it is an index as RFC 6901 writes one: 0, or
// digits that don't start with 0. ok is false if there is nothing there.
func Lookup(v any, tokens []string) (any, bool) {
	for _, tok := range tokens {
		switch c := v.(type) {
		case map[string]any:
			var ok bool
			if v, ok = c[tok]; !ok {
				return nil, false
			}
		case []any:
			i, ok := index(tok, len(c))
			if !ok {
				return nil, false
			}
			v = c[i]
		default:
			return nil, false
		}
	}
	return v, true
}

// index returns the index a token names in a list n items long.
func index(tok string, n int) (int, bool) {
	if tok == "" || len(tok) > 1 && tok[0] == '0' {
		return 0, false
	}
	i := 0
	for _, c := range []byte(tok) {
		if c < '0' || c > '9' {
			return 0, false
		}
		i = i*10 + int(c-'0')
		if i >= n {
			return 0, false
		}
	}
	return i, true
}
