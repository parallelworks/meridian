package plan

import (
	"bytes"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func naiveFind(s string, values []string) []span {
	var found []span
	for _, v := range values {
		if v == "" {
			continue
		}
		for i := 0; i+len(v) <= len(s); i++ {
			if s[i:i+len(v)] == v {
				found = append(found, span{i, i + len(v)})
			}
		}
	}
	slices.SortStableFunc(found, func(a, b span) int { return a.start - b.start })
	var out []span
	for _, f := range found {
		if n := len(out); n > 0 && f.start < out[n-1].end {
			out[n-1].end = max(out[n-1].end, f.end)
			continue
		}
		out = append(out, f)
	}
	return out
}

func TestMatcherAgainstNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 5))
	alpha := []byte{'a', 'b', '\n', '"', 0xff, 0}
	word := func(max int) string {
		b := make([]byte, r.IntN(max+1))
		for i := range b {
			b[i] = alpha[r.IntN(len(alpha))]
		}
		return string(b)
	}
	for range 100000 {
		var values []string
		for range 1 + r.IntN(6) {
			values = append(values, word(5))
		}
		s := word(40)
		var nonEmpty []string
		for _, v := range values {
			if v != "" {
				nonEmpty = append(nonEmpty, v)
			}
		}
		got, _ := newMatcher(nonEmpty).find(s)
		want := naiveFind(s, values)
		if !slices.Equal(got, want) {
			t.Fatalf("values %q in %q: %v, want %v", values, s, got, want)
		}
	}
}

// naiveMask masks s with every shown form of added, found naively.
func naiveMask(s string, added []string, lines bool) string {
	var all []string
	for _, v := range added {
		all = append(all, shownAs(v, lines)...)
	}
	return masked(s, naiveFind(s, all))
}

// A Masker that values are added to between masks, so that it has several
// levels, masks as a naive search for every shown form does, and so does
// its Writer, however its text is split.
func TestMaskerLevelsAgainstNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 23))
	alpha := []string{"a", "b", "c", "\n", " ", "\"", "<", "&", "é", " ", "x9"}
	word := func(max int) string {
		var b strings.Builder
		for range r.IntN(max + 1) {
			b.WriteString(alpha[r.IntN(len(alpha))])
		}
		return b.String()
	}
	for range 5000 {
		lines := r.IntN(2) == 0
		m := &Masker{lines: lines}
		var added []string
		for range 1 + r.IntN(8) {
			v := word(12)
			m.Add(v)
			added = append(added, v)
			s := word(40)
			if got, want := m.Mask(s), naiveMask(s, added, lines); got != want {
				t.Fatalf("lines %v, added %q, %q: %q, want %q (levels %d)", lines, added, s, got, want, len(m.levels))
			}
		}
		s := word(60)
		var out bytes.Buffer
		w := m.Writer(&out)
		for rest := s; rest != ""; {
			n := min(len(rest), 1+r.IntN(7))
			if _, err := w.Write([]byte(rest[:n])); err != nil {
				t.Fatal(err)
			}
			rest = rest[n:]
		}
		_ = w.Close()
		if want := naiveMask(s, added, lines); out.String() != want {
			t.Fatalf("writer: added %q, %q: %q, want %q", added, s, out.String(), want)
		}
	}
}

// No shown form of a value is shorter than the value, or than minLine, but
// the value without the line breaks it ends with.
func TestShownFormsLength(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	alpha := []string{"a", "1", "\n", " ", "\"", "<", "&", "é", " ", "}", "\t"}
	for range 50000 {
		var b strings.Builder
		for range r.IntN(20) {
			b.WriteString(alpha[r.IntN(len(alpha))])
		}
		v := b.String()
		for _, f := range shownAs(v, true) {
			if f == "" {
				continue
			}
			if len(f) < min(len(v), minLine) && !slices.Contains([]string{strings.TrimRight(v, "\r\n")}, f) {
				t.Fatalf("%q shows as %q", v, f)
			}
		}
	}
}

// A key a credential is kept under is one of the credential words, or has
// one among its words, whatever its case.
func TestCredentialKey(t *testing.T) {
	for key, want := range map[string]bool{
		"password": true, "PASSWORD": true, "db_password": true, "api_key": true, "apiKey": true, "APIKey": true,
		"accessToken": true, "aws-secret-access-key": true, "x.pin": true, "credentials": true, "pwd": true,
		"keyboard": false, "passport": false, "token_count_x": true, "tokenizer": false, "dbname": false, "": false,
		"user": false, "host": false, "PIN": true, "pinned": false,
	} {
		if got := credentialKey(key); got != want {
			t.Errorf("credentialKey(%q) = %v, want %v (words %q)", key, got, want, keyWords(key))
		}
	}
	fields := credentialFields([]string{
		`{"user":"svc","password":"hunt3r2","nested":{"apiKey":"k3y1","list":["a"]},"keys":["aaaa"],"token":["tok1","true"],"pin":"12"}`,
		`not json`, `["x"]`, `{"bad": `,
	})
	if want := map[string]bool{"hunt3r2": true, "k3y1": true, "tok1": true}; !maps.Equal(fields, want) {
		t.Errorf("credentialFields = %v, want %v", fields, want)
	}
}
