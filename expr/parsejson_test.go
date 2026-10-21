package expr

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/jsonvalue"
)

// lax is how parseJSON read JSON before it read it itself: with
// encoding/json/v2, allowing a key given twice and invalid UTF-8.
// parseJSON must agree with it on every text.
var lax = jsonv2.JoinOptions(
	jsontext.AllowDuplicateNames(true),
	jsontext.AllowInvalidUTF8(true),
	jsonv2.WithUnmarshalers(jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, v *any) error {
		out, err := jsonvalue.Decode(dec, parseFloat)
		*v = out
		return err
	})),
)

func parseLax(s string) (any, error) {
	var v any
	if err := jsonv2.Unmarshal([]byte(s), &v, lax); err != nil {
		return nil, err
	}
	return v, nil
}

// identical reports whether a and b are the same values: of the same
// types, with numbers of the same bits, so that -0 isn't 0, and a list or
// mapping without items isn't nil.
func identical(a, b any) bool {
	switch a := a.(type) {
	case float64:
		b, ok := b.(float64)
		return ok && math.Float64bits(a) == math.Float64bits(b)
	case []any:
		b, ok := b.([]any)
		if !ok || (a == nil) != (b == nil) || len(a) != len(b) {
			return false
		}
		for i := range a {
			if !identical(a[i], b[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		b, ok := b.(map[string]any)
		if !ok || (a == nil) != (b == nil) || len(a) != len(b) {
			return false
		}
		for k, v := range a {
			if w, ok := b[k]; !ok || !identical(v, w) {
				return false
			}
		}
		return true
	}
	return a == b
}

func readsAsLax(t *testing.T, s string) {
	t.Helper()
	want, wantErr := parseLax(s)
	got, err := parseJSON(s)
	if (err == nil) != (wantErr == nil) || !identical(got, want) {
		t.Fatalf("%q reads as %#v, %v; want %#v, %v", s, got, err, want, wantErr)
	}
}

// jsonSeeds are texts that JSON readers tell apart. \x5c is a backslash,
// which starts the escapes \x5cu0041 and the like in a JSON string.
var jsonSeeds = []string{
	``, ` `, `1`, ` 1 `, "\t[\n]\r", `1 2`, `[1,]`, `[,1]`, `[1 2]`, `{,}`, `{"a" 1}`, `{1:2}`, `{"a":1,}`, `{"a":}`,
	`[]`, `{}`, `[[]]`, `[{}]`, `{"a":[]}`, `[`, `]`, `{`, `"`, `"a`, `[1`, `{"a":1`, `{"a"`,
	`true`, `false`, `null`, `tru`, `truex`, `nul`, `nulll`, `[true,false,null]`, `True`, `NULL`,
	`0`, `-0`, `-`, `00`, `01`, `-01`, `1.`, `.5`, `+1`, `1e`, `1e+`, `1e-5`, `1E5`, `1e+5`, `-1.5e-3`, `0.0`, `-0.0`,
	`1e400`, `-1e400`, `1e-400`, `[1e-400]`, `123456789012345`, `1234567890123456`, `-999999999999999`,
	`123456789012345678901234567890`, `1.7976931348623157e308`, `1.7976931348623159e308`, `4.9e-324`, `2e-324`,
	`255`, `256`, `[0,1,255,256,-1]`, `NaN`, `Infinity`, `0x10`,
	`2147483647`, `2147483648`, `-2147483649`, `3000000000`, `4294967296`, `4294967551`, `999999999999999`,
	`""`, `"a"`, `"\""`, `"\\"`, `"\/"`, `"\b\f\n\r\t"`, `"\a"`, "\"\x5cU00e9\"", "\"\x5cu00e9\"", "\"\x5cu00E9\"",
	"\"\x5cu0000\"", "\"\x5cu001f\"", "\"\x5cu12\"", "\"\x5cu12G4\"", `"\`, "\"\x5cu\"", "\"\x00\"", "\"\x1f\"",
	"\"\x7f\"", "\"a\tb\"", "\"\x5cud800\"", "\"\x5cudc00\"", "\"\x5cud800\x5cudc00\"", "\"\x5cuD800\x5cuDC00\"",
	"\"\x5cudbff\x5cudfff\"", "\"\x5cud800\x5cu0041\"", "\"\x5cud800x\"", "\"\x5cud800\x5cud800\"",
	"\"\x5cudc00\x5cud800\"", "\"\x5cud800\x5cudc0\"", "\"\x5cud800\x5cu\"", "\"\x5cud800\x5c", "\"\x5cud800\x5cuzzzz\"",
	"\"\x5cud800\x5cn\"", "\"\x5cud800\x5cud800\x5cudc00\"",
	"\"\xff\"", "\"\xff\xfe\"", "\"\xe2\x80\"", "\"\xe2\x80x\"", "\"\xed\xa0\x80\"", "\"\xf4\x90\x80\x80\"",
	"\"\xc0\xaf\"", "\"\xef\xbf\xbd\"", "\"\xc3\xa9\xf0\x9f\x98\x80\"", "\"\xe2\x80", "\xff", "[\xff]",
	"\xef\xbb\xbf1", "1\xef\xbb\xbf",
	`{"a":1,"a":2}`, `{"a":{"b":1},"a":{"c":2}}`, `{"":1}`, "{\"\xff\":1,\"\xef\xbf\xbd\":2}",
	`{"a":1,"b":[1,{"c":null}]}`, "{\"\x5cu0061\":1,\"a\":2}", ` { "a" : [ 1 , 2 ] } `,
}

func TestParseJSON(t *testing.T) {
	for _, s := range jsonSeeds {
		readsAsLax(t, s)
	}
	for _, depth := range []int{maxJSONDepth, maxJSONDepth + 1} {
		readsAsLax(t, strings.Repeat("[", depth)+strings.Repeat("]", depth))
		readsAsLax(t, strings.Repeat(`{"a":`, depth)+"1"+strings.Repeat("}", depth))
		readsAsLax(t, strings.Repeat(`[{"a":`, depth/2)+"1"+strings.Repeat("}]", depth/2))
	}
	// Random texts of pieces of JSON, and of what isn't.
	pieces := []string{
		"[", "]", "{", "}", ",", ":", `"`, `"a"`, "\x5cu", "\x5cud83d", "\x5cude00", "\x5cu0041", `\`, `\n`, "u",
		"d8", "dc", "0", "1", "9", "-", ".", "e", "E", "+", "12", "true", "false", "null", " ", "\t", "\n", "\r",
		"\xff", "\xe2\x80", "\xc3\xa9", "\xf0\x9f\x98\x80", "\x00", "\x1f", "x",
	}
	r := rand.New(rand.NewPCG(3, 4))
	for range 100000 {
		var b strings.Builder
		for range r.IntN(16) {
			b.WriteString(pieces[r.IntN(len(pieces))])
		}
		readsAsLax(t, b.String())
	}
}

func FuzzParseJSON(f *testing.F) {
	for _, s := range jsonSeeds {
		f.Add(s)
	}
	f.Fuzz(readsAsLax)
}

// Reading JSON makes each list and mapping once, at its size, and a small
// whole number without making it.
func BenchmarkParseJSON(b *testing.B) {
	var objects strings.Builder
	objects.WriteString("[")
	for i := range 50000 {
		fmt.Fprintf(&objects, "{\"name\": \"host-%d\", \"gpus\": %d, \"up\": true, \"load\": 1.5e-3, \"tags\": [\"a\", \"b\x5cu00e9\"]}, ", i, i%8)
	}
	objects.WriteString("null]")
	for _, tc := range []struct{ name, text string }{
		{"numbers", "[" + strings.Repeat("1,", 2<<20) + "1]"},
		{"objects", objects.String()},
		{"strings", "[" + strings.Repeat(`"abcdefghijklmnop",`, 1<<17) + `""]`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.text)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parseJSON(tc.text); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
