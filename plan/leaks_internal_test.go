package plan

import (
	"encoding/json/jsontext"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf16"
	"unicode/utf8"
)

// What outputChecks find of each output, as the phases of an attempt
// share them, is what a Masker made for each phase alone finds, however
// the values a phase is given grow or change, and however its outputs do:
// an output that holds a value whose mask leaves it as it was, such as
// ***, doesn't hold a secret.
func TestOutputChecksAgainstFresh(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 11))
	alpha := []string{"a", "b", "*", "\n", "\"", "<", "é", "tok", "x9", " "}
	word := func(max int) string {
		var b strings.Builder
		for range r.IntN(max + 1) {
			b.WriteString(alpha[r.IntN(len(alpha))])
		}
		return b.String()
	}
	// rare is a value no output holds by chance, as most a step makes from
	// a secret are.
	rare := func() string { return fmt.Sprintf("rare-%08x", r.Uint32()) }
	for round := range 300 {
		secrets := &secretTexts{other: []string{word(4), word(6)}}
		var (
			c       outputChecks
			values  = []string{word(5)}
			outputs []string
		)
		for call := range 12 {
			switch k := r.IntN(10); {
			case k < 5:
				values = append(values, word(6))
			case k < 7:
				values = append(values, rare())
			case k < 8:
				// Another phase's values, which hold fewer.
				values = slices.Clone(values[:r.IntN(len(values)+1)])
			case k < 9 && len(values) > 0:
				// The same values, in another order.
				values = slices.Clone(values)
				r.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
			}
			switch k := r.IntN(6); {
			case k < 3:
				outputs = append(outputs, word(30))
			case k < 4 && len(values) > 0:
				outputs = append(outputs, word(5)+values[r.IntN(len(values))]+word(5))
			case k < 5 && len(outputs) > 0:
				// An output that isn't what it was.
				outputs[r.IntN(len(outputs))] = word(30)
			}
			c.mu.Lock()
			c.use(secrets, values)
			fresh := secrets.leakMasker().extend(false)
			fresh.Add(secrets.allMasked()...)
			fresh.Add(values...)
			for i, v := range outputs {
				if got, want := c.leaked(outputKey{step: i, name: "out"}, v), fresh.Mask(v) != v; got != want {
					t.Fatalf("round %d, call %d: values %q, output %d %q: leaked %v, want %v", round, call, values, i, v, got, want)
				}
				written := map[string]string{"out": v}
				if got, want := c.stepContext(i, written), stepsContext(map[string]map[string]string{"s": written})["s"]; !reflect.DeepEqual(got, want) {
					t.Fatalf("round %d, call %d: step %d's context %v, want %v", round, call, i, got, want)
				}
			}
			all := c.finder()
			for _, v := range outputs {
				if got, want := all.mask(v), fresh.Mask(v); got != want {
					t.Fatalf("round %d, call %d: values %q mask %q as %q, want %q", round, call, values, v, got, want)
				}
			}
			c.mu.Unlock()
		}
	}
}

// jsonFormsEach is jsonForms as it was before it had a fast path for what
// JSON writes as it is, and before its Replacers were shared.
func jsonFormsEach(v string) []string {
	quoted, err := jsontext.AppendQuote(nil, v)
	if err != nil {
		return nil
	}
	base := string(quoted[1 : len(quoted)-1])
	js := strings.NewReplacer("\u2028", `\u2028`, "\u2029", `\u2029`).Replace(base)
	html := strings.NewReplacer("<", `\u003c`, ">", `\u003e`, "&", `\u0026`).Replace(js)
	var ascii strings.Builder
	for _, r := range base {
		switch {
		case r < utf8.RuneSelf:
			ascii.WriteRune(r)
		case r > 0xffff:
			r1, r2 := utf16.EncodeRune(r)
			fmt.Fprintf(&ascii, `\u%04x\u%04x`, r1, r2)
		default:
			fmt.Fprintf(&ascii, `\u%04x`, r)
		}
	}
	var out []string
	for _, form := range []string{base, js, html, ascii.String()} {
		if form != v && !slices.Contains(out, form) {
			out = append(out, form)
		}
	}
	return out
}

// jsonForms gives what it gave before it had a fast path, for each byte,
// and for text of every kind it escapes.
func TestJSONFormsFastPath(t *testing.T) {
	check := func(v string) {
		t.Helper()
		if got, want := jsonForms(v), jsonFormsEach(v); !slices.Equal(got, want) {
			t.Fatalf("jsonForms(%q) = %q, want %q", v, got, want)
		}
	}
	for b := range 256 {
		check(string([]byte{byte(b)}))
		check("tok_" + string([]byte{byte(b)}) + "_end")
	}
	r := rand.New(rand.NewPCG(13, 17))
	alpha := []string{"a", "Z", "0", " ", "~", "\x7f", "\"", "\\", "<", ">", "&", "\n", "\t", "\x00", "é", "\u2028", "\u2029", "\U0001F600", "\xff"}
	for range 20000 {
		var b strings.Builder
		for range r.IntN(12) {
			b.WriteString(alpha[r.IntN(len(alpha))])
		}
		check(b.String())
	}
}
