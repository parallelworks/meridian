package expr

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// errNotJSON is parseJSON's error for text that isn't JSON. What reads JSON
// reports its own problem, with the text.
var errNotJSON = errors.New("expr: not JSON")

// maxJSONDepth is how deeply lists and mappings nest in JSON that parseJSON
// reads, as encoding/json/jsontext allows.
const maxJSONDepth = 10000

// parseJSON reads JSON text as values, as encoding/json has always read it
// here: a list is a []any, a mapping a map[string]any, and a number a
// float64, and a number too large for one isn't JSON. A key given twice has
// its later value. In a string, each byte that isn't part of a character in
// UTF-8 is replaced with U+FFFD, and so is an escaped half of a surrogate
// pair without its other half.
//
// It reads the text in one pass, and makes each list and mapping once, at
// its size.
func parseJSON(s string) (any, error) {
	r := jsonReader{s: s}
	v, err := r.value()
	if err != nil {
		return nil, err
	}
	r.space()
	if r.pos < len(s) {
		return nil, errNotJSON
	}
	return v, nil
}

// A jsonReader reads JSON text.
type jsonReader struct {
	s     string
	pos   int
	depth int
	// items holds the items of the lists being read, and the values of
	// the mappings, innermost last, and keys the mappings' keys, until
	// each list or mapping is made.
	items []any
	keys  []string
	// buf holds a string with escapes while it is read.
	buf []byte
}

// space skips JSON's whitespace.
func (r *jsonReader) space() {
	for r.pos < len(r.s) {
		switch r.s[r.pos] {
		case ' ', '\t', '\n', '\r':
			r.pos++
		default:
			return
		}
	}
}

// next skips whitespace and returns the byte after it, or 0 at the end.
func (r *jsonReader) next() byte {
	r.space()
	if r.pos < len(r.s) {
		return r.s[r.pos]
	}
	return 0
}

// value reads a value.
func (r *jsonReader) value() (any, error) {
	switch c := r.next(); c {
	case '[':
		return r.list()
	case '{':
		return r.mapping()
	case '"':
		return r.string()
	case 't':
		return r.literal("true", true)
	case 'f':
		return r.literal("false", false)
	case 'n':
		return r.literal("null", nil)
	default:
		if c == '-' || isDigit(c) {
			return r.number()
		}
	}
	return nil, errNotJSON
}

// literal reads true, false or null, whose text is word, as v.
func (r *jsonReader) literal(word string, v any) (any, error) {
	if !strings.HasPrefix(r.s[r.pos:], word) {
		return nil, errNotJSON
	}
	r.pos += len(word)
	return v, nil
}

// push adds v to the items. They double as they grow, rather than grow by
// a quarter as append's do once they are long, so that a long list's items
// are copied a dozen times rather than forty, but by no more than the rest
// of the text can hold: an item and the comma after it take two bytes.
func (r *jsonReader) push(v any) {
	if len(r.items) == cap(r.items) {
		r.items = slices.Grow(r.items, min(len(r.items)+16, (len(r.s)-r.pos)/2+1))
	}
	r.items = append(r.items, v)
}

// enter counts a level of lists and mappings, and fails past the most
// there can be.
func (r *jsonReader) enter() error {
	if r.depth++; r.depth > maxJSONDepth {
		return errNotJSON
	}
	return nil
}

// list reads a list, whose [ is next.
func (r *jsonReader) list() (any, error) {
	if err := r.enter(); err != nil {
		return nil, err
	}
	r.pos++
	from := len(r.items)
	if r.next() != ']' {
		for {
			v, err := r.value()
			if err != nil {
				return nil, err
			}
			r.push(v)
			if c := r.next(); c != ',' {
				if c != ']' {
					return nil, errNotJSON
				}
				break
			}
			r.pos++
		}
	}
	r.pos++
	r.depth--
	list := make([]any, len(r.items)-from)
	copy(list, r.items[from:])
	r.items = r.items[:from]
	return list, nil
}

// mapping reads a mapping, whose { is next. A key given twice has its later
// value.
func (r *jsonReader) mapping() (any, error) {
	if err := r.enter(); err != nil {
		return nil, err
	}
	r.pos++
	from, fromKeys := len(r.items), len(r.keys)
	if r.next() != '}' {
		for {
			if r.next() != '"' {
				return nil, errNotJSON
			}
			k, err := r.string()
			if err != nil {
				return nil, err
			}
			if r.next() != ':' {
				return nil, errNotJSON
			}
			r.pos++
			v, err := r.value()
			if err != nil {
				return nil, err
			}
			r.keys = append(r.keys, k)
			r.push(v)
			if c := r.next(); c != ',' {
				if c != '}' {
					return nil, errNotJSON
				}
				break
			}
			r.pos++
		}
	}
	r.pos++
	r.depth--
	m := make(map[string]any, len(r.keys)-fromKeys)
	for i, k := range r.keys[fromKeys:] {
		m[k] = r.items[from+i]
	}
	r.items, r.keys = r.items[:from], r.keys[:fromKeys]
	return m, nil
}

// string reads a string, whose " is next.
func (r *jsonReader) string() (string, error) {
	s := r.s
	start := r.pos + 1
	i := start
	for i < len(s) && s[i] >= ' ' && s[i] < utf8.RuneSelf && s[i] != '"' && s[i] != '\\' {
		i++
	}
	if i < len(s) && s[i] == '"' {
		r.pos = i + 1
		// A copy, so that what is read doesn't hold the whole text.
		return strings.Clone(s[start:i]), nil
	}
	b := r.buf[:0]
	b = append(b, s[start:i]...)
	for i < len(s) {
		switch c := s[i]; {
		case c == '"':
			r.pos, r.buf = i+1, b
			return string(b), nil
		case c < ' ':
			return "", errNotJSON
		case c == '\\':
			if i+1 >= len(s) {
				return "", errNotJSON
			}
			switch e := s[i+1]; e {
			case '"', '\\', '/':
				b = append(b, e)
			case 'b':
				b = append(b, '\b')
			case 'f':
				b = append(b, '\f')
			case 'n':
				b = append(b, '\n')
			case 'r':
				b = append(b, '\r')
			case 't':
				b = append(b, '\t')
			case 'u':
				c, ok := hex4(s, i+2)
				if !ok {
					return "", errNotJSON
				}
				i += 4
				if utf16.IsSurrogate(c) {
					// Half of a surrogate pair is U+FFFD without the other,
					// which is the escape that follows.
					low, ok := rune(0), false
					if i+8 <= len(s) && s[i+2] == '\\' && s[i+3] == 'u' {
						low, ok = hex4(s, i+4)
					}
					if c = utf16.DecodeRune(c, low); ok && c != utf8.RuneError {
						i += 6
					}
				}
				b = utf8.AppendRune(b, c)
			default:
				return "", errNotJSON
			}
			i += 2
		case c < utf8.RuneSelf:
			b = append(b, c)
			i++
		default:
			// Each byte that isn't part of a character in UTF-8 is U+FFFD.
			_, n := utf8.DecodeRuneInString(s[i:])
			if n == 1 {
				b = utf8.AppendRune(b, utf8.RuneError)
			} else {
				b = append(b, s[i:i+n]...)
			}
			i += n
		}
	}
	return "", errNotJSON
}

// hex4 reads the four hexadecimal digits at s[i:], of a \u escape.
func hex4(s string, i int) (rune, bool) {
	if i+4 > len(s) {
		return 0, false
	}
	var c rune
	for _, d := range []byte(s[i : i+4]) {
		switch {
		case '0' <= d && d <= '9':
			d -= '0'
		case 'a' <= d && d <= 'f':
			d -= 'a' - 10
		case 'A' <= d && d <= 'F':
			d -= 'A' - 10
		default:
			return 0, false
		}
		c = c<<4 | rune(d)
	}
	return c, true
}

// smallWholes are the whole numbers from 0 to 255 as values, so that a
// list of small numbers is read without making each one.
var smallWholes = func() (wholes [256]any) {
	for i := range wholes {
		wholes[i] = float64(i)
	}
	return wholes
}()

// number reads a number, whose first byte is next.
func (r *jsonReader) number() (any, error) {
	s, start := r.s, r.pos
	i := start
	if s[i] == '-' {
		i++
	}
	digits := i
	switch {
	case i < len(s) && s[i] == '0':
		i++
	case i < len(s) && isDigit(s[i]):
		for i < len(s) && isDigit(s[i]) {
			i++
		}
	default:
		return nil, errNotJSON
	}
	whole := i
	if i < len(s) && s[i] == '.' {
		if i++; i >= len(s) || !isDigit(s[i]) {
			return nil, errNotJSON
		}
		for i < len(s) && isDigit(s[i]) {
			i++
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		if i++; i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if i >= len(s) || !isDigit(s[i]) {
			return nil, errNotJSON
		}
		for i < len(s) && isDigit(s[i]) {
			i++
		}
	}
	r.pos = i
	// A whole number of up to 15 digits is a float64 exactly, and an
	// int64, however wide an int is.
	if i == whole && whole-digits <= 15 {
		var n int64
		for _, d := range []byte(s[digits:whole]) {
			n = n*10 + int64(d-'0')
		}
		switch {
		case digits > start:
			return math.Copysign(float64(n), -1), nil
		case n < int64(len(smallWholes)):
			return smallWholes[n], nil
		}
		return float64(n), nil
	}
	n, err := strconv.ParseFloat(s[start:i], 64)
	if err != nil {
		return nil, errNotJSON
	}
	return n, nil
}
