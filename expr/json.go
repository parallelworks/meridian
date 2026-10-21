package expr

import (
	"bytes"
	"cmp"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/problems"
)

// Expressions encode as their parsed tree, not their source, so that a
// stored expression keeps its meaning whatever later releases change in the
// parser. Each node is a JSON object with one key that names its kind:
//
//	{"lit": 1}                               a literal; NaN and the infinities are {"num": "NaN"}
//	{"word": "debug"}                        a bare word
//	{"ref": "inputs", "path": [...]}         a reference to a context
//	{"up": 1, "path": [...]}                 a relative reference
//	{"op": "!", "x": ...}                    a unary operator
//	{"op": "&&", "x": ..., "y": ...}         a binary operator, with "not": true for !in and the like
//	{"if": ..., "then": ..., "else": ...}    a ternary
//	{"member": ..., "path": [...]}           properties of a computed value
//	{"call": "contains", "args": [...]}      a function call
//	{"lambda": ["x", "i"], "body": ...}      the lambda that is the last argument of map() or filter()
//	{"param": "x", "path": [...]}            a lambda's parameter, read in its body
//	{"slice": [start, end]}                  the operand (start):(end); end is null when open
//	{"concat": "2:", "x": ...}               the operand 2:(end)
//
// A path is a list of segments: a string for a name, or {"quoted": "my-key"},
// {"bare": "hidden"}, {"index": true} for [index], and {"each": true} for .*.
//
// Positions aren't encoded: an expression decoded from JSON reports errors
// at offset 0.
//
// A tree is written in one pass, with each node's keys in sorted order and
// strings escaped as encoding/json escapes them by default, as trees have
// always been written. It is read in one pass too, and what a node may be
// that depends on the nodes around it, a lambda or a parameter, is checked
// once its parent is read.

// MarshalJSON encodes the expression as its source and its tree.
func (e *Expr) MarshalJSON() ([]byte, error) { return e.appendJSON(nil) }

func (e *Expr) appendJSON(b []byte) ([]byte, error) {
	b = appendString(append(b, `{"src":`...), e.src, true)
	b, err := appendNode(append(b, `,"tree":`...), e.root)
	if err != nil {
		return nil, err
	}
	return append(b, '}'), nil
}

// UnmarshalJSON decodes an expression that MarshalJSON encoded.
func (e *Expr) UnmarshalJSON(data []byte) error {
	return decodeAll(data, func(dec *jsontext.Decoder) error {
		out, err := decodeExpr(dec)
		if err == nil {
			*e = *out
		}
		return err
	})
}

// MarshalJSON encodes the template as its parts: text, and expressions.
func (t *Template) MarshalJSON() ([]byte, error) {
	b := []byte{'['}
	for i, p := range t.parts {
		if i > 0 {
			b = append(b, ',')
		}
		if p.expr == nil {
			b = appendString(b, p.text, true)
			continue
		}
		var err error
		if b, err = p.expr.appendJSON(b); err != nil {
			return nil, err
		}
	}
	return append(b, ']'), nil
}

// UnmarshalJSON decodes a template that MarshalJSON encoded.
func (t *Template) UnmarshalJSON(data []byte) error {
	return decodeAll(data, func(dec *jsontext.Decoder) error {
		out, err := decodeTemplate(dec)
		if err == nil {
			*t = *out
		}
		return err
	})
}

// decoders holds decoders to reuse, since a plan decodes each template on
// its own.
var decoders = sync.Pool{New: func() any { return new(jsontext.Decoder) }}

// decodeAll decodes data, which must hold one JSON value, with decode. A key
// given twice and invalid UTF-8 are errors, however the caller decodes.
func decodeAll(data []byte, decode func(*jsontext.Decoder) error) error {
	dec := decoders.Get().(*jsontext.Decoder)
	defer decoders.Put(dec)
	dec.Reset(bytes.NewBuffer(data))
	if err := decode(dec); err != nil {
		return err
	}
	switch _, err := dec.ReadToken(); {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return err
	}
	return errors.New("expr: more than one JSON value")
}

func appendNode(b []byte, n node) ([]byte, error) {
	var err error
	switch n := n.(type) {
	case *literal:
		if f, ok := n.value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			b = appendString(append(b, `{"num":`...), formatNumber(f), true)
		} else {
			b, err = appendValue(append(b, `{"lit":`...), n.value)
		}
	case *word:
		b = appendString(append(b, `{"word":`...), n.text, true)
	case *ref:
		b = appendSegments(append(b, `{"path":`...), n.path)
		b = appendString(append(b, `,"ref":`...), n.context, true)
	case *relative:
		b = appendSegments(append(b, `{"path":`...), n.path)
		b = strconv.AppendInt(append(b, `,"up":`...), int64(n.up), 10)
	case *unary:
		b = appendString(append(b, `{"op":`...), n.op, true)
		b, err = appendNode(append(b, `,"x":`...), n.x)
	case *binary:
		b = append(b, '{')
		if n.negated {
			b = append(b, `"not":true,`...)
		}
		b = appendString(append(b, `"op":`...), n.op, true)
		if b, err = appendNode(append(b, `,"x":`...), n.x); err == nil {
			b, err = appendNode(append(b, `,"y":`...), n.y)
		}
	case *ternary:
		if b, err = appendNode(append(b, `{"else":`...), n.els); err == nil {
			if b, err = appendNode(append(b, `,"if":`...), n.cond); err == nil {
				b, err = appendNode(append(b, `,"then":`...), n.then)
			}
		}
	case *member:
		if b, err = appendNode(append(b, `{"member":`...), n.x); err == nil {
			b = appendSegments(append(b, `,"path":`...), n.path)
		}
	case *call:
		b = append(b, `{"args":[`...)
		for i, a := range n.args {
			if i > 0 {
				b = append(b, ',')
			}
			if b, err = appendNode(b, a); err != nil {
				return nil, err
			}
		}
		b = appendString(append(b, `],"call":`...), n.name, true)
	case *lambda:
		if b, err = appendNode(append(b, `{"body":`...), n.body); err == nil {
			b = append(b, `,"lambda":[`...)
			for i, p := range n.params {
				if i > 0 {
					b = append(b, ',')
				}
				b = appendString(b, p, true)
			}
			b = append(b, ']')
		}
	case *param:
		b = appendString(append(b, `{"param":`...), n.name, true)
		b = appendSegments(append(b, `,"path":`...), n.path)
	case *slice:
		if b, err = appendNode(append(b, `{"slice":[`...), n.start); err == nil {
			b = append(b, ',')
			if n.end == nil {
				b = append(b, "null"...)
			} else {
				b, err = appendNode(b, n.end)
			}
			b = append(b, ']')
		}
	case *concat:
		b = appendString(append(b, `{"concat":`...), n.prefix, true)
		b, err = appendNode(append(b, `,"x":`...), n.x)
	default:
		return nil, fmt.Errorf("expr: can't encode %T", n)
	}
	if err != nil {
		return nil, err
	}
	return append(b, '}'), nil
}

func appendSegments(b []byte, path []segment) []byte {
	b = append(b, '[')
	for i, s := range path {
		if i > 0 {
			b = append(b, ',')
		}
		switch s.kind {
		case quoted:
			b = append(appendString(append(b, `{"quoted":`...), s.name, true), '}')
		case bareKey:
			b = append(appendString(append(b, `{"bare":`...), s.name, true), '}')
		case index:
			b = append(b, `{"index":true}`...)
		case wildcard:
			b = append(b, `{"each":true}`...)
		default:
			b = appendString(b, s.name, true)
		}
	}
	return append(b, ']')
}

// appendValue appends a literal's value as JSON, with each mapping's keys
// in sorted order.
func appendValue(b []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(b, "null"...), nil
	case bool:
		return strconv.AppendBool(b, v), nil
	case string:
		return appendString(b, v, true), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("expr: can't encode %v", v)
		}
		return appendFloat(b, v), nil
	case []any:
		if v == nil {
			return append(b, "null"...), nil
		}
		b = append(b, '[')
		for i, item := range v {
			if i > 0 {
				b = append(b, ',')
			}
			var err error
			if b, err = appendValue(b, item); err != nil {
				return nil, err
			}
		}
		return append(b, ']'), nil
	case map[string]any:
		if v == nil {
			return append(b, "null"...), nil
		}
		b = append(b, '{')
		first := true
		for k, item := range jsonvalue.Sorted(v) {
			if !first {
				b = append(b, ',')
			}
			first = false
			b = append(appendString(b, k, true), ':')
			var err error
			if b, err = appendValue(b, item); err != nil {
				return nil, err
			}
		}
		return append(b, '}'), nil
	}
	return nil, fmt.Errorf("expr: can't encode a literal of type %T", v)
}

// appendFloat appends a finite number as encoding/json writes it: as
// JavaScript does, with exponents for very large and very small numbers.
func appendFloat(b []byte, f float64) []byte {
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// Go writes e-07 where JavaScript writes e-7.
		if n := len(b); n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return b
}

const hexDigits = "0123456789abcdef"

// appendString appends s as a JSON string, escaped as encoding/json escapes
// it: quotes, backslashes and control characters, U+2028 and U+2029, which
// JavaScript reads as line breaks, and <, > and & when html is set. Each byte
// of invalid UTF-8 becomes U+FFFD.
func appendString(b []byte, s string, html bool) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= ' ' && c != '"' && c != '\\' && (!html || c != '<' && c != '>' && c != '&') {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\b':
				b = append(b, `\b`...)
			case '\f':
				b = append(b, `\f`...)
			case '\n':
				b = append(b, `\n`...)
			case '\r':
				b = append(b, `\r`...)
			case '\t':
				b = append(b, `\t`...)
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			b = append(append(b, s[start:i]...), "\ufffd"...)
		case r == '\u2028' || r == '\u2029':
			b = append(append(b, s[start:i]...), '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
		default:
			i += size
			continue
		}
		i += size
		start = i
	}
	return append(append(b, s[start:]...), '"')
}

// stringLen returns the length of s as appendString appends it.
func stringLen(s string, html bool) int {
	n := len(`""`) + len(s)
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t':
				n++ // and a backslash
			case c < ' ' || html && (c == '<' || c == '>' || c == '&'):
				n += len(`\u0000`) - 1
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			n += len("\ufffd") - 1
		case r == '\u2028' || r == '\u2029':
			n += len(`\u2028`) - size
		}
		i += size
	}
	return n
}

// errTooDeep reports a tree nested more deeply than a parse allows.
var errTooDeep = fmt.Errorf("expr: the tree nests more than %d levels deep", maxDepth)

var (
	errNotObject   = errors.New("expr: a node must be an object")
	errNoKind      = errors.New("expr: a node has no kind")
	errLambdaPlace = errors.New("expr: a lambda is the last argument of map() and filter(), and nothing else")
	errParams      = errors.New("expr: a lambda has one or two parameters")
	errSlice       = errors.New("expr: a slice is [start, end]")
	errSegment     = errors.New("expr: a path segment is a name or an object with one key")
)

// decodeExpr reads an expression that MarshalJSON encoded: its source, and
// its tree.
func decodeExpr(dec *jsontext.Decoder) (*Expr, error) {
	if dec.PeekKind() != '{' {
		return nil, errors.New("expr: an expression must be an object")
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	e := new(Expr)
	var root decoded
	var hasTree bool
	for dec.PeekKind() != '}' {
		name, err := readName(dec)
		if err != nil {
			return nil, err
		}
		switch string(name) {
		case "src":
			if e.src, err = readString(dec); err != nil {
				return nil, err
			}
		case "tree":
			if root, err = decodeNode(dec, 0); err != nil {
				return nil, err
			}
			hasTree = true
		default:
			return nil, fmt.Errorf("expr: unknown field %q", name)
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	switch {
	case !hasTree:
		return nil, errors.New(`expr: an expression has no "tree"`)
	case root.lambda:
		return nil, errLambdaPlace
	case len(root.free) > 0:
		return nil, fmt.Errorf("expr: %q isn't a parameter of a lambda around it", root.free[0])
	}
	e.root = root.n
	return e, nil
}

// decodeTemplate reads a template that MarshalJSON encoded: its parts.
func decodeTemplate(dec *jsontext.Decoder) (*Template, error) {
	t := &Template{parts: []part{}}
	switch tok, err := dec.ReadToken(); {
	case err != nil:
		return nil, err
	case tok.Kind() == 'n':
		return t, nil
	case tok.Kind() != '[':
		return nil, errors.New("expr: a template must be a list")
	}
	n := 0
	for i := 0; dec.PeekKind() != ']'; i++ {
		var p part
		var err error
		if dec.PeekKind() == '{' {
			p.expr, err = decodeExpr(dec)
		} else {
			p.text, err = readString(dec)
		}
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", i, err)
		}
		if p.expr != nil {
			p.at = n + len("${{")
			n += len("${{}}") + len(p.expr.src)
		} else {
			n += len(p.text)
		}
		t.parts = append(t.parts, p)
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	var src strings.Builder
	src.Grow(n)
	for _, p := range t.parts {
		if p.expr == nil {
			src.WriteString(p.text)
			continue
		}
		src.WriteString("${{")
		src.WriteString(p.expr.src)
		src.WriteString("}}")
	}
	t.src = src.String()
	return t, nil
}

// readName reads an object's key, unescaped. It is valid only until the
// decoder reads again.
func readName(dec *jsontext.Decoder) ([]byte, error) {
	raw, err := dec.ReadValue()
	if err != nil {
		return nil, err
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw[1 : len(raw)-1], nil
	}
	return jsontext.AppendUnquote(nil, raw)
}

// readString reads a string, or null, which is "" as encoding/json reads it.
func readString(dec *jsontext.Decoder) (string, error) {
	tok, err := dec.ReadToken()
	switch {
	case err != nil:
		return "", err
	case tok.Kind() == '"':
		return tok.String(), nil
	case tok.Kind() == 'n':
		return "", nil
	}
	return "", errors.New("expr: want a string")
}

// A nodeKey is one of the keys a node can have, and a set of them one of
// their sums. They are in sorted order, which is the order errors name them
// in.
type nodeKey uint32

const (
	keyArgs nodeKey = 1 << iota
	keyBody
	keyCall
	keyConcat
	keyElse
	keyIf
	keyLambda
	keyLit
	keyMember
	keyNot
	keyNum
	keyOp
	keyParam
	keyPath
	keyRef
	keySlice
	keyThen
	keyUp
	keyWord
	keyX
	keyY
)

var nodeKeyNames = [...]string{
	"args", "body", "call", "concat", "else", "if", "lambda", "lit", "member", "not", "num", "op",
	"param", "path", "ref", "slice", "then", "up", "word", "x", "y",
}

func (k nodeKey) String() string { return nodeKeyNames[bits.TrailingZeros32(uint32(k))] }

// nodeKeyOf maps each key's name to it.
var nodeKeyOf = func() map[string]nodeKey {
	m := make(map[string]nodeKey, len(nodeKeyNames))
	for i, name := range nodeKeyNames {
		m[name] = 1 << i
	}
	return m
}()

// nodeKinds maps the key that names each kind of node to the other keys a
// node of that kind can have.
var nodeKinds = map[nodeKey]nodeKey{
	keyLit: 0, keyNum: 0, keyWord: 0,
	keyRef: keyPath, keyUp: keyPath,
	keyOp:     keyX | keyY | keyNot,
	keyIf:     keyThen | keyElse,
	keyMember: keyPath,
	keyCall:   keyArgs,
	keyLambda: keyBody,
	keyParam:  keyPath,
	keySlice:  0,
	keyConcat: keyX,
}

var (
	// kindKeys is every key that names a kind of node.
	kindKeys = func() nodeKey {
		var all nodeKey
		for k := range nodeKinds {
			all |= k
		}
		return all
	}()
	// needed is the keys of the nodes under a node that a node of a kind
	// that can have them must have.
	needed = keyX | keyThen | keyElse | keyBody
)

// A decoded node, with what the node around it checks: whether it is a
// lambda, which only some arguments can be, and the parameters it reads
// that no lambda in it has, which a lambda around it must.
type decoded struct {
	n      node
	lambda bool
	free   []string
}

// A wireNode holds a node's keys as they are read, in whatever order.
type wireNode struct {
	keys nodeKey
	// unknown is the first key no node has, if hasUnknown is set.
	unknown    string
	hasUnknown bool
	// text is the value of the key that names the kind, for the kinds
	// whose value is a string.
	text   string
	lit    any
	up     int
	upOK   bool
	params []string
	path   []segment
	// x is the node under the operator, member or concat, and cond, then
	// and els the ternary's.
	x, y, cond, then, els, body decoded
	args                        []decoded
	start, end                  decoded
	// not is the value of not: true, false, or null; notOK is false for any
	// other.
	not, notOK bool
}

// decodeNode reads a node depth levels deep in a tree.
func decodeNode(dec *jsontext.Decoder, depth int) (decoded, error) {
	if depth > maxDepth {
		return decoded{}, errTooDeep
	}
	switch tok, err := dec.ReadToken(); {
	case err != nil:
		return decoded{}, err
	case tok.Kind() == 'n':
		return decoded{}, errNoKind
	case tok.Kind() != '{':
		return decoded{}, errNotObject
	}
	var w wireNode
	for dec.PeekKind() != '}' {
		name, err := readName(dec)
		if err != nil {
			return decoded{}, err
		}
		if err := w.read(dec, name, depth); err != nil {
			return decoded{}, err
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return decoded{}, err
	}
	return w.node()
}

// read reads the value of a node's key.
func (w *wireNode) read(dec *jsontext.Decoder, name []byte, depth int) error {
	k, ok := nodeKeyOf[string(name)]
	if !ok {
		if !w.hasUnknown {
			w.unknown, w.hasUnknown = string(name), true
		}
		return dec.SkipValue()
	}
	w.keys |= k
	var err error
	switch k {
	case keyLit:
		w.lit, err = jsonvalue.Decode(dec, parseFloat)
	case keyNum, keyWord, keyRef, keyOp, keyCall, keyParam, keyConcat:
		w.text, err = readString(dec)
	case keyUp:
		w.up, w.upOK, err = readInt(dec)
	case keyLambda:
		w.params, err = readParams(dec)
	case keyPath:
		w.path, err = decodePath(dec)
	case keyNot:
		w.not, w.notOK, err = readBool(dec)
	case keyX, keyMember:
		w.x, err = decodeNode(dec, depth+1)
	case keyY:
		w.y, err = decodeNode(dec, depth+1)
	case keyIf:
		w.cond, err = decodeNode(dec, depth+1)
	case keyThen:
		w.then, err = decodeNode(dec, depth+1)
	case keyElse:
		w.els, err = decodeNode(dec, depth+1)
	case keyBody:
		w.body, err = decodeNode(dec, depth+1)
	case keyArgs:
		w.args, err = decodeArgs(dec, depth+1)
	case keySlice:
		err = w.readSlice(dec, depth+1)
	}
	return err
}

func parseFloat(s string) (any, error) { return strconv.ParseFloat(s, 64) }

// node checks a node's keys, and makes it.
func (w *wireNode) node() (decoded, error) {
	kinds := w.keys & kindKeys
	switch bits.OnesCount32(uint32(kinds)) {
	case 0:
		return decoded{}, errNoKind
	case 1:
	default:
		first := kinds & -kinds
		return decoded{}, fmt.Errorf("expr: a node is both %s and %s", first, (kinds&^first)&-(kinds&^first))
	}
	kind := kinds
	if extra := w.keys &^ kind &^ nodeKinds[kind]; extra != 0 {
		return decoded{}, fmt.Errorf("expr: a %s node has no %q", kind, (extra & -extra).String())
	}
	if w.hasUnknown {
		return decoded{}, fmt.Errorf("expr: a %s node has no %q", kind, w.unknown)
	}
	if missing := nodeKinds[kind] & needed &^ w.keys; missing != 0 {
		return decoded{}, fmt.Errorf("expr: a node is missing %q", (missing & -missing).String())
	}
	// Only a call's last argument can be a lambda: the call checks its
	// arguments, and every other node under a node is none.
	if w.x.lambda || w.y.lambda || w.cond.lambda || w.then.lambda || w.els.lambda || w.body.lambda || w.start.lambda || w.end.lambda {
		return decoded{}, errLambdaPlace
	}
	switch kind {
	case keyLit:
		return decoded{n: &literal{value: w.lit}}, nil
	case keyNum:
		switch w.text {
		case "NaN":
			return decoded{n: &literal{value: math.NaN()}}, nil
		case "Infinity":
			return decoded{n: &literal{value: math.Inf(1)}}, nil
		case "-Infinity":
			return decoded{n: &literal{value: math.Inf(-1)}}, nil
		}
		return decoded{}, fmt.Errorf("expr: %q isn't NaN, Infinity or -Infinity", w.text)
	case keyWord:
		return decoded{n: &word{text: w.text}}, nil
	case keyRef:
		if c, ok := contexts[w.text]; !ok || c.name != w.text {
			return decoded{}, fmt.Errorf("expr: %q isn't a context", w.text)
		}
		return decoded{n: &ref{context: w.text, path: w.path}}, nil
	case keyUp:
		if !w.upOK || w.up < 1 {
			return decoded{}, errors.New("expr: a relative reference goes up at least one level")
		}
		return decoded{n: &relative{up: w.up, path: w.path}}, nil
	case keyIf:
		return decoded{n: &ternary{cond: w.cond.n, then: w.then.n, els: w.els.n}, free: union(w.cond.free, w.then.free, w.els.free)}, nil
	case keyMember:
		return decoded{n: &member{x: w.x.n, path: w.path}, free: w.x.free}, nil
	case keyCall:
		return w.call()
	case keyLambda:
		return w.lambda()
	case keyParam:
		return decoded{n: &param{name: w.text, path: w.path}, free: []string{w.text}}, nil
	case keySlice:
		return decoded{n: &slice{start: w.start.n, end: w.end.n}, free: union(w.start.free, w.end.free)}, nil
	case keyConcat:
		return decoded{n: &concat{prefix: w.text, x: w.x.n}, free: w.x.free}, nil
	}
	return w.op()
}

func (w *wireNode) call() (decoded, error) {
	fn, ok := functions[w.text]
	if !ok {
		return decoded{}, fmt.Errorf("expr: %s isn't a function", w.text)
	}
	n := len(w.args)
	if n < fn.minArgs || fn.maxArgs >= 0 && n > fn.maxArgs || w.text == "case" && n%2 == 0 {
		return decoded{}, fmt.Errorf("expr: %s() can't take %d arguments", w.text, n)
	}
	c := &call{name: w.text, args: make([]node, n)}
	var free []string
	for i, a := range w.args {
		if a.lambda != (fn.each != nil && i == n-1) {
			return decoded{}, errLambdaPlace
		}
		c.args[i] = a.n
		free = union(free, a.free)
	}
	return decoded{n: c, free: free}, nil
}

func (w *wireNode) lambda() (decoded, error) {
	if len(w.params) == 0 || len(w.params) > 2 {
		return decoded{}, errParams
	}
	for _, name := range w.params {
		if !identifier(name) {
			return decoded{}, fmt.Errorf("expr: %q isn't a parameter's name", name)
		}
	}
	if i, k := paramProblem(w.params); k != nil {
		return decoded{}, fmt.Errorf("expr: %s", k.Message(problems.Params{"name": w.params[i]}))
	}
	var free []string
	for _, name := range w.body.free {
		if !slices.Contains(w.params, name) {
			free = append(free, name)
		}
	}
	return decoded{n: &lambda{params: w.params, body: w.body.n}, lambda: true, free: free}, nil
}

func (w *wireNode) op() (decoded, error) {
	if w.keys&keyY == 0 {
		if w.text != "!" && w.text != "-" && !prefixOperators[w.text] {
			return decoded{}, fmt.Errorf("expr: %q isn't a unary operator", w.text)
		}
		return decoded{n: &unary{op: w.text, x: w.x.n}, free: w.x.free}, nil
	}
	if !binaryOperator(w.text) {
		return decoded{}, fmt.Errorf("expr: %q isn't a binary operator", w.text)
	}
	if w.keys&keyNot != 0 && !w.notOK {
		return decoded{}, errors.New("expr: not is true or false")
	}
	if w.not && !negatable[w.text] {
		return decoded{}, fmt.Errorf("expr: %q can't be negated", w.text)
	}
	return decoded{n: &binary{op: w.text, negated: w.not, x: w.x.n, y: w.y.n}, free: union(w.x.free, w.y.free)}, nil
}

// union returns the names in any of sets, each once.
func union(sets ...[]string) []string {
	var out []string
	for _, set := range sets {
		for _, name := range set {
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out
}

// decodeArgs reads a call's arguments: a list of nodes, or null for none.
func decodeArgs(dec *jsontext.Decoder, depth int) ([]decoded, error) {
	switch tok, err := dec.ReadToken(); {
	case err != nil:
		return nil, err
	case tok.Kind() == 'n':
		return nil, nil
	case tok.Kind() != '[':
		return nil, errors.New("expr: a call's arguments are a list")
	}
	var args []decoded
	for dec.PeekKind() != ']' {
		a, err := decodeNode(dec, depth)
		if err != nil {
			return nil, err
		}
		args = append(args, a)
	}
	_, err := dec.ReadToken()
	return args, err
}

// readSlice reads a slice's [start, end], with end null when it is open.
func (w *wireNode) readSlice(dec *jsontext.Decoder, depth int) error {
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '[' {
		return cmp.Or(err, errSlice)
	}
	if dec.PeekKind() == ']' {
		return errSlice
	}
	var err error
	if w.start, err = decodeNode(dec, depth); err != nil {
		return err
	}
	switch dec.PeekKind() {
	case ']':
		return errSlice
	case 'n':
		_, err = dec.ReadToken()
	default:
		w.end, err = decodeNode(dec, depth)
	}
	if err != nil {
		return err
	}
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != ']' {
		return cmp.Or(err, errSlice)
	}
	return nil
}

// readInt reads a whole number, or null, which is 0 as encoding/json reads
// it into an int. ok is false for anything else, which is read all the
// same.
func readInt(dec *jsontext.Decoder) (n int, ok bool, err error) {
	switch dec.PeekKind() {
	case 'n':
		_, err := dec.ReadToken()
		return 0, true, err
	case '0':
		tok, err := dec.ReadToken()
		if err != nil {
			return 0, false, err
		}
		i, err := strconv.ParseInt(tok.String(), 10, strconv.IntSize)
		return int(i), err == nil, nil
	}
	return 0, false, dec.SkipValue()
}

// readBool reads true, false or null, which is false as encoding/json reads
// it. ok is false for anything else, which is read all the same.
func readBool(dec *jsontext.Decoder) (b, ok bool, err error) {
	switch dec.PeekKind() {
	case 't', 'f', 'n':
		tok, err := dec.ReadToken()
		return tok.Kind() == 't', true, err
	}
	return false, false, dec.SkipValue()
}

// readParams reads a lambda's parameters: a list of strings, in which null
// is "" as encoding/json reads it, or null for none.
func readParams(dec *jsontext.Decoder) ([]string, error) {
	switch tok, err := dec.ReadToken(); {
	case err != nil:
		return nil, err
	case tok.Kind() == 'n':
		return nil, nil
	case tok.Kind() != '[':
		return nil, errParams
	}
	params := []string{}
	for dec.PeekKind() != ']' {
		name, err := readString(dec)
		if err != nil {
			return nil, errParams
		}
		params = append(params, name)
	}
	_, err := dec.ReadToken()
	return params, err
}

func binaryOperator(op string) bool {
	if _, ok := wordOperators[op]; ok {
		return true
	}
	for _, s := range symbolOperators {
		if s.sym == op {
			return true
		}
	}
	return false
}

// decodePath reads a path: a list of segments, or null for none.
func decodePath(dec *jsontext.Decoder) ([]segment, error) {
	switch tok, err := dec.ReadToken(); {
	case err != nil:
		return nil, err
	case tok.Kind() == 'n':
		return []segment{}, nil
	case tok.Kind() != '[':
		return nil, errors.New("expr: a path is a list")
	}
	path := []segment{}
	for dec.PeekKind() != ']' {
		s, err := decodeSegment(dec)
		if err != nil {
			return nil, err
		}
		path = append(path, s)
	}
	_, err := dec.ReadToken()
	return path, err
}

// decodeSegment reads a path segment: a name, null, which is "" as
// encoding/json reads it, or an object with one key.
func decodeSegment(dec *jsontext.Decoder) (segment, error) {
	if dec.PeekKind() != '{' {
		name, err := readString(dec)
		if err != nil {
			return segment{}, errSegment
		}
		return segment{name: name, kind: field}, nil
	}
	if _, err := dec.ReadToken(); err != nil {
		return segment{}, err
	}
	if dec.PeekKind() == '}' {
		return segment{}, errSegment
	}
	name, err := readName(dec)
	if err != nil {
		return segment{}, err
	}
	var s segment
	switch string(name) {
	case "quoted":
		s.kind = quoted
		s.name, err = readString(dec)
	case "bare":
		s.kind = bareKey
		s.name, err = readString(dec)
	case "index":
		s = segment{name: "index", kind: index}
		err = dec.SkipValue()
	case "each":
		s = segment{name: "*", kind: wildcard}
		err = dec.SkipValue()
	default:
		return segment{}, errors.New("expr: unknown path segment")
	}
	if err != nil {
		return segment{}, err
	}
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '}' {
		return segment{}, cmp.Or(err, errSegment)
	}
	return s, nil
}
