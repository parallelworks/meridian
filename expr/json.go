package expr

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
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
//	{"slice": [start, end]}                  the operand (start):(end); end is null when open
//	{"concat": "2:", "x": ...}               the operand 2:(end)
//
// A path is a list of segments: a string for a name, or {"quoted": "my-key"},
// {"bare": "hidden"}, {"index": true} for [index], and {"each": true} for .*.
//
// Positions aren't encoded: an expression decoded from JSON reports errors
// at offset 0.

// MarshalJSON encodes the expression as its source and its tree.
func (e *Expr) MarshalJSON() ([]byte, error) {
	root, err := encodeNode(e.root)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Src  string          `json:"src"`
		Tree json.RawMessage `json:"tree"`
	}{e.src, root})
}

// UnmarshalJSON decodes an expression that MarshalJSON encoded.
func (e *Expr) UnmarshalJSON(data []byte) error {
	var v struct {
		Src  string          `json:"src"`
		Tree json.RawMessage `json:"tree"`
	}
	if err := strictUnmarshal(data, &v); err != nil {
		return err
	}
	root, err := decodeNode(v.Tree, 0)
	if err != nil {
		return err
	}
	e.src, e.root = v.Src, root
	return nil
}

// MarshalJSON encodes the template as its parts: text, and expressions.
func (t *Template) MarshalJSON() ([]byte, error) {
	parts := make([]any, len(t.parts))
	for i, p := range t.parts {
		if p.expr == nil {
			parts[i] = p.text
		} else {
			parts[i] = p.expr
		}
	}
	return json.Marshal(parts)
}

// UnmarshalJSON decodes a template that MarshalJSON encoded.
func (t *Template) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	t.parts = make([]part, len(raw))
	var src strings.Builder
	for i, r := range raw {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			t.parts[i] = part{text: s}
			src.WriteString(s)
			continue
		}
		e := new(Expr)
		if err := e.UnmarshalJSON(r); err != nil {
			return fmt.Errorf("part %d: %w", i, err)
		}
		src.WriteString("${{")
		t.parts[i] = part{expr: e, at: src.Len()}
		src.WriteString(e.src)
		src.WriteString("}}")
	}
	t.src = src.String()
	return nil
}

func encodeNode(n node) (json.RawMessage, error) {
	var v any
	switch n := n.(type) {
	case *literal:
		if f, ok := n.value.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			v = map[string]any{"num": formatNumber(f)}
		} else {
			v = map[string]any{"lit": n.value}
		}
	case *word:
		v = map[string]any{"word": n.text}
	case *ref:
		v = map[string]any{"ref": n.context, "path": encodePath(n.path)}
	case *relative:
		v = map[string]any{"up": n.up, "path": encodePath(n.path)}
	case *unary:
		x, err := encodeNode(n.x)
		if err != nil {
			return nil, err
		}
		v = map[string]any{"op": n.op, "x": x}
	case *binary:
		x, err := encodeNode(n.x)
		if err != nil {
			return nil, err
		}
		y, err := encodeNode(n.y)
		if err != nil {
			return nil, err
		}
		m := map[string]any{"op": n.op, "x": x, "y": y}
		if n.negated {
			m["not"] = true
		}
		v = m
	case *ternary:
		nodes, err := encodeNodes(n.cond, n.then, n.els)
		if err != nil {
			return nil, err
		}
		v = map[string]any{"if": nodes[0], "then": nodes[1], "else": nodes[2]}
	case *member:
		x, err := encodeNode(n.x)
		if err != nil {
			return nil, err
		}
		v = map[string]any{"member": x, "path": encodePath(n.path)}
	case *call:
		args, err := encodeNodes(n.args...)
		if err != nil {
			return nil, err
		}
		v = map[string]any{"call": n.name, "args": args}
	case *slice:
		start, err := encodeNode(n.start)
		if err != nil {
			return nil, err
		}
		end := json.RawMessage("null")
		if n.end != nil {
			if end, err = encodeNode(n.end); err != nil {
				return nil, err
			}
		}
		v = map[string]any{"slice": []json.RawMessage{start, end}}
	case *concat:
		x, err := encodeNode(n.x)
		if err != nil {
			return nil, err
		}
		v = map[string]any{"concat": n.prefix, "x": x}
	default:
		return nil, fmt.Errorf("expr: can't encode %T", n)
	}
	return json.Marshal(v)
}

func encodeNodes(ns ...node) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, len(ns))
	for i, n := range ns {
		var err error
		if out[i], err = encodeNode(n); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func encodePath(path []segment) []any {
	out := make([]any, len(path))
	for i, s := range path {
		switch s.kind {
		case quoted:
			out[i] = map[string]string{"quoted": s.name}
		case bareKey:
			out[i] = map[string]string{"bare": s.name}
		case index:
			out[i] = map[string]bool{"index": true}
		case wildcard:
			out[i] = map[string]bool{"each": true}
		default:
			out[i] = s.name
		}
	}
	return out
}

// errTooDeep reports a tree nested more deeply than a parse allows.
var errTooDeep = fmt.Errorf("expr: the tree nests more than %d levels deep", maxDepth)

func decodeNode(data json.RawMessage, depth int) (node, error) {
	if depth > maxDepth {
		return nil, errTooDeep
	}
	var m map[string]json.RawMessage
	if err := jsonv2.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("expr: a node must be an object: %w", err)
	}
	kind, err := nodeKind(m)
	if err != nil {
		return nil, err
	}
	str := func(k string) (string, error) {
		var s string
		err := json.Unmarshal(m[k], &s)
		return s, err
	}
	sub := func(k string) (node, error) {
		raw, ok := m[k]
		if !ok {
			return nil, fmt.Errorf("expr: a node is missing %q", k)
		}
		return decodeNode(raw, depth+1)
	}
	switch kind {
	case "lit":
		var v any
		if err := json.Unmarshal(m["lit"], &v); err != nil {
			return nil, err
		}
		return &literal{value: v}, nil
	case "num":
		s, err := str("num")
		if err != nil {
			return nil, err
		}
		n := stringToNumber(s)
		if !math.IsNaN(n) && !math.IsInf(n, 0) && s != "NaN" {
			return nil, fmt.Errorf("expr: %q isn't NaN or an infinity", s)
		}
		return &literal{value: n}, nil
	case "word":
		s, err := str("word")
		return &word{text: s}, err
	case "ref":
		name, err := str("ref")
		if err != nil {
			return nil, err
		}
		if c, ok := contexts[name]; !ok || c.name != name {
			return nil, fmt.Errorf("expr: %q isn't a context", name)
		}
		path, err := decodePath(m["path"])
		return &ref{context: name, path: path}, err
	case "up":
		var up int
		if err := json.Unmarshal(m["up"], &up); err != nil || up < 1 {
			return nil, fmt.Errorf("expr: a relative reference goes up at least one level")
		}
		path, err := decodePath(m["path"])
		return &relative{up: up, path: path}, err
	case "if":
		cond, err := sub("if")
		if err != nil {
			return nil, err
		}
		then, err := sub("then")
		if err != nil {
			return nil, err
		}
		els, err := sub("else")
		return &ternary{cond: cond, then: then, els: els}, err
	case "member":
		x, err := sub("member")
		if err != nil {
			return nil, err
		}
		path, err := decodePath(m["path"])
		return &member{x: x, path: path}, err
	case "call":
		name, err := str("call")
		if err != nil {
			return nil, err
		}
		fn, ok := functions[name]
		if !ok {
			return nil, fmt.Errorf("expr: %s isn't a function", name)
		}
		var raw []json.RawMessage
		if err := json.Unmarshal(m["args"], &raw); err != nil && has(m, "args") {
			return nil, err
		}
		if len(raw) < fn.minArgs || fn.maxArgs >= 0 && len(raw) > fn.maxArgs || name == "case" && len(raw)%2 == 0 {
			return nil, fmt.Errorf("expr: %s() can't take %d arguments", name, len(raw))
		}
		args := make([]node, len(raw))
		for i, r := range raw {
			if args[i], err = decodeNode(r, depth+1); err != nil {
				return nil, err
			}
		}
		return &call{name: name, args: args}, nil
	case "slice":
		var raw []json.RawMessage
		if err := json.Unmarshal(m["slice"], &raw); err != nil || len(raw) != 2 {
			return nil, errors.New("expr: a slice is [start, end]")
		}
		start, err := decodeNode(raw[0], depth+1)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(bytes.TrimSpace(raw[1]), []byte("null")) {
			return &slice{start: start}, nil
		}
		end, err := decodeNode(raw[1], depth+1)
		return &slice{start: start, end: end}, err
	case "concat":
		prefix, err := str("concat")
		if err != nil {
			return nil, err
		}
		x, err := sub("x")
		return &concat{prefix: prefix, x: x}, err
	case "op":
		op, err := str("op")
		if err != nil {
			return nil, err
		}
		x, err := sub("x")
		if err != nil {
			return nil, err
		}
		if !has(m, "y") {
			if op != "!" && op != "-" && !prefixOperators[op] {
				return nil, fmt.Errorf("expr: %q isn't a unary operator", op)
			}
			return &unary{op: op, x: x}, nil
		}
		if !binaryOperator(op) {
			return nil, fmt.Errorf("expr: %q isn't a binary operator", op)
		}
		y, err := sub("y")
		if err != nil {
			return nil, err
		}
		var negated bool
		if has(m, "not") {
			if err := json.Unmarshal(m["not"], &negated); err != nil {
				return nil, err
			}
		}
		if negated && !negatable[op] {
			return nil, fmt.Errorf("expr: %q can't be negated", op)
		}
		return &binary{op: op, negated: negated, x: x, y: y}, nil
	}
	return nil, fmt.Errorf("expr: can't decode a %s node", kind)
}

// nodeKinds maps the key that names each kind of node to the other keys a
// node of that kind can have.
var nodeKinds = map[string][]string{
	"lit": nil, "num": nil, "word": nil,
	"ref": {"path"}, "up": {"path"},
	"op":     {"x", "y", "not"},
	"if":     {"then", "else"},
	"member": {"path"},
	"call":   {"args"},
	"slice":  nil,
	"concat": {"x"},
}

// nodeKind returns the kind of the node m, which has exactly one key that
// names a kind, and no keys that kind doesn't have.
func nodeKind(m map[string]json.RawMessage) (string, error) {
	keys := slices.Sorted(maps.Keys(m))
	kind := ""
	for _, k := range keys {
		if _, ok := nodeKinds[k]; !ok {
			continue
		}
		if kind != "" {
			return "", fmt.Errorf("expr: a node is both %s and %s", kind, k)
		}
		kind = k
	}
	if kind == "" {
		return "", errors.New("expr: a node has no kind")
	}
	for _, k := range keys {
		if k != kind && !slices.Contains(nodeKinds[kind], k) {
			return "", fmt.Errorf("expr: a %s node has no %q", kind, k)
		}
	}
	return kind, nil
}

func has(m map[string]json.RawMessage, k string) bool {
	_, ok := m[k]
	return ok
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

func decodePath(data json.RawMessage) ([]segment, error) {
	if data == nil {
		return nil, nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	path := make([]segment, len(raw))
	for i, r := range raw {
		var name string
		if err := json.Unmarshal(r, &name); err == nil {
			path[i] = segment{name: name, kind: field}
			continue
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(r, &m); err != nil || len(m) != 1 {
			return nil, errors.New("expr: a path segment is a name or an object with one key")
		}
		switch {
		case has(m, "quoted"), has(m, "bare"):
			kind, k := quoted, "quoted"
			if has(m, "bare") {
				kind, k = bareKey, "bare"
			}
			if err := json.Unmarshal(m[k], &name); err != nil {
				return nil, err
			}
			path[i] = segment{name: name, kind: kind}
		case has(m, "index"):
			path[i] = segment{name: "index", kind: index}
		case has(m, "each"):
			path[i] = segment{name: "*", kind: wildcard}
		default:
			return nil, errors.New("expr: unknown path segment")
		}
	}
	return path, nil
}

// strictUnmarshal decodes JSON, failing on keys v doesn't have, matched
// exactly, and on keys given twice.
func strictUnmarshal(data []byte, v any) error {
	return jsonv2.Unmarshal(data, v, jsonv2.RejectUnknownMembers(true))
}
