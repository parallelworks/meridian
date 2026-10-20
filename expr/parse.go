package expr

import (
	"fmt"
	"strconv"
	"strings"
)

// maxDepth bounds how deeply an expression nests, so that parsing and
// evaluating it can't exhaust the stack.
const maxDepth = 200

// An Expr is a parsed expression: the text between ${{ and }}.
type Expr struct {
	src  string
	root node
}

// String returns the expression's source.
func (e *Expr) String() string { return e.src }

// A SyntaxError is an expression that can't be parsed.
type SyntaxError struct {
	// Offset is the byte offset in the expression of the problem.
	Offset int
	Msg    string
}

func (e *SyntaxError) Error() string { return e.Msg }

// Parse parses an expression: the text between ${{ and }}.
func Parse(src string) (*Expr, error) {
	p := &parser{src: src}
	p.skipSpace()
	if p.pos == len(src) {
		return nil, p.errorf("the expression is empty")
	}
	root, err := p.expression(0)
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos < len(src) {
		return nil, p.unexpected()
	}
	if n := tooDeep(root, 0); n != nil {
		return nil, p.errorAt(n.offset(), "the expression nests more than %d levels deep", maxDepth)
	}
	return &Expr{src: src, root: root}, nil
}

// tooDeep returns a node of the tree under n, which is depth levels deep,
// that is more than maxDepth levels deep, or nil. The parser counts the
// levels it recurses through, but a chain such as a + b + c or !!x nests
// without recursing, and a decoded tree must not be deeper than a parsed one.
func tooDeep(n node, depth int) node {
	if depth > maxDepth {
		return n
	}
	for _, c := range children(n) {
		if deep := tooDeep(c, depth+1); deep != nil {
			return deep
		}
	}
	return nil
}

// Operator precedence, from loosest to tightest. Binary operators of the
// same precedence are left-associative.
const (
	precTernary = iota + 1
	precOr
	precAnd
	precCompare
	precRange
	precAdd
	precMul
	precPow
	precSplit
)

// wordOperators are the binary operators written as words.
var wordOperators = map[string]int{
	"in": precCompare, "includes": precCompare, "startsWith": precCompare, "endsWith": precCompare, "get": precCompare,
	"range": precRange,
	"mod":   precMul,
	"exp":   precPow,
	"split": precSplit, "join": precSplit,
}

// negatable are the word operators that ! negates: !in, !includes.
var negatable = map[string]bool{"in": true, "includes": true, "startsWith": true, "endsWith": true}

// symbolOperators are the binary operators written as symbols, longest
// first so that === wins over ==.
var symbolOperators = []struct {
	sym  string
	prec int
}{
	{"===", precCompare},
	{"!==", precCompare},
	{"==", precCompare},
	{"!=", precCompare},
	{"<=", precCompare},
	{">=", precCompare},
	{"??", precCompare},
	{"&&", precAnd},
	{"||", precOr},
	{"**", precPow},
	{"//", precMul},
	{"<", precCompare},
	{">", precCompare},
	{"+", precAdd},
	{"-", precAdd},
	{"*", precMul},
	{"/", precMul},
	{"%", precMul},
	{"^", precPow},
}

// prefixOperators are the unary operators written as words: length x.
var prefixOperators = map[string]bool{
	"length": true, "len": true, "upper": true, "lower": true, "trim": true,
	"keys": true, "values": true, "abs": true, "floor": true, "ceil": true,
	"round": true, "fromJSON": true, "range": true,
}

// contexts are the names an operand can start with to read a context, and
// the context each reads: var is another name for vars. A word that starts
// with any other name is a string. job is one of Meridian's additions to
// format 1.
var contexts = map[string]string{
	"inputs": "inputs", "needs": "needs", "steps": "steps", "matrix": "matrix", "env": "env", "job": "job",
	"org": "org", "vars": "vars", "var": "vars", "remote": "remote", "sessions": "sessions",
}

type parser struct {
	src   string
	pos   int
	depth int
	// colonEndsWord is non-zero inside a ? branch written without a ':'
	// that has a space before it, where a ':' ends a word: a?1:2. Elsewhere
	// a ':' is part of a word, as in 12:30 or 1:3.
	colonEndsWord int
}

func (p *parser) errorAt(at int, format string, args ...any) error {
	return &SyntaxError{Offset: at, Msg: fmt.Sprintf(format, args...)}
}

func (p *parser) errorf(format string, args ...any) error {
	return p.errorAt(p.pos, format, args...)
}

func (p *parser) unexpected() error {
	if p.pos >= len(p.src) {
		return p.errorf("the expression ends too soon")
	}
	end := min(p.pos+8, len(p.src))
	if i := strings.IndexAny(p.src[p.pos:end], " \t\r\n"); i > 0 {
		end = p.pos + i
	}
	return p.errorf("unexpected %q", p.src[p.pos:end])
}

func (p *parser) skipSpace() {
	for p.pos < len(p.src) && isSpace(p.src[p.pos]) {
		p.pos++
	}
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func isLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

// isWordByte reports whether c can be part of a word.
func isWordByte(c byte) bool {
	return isLetter(c) || isDigit(c) || c == '_' || c == '.' || c == '$' || c == '@' || c == '~' || c == '#' || c >= 0x80
}

func (p *parser) peek(off int) byte {
	if p.pos+off < len(p.src) {
		return p.src[p.pos+off]
	}
	return 0
}

// enter counts a level of nesting and fails past maxDepth.
func (p *parser) enter() error {
	p.depth++
	if p.depth > maxDepth {
		return p.errorf("the expression nests more than %d levels deep", maxDepth)
	}
	return nil
}

// letters returns the word of letters at pos, or "" if a word byte follows
// the letters.
func (p *parser) letters(pos int) string {
	end := pos
	for end < len(p.src) && isLetter(p.src[end]) {
		end++
	}
	if end < len(p.src) && isWordByte(p.src[end]) {
		return ""
	}
	return p.src[pos:end]
}

type infix struct {
	op      string
	negated bool
	prec    int
	width   int
}

// peekInfix returns the binary operator at the current position, if any.
func (p *parser) peekInfix() (infix, bool) {
	if p.pos >= len(p.src) {
		return infix{}, false
	}
	if p.src[p.pos] == '!' {
		if w := p.letters(p.pos + 1); negatable[w] {
			return infix{op: w, negated: true, prec: wordOperators[w], width: 1 + len(w)}, true
		}
	}
	for _, s := range symbolOperators {
		if strings.HasPrefix(p.src[p.pos:], s.sym) {
			return infix{op: s.sym, prec: s.prec, width: len(s.sym)}, true
		}
	}
	w := p.letters(p.pos)
	switch strings.ToLower(w) {
	case "or":
		return infix{op: "||", prec: precOr, width: len(w)}, true
	case "and":
		return infix{op: "&&", prec: precAnd, width: len(w)}, true
	}
	if prec, ok := wordOperators[w]; ok {
		return infix{op: w, prec: prec, width: len(w)}, true
	}
	return infix{}, false
}

// expression parses operators of at least minPrec.
func (p *parser) expression(minPrec int) (node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer func() { p.depth-- }()
	left, err := p.unary()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.src) {
			return left, nil
		}
		switch c := p.src[p.pos]; {
		case c == ')' || c == ',' || c == ':':
			return left, nil
		case c == '?' && p.peek(1) != '?':
			if precTernary < minPrec {
				return left, nil
			}
			at := p.pos
			p.pos++
			glued := !p.spacedColonAhead()
			if glued {
				p.colonEndsWord++
			}
			then, err := p.expression(0)
			if glued {
				p.colonEndsWord--
			}
			if err != nil {
				return nil, err
			}
			p.skipSpace()
			if p.peek(0) != ':' {
				return nil, p.errorf("expected ':' for the '?' at offset %d", at)
			}
			p.pos++
			els, err := p.expression(precTernary)
			if err != nil {
				return nil, err
			}
			left = &ternary{at: at, cond: left, then: then, els: els}
			continue
		}
		op, ok := p.peekInfix()
		if !ok {
			return nil, p.unexpected()
		}
		if op.prec < minPrec {
			return left, nil
		}
		at := p.pos
		p.pos += op.width
		right, err := p.expression(op.prec + 1)
		if err != nil {
			return nil, err
		}
		left = &binary{at: at, op: op.op, negated: op.negated, x: left, y: right}
	}
}

// unary parses an operand with any prefix operators.
func (p *parser) unary() (node, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer func() { p.depth-- }()
	p.skipSpace()
	if p.pos >= len(p.src) {
		return nil, p.unexpected()
	}
	at := p.pos
	switch c := p.src[p.pos]; {
	case c == '!':
		return p.not()
	case c == '-' && (isDigit(p.peek(1)) || p.peek(1) == '.' && isDigit(p.peek(2))):
		return operand(p.word(), at), nil
	case c == '-':
		p.pos++
		x, err := p.unary()
		if err != nil {
			return nil, err
		}
		return &unary{at: at, op: "-", x: x}, nil
	case c == '(':
		x, err := p.group()
		if err != nil {
			return nil, err
		}
		if p.peek(0) == ':' && p.colonEndsWord == 0 {
			return p.sliceEnd(x, at)
		}
		return p.postfix(x, at)
	case c == '\'' || c == '"' || c == '`':
		s, err := p.quoted(c)
		if err != nil {
			return nil, err
		}
		return &literal{at: at, value: s}, nil
	case c == '[' || c == '{':
		raw, err := p.balanced()
		if err != nil {
			return nil, err
		}
		v, err := parseJSON(raw)
		if err != nil {
			return nil, p.errorAt(at, `%s isn't valid JSON; write lists and mappings in JSON, with double quotes, as in ["a", "b"]`, raw)
		}
		return &literal{at: at, value: v}, nil
	case c == ':' && isDigit(p.peek(1)) && p.colonEndsWord == 0:
		return operand(p.word(), at), nil
	}
	w := p.word()
	if w == "" {
		return nil, p.unexpected()
	}
	if prefixOperators[w] && p.canStartOperand() {
		if p.peek(0) == '(' {
			x, err := p.group()
			if err != nil {
				return nil, err
			}
			return p.postfix(&unary{at: at, op: w, x: x}, at)
		}
		var x node
		var err error
		if w == "range" {
			// range takes arithmetic: range n + 1.
			x, err = p.expression(precAdd)
		} else {
			x, err = p.unary()
		}
		if err != nil {
			return nil, err
		}
		return &unary{at: at, op: w, x: x}, nil
	}
	if p.peek(0) == '(' {
		if strings.HasSuffix(w, ":") {
			x, err := p.group()
			if err != nil {
				return nil, err
			}
			return &concat{at: at, prefix: w, x: x}, nil
		}
		return p.call(w, at)
	}
	return operand(w, at), nil
}

func (p *parser) call(name string, at int) (node, error) {
	fn, ok := functions[name]
	if !ok {
		return nil, p.errorAt(at, "%s isn't a function", name)
	}
	p.pos++
	var args []node
	p.skipSpace()
	if p.peek(0) == ')' {
		p.pos++
	} else {
		for {
			arg, err := p.expression(0)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			p.skipSpace()
			if p.peek(0) == ',' {
				p.pos++
				continue
			}
			if p.peek(0) != ')' {
				return nil, p.errorf("expected ',' or ')' in the call to %s", name)
			}
			p.pos++
			break
		}
	}
	if len(args) < fn.minArgs || fn.maxArgs >= 0 && len(args) > fn.maxArgs {
		return nil, p.errorAt(at, "%s() takes %s, not %d", name, fn.arity(), len(args))
	}
	if name == "case" && len(args)%2 == 0 {
		return nil, p.errorAt(at, "case() takes pairs of a condition and a value, then a default")
	}
	return p.postfix(&call{at: at, name: name, args: args}, at)
}

// not parses one or more ! and the operand they negate.
func (p *parser) not() (node, error) {
	var bangs []int
	for p.peek(0) == '!' {
		bangs = append(bangs, p.pos)
		p.pos++
	}
	x, err := p.unary()
	if err != nil {
		return nil, err
	}
	for i := len(bangs) - 1; i >= 0; i-- {
		x = &unary{at: bangs[i], op: "!", x: x}
	}
	return x, nil
}

func (p *parser) group() (node, error) {
	p.pos++
	outer := p.colonEndsWord
	p.colonEndsWord = 0
	x, err := p.expression(0)
	p.colonEndsWord = outer
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.peek(0) != ')' {
		return nil, p.errorf("expected ')'")
	}
	p.pos++
	return x, nil
}

// sliceEnd parses the :end after (start), where end is a group, a number, or
// nothing.
func (p *parser) sliceEnd(start node, at int) (node, error) {
	p.pos++
	switch c := p.peek(0); {
	case c == '(':
		end, err := p.group()
		if err != nil {
			return nil, err
		}
		return &slice{at: at, start: start, end: end}, nil
	case isDigit(c) || c == '-' && isDigit(p.peek(1)):
		endAt := p.pos
		return &slice{at: at, start: start, end: operand(p.word(), endAt)}, nil
	}
	return &slice{at: at, start: start}, nil
}

// spacedColonAhead reports whether the rest of the current group has a ':'
// with a space before it. Without one, a ? branch is written glued, as in
// a?1:2, and its ':' ends a word instead of joining 12:30.
func (p *parser) spacedColonAhead() bool {
	depth := 0
	var quote byte
	for i := p.pos; i < len(p.src); i++ {
		c := p.src[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			if depth == 0 {
				return false
			}
			depth--
		case c == ',' && depth == 0:
			return false
		case c == ':' && depth == 0 && i > 0 && isSpace(p.src[i-1]):
			return true
		}
	}
	return false
}

// postfix parses .name and [key] after a computed value.
func (p *parser) postfix(x node, at int) (node, error) {
	start := p.pos
	for p.pos < len(p.src) {
		switch c := p.src[p.pos]; {
		case c == '.' && isWordByte(p.peek(1)):
			p.pos++
			for p.pos < len(p.src) && isWordByte(p.src[p.pos]) && p.src[p.pos] != '.' {
				p.pos++
			}
		case c == '[':
			if _, err := p.balanced(); err != nil {
				return nil, err
			}
		default:
			if p.pos == start {
				return x, nil
			}
			return &member{at: at, x: x, path: splitPath(p.src[start:p.pos])}, nil
		}
	}
	if p.pos == start {
		return x, nil
	}
	return &member{at: at, x: x, path: splitPath(p.src[start:p.pos])}, nil
}

// canStartOperand reports whether an operand follows, so that a prefix
// operator's name is the operator rather than a word: length is a word in
// `inputs.x == length`.
func (p *parser) canStartOperand() bool {
	save := p.pos
	defer func() { p.pos = save }()
	p.skipSpace()
	if p.pos >= len(p.src) {
		return false
	}
	switch p.src[p.pos] {
	case ')', ',', ':', '?':
		return false
	case '!', '-', '(':
		return true
	}
	_, isInfix := p.peekInfix()
	return !isInfix
}

// word scans a word: a reference such as inputs.a['b-c'][0] or ../x, a
// number such as -2.5e-3, or a bare word such as 12:30. A '-' is
// subtraction unless it signs a number or its exponent, and a '/' is
// division except in ./ and ../.
func (p *parser) word() string {
	start := p.pos
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		inWord := p.pos > start
		switch {
		case isWordByte(c):
			p.pos++
		case c == '-' && (!inWord || exponentSign(p.src[start:p.pos], p.peek(1))):
			p.pos++
		case c == '/' && inWord && p.src[p.pos-1] == '.':
			p.pos++
		case c == ':' && (inWord || isDigit(p.peek(1))) && p.colonEndsWord == 0:
			p.pos++
		case c == '*' && inWord && p.src[p.pos-1] == '.':
			p.pos++
		case c == '[' && inWord:
			if _, err := p.balanced(); err != nil {
				return p.src[start:p.pos]
			}
		default:
			return p.src[start:p.pos]
		}
	}
	return p.src[start:p.pos]
}

// exponentSign reports whether a '-' after w, followed by next, is the sign
// of a number's exponent, as in 2.5e-3.
func exponentSign(w string, next byte) bool {
	mantissa, ok := strings.CutSuffix(strings.TrimPrefix(w, "-"), "e")
	if !ok {
		mantissa, ok = strings.CutSuffix(strings.TrimPrefix(w, "-"), "E")
	}
	if !ok || mantissa == "" || !isDigit(next) {
		return false
	}
	_, err := strconv.ParseFloat(mantissa, 64)
	return err == nil
}

// quoted scans a string in quotes. Inside single quotes, ” is a quote.
// Nothing else is escaped.
func (p *parser) quoted(q byte) (string, error) {
	start := p.pos
	p.pos++
	var b strings.Builder
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if c == q {
			if q == '\'' && p.peek(1) == '\'' {
				b.WriteByte('\'')
				p.pos += 2
				continue
			}
			p.pos++
			return b.String(), nil
		}
		b.WriteByte(c)
		p.pos++
	}
	p.pos = start
	return "", p.errorf("the string has no closing %c", q)
}

// balanced scans a [...] or {...}, skipping brackets in quotes.
func (p *parser) balanced() (string, error) {
	start := p.pos
	depth := 0
	var quote byte
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		p.pos++
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '[' || c == '{':
			depth++
		case c == ']' || c == '}':
			depth--
			if depth == 0 {
				return p.src[start:p.pos], nil
			}
		}
	}
	p.pos = start
	return "", p.errorf("the %c has no closing bracket", p.src[start])
}

// operand classifies a scanned word: a relative reference, a reference to a
// context, a number, true, false, null, or else a bare word.
func operand(w string, at int) node {
	if strings.HasPrefix(w, ".") && len(w) > 1 {
		return relativeRef(w, at)
	}
	if name, rest, ok := contextPrefix(w); ok {
		return &ref{at: at, context: contexts[name], path: splitPath(rest)}
	}
	if n, err := strconv.ParseFloat(w, 64); err == nil {
		return &literal{at: at, value: n}
	}
	lower := strings.ToLower(w)
	if hex, ok := strings.CutPrefix(lower, "0x"); ok {
		if n, err := strconv.ParseInt(hex, 16, 64); err == nil {
			return &literal{at: at, value: float64(n)}
		}
	}
	switch lower {
	case "true":
		return &literal{at: at, value: true}
	case "false":
		return &literal{at: at, value: false}
	case "null", "undefined":
		return &literal{at: at, value: nil}
	}
	return &word{at: at, text: w}
}

// contextPrefix splits a word that reads a context into the context's name
// and the rest of the path.
func contextPrefix(w string) (name, rest string, ok bool) {
	i := strings.IndexAny(w, ".[")
	if i <= 0 || i == len(w)-1 {
		return "", "", false
	}
	if _, ok := contexts[w[:i]]; !ok {
		return "", "", false
	}
	return w[:i], w[i:], true
}

// relativeRef parses .x, ..x, ../x and ../../.x: each leading dot, and each
// ../, is a level up from the key the expression is in, so .x is a sibling.
func relativeRef(w string, at int) *relative {
	rest, up := w, 0
	for strings.HasPrefix(rest, "../") {
		up++
		rest = rest[3:]
	}
	if up > 0 && !strings.HasPrefix(rest, ".") {
		// ../x is a level above .x.
		up++
	}
	for strings.HasPrefix(rest, ".") {
		up++
		rest = rest[1:]
	}
	return &relative{at: at, up: up, path: splitPath(rest)}
}

// splitPath splits a path such as .a['b.c'][0].*.d into segments.
func splitPath(s string) []segment {
	var path []segment
	for i := 0; i < len(s); {
		switch s[i] {
		case '.', '/', ']':
			i++
		case '[':
			end := closingBracket(s, i)
			path = append(path, bracketSegment(s[i+1:end]))
			i = end + 1
		default:
			end := i
			for end < len(s) && !strings.ContainsRune("./[]", rune(s[end])) {
				end++
			}
			name := s[i:end]
			kind := field
			if name == "*" {
				kind = wildcard
			}
			path = append(path, segment{name: name, kind: kind})
			i = end
		}
	}
	return path
}

// closingBracket returns the index of the ']' that closes the '[' at i, or
// len(s) if there is none.
func closingBracket(s string, i int) int {
	var quote byte
	for j := i + 1; j < len(s); j++ {
		switch c := s[j]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == ']':
			return j
		}
	}
	return len(s)
}

func bracketSegment(content string) segment {
	content = strings.TrimSpace(content)
	if len(content) >= 2 && (content[0] == '\'' || content[0] == '"') && content[len(content)-1] == content[0] {
		return segment{name: content[1 : len(content)-1], kind: quoted}
	}
	if content == "index" {
		return segment{name: content, kind: index}
	}
	if _, err := strconv.Atoi(content); err == nil {
		return segment{name: content, kind: field}
	}
	return segment{name: content, kind: bareKey}
}
