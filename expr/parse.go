package expr

import (
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/parallelworks/meridian/problems"
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
	// Offset is the byte offset of the problem: in the expression, in the
	// template ParseTemplate parsed, or in the string at Location.
	Offset int
	// Location is the JSON Pointer of the string in a workflow the problem
	// is in, when it is in another expression than the one evaluated: one
	// that it reads, as a relative reference or an input's definition
	// does. It is "" for the expression itself.
	Location string
	// Kind is the kind of problem, and Params the values its message shows.
	Kind   *problems.Kind
	Params problems.Params
}

func (e *SyntaxError) Error() string { return e.Kind.Message(e.Params) }

// Parse parses an expression: the text between ${{ and }}.
func Parse(src string) (*Expr, error) {
	p := &parser{src: src}
	p.skipSpace()
	if p.pos == len(src) {
		return nil, p.errorf(problems.ExpressionEmpty, nil)
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
		return nil, p.errorAt(n.offset(), problems.ExpressionTooDeep, problems.Params{"max": maxDepth})
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
// same precedence are left-associative, except the power operators, which
// are right-associative as JavaScript's ** is.
const (
	precTernary = iota + 1
	// ?? binds looser than || and &&, but can't be mixed with them without
	// parentheses, as in JavaScript.
	precCoalesce
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

// anyCaseOperators are the binary operators written as words in any case:
// OR is ||, and And is &&.
var anyCaseOperators = map[string]infix{
	"or":  {op: "||", prec: precOr},
	"and": {op: "&&", prec: precAnd},
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
	{"??", precCoalesce},
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

// A contextInfo is what the language says about a context.
type contextInfo struct {
	// name is the context's name.
	name string
	// strings is set for a context whose values are strings, where a name
	// that isn't there reads as "".
	strings bool
}

// contexts are the contexts an expression can read, by the names an operand
// can start with to read each: var is another name for vars. A word that
// starts with any other name is a string. job is one of Meridian's additions
// to format 1.
var contexts = map[string]contextInfo{
	"inputs": {name: "inputs"}, "matrix": {name: "matrix"}, "job": {name: "job"},
	"needs": {"needs", true}, "steps": {"steps", true}, "env": {"env", true},
	"org": {"org", true}, "vars": {"vars", true}, "var": {"vars", true},
	"remote": {"remote", true}, "sessions": {"sessions", true},
}

type parser struct {
	src string
	pos int
	// depth is how many levels the operand being parsed is nested in:
	// groups, calls, operators' operands and ternaries' branches.
	depth int
	// colonEndsWord is non-zero inside a ? branch written without a ':'
	// that has a space before it, where a ':' ends a word: a?1:2. Elsewhere
	// a ':' is part of a word, as in 12:30 or 1:3.
	colonEndsWord int
	// grouped holds the nodes written in parentheses, which ?? can be mixed
	// with.
	grouped map[node]bool
	// params are the parameters of the lambdas whose bodies are being
	// parsed, innermost last.
	params []string
}

func (p *parser) errorAt(at int, k *problems.Kind, params problems.Params) error {
	return &SyntaxError{Offset: at, Kind: k, Params: params}
}

func (p *parser) errorf(k *problems.Kind, params problems.Params) error {
	return p.errorAt(p.pos, k, params)
}

func (p *parser) unexpected() error {
	if p.pos >= len(p.src) {
		return p.errorf(problems.ExpressionEndsEarly, nil)
	}
	if strings.HasPrefix(p.src[p.pos:], "=>") {
		// A lambda is parsed only as the last argument of map() or
		// filter().
		return p.errorf(problems.LambdaMisplaced, nil)
	}
	end := min(p.pos+8, len(p.src))
	if i := strings.IndexAny(p.src[p.pos:end], " \t\r\n"); i > 0 {
		end = p.pos + i
	}
	return p.errorf(problems.UnexpectedText, problems.Params{"text": p.src[p.pos:end]})
}

func (p *parser) skipSpace() {
	for p.pos < len(p.src) && isSpace(p.src[p.pos]) {
		p.pos++
	}
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func isLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

// isNameByte reports whether c can be part of a name such as my_var2: a
// letter, a digit or an underscore. A name doesn't start with a digit.
func isNameByte(c byte) bool { return isLetter(c) || isDigit(c) || c == '_' }

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

// enter fails if the operand about to be parsed is nested more than
// maxDepth levels deep, and otherwise counts a level for what it holds.
func (p *parser) enter() error {
	if p.depth > maxDepth {
		return p.errorf(problems.ExpressionTooDeep, problems.Params{"max": maxDepth})
	}
	p.depth++
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
	if op, ok := anyCaseOperators[strings.ToLower(w)]; ok {
		op.width = len(w)
		return op, true
	}
	if prec, ok := wordOperators[w]; ok {
		return infix{op: w, prec: prec, width: len(w)}, true
	}
	return infix{}, false
}

// expression parses operators of at least minPrec. Every operand is parsed
// by unary, which counts how deeply it is nested.
func (p *parser) expression(minPrec int) (node, error) {
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
				return nil, p.errorf(problems.TernaryMissingColon, problems.Params{"at": strconv.Itoa(at)})
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
		rightPrec := op.prec + 1
		if op.prec == precPow {
			rightPrec = op.prec
		}
		right, err := p.expression(rightPrec)
		if err != nil {
			return nil, err
		}
		if op.op == "??" && (p.logical(left) || p.logical(right)) {
			return nil, p.errorAt(at, problems.CoalesceMixed, nil)
		}
		left = &binary{at: at, op: op.op, negated: op.negated, x: left, y: right}
	}
}

// logical reports whether n is an || or && written without parentheses.
// With ?? looser than both, an || or && beside a ?? is always its operand.
func (p *parser) logical(n node) bool {
	b, ok := n.(*binary)
	return ok && (b.op == "||" || b.op == "&&") && !p.grouped[n]
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
			return nil, p.errorAt(at, problems.InvalidJSONLiteral, problems.Params{"text": raw})
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
	if n, ok := p.param(w, at); ok {
		return n, nil
	}
	return operand(w, at), nil
}

// param returns the reference to a lambda's parameter that w is, if it is
// in the body of a lambda that has the parameter: its name alone, or
// followed by a path, as in x, x.name or x[0].
func (p *parser) param(w string, at int) (*param, bool) {
	end := 0
	for end < len(w) && isNameByte(w[end]) {
		end++
	}
	if end == 0 || end < len(w) && w[end] != '.' && w[end] != '[' || !slices.Contains(p.params, w[:end]) {
		return nil, false
	}
	return &param{at: at, name: w[:end], path: splitPath(w[end:])}, true
}

func (p *parser) call(name string, at int) (node, error) {
	fn, ok := functions[name]
	if !ok {
		return nil, p.errorAt(at, problems.NotAFunction, problems.Params{"name": name})
	}
	p.pos++
	var args []node
	p.skipSpace()
	if p.peek(0) == ')' {
		p.pos++
	} else {
		for {
			var arg node
			var err error
			if fn.each != nil && len(args) == fn.maxArgs-1 {
				arg, err = p.lambda(name)
			} else {
				arg, err = p.expression(0)
			}
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
				return nil, p.errorf(problems.CallMissingComma, problems.Params{"name": name})
			}
			p.pos++
			break
		}
	}
	if len(args) < fn.minArgs || fn.maxArgs >= 0 && len(args) > fn.maxArgs {
		params := fn.arity()
		params["name"], params["count"] = name, len(args)
		return nil, p.errorAt(at, problems.WrongArgumentCount, params)
	}
	if name == "case" && len(args)%2 == 0 {
		return nil, p.errorAt(at, problems.CaseArguments, nil)
	}
	return p.postfix(&call{at: at, name: name, args: args}, at)
}

// lambda parses the lambda that is the last argument of fn, map() or
// filter(): x => body, (x) => body or (x, i) => body. As in JavaScript, a
// comma can end the parameters in parentheses. The lambda isn't a level of
// nesting for the parser, as the call is, but it is one of the tree's.
func (p *parser) lambda(fn string) (node, error) {
	p.skipSpace()
	at := p.pos
	open := p.peek(0) == '('
	if open {
		p.pos++
	}
	var params []string
	var offsets []int
	for {
		p.skipSpace()
		start := p.pos
		for p.pos < len(p.src) && isNameByte(p.src[p.pos]) && (p.pos > start || !isDigit(p.src[p.pos])) {
			p.pos++
		}
		if p.pos == start {
			break
		}
		params, offsets = append(params, p.src[start:p.pos]), append(offsets, start)
		p.skipSpace()
		if !open || p.peek(0) != ',' {
			break
		}
		p.pos++
	}
	if open && p.peek(0) == ')' {
		p.pos++
		open = false
	}
	p.skipSpace()
	if open || len(params) == 0 || !strings.HasPrefix(p.src[p.pos:], "=>") {
		return nil, p.errorAt(at, problems.LambdaExpected, problems.Params{"name": fn})
	}
	if len(params) > 2 {
		return nil, p.errorAt(at, problems.LambdaTooManyParameters, problems.Params{"name": fn, "count": len(params)})
	}
	if i, k := paramProblem(params); k != nil {
		return nil, p.errorAt(offsets[i], k, problems.Params{"name": params[i]})
	}
	p.pos += len("=>")
	p.params = append(p.params, params...)
	body, err := p.expression(0)
	p.params = p.params[:len(p.params)-len(params)]
	if err != nil {
		return nil, err
	}
	return &lambda{at: at, params: params, body: body}, nil
}

// paramProblem returns the kind of problem with a lambda's parameters, if
// any, and the parameter that has it: a name that a word already means
// something by, or the same name as the parameter before it.
func paramProblem(params []string) (i int, k *problems.Kind) {
	for i, name := range params {
		switch {
		case reserved(name):
			return i, problems.LambdaParameterReserved
		case slices.Contains(params[:i], name):
			return i, problems.LambdaParameterTwice
		}
	}
	return 0, nil
}

// reserved reports whether name can't name a lambda's parameter, because a
// word of that name already means something: a context, a keyword or a word
// operator, NaN or Infinity, or index, which [index] reads. The keywords,
// the operators and and or, NaN and Infinity are reserved in any case.
func reserved(name string) bool {
	_, context := contexts[name]
	_, keyword := keywords[strings.ToLower(name)]
	_, anyCase := anyCaseOperators[strings.ToLower(name)]
	_, operator := wordOperators[name]
	return context || keyword || anyCase || operator || prefixOperators[name] || name == "index" ||
		strings.EqualFold(name, "NaN") || strings.EqualFold(name, "Infinity")
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
	for _, at := range slices.Backward(bangs) {
		x = &unary{at: at, op: "!", x: x}
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
		return nil, p.errorf(problems.ExpectedCloseParen, nil)
	}
	p.pos++
	if p.grouped == nil {
		p.grouped = map[node]bool{}
	}
	p.grouped[x] = true
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

// spacedColonAhead reports whether the ternary whose ? was just read is
// closed by a ':' with a space before it, later in the current group.
// Without one, its first branch is written glued, as in a?1:2, and a ':' in
// it ends a word instead of joining 12:30. A later ? with a space before it
// starts a spaced ternary, which a spaced ':' closes first, so in
// true?1:x ? 2 : 3 the first ? has none.
func (p *parser) spacedColonAhead() bool {
	depth, later := 0, 0
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
		case depth > 0:
		case c == ',':
			return false
		case c == '?' && i+1 < len(p.src) && p.src[i+1] == '?':
			// ?? isn't a ternary.
			i++
		case c == '?' && isSpace(p.src[i-1]):
			later++
		case c == ':' && isSpace(p.src[i-1]):
			if later == 0 {
				return true
			}
			later--
		}
	}
	return false
}

// postfix parses .name and [key] after a computed value.
func (p *parser) postfix(x node, at int) (node, error) {
	start := p.pos
	for p.pos < len(p.src) {
		switch c := p.src[p.pos]; {
		case c == '.' && p.peek(1) == '*':
			// .* reads the rest of the path from each item, as in a
			// reference's path.
			p.pos += 2
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
	if !ok || !isDigit(next) {
		return false
	}
	return decimal(mantissa)
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
	return "", p.errorf(problems.UnclosedString, problems.Params{"quote": string(q)})
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
	return "", p.errorf(problems.UnclosedBracket, problems.Params{"bracket": string(p.src[start])})
}

// operand classifies a scanned word: a relative reference, a reference to a
// context, a number, true, false, null, or else a bare word.
func operand(w string, at int) node {
	if strings.HasPrefix(w, ".") && len(w) > 1 {
		// A word that starts with a dot and reads as a number is one, as in
		// JavaScript: .5 is 0.5. Any other is a relative reference: .name,
		// ..5, .5.x. A key named with digits is read as .[5].
		if n, ok := wordNumber(w); ok {
			return &literal{at: at, value: n}
		}
		return relativeRef(w, at)
	}
	if name, rest, ok := contextPrefix(w); ok {
		return &ref{at: at, context: contexts[name].name, path: splitPath(rest)}
	}
	if n, ok := wordNumber(w); ok {
		return &literal{at: at, value: n}
	}
	if v, ok := keywords[strings.ToLower(w)]; ok {
		return &literal{at: at, value: v}
	}
	return &word{at: at, text: w}
}

// keywords are the words that are values, in any case: true, false, and
// null, which undefined is too.
var keywords = map[string]any{"true": true, "false": false, "null": nil, "undefined": nil}

// wordNumber reads a word as a number, as a string is read (stringToNumber),
// except that a word of nothing but space isn't one, and a - before a
// hexadecimal, octal or binary number negates it: -0x10 is -16, where the
// string "-0x10" is NaN. NaN and Infinity are numbers, as JavaScript's
// globals are.
func wordNumber(w string) (float64, bool) {
	if w == "NaN" {
		return math.NaN(), true
	}
	w = strings.TrimFunc(w, isJSSpace)
	if digits, ok := strings.CutPrefix(w, "-"); ok && len(digits) > 2 && digits[0] == '0' && radix(digits[1]) != 0 {
		n, ok := numeral(digits)
		return -n, ok
	}
	if w == "" {
		return 0, false
	}
	return numeral(w)
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
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == ']':
			return j
		}
	}
	return len(s)
}

// bracketSegment reads what is in brackets: a key in any of the three kinds
// of quotes, [index], a number, or a bare key.
func bracketSegment(content string) segment {
	content = strings.TrimSpace(content)
	if len(content) >= 2 && strings.IndexByte("'\"`", content[0]) >= 0 && content[len(content)-1] == content[0] {
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
