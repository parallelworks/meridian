package expr

// A node is part of a parsed expression. at is the byte offset in the
// expression's source of what the node reports errors at: the start of an
// operand, an operator, or a function's name.
type node interface {
	offset() int
}

// A literal is a number, string, true, false, null, or a list or mapping
// written as JSON.
type literal struct {
	at    int
	value any
}

// A word is a bare word that is none of the other operands. Its value is its
// text: in `inputs.mode == debug`, debug is "debug".
type word struct {
	at   int
	text string
}

// A ref reads a context: inputs.cluster.ip, needs['build'].outputs.path.
type ref struct {
	at      int
	context string
	path    []segment
}

// A relative reads the workflow near the expression: .hidden is a sibling
// key, and each further dot, or ../, is a level up.
type relative struct {
	at   int
	up   int
	path []segment
}

// A unary is !x, -x, or a prefix operator such as length x.
type unary struct {
	at int
	op string
	x  node
}

// A binary is x op y. negated is set for !in, !includes, !startsWith and
// !endsWith.
type binary struct {
	at      int
	op      string
	negated bool
	x, y    node
}

// A ternary is cond ? then : els.
type ternary struct {
	at              int
	cond, then, els node
}

// A member reads properties of a computed value: (fromJSON x).a[0].
type member struct {
	at   int
	x    node
	path []segment
}

// A call is a function call: contains(inputs.list, 'x').
type call struct {
	at   int
	name string
	args []node
}

// A lambda is the x => body, (x) => body or (x, i) => body that is the last
// argument of map() and filter(), which evaluate body for each item: x is
// the item, and i its index in a list or its key in a mapping.
type lambda struct {
	at     int
	params []string
	body   node
}

// A param reads a parameter of a lambda around it, and the path in its
// value: x, x.name, x[0]. A word whose name is a parameter's reads it only in
// the lambda's body.
type param struct {
	at   int
	name string
	path []segment
}

// A slice is the slice operand (start):(end), (start):5 or (start):, whose
// value is the string "start:end". end is nil when it is left open.
type slice struct {
	at         int
	start, end node
}

// A concat is the slice operand 2:(end), whose value is the word before the
// group followed by the group's value.
type concat struct {
	at     int
	prefix string
	x      node
}

func (n *literal) offset() int  { return n.at }
func (n *word) offset() int     { return n.at }
func (n *ref) offset() int      { return n.at }
func (n *relative) offset() int { return n.at }
func (n *unary) offset() int    { return n.at }
func (n *binary) offset() int   { return n.at }
func (n *ternary) offset() int  { return n.at }
func (n *member) offset() int   { return n.at }
func (n *call) offset() int     { return n.at }
func (n *lambda) offset() int   { return n.at }
func (n *param) offset() int    { return n.at }
func (n *slice) offset() int    { return n.at }
func (n *concat) offset() int   { return n.at }

// A segment is one step of a path.
type segment struct {
	name string
	kind segmentKind
}

type segmentKind int

const (
	// field is a name after a dot, or a name or number in brackets:
	// .cluster, [0], [1:3].
	field segmentKind = iota
	// quoted is a name in quotes in brackets: ['my-key'].
	quoted
	// bareKey is a name in brackets without quotes, such as [hidden]. At
	// the end of an inputs reference it reads the input's definition
	// rather than its value.
	bareKey
	// index is [index], the index of the list item an input template is
	// for.
	index
	// wildcard is .*, which reads the rest of the path from each item of a
	// list or mapping.
	wildcard
)
