# Expressions

This is the grammar of format 1's `${{ }}` expressions, with the limits and
conversions that are part of the language. [The design](design.md#the-language)
says what each part means, and the conformance suite's `expressions` area pins
it case by case. The parser, in `expr/parse.go` and `expr/template.go`, is the
reference: a pull request that changes what it accepts updates this document.

## Notation

The grammar uses the notation of [the Go specification](https://go.dev/ref/spec#Notation):
`|` separates alternatives, `()` groups, `[]` is optional and `{}` repeats
zero or more times. Lowercase names are scanned from bytes, and CamelCase
names are parsed from what is scanned. `a … b` is a range of bytes, and
comments say what the notation can't.

## Templates

A string in a workflow is a template: text with expressions in it.

```
Template = { text | "${{" Expression "}}" } .
text     = /* bytes that aren't an expression */ .
```

- An expression starts at the first `${{` and ends at the first `}}` after it
  that isn't in quotes or brackets. A `'`, `"` or `` ` `` starts a quote that
  ends at the next one of its kind. A `(`, `[` or `{` starts a bracket that
  ends when brackets of that kind balance, and brackets of other kinds don't
  count inside it. So `${{ {"a": "}}"} }}` is one expression.
- A `${{` without such a `}}` is a syntax error. So is `${{ 'a }} ${{ 1 }}`,
  whose quote never closes.
- Text has no escapes. A `}}` in text is text, so `${{ 1 }}}` is `1}`. A
  template writes `${{` as text by building it: `${{ '${{' }}`.

A template that is one expression and nothing else has the expression's
value, of any type. Otherwise each expression is replaced by its value's text
([Values](#values)).

## Tokens

```
byte       = /* any byte */ .
whitespace = " " | "\t" | "\n" | "\r" .
letter     = "a" … "z" | "A" … "Z" .
digit      = "0" … "9" .
word_byte  = letter | digit | "_" | "." | "$" | "@" | "~" | "#" | non_ascii .
non_ascii  = /* a byte from 0x80 to 0xFF */ .
```

Whitespace separates tokens and is otherwise ignored, except where this
document says so. Other spaces, such as a no-break space, are word bytes.

### Words

Most of an expression is words: references, numbers and text written without
quotes.

```
word       = word_start { word_byte | ":" | exp_sign | dot_slash | dot_star | brackets } .
word_start = word_byte | "-" digit | "-" "." digit | ":" digit .
exp_sign   = "-" .         /* only after a number's e or E, before a digit: 2.5e-3 */
dot_slash  = "/" .         /* only after a ".": ../x */
dot_star   = "*" .         /* only after a ".": .*.name */
brackets   = "[" { byte } "]" .   /* to the balancing bracket; brackets in quotes don't count */
```

A word ends at the first byte that can't continue it, such as whitespace or
an operator. A `:` is part of a word except in some ternaries
([Where the grammar depends on context](#where-the-grammar-depends-on-context)).
A `-` starts a word only as a number's sign: elsewhere it is an operator.

A word is scanned whole, then read as the first of these that fits:

1. **A lambda's parameter**, in the lambda's body: the parameter's name,
   alone or followed by a `.` or `[` and more ([Lambdas](#lambdas)).
2. **A relative reference**: a word that starts with `.` and has more after
   it, unless it is a number ([Relative references](#relative-references)).
   `.5` and `.5e1` are numbers, as in JavaScript, while `.name`, `..5` and
   `.5.x` are relative references. A key named with digits is read in
   brackets: `.[5]`.
3. **A reference**: a context's name, then a `.` or `[` and more
   ([References](#references)).
4. **A number**: a word that is a number as a string is one
   ([Values](#values)), such as `42`, `2.5e-3`, `017` (17), `0xff`, `0o17` or
   `0b101`, and `1e400`, which is beyond a 64-bit float's range and so is
   Infinity. A `-` before a hexadecimal, octal or binary number negates it:
   `-0x10` is -16. `NaN` and `Infinity`, in that case, are numbers too, as
   JavaScript's globals are. `1_000`, `0x1p4`, `inf` and `nan` aren't
   numbers, so they are bare words.
5. **`true`, `false`, `null`**, and `undefined`, which is `null`, in any
   case.
6. **A bare word**, whose value is its text: `debug`, `ubuntu:22.04`, `12:30`,
   `10.0.0.5`, `1.5.3`, `$HOME`. The compiler warns about bare words that
   look like mistakes ([the design](design.md#the-language)).

### References

```
Reference = context path .   /* scanned as one word; path starts with "." or "[" */
context   = "inputs" | "needs" | "steps" | "matrix" | "env" | "job"
          | "org" | "vars" | "var" | "remote" | "sessions" .
path      = { "." | "/" | segment } .
segment   = name | "*" | "[" key "]" .
name      = /* one or more bytes other than ".", "/", "[" and "]" */ .
key       = "'" { byte } "'" | `"` { byte } `"` | "index" | integer | bare_key .
integer   = [ "+" | "-" ] digit { digit } .
bare_key  = /* any other text */ .
```

- Context names are case-sensitive. `var` is another name for `vars`, and
  `job` is one of Meridian's additions to format 1. A context's name alone,
  or followed only by `.`, is a bare word, and so is a word that starts with
  any other name, such as `secrets.TOKEN` or `jobs.x`.
- `inputs.hosts[0]`, `inputs.hosts.[0]`, `inputs.hosts.0` and
  `inputs[hosts]0` read the same thing.
- A bracket holds a key, never an expression: its text up to the first `]`
  outside quotes, without the whitespace around it. So
  `inputs.list[inputs.i]` reads the key `inputs.i`. To read a key that an
  expression computes, use `get`: `inputs.list get inputs.i`.
- A key in quotes, single, double or back, is the text between them, as it
  is: ``inputs[`my-key`]`` reads the key `my-key`, as `inputs['my-key']`
  does. `[index]` is the index of the list item that an input's template is
  for. A whole number is a key or a list index. Any
  other key is a bare key, which reads like a name, except at the end of an
  `inputs` reference, where it reads the input's definition before its value:
  `inputs.x[hidden]`.
- `*` reads the rest of the path from each item of a list, or each value of a
  mapping in the order of its keys, and makes a list of what it reads. A
  further `*` flattens the lists: `inputs.people.*.tags.*`. It reads the
  same way after a group or a call: `fromJSON(inputs.json).*.name`.
- A name or key with a `:` in it, such as `1:3`, `-2:` or `:2`, is a slice of
  a list or a string ([Values](#values)).

### Relative references

```
Relative = { "../" } { "." } path .   /* starts with "."; scanned as one word that isn't a number */
```

A relative reference reads the workflow around the expression. Each `.`
before the path goes up one level from the key the expression is in, so `.x`
is a key beside it and `..x` is a key beside its parent. Each `../` goes up a
level too, and `../x` is `..x`: `../../x` and `../../.x` both go up three
levels. A word that would read as a number, such as `.5` or `.0`, is the
number, so a key or an index named with digits beside the expression is
written in brackets: `.[5]`, `.['5']` or `.[0]`.

### Strings

```
string = "'" { byte | "''" } "'"
       | `"` { byte } `"`
       | "`" { byte } "`" .
```

A string runs to the next quote of its kind. Nothing in it is escaped, so
`"a \n b"` holds a backslash and an `n`. Only a single-quoted string can hold
its own quote, written twice: `'it''s'`.

### Lists and mappings

```
JSON = "[" { byte } "]" | "{" { byte } "}" .   /* to the balancing bracket, then decoded */
```

A list or mapping is written in JSON ([RFC 8259](https://www.rfc-editor.org/rfc/rfc8259)),
with double quotes. The parser scans to the bracket that balances the first,
counting `[` and `{` alike and skipping brackets in quotes, and decodes what
it scanned. `['a']` is a syntax error.

## Expressions

```
Expression = Binary [ "?" Expression ":" Expression ] .
Binary     = Unary { binary_op Unary } .   /* by precedence; see below */
Unary      = "!" Unary
           | "-" Unary
           | prefix_op Unary
           | "range" Arith
           | Operand .
Arith      = Unary { arith_op Unary } .
Operand    = Group [ Slice | Postfix ]
           | Call
           | word "(" Expression ")"       /* the word ends in ":": 2:(len x) */
           | Param | Reference | Relative | string | JSON | word .
Group      = "(" Expression ")" .
Slice      = ":" [ Group | word ] .         /* the word is a number: (start):5 */
Postfix    = { "." name | ".*" | "[" key "]" } .
Call       = function "(" [ Expression { "," Expression } ] ")" Postfix
           | each "(" Expression "," Lambda ")" Postfix .

prefix_op  = "length" | "len" | "upper" | "lower" | "trim" | "keys" | "values"
           | "abs" | "floor" | "ceil" | "round" | "fromJSON" .
function   = "contains" | "startsWith" | "endsWith" | "format" | "join" | "toJSON"
           | "case" | "success" | "failure" | "cancelled" | "canceled" | "always" .
each       = "map" | "filter" .
```

- **No whitespace** comes before a call's `(`, the `(` after a word that
  ends in `:`, a group's `Slice`, or each part of a `Postfix`.
  `contains (a, b)` is a syntax error, and so is a call to a name that isn't
  a function, such as `hashFiles(x)`.
- **Prefix operators.** A prefix operator's name is an operator only when an
  operand follows it: after any whitespace, a `!`, `-` or `(`, or anything
  other than `)`, `,`, `:`, `?`, a binary operator or the end. Otherwise it is
  a bare word, so `inputs.x == length` compares with `"length"`. Directly
  before `(` it takes the group, and a `Postfix` can follow:
  `fromJSON(inputs.json).a`. `range` takes arithmetic, so `range n + 1` is
  `range (n + 1)`; the others take one operand, so `length inputs.list + 1` is
  `(length inputs.list) + 1`.
- **`-`** before a digit, or before `.` and a digit, starts a number: `-2.5`.
  Otherwise it negates what follows: `-inputs.num`.
- **Slices after a group** are text that `get` reads as a slice:
  `(start):(end)`, `(start):5` and `(start):` are the string `start:end`, and
  `2:(len x)` is `2:` followed by the group's value.
- **Functions** check their number of arguments when they're parsed. Their
  names are case-sensitive.

  | Function | Arguments |
  | --- | --- |
  | `contains`, `startsWith`, `endsWith` | 2 |
  | `format` | 1 or more |
  | `join` | 1 or 2 |
  | `toJSON` | 1 |
  | `case` | an odd number, 3 or more: conditions and values, then a default |
  | `success`, `failure`, `cancelled`, `canceled`, `always` | none |
  | `map`, `filter` | 2: a list, then a lambda ([Lambdas](#lambdas)) |

  `fromJSON(x)` is the prefix operator with a group.

### Operators

```
binary_op = "||" | "or" | "&&" | "and"
          | "==" | "!=" | "===" | "!==" | "<" | "<=" | ">" | ">=" | "??"
          | "in" | "!in" | "includes" | "!includes"
          | "startsWith" | "!startsWith" | "endsWith" | "!endsWith" | "get"
          | "range" | arith_op .
arith_op  = "+" | "-" | "*" | "/" | "//" | "%" | "mod"
          | "**" | "^" | "exp" | "split" | "join" .
```

From loosest to tightest:

| Precedence | Operators |
| --- | --- |
| 1 | `? :` |
| 2 | `??` |
| 3 | `\|\|`, `or` |
| 4 | `&&`, `and` |
| 5 | `==`, `!=`, `===`, `!==`, `<`, `<=`, `>`, `>=`, `in`, `includes`, `startsWith`, `endsWith`, `get`, and the negated `!in`, `!includes`, `!startsWith`, `!endsWith` |
| 6 | `range` |
| 7 | `+`, `-` |
| 8 | `*`, `/`, `//`, `%`, `mod` |
| 9 | `**`, `^`, `exp` |
| 10 | `split`, `join` |

The unary `!` and `-` and the prefix operators bind tighter than all of them,
except that `range` before an operand takes arithmetic: `!inputs.count - 1` is
`(!inputs.count) - 1`, and `-2 ** 2` is `(-2) ** 2`, 4.

- Binary operators of the same precedence are left-associative, so
  `1 < 2 < 3` compares `true` with 3. The power operators are the exception:
  they are right-associative, as JavaScript's `**` is, so `2 ** 3 ** 2` is
  `2 ** (3 ** 2)`, 512.
- `??` binds looser than comparisons, `||` and `&&`, as in JavaScript:
  `a ?? b == c` is `a ?? (b == c)`. As in JavaScript, `??` can't be mixed
  with `||`, `or`, `&&` or `and` without parentheses: `a || b ?? c` and
  `a ?? b && c` are syntax errors, and `(a || b) ?? c` is fine.
- The ternary is right-associative: `a ? b : c ? d : e` is
  `a ? b : (c ? d : e)`, and its condition is everything before the `?`
  that binds tighter, so `a || b ? c : d` is `(a || b) ? c : d`.
- Symbols are read longest first: `===` before `==`, `**` before `*`, `//`
  before `/`. `=` alone isn't an operator.
- Word operators are whole words. They are case-sensitive, except `and` and
  `or`, which are operators in any case. A `!` directly before `in`,
  `includes`, `startsWith` or `endsWith` negates it, with nothing between.

### Lambdas

```
Lambda     = ( param_name | "(" param_name [ "," param_name ] [ "," ] ")" ) "=>" Expression .
param_name = ( letter | "_" ) { letter | digit | "_" } .
Param      = param_name path .   /* in the lambda's body, scanned as one word; path is empty or starts with "." or "[" */
```

`map(list, x => expr)` makes a list of what its lambda gives for each item of
`list`, and `filter(list, x => cond)` a list of the items its lambda gives a
true value for.

- **Where.** A lambda is the last argument of `map` and `filter`, and nothing
  else. A `=>` anywhere else is a syntax error, as in `map(x => x, list)` or
  `contains(list, x => x)`.
- **Parameters.** A lambda has one or two: the item, and its index in a list
  or its key in a mapping. The index is a number, and the key a string.
  `x => x`, `(x) => x` and `(x, i) => i` are lambdas, and as in JavaScript a
  comma can end the parameters in parentheses: `(x,) => x`. Whitespace is
  optional around them and around `=>`.
- **Names.** A parameter can't be named after a context, a keyword or a word
  operator, such as `inputs`, `true`, `and`, `in` or `len`, or `NaN`,
  `Infinity` or `index`, and two can't have the same name. The keywords,
  `and`, `or`, `NaN` and `Infinity` are reserved in any case, so `False` and
  `nan` can't name one, while `Inputs` and `Len` can. A function's name, such
  as `format`, can name one, and a call to the function still calls it.
- **The body.** In the lambda's body, a word that is a parameter's name,
  alone or followed by a path, reads the parameter: `h`, `h.name`,
  `h['my-key']`, `h.nodes.*.host`. The path reads it as a reference's path
  reads a context. An inner lambda's parameter hides an outer one of the same
  name in the inner body: `map(lists, x => map(x, x => x * 10))`. Anywhere
  else the name is a word, so in `format('{0} {1}', map(l, x => x), x)` the
  last `x` is `"x"`, and so is an `x` in an expression the body reads, such
  as an input's default or a key that a relative reference reads.
- **What they go through.** A list's items in order, or a mapping's values in
  the order of their keys, as `keys` and `values` give them. A string that
  holds a JSON list or mapping is read as its value, as outputs are, and null
  has no items, so a missing input gives `[]`. Anything else, such as a
  number, a boolean or other text, is an evaluation error. A list without
  items runs no lambda: `map([], x => needs.a.outputs.b)` is `[]`, and reads
  nothing.
- **What they make.** `map` makes a list of what its lambda gives for each
  item, of a mapping too. `filter` makes a list of the items, or a mapping's
  values, that its lambda gives a true value for, by the language's
  truthiness ([Values](#values)). So `filter(hosts, h => h.enabled)` keeps a
  host whose `enabled` is the text `"false"`, which is true. To keep only
  what is `true`, compare with it: `h => h.enabled == true` for a boolean, or
  `h => h.enabled == 'true'` for text.

The list composes like any other value:
`join(map(filter(inputs.hosts, h => h.gpus > 0), h => '--host=' + h.name), ' ')`,
`length(filter(inputs.hosts, h => h.gpus > 1))`,
`map(inputs.hosts, h => h.name)[0]` and
`filter(inputs.hosts, h => h.gpus > 0).*.name`.

### Where the grammar depends on context

What some bytes mean depends on what is around them:

- **`:`** is part of a word, as in `12:30`, `ubuntu:22.04` or the slice
  `1:3`, and it also separates a ternary's branches. At each `?`, the parser
  looks ahead, to the end of the enclosing group or a `,` at the same level,
  for the `:` with whitespace before it that closes this ternary. A later
  `?` with whitespace before it, at the same level, starts a spaced
  ternary, and a spaced `:` closes the latest of those still open before it
  can close this one. A `??` isn't a ternary's `?`. If this ternary has a spaced `:`, a `:` in its first
  branch stays part of a word: `true ? 12:30 : 13:00`, and
  `a ? b ? 12:30 : d : e`. If it hasn't, the ternary is written without
  spaces, and a `:` ends a word in its first branch: `inputs.flag?1:2`. So
  in `true?1:x ? 2 : 3`, the spaced `:` is the second ternary's, the first
  is written without spaces, and the whole is `true ? 1 : (x ? 2 : 3)`. A
  `?` written without a space before it doesn't count, so
  `true ? false?1:2 : 3` reads `1:2` as a word, and is a syntax error;
  `true ? (false?1:2) : 3` isn't.
- **`-`** is subtraction between words: `inputs.num-1` subtracts 1, and
  `my-word` is `my` minus `word`, which is NaN. Brackets read a key with a
  `-` in it: `inputs['num-1']`. A `-` at the start of an operand before a
  digit, or inside a number before its exponent's digits, is a sign. The
  compiler warns about a reference that subtracts a word with nothing
  between them, such as `inputs.pw-user`: it is NaN, and probably meant the
  name with the hyphen, `inputs['pw-user']`. The warning says so when the
  workflow has that name.
- **`/` and `*`** are division and multiplication, except right after a `.`
  in a word: `../x` and `.*`. So `~/dir` is `~` divided by `dir`.
- **Prefix operators' names** are operators only before an operand.
- **A lambda's parameter** is a parameter only in the lambda's body, and a
  word elsewhere.
- **The last argument of `map` and `filter`** is a lambda, so a `(` there
  starts its parameters rather than a group: `map(l, (x) => x)` is a lambda,
  and `map(l, (x))` is a syntax error.
- **`!`** before `in`, `includes`, `startsWith` or `endsWith` where an
  operator can be is a negated operator. Where an operand can be, `!in` is
  `!` applied to the bare word `in`.
- **A word's kind** is known only once it is scanned whole: `1.5` is a
  number, and `1.5.3` is a bare word.

This is why the parser is written by hand: precedence climbing over the
expression's bytes, without a separate tokenizer, because a token's extent
depends on what is around it. A parser generator would need the same
exceptions written as code.

## Limits

The limits are part of the language, so that a workflow can't make the
compiler or coordinator do unbounded work:

- **Nesting.** An expression nests at most 200 levels. The parser counts a
  level for each group, call, operator's operand and ternary branch that an
  operand is inside, so 200 groups nest and 201 don't. The tree it builds is
  at most 200 levels deep too, which bounds chains such as `a + b + …` and
  `!!…x`. A stored tree is decoded by the same rule. A lambda's body counts
  toward both: the call is a level for the parser, as it is for any of its
  arguments, and the call and the lambda are two levels of the tree. So 199
  groups nest in a lambda's body, and 198 `!` before an operand in it.
- **`range`** takes whole numbers from -2^53 to 2^53, and makes at most
  10,000 of them.
- **What a template builds.** A template, a `${{ }}` alone or text with one
  or more `${{ }}` in it, builds at most 8 MiB. It doesn't count what another
  template that it reads builds, such as an input's default or a key that a
  relative reference reads: that template has its own count.
- **The work a template does.** A template does at most 32 MiB of work,
  counted in bytes as what it builds is: 8 for each step it takes, the size
  of each value its operations read, and what it builds. A template it reads
  has its own count, as for what it builds.

### What a template builds

What a template builds is the size of each value that its operations make. A
value's size is:

- for a string, its bytes;
- for a list, 8 for each item, plus each item's size;
- for a mapping, 8 for each entry, plus each key's bytes and each value's
  size;
- for anything else, 8.

Each operation in this table counts the size of what it makes, and nothing
else counts toward what a template builds: reading a value, a path, `get`,
`keys`, `values` and `trim` build nothing.

| Operation | Counts |
| --- | --- |
| A template with text around its `${{ }}` | Each value's text; the text around them is free |
| `+` with a string on either side | The string it makes |
| `format` | The string it makes, once |
| `join`, the function and the word operator | The string it makes, before it makes it |
| `split` | The string's bytes, plus 8 for each part, before it splits it |
| `toJSON` | The indented JSON it makes |
| `fromJSON` | The value it reads from the text |
| `upper` and `lower` | The string it makes |
| `range`, the prefix and the word operator | The list it makes: 16 for each number |
| `map` | The list it makes: 8 for each item, plus the item's size |
| `filter` | The list it makes: 8 for each item it keeps, plus the item's size |
| The slice operands `(start):(end)` and `2:(end)` | The string they make |

- An operation that gives back what it was given makes nothing, and counts
  nothing: `join` of a string, which is the string, and `fromJSON` of a value
  that isn't a string.
- `split` makes one part more than the times it finds the separator in the
  string. With the separator `''`, it makes a part of each character, and
  none of an empty string.
- `format` counts its string's length once, however many placeholders it
  has.
- `map` and `filter` count each item of their list, even one that was there
  already, as in `map(l, x => x)`. What a lambda's body makes counts too,
  each time it runs, and an item that `filter` doesn't keep counts nothing.

### The work a template does

A template's work is the sum of three counts, in bytes.

**Its steps, 8 each,** the size of a scalar. Each of these is a step, each
time the template evaluates it:

- an operand: a literal, such as `1`, `-5`, `'text'`, `true`, `null`, or a
  list or mapping written as JSON; a bare word; a reference, such as
  `inputs.a`; a relative reference, such as `.hidden`; a lambda's parameter;
  a computed value that a path reads, as `(x).a` and `fromJSON(x).a` do; and
  the slice operands `(start):(end)` and `2:(end)`;
- an operator: a unary one, such as `!`, `-` or `length`; a binary one, such
  as `+`, `==`, `&&`, `??`, `in`, `!in` or `get`; and the ternary `? :`;
- a call of a function, `case` and the status functions among them;
- each name, index, `[index]` or `*` of a path: `inputs.a[0].b` has three,
  and counts 32 with its operand;
- each item that a `.*` visits. The rest of the path is followed from each
  item, and counts again for each: `inputs.l.*.a` over a list of three items
  counts 8 for the operand, 8 each for `l` and `*`, and 16 for each item, 72;
- each run of a lambda, besides what its body counts each time it runs. A
  lambda in another runs for each of its items each time the other runs.

A step is the same whatever the operands are, and whatever tree an
implementation parses the expression into:

- Parentheses that only group aren't a step: `((x))` counts what `x` does.
- A number written with its sign, `-5`, is one operand. `- 5` and `-x` are
  `-` and its operand, two steps.
- `!in`, `!includes`, `!startsWith` and `!endsWith` are one operator each.
- A path's names count whether or not the path leads anywhere:
  `inputs.missing.a.b` counts 32.
- What isn't evaluated counts nothing: the operand that `&&`, `||` and `??`
  don't need, the branch of `? :` that isn't taken, and of `case`'s
  arguments, the values it doesn't choose and the predicates after the one
  that holds.
- The text around `${{ }}` counts nothing.

**What its operations read.** Each operation in this table counts the size
of each value it reads, by the rule above, whatever the value's type. It
counts the whole size, before it reads anything, even when it could stop
early, as `==` could at the first byte two strings differ in.

| Operation | Reads |
| --- | --- |
| `==`, `!=`, `===`, `!==`, `<`, `>`, `<=` and `>=` | Both operands |
| `contains`, `in`, `includes`, `startsWith` and `endsWith`, the functions and the word operators | Both operands |
| `length` | A string; a list's or mapping's length reads nothing |
| `trim`, `upper` and `lower` | The operand |
| `keys` and `values` | The operand |
| `toJSON` | The value |
| `fromJSON` | The text, or the value, which it gives back, if it isn't a string |
| `format` | The format string and each argument |
| `join`, the function and the word operator | The list or string, and the separator: `','` when it isn't given |
| `split` | The string and the separator |
| A list or mapping written as JSON | Itself, each time it is evaluated |
| A name or index of a path, and `get`, that read into a string | The string, for a character of it or a key of the JSON it holds |
| `get` | Its key, if it is text |
| Arithmetic, `-`, `abs`, `floor`, `ceil`, `round` and `range` | A string they convert to a number |
| `map`, `filter` and `.*` | A string they go through, which holds the items as JSON; a mapping's keys, which they go through in order: 8 for each and its bytes |

Every other operation reads nothing, however large its operands: reading a
reference, a path through lists and mappings, `get` of them by a number,
arithmetic on numbers, and `!` count their steps only, and so do `map`,
`filter` and `.*` going through a list, besides each item.

**What it builds,** as counted above, counts as work too.

So `inputs.name == 'admin'` counts 8 for `==`, 16 for the reference, 8 for
`'admin'`, and the bytes of both strings: 41 for a name of 4 bytes.
`filter(range 10, i => i > 4)` counts 8 for `filter`; 16 for `range` and
`10`, and 160 for the numbers it makes; 48 for each of the 10 runs, which is
8, `i > 4` 24, and its two numbers 16; and 80 for the five numbers `filter`
keeps: 744.

Meridian bounds what it evaluates together as well, such as all of a run's
input defaults, and how long that takes, but those bounds are its own and
not part of the language: [the design](design.md#the-language) describes
them.

## Values

Values are JSON's: null, booleans, numbers, strings, lists and mappings.
Numbers are 64-bit floats, as in JavaScript. `expr/value.go` has the rules;
in short:

- **Truthiness.** `false`, `0`, NaN, `""` and `null` are false. Everything
  else is true, including `"false"`, `"0"`, `[]` and `{}`. Conditions, `!`,
  `||`, `&&`, `? :` and `filter` go by it.
- **To a number**, for arithmetic, for ordering, and for some of `==`'s
  comparisons: `null` is 0, `true` 1 and `false` 0. A string is read as
  JavaScript's `Number()` reads it:
  - The space JavaScript ignores around it is removed: spaces, tabs, line
    breaks, no-break spaces and the other Unicode spaces. A string that is
    then empty is 0.
  - A decimal has an optional sign, digits with an optional fraction, or a
    fraction alone, and an optional exponent: `"017"` is 17, `".5"` 0.5,
    `"5."` 5 and `"-2.5E-3"` -0.0025. One beyond a 64-bit float's range is
    an infinity: `"1e400"` is Infinity.
  - `"0x"`, `"0o"` and `"0b"`, in either case, then hexadecimal, octal or
    binary digits, are whole numbers, without a sign: `"0xff"` is 255, and
    `"-0x10"` is NaN.
  - `"Infinity"`, `"+Infinity"` and `"-Infinity"`, in that case, are
    infinities.
  - Anything else is NaN, including `"1_000"`, `"0x1p4"`, `"inf"` and
    `"NaN"`.

  A list or a mapping is NaN.
- **To a string**, for template text, for `+` with a string, and for the
  string operators and functions: `null` is `""`, as GitHub Actions makes
  it, a number is written as JavaScript's `String(n)` writes it, and a list
  or mapping is its JSON on one line, with keys in sorted order. So
  `'a' + null` is `"a"`, `upper null` is `""`, and `["a", null] join "-"`
  is `"a-"`. In JSON, such as `toJSON(x)`, `null` stays `null`.
- **`in`, `includes` and `contains`** look for a substring in a string, an
  item in a list and a key in a mapping. In a string, what they look for is
  its text: `'a1' includes 1` is true, and `null` is `""`, which every string
  has, so `'abc' includes null` is true. In a list, an item matches as `==`
  does, so `null` matches only `null`.
- **Each word operator gives what its function gives** for the same
  arguments: `in` and `includes` are `contains`, the one with its arguments
  reversed, and `startsWith`, `endsWith` and `join` are the functions of
  those names. So `'ab' join ','` is `"ab"`, as `join('ab', ',')` is.
- **`+`** joins strings if either side is a string, and otherwise adds
  numbers. `-`, `*`, `/`, `//` (division rounded down), `%` and `mod` (the
  remainder, with the sign of the left side) and `**`, `^` and `exp` convert
  both sides to numbers.
- **`==` and `!=`**: `null` equals only `null`. A number and a string or a
  boolean, or a boolean and a string, are compared as numbers. Strings are
  compared byte for byte, so case matters. Lists and mappings are equal when
  their contents are. **`===` and `!==`** don't convert: values of different
  types are never equal.
- **`<`, `<=`, `>` and `>=`** compare two strings byte for byte, and anything
  else as numbers. NaN makes each of them false.
- **`round`** rounds as JavaScript's `Math.round` does, halves up toward
  Infinity: `round 2.5` is 3, and `round -2.5` is -2.
- **Strings that hold JSON.** Outputs and environment variables are strings,
  so a path, `get`, `length`, `keys`, `values` and `join` read a string that
  holds a JSON list or mapping as its value. `join` gives any other string
  as it is.
- **Slices** count from the start, or from the end when negative, and are
  clamped to the list or string: `"hello" get 1:4` is `ell`. A string's
  characters are Unicode code points.
