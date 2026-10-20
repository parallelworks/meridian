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

1. **A relative reference**: a word that starts with `.` and has more after
   it ([Relative references](#relative-references)). So `.5` reads the key
   `5`, not a number, and the compiler warns about a relative reference that
   starts with a digit: `0.5` is the number.
2. **A reference**: a context's name, then a `.` or `[` and more
   ([References](#references)).
3. **A number**: a word that is a number as a string is one
   ([Values](#values)), such as `42`, `2.5e-3`, `017` (17), `0xff`, `0o17` or
   `0b101`, and `1e400`, which is beyond a 64-bit float's range and so is
   Infinity. A `-` before a hexadecimal, octal or binary number negates it:
   `-0x10` is -16. `NaN` and `Infinity`, in that case, are numbers too, as
   JavaScript's globals are. `1_000`, `0x1p4`, `inf` and `nan` aren't
   numbers, so they are bare words.
4. **`true`, `false`, `null`**, and `undefined`, which is `null`, in any
   case.
5. **A bare word**, whose value is its text: `debug`, `ubuntu:22.04`, `12:30`,
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
  outside single or double quotes, without the whitespace around it. So
  `inputs.list[inputs.i]` reads the key `inputs.i`. To read a key that an
  expression computes, use `get`: `inputs.list get inputs.i`.
- A key in single or double quotes is the text between them, as it is.
  Backticks aren't quotes here. `[index]` is the index of the list item that
  an input's template is for. A whole number is a key or a list index. Any
  other key is a bare key, which reads like a name, except at the end of an
  `inputs` reference, where it reads the input's definition before its value:
  `inputs.x[hidden]`.
- `*` reads the rest of the path from each item of a list, or each value of a
  mapping in the order of its keys, and makes a list of what it reads. A
  further `*` flattens the lists: `inputs.people.*.tags.*`.
- A name or key with a `:` in it, such as `1:3`, `-2:` or `:2`, is a slice of
  a list or a string ([Values](#values)).

### Relative references

```
Relative = { "../" } { "." } path .   /* starts with "."; scanned as one word */
```

A relative reference reads the workflow around the expression. Each `.`
before the path goes up one level from the key the expression is in, so `.x`
is a key beside it and `..x` is a key beside its parent. Each `../` goes up a
level too, and `../x` is `..x`: `../../x` and `../../.x` both go up three
levels.

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
           | Reference | Relative | string | JSON | word .
Group      = "(" Expression ")" .
Slice      = ":" [ Group | word ] .         /* the word is a number: (start):5 */
Postfix    = { "." name | "[" key "]" } .
Call       = function "(" [ Expression { "," Expression } ] ")" Postfix .

prefix_op  = "length" | "len" | "upper" | "lower" | "trim" | "keys" | "values"
           | "abs" | "floor" | "ceil" | "round" | "fromJSON" .
function   = "contains" | "startsWith" | "endsWith" | "format" | "join" | "toJSON"
           | "case" | "success" | "failure" | "cancelled" | "canceled" | "always" .
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
  digit, or inside a number before its exponent's digits, is a sign.
- **`/` and `*`** are division and multiplication, except right after a `.`
  in a word: `../x` and `.*`. So `~/dir` is `~` divided by `dir`.
- **Prefix operators' names** are operators only before an operand.
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
  `!!…x`. A stored tree is decoded by the same rule.
- **`range`** takes whole numbers from -2^53 to 2^53, and makes at most
  10,000 of them.
- **What an evaluation builds.** An evaluation builds at most 8 MiB of strings
  and lists. A string counts its bytes, any other value 8 bytes, and a list or
  mapping 8 bytes for each item as well as its items and keys. The
  expressions of one template share the budget, and so do those the compiler
  evaluates together: all of a run's input defaults and conditions, or every
  expression in one value, such as a step's `with:`.

## Values

Values are JSON's: null, booleans, numbers, strings, lists and mappings.
Numbers are 64-bit floats, as in JavaScript. `expr/value.go` has the rules;
in short:

- **Truthiness.** `false`, `0`, NaN, `""` and `null` are false. Everything
  else is true, including `"false"`, `"0"`, `[]` and `{}`.
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
  item in a list and a key in a mapping. In a string, `null` is `""`, which
  every string has, so `'abc' includes null` is true. In a list, an item
  matches as `==` does, so `null` matches only `null`.
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
  holds a JSON list or mapping as its value.
- **Slices** count from the start, or from the end when negative, and are
  clamped to the list or string: `"hello" get 1:4` is `ell`. A string's
  characters are Unicode code points.
