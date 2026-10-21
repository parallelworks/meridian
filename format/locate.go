package format

import (
	"strings"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"

	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/problems"
)

// Locate returns where in the file a place in a string value is: offset is
// a byte offset in the string at a JSON Pointer. It is exact for a string
// written on one line, plain or in quotes without escapes, and for a literal
// block (|). For other strings, such as a folded block (>) or one with
// escapes, it returns where the string starts.
func (d *Document) Locate(pointer string, offset int) problems.Position {
	return d.Locator(pointer).Locate(offset)
}

// A Locator finds places in one string of a Document, as Locate does. It
// finds where the string is written once, and each place from the last one
// it found, so that finding a string's places in order takes time in
// proportion to the string's length, however many there are.
type Locator struct {
	start problems.Position
	value string
	// style is the string's style, which Locate is exact for when ok, and
	// column is the column of its first character on one line.
	style  yaml.Style
	ok     bool
	column int
	lines  []string
	// read is how far the string has been read, and line the line of a
	// literal block whose source was checked last.
	read reader
	line blockLine
}

// A reader reads a string up to an offset, and counts what is before it:
// its runes and newlines, and where its line starts. It reads on from where
// it got to, or from the start for an offset before that.
type reader struct {
	at, runes, newlines int
	// lineStart is the offset of the start of at's line, and lineRunes the
	// runes before it.
	lineStart, lineRunes int
}

// readTo reads s up to offset, and returns the runes before it. It stops
// at the start of the rune that offset is in, so that reading on counts
// the runes that reading from the start would.
func (r *reader) readTo(s string, offset int) int {
	if offset < r.at {
		*r = reader{}
	}
	stop := offset
	for stop > r.at && stop < len(s) && !utf8.RuneStart(s[stop]) {
		stop--
	}
	part := s[r.at:stop]
	if i := strings.LastIndexByte(part, '\n'); i >= 0 {
		r.newlines += strings.Count(part, "\n")
		r.lineStart = r.at + i + 1
		r.lineRunes = r.runes + utf8.RuneCountInString(part[:i+1])
	}
	r.runes += utf8.RuneCountInString(part)
	r.at = stop
	return r.runes + utf8.RuneCountInString(s[stop:offset])
}

// A blockLine is a line of a literal block, by its number in the file, and
// whether it is the block's line as the file writes it, after an indent of
// so many runes.
type blockLine struct {
	no, indent int
	ok         bool
}

// Locator returns a Locator for the string at a JSON Pointer.
func (d *Document) Locator(pointer string) *Locator {
	l := &Locator{start: d.positions[pointer], lines: d.lines}
	style, ok := d.styles[pointer]
	if !ok || l.start.Line == 0 || l.start.Line > len(d.lines) {
		return l
	}
	tokens, err := jsonptr.Split(pointer)
	if err != nil {
		return l
	}
	v, _ := jsonptr.Lookup(d.Value, tokens)
	l.value, _ = v.(string)
	l.style = style
	switch style {
	case 0, yaml.SingleQuotedStyle, yaml.DoubleQuotedStyle:
		if strings.Contains(l.value, "\n") {
			return l
		}
		quote := 0
		if style != 0 {
			quote = 1
		}
		line := d.lines[l.start.Line-1]
		at := byteOffset(line, l.start.Column-1+quote)
		l.ok = strings.HasPrefix(line[at:], l.value)
		l.column = l.start.Column + quote
	case yaml.LiteralStyle:
		l.ok = true
	}
	return l
}

// Locate returns where in the file the byte at offset in the string is.
func (l *Locator) Locate(offset int) problems.Position {
	if !l.ok || offset < 0 || offset > len(l.value) {
		return l.start
	}
	runes := l.read.readTo(l.value, offset)
	if l.style != yaml.LiteralStyle {
		return problems.Position{Line: l.start.Line, Column: l.column + runes}
	}
	// Each line of a literal block is a line of the file, after the
	// block's indentation.
	lineNo := l.start.Line + 1 + l.read.newlines
	if lineNo > len(l.lines) {
		return l.start
	}
	if l.line.no != lineNo {
		lineStart := l.read.lineStart
		lineEnd := len(l.value)
		if i := strings.IndexByte(l.value[lineStart:], '\n'); i >= 0 {
			lineEnd = lineStart + i
		}
		src := strings.TrimSuffix(l.lines[lineNo-1], "\r")
		indent := len(src) - (lineEnd - lineStart)
		l.line = blockLine{no: lineNo}
		if indent >= 0 && src[indent:] == l.value[lineStart:lineEnd] {
			l.line.ok, l.line.indent = true, utf8.RuneCountInString(src[:indent])
		}
	}
	if !l.line.ok {
		return l.start
	}
	return problems.Position{Line: lineNo, Column: 1 + l.line.indent + runes - l.read.lineRunes}
}

// byteOffset returns the byte offset of the rune at index i in s.
func byteOffset(s string, i int) int {
	for at := range s {
		if i == 0 {
			return at
		}
		i--
	}
	return len(s)
}
