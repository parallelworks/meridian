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
	start := d.positions[pointer]
	style, ok := d.styles[pointer]
	if !ok || start.Line == 0 || start.Line > len(d.lines) {
		return start
	}
	tokens, err := jsonptr.Split(pointer)
	if err != nil {
		return start
	}
	v, _ := jsonptr.Lookup(d.Value, tokens)
	value, _ := v.(string)
	if offset < 0 || offset > len(value) {
		return start
	}
	switch style {
	case 0, yaml.SingleQuotedStyle, yaml.DoubleQuotedStyle:
		if strings.Contains(value, "\n") {
			return start
		}
		quote := 0
		if style != 0 {
			quote = 1
		}
		line := d.lines[start.Line-1]
		at := byteOffset(line, start.Column-1+quote)
		if !strings.HasPrefix(line[at:], value) {
			return start
		}
		return problems.Position{Line: start.Line, Column: start.Column + quote + utf8.RuneCountInString(value[:offset])}
	case yaml.LiteralStyle:
		// Each line of a literal block is a line of the file, after the
		// block's indentation.
		lineNo := start.Line + 1 + strings.Count(value[:offset], "\n")
		if lineNo > len(d.lines) {
			return start
		}
		lineStart := strings.LastIndexByte(value[:offset], '\n') + 1
		lineEnd := len(value)
		if i := strings.IndexByte(value[lineStart:], '\n'); i >= 0 {
			lineEnd = lineStart + i
		}
		src := strings.TrimSuffix(d.lines[lineNo-1], "\r")
		indent := len(src) - (lineEnd - lineStart)
		if indent < 0 || src[indent:] != value[lineStart:lineEnd] {
			return start
		}
		return problems.Position{Line: lineNo, Column: 1 + utf8.RuneCountInString(src[:indent]) + utf8.RuneCountInString(value[lineStart:offset])}
	}
	return start
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
