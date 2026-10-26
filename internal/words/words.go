// Package words splits a name into its words, as both of Meridian's tests
// of a name read it: whether a key holds a credential, which the masker
// asks of a value's keys, and whether an input's name looks like a
// secret's, which the compiler asks of a workflow's inputs.
package words

import (
	"strings"
	"unicode"
)

// Split returns the words of name, in lower case. A word ends at anything
// that isn't a letter or a digit, such as _, -, ., / or a space, and where
// letters and digits meet. An upper-case letter after a lower-case one
// starts a word, as in apiKey, and so does the last of a run of them before
// a lower-case one, as in APIKey. So apiKey, API_KEY, APIKey and "API key"
// are api and key, and OAuth2Token is o, auth, 2 and token.
func Split(name string) []string {
	var words []string
	runes := []rune(name)
	start := -1
	end := func(i int) {
		if start >= 0 {
			words = append(words, strings.ToLower(string(runes[start:i])))
			start = -1
		}
	}
	for i, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			end(i)
			continue
		}
		if start >= 0 {
			prev := runes[i-1]
			if unicode.IsDigit(r) != unicode.IsDigit(prev) ||
				unicode.IsUpper(r) && unicode.IsLower(prev) ||
				unicode.IsUpper(r) && unicode.IsUpper(prev) && i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
				end(i)
			}
		}
		if start < 0 {
			start = i
		}
	}
	end(len(runes))
	return words
}
