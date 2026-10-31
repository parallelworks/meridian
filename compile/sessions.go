package compile

import (
	"slices"

	"github.com/parallelworks/meridian/problems"
)

// redirected returns the keys of the sessions whose redirect: is true, as
// format 1 reads it, a boolean, in order: an expression's text isn't true,
// and is read once Start evaluates it.
func redirected(sessions map[string]any) []string {
	var keys []string
	for key, v := range sessions {
		if s, ok := v.(map[string]any); ok && s["redirect"] == true {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// checkRedirects reports more than one session whose redirect: is true,
// which format 1 refuses as the run is created, as it can take the person
// who runs the workflow to one session only (session_redirect_twice), at
// the redirect: of the second, in order. A redirect: an expression gives is
// checked as Start evaluates it (starter.sessions).
func (c *compiler) checkRedirects() {
	if keys := redirected(mapping(c.doc.Value["sessions"])); len(keys) > 1 {
		c.reportAt(problems.SessionRedirectTwice, []string{"sessions", keys[1], "redirect"}, problems.Params{"keys": problems.QuoteAll(keys)})
	}
}
