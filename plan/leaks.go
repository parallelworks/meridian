package plan

import (
	"maps"
	"slices"
	"sync"
)

// outputChecks are what the phases of an attempt find of its steps'
// outputs, shared by each phase that shares the job's secrets' texts
// (secretTexts, JobEnv.own), so that a phase reads again only what it must
// of the outputs before it: the Masker that tells those that hold a
// secret, as addSelf tells them, which grows as the steps give more to
// mask, what was found of each output, and each step's context. What a
// phase finds with them is what it would find without them: they are used
// only while they mask nothing that the phase doesn't, and start again
// otherwise, and each output is compared with what was found of it.
type outputChecks struct {
	mu sync.Mutex
	// masker masks the secrets' own texts and their credential fields, as
	// their leak Masker does (secretTexts.leakMasker), what a Masker is
	// given of the secrets (secretTexts.allMasked), and values, the values
	// the phases gave it since it started, each as it shows but for its
	// lines; given are those the last phase gave it, in its order. It is
	// only added to: the texts each addition added to it are a batch of
	// added too.
	masker *Masker
	values map[string]bool
	given  []string
	added  []batch
	// all finds the masker's texts, once it is needed, until they change.
	all finder
	// outputs are what was found of each output, by where it was written,
	// and grams the hashes of the 4-byte substrings (gramsOf) of each
	// output read whole.
	outputs map[outputKey]outputCheck
	grams   []uint64
	// contexts are each step's context, by its index (stepsContext).
	contexts map[int]stepContext
}

// A batch is the texts one addition added to an outputChecks' masker, and
// a matcher of them. unseen is set when none of them is in an output read
// whole before they were added, as grams tell.
type batch struct {
	m      *matcher
	unseen bool
}

// An outputKey is where an output was written: by the step at index, or
// by its cleanup:, under name.
type outputKey struct {
	step    int
	cleanup bool
	name    string
}

// An outputCheck is what was found of an output, value, with the masker's
// texts as they were once its first checked batches were added: whether
// one of them is in it, and whether masking it changes it (Masker.Mask).
type outputCheck struct {
	value   string
	checked int
	found   bool
	leaked  bool
}

// A stepContext is a step's context, its outputs, which value holds a
// copy of.
type stepContext struct {
	value   map[string]string
	context map[string]any
}

// use makes c's masker mask the secrets' texts, from t, and values, and
// nothing else. c starts again if its masker masks a value that values
// doesn't hold. It keeps values, which the caller doesn't change. The
// caller holds c.mu.
func (c *outputChecks) use(t *secretTexts, values []string) {
	// The values a phase is given are most often those the one before was
	// given, and more after them.
	fresh := values
	if c.masker != nil {
		if n := len(c.given); n <= len(values) && slices.Equal(c.given, values[:n]) {
			fresh = values[n:]
		} else {
			given := setOf(values)
			for v := range c.values {
				if !given[v] {
					c.masker = nil
					break
				}
			}
		}
	}
	if c.masker == nil {
		c.masker, c.values, c.added, c.all, c.outputs = t.leakMasker().extend(false), map[string]bool{}, nil, nil, map[outputKey]outputCheck{}
		c.masker.Add(t.allMasked()...)
		fresh = values
	}
	c.given = values
	var texts []string
	for _, v := range fresh {
		if !c.values[v] {
			c.values[v] = true
			texts = append(texts, c.masker.add(v)...)
		}
	}
	if len(texts) > 0 {
		unseen := c.grams != nil
		for _, text := range texts {
			unseen = unseen && !c.mayHold(text)
		}
		c.added, c.all = append(c.added, batch{m: newMatcher(texts), unseen: unseen}), nil
	}
}

// finder returns what finds the masker's texts, which it builds only
// once it is needed, as a phase whose steps wrote nothing doesn't need
// it. The caller holds c.mu.
func (c *outputChecks) finder() finder {
	if c.all == nil {
		c.all = c.masker.current()
	}
	return c.all
}

// leaked reports whether masking v, the output written where key says,
// with what finds the masker's texts, changes it, as Masker.Mask does. An
// output it checked before that held none of the texts is read again only
// for the batches added since that an output read whole before them could
// hold. The caller holds c.mu.
func (c *outputChecks) leaked(key outputKey, v string) bool {
	ch, ok := c.outputs[key]
	if ok && ch.value == v {
		if ch.checked == len(c.added) {
			return ch.leaked
		}
		if !ch.found && !c.foundSince(ch.checked, v) {
			ch.checked = len(c.added)
			c.outputs[key] = ch
			return false
		}
	}
	spans, _ := c.finder().find(v)
	c.addGrams(v)
	ch = outputCheck{value: v, checked: len(c.added), found: len(spans) > 0, leaked: masked(v, spans) != v}
	c.outputs[key] = ch
	return ch.leaked
}

// foundSince reports whether a text of a batch from the nth on is in v,
// an output read whole before that batch was added.
func (c *outputChecks) foundSince(n int, v string) bool {
	for _, b := range c.added[n:] {
		if b.unseen {
			continue
		}
		if spans, _ := b.m.find(v); len(spans) > 0 {
			return true
		}
	}
	return false
}

// gramBits is the number of bits of a 4-byte substring's hash (gramsOf):
// grams has a bit for each hash.
const gramBits = 20

// gramsOf calls f with the hash of each 4-byte substring of s.
func gramsOf(s string, f func(h uint32)) {
	var g uint32
	for i := range len(s) {
		g = g>>8 | uint32(s[i])<<24
		if i >= 3 {
			f(g * 0x9e3779b1 >> (32 - gramBits))
		}
	}
}

// addGrams notes the 4-byte substrings of v, an output read whole.
func (c *outputChecks) addGrams(v string) {
	if c.grams == nil {
		c.grams = make([]uint64, 1<<gramBits/64)
	}
	gramsOf(v, func(h uint32) { c.grams[h/64] |= 1 << (h % 64) })
}

// mayHold reports whether an output read whole can hold text: whether
// text is shorter than 4 bytes, or each of its 4-byte substrings is one of
// theirs, as their hashes tell, which are the same for the same bytes.
func (c *outputChecks) mayHold(text string) bool {
	if len(text) < 4 {
		return true
	}
	holds := true
	gramsOf(text, func(h uint32) { holds = holds && c.grams[h/64]&(1<<(h%64)) != 0 })
	return holds
}

// stepContext returns the context of the step at index, whose outputs
// are outputs, as stepsContext gives it, which the phases share while the
// step's outputs are the same.
func (c *outputChecks) stepContext(index int, outputs map[string]string) map[string]any {
	if s, ok := c.contexts[index]; ok && maps.Equal(s.value, outputs) {
		return s.context
	}
	if c.contexts == nil {
		c.contexts = map[int]stepContext{}
	}
	s := stepContext{value: maps.Clone(outputs), context: map[string]any{"outputs": stringsContext(outputs)}}
	c.contexts[index] = s
	return s.context
}
