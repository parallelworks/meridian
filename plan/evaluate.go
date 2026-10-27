package plan

import (
	"crypto/sha256"
	"encoding/binary"
	jsonv2 "encoding/json/v2"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/parallelworks/meridian/expr"
	"github.com/parallelworks/meridian/internal/jsonptr"
	"github.com/parallelworks/meridian/internal/jsonvalue"
	"github.com/parallelworks/meridian/internal/words"
	"github.com/parallelworks/meridian/problems"
)

// An evaluation evaluates what a phase leaves to a job or a step, each
// value in its scope as a group of its own, and gathers the problems it
// finds and the text of what it made from a secret.
type evaluation struct {
	phase Phase
	scope expr.Scope
	diags problems.Diagnostics
	// masked holds the text of each value made from a secret, once.
	masked []string
	seen   map[string]bool
	// masker, if set, masks the params of what is reported: on the worker,
	// where a step's outputs are read as they were written, which can hold
	// a secret's value. It masks what masked holds too.
	masker *Masker
	// secrets are the texts of the run's secrets the phase reads, for mask.
	secrets *secretTexts
	// read is what the values evaluated read, together (reading).
	read expr.Read
}

func newEvaluation(phase Phase, c Contexts) *evaluation {
	return &evaluation{phase: phase, scope: NewScope(phase, c), secrets: &secretTexts{run: c.Run, needs: c.Needs}}
}

// next returns an evaluation in phase of c, which keeps e's problems and
// what e noted to mask.
func (e *evaluation) next(phase Phase, c Contexts) *evaluation {
	return &evaluation{
		phase: phase, scope: NewScope(phase, c), diags: e.diags, masked: e.masked, seen: e.seen, masker: e.masker, secrets: e.secrets,
	}
}

// secretTexts are the secrets' own texts a phase knows of, the run's
// (RunValues.Masked), those the worker knows of that don't come from the
// run, other (JobState.Secrets), and those of the outputs of the jobs it
// needs that hold one (JobResult.Secrets), what they hold that mask notes
// though it is short, the fields of a structured secret that a credential
// is kept in (credentialFields), and what a Masker is given of them, found
// the first time they are needed. A job's are the same for each phase of
// an attempt, so JobEnv keeps them for its steps and End (JobEnv.own),
// which find them, and build what masks them, once.
type secretTexts struct {
	run   RunValues
	other []string
	needs map[string]JobResult
	// mu guards what follows, as an attempt's phases can share them.
	mu    sync.Mutex
	found bool
	// own are the own texts, fields their credential fields, masked what a
	// Masker is given of the run's secrets and the needs' outputs,
	// fromNeeds what of it the needs' outputs give (needsSecrets), and
	// fromOther what other gives: its texts and their credential fields.
	own       []string
	fields    map[string]bool
	masked    []string
	fromNeeds []string
	fromOther []string
	// leak masks own and fields as they show but for their lines
	// (newLeakMasker), built the first time it is needed.
	leak *Masker
	// sum is what they were found from (secretsSum).
	sum [sha256.Size]byte
	// checks tell which of the job's steps' outputs hold a secret, for
	// each phase that shares them.
	checks outputChecks
}

// secretsSum returns a digest of what a job's secretTexts are found from:
// the names of the run's secrets, which of them are derived, and their
// values' strings; the texts of secrets that don't come from the run,
// other; and the names of the outputs, and steps' outputs, of the jobs in
// needs that hold a secret, which of them are derived, and their values.
// JobEnv's are used for a Step, Cleanup or End only when theirs is the
// same, as for another run or attempt it isn't.
func secretsSum(run RunValues, other []string, needs map[string]JobResult) [sha256.Size]byte {
	h := sha256.New()
	write := func(parts ...string) {
		for _, p := range parts {
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], uint64(len(p)))
			h.Write(n[:])
			h.Write([]byte(p))
		}
		h.Write([]byte{0})
	}
	write(run.Secrets...)
	write(run.Derived...)
	own, derived := run.secretStrings()
	write(own...)
	write(derived...)
	write(other...)
	for name, r := range jsonvalue.Sorted(needs) {
		write(name)
		write(r.Secrets...)
		write(r.Derived...)
		for id, names := range jsonvalue.Sorted(r.SecretSteps) {
			write(id)
			write(names...)
			write(r.DerivedSteps[id]...)
		}
	}
	needsOwn, needsDerived := needsSecrets(needs)
	write(needsOwn...)
	write(needsDerived...)
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

// find finds the secrets' own texts, their credential fields, and what a
// Masker is given of them.
func (t *secretTexts) find() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.found {
		return
	}
	t.found = true
	t.sum = secretsSum(t.run, t.other, t.needs)
	runOwn, runDerived := t.run.secretStrings()
	// The worker's own secrets are as the run's.
	other := slices.DeleteFunc(slices.Clone(t.other), func(s string) bool { return s == "" })
	t.fromOther = slices.Concat(other, slices.Sorted(maps.Keys(credentialFields(other))))
	runOwn = slices.Concat(runOwn, other)
	needsOwn, needsDerived := needsSecrets(t.needs)
	needsFields := credentialFields(needsOwn)
	t.own = slices.Concat(runOwn, needsOwn)
	t.fields = credentialFields(runOwn)
	maps.Copy(t.fields, needsFields)
	t.masked = maskedFrom(t.own, slices.Concat(runDerived, needsDerived), t.fields)
	t.fromNeeds = slices.Concat(needsOwn, slices.Sorted(maps.Keys(needsFields)))
	for _, s := range needsDerived {
		if len(s) >= minDerived || t.fields[s] {
			t.fromNeeds = append(t.fromNeeds, s)
		}
	}
}

// allMasked returns what a Masker is given of the run's secrets and of the
// outputs of the jobs it needs that hold one, as RunValues.Masked gives a
// run's.
func (t *secretTexts) allMasked() []string {
	t.find()
	return t.masked
}

// needsMasked returns what a Masker is given of the outputs of the jobs it
// needs that hold a secret: one that holds a secret's own text however
// short, as its credential fields are, and one only made from one as what
// is made from a secret is (needsSecrets).
func (t *secretTexts) needsMasked() []string {
	t.find()
	return t.fromNeeds
}

// otherMasked returns what a Masker is given of the secrets that don't
// come from the run (JobState.Secrets): their texts, and the credential
// fields they hold, however short.
func (t *secretTexts) otherMasked() []string {
	t.find()
	return t.fromOther
}

// sameAs reports whether t was found from the secrets that run, other and
// needs hold (secretsSum).
func (t *secretTexts) sameAs(run RunValues, other []string, needs map[string]JobResult) bool {
	t.find()
	return t.sum == secretsSum(run, other, needs)
}

// field reports whether s is one of the secrets' credential fields.
func (t *secretTexts) field(s string) bool {
	t.find()
	return t.fields[s]
}

// leakMasker returns a Masker of the secrets' own texts and their
// credential fields as they show but for their lines, which masks what
// holds one, built once: what End tells an output that holds a secret's
// own text with, and what the other Maskers of an attempt extend
// (Masker.extend).
func (t *secretTexts) leakMasker() *Masker {
	t.find()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.leak == nil {
		t.leak = newLeakMasker(slices.Concat(t.own, slices.Sorted(maps.Keys(t.fields)))...)
	}
	return t.leak
}

// credentialFields returns what texts hold, read as JSON, under a key a
// credential is kept under (credentialKey): each string, and each number's
// text, of at least minField bytes, but true, false and null, that is the
// key's value or an item of a list that is, but not what a mapping under
// it holds, which has keys of its own, but for a section's
// (credentialSection), whose own values are. A key a plural credential word
// decides, such as tokens, holds credentials only as a list's items: its
// value alone is a setting. So {"user":"svc","password":"hunt3r2"} holds
// hunt3r2, {"pin":1234} 1234, {"tokens":["tok1"]} tok1 and
// {"secrets":{"db":"hunt3r"}} hunt3r, and {"token":{"type":"bearer"}},
// {"tokens":"none"}, {"secrets":"inherit"} and
// {"secrets":{"db":{"file":"./db.txt"}}} nothing. A text that isn't a JSON
// object or list holds none.
func credentialFields(texts []string) map[string]bool {
	fields := map[string]bool{}
	add := func(v any) bool {
		var text string
		switch v := v.(type) {
		case string:
			text = v
		case float64:
			text = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			return false
		}
		if len(text) >= minField && !jsonWord(text) {
			fields[text] = true
		}
		return true
	}
	leaf := func(v any) bool {
		switch v.(type) {
		case string, float64, bool, nil:
			return true
		}
		return false
	}
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, item := range v {
				if section, isMap := item.(map[string]any); isMap && credentialSection(k) {
					// A section's values are credentials, one level down.
					for _, x := range section {
						if !leaf(x) {
							walk(x)
						} else {
							add(x)
						}
					}
					continue
				}
				credential, isPlural := credentialKey(k)
				if items, isList := item.([]any); isList {
					for _, x := range items {
						if !leaf(x) {
							walk(x)
						} else if credential {
							add(x)
						}
					}
					continue
				}
				if !leaf(item) {
					walk(item)
				} else if credential && !isPlural {
					add(item)
				}
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		}
	}
	for _, text := range texts {
		if t := strings.TrimSpace(text); t == "" || t[0] != '{' && t[0] != '[' {
			continue
		}
		var v any
		if jsonv2.Unmarshal([]byte(text), &v) == nil {
			walk(v)
		}
	}
	return fields
}

// credentialSection reports whether key names a section of credentials,
// whose values, one level down, are credentials when it is a mapping, as
// {"secrets":{"github":"ghp_…"}} and {"credentials":{"value":"…"}} are:
// its last word is secrets, credentials or creds.
func credentialSection(key string) bool {
	words := words.Split(key)
	if len(words) == 0 {
		return false
	}
	switch words[len(words)-1] {
	case "secrets", "credentials", "creds":
		return true
	}
	return false
}

// credentialWords are the words of a key that a credential is kept under,
// and plural those of them that name a list of them. secrets isn't one: it
// names a section, as docker-compose's and GitHub Actions' do, which holds
// the secrets' names or settings.
var (
	credentialWords = map[string]bool{
		"password": true, "passwd": true, "pass": true, "passphrase": true, "passcode": true, "pwd": true, "pw": true,
		"secret": true, "token": true, "pin": true, "credential": true, "cred": true,
		"passwords": true, "tokens": true, "credentials": true, "creds": true,
	}
	plural = map[string]bool{"passwords": true, "tokens": true, "credentials": true, "creds": true, "keys": true}
)

// keyQualifiers are the words before key, or keys, that make a key a
// credential is kept under, as api_key and privateKey are, where key alone,
// as an object store's Key or a sortKey, names something else.
var keyQualifiers = map[string]bool{
	"api": true, "private": true, "secret": true, "access": true, "client": true, "signing": true,
	"encryption": true, "master": true, "ssh": true, "account": true, "service": true,
}

// credentialHeads are what a key written in lower case without breaks
// starts with before a credential word, as apikey and clientsecret do.
var credentialHeads = map[string]bool{
	"access": true, "account": true, "admin": true, "api": true, "app": true, "auth": true, "bearer": true,
	"client": true, "db": true, "encryption": true, "id": true, "master": true, "oauth": true, "private": true,
	"refresh": true, "root": true, "secret": true, "service": true, "session": true, "signing": true,
	"ssh": true, "user": true,
}

// keyDescriptors are the last words of a key that names something about a
// credential rather than holding one, as keyFile, tokenUrl and secret_name
// do.
var keyDescriptors = map[string]bool{
	"name": true, "id": true, "type": true, "url": true, "uri": true, "endpoint": true, "path": true, "file": true,
	"alias": true, "prefix": true, "policy": true, "length": true, "version": true, "schema": true, "region": true,
	"arn": true, "format": true, "algorithm": true, "expiry": true, "ttl": true, "scope": true, "provider": true,
	"count": true,
}

// credentialKey reports whether key is one a credential is kept under, and
// whether a plural word decides it, as credentialFields reads it. Its
// words (words.Split), whatever their case, decide it: it isn't one if its
// last word is a descriptor (keyDescriptors), as keyFile, tokenUrl and
// token_count are, nor if its last word is key or keys without a
// qualifier before it (keyQualifiers), as key, sortKey and token-key are,
// where api_key and privateKey are; it is one if a word is a credential
// word, as password, accessToken and password2 are, or key or keys after
// a qualifier; and a word written without breaks is one if it is a
// credential word after a head (credentialHeads), or key or keys after a
// qualifier, as apikey and clientsecret are, and keyboard, monkey and
// passport aren't. A plural word decides it only if no singular one does,
// so credentials/db_password holds one password, as db_password does.
func credentialKey(key string) (credential, isPlural bool) {
	words := words.Split(key)
	n := len(words)
	if n == 0 || keyDescriptors[words[n-1]] || keyDescriptors[strings.TrimSuffix(words[n-1], "s")] {
		return false, false
	}
	keyWord := func(w string) bool { return w == "key" || w == "keys" }
	if keyWord(words[n-1]) && (n < 2 || !keyQualifiers[words[n-2]]) {
		// key or keys last is one only after a qualifier: api_key is, and
		// key, sortKey and token-key aren't.
		return false, false
	}
	singular := false
	note := func(w string) {
		credential = true
		singular = singular || !plural[w]
	}
	for i, w := range words {
		switch {
		case credentialWords[w]:
			note(w)
			continue
		case keyWord(w) && i > 0 && keyQualifiers[words[i-1]]:
			note(w)
			continue
		}
		for c := range credentialWords {
			if head, ok := strings.CutSuffix(w, c); ok && credentialHeads[head] {
				note(c)
			}
		}
		for _, c := range []string{"key", "keys"} {
			if head, ok := strings.CutSuffix(w, c); ok && keyQualifiers[head] {
				note(c)
			}
		}
	}
	return credential, credential && !singular
}

// jsonWord reports whether s is true, false or null, which a secret
// deciding masks nowhere.
func jsonWord(s string) bool { return s == "true" || s == "false" || s == "null" }

// value evaluates v, at pointer in the workflow, and notes the strings of
// what each of its templates that read a secret gives, and each expression
// in one that read one, which are to be masked (mask): not those of the
// rest of v, nor the text of a boolean, a number or null (Masker). ok is
// false, and the problem reported, if it fails.
func (e *evaluation) value(v Value, pointer string) (out any, read expr.Read, ok bool) {
	return e.valuePlaced(v, pointer, nil)
}

// valuePlaced evaluates v as value does, and calls placed, if it isn't
// nil, with the path in v of each of its templates, and what it read.
func (e *evaluation) valuePlaced(v Value, pointer string, placed func(path []string, read expr.Read)) (out any, read expr.Read, ok bool) {
	out, read, err := v.evalLeaves(e.scope, func(out any, read expr.Read) {
		if read.Marked {
			maskedStrings(out, e.mask)
		}
	}, placed)
	e.read = e.read.Or(read)
	if err != nil {
		e.reportError(pointer, err, read)
		return nil, read, false
	}
	return out, read, true
}

// reading calls f, and returns what the values it evaluated read, together.
func (e *evaluation) reading(f func()) expr.Read {
	before := e.read
	e.read = expr.Read{}
	f()
	read := e.read
	e.read = before.Or(read)
	return read
}

// text evaluates v as value does, and gives its text, as a template makes
// text of a value (expr.Text).
func (e *evaluation) text(v Value, pointer string) (string, expr.Read, bool) {
	out, read, ok := e.value(v, pointer)
	return expr.Text(out), read, ok
}

// minDerived is how long a string made from a secret must be for a phase
// to give it to mask: a shorter one, such as yes, would mask common text
// wherever it appears. A secret's own text is masked however short, and an
// output made from one is private and a secret (End). minField is how
// long a shorter one must be that a structured secret keeps a credential
// in, such as a password that fromJSON took from a secret's JSON
// (credentialFields).
const (
	minDerived = 8
	minField   = 4
)

// mask notes s, text made from a secret, if it is at least minDerived
// bytes long, or a credential field of a structured secret
// (credentialFields).
func (e *evaluation) mask(s string) {
	if e.masks(s) {
		e.maskOwn(s)
	}
}

// maskOwn notes s, a secret's own text, which is masked however short,
// but for "".
func (e *evaluation) maskOwn(s string) {
	if s == "" || e.seen[s] {
		return
	}
	if e.seen == nil {
		e.seen = map[string]bool{}
	}
	e.seen[s] = true
	e.masked = append(e.masked, s)
	if e.masker != nil {
		e.masker.Add(s)
	}
}

// masks reports whether mask notes s.
func (e *evaluation) masks(s string) bool {
	switch {
	case len(s) >= minDerived:
		return true
	case len(s) < minField || jsonWord(s):
		return false
	}
	return e.secrets != nil && e.secrets.field(s)
}

// A decision is what an if: decided.
type decision struct {
	// run is whether what the if: is on runs, and always whether its value
	// is the word always, the older spelling of always(), which format 1
	// reads more leniently (Contexts.StrictOutputs).
	run, always bool
	// outlivesCancel is whether it can be true once its job is stopping:
	// always or its word, cancelled(), or a negated success() or failure(),
	// as format 1 reads it.
	outlivesCancel bool
}

// condition evaluates an if:, v, at pointer, and decides whether what it is
// on runs, with the status functions as the scope's Status says: the
// default if:, success(), when v is unset; an if: whose value is the word
// always, always(); one that calls a status function, its value's
// truthiness; and any other, its truthiness and success(), whose place it
// doesn't take. ok is false if it fails.
func (e *evaluation) condition(v Value, pointer string) (d decision, ok bool) {
	return e.decide(v, pointer, true)
}

// cleanupCondition evaluates the if: of a step whose cleanup: is about to
// run, v, at pointer, as condition does, but without success(): the
// cleanup runs when the if: is unset, the word always, or true, whatever
// happened to the job, as format 1 evaluates its copy of the step's if:
// with the job's status as it ends. ok is false if it fails.
func (e *evaluation) cleanupCondition(v Value, pointer string) (d decision, ok bool) {
	return e.decide(v, pointer, false)
}

// decide is condition, which takes success() into account when gated is
// set, and cleanupCondition, which doesn't.
func (e *evaluation) decide(v Value, pointer string, gated bool) (d decision, ok bool) {
	status := e.scope.Status
	success := !gated || !status.Failed && !status.Cancelled
	if v.IsZero() {
		return decision{run: success}, true
	}
	out, read, err := v.EvalRead(e.scope)
	if err != nil {
		e.reportError(pointer, err, read)
		return decision{}, false
	}
	if s, isString := out.(string); isString && s == "always" {
		return decision{run: true, always: true, outlivesCancel: true}, true
	}
	used, _ := statusChecks(v)
	return decision{run: expr.Truthy(out) && (success || used), outlivesCancel: OutlivesCancel(v)}, true
}

// OutlivesCancel reports whether an if:, cond, can be true once its job is
// stopping, by its form, as Step decides a step's
// (ReadyStep.OutlivesCancel), and its later tries take: the word always,
// or an expression that calls always() or cancelled(), or negates
// success() or failure() (expr.Expr.StatusChecks). The coordinator asks
// it of a step's plan, rather than take a worker's word for it, as a
// step's child is asked for (coordinator.CreateChild). Of an if: whose
// value is the word always only once it is evaluated, as one that reads
// an input that holds it, only Step can tell.
func OutlivesCancel(cond Value) bool {
	if lit, ok := cond.Literal(); ok {
		if s, isString := lit.(string); isString && s == "always" {
			return true
		}
	}
	_, afterCancel := statusChecks(cond)
	return afterCancel
}

// statusChecks reports whether one of v's expressions calls a status
// function, and whether one can be true once its job is stopping
// (expr.Expr.StatusChecks).
func statusChecks(v Value) (used, afterCancel bool) {
	for _, e := range v.Exprs() {
		u, a := e.StatusChecks()
		used, afterCancel = used || u, afterCancel || a
	}
	return used, afterCancel
}

// readsContext reports whether one of v's expressions reads context.
func readsContext(v Value, context string) bool {
	for _, e := range v.Exprs() {
		for _, r := range e.Refs() {
			if r.Context == context {
				return true
			}
		}
	}
	return false
}

// report reports a problem of kind k with what is at pointer, with the
// params that show a value masked if read says it is private
// (problems.MaskValues). On the worker, the text of each param is masked
// as the job's logs are too, after its value is shown, as a last guard: a
// value read that holds a secret is marked as one, and one that the phase
// shows itself is masked before it is shown (reportInvalid), since what
// shows a value can quote it, escape it or cut it short.
func (e *evaluation) report(k *problems.Kind, pointer string, params problems.Params, read expr.Read) {
	if read.Private {
		params = problems.MaskValues(params)
	}
	if e.masker != nil {
		masked := make(problems.Params, len(params))
		for name, v := range params {
			if s, ok := v.(string); ok {
				v = e.masker.Mask(s)
			}
			masked[name] = v
		}
		params = masked
	}
	e.diags = append(e.diags, k.At("", problems.Position{}, pointer, params))
}

// reportInvalid reports a value, v, that doesn't fit its key: a problem of
// kind k, with v as its param value, and key, if it isn't "", as its param
// key.
func (e *evaluation) reportInvalid(k *problems.Kind, pointer, key string, v any, read expr.Read) {
	if e.masker != nil {
		v = maskStrings(v, e.masker)
	}
	params := problems.Params{"value": problems.Show(v)}
	if key != "" {
		params["key"] = key
	}
	e.report(k, pointer, params, read)
}

// reportType reports a value, v, at pointer that isn't of one of the types
// the format takes there, by their names in the schema, such as string.
func (e *evaluation) reportType(pointer string, v any, read expr.Read, types ...string) {
	params := problems.Params{"count": len(types), "has_got": true, "got": typeName(v)}
	for i, t := range types {
		params["type"+strconv.Itoa(i+1)] = t
	}
	// The type of a value says nothing of it, so it isn't masked.
	e.report(problems.WrongType, pointer, params, expr.Read{})
}

// typeName returns the name in a schema of v's type.
func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case int64, int, float64:
		return "number"
	case []any:
		return "array"
	}
	return "object"
}

// reportError reports err, an error evaluating the value at pointer, which
// read what read says: as its problem, with the values it shows masked if
// the evaluation read a private value.
func (e *evaluation) reportError(pointer string, err error, read expr.Read) {
	if ee, ok := errors.AsType[*expr.EvalError](err); ok {
		e.report(ee.Kind, pointer, ee.Params, read)
		return
	}
	if se, ok := errors.AsType[*expr.SyntaxError](err); ok {
		e.report(se.Kind, pointer, se.Params, expr.Read{})
		return
	}
	if be, ok := errors.AsType[*expr.BudgetError](err); ok {
		e.report(be.Kind, pointer, problems.Params{"limit": be.Limit}, expr.Read{})
		return
	}
	if ue, ok := errors.AsType[*expr.UnknownError](err); ok {
		if ue.Name == "status" {
			e.report(problems.StatusNotKnown, pointer, problems.Params{"phase": e.phase.String()}, expr.Read{})
			return
		}
		e.report(problems.ContextNotKnown, pointer, problems.Params{"context": ue.Name, "phase": e.phase.String()}, expr.Read{})
		return
	}
	if xe, ok := errors.AsType[*ExpandError](err); ok {
		e.report(xe.Kind, xe.Path, xe.Params, expr.Read{})
		return
	}
	e.report(problems.ExpressionFailed, pointer, problems.Params{"reason": err.Error()}, read)
}

// maskStrings returns v, JSON values, with each string in it masked by
// masker.
func maskStrings(v any, masker *Masker) any {
	switch v := v.(type) {
	case string:
		return masker.Mask(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = maskStrings(item, masker)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[masker.Mask(k)] = maskStrings(item, masker)
		}
		return out
	}
	return v
}

// at returns the JSON Pointer of keys under pointer.
func at(pointer string, keys ...string) string { return jsonptr.Append(pointer, keys...) }
