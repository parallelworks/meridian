package seal_test

import (
	"bytes"
	"crypto/fips140"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// placement is a purpose of parts beside a job's, as a coordinator's other
// parts are.
const placement seal.Purpose = "meridian/placement/1"

func jobRecord(v string) plan.JobRecord {
	return plan.JobRecord{Outputs: map[string]string{"token": v}, Steps: map[string]map[string]string{"login": {"pw": v}}}
}

// partKey is the key of a part, as SealPart says: derived from the run's
// data key with HKDF-SHA256, with no salt, and the part's associated data
// as its info.
func partKey(t *testing.T, dataKey []byte, info string) []byte {
	t.Helper()
	key, err := hkdf.Key(sha256.New, dataKey, nil, info, 32)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// A part opens as it was sealed, under a key of its own that the run's
// data key derives, as SealPart says, with its info as the associated
// data, and not under the data key itself, in FIPS 140-3 mode; sealing a
// part unwraps the data key once, and opening several does too; and each
// sealing has a nonce of its own.
func TestPartRoundTrip(t *testing.T) {
	if !fips140.Enforced() {
		t.Fatal("the tests don't run under GODEBUG=fips140=only")
	}
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	id := seal.PartID{Purpose: seal.PurposeJobRecord, ID: "build#2", Generation: 1}
	before := keys.unwraps
	a, err := s.SealPart(t.Context(), "42", sealed.Key, id, []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	if keys.unwraps != before+1 {
		t.Errorf("sealing a part unwrapped the data key %d times", keys.unwraps-before)
	}
	if bytes.Contains(a, []byte(secret)) || a[0] != 1 {
		t.Errorf("a sealed part is %x", a)
	}
	info := "meridian/job-record/1\x00store-1\x0042\x00build#2\x001"
	dataKey := keys.dataKeys()[0]
	if _, err := newAEAD(t, dataKey).Open(nil, nil, a[1:], []byte(info)); err == nil {
		t.Error("the part opens under the run's data key itself")
	}
	data, err := newAEAD(t, partKey(t, dataKey, info)).Open(nil, nil, a[1:], []byte(info))
	if err != nil || string(data) != secret {
		t.Fatalf("the part opens with its key and associated data as %q, %v", data, err)
	}

	other := seal.PartID{Purpose: placement, ID: "7f3a", Generation: 3}
	b, err := s.SealPart(t.Context(), "42", sealed.Key, other, []byte("member-9"))
	if err != nil {
		t.Fatal(err)
	}
	before = keys.unwraps
	got, err := s.OpenParts(t.Context(), "42", sealed.Key, []seal.SealedPart{{PartID: id, Sealed: a}, {PartID: other, Sealed: b}})
	if err != nil {
		t.Fatal(err)
	}
	if keys.unwraps != before+1 {
		t.Errorf("opening two parts unwrapped the data key %d times", keys.unwraps-before)
	}
	if len(got) != 2 || string(got[0]) != secret || string(got[1]) != "member-9" {
		t.Errorf("the parts open as %q", got)
	}
	if opened, err := s.OpenParts(t.Context(), "42", sealed.Key, nil); err != nil || opened != nil {
		t.Errorf("opening no parts gives %q, %v", opened, err)
	}
	again, err := s.SealPart(t.Context(), "42", sealed.Key, id, []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(again[1:13], a[1:13]) {
		t.Error("two sealings of a part share a nonce")
	}
	if !keys.cleared() {
		t.Error("a data key wasn't cleared")
	}
}

// A part opens only as the latest generation of its own purpose, ID, run
// and store: one of an older generation, one moved to another job, run,
// store or purpose, one changed in any byte or cut short, and a whole
// record in a part's place or a part in a whole record's, are each
// record_unreadable. A Keys that binds nothing to what it wraps leaves the
// part's own binding alone to refuse them.
func TestPartRefused(t *testing.T) {
	loose := newKeys(t, true)
	s := newSealer(t, loose, "store-1")
	sealed := sealRecord(t, s, "42")
	build := func(gen uint64) seal.PartID {
		return seal.PartID{Purpose: seal.PurposeJobRecord, ID: "build", Generation: gen}
	}
	old, err := s.SealPart(t.Context(), "42", sealed.Key, build(1), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.SealPart(t.Context(), "42", sealed.Key, build(2), []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	open := func(s *seal.Sealer, run string, id seal.PartID, sealedPart []byte) error {
		_, err := s.OpenParts(t.Context(), run, sealed.Key, []seal.SealedPart{{PartID: id, Sealed: sealedPart}})
		return err
	}
	if err := open(s, "42", build(2), part); err != nil {
		t.Fatal(err)
	}
	refused := map[string]error{
		"an older generation in the latest's place": open(s, "42", build(2), old),
		"the latest as an older generation":         open(s, "42", build(1), part),
		"moved to another job":                      open(s, "42", seal.PartID{Purpose: seal.PurposeJobRecord, ID: "build#0", Generation: 2}, part),
		"moved to another purpose":                  open(s, "42", seal.PartID{Purpose: placement, ID: "build", Generation: 2}, part),
		"moved to another run":                      open(s, "43", build(2), part),
		"moved to another store":                    open(newSealer(t, loose, "store-2"), "42", build(2), part),
		"a whole record in a part's place":          open(s, "42", build(1), sealed.Record),
		"a part in a whole record's place":          second(s.Open(t.Context(), "42", 2, seal.Sealed{Key: sealed.Key, Record: part})),
		"empty":                                     open(s, "42", build(2), nil),
	}
	for i := range part {
		changed := bytes.Clone(part)
		changed[i] ^= 1
		refused["changed in byte "+strconv.Itoa(i)] = open(s, "42", build(2), changed)
	}
	for n := 1; n < len(part); n++ {
		if err := open(s, "42", build(2), part[:n]); err == nil {
			t.Errorf("the part cut to %d bytes opened", n)
		}
	}
	for what, err := range refused {
		if p, ok := errors.AsType[*problem.Problem](err); !ok || p.Code != problems.RecordUnreadable.Code() {
			t.Errorf("%s: %v, want record_unreadable", what, err)
		}
	}
	for _, id := range []seal.PartID{
		{Purpose: seal.PurposeJobRecord, ID: "", Generation: 1},
		{Purpose: seal.PurposeJobRecord, ID: "a\x00b", Generation: 1},
		{Purpose: seal.PurposeJobRecord, ID: "\xff", Generation: 1},
		{Purpose: "", ID: "build", Generation: 1},
		{Purpose: "a\x00b", ID: "build", Generation: 1},
		{Purpose: seal.PurposeJobRecord, ID: "build", Generation: 0},
	} {
		if _, err := s.SealPart(t.Context(), "42", sealed.Key, id, []byte("x")); err == nil {
			t.Errorf("SealPart takes %+v", id)
		}
	}
}

// A job's part is a part of PurposeJobRecord whose ID is the job's key,
// which holds the job's record; parts of several jobs open, and decode, with
// one unwrap.
func TestJobParts(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	a, err := s.SealJob(t.Context(), "42", "build#2", 1, sealed.Key, jobRecord(secret))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SealJob(t.Context(), "42", "test", 3, sealed.Key, jobRecord("other"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(a, []byte(secret)) {
		t.Errorf("a sealed part is %x", a)
	}
	raw, err := s.OpenParts(t.Context(), "42", sealed.Key, []seal.SealedPart{{Purpose: seal.PurposeJobRecord, ID: "build#2", Generation: 1, Sealed: a}})
	if err != nil || !bytes.Contains(raw[0], []byte(secret)) {
		t.Errorf("the job's part opens as a part as %q, %v", raw, err)
	}
	before := keys.unwraps
	got, err := s.OpenJobs(t.Context(), "42", sealed.Key, map[string]seal.JobPart{"build#2": {Generation: 1, Sealed: a}, "test": {Generation: 3, Sealed: b}})
	if err != nil {
		t.Fatal(err)
	}
	if keys.unwraps != before+1 {
		t.Errorf("opening two jobs' parts unwrapped the data key %d times", keys.unwraps-before)
	}
	if !reflect.DeepEqual(got, map[string]plan.JobRecord{"build#2": jobRecord(secret), "test": jobRecord("other")}) {
		t.Errorf("the parts open as %+v", got)
	}
	if _, err := s.OpenJobs(t.Context(), "42", sealed.Key, map[string]seal.JobPart{"test": {Generation: 3, Sealed: a}}); err == nil {
		t.Error("one job's part opened as another's")
	}

	// A part that opens but isn't a job's record is malformed, and the error
	// quotes nothing of it.
	notRecord, err := s.SealPart(t.Context(), "42", sealed.Key, seal.PartID{Purpose: seal.PurposeJobRecord, ID: "lint", Generation: 1}, []byte(`{"outputs":{"x":"`+secret+`"},"extra":1}`))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.OpenJobs(t.Context(), "42", sealed.Key, map[string]seal.JobPart{"lint": {Generation: 1, Sealed: notRecord}})
	if !errors.Is(err, plan.ErrMalformed) || bytes.Contains([]byte(err.Error()), []byte(secret)) {
		t.Errorf("a part that isn't a job's record: %v", err)
	}
	if !keys.cleared() {
		t.Error("a data key wasn't cleared")
	}
}
