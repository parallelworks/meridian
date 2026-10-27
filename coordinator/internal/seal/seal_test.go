package seal_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/fips140"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/coordinator/internal/seal"
	"github.com/parallelworks/meridian/internal/kindtest"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// secret is a password the test records hold, which no error may show.
const secret = "hunter2-p4ssw0rd"

// record returns a secret record with each of its members set.
func record() *plan.Record {
	seconds := int64(3600)
	return &plan.Record{
		Version:    plan.Version,
		PlanDigest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Submitted:  plan.DataOf(map[string]any{"name": "Alice", "pw": secret, "n": int64(3)}),
		Withheld:   map[string]plan.Data{"/on/execute/inputs/pw/default": plan.DataOf("${{ vars.PW }}")},
		Decisions:  map[string]bool{"/on/execute/inputs/x/hidden": true},
		Means:      map[string]string{"inputs.region": "/on/execute/inputs/region/default"},
		Run: &plan.RunValues{
			Format:  1,
			Inputs:  plan.DataOf(map[string]any{"pw": secret}),
			Vars:    map[string]string{"TOKEN": secret},
			Secrets: []string{"inputs.pw", "vars.TOKEN"},
		},
		TimeoutSeconds: &seconds,
		Expansions: map[string]*plan.Expansion{"build": {Instances: []plan.Instance{
			{Key: "build#0", Matrix: plan.DataOf(map[string]any{"pw": secret}), Private: true, Secret: true},
		}}},
		Jobs: map[string]plan.JobRecord{"test": {Outputs: map[string]string{"dsn": "postgres://u:" + secret + "@db"}}},
	}
}

// testKeys wraps data keys as the local Keys does: with AES-256-GCM under
// a key of its own, a random nonce, and the run's ID as the associated
// data, unless loose is set, as for a Keys that binds nothing to what it
// wraps, which leaves the record's own binding alone to refuse what was
// moved. A wrapped key that doesn't open is a problem. It keeps each data
// key Wrap is given and Unwrap gives, as they are, so a test can check
// that the caller clears them, and a copy of each.
type testKeys struct {
	aead  cipher.AEAD
	loose bool

	mu      sync.Mutex
	ids     []string
	handled [][]byte
	copies  [][]byte
	unwraps int
}

var _ coordinator.Keys = (*testKeys)(nil)

func newKeys(t *testing.T, loose bool) *testKeys {
	t.Helper()
	return &testKeys{aead: newAEAD(t, randomKey(t)), loose: loose}
}

func randomKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func newAEAD(t *testing.T, key []byte) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}

func (k *testKeys) associated(runID string) []byte {
	if k.loose {
		return nil
	}
	return []byte(runID)
}

func (k *testKeys) keep(runID string, key []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.ids = append(k.ids, runID)
	k.handled = append(k.handled, key)
	k.copies = append(k.copies, bytes.Clone(key))
}

func (k *testKeys) Wrap(ctx context.Context, runID string, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k.keep(runID, key)
	return k.aead.Seal(nil, nil, key, k.associated(runID)), nil
}

func (k *testKeys) Unwrap(ctx context.Context, runID string, wrapped []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k.mu.Lock()
	k.unwraps++
	k.mu.Unlock()
	key, err := k.aead.Open(nil, nil, wrapped, k.associated(runID))
	if err != nil {
		return nil, problems.KeyUnwrapFailed.Problem(problems.Params{"run": runID, "path": "test.key"})
	}
	k.keep(runID, key)
	return key, nil
}

// dataKeys returns a copy of each data key k has handled.
func (k *testKeys) dataKeys() [][]byte {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.copies
}

// cleared reports whether each data key k has handled has been cleared.
func (k *testKeys) cleared() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, key := range k.handled {
		if !bytes.Equal(key, make([]byte, len(key))) {
			return false
		}
	}
	return true
}

// funcKeys is a Keys whose methods are funcs, as one that fails.
type funcKeys struct {
	wrap   func(runID string, key []byte) ([]byte, error)
	unwrap func(runID string, wrapped []byte) ([]byte, error)
}

func (k funcKeys) Wrap(_ context.Context, runID string, key []byte) ([]byte, error) {
	return k.wrap(runID, key)
}

func (k funcKeys) Unwrap(_ context.Context, runID string, wrapped []byte) ([]byte, error) {
	return k.unwrap(runID, wrapped)
}

func newSealer(t *testing.T, keys coordinator.Keys, storeID string) *seal.Sealer {
	t.Helper()
	s, err := seal.New(keys, storeID)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// sealRecord seals record() for the run runID, whose generation is then 1.
func sealRecord(t *testing.T, s *seal.Sealer, runID string) seal.Sealed {
	t.Helper()
	sealed, gen, err := s.Seal(t.Context(), runID, record())
	if err != nil {
		t.Fatal(err)
	}
	if gen != 1 {
		t.Fatalf("Seal gives generation %d, want 1", gen)
	}
	return sealed
}

// associated is a record's associated data, as the package says it is.
func associated(storeID, runID, gen string) []byte {
	return []byte("meridian/record/1\x00" + storeID + "\x00" + runID + "\x00" + gen)
}

// The tests run in FIPS 140-3 mode, which go.mod's godebug sets, so the
// sealing uses only what the module approves.
func TestFIPS(t *testing.T) {
	if !fips140.Enforced() {
		t.Fatal("the tests don't run under GODEBUG=fips140=only")
	}
}

// A record opens as it was sealed, under a data key Keys wrapped for the
// store's and the run's IDs; it is sealed as the package says, so it opens
// with its data key and its associated data alone; and each sealing has a
// data key and a nonce of its own.
func TestRoundTrip(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	got, err := s.Open(t.Context(), "42", 1, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, record()) {
		t.Errorf("the record opens as %+v", got)
	}
	if keys.ids[0] != "store-1/42" {
		t.Errorf("Keys wrapped the data key for %q, want store-1/42", keys.ids[0])
	}
	if bytes.Contains(sealed.Record, []byte(secret)) || bytes.Contains(sealed.Key, keys.dataKeys()[0]) {
		t.Error("what is sealed holds the record or the data key in the clear")
	}

	if sealed.Record[0] != 1 {
		t.Errorf("a sealed record starts with %d, want 1", sealed.Record[0])
	}
	data, err := newAEAD(t, keys.dataKeys()[0]).Open(nil, nil, sealed.Record[1:], associated("store-1", "42", "1"))
	if err != nil {
		t.Fatalf("the record doesn't open with its data key and associated data: %v", err)
	}
	if want, _ := plan.EncodeRecord(record()); !bytes.Equal(data, want) {
		t.Errorf("the record seals\n%s\nwant\n%s", data, want)
	}

	again := sealRecord(t, s, "42")
	if bytes.Equal(again.Record[1:13], sealed.Record[1:13]) || bytes.Equal(keys.dataKeys()[0], keys.dataKeys()[2]) {
		t.Error("two sealings share a nonce or a data key")
	}
}

// Reseal seals a record again under the same data key, which it keeps
// wrapped as it was, with a new nonce each time, and the next generation,
// written in decimal in the associated data.
func TestReseal(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	rec, err := s.Open(t.Context(), "42", 1, sealed)
	if err != nil {
		t.Fatal(err)
	}
	rec.Jobs["deploy#2"] = plan.JobRecord{Steps: map[string]map[string]string{"login": {"token": secret}}}
	resealed, gen, err := s.Reseal(t.Context(), "42", 1, sealed, rec)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 2 {
		t.Errorf("Reseal of generation 1 gives generation %d, want 2", gen)
	}
	if !bytes.Equal(resealed.Key, sealed.Key) {
		t.Error("Reseal changed the wrapped data key")
	}
	got, err := s.Open(t.Context(), "42", 2, resealed)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, rec) {
		t.Errorf("the resealed record opens as %+v", got)
	}
	data, err := newAEAD(t, keys.dataKeys()[0]).Open(nil, nil, resealed.Record[1:], associated("store-1", "42", "2"))
	if err != nil {
		t.Fatalf("the resealed record doesn't open with the first data key and generation 2: %v", err)
	}
	if want, _ := plan.EncodeRecord(rec); !bytes.Equal(data, want) {
		t.Errorf("the record reseals\n%s\nwant\n%s", data, want)
	}
	again, _, err := s.Reseal(t.Context(), "42", 2, resealed, rec)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(again.Record[1:13], resealed.Record[1:13]) {
		t.Error("two resealings share a nonce")
	}
}

// A record that a later one replaced doesn't open in its place, as one
// rolled back in the store would, nor does a later one open as an earlier
// generation: each opens only as its own.
func TestRollback(t *testing.T) {
	for _, loose := range []bool{false, true} {
		s := newSealer(t, newKeys(t, loose), "store-1")
		records := []seal.Sealed{sealRecord(t, s, "42")}
		for gen := uint64(1); gen < 4; gen++ {
			rec := record()
			rec.Decisions["/on/execute/inputs/x/ignore"] = gen%2 == 0
			next, got, err := s.Reseal(t.Context(), "42", gen, records[gen-1], rec)
			if err != nil {
				t.Fatal(err)
			}
			if got != gen+1 {
				t.Fatalf("Reseal of generation %d gives %d", gen, got)
			}
			records = append(records, next)
		}
		for i, sealed := range records {
			for gen := uint64(1); gen <= uint64(len(records)); gen++ {
				_, err := s.Open(t.Context(), "42", gen, seal.Sealed{Key: records[len(records)-1].Key, Record: sealed.Record})
				if gen == uint64(i+1) {
					if err != nil {
						t.Errorf("generation %d doesn't open as itself: %v", gen, err)
					}
					continue
				}
				if p := kindtest.Want(t, err, problems.RecordUnreadable); p.Params["run"] != "42" {
					t.Errorf("the problem's params are %v", p.Params)
				}
			}
		}
	}
}

// A record is sealed again at most MaxReseals times, after which Reseal
// refuses with a problem, which fails the run.
func TestResealLimit(t *testing.T) {
	s := newSealer(t, newKeys(t, false), "store-1")
	sealed := sealRecord(t, s, "42")
	last, gen, err := s.Reseal(t.Context(), "42", seal.MaxReseals, sealed, record())
	if err != nil || gen != seal.MaxReseals+1 {
		t.Fatalf("the last reseal gives generation %d, %v", gen, err)
	}
	if _, err := s.Open(t.Context(), "42", gen, last); err != nil {
		t.Errorf("the last generation doesn't open: %v", err)
	}
	for _, gen := range []uint64{seal.MaxReseals + 1, 1 << 63, ^uint64(0)} {
		_, _, err = s.Reseal(t.Context(), "42", gen, last, record())
		if p := kindtest.Want(t, err, problems.RecordResealLimit); p.Params["run"] != "42" || p.Params["limit"] != seal.MaxReseals {
			t.Errorf("the problem's params are %v", p.Params)
		}
	}
}

// Each Open decrypts and decodes the record anew, so what one caller does
// to its record isn't what another opens.
func TestOpenGivesItsOwnRecord(t *testing.T) {
	s := newSealer(t, newKeys(t, false), "store-1")
	sealed := sealRecord(t, s, "42")
	first, err := s.Open(t.Context(), "42", 1, sealed)
	if err != nil {
		t.Fatal(err)
	}
	first.Decisions["/on/execute/inputs/x/hidden"] = false
	first.Run.Vars["TOKEN"] = "changed"
	second, err := s.Open(t.Context(), "42", 1, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, record()) {
		t.Errorf("a second Open gives %+v", second)
	}
}

// A record moved to another run, or to a run with the same ID in another
// store, doesn't open, however alike the IDs: under a Keys that binds
// nothing, its associated data alone refuses it. Under one that binds the
// run, the data key of the run it was moved to doesn't open it either, and
// the data key moved with it doesn't unwrap.
func TestMovedRecord(t *testing.T) {
	loose := newKeys(t, true)
	sealed := sealRecord(t, newSealer(t, loose, "store-1"), "42")
	for _, to := range []struct{ store, run string }{
		{"store-1", "43"},
		{"store-1", "4"},
		{"store-1", "420"},
		{"store-2", "42"},
		{"store-14", "2"},
		{"store-", "142"},
	} {
		_, err := newSealer(t, loose, to.store).Open(t.Context(), to.run, 1, sealed)
		if p := kindtest.Want(t, err, problems.RecordUnreadable); p.Params["run"] != to.run {
			t.Errorf("moved to %q, the problem's params are %v", to, p.Params)
		}
	}

	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed, other := sealRecord(t, s, "42"), sealRecord(t, s, "43")
	_, err := s.Open(t.Context(), "43", 1, seal.Sealed{Key: other.Key, Record: sealed.Record})
	kindtest.Want(t, err, problems.RecordUnreadable)
	_, err = s.Open(t.Context(), "43", 1, sealed)
	kindtest.Want(t, err, problems.KeyUnwrapFailed)
	_, err = newSealer(t, keys, "store-2").Open(t.Context(), "42", 1, sealed)
	kindtest.Want(t, err, problems.KeyUnwrapFailed)
}

// A wrapped data key moved to another run doesn't unwrap there, which is
// Keys' problem; under a Keys that binds nothing, the data key it gives
// doesn't open the other run's record.
func TestMovedKey(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed, other := sealRecord(t, s, "42"), sealRecord(t, s, "43")
	_, err := s.Open(t.Context(), "43", 1, seal.Sealed{Key: sealed.Key, Record: other.Record})
	if p := kindtest.Want(t, err, problems.KeyUnwrapFailed); p.Params["run"] != "store-1/43" {
		t.Errorf("the problem's params are %v", p.Params)
	}
	_, _, err = s.Reseal(t.Context(), "43", 1, seal.Sealed{Key: sealed.Key, Record: other.Record}, record())
	kindtest.Want(t, err, problems.KeyUnwrapFailed)

	loose := newKeys(t, true)
	s = newSealer(t, loose, "store-1")
	sealed, other = sealRecord(t, s, "42"), sealRecord(t, s, "43")
	_, err = s.Open(t.Context(), "43", 1, seal.Sealed{Key: sealed.Key, Record: other.Record})
	kindtest.Want(t, err, problems.RecordUnreadable)
}

// A record changed in any byte doesn't open: its nonce, its ciphertext and
// its tag are each authenticated, and a first byte that names no version
// is no record. One that names a later version is a newer release's, and
// Keys isn't asked to unwrap its key. A wrapped key changed in any byte
// doesn't unwrap.
func TestTampered(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	for i := range sealed.Record {
		for _, bit := range []byte{0x01, 0x80} {
			if i == 0 && bit == 0x80 {
				continue // a later version, below
			}
			changed := bytes.Clone(sealed.Record)
			changed[i] ^= bit
			_, err := s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key, Record: changed})
			kindtest.Want(t, err, problems.RecordUnreadable)
		}
	}
	_, err := s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key, Record: append(bytes.Clone(sealed.Record), 0)})
	kindtest.Want(t, err, problems.RecordUnreadable)

	before := keys.unwraps
	later := bytes.Clone(sealed.Record)
	later[0] = 2
	_, err = s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key, Record: later})
	if !errors.Is(err, plan.ErrVersion) || problems.IsProblem(err) {
		t.Errorf("a record of version 2 gives %v, want plan.ErrVersion", err)
	}
	if keys.unwraps != before {
		t.Error("Keys was asked to unwrap the key of a record of version 2")
	}

	for i := range sealed.Key {
		changed := bytes.Clone(sealed.Key)
		changed[i] ^= 0x01
		_, err := s.Open(t.Context(), "42", 1, seal.Sealed{Key: changed, Record: sealed.Record})
		kindtest.Want(t, err, problems.KeyUnwrapFailed)
	}
}

// A record or a wrapped key cut short, to nothing among the rest, doesn't
// open, and nothing panics.
func TestTruncated(t *testing.T) {
	s := newSealer(t, newKeys(t, false), "store-1")
	sealed := sealRecord(t, s, "42")
	for n := range len(sealed.Record) {
		_, err := s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key, Record: sealed.Record[:n]})
		kindtest.Want(t, err, problems.RecordUnreadable)
	}
	_, err := s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key})
	kindtest.Want(t, err, problems.RecordUnreadable)
	for n := range len(sealed.Key) {
		_, err := s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key[:n], Record: sealed.Record})
		kindtest.Want(t, err, problems.KeyUnwrapFailed)
		_, _, err = s.Reseal(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key[:n], Record: sealed.Record}, record())
		kindtest.Want(t, err, problems.KeyUnwrapFailed)
	}
}

// A problem from Keys is the caller's to fail the run with; any other
// error, the context's end among them, isn't a problem, and the caller
// tries again. A key Unwrap gives that isn't an AES-256 key is no record's,
// and is cleared.
func TestKeysErrors(t *testing.T) {
	sealed := sealRecord(t, newSealer(t, newKeys(t, false), "store-1"), "42")
	down := errors.New("the key service is down")
	failing := funcKeys{
		wrap:   func(string, []byte) ([]byte, error) { return nil, down },
		unwrap: func(string, []byte) ([]byte, error) { return nil, down },
	}
	s := newSealer(t, failing, "store-1")
	_, _, err := s.Seal(t.Context(), "42", record())
	if !errors.Is(err, down) || problems.IsProblem(err) {
		t.Errorf("Seal gives %v, want the Keys' error, which isn't a problem", err)
	}
	_, err = s.Open(t.Context(), "42", 1, sealed)
	if !errors.Is(err, down) || problems.IsProblem(err) {
		t.Errorf("Open gives %v, want the Keys' error, which isn't a problem", err)
	}
	_, _, err = s.Reseal(t.Context(), "42", 1, sealed, record())
	if !errors.Is(err, down) || problems.IsProblem(err) {
		t.Errorf("Reseal gives %v, want the Keys' error, which isn't a problem", err)
	}

	refused := problems.KeyWrappingUnknown.Problem(problems.Params{"run": "store-1/42"})
	refusing := funcKeys{
		wrap:   func(string, []byte) ([]byte, error) { return nil, refused },
		unwrap: func(string, []byte) ([]byte, error) { return nil, refused },
	}
	s = newSealer(t, refusing, "store-1")
	_, _, err = s.Seal(t.Context(), "42", record())
	kindtest.Want(t, err, problems.KeyWrappingUnknown)
	_, err = s.Open(t.Context(), "42", 1, sealed)
	kindtest.Want(t, err, problems.KeyWrappingUnknown)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s = newSealer(t, newKeys(t, false), "store-1")
	if _, _, err := s.Seal(ctx, "42", record()); !errors.Is(err, context.Canceled) || problems.IsProblem(err) {
		t.Errorf("Seal with a cancelled context gives %v", err)
	}

	for _, size := range []int{0, 16, 31, 33} {
		var given []byte
		short := funcKeys{unwrap: func(string, []byte) ([]byte, error) {
			given = bytes.Repeat([]byte{0xa5}, size)
			return given, nil
		}}
		s := newSealer(t, short, "store-1")
		_, err := s.Open(t.Context(), "42", 1, sealed)
		kindtest.Want(t, err, problems.RecordUnreadable)
		if !bytes.Equal(given, make([]byte, size)) {
			t.Errorf("a %d-byte key from Unwrap isn't cleared", size)
		}
		_, _, err = s.Reseal(t.Context(), "42", 1, sealed, record())
		kindtest.Want(t, err, problems.RecordUnreadable)
	}
}

// sealData seals data, as if it were an encoded record, as the package
// does, under key, for the run runID of the store storeID, of generation 1.
func sealData(t *testing.T, key []byte, storeID, runID string, data []byte) []byte {
	t.Helper()
	return newAEAD(t, key).Seal([]byte{1}, nil, data, associated(storeID, runID, "1"))
}

// A record that opens but doesn't decode is malformed, or of a version
// this release doesn't read, as DecodeRecord says; neither is a problem,
// and neither error quotes the record, as what DecodeRecord says can.
func TestUndecodable(t *testing.T) {
	key := randomKey(t)
	given := funcKeys{unwrap: func(string, []byte) ([]byte, error) { return bytes.Clone(key), nil }}
	s := newSealer(t, given, "store-1")
	for data, want := range map[string]error{
		`{"version":1,"decisions":{"x":"` + secret + `"}}`: plan.ErrMalformed,
		`{"version":1,"submitted":"` + secret + `"}`:       plan.ErrMalformed,
		`{"version":1,"` + secret + `":true}`:              plan.ErrMalformed,
		`not JSON ` + secret:                               plan.ErrMalformed,
		`{"version":3,"secret":"` + secret + `"}`:          plan.ErrVersion,
	} {
		sealed := seal.Sealed{Record: sealData(t, key, "store-1", "42", []byte(data))}
		_, err := s.Open(t.Context(), "42", 1, sealed)
		if !errors.Is(err, want) || problems.IsProblem(err) {
			t.Errorf("%s opens with %v, want %v", data, err, want)
		}
		if err != nil && strings.Contains(fmt.Sprintf("%+v", err), secret) {
			t.Errorf("%s opens with %q, which quotes it", data, err)
		}
	}
}

// Seal clears the data key once Keys has wrapped it and it has sealed the
// record, and Open and Reseal the one Unwrap gave; no error, whatever it
// is, holds a data key, as it is or written as hex or base64, nor the
// record's secret.
func TestDataKeyNeverShown(t *testing.T) {
	keys := newKeys(t, false)
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	other := sealRecord(t, s, "43")
	if _, err := s.Open(t.Context(), "42", 1, sealed); err != nil {
		t.Fatal(err)
	}
	resealed, _, err := s.Reseal(t.Context(), "42", 1, sealed, record())
	if err != nil {
		t.Fatal(err)
	}
	if !keys.cleared() {
		t.Error("a data key wasn't cleared")
	}

	dataKey := keys.dataKeys()[0]
	changed := bytes.Clone(sealed.Record)
	changed[len(changed)-1] ^= 1
	var errs []error
	try := func(err error) {
		if err == nil {
			t.Error("an operation that should fail didn't")
		}
		errs = append(errs, err)
	}
	try(second(s.Open(t.Context(), "43", 1, sealed)))
	try(second(s.Open(t.Context(), "43", 1, seal.Sealed{Key: other.Key, Record: sealed.Record})))
	try(second(s.Open(t.Context(), "42", 2, sealed)))
	try(second(s.Open(t.Context(), "42", 1, resealed)))
	try(second(newSealer(t, newKeys(t, true), "store-2").Open(t.Context(), "42", 1, sealed)))
	try(second(s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key, Record: changed})))
	try(second(s.Open(t.Context(), "42", 1, seal.Sealed{Key: sealed.Key, Record: sealed.Record[:20]})))
	for _, give := range [][]byte{dataKey[:16], append(bytes.Clone(dataKey), dataKey...)} {
		wrong := funcKeys{unwrap: func(string, []byte) ([]byte, error) { return bytes.Clone(give), nil }}
		try(second(newSealer(t, wrong, "store-1").Open(t.Context(), "42", 1, sealed)))
	}
	given := funcKeys{unwrap: func(string, []byte) ([]byte, error) { return bytes.Clone(dataKey), nil }}
	try(second(newSealer(t, given, "store-1").Open(t.Context(), "42", 1,
		seal.Sealed{Record: sealData(t, dataKey, "store-1", "42", []byte(`{"version":1,"decisions":{"x":"`+secret+`"}}`))})))
	try(third(s.Seal(t.Context(), "42", &plan.Record{Version: plan.Version, Submitted: plan.DataOf(map[string]any{"pw": func() {}})})))
	try(third(s.Reseal(t.Context(), "42", seal.MaxReseals+1, sealed, record())))

	half := dataKey[:16]
	for _, err := range errs {
		for _, text := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
			for _, shown := range []string{
				string(half), hex.EncodeToString(half), strings.ToUpper(hex.EncodeToString(half)),
				base64.StdEncoding.EncodeToString(dataKey), base64.RawURLEncoding.EncodeToString(dataKey),
				fmt.Sprintf("%d", half[:8]), secret,
			} {
				if strings.Contains(text, shown) {
					t.Errorf("the error %q shows the data key or the record", err)
				}
			}
		}
	}
}

// A caller's mistakes, which no store or run makes, are refused with an
// error that isn't a problem: no Keys; a store or run ID that is empty,
// isn't UTF-8, or holds a NUL or a slash, which could otherwise make two
// pairs of IDs read as one in the associated data or Keys' ID; generation
// 0; no record, and one of another version.
func TestCallerMistakes(t *testing.T) {
	keys := newKeys(t, false)
	if _, err := seal.New(nil, "store-1"); err == nil {
		t.Error("New takes no Keys")
	}
	bad := []string{"", "a/b", "a\x00b", "/", "\x00", "4\x00", "\xff", "a\xc3"}
	for _, id := range bad {
		if _, err := seal.New(keys, id); err == nil {
			t.Errorf("New takes the store ID %q", id)
		}
	}
	s := newSealer(t, keys, "store-1")
	sealed := sealRecord(t, s, "42")
	mistakes := map[string]error{
		"Seal without a record":     third(s.Seal(t.Context(), "42", nil)),
		"Seal of version 0":         third(s.Seal(t.Context(), "42", &plan.Record{})),
		"Reseal without a record":   third(s.Reseal(t.Context(), "42", 1, sealed, nil)),
		"Reseal of another version": third(s.Reseal(t.Context(), "42", 1, sealed, &plan.Record{Version: plan.Version + 1})),
		"Reseal of generation 0":    third(s.Reseal(t.Context(), "42", 0, sealed, record())),
		"Open of generation 0":      second(s.Open(t.Context(), "42", 0, sealed)),
	}
	for _, id := range bad {
		mistakes[fmt.Sprintf("Seal for run %q", id)] = third(s.Seal(t.Context(), id, record()))
		mistakes[fmt.Sprintf("Reseal for run %q", id)] = third(s.Reseal(t.Context(), id, 1, sealed, record()))
		mistakes[fmt.Sprintf("Open for run %q", id)] = second(s.Open(t.Context(), id, 1, sealed))
	}
	for name, err := range mistakes {
		if err == nil || problems.IsProblem(err) {
			t.Errorf("%s gives %v, want an error that isn't a problem", name, err)
		}
	}
	if len(keys.ids) != 1 {
		t.Errorf("Keys was asked %d times for a caller's mistakes", len(keys.ids)-1)
	}
}

func second[T any](_ T, err error) error { return err }

func third[T, U any](_ T, _ U, err error) error { return err }
