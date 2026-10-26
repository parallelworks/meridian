package jsonshape_test

import (
	"encoding/json/jsontext"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/internal/jsonshape"
)

// held holds any JSON, and decodes it itself, as a plan's Value does.
type held struct{}

// zeroed holds any JSON, and its zero value is written as null, as a
// plan's Data's is.
type zeroed struct{}

type inner struct {
	Name string `json:"name"`
	Kind string `json:"kind,omitempty" enum:"a,b"`
}

type Embedded struct {
	Extra int `json:"extra"`
}

type outer struct {
	Embedded
	Key     string            `json:"key"`
	Count   int               `json:"count,omitzero"`
	Big     int64             `json:"big,omitzero"`
	Items   []string          `json:"items"`
	More    []string          `json:"more,omitempty"`
	Inner   *inner            `json:"inner,omitzero"`
	List    []inner           `json:"list,omitempty"`
	Strings map[string]string `json:"strings,omitempty"`
	Held    held              `json:"held,omitzero"`
	Zero    zeroed            `json:"zero,omitzero"`
	Always  zeroed            `json:"always"`
	Zeros   map[string]zeroed `json:"zeros,omitempty"`
	Any     map[string]any    `json:"any,omitempty"`
	Time    time.Time         `json:"time,omitzero"`
	Raw     jsontext.Value    `json:"raw,omitempty"`
	Ignored string            `json:"-"`
}

var options = &jsonshape.Options{
	NilAsNull: true,
	Any:       []reflect.Type{reflect.TypeFor[held]()},
	Zero:      []reflect.Type{reflect.TypeFor[zeroed]()},
}

const whole = `{"extra":1,"key":"k","count":2147483647,"big":9223372036854775807,"items":null,"more":["a"],` +
	`"inner":{"name":"n","kind":"a"},"list":[{"name":"m"}],"strings":{"a":"b"},"held":null,"zero":{"a":null},` +
	`"always":null,"zeros":{"a":null},"any":{"a":null},"time":"2026-01-01T00:00:00Z","raw":{"x":null}}`

func TestCheckTakesWhatIsWritten(t *testing.T) {
	for _, data := range []string{
		whole,
		`{"extra":0,"key":"","items":[],"always":1}`,
		`{"extra":-2147483648,"key":"k","items":null,"always":null,"count":-2147483648}`,
	} {
		if err := jsonshape.Check([]byte(data), reflect.TypeFor[outer](), options); err != nil {
			t.Errorf("%s: %v", data, err)
		}
	}
}

func TestCheckRefuses(t *testing.T) {
	for _, tc := range []struct{ name, data, want string }{
		{"a missing member", `{"extra":1,"items":[],"always":1}`, `no "key" within ""`},
		{"an embedded struct's missing member", `{"key":"k","items":[],"always":1}`, `no "extra" within ""`},
		{"a missing member within", `{"extra":1,"key":"k","items":[],"always":1,"list":[{}]}`, `no "name" within "/list/0"`},
		{"null for a string", `{"extra":1,"key":null,"items":[],"always":1}`, `null within "/key"`},
		{"null for a list left out when empty", `{"extra":1,"key":"k","items":[],"more":null,"always":1}`, `null within "/more"`},
		{"null for a pointer left out when nil", `{"extra":1,"key":"k","items":[],"inner":null,"always":1}`, `null within "/inner"`},
		{"null for an item", `{"extra":1,"key":"k","items":[null],"always":1}`, `null within "/items/0"`},
		{"null for a struct item", `{"extra":1,"key":"k","items":[],"list":[null],"always":1}`, `null within "/list/0"`},
		{"null for a mapping's item", `{"extra":1,"key":"k","items":[],"strings":{"a":null},"always":1}`, `null within "/strings/a"`},
		{"null where the zero is left out", `{"extra":1,"key":"k","items":[],"zero":null,"always":1}`, `null within "/zero"`},
		{"null for a time", `{"extra":1,"key":"k","items":[],"time":null,"always":1}`, `null within "/time"`},
		{"null for raw JSON", `{"extra":1,"key":"k","items":[],"raw":null,"always":1}`, `null within "/raw"`},
		{"null for the whole", `null`, `null within ""`},
		{"an int past 32 bits", `{"extra":1,"key":"k","items":[],"always":1,"count":2147483648}`, `2147483648 within "/count" is out of range`},
		{"an int below 32 bits", `{"extra":-2147483649,"key":"k","items":[],"always":1}`, `-2147483649 within "/extra" is out of range`},
		{"a string not in its enum", `{"extra":1,"key":"k","items":[],"always":1,"inner":{"name":"n","kind":"c"}}`, `"c" within "/inner/kind" isn't one of a, b`},
		{"an empty string not in its enum", `{"extra":1,"key":"k","items":[],"always":1,"inner":{"name":"n","kind":""}}`, `"" within "/inner/kind" isn't one of a, b`},
		{"null for an enum", `{"extra":1,"key":"k","items":[],"always":1,"inner":{"name":"n","kind":null}}`, `null within "/inner/kind" isn't one of a, b`},
	} {
		err := jsonshape.Check([]byte(tc.data), reflect.TypeFor[outer](), options)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error with %s", tc.name, err, tc.want)
		}
	}
}

// Without NilAsNull, as for an encoding that writes a nil list as [], null
// is no list's value.
func TestCheckWithoutNilAsNull(t *testing.T) {
	err := jsonshape.Check([]byte(`{"extra":1,"key":"k","items":null,"always":1}`), reflect.TypeFor[outer](), &jsonshape.Options{})
	if err == nil || !strings.Contains(err.Error(), `null within "/items"`) {
		t.Errorf("got %v", err)
	}
}
