package slurm

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"maps"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/parallelworks/meridian/backend/batch"
	"github.com/parallelworks/meridian/plan"
	"github.com/parallelworks/meridian/problems"
)

// namePattern is a partition's, an account's, a QoS's, a reservation's or
// a GPU type's name: what Slurm's names hold, and never a comma or a
// colon, which would make an option of one name a list, or a gres more
// than one.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)

// maxWalltime is the longest walltime a member can fix.
const maxWalltime = 366 * 24 * 60 * 60

// Settings decodes a member's settings: partition, account, qos,
// reservation and gpu-type, names as Slurm's are; host, as text; nodes, a
// whole number of at least 1; exclusive, true or false; and walltime, a
// duration as a job's timeout: is written. A key it doesn't take, a value
// of another type, or one it can't use is member_setting_invalid, of the
// first such key, as they sort.
func (*Scheduler) Settings(member string, raw jsontext.Value) (*batch.Settings, error) {
	s := new(batch.Settings)
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return s, nil
	}
	invalid := func(key, reason string) error {
		return problems.MemberSettingInvalid.Problem(problems.Params{"member": member, "key": key, "reason": reason})
	}
	var keys map[string]jsontext.Value
	if err := jsonv2.Unmarshal(raw, &keys); err != nil {
		return nil, invalid("settings", "type")
	}
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		v := keys[key]
		text := func(dst *string, valid func(string) bool) error {
			if err := jsonv2.Unmarshal(v, dst); err != nil {
				return invalid(key, "type")
			}
			if !valid(*dst) {
				return invalid(key, "value")
			}
			return nil
		}
		var err error
		switch key {
		case "host":
			err = text(&s.Host, plain)
		case "partition":
			err = text(&s.Partition, namePattern.MatchString)
		case "account":
			err = text(&s.Account, namePattern.MatchString)
		case "qos":
			err = text(&s.QOS, namePattern.MatchString)
		case "reservation":
			err = text(&s.Reservation, namePattern.MatchString)
		case "gpu-type":
			err = text(&s.GPUType, namePattern.MatchString)
		case "nodes":
			if jsonv2.Unmarshal(v, &s.Nodes) != nil {
				err = invalid(key, "type")
			} else if s.Nodes < 1 {
				err = invalid(key, "value")
			}
		case "exclusive":
			if jsonv2.Unmarshal(v, &s.Exclusive) != nil {
				err = invalid(key, "type")
			}
		case "walltime":
			err = walltime(v, &s.WalltimeSeconds, invalid)
		default:
			err = invalid(key, "unknown")
		}
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// walltime decodes a walltime: a number or text, as a job's timeout: is
// written (plan.ParseDuration), greater than 0 and at most maxWalltime.
func walltime(v jsontext.Value, dst *int64, invalid func(key, reason string) error) error {
	var d any
	if err := jsonv2.Unmarshal(v, &d); err != nil {
		return invalid("walltime", "type")
	}
	switch d.(type) {
	case float64, string:
	default:
		return invalid("walltime", "type")
	}
	n, ok := plan.ParseDuration(d)
	if !ok || n <= 0 || n > maxWalltime {
		return invalid("walltime", "value")
	}
	*dst = n
	return nil
}

// plain reports whether s is text a setting can be: UTF-8, without
// control characters, at most 256 bytes, and not empty.
func plain(s string) bool {
	return s != "" && len(s) <= 256 && utf8.ValidString(s) && !strings.ContainsFunc(s, unicode.IsControl)
}
