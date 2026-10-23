package safepath

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
)

// overflowUID is the user ID the kernel shows for an owner a user
// namespace doesn't map: /proc/sys/kernel/overflowuid, 65534 by default.
var overflowUID = sync.OnceValue(func() int {
	data, err := os.ReadFile("/proc/sys/kernel/overflowuid")
	if err != nil {
		return 65534
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 65534
	}
	return n
})

// uidMap is this process's user namespace's map of user IDs, from
// /proc/self/uid_map: each line an ID inside, the ID outside it maps to,
// and how many.
var uidMap = sync.OnceValue(func() [][3]int64 {
	f, err := os.Open("/proc/self/uid_map")
	if err != nil {
		return nil
	}
	defer f.Close()
	var out [][3]int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			continue
		}
		var m [3]int64
		ok := true
		for i, s := range fields {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				ok = false
				break
			}
			m[i] = n
		}
		if ok {
			out = append(out, m)
		}
	}
	return out
})

// Unmapped says whether uid, a file's owner, is one this user namespace
// hides: the kernel's overflow ID, when the namespace doesn't map it to a
// user of its own, as it shows root outside a namespace that doesn't map
// root.
func Unmapped(uid int) bool {
	if uid != overflowUID() {
		return false
	}
	return !mapsUID(uidMap(), int64(uid))
}

// mapsUID says whether m, a uid_map, maps the ID uid inside it.
func mapsUID(m [][3]int64, uid int64) bool {
	for _, r := range m {
		if uid >= r[0] && uid < r[0]+r[2] {
			return true
		}
	}
	return false
}
