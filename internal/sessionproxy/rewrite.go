package sessionproxy

import (
	"net/url"
	"strings"
)

// codeOf returns the one-time code in a query, as it is, and whether it
// has one: the first CodeParam.
func codeOf(rawQuery string) (string, bool) {
	for part := range strings.SplitSeq(rawQuery, "&") {
		key, value, _ := strings.Cut(part, "=")
		if k, err := url.QueryUnescape(key); err == nil && k == CodeParam {
			v, err := url.QueryUnescape(value)
			return v, err == nil && v != ""
		}
	}
	return "", false
}

// withoutCode returns a query without its CodeParams, and the rest of it as
// it is, byte for byte, in its order.
func withoutCode(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	var kept []string
	for part := range strings.SplitSeq(rawQuery, "&") {
		key, _, _ := strings.Cut(part, "=")
		if k, err := url.QueryUnescape(key); err == nil && k == CodeParam {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "&")
}

// withoutOwnCookies returns a Cookie header's value without the proxy's
// cookies, any whose name starts with meridian-, case aside (ownCookie),
// another session's as well as its own, nor any without a name, and the
// rest of it as it is, in its order.
func withoutOwnCookies(header string) string {
	var kept []string
	for part := range strings.SplitSeq(header, ";") {
		name, _, _ := strings.Cut(strings.TrimSpace(part), "=")
		if name == "" || ownCookie(name) {
			continue
		}
		kept = append(kept, strings.TrimSpace(part))
	}
	return strings.Join(kept, "; ")
}

// ownCookie reports whether a cookie's name is one of the proxy's, which
// start with meridian-, case aside.
func ownCookie(name string) bool { return strings.HasPrefix(strings.ToLower(name), "meridian-") }

// setCookie returns a Set-Cookie header's value of the service's as the
// browser gets it, and whether it does at all: held to the session's
// path, without a Domain, which could widen it, and none whose name is
// one of the proxy's, case aside, which the service can't set, nor any
// without a name, whose value a browser sends as a name and a value of
// its own, as =meridian-session-x=v does. Its other attributes are kept
// as they are.
func (p *Proxy) setCookie(value string) (string, bool) {
	parts := strings.Split(value, ";")
	name, _, named := strings.Cut(strings.TrimSpace(parts[0]), "=")
	if !named || strings.TrimSpace(name) == "" || ownCookie(strings.TrimSpace(name)) {
		return "", false
	}
	kept := []string{strings.TrimSpace(parts[0])}
	for _, attr := range parts[1:] {
		key, _, _ := strings.Cut(strings.TrimSpace(attr), "=")
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "domain", "path":
			continue
		}
		kept = append(kept, strings.TrimSpace(attr))
	}
	return strings.Join(append(kept, "Path="+p.Path()), "; "), true
}

// location returns a Location of the service's as the browser gets it: a
// path, which the service gives as though it were served at /, under the
// session's path, unless it is there already; one at the host the service
// was asked for, localhost, without a port, under the session's path, at
// the proxy's; and anything else, a relative path, which the browser reads
// against the session's path, or another site, another port of
// localhost's among them, as it is.
func (p *Proxy) location(loc string) string {
	u, err := url.Parse(loc)
	if err != nil {
		return loc
	}
	switch {
	case u.Scheme == "" && u.Host == "" && strings.HasPrefix(loc, "/") && !strings.HasPrefix(loc, "//"):
	case (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Host, upstreamHost):
		u.Scheme, u.Host, u.User = "", "", nil
	default:
		return loc
	}
	if u.Path == p.prefix || strings.HasPrefix(u.Path, p.prefix+"/") {
		return u.String()
	}
	u.Path = p.prefix + u.Path
	if u.RawPath != "" {
		u.RawPath = p.prefix + u.RawPath
	}
	return u.String()
}
