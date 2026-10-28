package sshname_test

import (
	"testing"

	"github.com/parallelworks/meridian/internal/sshname"
)

// TestValid checks which hosts and users are safe to give ssh, as OpenSSH
// 10.1's valid_hostname and valid_ruser say: a user's control characters
// too, which 9.6's took.
func TestValid(t *testing.T) {
	for host, want := range map[string]bool{
		"farm": true, "farm-1.example.org": true, "10.0.0.5:2222": true, "[::1]:22": true, "": true, "héte": true,
		"-oProxyCommand=x": false, "a;b": false, "a b": false, "a\tb": false, "a$b": false, "a`b`": false, "a|b": false,
		"a&b": false, "a>b": false, "a(b)": false, "a{b}": false, "a'b": false, "a\"b": false, "a\\b": false, "a\x01b": false, "a\x7fb": false,
		"a\nproxycommand touch p": false, "a\rb": false, "a\x00b": false, "a\x1b[31m": false,
	} {
		if got := sshname.ValidHost(host); got != want {
			t.Errorf("ValidHost(%q) = %v, want %v", host, got, want)
		}
	}
	for user, want := range map[string]bool{
		"": true, "alice": true, "a.b-c_d": true, "host$": true, "a b": true, "a\\b": true,
		"-l": false, "a -b": false, "a;b": false, "a`b": false, "a(b": false, "a'b": false, "a\\": false, "a|b": false,
		"x\nproxycommand touch p": false, "a\rb": false, "a\x00b": false, "a\x1b[31m": false, "a\x7f": false, "a\tb": false,
	} {
		if got := sshname.ValidUser(user); got != want {
			t.Errorf("ValidUser(%q) = %v, want %v", user, got, want)
		}
	}
}
