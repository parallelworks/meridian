package main

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/cmd/meridian/internal/local"
	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/coordinator"
	"github.com/parallelworks/meridian/internal/sessionproxy"
)

// TestPlatformEnv checks that PW_USER is the login a session's path holds,
// as the one read gives both, and none where there is none.
func TestPlatformEnv(t *testing.T) {
	parallel(t)
	if env := platformEnv(local.Login{Name: "alice"}); env["PW_USER"].Value != "alice" {
		t.Errorf("platformEnv = %v", env)
	}
	if env := platformEnv(local.Login{Err: errors.New("no such user")}); env != nil {
		t.Errorf("platformEnv without a login = %v", env)
	}
}

// TestClusterMembers checks the member of the ssh backend that reaches each
// cluster of the clusters file, which a session's connections to a
// cluster's service share, by the cluster's host.
func TestClusterMembers(t *testing.T) {
	parallel(t)
	cfg, err := local.LoadConfig(t.TempDir(), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if members, err := clusterMembers(cfg); err != nil || len(members) != 0 {
		t.Errorf("without a clusters file, the members are %v, %v", members, err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, local.ClustersFile), []byte("hpc:\n  host: farm\nlab:\n  host: bench\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = local.LoadConfig(dir, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	members, err := clusterMembers(cfg)
	if err != nil || len(members) != 2 || members["farm"] == nil || string(members["farm"].Settings) != `{"remoteHost":"farm"}` || members["farm"].Backend != "ssh" {
		t.Errorf("the clusters' members are %v, %v", members, err)
	}
}

// TestCoordinationClose checks that closing what coordinate opened closes
// its session host too, whose sessions' ports then listen no more, and
// before the coordinator, whose store the host's proxies redeem codes
// with.
func TestCoordinationClose(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	c := &cli{
		stdin: strings.NewReader(""), stdout: io.Discard, stderr: io.Discard,
		lookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
	}
	dirs, err := local.Locate(c.getenv)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := local.LoadConfig(dirs.Config, c.lookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	db, err := dirs.Database()
	if err != nil {
		t.Fatal(err)
	}
	co, err := coordinate(t.Context(), c, dirs, cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	s := coordinator.Session{ID: "abc", Root: "1", Run: "1", Key: "web", Name: "web-1", Label: sessionproxy.NewLabel(), Salt: make([]byte, sessionproxy.SaltLen)}
	sp, err := co.sessions.Point(t.Context(), s, compile.SessionUpdate{Name: s.Name, With: map[string]any{"remotePort": float64(1)}}, true)
	if err != nil {
		t.Fatal(err)
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(sp.Port))
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("the session's port doesn't listen: %v", err)
	}
	conn.Close()
	var closed []string
	closeSessions, closeCoord := co.closeSessions, co.closeCoord
	co.closeSessions = func() { closed = append(closed, "sessions"); closeSessions() }
	co.closeCoord = func() error { closed = append(closed, "coordinator"); return closeCoord() }
	if err := co.close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(closed, []string{"sessions", "coordinator"}) {
		t.Errorf("close closed %q, in that order", closed)
	}
	if conn, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		conn.Close()
		t.Error("once closed, the session's port listens")
	}
}
