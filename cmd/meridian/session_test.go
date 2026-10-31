package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// envSessionService has TestMain run the test binary as a session's
// service (sessionService), which writes its port to the file it names.
const envSessionService = "MERIDIAN_TEST_SESSION_SERVICE"

// sessionService serves HTTP on a port of 127.0.0.1 the system picks,
// which it writes to file, answering each request with its path, until a
// request for /quit, which it answers before it ends.
func sessionService(file string) int {
	// TestMain runs it, as a process of its own, which has no other.
	ctx := context.Background() //nolint:forbidigo // the service process's own
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	quit := make(chan struct{})
	var once sync.Once
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "the service saw %s", r.URL.Path)
		if r.URL.Path == "/quit" {
			once.Do(func() { close(quit) })
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	if err := os.WriteFile(file+".new", []byte(port), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := os.Rename(file+".new", file); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	<-quit
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	return 0
}

// sessionWorkflow is a workflow whose job service runs a session's
// service, the test binary its input bin names, on this machine, or on
// the cluster its input names; and whose job point waits for the
// service's port and points the session web there.
func sessionWorkflow(cluster bool) string {
	inputs, ssh, target, dir := "", "", "", `"$PW_JOB_DIR"`
	if cluster {
		inputs = "\n      cluster: {type: compute-clusters}"
		ssh = "\n    ssh: {remoteHost: \"${{ inputs.cluster.ip }}\"}"
		target = "\n          target: ${{ inputs.cluster.id }}"
		dir = `"$HOME"`
	}
	return `on:
  execute:
    inputs:
      bin: {type: string}` + inputs + `
sessions:
  web: {}
jobs:
  service:` + ssh + `
    steps:
      - run: ` + envSessionService + `=` + dir + `/service-port "${{ inputs.bin }}"
  point:` + ssh + `
    steps:
      - id: port
        run: |
          while [ ! -s ` + dir + `/service-port ]; do sleep 0.05; done
          echo "PORT=$(cat ` + dir + `/service-port)" >> "$OUTPUTS"
      - uses: parallelworks/update-session
        with:
          name: ${{ sessions.web }}` + target + `
          remotePort: ${{ steps.port.outputs.PORT }}
`
}

// sessionHTTP returns a client that reaches the port of 127.0.0.1 a
// session is served at, whatever name a URL gives, as a browser resolves
// <label>.localhost, keeps cookies, as a browser does, and follows no
// redirect.
func sessionHTTP(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{
		Jar: jar,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			_, port, _ := net.SplitHostPort(addr)
			return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
		}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       time.Minute,
	}
}

// sessionGet returns what client gets of u: its status and its body.
func sessionGet(t *testing.T, client *http.Client, u string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// checkSessionServed checks a run of sessionWorkflow, which meridian run
// with args coordinates, as setUp sets it up, in the environment env:
// meridian run says its session is served, and how to open it, without a
// link, as its output is no terminal; meridian session url prints a link,
// whose one-time code gives the browser the session's cookie, with which
// it reaches the service; meridian session reset signs it out; and once
// the service, asked to, ends, and with it the run, nothing listens at
// the session's port.
func checkSessionServed(t *testing.T, env map[string]string, setUp func(*cli), args ...string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := startCommand(t, env, setUp, append([]string{"run", "--input", "bin=" + self}, args...)...)
	ready := "session web of run 1 is ready: meridian session url 1 web\n"
	for deadline := time.Now().Add(3 * time.Minute); !strings.Contains(cmd.stdout.String(), ready); {
		select {
		case r := <-cmd.done:
			t.Fatalf("meridian run ended before its session was served: %+v", r)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("the session isn't served:\n%s%s", cmd.stdout.String(), cmd.stderr.String())
		}
	}
	session := func(args ...string) result {
		t.Helper()
		return <-startCommand(t, env, setUp, append([]string{"session"}, args...)...).done
	}
	link := func() string {
		t.Helper()
		r := session("url", "1", "web")
		if r.code != exitOK || r.stderr != "" {
			t.Fatalf("meridian session url = %+v", r)
		}
		return strings.TrimSpace(r.stdout)
	}
	first := link()
	u, err := url.Parse(first)
	if err != nil || u.Query().Get("meridian-code") == "" || !strings.HasSuffix(u.Hostname(), ".localhost") {
		t.Fatalf("meridian session url printed %q", first)
	}
	base := u.Scheme + "://" + u.Host + u.Path
	client := sessionHTTP(t)
	if code, _ := sessionGet(t, client, base+"hello"); code != http.StatusUnauthorized {
		t.Errorf("without the link, the session answers %d", code)
	}
	if code, _ := sessionGet(t, client, first); code != http.StatusOK {
		t.Fatalf("the link answers %d", code)
	}
	if code, body := sessionGet(t, client, base+"hello"); code != http.StatusOK || body != "the service saw /hello" {
		t.Errorf("with the cookie, the session answers %d %q", code, body)
	}
	if code, _ := sessionGet(t, sessionHTTP(t), first); code != http.StatusUnauthorized {
		t.Errorf("a link used once answers another browser %d", code)
	}
	if r := session("reset", "1", "web"); r.code != exitOK || r.stdout != "" || r.stderr != "" {
		t.Fatalf("meridian session reset = %+v", r)
	}
	// Another process reset it: the run's own sees it as it next looks.
	waitUntil(t, "the reset to sign the browser out", func() bool {
		code, _ := sessionGet(t, client, base+"hello")
		return code == http.StatusUnauthorized
	})
	if code, _ := sessionGet(t, client, link()); code != http.StatusOK {
		t.Fatalf("a link after the reset answers %d", code)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"url", "1", "nothing"}, "session_key_unknown"},
		{[]string{"url", "2", "web"}, "run_not_found"},
		{[]string{"url", "1"}, "arguments_invalid"},
	} {
		if r := session(tc.args...); r.code != exitError || !strings.Contains(r.stderr, tc.want) {
			t.Errorf("meridian session %s = %+v, want %s", strings.Join(tc.args, " "), r, tc.want)
		}
	}
	if code, body := sessionGet(t, client, base+"quit"); code != http.StatusOK || body != "the service saw /quit" {
		t.Errorf("the service, asked to end, answers %d %q", code, body)
	}
	r := endedWithin(t, cmd)
	if r.code != exitOK || !strings.HasSuffix(r.stdout, "run 1 finished: success\n") {
		t.Fatalf("meridian run of a session = %+v", r)
	}
	if conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", u.Port()), time.Second); err == nil {
		conn.Close()
		t.Error("once its run ended, the session's port still listens")
	}
	if r := session("url", "1", "web"); r.code != exitError || !strings.Contains(r.stderr, "session_key_unknown") {
		t.Errorf("meridian session url of an ended session = %+v", r)
	}
}

// TestSession checks a session that points at a service of this machine,
// the workspace, as meridian run serves it.
func TestSession(t *testing.T) {
	parallel(t)
	checkSessionServed(t, runEnv(t), nil, writeFile(t, "w.yaml", sessionWorkflow(false)))
}

// TestSSHSession checks a session that points at a service on a cluster of
// clusters.yaml, which meridian reaches over SSH through the cluster's
// host, as its jobs' workers are.
func TestSSHSession(t *testing.T) {
	parallel(t)
	env := runEnv(t)
	h := newSSHHost(t, env)
	checkSessionServed(t, env, h.setUp, "--input", "cluster=hpc", writeFile(t, "w.yaml", sessionWorkflow(true)))
}
