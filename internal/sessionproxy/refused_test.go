package sessionproxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/meridian/problems"
)

// TestRefusedPages checks what a request is answered with whose
// connection the dial refused (DialFunc): 502, with a page of the problem
// it refused it with, of another user's service, or of a connection
// whose owner the system won't say, each of which names its port, but for
// a private session, or of a system that can't say who listens at all;
// and that the first two are logged once each for each place the session
// points, however often a browser retries, and of a private session only
// as their code.
func TestRefusedPages(t *testing.T) {
	foreign := fmt.Errorf("%w: user 4242 listens there", problems.SessionServiceForeign.Problem(problems.Params{"name": testName, "port": "8642"}))
	unknown := fmt.Errorf("%w: sockowner: the socket at the connection's other end is in TCP state 5", problems.SessionServiceOwnerUnknown.Problem(problems.Params{"name": testName, "port": "8642"}))
	unchecked := fmt.Errorf("%w: sockowner: NETLINK_SOCK_DIAG: permission denied", problems.SessionServiceUnchecked.Problem(problems.Params{"os": "linux"}))
	for _, tc := range []struct {
		name    string
		err     error
		k       *problems.Kind
		private bool
		shown   string
		// once is what each place the session points logs once, and
		// detail what the error says, which a private session's log
		// leaves out.
		once, detail string
	}{
		{"another user's", foreign, problems.SessionServiceForeign, false, "8642", "another user listens", "user 4242"},
		{"another user's, private", foreign, problems.SessionServiceForeign, true, problems.Masked, "another user listens", "user 4242"},
		{"owner unknown", unknown, problems.SessionServiceOwnerUnknown, false, "8642", "can't tell who has", "TCP state 5"},
		{"owner unknown, private", unknown, problems.SessionServiceOwnerUnknown, true, problems.Masked, "can't tell who has", "TCP state 5"},
		{"unchecked", unchecked, problems.SessionServiceUnchecked, false, "linux", "", "permission denied"},
		{"unchecked, private", unchecked, problems.SessionServiceUnchecked, true, "linux", "", "permission denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out lockedLog
			dial := func(context.Context) (net.Conn, error) { return nil, tc.err }
			r := newRig(t, nil, func(c *Config) {
				c.Private = tc.private
				c.Log = slog.New(slog.NewTextHandler(&out, nil))
				c.Dial = dial
			})
			cookie := r.login()
			for range 3 {
				resp, body := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}})
				wantPage(t, resp, body, http.StatusBadGateway, tc.k)
				if !strings.Contains(body, tc.shown) || tc.private && strings.Contains(body, "8642") {
					t.Errorf("the page is %q", body)
				}
			}
			logged := out.String()
			if tc.once != "" {
				if n := strings.Count(logged, tc.once); n != 1 {
					t.Errorf("three requests logged %d times:\n%s", n, logged)
				}
				r.p.Repoint(dial)
				r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}})
				if n := strings.Count(out.String(), tc.once); n != 2 {
					t.Errorf("pointed again, it logged %d times:\n%s", n, out.String())
				}
			} else if n := strings.Count(logged, "didn't answer"); n != 1 {
				t.Errorf("three requests logged %d times:\n%s", n, logged)
			}
			if tc.private == strings.Contains(out.String(), tc.detail) {
				t.Errorf("private %t, the log says:\n%s", tc.private, out.String())
			}
		})
	}
}

// TestDialToldOfGivingUp checks that a dial's context ends once the
// request it was dialed for gives up on it, as one whose browser went
// away does, so that a dial that waits, as for its service to accept its
// connection, stops, and gives its place back, rather than go on for what
// no request will use; and that a connection's goes on while it is
// open, and ends as it closes.
func TestDialToldOfGivingUp(t *testing.T) {
	ended := make(chan time.Duration, 1)
	var (
		dialCtx context.Context
		mine    DialFunc
	)
	r := newRig(t, nil, func(c *Config) {
		dial := c.Dial
		mine = func(ctx context.Context) (net.Conn, error) {
			if dialCtx == nil {
				dialCtx = ctx
				return dial(ctx)
			}
			start := time.Now()
			<-ctx.Done()
			ended <- time.Since(start)
			return nil, ctx.Err()
		}
		c.Dial = mine
	})
	cookie := r.login()
	// The first request's connection is kept, and its context with it.
	if resp, _ := r.do("GET", prefix+"/x", http.Header{"Cookie": {cookie}}); resp.StatusCode != http.StatusOK {
		t.Fatalf("the first request answers %s", resp.Status)
	}
	if dialCtx.Err() != nil {
		t.Error("the context of a connection the transport keeps ended")
	}
	// Pointed elsewhere, the session's kept connection closes, and its
	// context ends with it, rather than live on as the proxy's.
	r.p.Repoint(mine)
	select {
	case <-dialCtx.Done():
	case <-time.After(5 * time.Second):
		t.Error("the context of a connection that closed goes on")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.addr+prefix+"/x", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = r.host
	req.Header.Set("Cookie", cookie)
	if resp, err := r.client.Do(req); err == nil {
		resp.Body.Close()
	}
	select {
	case took := <-ended:
		if took > dialWait/2 {
			t.Errorf("the dial was told after %v", took)
		}
	case <-time.After(dialWait):
		t.Error("the dial of a request that gave up on it went on")
	}
}
