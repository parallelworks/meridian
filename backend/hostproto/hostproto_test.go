package hostproto

import (
	jsonv2 "encoding/json/v2"
	"slices"
	"testing"

	"github.com/parallelworks/meridian/backend"
	"github.com/parallelworks/meridian/problems"
)

// The wire's names are the protocol: a ready line, and a request whose
// members are empty, keep the members they have.
func TestWire(t *testing.T) {
	for _, tc := range []struct {
		v    any
		want string
	}{
		{Message{Protocol: Version, Ready: true}, `{"protocol":1,"ready":true}`},
		{Request{Member: &backend.Member{Name: "m", Backend: "ssh"}, Demand: &Demand{}}, `{"member":{"name":"m","backend":"ssh"},"demand":{}}`},
		{Request{Member: &backend.Member{Name: "m", Backend: "ssh"}, Demand: &Demand{Parked: []string{"a1"}}}, `{"member":{"name":"m","backend":"ssh"},"demand":{"parked":["a1"]}}`},
		{Message{Protocol: Version, Status: StatusOf(backend.Status{State: backend.Dead, End: backend.EndWithdrawn})}, `{"protocol":1,"status":{"state":"dead","end":"withdrawn"}}`},
		{Message{Protocol: Version, Error: "busy"}, `{"protocol":1,"error":"busy"}`},
	} {
		data, err := jsonv2.Marshal(tc.v, jsonv2.Deterministic(true))
		if err != nil || string(data) != tc.want {
			t.Errorf("%+v is %s, %v; want %s", tc.v, data, err, tc.want)
		}
	}
}

// What a Driver gives goes over the wire and back as it was.
func TestRoundTrip(t *testing.T) {
	for _, s := range []backend.Status{{State: backend.Unknown}, {State: backend.Queued}, {State: backend.Running}, {State: backend.Dead, End: backend.EndExited}} {
		if got := StatusOf(s).Status(); got != s {
			t.Errorf("%+v came back %+v", s, got)
		}
	}
	if got := (&Status{State: "lost"}).Status(); got.State != backend.Unknown {
		t.Errorf("a state the protocol doesn't have is %+v", got)
	}
	fit := backend.Fit{
		Problem:      problems.ResourcesExceedMember.Problem(problems.Params{"member": "m", "resource": "cpu", "asks": int64(2), "offers": int64(1)}),
		Availability: backend.HasCapacity, MaxWalltimeSeconds: 3600, MarginSeconds: 60, DefaultTimeoutSeconds: 600,
	}
	data, err := jsonv2.Marshal(FitOf(fit))
	if err != nil {
		t.Fatal(err)
	}
	var wire Fit
	if err := jsonv2.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	got := wire.Fit()
	if got.Problem == nil || got.Problem.Code != fit.Problem.Code || got.Availability != fit.Availability ||
		got.MaxWalltimeSeconds != fit.MaxWalltimeSeconds || got.MarginSeconds != fit.MarginSeconds || got.DefaultTimeoutSeconds != fit.DefaultTimeoutSeconds {
		t.Errorf("%+v came back %+v", fit, got)
	}
	p := Place{Attempt: "a", Name: "n", Coordinator: Endpoint{Address: "/s", KeyHash: "sha256:00"}, BootstrapToken: "t", TimeoutSeconds: 5, Resources: backend.Resources{GPU: 1}, TraceParent: "tp", Run: "1", Store: "s"}
	req := p.Request()
	if req.Attempt != "a" || req.Name != "n" || req.Coordinator != (backend.Endpoint{Address: "/s", KeyHash: "sha256:00"}) || req.BootstrapToken != "t" ||
		req.TimeoutSeconds != 5 || req.Resources.GPU != 1 || req.TraceParent != "tp" || req.RunDir != "" {
		t.Errorf("the place request is %+v", req)
	}
	if d := (&Demand{Resources: backend.Resources{Memory: 1}, TimeoutSeconds: 2, Parked: []string{"a1"}}).Demand(); d.Resources.Memory != 1 || d.TimeoutSeconds != 2 || !slices.Equal(d.Parked, []string{"a1"}) {
		t.Errorf("the demand is %+v", d)
	}
}
