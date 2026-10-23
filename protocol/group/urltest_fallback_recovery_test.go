package group

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestFallbackRecoveryConfirmationAndReset(t *testing.T) {
	delay := badoption.Duration(time.Minute)
	r := newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{Successes: 3, MinHealthyTime: &delay})
	base := time.Now().Add(-5 * time.Minute)
	r.members["a"] = &fallbackRecoveryMember{failuresAt: base}
	r.answered("a", base.Add(time.Second), 30*time.Second)
	if r.recovered("a", base.Add(time.Minute)) {
		t.Fatal("one answer is not recovery")
	}
	// Repeated manual tests are not independent scheduled successes.
	for i := 2; i < 20; i++ {
		r.answered("a", base.Add(time.Duration(i)*time.Second), 30*time.Second)
	}
	if r.recovered("a", base.Add(2*time.Minute)) {
		t.Fatal("manual probe burst bypassed confirmation")
	}
	r.answered("a", base.Add(31*time.Second), 30*time.Second)
	r.answered("a", base.Add(61*time.Second), 30*time.Second)
	if !r.recovered("a", base.Add(61*time.Second)) {
		t.Fatal("stable recovery was not admitted")
	}
	r.failed("a")
	r.answered("a", base.Add(91*time.Second), 30*time.Second) // stale proof from before failure
	if r.recovered("a", time.Now().Add(time.Hour)) {
		t.Fatal("time or stale history erased a new failure")
	}
	if !r.recovered("never-failed", time.Now()) {
		t.Fatal("healthy cold start must not wait")
	}
}

func TestFallbackRecoveryHoldsReserveUntilStable(t *testing.T) {
	g, nodes := fallbackFixture(t)
	delay := badoption.Duration(time.Minute)
	g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{Successes: 3, MinHealthyTime: &delay})
	g.history.DeleteURLTestHistory(nodes[0].Tag())
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, nodes[1])
	base := time.Now().Add(-3 * time.Minute)
	g.fallbackRecovery.members[nodes[0].Tag()] = &fallbackRecoveryMember{failuresAt: base}
	for index := 0; index < 3; index++ {
		at := base.Add(time.Second + time.Duration(index)*30*time.Second)
		g.history.StoreURLTestHistory(nodes[0].Tag(), &adapter.URLTestHistory{Time: at, Delay: 1})
		g.fallbackRecovery.answered(nodes[0].Tag(), at, 30*time.Second)
		g.performUpdateCheck()
		want := nodes[1]
		if index == 2 {
			want = nodes[0]
		}
		assertSelected(t, g, N.NetworkTCP, want)
	}
}

func TestFallbackRecoveryDoesNotStrandAllDownGroup(t *testing.T) {
	g, nodes := fallbackFixture(t)
	g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	g.fallbackRecovery.failed(nodes[0].Tag())
	g.history.DeleteURLTestHistory(nodes[1].Tag())
	g.history.DeleteURLTestHistory(nodes[2].Tag())
	chosen, alive := g.Select(N.NetworkTCP)
	if !alive || chosen != nodes[0] {
		t.Fatal("only responding path must remain usable")
	}
}

func TestFallbackRecoveryConfiguration(t *testing.T) {
	negative := badoption.Duration(-time.Second)
	for _, options := range []option.URLTestOutboundOptions{
		{Outbounds: []string{"a"}, FallbackRecovery: &option.URLTestFallbackRecoveryOptions{}},
		{Outbounds: []string{"a"}, Fallbacks: []string{"b"}, FallbackRecovery: &option.URLTestFallbackRecoveryOptions{Successes: -1}},
		{Outbounds: []string{"a"}, Fallbacks: []string{"b"}, FallbackRecovery: &option.URLTestFallbackRecoveryOptions{MinHealthyTime: &negative}},
	} {
		if validateFallbackRecovery(options) == nil {
			t.Fatal("invalid confirmation settings accepted")
		}
	}
	if newFallbackRecovery(nil) != nil {
		t.Fatal("confirmation must be opt-in")
	}
}

func TestFallbackRecoveryNeedsProofAcrossHealthyWindow(t *testing.T) {
	delay := badoption.Duration(time.Minute)
	r := newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{Successes: 1, MinHealthyTime: &delay})
	base := time.Now().Add(-5 * time.Minute)
	r.members["a"] = &fallbackRecoveryMember{failuresAt: base}
	r.answered("a", base.Add(time.Second), time.Second)
	if r.recovered("a", time.Now()) {
		t.Fatal("elapsed time without another proof is not continuous recovery")
	}
	r.answered("a", base.Add(61*time.Second), time.Second)
	if !r.recovered("a", time.Now()) {
		t.Fatal("proof spanning the healthy window was not admitted")
	}
}

func fallbackFixture(t *testing.T) (*URLTestGroup, []*testMember) {
	a := newTestMember("vless", "a", dialWorking)
	b := newTestMember("vless", "b", dialWorking)
	c := newTestMember("vless", "c", dialWorking)
	_, g := newTestFallbackGroup(t, "", "", members(a), members(b, c), map[string]uint16{"a": 10, "b": 20, "c": 30})
	return g, []*testMember{a, b, c}
}
func assertSelected(t *testing.T, g *URLTestGroup, network string, want adapter.Outbound) {
	t.Helper()
	selected := g.selectedOutboundTCP.Load()
	if network == N.NetworkUDP {
		selected = g.selectedOutboundUDP.Load()
	}
	if selected != want {
		t.Fatalf("selected %v, want %v", selected, want)
	}
}

func TestFallbackRecoveryIgnoresCanceledAndNetworkResetFailures(t *testing.T) {
	g, nodes := fallbackFixture(t)
	g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.memberFailed(ctx, nodes[0], N.NetworkTCP, syscall.ECONNREFUSED, time.Now())
	if len(g.fallbackRecovery.members) != 0 {
		t.Fatal("caller cancellation changed recovery")
	}
	start := time.Now()
	g.networkReset()
	g.memberFailed(context.Background(), nodes[0], N.NetworkTCP, syscall.ECONNREFUSED, start)
	if len(g.fallbackRecovery.members) != 0 {
		t.Fatal("failure across network reset changed recovery")
	}
}

func TestFallbackRecoveryRejectedProbeResetsProof(t *testing.T) {
	link, _ := testURL(t, 0)
	a := newTestMember("vless", "a", dialRefused)
	b := newTestMember("vless", "b", dialWorking)
	_, g := newTestFallbackGroup(t, link, "", members(a), members(b), map[string]uint16{"a": 10, "b": 20})
	g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	g.CheckOutbounds(context.Background(), true)
	if g.fallbackRecovered("a") {
		t.Fatal("failed periodic probe did not start recovery")
	}
}

func TestFallbackRecoveryIgnoresCancelledManualProbe(t *testing.T) {
	for _, responds := range []bool{true, false} {
		t.Run(map[bool]string{true: "success", false: "failure"}[responds], func(t *testing.T) {
			link, _ := testURL(t, 0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a := newTestMember("vless", "a", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
				cancel()
				if responds {
					return dialWorking(ctx, network, destination)
				}
				return dialRefused(ctx, network, destination)
			})
			b := newTestMember("vless", "b", dialWorking)
			_, g := newTestFallbackGroup(t, link, "", members(a), members(b), map[string]uint16{"a": 10, "b": 20})
			g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
			g.fallbackRecovery.failed("a")
			before := g.fallbackRecovery.members["a"].failuresAt
			history := g.history.LoadURLTestHistory("a")
			_, _ = g.urlTestRound(ctx, true, true)
			member := g.fallbackRecovery.members["a"]
			if member.successes != 0 || !member.failuresAt.Equal(before) || g.history.LoadURLTestHistory("a") != history {
				t.Fatal("cancelled manual probe changed recovery or history")
			}
		})
	}
}
