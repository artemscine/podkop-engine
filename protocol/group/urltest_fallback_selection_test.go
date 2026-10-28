package group

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func TestFallbackSelectionFastestAndTolerance(t *testing.T) {
	link, _ := testURL(t, 0)
	a := newTestMember(C.TypeVLESS, "a", dialWorking)
	f1 := newTestMember(C.TypeVLESS, "f1", dialWorking)
	f2 := newTestMember(C.TypeVLESS, "f2", dialWorking)
	_, g := newTestFallbackGroup(t, link, "", members(a), members(f1, f2), map[string]uint16{"a": 900, "f1": 300, "f2": 100})
	g.fallbackFastest = true
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, a)
	setResult(g, "a", 0)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, f2)
	assertSelected(t, g, N.NetworkUDP, f2)
	setResult(g, "f1", 70) // within the existing 50 ms tolerance
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, f2)
	setResult(g, "f1", 40)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, f1)
	setResult(g, "f1", 0)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, f2)
	setResult(g, "a", 900)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, a)
}

func TestFallbackSelectionRecoveryAndFrozen(t *testing.T) {
	g, nodes := fallbackFixture(t)
	g.fallbackFastest = true
	delay := badoption.Duration(time.Minute)
	g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{Successes: 3, MinHealthyTime: &delay})
	g.history.DeleteURLTestHistory(nodes[0].Tag())
	setResult(g, nodes[1].Tag(), 300)
	setResult(g, nodes[2].Tag(), 10)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, nodes[2])
	g.recoveryFailed(nodes[0].Tag())
	setResult(g, nodes[0].Tag(), 1)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, nodes[2])
	g.setFrozen(nodes[2].Tag(), time.Now())
	chosen, alive := g.selectAmong(N.NetworkTCP, g.skipFrozen(nil))
	require.True(t, alive)
	require.Equal(t, adapter.Outbound(nodes[1]), chosen)
	g.history.DeleteURLTestHistory(nodes[1].Tag())
	g.history.DeleteURLTestHistory(nodes[2].Tag())
	chosen, alive = g.Select(N.NetworkTCP)
	require.True(t, alive)
	require.Equal(t, adapter.Outbound(nodes[0]), chosen, "only responding recovering member remains usable")
}

func TestFallbackSelectionConfiguration(t *testing.T) {
	_, err := NewURLTest(context.Background(), nil, log.NewNOPFactory().NewLogger("test"), "group", option.URLTestOutboundOptions{
		Outbounds: []string{"a"}, Fallbacks: []string{"b"}, FallbackSelection: "invalid",
	})
	require.ErrorContains(t, err, "unknown fallback_selection")
}

type fallbackNetworkGroup struct {
	*testGroup
	udp adapter.Outbound
}

func (g *fallbackNetworkGroup) All() []string {
	return []string{g.member.Tag(), g.udp.Tag()}
}

func (g *fallbackNetworkGroup) Selected(network string) adapter.Outbound {
	if network == N.NetworkUDP {
		return g.udp
	}
	return g.member
}

// Recovery and the fastest fallback must judge the nested member selected for the
// requested network: a recovering TCP path need not disqualify its healthy UDP path.
func TestFallbackSelectionRecoveryUsesNestedMemberForNetwork(t *testing.T) {
	a := newTestMember(C.TypeVLESS, "a", dialWorking)
	tcp := newTestMember(C.TypeVLESS, "tcp", dialWorking)
	udp := newTestMember(C.TypeVLESS, "udp", dialWorking)
	selector := &fallbackNetworkGroup{
		testGroup: &testGroup{testMember: newTestMember(C.TypeSelector, "selector", dialWorking), member: tcp},
		udp:       udp,
	}
	f := newTestMember(C.TypeVLESS, "f", dialWorking)
	_, g := newTestFallbackGroup(t, "", "", members(a), []adapter.Outbound{selector, f}, map[string]uint16{"tcp": 10, "udp": 20, "f": 100})
	g.fallbackFastest = true
	g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	g.recoveryFailed(tcp.Tag())
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, f)
	assertSelected(t, g, N.NetworkUDP, selector)
}
