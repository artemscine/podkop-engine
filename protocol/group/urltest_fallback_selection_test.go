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
