package group

import (
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func TestFastestFallbackBeforeBlock(t *testing.T) {
	a := newTestMember(C.TypeVLESS, "a", dialRefused)
	slow := newTestMember(C.TypeVLESS, "slow", dialWorking)
	fast := newTestMember(C.TypeVLESS, "fast", dialWorking)
	block := newTestMember(C.TypeBlock, "block", dialRefused)
	_, g := newTestFallbackGroup(t, "", "", members(a), members(slow, fast, block), map[string]uint16{"slow": 300, "fast": 100})
	t.Cleanup(func() { _ = g.Close() })
	g.fallbackFastest = true
	g.performUpdateCheck()
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		assertSelected(t, g, network, fast)
	}
	setResult(g, "slow", 70)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, fast) // the existing tolerance still applies

	// A retained choice can move beyond block when a replacement changes the list.
	g.fallbacks = members(block, slow, fast)
	g.performUpdateCheck()
	assertSelected(t, g, N.NetworkTCP, block)
	assertSelected(t, g, N.NetworkUDP, block)
	require.Zero(t, block.dials.Load())
}

func TestRecoveryUsesRespondingPathBeforeBlock(t *testing.T) {
	for _, fastest := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordered", true: "fastest"}[fastest], func(t *testing.T) {
			a := newTestMember(C.TypeVLESS, "a", dialWorking)
			backup := newTestMember(C.TypeVLESS, "backup", dialWorking)
			block := newTestMember(C.TypeBlock, "block", dialRefused)
			_, g := newTestFallbackGroup(t, "", "", members(a), members(backup, block), map[string]uint16{"a": 10, "backup": 100})
			t.Cleanup(func() { _ = g.Close() })
			g.fallbackFastest = fastest
			g.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
			g.recoveryFailed("a")
			g.performUpdateCheck()
			assertSelected(t, g, N.NetworkTCP, backup)
			g.recoveryFailed("backup")
			g.performUpdateCheck()
			for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
				assertSelected(t, g, network, a)
			}
			setResult(g, "a", 0)
			g.performUpdateCheck()
			assertSelected(t, g, N.NetworkTCP, backup)
			setResult(g, "backup", 0)
			g.performUpdateCheck()
			assertSelected(t, g, N.NetworkTCP, block)
			assertSelected(t, g, N.NetworkUDP, block)
		})
	}
}

func TestTakeOverPreservesRecoveryOfSharedObjects(t *testing.T) {
	history := urltest.NewHistoryStorage()
	a := newTestMember(C.TypeVLESS, "a", dialWorking)
	backup := newTestMember(C.TypeVLESS, "backup", dialWorking)
	changed := newTestMember(C.TypeVLESS, "changed", dialWorking)
	removed := newTestMember(C.TypeVLESS, "removed", dialWorking)
	old := newSwapGroup(t, history, "", a, backup, changed, removed)
	old.group.outbounds, old.group.fallbacks = members(a), members(backup, changed, removed)
	old.group.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	old.group.roundDone.Store(true)
	base := time.Now().Add(-3 * time.Minute)
	for _, tag := range []string{"a", "changed", "removed"} {
		old.group.fallbackRecovery.members[tag] = &fallbackRecoveryMember{failuresAt: base}
		old.group.fallbackRecovery.answered(tag, base.Add(time.Second), 30*time.Second)
		old.group.fallbackRecovery.answered(tag, base.Add(31*time.Second), 30*time.Second)
	}
	for tag, delay := range map[string]uint16{"a": 10, "backup": 100, "changed": 200} {
		setResult(old.group, tag, delay)
	}
	old.group.performUpdateCheck()
	assertSelected(t, old.group, N.NetworkTCP, backup)

	replacement := newTestMember(C.TypeVLESS, "changed", dialWorking)
	next := newSwapGroup(t, history, "", a, backup, replacement)
	next.group.outbounds, next.group.fallbacks = members(a), members(backup, replacement)
	next.group.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	next.TakeOver(old)
	require.Contains(t, next.group.fallbackRecovery.members, "a")
	require.NotContains(t, next.group.fallbackRecovery.members, "changed", "same tag is another server")
	require.NotContains(t, next.group.fallbackRecovery.members, "removed")
	require.NotSame(t, old.group.fallbackRecovery.members["a"], next.group.fallbackRecovery.members["a"])
	next.group.checkUntested() // every current member already has a result
	assertSelected(t, next.group, N.NetworkTCP, backup)
	require.Equal(t, 2, next.group.fallbackRecovery.members["a"].successes)
	old.group.fallbackRecovery.answered("a", base.Add(61*time.Second), 30*time.Second)
	require.Equal(t, 2, next.group.fallbackRecovery.members["a"].successes, "retiring proof is isolated")
	next.group.fallbackRecovery.answered("a", base.Add(61*time.Second), 30*time.Second)
	next.group.performUpdateCheck()
	assertSelected(t, next.group, N.NetworkTCP, a)
	assertSelected(t, next.group, N.NetworkUDP, a)
}

func TestTakeOverRoundRecordsRecoveryFailures(t *testing.T) {
	link, _ := testURL(t, 0)
	history := urltest.NewHistoryStorage()
	a := newTestMember(C.TypeVLESS, "a", dialWorking)
	bad := newTestMember(C.TypeVLESS, "bad", dialRefused)
	history.StoreURLTestHistory("a", &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
	old := newSwapGroup(t, history, link, a)
	old.group.history.StoreURLTestHistory("a", history.LoadURLTestHistory("a"))
	next := newSwapGroup(t, history, link, a, bad)
	next.group.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	next.TakeOver(old)
	next.group.checkUntested()
	require.False(t, next.group.fallbackRecovered("bad"), "hot-start probes must retain the owner policy")
	require.Zero(t, a.dials.Load(), "unchanged tested member is not probed again")
	require.NotZero(t, bad.dials.Load())
}
