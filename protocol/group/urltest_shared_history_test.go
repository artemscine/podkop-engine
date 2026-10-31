package group

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func newSharedHistoryGroup(t *testing.T, history *urltest.HistoryStorage, link string, interval time.Duration, primary, reserve *testMember) *URLTest {
	t.Helper()
	ctx := service.ContextWithPtr(context.Background(), history)
	manager := &testOutboundManager{members: map[string]adapter.Outbound{primary.Tag(): primary, reserve.Tag(): reserve}}
	g, err := NewURLTestGroup(ctx, manager, log.NewNOPFactory().Logger(), members(primary), members(reserve), link, "", interval, 50, 0, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = g.Close() })
	return &URLTest{group: g, logger: log.NewNOPFactory().NewLogger("urltest")}
}

func TestSharedProxyChecksKeepIndependentURLResults(t *testing.T) {
	goodURL, goodRequests := testURL(t, 0)
	var badRequests atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badRequests.Add(1)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	t.Cleanup(bad.Close)
	history := urltest.NewHistoryStorage()
	primary := newTestMember(C.TypeVLESS, "shared", dialWorking)
	reserve := newTestMember(C.TypeBlock, "block", dialRefused)
	a := newSharedHistoryGroup(t, history, goodURL, time.Minute, primary, reserve)
	b := newSharedHistoryGroup(t, history, bad.URL, 2*time.Minute, primary, reserve)
	require.Empty(t, a.MemberHistories(), "unknown members are not tested failures")
	a.group.CheckOutbounds(context.Background(), false)
	require.EqualValues(t, 1, goodRequests.Load())
	require.NotNil(t, history.LoadURLTestHistory("shared"), "dashboard still receives ordinary proxy history")
	b.group.CheckOutbounds(context.Background(), false)
	require.EqualValues(t, 1, badRequests.Load(), "another group's fresh result must not skip this URL")
	_, aliveA := a.group.Select(N.NetworkTCP)
	selectedB, aliveB := b.group.Select(N.NetworkTCP)
	require.True(t, aliveA, "a failed check of another URL must not erase this group's evidence")
	require.True(t, aliveB)
	require.Equal(t, "block", selectedB.Tag())
	require.Len(t, a.MemberHistories()["shared"], 1)
	failed, tested := b.MemberHistories()["shared"]
	require.True(t, tested)
	require.Empty(t, failed)
	a.group.CheckOutbounds(context.Background(), false)
	require.EqualValues(t, 1, goodRequests.Load(), "own fresh evidence still suppresses redundant checks")
}

func TestSharedProxyRecoveryCountsEachGroupsOwnEvidence(t *testing.T) {
	link, _ := testURL(t, 0)
	var online atomic.Bool
	primary := newTestMember(C.TypeVLESS, "shared", func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		if !online.Load() {
			return nil, syscall.ECONNREFUSED
		}
		return dialWorking(ctx, network, destination)
	})
	reserve := newTestMember(C.TypeVLESS, "reserve", dialWorking)
	history := urltest.NewHistoryStorage()
	a := newSharedHistoryGroup(t, history, link, time.Minute, primary, reserve)
	b := newSharedHistoryGroup(t, history, link, 2*time.Minute, primary, reserve)
	delay := badoption.Duration(0)
	for _, s := range []*URLTest{a, b} {
		s.group.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{Successes: 1, MinHealthyTime: &delay})
		s.group.CheckOutbounds(context.Background(), false)
		assertSelected(t, s.group, N.NetworkTCP, reserve)
	}
	online.Store(true)
	a.group.CheckOutbounds(context.Background(), false)
	assertSelected(t, a.group, N.NetworkTCP, primary)
	require.False(t, b.group.fallbackRecovered("shared"), "another group cannot manufacture recovery proof")
	b.group.CheckOutbounds(context.Background(), false)
	assertSelected(t, b.group, N.NetworkTCP, primary)
	for _, s := range []*URLTest{a, b} {
		require.Equal(t, 1, s.group.fallbackRecovery.members["shared"].successes)
	}
}

func TestSharedProxyHistoryOmitsInconclusiveNetworkFailure(t *testing.T) {
	primary := newTestMember(C.TypeVLESS, "shared", func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, syscall.ENETUNREACH
	})
	reserve := newTestMember(C.TypeBlock, "block", dialRefused)
	s := newSharedHistoryGroup(t, urltest.NewHistoryStorage(), "http://example.invalid", time.Minute, primary, reserve)
	s.group.fallbackRecovery = newFallbackRecovery(&option.URLTestFallbackRecoveryOptions{})
	s.group.CheckOutbounds(context.Background(), false)
	require.True(t, s.group.roundDone.Load())
	require.NotContains(t, s.MemberHistories(), "shared", "a deferred local-network judgment is still unknown")
}

func TestTakeOverSharedHistoryRequiresSameCheckAndMember(t *testing.T) {
	firstURL, _ := testURL(t, 0)
	otherURL, otherRequests := testURL(t, 0)
	history := urltest.NewHistoryStorage()
	primary := newTestMember(C.TypeVLESS, "shared", dialWorking)
	reserve := newTestMember(C.TypeBlock, "block", dialRefused)
	old := newSharedHistoryGroup(t, history, firstURL, time.Minute, primary, reserve)
	old.group.CheckOutbounds(context.Background(), false)
	next := newSharedHistoryGroup(t, history, otherURL, time.Minute, primary, reserve)
	next.TakeOver(old)
	require.Nil(t, next.group.history.LoadURLTestHistory("shared"), "a changed URL needs new evidence")
	next.group.checkUntested()
	require.EqualValues(t, 1, otherRequests.Load())
	unchanged := newSharedHistoryGroup(t, history, otherURL, time.Minute, primary, reserve)
	unchanged.TakeOver(next)
	unchanged.group.checkUntested()
	require.EqualValues(t, 1, otherRequests.Load(), "identical members and checks retain accepted evidence")
	changed := newTestMember(C.TypeVLESS, "shared", dialWorking)
	replacement := newSharedHistoryGroup(t, history, otherURL, time.Minute, changed, reserve)
	replacement.TakeOver(unchanged)
	require.Nil(t, replacement.group.history.LoadURLTestHistory("shared"), "tag reuse is not object identity")
	replacement.group.checkUntested()
	require.EqualValues(t, 2, otherRequests.Load())
}
