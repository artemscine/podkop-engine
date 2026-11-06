package group

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/probesock"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// The weight is the crossing of the prices of report 02: a server 20 ms away with 0.8 % of
// loss costs what a clean one 50 ms away does (delays in proportion to the RTT).
func TestWeighDelayPrice(t *testing.T) {
	near := func(percent float64) uint16 { return weighDelay(40, percent, 20*time.Millisecond) }
	require.Equal(t, uint16(40), near(0))
	require.InDelta(t, 100, float64(near(0.8)), 3)
	require.Less(t, near(0.5), uint16(100))
	require.Greater(t, near(1.2), uint16(100))
	require.Equal(t, uint16(65535), weighDelay(60000, 50, time.Millisecond))
}

func TestGroupModeOptions(t *testing.T) {
	mode, options, err := groupMode("", "", 0, 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, modeFastest, mode)
	_, _, err = groupMode("other", "", 0, 0, 0, 0)
	require.Error(t, err)
	_, _, err = groupMode(modeLoss, "", 0, 0, 0, 0)
	require.ErrorContains(t, err, "loss_url")
	_, _, err = groupMode(modeLoss, "ftp://host/file", 0, 0, 0, 0)
	require.Error(t, err)
	_, _, err = groupMode(modeLoss, "https://host/file", 30*time.Second, 0, 0, 0)
	require.ErrorContains(t, err, "loss_interval")
	_, _, err = groupMode(modeBalance, "https://host/file", 0, 120, 0, 0)
	require.ErrorContains(t, err, "loss_threshold")
	mode, options, err = groupMode(modeBalance, "https://host/file", 0, 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, modeBalance, mode)
	require.Equal(t, defaultLossInterval, options.interval)
	require.Equal(t, defaultLossThreshold, options.threshold)
	require.Equal(t, defaultBalanceSize, options.balanceSize)
	require.Equal(t, defaultBalanceIdle, options.balanceIdle)
}

// storeLoss gives a member a count of lost of segments, measured now over a path with rtt.
func storeLoss(group *URLTestGroup, tag string, segments, lost int, rtt time.Duration) {
	group.history.StoreLossSample(tag, urltest.LossSample{At: time.Now(), Received: urltest.LossProbeLarge, Segments: segments, Lost: lost, RTT: rtt})
}

func newModeGroup(t *testing.T, mode string, outbounds []adapter.Outbound, delays map[string]uint16, prepare func(group *URLTestGroup)) (*URLTest, *URLTestGroup) {
	urlTest, group := newTestFallbackGroup(t, "", "", outbounds, nil, nil)
	var err error
	group.mode, group.loss, err = groupMode(mode, "http://127.0.0.1:1/loss", 0, 0, 0, 0)
	require.NoError(t, err)
	urlTest.mode, urlTest.loss = group.mode, group.loss
	if mode == modeBalance {
		group.balance = newBalancer()
	}
	if prepare != nil {
		prepare(group)
	}
	for tag, delay := range delays {
		setResult(group, tag, delay)
	}
	group.performUpdateCheck()
	return urlTest, group
}

// A faster member that loses 3 % is passed over for a clean one a little slower.
func TestLossModeAvoidsLossyMember(t *testing.T) {
	a, b, c := newTestMember(C.TypeSOCKS, "a", dialWorking), newTestMember(C.TypeSOCKS, "b", dialWorking), newTestMember(C.TypeSOCKS, "c", dialWorking)
	_, group := newModeGroup(t, modeLoss, members(a, b, c), map[string]uint16{"a": 100, "b": 150, "c": 300}, func(group *URLTestGroup) {
		storeLoss(group, "a", 1000, 30, 30*time.Millisecond)
		storeLoss(group, "b", 1000, 0, 50*time.Millisecond)
	})
	tcp, _ := selectedTags(group)
	require.Equal(t, "b", tcp)
	// fastest goes by the delay alone
	_, fastest := newModeGroup(t, modeFastest, members(a, b, c), map[string]uint16{"a": 100, "b": 150, "c": 300}, func(group *URLTestGroup) {
		storeLoss(group, "a", 1000, 30, 30*time.Millisecond)
	})
	tcp, _ = selectedTags(fastest)
	require.Equal(t, "a", tcp)
}

// Loss every counted member shows is the line's: the fastest stays.
func TestLossModeLineLoss(t *testing.T) {
	a, b, c := newTestMember(C.TypeSOCKS, "a", dialWorking), newTestMember(C.TypeSOCKS, "b", dialWorking), newTestMember(C.TypeSOCKS, "c", dialWorking)
	_, group := newModeGroup(t, modeLoss, members(a, b, c), map[string]uint16{"a": 100, "b": 150, "c": 200}, func(group *URLTestGroup) {
		storeLoss(group, "a", 1000, 20, 30*time.Millisecond)
		storeLoss(group, "b", 1000, 20, 50*time.Millisecond)
		storeLoss(group, "c", 1000, 21, 60*time.Millisecond)
	})
	tcp, _ := selectedTags(group)
	require.Equal(t, "a", tcp)
}

// A window with too few segments decides nothing.
func TestLossModeTooFewSegments(t *testing.T) {
	a, b := newTestMember(C.TypeSOCKS, "a", dialWorking), newTestMember(C.TypeSOCKS, "b", dialWorking)
	_, group := newModeGroup(t, modeLoss, members(a, b), map[string]uint16{"a": 100, "b": 150}, func(group *URLTestGroup) {
		storeLoss(group, "a", 180, 10, 30*time.Millisecond)
	})
	tcp, _ := selectedTags(group)
	require.Equal(t, "a", tcp)
}

// dialProbe dials as a member over the default dialer would: a probe context gets its
// probe connection.
func dialProbe(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if socket := probesock.From(ctx); socket != nil {
		return socket.Dial(ctx, net.Dialer{}, network, destination.String())
	}
	return dialWorking(ctx, network, destination)
}

func lossURL(t *testing.T, requests *int) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*requests++
		w.Header().Set("Content-Length", fmt.Sprint(urltest.LossProbeLarge))
		w.Write(make([]byte, urltest.LossProbeLarge))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// A member that became faster is probed before the switch; the switch follows a clean count,
// or one that could not count (the member is then chosen by its delay).
func TestLossModeProbesBeforeSwitch(t *testing.T) {
	requests := 0
	link := lossURL(t, &requests)
	a, b := newTestMember(C.TypeSOCKS, "a", dialProbe), newTestMember(C.TypeSOCKS, "b", dialProbe)
	_, group := newModeGroup(t, modeLoss, members(a, b), map[string]uint16{"a": 100, "b": 300}, nil)
	group.loss.link = link
	tcp, _ := selectedTags(group)
	require.Equal(t, "a", tcp)
	setResult(group, "b", 20)
	group.performUpdateCheck()
	tcp, _ = selectedTags(group)
	require.Equal(t, "b", tcp)
	require.Equal(t, 1, requests)
	_, counted := group.history.LastLossSample("b")
	notCounted := group.history.LossNotCounted("b")
	require.True(t, counted || !notCounted.IsZero())
}

// What makes a switch wait for a probe: no count fresher than the interval with enough
// segments, unless the member could not be counted lately.
func TestNeedsSwitchProbe(t *testing.T) {
	a := newTestMember(C.TypeSOCKS, "a", dialWorking)
	_, group := newModeGroup(t, modeLoss, members(a), map[string]uint16{"a": 100}, nil)
	require.True(t, group.needsSwitchProbe("x"))
	storeLoss(group, "x", 1000, 0, 20*time.Millisecond)
	require.False(t, group.needsSwitchProbe("x"))
	storeLoss(group, "y", 180, 0, 20*time.Millisecond)
	require.True(t, group.needsSwitchProbe("y"))
	group.history.StoreLossSample("z", urltest.LossSample{At: time.Now().Add(-time.Hour), Segments: 1000})
	require.True(t, group.needsSwitchProbe("z"))
	group.history.StoreLossNotCounted("w", time.Now())
	require.False(t, group.needsSwitchProbe("w"))
	require.False(t, group.lossDue("w"))
	require.False(t, group.lossDue("x"))
	require.True(t, group.lossDue("z"))
}

// A member whose fresh count shows a loss is not switched to.
func TestLossModeFreshCountHoldsBack(t *testing.T) {
	a, b, c := newTestMember(C.TypeSOCKS, "a", dialWorking), newTestMember(C.TypeSOCKS, "b", dialWorking), newTestMember(C.TypeSOCKS, "c", dialWorking)
	_, group := newModeGroup(t, modeLoss, members(a, b, c), map[string]uint16{"a": 100, "b": 300, "c": 400}, nil)
	storeLoss(group, "a", 1000, 0, 50*time.Millisecond)
	storeLoss(group, "b", 1000, 50, 20*time.Millisecond)
	setResult(group, "b", 60)
	group.performUpdateCheck()
	tcp, _ := selectedTags(group)
	require.Equal(t, "a", tcp)
}

func sourceContext(source string) context.Context {
	metadata := adapter.InboundContext{Source: M.SocksaddrFrom(netip.MustParseAddr(source), 40000)}
	return adapter.WithContext(context.Background(), &metadata)
}

func balanceTarget(t *testing.T) M.Socksaddr {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				buffer := make([]byte, 1)
				conn.Read(buffer)
				conn.Close()
			}()
		}
	}()
	return M.ParseSocksaddr(listener.Addr().String())
}

// The devices are spread over the members of the balance and keep their member.
func TestBalanceSpreadsAndKeeps(t *testing.T) {
	a, b, c, d := newTestMember(C.TypeSOCKS, "a", dialWorking), newTestMember(C.TypeSOCKS, "b", dialWorking),
		newTestMember(C.TypeSOCKS, "c", dialWorking), newTestMember(C.TypeSOCKS, "d", dialWorking)
	urlTest, group := newModeGroup(t, modeBalance, members(a, b, c, d), map[string]uint16{"a": 100, "b": 110, "c": 120, "d": 400}, nil)
	target := balanceTarget(t)
	require.Len(t, urlTest.Balance(), 3)
	given := make(map[string]string)
	used := make(map[string]int)
	for i := 1; i <= 30; i++ {
		source := fmt.Sprint("10.0.0.", i)
		conn, err := urlTest.DialContext(sourceContext(source), N.NetworkTCP, target)
		require.NoError(t, err)
		member := group.balance.devices["tcp "+source].member.Tag()
		given[source] = member
		used[member]++
		conn.Close()
	}
	require.NotContains(t, used, "d")
	require.GreaterOrEqual(t, len(used), 2)
	for source, member := range given {
		conn, err := urlTest.DialContext(sourceContext(source), N.NetworkTCP, target)
		require.NoError(t, err)
		require.Equal(t, member, group.balance.devices["tcp "+source].member.Tag())
		conn.Close()
	}
}

// A device whose member goes down moves at once; one whose member left the balance moves
// only after it has been idle.
func TestBalanceMoves(t *testing.T) {
	a, b, c := newTestMember(C.TypeSOCKS, "a", dialWorking), newTestMember(C.TypeSOCKS, "b", dialWorking), newTestMember(C.TypeSOCKS, "c", dialWorking)
	urlTest, group := newModeGroup(t, modeBalance, members(a, b, c), map[string]uint16{"a": 100, "b": 110, "c": 120}, nil)
	group.loss.balanceIdle = time.Hour
	target := balanceTarget(t)
	memberOf := func(source string) string {
		conn, err := urlTest.DialContext(sourceContext(source), N.NetworkTCP, target)
		require.NoError(t, err)
		conn.Close()
		return group.balance.devices["tcp "+source].member.Tag()
	}
	// a device on each member
	sources := make(map[string]string)
	for i := 1; len(sources) < 3 && i < 200; i++ {
		source := fmt.Sprint("10.1.0.", i)
		member := memberOf(source)
		if _, seen := sources[member]; !seen {
			sources[member] = source
		}
	}
	require.Len(t, sources, 3)
	// c goes down: its device moves at once
	setResult(group, "c", 0)
	group.performUpdateCheck()
	require.NotEqual(t, "c", memberOf(sources["c"]))
	// a loses 5 %, and leaves the balance; its device keeps it while active
	setResult(group, "c", 120)
	storeLoss(group, "a", 1000, 50, 20*time.Millisecond)
	storeLoss(group, "b", 1000, 0, 20*time.Millisecond)
	storeLoss(group, "c", 1000, 0, 20*time.Millisecond)
	group.performUpdateCheck()
	for _, member := range urlTest.Balance() {
		require.NotEqual(t, "a", member.Tag)
	}
	held, err := urlTest.DialContext(sourceContext(sources["a"]), N.NetworkTCP, target)
	require.NoError(t, err)
	require.Equal(t, "a", group.balance.devices["tcp "+sources["a"]].member.Tag())
	held.Close()
	group.loss.balanceIdle = 20 * time.Millisecond
	time.Sleep(50 * time.Millisecond)
	require.NotEqual(t, "a", memberOf(sources["a"]))
}
