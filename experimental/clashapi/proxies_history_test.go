package clashapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group/urltestgroup"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type historyTestMember struct{ outbound.Adapter }

func (m *historyTestMember) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func (m *historyTestMember) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

type historyTestManager struct {
	adapter.OutboundManager
	member adapter.Outbound
}

func (m *historyTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	return m.member, tag == m.member.Tag()
}

func TestProxyInfoPublishesGroupScopedMemberHistory(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(good.Close)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	t.Cleanup(bad.Close)
	history := urltest.NewHistoryStorage()
	member := &historyTestMember{Adapter: outbound.NewAdapter(C.TypeVLESS, "shared", []string{N.NetworkTCP}, nil)}
	manager := &historyTestManager{member: member}
	ctx := service.ContextWith[adapter.OutboundManager](service.ContextWithPtr(context.Background(), history), manager)
	makeGroup := func(tag, link string) *urltestgroup.URLTest {
		created, err := urltestgroup.NewURLTest(ctx, nil, log.NewNOPFactory().NewLogger(tag), tag, option.URLTestOutboundOptions{Outbounds: []string{"shared"}, URL: link})
		require.NoError(t, err)
		g := created.(*urltestgroup.URLTest)
		require.NoError(t, g.Start(adapter.StartStateStart, nil))
		t.Cleanup(g.Retire)
		return g
	}
	a, b := makeGroup("a", good.URL), makeGroup("b", bad.URL)
	server := &Server{urlTestHistory: history}
	read := func(member adapter.Outbound) map[string]any {
		content, err := proxyInfo(server, member).MarshalJSON()
		require.NoError(t, err)
		var result map[string]any
		require.NoError(t, json.Unmarshal(content, &result))
		return result
	}
	require.Empty(t, read(a)["member_history"], "unprobed group cannot report failed members")
	a.CheckOutbounds()
	require.Len(t, read(member)["history"], 1, "standalone proxy telemetry is preserved")
	b.CheckOutbounds()
	require.Empty(t, read(member)["history"], "standalone telemetry is the latest result of any check")
	goodInfo, badInfo := read(a), read(b)
	require.Len(t, goodInfo["history"], 1, "selected group history belongs to its own URL")
	require.Empty(t, badInfo["history"])
	require.Len(t, goodInfo["member_history"].(map[string]any)["shared"], 1)
	require.Empty(t, badInfo["member_history"].(map[string]any)["shared"])
}
