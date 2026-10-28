package tls

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/finalmask"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

func fragmentOptions() *option.FinalMaskOptions {
	return &option.FinalMaskOptions{TCP: []option.FinalMaskTCPOptions{{
		Type: "fragment",
		Settings: option.FinalMaskFragmentOptions{
			Packets: "1-3",
			Length:  &option.V2RayXHTTPRange{From: 100, To: 200},
			Delay:   &option.V2RayXHTTPRange{From: 1, To: 2},
		},
	}}}
}

func TestFinalMaskSTDTLSRoundTrip(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	config, err := NewSTDClient(ctx, log.NewNOPFactory().Logger(), "example.org", option.OutboundTLSOptions{
		Enabled: true, Insecure: true, FinalMask: fragmentOptions(),
	})
	require.NoError(t, err)
	cloned := config.Clone().(*STDClientConfig)
	require.NotNil(t, cloned.finalMask)
	require.Same(t, config.(*STDClientConfig).finalMask, cloned.finalMask)
	_, err = cloned.STDConfig()
	require.ErrorContains(t, err, "finalmask")
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	defer raw.Close()
	client, err := cloned.Client(raw)
	require.NoError(t, err)
	require.IsType(t, &finalmask.Conn{}, client.(*STDConn).NetConn())
	require.NoError(t, client.HandshakeContext(ctx))
	_, err = fmt.Fprintf(client, "GET / HTTP/1.1\r\nHost: example.org\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusNoContent, response.StatusCode)
}

func TestFinalMaskTLSRejectsConflicts(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*option.OutboundTLSOptions)
	}{
		{"disabled", func(o *option.OutboundTLSOptions) { o.Enabled = false }},
		{"native-packet", func(o *option.OutboundTLSOptions) { o.Fragment = true }},
		{"native-record", func(o *option.OutboundTLSOptions) { o.RecordFragment = true }},
		{"ktls", func(o *option.OutboundTLSOptions) { o.KernelTx = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := option.OutboundTLSOptions{Enabled: true, Insecure: true, FinalMask: fragmentOptions()}
			test.edit(&o)
			_, err := NewClient(context.Background(), log.NewNOPFactory().Logger(), "example.org", o)
			require.ErrorContains(t, err, "finalmask")
		})
	}
	_, err := ValidateSystemTLSOptions(context.Background(), option.OutboundTLSOptions{FinalMask: fragmentOptions()}, "system-test")
	require.ErrorContains(t, err, "finalmask")
}
