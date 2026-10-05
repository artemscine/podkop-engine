//go:build with_quic

package v2rayxhttp

import (
	"context"
	stdTLS "crypto/tls"
	"net/http"
	"runtime"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	congestion_meta2 "github.com/sagernet/sing-quic/congestion_meta2"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// quicKeepAlivePeriod is the QUIC keep-alive of an idle HTTP/3 connection
// (Xray's net.QuicgoH3KeepAlivePeriod).
const quicKeepAlivePeriod = 10 * time.Second

// http3Connection carries every request of its sessions as streams of one QUIC
// connection.
type http3Connection struct {
	transport *http3.Transport
}

func newHTTP3Connections(dialer N.Dialer, serverAddr M.Socksaddr, tlsConfig tls.Config, keepAlivePeriod time.Duration) (func() pooledConnection, error) {
	stdConfig, err := tls.STDConfigForQUIC(tlsConfig)
	if err != nil {
		return nil, E.Cause(err, "xhttp: HTTP/3 TLS")
	}
	stdConfig = stdConfig.Clone()
	stdConfig.NextProtos = []string{http3.NextProtoH3}
	// The QUIC handshake looks like Chrome's, as with Xray. quic-go's Chrome
	// profile refuses VerifyConnection, which checks the server name with
	// disable_sni, and the Certificates field, which carries a client
	// certificate here.
	chromeParrot := true
	if stdConfig.VerifyConnection != nil {
		chromeParrot = false
		log.StdLogger().Warn("xhttp: disable_sni turns the Chrome profile of HTTP/3 off")
	}
	if len(stdConfig.Certificates) > 0 {
		certificate := stdConfig.Certificates[0]
		stdConfig.Certificates = nil
		stdConfig.GetClientCertificate = func(*stdTLS.CertificateRequestInfo) (*stdTLS.Certificate, error) {
			return &certificate, nil
		}
	}
	switch {
	case keepAlivePeriod == 0:
		keepAlivePeriod = quicKeepAlivePeriod
	case keepAlivePeriod < 0:
		keepAlivePeriod = 0
	}
	quicConfig := &quic.Config{
		MaxIdleTimeout:          connectionIdleTimeout,
		KeepAlivePeriod:         keepAlivePeriod,
		MaxIncomingStreams:      -1,
		DisablePathMTUDiscovery: !(runtime.GOOS == "linux" || runtime.GOOS == "windows" || runtime.GOOS == "darwin"),
		ChromeParrot:            chromeParrot,
	}
	return func() pooledConnection {
		return &http3Connection{transport: &http3.Transport{
			TLSClientConfig: stdConfig,
			QUICConfig:      quicConfig,
			Dial: func(ctx context.Context, addr string, tlsConfig *tls.STDConfig, quicConfig *quic.Config) (*quic.Conn, error) {
				conn, err := dialer.DialContext(ctx, N.NetworkUDP, serverAddr)
				if err != nil {
					return nil, err
				}
				quicConn, err := quic.DialEarly(ctx, bufio.NewUnbindPacketConn(conn), conn.RemoteAddr(), tlsConfig, quicConfig)
				if err != nil {
					conn.Close()
					return nil, err
				}
				// quic-go does not own a packet conn passed to DialEarly: it
				// only stops reading it when the connection ends.
				context.AfterFunc(quicConn.Context(), func() {
					conn.Close()
				})
				// BBR with the standard profile, as Xray's HTTP/3 uses.
				quicConn.SetCongestionControl(congestion_meta2.NewBbrSenderWithProfile(quicConn.InitialPacketSize(), congestion_meta2.ProfileStandard))
				return quicConn, nil
			},
		}}
	}, nil
}

func (c *http3Connection) roundTrip(request *http.Request) (*http.Response, error) {
	return c.transport.RoundTrip(request)
}

func (c *http3Connection) close() {
	c.transport.Close()
}
