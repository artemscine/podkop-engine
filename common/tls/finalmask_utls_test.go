//go:build with_utls

package tls

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/finalmask"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

type finalMaskRecordingConn struct {
	writes [][]byte
}

func (c *finalMaskRecordingConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *finalMaskRecordingConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, bytes.Clone(p))
	return len(p), nil
}
func (*finalMaskRecordingConn) Close() error                     { return nil }
func (*finalMaskRecordingConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*finalMaskRecordingConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*finalMaskRecordingConn) SetDeadline(time.Time) error      { return nil }
func (*finalMaskRecordingConn) SetReadDeadline(time.Time) error  { return nil }
func (*finalMaskRecordingConn) SetWriteDeadline(time.Time) error { return nil }

func TestFinalMaskUTLSCloneAndQUIC(t *testing.T) {
	config, err := NewUTLSClient(context.Background(), log.NewNOPFactory().Logger(), "example.org", option.OutboundTLSOptions{
		Enabled: true, Insecure: true, FinalMask: fragmentOptions(),
		UTLS: &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "firefox"},
	})
	require.NoError(t, err)
	cloned := config.Clone().(*UTLSClientConfig)
	require.Same(t, config.(*UTLSClientConfig).finalMask, cloned.finalMask)
	client, err := cloned.Client(&finalMaskRecordingConn{})
	require.NoError(t, err)
	require.IsType(t, &finalmask.Conn{}, client.(*utlsALPNWrapper).NetConn())
	_, err = STDConfigForQUIC(cloned)
	require.ErrorContains(t, err, "finalmask")
}

func TestFinalMaskRealityActuallyFragmentsHello(t *testing.T) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	mask := fragmentOptions()
	mask.TCP[0].Settings.Length = &option.V2RayXHTTPRange{From: 64, To: 64}
	mask.TCP[0].Settings.Delay = nil
	mask.TCP[0].Settings.MaxSplit = &option.V2RayXHTTPRange{From: 2, To: 2}
	config, err := NewRealityClient(context.Background(), log.NewNOPFactory().Logger(), "example.org", option.OutboundTLSOptions{
		Enabled: true, FinalMask: mask,
		UTLS: &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "firefox"},
		Reality: &option.OutboundRealityOptions{
			Enabled: true, PublicKey: base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()),
		},
	})
	require.NoError(t, err)
	cloned := config.Clone().(*RealityClientConfig)
	require.Same(t, config.(*RealityClientConfig).uClient.finalMask, cloned.uClient.finalMask)
	raw := &finalMaskRecordingConn{}
	_, err = cloned.ClientHandshake(context.Background(), raw)
	require.Error(t, err, "the fake peer ends after receiving ClientHello")
	require.GreaterOrEqual(t, len(raw.writes), 2)
	require.Len(t, raw.writes[0], 64, "REALITY must use the byte fragment wrapper, not bypass it")
	require.Equal(t, byte(22), raw.writes[0][0])
	require.Greater(t, len(raw.writes[1]), 64, "maxSplit sends the rest of ClientHello together")
}

func TestFinalMaskRealityHandshakeCancelInterruptsDelay(t *testing.T) {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	require.NoError(t, err)
	mask := fragmentOptions()
	mask.TCP[0].Settings.Length = &option.V2RayXHTTPRange{From: 1, To: 1}
	mask.TCP[0].Settings.Delay = &option.V2RayXHTTPRange{From: 10000, To: 10000}
	config, err := NewRealityClient(context.Background(), log.NewNOPFactory().Logger(), "example.org", option.OutboundTLSOptions{
		Enabled: true, FinalMask: mask,
		UTLS: &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "firefox"},
		Reality: &option.OutboundRealityOptions{
			Enabled: true, PublicKey: base64.RawURLEncoding.EncodeToString(key.PublicKey().Bytes()),
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = config.(*RealityClientConfig).ClientHandshake(ctx, &finalMaskRecordingConn{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second, "handshake cancellation must interrupt a fragment's 10-second delay")
}
