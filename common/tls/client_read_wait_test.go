//go:build go1.25 && badlinkname

package tls

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// A connection from the dialer reads through the read waiter of badtls: the copy loop takes
// a buffer only once a record has arrived, not before it waits.
func TestWithReadWait(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	defer serverSide.Close()
	defer clientSide.Close()
	certificate := testCertificate(t)
	go func() {
		server := tls.Server(serverSide, &tls.Config{Certificates: []tls.Certificate{certificate}})
		server.Write([]byte("hello"))
	}()
	client := tls.Client(clientSide, &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, client.HandshakeContext(context.Background()))

	readWaiter, isReadWaiter := withReadWait(client).(N.ReadWaiter)
	require.True(t, isReadWaiter, "the dialer's connection has no read waiter")
	require.False(t, readWaiter.InitializeReadWaiter(N.ReadWaitOptions{}), "the read waiter asks for a copy")
	buffer, err := readWaiter.WaitReadBuffer()
	require.NoError(t, err)
	require.Equal(t, "hello", string(buffer.Bytes()))
	buffer.Release()
}

type plainConn struct {
	net.Conn
}

func (c plainConn) NetConn() net.Conn {
	return c.Conn
}

func (c plainConn) HandshakeContext(context.Context) error {
	return nil
}

func (c plainConn) ConnectionState() tls.ConnectionState {
	return tls.ConnectionState{}
}

// A connection badtls does not know stays as it is, and the dial goes on.
func TestWithReadWaitUnknownConn(t *testing.T) {
	side, peer := net.Pipe()
	defer side.Close()
	defer peer.Close()
	conn := plainConn{side}
	require.Equal(t, Conn(conn), withReadWait(conn))
}
