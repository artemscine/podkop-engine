//go:build !linux

package probesock

import (
	"net"
	"syscall"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
)

// appendControl leaves the dialer as it is: without a capture the probe counts nothing.
func appendControl(base func(network, address string, conn syscall.RawConn) error, window int) func(network, address string, conn syscall.RawConn) error {
	return base
}

type connInfo struct {
	mss    int
	minRTT time.Duration
}

func tcpInfo(conn *net.TCPConn) (connInfo, error) {
	return connInfo{}, E.New("not supported on this platform")
}

type capture struct{}

func startCapture(conn *net.TCPConn, window time.Duration) (*capture, error) {
	return nil, E.New("the capture is not supported on this platform")
}

func (c *capture) stop() (Counts, int) {
	return Counts{}, 0
}
