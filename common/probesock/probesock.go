package probesock

import (
	"context"
	"net"
	"sync"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
)

// Window is the receive buffer a probe connection asks for: the kernel doubles it, and the
// window it offers stays near 16-24 KB, a dozen segments. On the stand a 1 MB download with
// it lost nothing on a 25-packet queue and on a 10 Mbit/s line, where a download with the
// ordinary buffer lost 6-13 %, and still showed a random loss of 1 % as 0.8-1.25 %.
const Window = 16 << 10

type contextKey struct{}

// Socket is the probe connection of a context: the first TCP connection a dialer opens
// under it (see Dial). One probe, one socket: a context is made for each probe.
type Socket struct {
	window int

	access  sync.Mutex
	taken   bool
	conn    *net.TCPConn
	connect time.Duration
	capture *capture
	err     error
}

// With returns a context under which the first TCP dial is a probe connection with a
// receive buffer of window bytes.
func With(ctx context.Context, window int) (context.Context, *Socket) {
	socket := &Socket{window: window}
	return context.WithValue(ctx, contextKey{}, socket), socket
}

// From returns the probe socket of the context, nil when it has none.
func From(ctx context.Context) *Socket {
	socket, _ := ctx.Value(contextKey{}).(*Socket)
	return socket
}

// Without returns a context whose dials are no probe connection: for the dials a probe
// causes on the side, as the queries that resolve the server's name.
func Without(ctx context.Context) context.Context {
	if From(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, (*Socket)(nil))
}

// take reports whether this dial is the probe connection: the first one under the context.
func (s *Socket) take() bool {
	if s == nil {
		return false
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.taken {
		return false
	}
	s.taken = true
	return true
}

// Dial dials address with dialer. The first dial under the context is the probe
// connection: its receive buffer is set before the connect, when the window scale is
// chosen, and its packets are captured from the connect on (no data moves before the
// connect returns). Every later dial is an ordinary one.
func (s *Socket) Dial(ctx context.Context, dialer net.Dialer, network string, address string) (net.Conn, error) {
	if !s.take() {
		return dialer.DialContext(ctx, network, address)
	}
	dialer.Control = appendControl(dialer.Control, s.window)
	start := time.Now()
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	connect := time.Since(start)
	s.access.Lock()
	defer s.access.Unlock()
	s.connect = connect
	tcpConn, isTCP := conn.(*net.TCPConn)
	if !isTCP {
		s.err = E.New("not a TCP connection")
		return conn, nil
	}
	s.conn = tcpConn
	s.capture, s.err = startCapture(tcpConn, reorderWindow(connect))
	return conn, nil
}

// Result is what a probe connection showed.
type Result struct {
	Counts
	Segments int           // segments of data that came
	Lost     int           // of them lost on the way
	RTT      time.Duration // the least RTT of the connection, or its connect time
	MSS      int
	Dropped  int // packets the capture could not keep: the counts are short
}

// Finish ends the capture and returns the counts, before the connection is closed (its
// TCP information is read). An error tells that the probe had no connection of its own to
// count: the dial went over a connection opened before (a multiplexed transport), over UDP,
// or the capture could not run.
func (s *Socket) Finish() (Result, error) {
	s.access.Lock()
	defer s.access.Unlock()
	var result Result
	if !s.taken || s.conn == nil {
		if s.err != nil {
			return result, s.err
		}
		return result, E.New("no connection of its own")
	}
	if s.capture == nil {
		return result, E.Cause(s.err, "capture")
	}
	counts, dropped := s.capture.stop()
	s.capture = nil
	result.Counts = counts
	result.Dropped = dropped
	result.RTT = s.connect
	if info, err := tcpInfo(s.conn); err == nil {
		result.MSS = info.mss
		if info.minRTT > 0 {
			result.RTT = info.minRTT
		}
	}
	result.Segments, result.Lost = counts.segments(result.MSS)
	return result, nil
}

// Close ends a capture that Finish did not end: the probe failed on the way.
func (s *Socket) Close() {
	if s == nil {
		return
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.capture != nil {
		s.capture.stop()
		s.capture = nil
	}
}
