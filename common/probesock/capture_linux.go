//go:build linux

package probesock

import (
	"encoding/binary"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sagernet/sing/common/control"
	E "github.com/sagernet/sing/common/exceptions"

	"golang.org/x/net/bpf"
	"golang.org/x/sys/unix"
)

// clampWindow holds the window a connection offers to window bytes: its receive buffer and the
// clamp of its window. Set once the connection is established and before data moves, it leaves
// the window scale the connect chose by the system maximum: a buffer set before the connect has
// the kernel choose the scale by it (tcp_connect_init clamps the window to a buffer the user
// set), and the window could never grow past 64 KB again. The buffer is set too, not the clamp
// alone: the kernel's tuning of a buffer the user did not set moves the clamp along with it.
// The connection keeps the buffer it is given (restoreWindow).
func clampWindow(conn *net.TCPConn, window int) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	return control.Raw(raw, func(fd uintptr) error {
		if err := setBuffer(int(fd), window); err != nil {
			return err
		}
		return unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_WINDOW_CLAMP, window)
	})
}

// setBuffer sets the receive buffer of a socket to size bytes (the kernel doubles what it is
// given, as the bookkeeping counts): SO_RCVBUFFORCE passes over net.core.rmem_max, small on
// routers, for a process with CAP_NET_ADMIN.
func setBuffer(fd int, size int) error {
	if unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUFFORCE, size) == nil {
		return nil
	}
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, size)
}

var (
	bufferMaxOnce sync.Once
	bufferMax     int
)

// systemBufferMax is the largest receive buffer the system grows a TCP connection to (the
// last of net.ipv4.tcp_rmem), 4 MB when it cannot be read: the clamp of a window no tuning
// reaches.
func systemBufferMax() int {
	bufferMaxOnce.Do(func() {
		bufferMax = 4 << 20
		content, err := os.ReadFile("/proc/sys/net/ipv4/tcp_rmem")
		if err != nil {
			return
		}
		fields := strings.Fields(string(content))
		if len(fields) == 3 {
			if value, err := strconv.Atoi(fields[2]); err == nil && value > 0 {
				bufferMax = value
			}
		}
	})
	return bufferMax
}

// restoreWindow lets the window of a probe connection grow as far as the system lets a TCP
// connection's once the probe is done: a multiplexed transport (gRPC, HTTP/2, xhttp, mux) keeps
// the connection a probe dialed in its pool and carries other connections over it. The buffer
// stays set (no tuning), at the most the tuning would reach: memory is taken only by data that
// waits to be read.
func restoreWindow(conn *net.TCPConn) {
	_ = clampWindow(conn, systemBufferMax())
}

type connInfo struct {
	mss    int
	minRTT time.Duration
}

func tcpInfo(conn *net.TCPConn) (connInfo, error) {
	var info connInfo
	raw, err := conn.SyscallConn()
	if err != nil {
		return info, err
	}
	var tcp *unix.TCPInfo
	err = control.Raw(raw, func(fd uintptr) error {
		var err error
		tcp, err = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		return err
	})
	if err != nil {
		return info, err
	}
	info.mss = int(tcp.Rcv_mss)
	info.minRTT = time.Duration(tcp.Min_rtt) * time.Microsecond
	return info, nil
}

// snapLength is how much of a packet the capture keeps: the IP header (IPv6 without
// extension headers) and the TCP header with its options.
const snapLength = 128

// capture takes the packets of one TCP connection from a packet socket of every interface,
// headers only, and counts the incoming sequence.
type capture struct {
	fd      int
	local   netip.AddrPort
	remote  netip.AddrPort
	stopped atomic.Bool
	done    chan struct{}

	access  sync.Mutex
	counter counter
	inIndex int // the interface the incoming packets are taken from: the first one seen
}

func startCapture(conn *net.TCPConn) (*capture, error) {
	local, err := netip.ParseAddrPort(conn.LocalAddr().String())
	if err != nil {
		return nil, err
	}
	remote, err := netip.ParseAddrPort(conn.RemoteAddr().String())
	if err != nil {
		return nil, err
	}
	local = netip.AddrPortFrom(local.Addr().Unmap(), local.Port())
	remote = netip.AddrPortFrom(remote.Addr().Unmap(), remote.Port())
	// protocol 0: nothing comes before the filter is on and the socket is bound
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, E.Cause(err, "open a packet socket")
	}
	c := &capture{fd: fd, local: local, remote: remote, done: make(chan struct{})}
	err = c.setup()
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	go c.run()
	return c, nil
}

func htons(value uint16) uint16 {
	return value<<8 | value>>8
}

func (c *capture) setup() error {
	filter, err := portFilter(c.local.Port(), c.remote.Port())
	if err != nil {
		return err
	}
	err = unix.SetsockoptSockFprog(c.fd, unix.SOL_SOCKET, unix.SO_ATTACH_FILTER, &unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]})
	if err != nil {
		return E.Cause(err, "attach the filter")
	}
	// a probe is a few hundred packets a second: a megabyte of buffer holds seconds of them
	_ = unix.SetsockoptInt(c.fd, unix.SOL_SOCKET, unix.SO_RCVBUF, 1<<20)
	err = unix.SetsockoptInt(c.fd, unix.SOL_SOCKET, unix.SO_TIMESTAMPNS, 1)
	if err != nil {
		return E.Cause(err, "timestamps")
	}
	err = unix.SetsockoptTimeval(c.fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Usec: 100_000})
	if err != nil {
		return err
	}
	err = unix.Bind(c.fd, &unix.SockaddrLinklayer{Protocol: htons(unix.ETH_P_ALL)})
	if err != nil {
		return E.Cause(err, "bind the packet socket")
	}
	return nil
}

// portFilter passes the TCP packets between the two ports either way, IPv4 not fragmented
// or IPv6 without extension headers, cut to snapLength. The addresses are checked as the
// packets are read: the ports are rarely shared, and the program stays short.
func portFilter(localPort uint16, remotePort uint16) ([]unix.SockFilter, error) {
	incoming := uint32(remotePort)<<16 | uint32(localPort)
	outgoing := uint32(localPort)<<16 | uint32(remotePort)
	program := []bpf.Instruction{
		/* 0 */ bpf.LoadAbsolute{Off: 0, Size: 1},
		/* 1 */ bpf.ALUOpConstant{Op: bpf.ALUOpAnd, Val: 0xf0},
		/* 2 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0x40, SkipFalse: 8}, // IPv6: 11
		/* 3 */ bpf.LoadAbsolute{Off: 9, Size: 1},
		/* 4 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: unix.IPPROTO_TCP, SkipFalse: 13},
		/* 5 */ bpf.LoadAbsolute{Off: 6, Size: 2},
		/* 6 */ bpf.JumpIf{Cond: bpf.JumpBitsSet, Val: 0x1fff, SkipTrue: 11}, // a fragment
		/* 7 */ bpf.LoadMemShift{Off: 0},
		/* 8 */ bpf.LoadIndirect{Off: 0, Size: 4},
		/* 9 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: incoming, SkipTrue: 7},
		/* 10 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: outgoing, SkipTrue: 6, SkipFalse: 7},
		/* 11 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: 0x60, SkipFalse: 6},
		/* 12 */ bpf.LoadAbsolute{Off: 6, Size: 1},
		/* 13 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: unix.IPPROTO_TCP, SkipFalse: 4},
		/* 14 */ bpf.LoadAbsolute{Off: 40, Size: 4},
		/* 15 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: incoming, SkipTrue: 1},
		/* 16 */ bpf.JumpIf{Cond: bpf.JumpEqual, Val: outgoing, SkipFalse: 1},
		/* 17 */ bpf.RetConstant{Val: snapLength},
		/* 18 */ bpf.RetConstant{Val: 0},
	}
	raw, err := bpf.Assemble(program)
	if err != nil {
		return nil, E.Cause(err, "assemble the filter")
	}
	filter := make([]unix.SockFilter, len(raw))
	for i, instruction := range raw {
		filter[i] = unix.SockFilter{Code: instruction.Op, Jt: instruction.Jt, Jf: instruction.Jf, K: instruction.K}
	}
	return filter, nil
}

func (c *capture) run() {
	defer close(c.done)
	buffer := make([]byte, snapLength)
	oob := make([]byte, unix.CmsgSpace(int(unsafe.Sizeof(unix.Timespec{}))))
	for {
		flags := 0
		if c.stopped.Load() {
			// what came before the stop is read without waiting, then the capture ends
			flags = unix.MSG_DONTWAIT
		}
		n, oobn, _, from, err := unix.Recvmsg(c.fd, buffer, oob, flags)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR {
				if c.stopped.Load() && flags != 0 {
					return
				}
				continue
			}
			return
		}
		link, isLink := from.(*unix.SockaddrLinklayer)
		if !isLink || link.Pkttype == unix.PACKET_OUTGOING {
			continue
		}
		at, hasTime := packetTime(oob[:oobn])
		if !hasTime {
			at = time.Now()
		}
		c.take(buffer[:n], link.Ifindex, at)
	}
}

// packetTime reads the kernel's receive time of a packet: a struct timespec of the platform
// (32-bit fields on the 32-bit routers).
func packetTime(oob []byte) (time.Time, bool) {
	messages, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return time.Time{}, false
	}
	for _, message := range messages {
		if message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_TIMESTAMPNS && len(message.Data) >= int(unsafe.Sizeof(unix.Timespec{})) {
			spec := *(*unix.Timespec)(unsafe.Pointer(&message.Data[0]))
			return time.Unix(int64(spec.Sec), int64(spec.Nsec)), true
		}
	}
	return time.Time{}, false
}

// take counts an incoming packet of the connection: the addresses are checked here, and
// only the first interface that brought one is believed (a packet may be seen on a device
// and on the device under it).
func (c *capture) take(packet []byte, index int, at time.Time) {
	if len(packet) < 20 {
		return
	}
	var (
		source, destination netip.Addr
		tcp                 []byte
		payload             int
	)
	switch packet[0] >> 4 {
	case 4:
		headerLength := int(packet[0]&0x0f) * 4
		if len(packet) < headerLength+20 {
			return
		}
		source = netip.AddrFrom4([4]byte(packet[12:16]))
		destination = netip.AddrFrom4([4]byte(packet[16:20]))
		tcp = packet[headerLength:]
		payload = int(binary.BigEndian.Uint16(packet[2:4])) - headerLength - int(tcp[12]>>4)*4
	case 6:
		if len(packet) < 60 {
			return
		}
		source = netip.AddrFrom16([16]byte(packet[8:24]))
		destination = netip.AddrFrom16([16]byte(packet[24:40]))
		tcp = packet[40:]
		payload = int(binary.BigEndian.Uint16(packet[4:6])) - int(tcp[12]>>4)*4
	default:
		return
	}
	if source != c.remote.Addr() || destination != c.local.Addr() ||
		binary.BigEndian.Uint16(tcp[0:2]) != c.remote.Port() || binary.BigEndian.Uint16(tcp[2:4]) != c.local.Port() {
		return
	}
	c.access.Lock()
	defer c.access.Unlock()
	if c.inIndex == 0 {
		c.inIndex = index
	} else if index != c.inIndex {
		return
	}
	c.counter.add(at, binary.BigEndian.Uint32(tcp[4:8]), payload)
}

// stop ends the capture once the packets already taken are read, and returns what it
// counted and how many packets the socket dropped.
func (c *capture) stop() (*counter, int) {
	if c.stopped.Swap(true) {
		return &counter{}, 0
	}
	<-c.done
	dropped := 0
	if stats, err := unix.GetsockoptTpacketStats(c.fd, unix.SOL_PACKET, unix.PACKET_STATISTICS); err == nil {
		dropped = int(stats.Drops)
	}
	unix.Close(c.fd)
	c.access.Lock()
	defer c.access.Unlock()
	return &c.counter, dropped
}
