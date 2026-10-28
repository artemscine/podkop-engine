package finalmask

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

type recordedConn struct {
	mu        sync.Mutex
	writes    [][]byte
	written   chan struct{}
	once      sync.Once
	short     int
	zeroWrite bool
}

func (c *recordedConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *recordedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.zeroWrite {
		return 0, nil
	}
	n := len(p)
	if c.short > 0 && n > c.short {
		n = c.short
	}
	c.writes = append(c.writes, bytes.Clone(p[:n]))
	if c.written != nil {
		c.once.Do(func() { close(c.written) })
	}
	return n, nil
}
func (*recordedConn) Close() error                     { return nil }
func (*recordedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*recordedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*recordedConn) SetDeadline(time.Time) error      { return nil }
func (*recordedConn) SetReadDeadline(time.Time) error  { return nil }
func (*recordedConn) SetWriteDeadline(time.Time) error { return nil }
func (c *recordedConn) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.writes...)
}

func fixed(n int32) *option.V2RayXHTTPRange { return &option.V2RayXHTTPRange{From: n, To: n} }

func testConfig(t *testing.T, settings option.FinalMaskFragmentOptions) *Config {
	t.Helper()
	config, err := New(&option.FinalMaskOptions{TCP: []option.FinalMaskTCPOptions{{Type: "fragment", Settings: settings}}})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func TestWriteRangeAndMaxSplit(t *testing.T) {
	raw := &recordedConn{}
	conn := testConfig(t, option.FinalMaskFragmentOptions{Packets: "2-3", Length: fixed(2), MaxSplit: fixed(2)}).WrapConn(context.Background(), raw).(*Conn)
	for _, input := range []string{"first", "abcdef", "123456", "last"} {
		n, err := conn.Write([]byte(input))
		if err != nil || n != len(input) {
			t.Fatalf("write: %d, %v", n, err)
		}
	}
	want := []string{"first", "ab", "cdef", "12", "3456", "last"}
	got := raw.snapshot()
	if len(got) != len(want) {
		t.Fatalf("writes: %q", got)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("write %d: %q", i, got[i])
		}
	}
	if !conn.WriterReplaceable() {
		t.Fatal("completed finite mask still wraps writes")
	}
}

func hello(payload string) []byte {
	packet := []byte{22, 3, 1, 0, 0}
	binary.BigEndian.PutUint16(packet[3:], uint16(len(payload)))
	return append(packet, payload...)
}

func TestTLSHelloRecordsAndTail(t *testing.T) {
	for _, merge := range []bool{true, false} {
		t.Run(map[bool]string{true: "merged", false: "separate"}[merge], func(t *testing.T) {
			raw := &recordedConn{}
			delay := fixed(0)
			if !merge {
				delay = fixed(1)
			}
			conn := testConfig(t, option.FinalMaskFragmentOptions{Packets: "tlshello", Length: fixed(2), Delay: delay, MaxSplit: fixed(3)}).WrapConn(context.Background(), raw)
			input := append(hello("abcdefgh"), []byte("tail")...)
			n, err := conn.Write(input)
			if err != nil || n != len(input) {
				t.Fatalf("write: %d, %v", n, err)
			}
			writes := raw.snapshot()
			if merge && len(writes) != 2 || !merge && len(writes) != 4 {
				t.Fatalf("TLS writes: %d", len(writes))
			}
			wire := bytes.Join(writes[:len(writes)-1], nil)
			var data []byte
			for i, want := range []int{2, 2, 4} {
				if len(wire) < 5 || wire[0] != 22 || wire[1] != 3 || wire[2] != 1 {
					t.Fatalf("record %d header", i)
				}
				length := int(binary.BigEndian.Uint16(wire[3:5]))
				if length != want {
					t.Fatalf("record %d length: %d", i, length)
				}
				data = append(data, wire[5:5+length]...)
				wire = wire[5+length:]
			}
			if len(wire) != 0 || string(data) != "abcdefgh" || string(writes[len(writes)-1]) != "tail" {
				t.Fatal("record data or trailing bytes changed")
			}
			_, err = conn.Write([]byte("subsequent"))
			if err != nil || string(raw.snapshot()[len(writes)]) != "subsequent" {
				t.Fatal("subsequent write fragmented")
			}
		})
	}
}

func TestLengthArraysOverrideAndRepeat(t *testing.T) {
	raw := &recordedConn{}
	conn := testConfig(t, option.FinalMaskFragmentOptions{Packets: "1-1", Length: fixed(99), Lengths: []option.V2RayXHTTPRange{*fixed(0), *fixed(1), *fixed(3)}}).WrapConn(context.Background(), raw)
	_, err := conn.Write([]byte("abcdefgh"))
	if err != nil {
		t.Fatal(err)
	}
	got := raw.snapshot()
	want := []string{"", "a", "bcd", "efg", "h"}
	if len(got) != len(want) {
		t.Fatalf("writes: %q", got)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("write %d: %q", i, got[i])
		}
	}
}

func TestShortWritesAndLargeLength(t *testing.T) {
	raw := &recordedConn{short: 1}
	conn := testConfig(t, option.FinalMaskFragmentOptions{Packets: "1-1", Length: fixed(2147483647)}).WrapConn(context.Background(), raw)
	n, err := conn.Write([]byte("contents"))
	if err != nil || n != 8 || string(bytes.Join(raw.snapshot(), nil)) != "contents" {
		t.Fatalf("short writes: %d, %v", n, err)
	}
	raw = &recordedConn{zeroWrite: true}
	conn = testConfig(t, option.FinalMaskFragmentOptions{Packets: "1-1", Length: fixed(2)}).WrapConn(context.Background(), raw)
	if _, err = conn.Write([]byte("contents")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write: %v", err)
	}
}

func TestDelayInterrupted(t *testing.T) {
	for _, action := range []string{"context", "close", "deadline", "changed-deadline"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			raw := &recordedConn{written: make(chan struct{})}
			conn := testConfig(t, option.FinalMaskFragmentOptions{Packets: "1-1", Length: fixed(1), Delay: fixed(10000)}).WrapConn(ctx, raw)
			if action == "deadline" {
				if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() { _, err := conn.Write([]byte("a")); done <- err }()
			<-raw.written
			var want error
			switch action {
			case "context":
				want = context.Canceled
				cancel()
			case "close":
				want = net.ErrClosed
				_ = conn.Close()
			case "deadline":
				want = os.ErrDeadlineExceeded
			case "changed-deadline":
				want = os.ErrDeadlineExceeded
				_ = conn.SetDeadline(time.Now().Add(-time.Second))
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("error: %v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("delay did not stop")
			}
		})
	}
}

func TestConcurrentWritesStayWhole(t *testing.T) {
	raw := &recordedConn{}
	conn := testConfig(t, option.FinalMaskFragmentOptions{Length: fixed(2)}).WrapConn(context.Background(), raw)
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func(b byte) {
			defer group.Done()
			_, _ = conn.Write(bytes.Repeat([]byte{b}, 4))
		}(byte('A' + i))
	}
	group.Wait()
	writes := raw.snapshot()
	if len(writes) != 40 {
		t.Fatalf("writes: %d", len(writes))
	}
	for i := 0; i < len(writes); i += 2 {
		if !bytes.Equal(writes[i], writes[i+1]) {
			t.Fatal("concurrent Write calls interleaved fragments")
		}
	}
}
