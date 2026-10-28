// Package finalmask implements Xray-compatible client-side TCP fragmentation.
package finalmask

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/option"
)

// Config is immutable and may be shared by TLS configuration clones. Each
// connection gets its own Write counter and cancellation/deadline state.
type Config struct {
	fragments []fragment
}

type fragment struct {
	from, to uint64
	lengths  []option.V2RayXHTTPRange
	delays   []option.V2RayXHTTPRange
	maxSplit option.V2RayXHTTPRange
}

func New(options *option.FinalMaskOptions) (*Config, error) {
	if options == nil || len(options.TCP) == 0 {
		return nil, nil
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	config := &Config{}
	for _, mask := range options.TCP {
		o := mask.Settings
		from, to, _ := o.PacketRange()
		f := fragment{from: from, to: to}
		f.lengths = append(f.lengths, o.Lengths...)
		if len(f.lengths) == 0 {
			f.lengths = []option.V2RayXHTTPRange{*o.Length}
		}
		f.delays = append(f.delays, o.Delays...)
		if len(f.delays) == 0 {
			f.delays = []option.V2RayXHTTPRange{{}}
			if o.Delay != nil {
				f.delays[0] = *o.Delay
			}
		}
		if o.MaxSplit != nil {
			f.maxSplit = *o.MaxSplit
		}
		config.fragments = append(config.fragments, f)
	}
	return config, nil
}

// WrapConn puts the first mask nearest to TLS, matching Xray's mask order.
func (c *Config) WrapConn(ctx context.Context, conn net.Conn) net.Conn {
	if c == nil {
		return conn
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for i := len(c.fragments) - 1; i >= 0; i-- {
		conn = &Conn{
			Conn: conn, ctx: ctx, fragment: c.fragments[i],
			closed: make(chan struct{}), changed: make(chan struct{}),
		}
	}
	return conn
}

// Conn preserves TCP byte contents in numeric packets mode. tlshello rewrites
// only the first complete TLS record into multiple TLS records.
type Conn struct {
	net.Conn
	ctx      context.Context
	fragment fragment
	writeMu  sync.Mutex
	count    uint64
	finished atomic.Bool
	close    sync.Once
	closed   chan struct{}
	stateMu  sync.Mutex
	deadline time.Time
	changed  chan struct{}
}

func pick(r option.V2RayXHTTPRange) int {
	if r.From == r.To {
		return int(r.From)
	}
	// Xray's RandBetween excludes the upper bound.
	n, err := rand.Int(rand.Reader, big.NewInt(int64(r.To)-int64(r.From)))
	if err != nil {
		return int(r.From)
	}
	return int(r.From) + int(n.Int64())
}

func rangeAt(ranges []option.V2RayXHTTPRange, index int) option.V2RayXHTTPRange {
	if index >= len(ranges) {
		index = len(ranges) - 1
	}
	return ranges[index]
}

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.count++
	f := &c.fragment
	if f.from == 0 && f.to == 1 {
		if c.count != 1 {
			return c.Conn.Write(p)
		}
		defer c.finished.Store(true)
		if len(p) <= 5 || p[0] != 22 {
			return c.Conn.Write(p)
		}
		recordLength := 5 + int(binary.BigEndian.Uint16(p[3:5]))
		if len(p) < recordLength {
			return c.Conn.Write(p)
		}
		return c.writeHello(p, recordLength)
	}
	if f.from != 0 && (c.count < f.from || c.count > f.to) {
		return c.Conn.Write(p)
	}
	if f.from != 0 && c.count == f.to {
		defer c.finished.Store(true)
	}
	maxSplit := pick(f.maxSplit)
	from := 0
	for index := 0; ; index++ {
		length := pick(rangeAt(f.lengths, index))
		to := len(p)
		if length < len(p)-from && !(maxSplit > 0 && index+1 >= maxSplit) {
			to = from + length
		}
		n, err := c.writeAll(p[from:to])
		from += n
		if err != nil {
			return from, err
		}
		if err = c.wait(time.Duration(pick(rangeAt(f.delays, index))) * time.Millisecond); err != nil {
			return from, err
		}
		if from >= len(p) {
			return from, nil
		}
	}
}

func (c *Conn) writeHello(p []byte, recordLength int) (int, error) {
	f := &c.fragment
	data := p[5:recordLength]
	merge := len(f.delays) == 1 && f.delays[0].To == 0
	maxSplit := pick(f.maxSplit)
	from := 0
	var merged []byte
	for index := 0; ; index++ {
		length := pick(rangeAt(f.lengths, index))
		to := len(data)
		if length < len(data)-from && !(maxSplit > 0 && index+1 >= maxSplit) {
			to = from + length
		}
		// A TLS record is at most 65535 bytes; allocations never follow an
		// arbitrarily large configured fragment length.
		part := make([]byte, 5+to-from)
		copy(part, p[:3])
		binary.BigEndian.PutUint16(part[3:5], uint16(to-from))
		copy(part[5:], data[from:to])
		if merge {
			merged = append(merged, part...)
		} else {
			_, err := c.writeAll(part)
			if err != nil {
				return from, err
			}
			if err = c.wait(time.Duration(pick(rangeAt(f.delays, index))) * time.Millisecond); err != nil {
				return to, err
			}
		}
		from = to
		if from == len(data) {
			if merge {
				if _, err := c.writeAll(merged); err != nil {
					return 0, err
				}
			}
			if len(p) > recordLength {
				n, err := c.writeAll(p[recordLength:])
				return recordLength + n, err
			}
			return len(p), nil
		}
	}
}

func (c *Conn) writeAll(p []byte) (int, error) {
	total := 0
	for {
		if err := c.wait(0); err != nil {
			return total, err
		}
		n, err := c.Conn.Write(p[total:])
		if n < 0 || n > len(p)-total {
			return total, io.ErrShortWrite
		}
		total += n
		if err != nil {
			return total, err
		}
		if total == len(p) {
			return total, nil
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
}

// wait observes changed write deadlines as well as context cancellation and
// Close. In particular a delay from a subscription cannot outlive its dial.
func (c *Conn) wait(delay time.Duration) error {
	until := time.Now().Add(delay)
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-c.closed:
			return net.ErrClosed
		default:
		}
		c.stateMu.Lock()
		deadline, changed := c.deadline, c.changed
		c.stateMu.Unlock()
		now := time.Now()
		if !deadline.IsZero() && !now.Before(deadline) {
			return os.ErrDeadlineExceeded
		}
		remaining := until.Sub(now)
		if remaining <= 0 {
			return nil
		}
		if !deadline.IsZero() && deadline.Sub(now) < remaining {
			remaining = deadline.Sub(now)
		}
		timer := time.NewTimer(remaining)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return c.ctx.Err()
		case <-c.closed:
			timer.Stop()
			return net.ErrClosed
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Conn) Close() error {
	c.close.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *Conn) SetDeadline(deadline time.Time) error {
	if err := c.Conn.SetDeadline(deadline); err != nil {
		return err
	}
	c.setWriteDeadline(deadline)
	return nil
}

func (c *Conn) SetWriteDeadline(deadline time.Time) error {
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	c.setWriteDeadline(deadline)
	return nil
}

func (c *Conn) setWriteDeadline(deadline time.Time) {
	c.stateMu.Lock()
	c.deadline = deadline
	close(c.changed)
	c.changed = make(chan struct{})
	c.stateMu.Unlock()
}

func (c *Conn) ReaderReplaceable() bool { return true }
func (c *Conn) WriterReplaceable() bool { return c.finished.Load() }
func (c *Conn) Upstream() any           { return c.Conn }
