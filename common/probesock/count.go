// Package probesock counts what a probe connection lost on its way to this host
// (podkop-engine).
//
// A loss probe downloads through a server over a TCP connection of its own, opened with a
// small receive buffer: the window the host offers stays at a few segments, so the server
// never sends a burst that a shallow queue on the way (the subscriber's line, mostly) would
// drop. Loss the probe made itself would show on every server alike. While the probe runs,
// the headers of the packets of its connection are captured, and the incoming sequence is
// followed: a hole a later packet fills is reordering when it is filled sooner than a
// quarter of the RTT, and a loss when filled later, by the retransmission of the lost
// segment. The kernel's own counters cannot tell the two apart: its count of out-of-order
// packets grows with the window, not with the loss, and calls a server that reorders lossy.
package probesock

import (
	"time"
)

// Counts are what the capture saw of the incoming data of the probe connection.
type Counts struct {
	Packets      int   // incoming packets with data
	Bytes        int64 // the sequence space they covered, once
	Holes        int   // gaps in the sequence that opened
	LostBytes    int64 // bytes of holes filled late: retransmitted
	LostFills    int   // packets that filled a hole late
	ReorderBytes int64 // bytes of holes filled soon: reordered
	DupBytes     int64 // bytes that came twice
	Unfilled     int64 // bytes of holes still open at the end
}

// minReorderWindow is the least time a hole may take to fill and still be reordering: a
// quarter of the RTT, but not less than this, so that the scheduling of the capture on a
// nearby server does not turn reordering into loss.
const minReorderWindow = 3 * time.Millisecond

// reorderWindow is that time for a connection whose connect took rtt.
func reorderWindow(rtt time.Duration) time.Duration {
	return max(rtt/4, minReorderWindow)
}

type hole struct {
	start, end int64
	at         time.Time
}

// counter follows the incoming sequence of one connection, packet by packet.
type counter struct {
	window  time.Duration
	started bool
	base    uint32
	highest int64
	holes   []hole
	Counts
}

// maxAhead bounds how far past the highest byte a packet may start: a sequence number
// further away is not of this stream (an older packet, wrapped around).
const maxAhead = 1 << 30

// add takes an incoming packet with payload bytes of data starting at seq, captured at at.
func (c *counter) add(at time.Time, seq uint32, payload int) {
	if payload <= 0 {
		return
	}
	if !c.started {
		c.base, c.started = seq, true
	}
	start := int64(seq - c.base)
	if start > c.highest+maxAhead {
		return
	}
	end := start + int64(payload)
	c.Packets++
	if start > c.highest {
		c.holes = append(c.holes, hole{c.highest, start, at})
		c.Holes++
		c.highest = end
		return
	}
	limit := min(end, c.highest)
	covered := int64(0)
	filledLate := false
	kept := c.holes[:0]
	for _, h := range c.holes {
		from, to := max(start, h.start), min(limit, h.end)
		if from >= to {
			kept = append(kept, h)
			continue
		}
		covered += to - from
		if at.Sub(h.at) < c.window {
			c.ReorderBytes += to - from
		} else {
			c.LostBytes += to - from
			filledLate = true
		}
		if h.start < from {
			kept = append(kept, hole{h.start, from, h.at})
		}
		if to < h.end {
			kept = append(kept, hole{to, h.end, h.at})
		}
	}
	c.holes = kept
	if filledLate {
		c.LostFills++
	}
	c.DupBytes += (limit - start) - covered
	if end > c.highest {
		c.highest = end
	}
}

// finish returns the counts of the connection so far.
func (c *counter) finish() Counts {
	counts := c.Counts
	counts.Bytes = c.highest
	for _, h := range c.holes {
		counts.Unfilled += h.end - h.start
	}
	return counts
}

// segments turns the counts into segments of size mss: how many the data took, and how many
// of them were lost (a lost segment is at least one late fill).
func (c Counts) segments(mss int) (segments int, lost int) {
	if mss <= 0 {
		mss = defaultMSS
	}
	segments = int((c.Bytes + int64(mss) - 1) / int64(mss))
	lost = int((c.LostBytes + int64(mss)/2) / int64(mss))
	lost = max(lost, c.LostFills)
	return
}

// defaultMSS is the segment size when the connection does not tell its own: Ethernet, IPv4,
// TCP timestamps.
const defaultMSS = 1448
