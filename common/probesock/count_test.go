package probesock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func at(milliseconds int) time.Time {
	return time.Unix(1000, 0).Add(time.Duration(milliseconds) * time.Millisecond)
}

func newCounter() *counter {
	return &counter{window: 10 * time.Millisecond}
}

func TestCounterInOrder(t *testing.T) {
	c := newCounter()
	for i := 0; i < 10; i++ {
		c.add(at(i), uint32(5000+i*1000), 1000)
	}
	counts := c.finish()
	require.Equal(t, int64(10000), counts.Bytes)
	require.Zero(t, counts.Holes)
	require.Zero(t, counts.LostBytes)
	require.Equal(t, 10, counts.Packets)
}

// A hole filled later than the window is a loss: the retransmission of the segment.
func TestCounterLoss(t *testing.T) {
	c := newCounter()
	c.add(at(0), 0, 1000)
	c.add(at(1), 1000, 1000)
	c.add(at(2), 3000, 1000)
	c.add(at(3), 4000, 1000)
	c.add(at(100), 2000, 1000)
	counts := c.finish()
	require.Equal(t, 1, counts.Holes)
	require.Equal(t, int64(1000), counts.LostBytes)
	require.Equal(t, 1, counts.LostFills)
	require.Zero(t, counts.ReorderBytes)
	require.Zero(t, counts.Unfilled)
	require.Equal(t, int64(5000), counts.Bytes)
}

// A hole filled within the window is reordering, not a loss.
func TestCounterReorder(t *testing.T) {
	c := newCounter()
	c.add(at(0), 0, 1000)
	c.add(at(1), 2000, 1000)
	c.add(at(2), 1000, 1000)
	counts := c.finish()
	require.Equal(t, int64(1000), counts.ReorderBytes)
	require.Zero(t, counts.LostBytes)
	require.Zero(t, counts.LostFills)
}

func TestCounterDuplicateAndUnfilled(t *testing.T) {
	c := newCounter()
	c.add(at(0), 0, 1000)
	c.add(at(1), 0, 1000)
	c.add(at(2), 2000, 1000)
	counts := c.finish()
	require.Equal(t, int64(1000), counts.DupBytes)
	require.Equal(t, int64(1000), counts.Unfilled)
}

// A hole of two segments filled late by two retransmissions is two lost segments.
func TestCounterPartialFills(t *testing.T) {
	c := newCounter()
	c.add(at(0), 0, 1000)
	c.add(at(1), 3000, 1000)
	c.add(at(50), 1000, 1000)
	c.add(at(60), 2000, 1000)
	counts := c.finish()
	require.Equal(t, int64(2000), counts.LostBytes)
	require.Equal(t, 2, counts.LostFills)
	segments, lost := counts.segments(1000)
	require.Equal(t, 4, segments)
	require.Equal(t, 2, lost)
}

// The sequence number wraps around; a number far behind the stream is not taken.
func TestCounterWrap(t *testing.T) {
	c := newCounter()
	c.add(at(0), 0xffffff00, 0x100)
	c.add(at(1), 0, 1000)
	c.add(at(2), 0x7fff0000, 1000)
	counts := c.finish()
	require.Equal(t, int64(0x100+1000), counts.Bytes)
	require.Zero(t, counts.Holes)
}

func TestSegments(t *testing.T) {
	segments, lost := Counts{Bytes: 14480, LostBytes: 1448}.segments(1448)
	require.Equal(t, 10, segments)
	require.Equal(t, 1, lost)
	segments, lost = Counts{Bytes: 14480, LostBytes: 100, LostFills: 2}.segments(0)
	require.Equal(t, 10, segments)
	require.Equal(t, 2, lost)
}

func TestReorderWindow(t *testing.T) {
	require.Equal(t, minReorderWindow, reorderWindow(time.Millisecond))
	require.Equal(t, 25*time.Millisecond, reorderWindow(100*time.Millisecond))
}
