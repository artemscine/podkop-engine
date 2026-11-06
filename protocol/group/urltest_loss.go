package group

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
)

// The modes of a group (podkop-engine).
//
// fastest (the default) chooses by the delay of the URL tests, as before. loss chooses by
// the delay weighed by the loss loss probes counted (urltest.LossProbe): the members it may
// switch to and the member in use are probed once a loss_interval with LossProbeSmall bytes,
// and a member the group would switch to without a fresh count first with LossProbeLarge.
// balance spreads the connections over the best members by their source address
// (urltest_balance.go), leaving out a member that loses more than loss_threshold percent.
//
// The weight is the price of a page of report 02 of the stand: a page takes about
// 59 + 4.85·RTT + p·(155 + 1.2·RTT) ms over a path with RTT ms and a loss of p percent, on
// servers with CUBIC and with BBR alike. A member with the loss p is thus as slow as one
// without loss whose delay is greater by the factor 1 + p·(155/RTT + 1.2)/4.85: a near
// server (RTT 20 ms) with 0.8 % costs what a clean one at 50 ms does.
//
// Loss that all the probed members show (the subscriber's line, not a server) does not
// move the choice: the least loss among them is taken off each one's.

const (
	modeFastest = "fastest"
	modeLoss    = "loss"
	modeBalance = "balance"
)

const (
	// defaultLossInterval is how often the members a group may use are probed.
	defaultLossInterval = 30 * time.Minute
	// defaultLossThreshold is the loss, in percent, above which balance leaves a member out.
	defaultLossThreshold = 1.0
	// lossCandidates is how many members besides the one in use loss probes once an interval.
	lossCandidates = 3
	// lossMinSegments is the least segments a window needs to decide by: about one large
	// probe, or a few hours of small ones.
	lossMinSegments = 700
	// lossCommonMembers is how many members with a count it takes to tell the loss of the
	// line from the loss of a server.
	lossCommonMembers = 3
	// lossNotCountedRest is how long a member whose probe could not count is not probed.
	lossNotCountedRest = 6 * time.Hour
	// lossSwitchProbes bounds the large probes of one selection.
	lossSwitchProbes = 2
)

// lossProbeSlot lets one loss probe run at a time in the process: two at once would share
// the line and the window each sees.
var lossProbeSlot = make(chan struct{}, 1)

// lossOptions are the options of the modes loss and balance.
type lossOptions struct {
	link        string
	interval    time.Duration
	threshold   float64
	balanceSize int
	balanceIdle time.Duration
}

// groupMode checks the mode options and fills the defaults.
func groupMode(mode string, link string, interval time.Duration, threshold float64, balanceSize int, balanceIdle time.Duration) (string, lossOptions, error) {
	options := lossOptions{link: link, interval: interval, threshold: threshold, balanceSize: balanceSize, balanceIdle: balanceIdle}
	switch mode {
	case "", modeFastest:
		return modeFastest, options, nil
	case modeLoss, modeBalance:
	default:
		return "", options, E.New("unknown mode: ", mode)
	}
	if link == "" {
		return "", options, E.New("mode ", mode, " needs loss_url")
	}
	if err := checkDownloadLink("loss_url", link); err != nil {
		return "", options, err
	}
	if options.interval == 0 {
		options.interval = defaultLossInterval
	} else if options.interval < time.Minute {
		return "", options, E.New("loss_interval must be at least 1m")
	}
	if options.threshold == 0 {
		options.threshold = defaultLossThreshold
	} else if options.threshold < 0 || options.threshold > 100 {
		return "", options, E.New("loss_threshold must be a percentage")
	}
	if options.balanceSize == 0 {
		options.balanceSize = defaultBalanceSize
	} else if options.balanceSize < 0 {
		return "", options, E.New("balance_size must be positive")
	}
	if options.balanceIdle == 0 {
		options.balanceIdle = defaultBalanceIdle
	}
	return mode, options, nil
}

// weighDelay is delay weighed by a loss of percent over a path with the RTT rtt.
func weighDelay(delay uint16, percent float64, rtt time.Duration) uint16 {
	if percent <= 0 {
		return delay
	}
	milliseconds := float64(rtt) / float64(time.Millisecond)
	if milliseconds < 5 {
		milliseconds = 5
	}
	weighed := float64(delay) * (1 + percent*(155/milliseconds+1.2)/4.85)
	if weighed > 65535 {
		return 65535
	}
	return uint16(weighed)
}

// memberLoss is what the probes of a member say, for a selection.
type memberLoss struct {
	percent float64 // over the window decided by
	excess  float64 // less the loss of the line
	rtt     time.Duration
	known   bool
}

// lossView is the loss of the members at one moment: what a selection weighs by.
type lossView map[string]memberLoss

// lossView reads the loss of the outbounds; nil in the mode fastest.
func (g *URLTestGroup) lossView() lossView {
	if g.mode == modeFastest {
		return nil
	}
	now := time.Now()
	view := make(lossView)
	floor, counted := 0.0, 0
	for _, member := range g.outbounds {
		tag := RealTag(member, N.NetworkTCP)
		window, known := g.history.LossOfWindow(tag, now, lossMinSegments)
		if !known {
			continue
		}
		loss := memberLoss{percent: window.Rate(), rtt: g.history.LossRTT(tag, now), known: true}
		if counted == 0 || loss.percent < floor {
			floor = loss.percent
		}
		counted++
		view[tag] = loss
	}
	for tag, loss := range view {
		loss.excess = loss.percent
		if counted >= lossCommonMembers {
			loss.excess -= floor
		}
		view[tag] = loss
	}
	return view
}

// delay is the delay of a member a selection compares: weighed by its loss in the mode loss.
func (v lossView) delay(tag string, history *adapter.URLTestHistory) uint16 {
	loss, known := v[tag]
	if !known || loss.excess <= 0 {
		return history.Delay
	}
	rtt := loss.rtt
	if rtt == 0 {
		// no RTT of its own: a URL test takes a few round trips
		rtt = time.Duration(history.Delay) * time.Millisecond / 3
	}
	return weighDelay(history.Delay, loss.excess, rtt)
}

// memberAlive tells whether a member may carry connections: it has a test result and has
// not failed the download test since.
func (g *URLTestGroup) memberAlive(member adapter.Outbound) bool {
	tag := RealTag(member, N.NetworkTCP)
	history := g.history.LoadURLTestHistory(tag)
	if history == nil {
		return false
	}
	frozenAt, isFrozen := g.frozen.Load()[tag]
	return !isFrozen || history.Time.After(frozenAt)
}

// rankedOutbounds returns the outbounds that may carry connections of the network, the
// fastest first by the delay a selection compares, with that delay.
func (g *URLTestGroup) rankedOutbounds(network string, view lossView) ([]adapter.Outbound, []uint16) {
	type ranked struct {
		member adapter.Outbound
		delay  uint16
	}
	var list []ranked
	for _, member := range g.outbounds {
		if !slices.Contains(member.Network(), network) || !g.memberAlive(member) {
			continue
		}
		tag := RealTag(member, network)
		history := g.history.LoadURLTestHistory(tag)
		if history == nil {
			continue
		}
		list = append(list, ranked{member, view.delay(tag, history)})
	}
	slices.SortStableFunc(list, func(a, b ranked) int {
		return int(a.delay) - int(b.delay)
	})
	members := make([]adapter.Outbound, len(list))
	delays := make([]uint16, len(list))
	for i, item := range list {
		members[i], delays[i] = item.member, item.delay
	}
	return members, delays
}

// lossDue tells whether a member is to be probed now: its last count is older than the
// interval, and its last probe did count (or long ago).
func (g *URLTestGroup) lossDue(tag string) bool {
	if notCounted := g.history.LossNotCounted(tag); !notCounted.IsZero() && time.Since(notCounted) < lossNotCountedRest {
		return false
	}
	last, exists := g.history.LastLossSample(tag)
	// a little early: the rounds that bring the probes come once a URL test interval
	return !exists || time.Since(last.At) >= g.loss.interval-g.interval/2
}

// needsSwitchProbe tells whether a member the group would switch to is probed first: it can
// be counted, and has no count to decide by that is fresher than the interval.
func (g *URLTestGroup) needsSwitchProbe(tag string) bool {
	if notCounted := g.history.LossNotCounted(tag); !notCounted.IsZero() && time.Since(notCounted) < lossNotCountedRest {
		return false
	}
	last, exists := g.history.LastLossSample(tag)
	if !exists || time.Since(last.At) >= g.loss.interval {
		return true
	}
	_, known := g.history.LossOfWindow(tag, time.Now(), lossMinSegments)
	return !known
}

// probeLoss runs a loss probe of size bytes through member and keeps what it counted. It
// reports whether a count was kept, and the error of a probe that did not download.
func (g *URLTestGroup) probeLoss(tag string, member adapter.Outbound, size int64, periodic bool) (bool, error) {
	ctx, cancel := context.WithTimeout(g.ctx, urltest.LossProbeTimeout+urltest.LossProbeTimeout/2)
	defer cancel()
	select {
	case lossProbeSlot <- struct{}{}:
	case <-ctx.Done():
		return false, nil
	}
	defer func() { <-lossProbeSlot }()
	if periodic && !g.lossDue(tag) {
		// another group probed it while this one waited
		return false, nil
	}
	// a new connection to the first server of the member, like a URL test (probes)
	if probes.waitHop(ctx, firstHop(g.outbound, member)) != nil {
		return false, nil
	}
	start := time.Now()
	sample, err := urltest.LossProbe(ctx, g.loss.link, size, member)
	switch {
	case err == nil:
		g.history.StoreLossSample(tag, sample)
		g.logger.Debug("outbound ", tag, " loss probe: ", sample.Lost, " of ", sample.Segments, " segments lost, ",
			sample.Reordered, " bytes reordered, RTT ", sample.RTT.Milliseconds(), "ms, ", sample.Received, " bytes in ",
			sample.Elapsed.Milliseconds(), "ms")
		return true, nil
	case errors.Is(err, urltest.ErrLossNotCounted):
		g.history.StoreLossNotCounted(tag, time.Now())
		g.logger.Info("outbound ", tag, " loss probe could not count, the member is chosen by its delay: ", err)
		return false, nil
	case g.ctx.Err() != nil || g.networkChangedSince(start):
		return false, nil
	default:
		g.logger.Warn("outbound ", tag, " loss probe: ", err)
		return false, err
	}
}

// lossTargets are the members the periodic probes keep counted: in the mode loss the one
// in use and the lossCandidates best others, in the mode balance the members of the
// balance and two more.
func (g *URLTestGroup) lossTargets() []adapter.Outbound {
	ranked, _ := g.rankedOutbounds(N.NetworkTCP, g.lossView())
	var targets []adapter.Outbound
	switch g.mode {
	case modeLoss:
		current := g.selectedOutboundTCP.Load()
		if current != nil && !g.isFallback(current) && g.memberAlive(current) {
			targets = append(targets, current)
		}
		for _, member := range ranked {
			if len(targets) >= lossCandidates+1 {
				break
			}
			if member != current {
				targets = append(targets, member)
			}
		}
	case modeBalance:
		targets = ranked[:min(len(ranked), g.loss.balanceSize+2)]
	}
	return targets
}

// lossRound runs the periodic probes of the members that are due, after a round, in the
// background: one round of probes at a time. A count kept asks for a selection.
func (g *URLTestGroup) lossRound() {
	if g.mode == modeFastest || g.closed.Load() || !g.lossRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer g.lossRunning.Store(false)
		counted := false
		for _, member := range g.lossTargets() {
			if g.ctx.Err() != nil || g.networkPaused() {
				return
			}
			tag := RealTag(member, N.NetworkTCP)
			if !g.lossDue(tag) {
				continue
			}
			if kept, _ := g.probeLoss(tag, member, urltest.LossProbeSmall, true); kept {
				counted = true
			}
		}
		if counted {
			g.requestUpdateCheck()
		}
	}()
}
