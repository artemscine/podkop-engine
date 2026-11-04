package group

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
)

// hopSpacing is the least time between the starts of two URL tests whose connections go to
// the same first server (podkop-engine). An operator's DPI that freezes bursts of TLS hellos to
// one server counts them per server (2026-10-31, from two Russian access networks: a burst to
// one server froze it while a single hello to another passed). Hellos to one server one every
// 2 s passed 20 of 20, one every second 12 and 7 of 20. Tests through different first servers
// run at once.
const hopSpacing = 2 * time.Second

// probes is the URL tester of the process (podkop-engine). The groups of a configuration share
// their members (a server in the groups of several rules) and the first servers of the members
// (twenty servers of a subscription reached through one detour): each group testing on its
// own would test a server as many times as it has groups, at the same moment, and its rounds
// would open a TLS connection to the shared detour per member at once. Here a test of one
// outbound with one URL that another group asked for already is waited for, not run again,
// and tests through one first server begin hopSpacing apart.
var probes = newProber(hopSpacing)

type prober struct {
	spacing time.Duration
	access  sync.Mutex
	calls   map[probeKey]*probeCall
	// when the next test through a first server may begin
	hops map[any]time.Time
}

type probeKey struct {
	detour adapter.Outbound
	link   string
}

type probeCall struct {
	done  chan struct{}
	delay uint16
	err   error
	// the test was cut because the round of the group that ran it ended: the groups that
	// waited for it test themselves
	abandoned bool
}

func newProber(spacing time.Duration) *prober {
	return &prober{spacing: spacing, calls: make(map[probeKey]*probeCall), hops: make(map[any]time.Time)}
}

// test runs one URL test of detour with link, or waits for the one that runs. The outbounds
// are the configuration's own objects: two configurations (a reload, the tests) share none.
func (p *prober) test(ctx context.Context, manager adapter.OutboundManager, link string, detour adapter.Outbound, slots chan struct{}) (uint16, error) {
	key := probeKey{detour, link}
	for {
		p.access.Lock()
		call, running := p.calls[key]
		if !running {
			call = &probeCall{done: make(chan struct{})}
			p.calls[key] = call
			p.access.Unlock()
			call.delay, call.err = p.run(ctx, firstHop(manager, detour), link, detour, slots)
			call.abandoned = call.err != nil && roundEnded(ctx)
			p.access.Lock()
			delete(p.calls, key)
			p.access.Unlock()
			close(call.done)
			return call.delay, call.err
		}
		p.access.Unlock()
		select {
		case <-call.done:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		if !call.abandoned {
			return call.delay, call.err
		}
	}
}

// run takes one of slots (nil: none), then waits until a test through hop may begin; while it
// waits it holds no slot, so that tests through other first servers go on.
func (p *prober) run(ctx context.Context, hop any, link string, detour adapter.Outbound, slots chan struct{}) (uint16, error) {
	for {
		if slots != nil {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		wait := p.reserve(hop, time.Now())
		if wait == 0 {
			break
		}
		if slots != nil {
			<-slots
		}
		if err := sleepContext(ctx, wait); err != nil {
			return 0, err
		}
	}
	if slots != nil {
		defer func() { <-slots }()
	}
	return probeOutbound(ctx, link, detour)
}

// waitHop waits until a connection through hop may begin, and reserves it: for a test that
// takes no slot (the download test, the test of a member whose connections fail).
func (p *prober) waitHop(ctx context.Context, hop any) error {
	for {
		wait := p.reserve(hop, time.Now())
		if wait == 0 {
			return nil
		}
		if err := sleepContext(ctx, wait); err != nil {
			return err
		}
	}
}

// reserve gives hop to a test beginning at now and returns 0, or tells how long to wait.
func (p *prober) reserve(hop any, now time.Time) time.Duration {
	if p.spacing <= 0 {
		return 0
	}
	p.access.Lock()
	defer p.access.Unlock()
	if next, reserved := p.hops[hop]; reserved && now.Before(next) {
		return next.Sub(now)
	}
	if len(p.hops) > 1024 {
		for key, next := range p.hops {
			if !now.Before(next) {
				delete(p.hops, key)
			}
		}
	}
	p.hops[hop] = now.Add(p.spacing)
	return 0
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// firstHop is the outbound whose server a connection through detour reaches first: detour, or
// the last of its chain of detours. A group in the chain ends it (which of its members a
// connection takes varies), and so does a detour the manager does not know (an endpoint).
func firstHop(manager adapter.OutboundManager, detour adapter.Outbound) any {
	current := detour
	for range 8 {
		if _, isGroup := current.(adapter.OutboundGroup); isGroup || manager == nil {
			break
		}
		dependencies := current.Dependencies()
		if len(dependencies) != 1 {
			break
		}
		next, loaded := manager.Outbound(dependencies[0])
		if !loaded {
			return dependencies[0]
		}
		current = next
	}
	return current
}

// probePlan is what a round tests its members with (podkop-engine).
type probePlan struct {
	// links[0] is the URL of the round; links[1], when there is one, is tried once after a
	// test with it fails: a member is unavailable when two URLs of the list do not answer
	// through it, not one (a server's hoster may block one of them)
	links []string
	round int64
	// keep is how many rounds the delay of a member is the median of
	// (urltest.HistoryStorage.StoreURLTestSample); 0: the delay stored is the one measured
	// (a test the Clash API asked for with its own URL)
	keep int
}

// roundPlan is the plan of the round that begins at now: the rounds are numbered by the clock
// (now divided by interval), so that the groups of the process test with the same URL in the
// same round, and the probes of one member by different groups are one.
func roundPlan(links []string, interval time.Duration, now time.Time) probePlan {
	if len(links) == 0 {
		return probePlan{links: []string{""}, keep: 1}
	}
	round := int64(0)
	if interval > 0 {
		round = now.UnixNano() / int64(interval)
	}
	first := int(round % int64(len(links)))
	plan := probePlan{links: []string{links[first]}, round: round, keep: len(links)}
	if len(links) > 1 {
		plan.links = append(plan.links, links[(first+1)%len(links)])
	}
	return plan
}

// testLinks are the URLs of the tests of a group: urls, or url, without empty and repeated ones;
// none for the default (urltest.URLTest).
func testLinks(options option.URLTestOutboundOptions) []string {
	var links []string
	for _, link := range options.URLs {
		if link != "" && !slices.Contains(links, link) {
			links = append(links, link)
		}
	}
	if len(links) == 0 {
		return linkList(options.URL)
	}
	return links
}

func linkList(link string) []string {
	if link == "" {
		return nil
	}
	return []string{link}
}

// checkTestLink tells whether a URL test can use link.
func checkTestLink(link string) error {
	return checkDownloadLink("urls", link)
}
