package urltestgroup

import (
	"slices"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
)

func (g *URLTestGroup) storeHistory(tag string, history *adapter.URLTestHistory) {
	g.history.StoreURLTestHistory(tag, history)
	g.checkedMembers.Store(tag, struct{}{})
	if g.sharedHistory != nil {
		g.sharedHistory.CopyMemberFrom(g.history, tag, true, false, false)
	}
}

func (g *URLTestGroup) deleteHistory(tag string) {
	at := time.Now()
	g.history.StoreURLTestFailure(tag, at)
	g.checkedMembers.Store(tag, struct{}{})
	if g.sharedHistory != nil {
		g.sharedHistory.StoreURLTestFailure(tag, at)
	}
}

// MemberHistory reads the evidence used to choose a member. A nested URLTest
// chooses by its own check, not the URL of the group above it.
// A selector is tested by the group containing it and uses that group's store.
func MemberHistory(history *urltest.HistoryStorage, member adapter.Outbound, network string) *adapter.URLTestHistory {
	for member != nil {
		if nested, ok := member.(*URLTest); ok {
			if nested.group == nil {
				return nil
			}
			return nested.group.memberHistory(nested.Selected(network), network)
		}
		if nested, ok := member.(adapter.OutboundGroup); ok {
			member = nested.Selected(network)
			continue
		}
		return history.LoadURLTestHistory(member.Tag())
	}
	return nil
}

func (g *URLTestGroup) memberHistory(member adapter.Outbound, network string) *adapter.URLTestHistory {
	return MemberHistory(g.history, member, network)
}

func (g *URLTestGroup) notifyHistory() {
	g.history.NotifyUpdated()
	if g.sharedHistory != nil {
		g.sharedHistory.NotifyUpdated()
	}
}

// MemberHistories exposes evidence scoped to this group's check. The ordinary
// proxy history is the last probe of that proxy by any group and cannot identify
// which URL answered. Missing keys are unknown, while [] is a tested failure.
func (s *URLTest) MemberHistories() map[string][]*adapter.URLTestHistory {
	result := make(map[string][]*adapter.URLTestHistory)
	if s.group != nil {
		s.group.memberHistories(result, make(map[adapter.Outbound]bool))
	}
	return result
}

func (g *URLTestGroup) memberHistories(result map[string][]*adapter.URLTestHistory, seen map[adapter.Outbound]bool) {
	if !g.roundDone.Load() || g.closed.Load() {
		return
	}
	g.access.Lock()
	members := slices.Clone(g.members)
	g.access.Unlock()
	var collect func(adapter.Outbound)
	collect = func(member adapter.Outbound) {
		if seen[member] {
			return
		}
		seen[member] = true
		if nested, ok := member.(*URLTest); ok {
			if nested.group != nil {
				nested.group.memberHistories(result, seen)
			}
			return
		}
		if nested, ok := member.(adapter.OutboundGroup); ok {
			for _, tag := range nested.All() {
				if child, loaded := g.outbound.Outbound(tag); loaded {
					collect(child)
				}
			}
			return
		}
		if _, checked := g.checkedMembers.Load(member.Tag()); !checked {
			return
		}
		result[member.Tag()] = []*adapter.URLTestHistory{}
		if history := g.history.LoadURLTestHistory(member.Tag()); history != nil {
			result[member.Tag()] = []*adapter.URLTestHistory{history}
		}
	}
	for _, member := range members {
		collect(member)
	}
}

// storeDownload publishes detector telemetry without sharing selection evidence.
func (g *URLTestGroup) storeDownload(tag string, result urltest.DownloadResult) {
	g.history.StoreDownloadResult(tag, result)
	if g.sharedHistory != nil {
		g.sharedHistory.StoreDownloadResult(tag, result)
	}
}

func (g *URLTestGroup) storeLossSample(tag string, sample urltest.LossSample) {
	g.history.StoreLossSample(tag, sample)
	if g.sharedHistory != nil {
		g.sharedHistory.StoreLossSample(tag, sample)
	}
}

func (g *URLTestGroup) storeLossNotCounted(tag string, at time.Time) {
	g.history.StoreLossNotCounted(tag, at)
	if g.sharedHistory != nil {
		g.sharedHistory.StoreLossNotCounted(tag, at)
	}
}

func (g *URLTestGroup) storeLossFailure(tag string, at time.Time) int {
	failures := g.history.StoreLossFailure(tag, at)
	if g.sharedHistory != nil {
		g.sharedHistory.StoreLossFailure(tag, at)
	}
	return failures
}
