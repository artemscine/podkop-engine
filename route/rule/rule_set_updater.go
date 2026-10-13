package rule

import (
	"context"
	"runtime"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

type RuleSetUpdater struct {
	ctx      context.Context
	cancel   context.CancelFunc
	ruleSets []*RemoteRuleSet
	// requests are updates asked for now, each with where to answer
	requests chan chan RuleSetUpdateSummary
}

// RuleSetUpdateSummary is what an update of the remote rule-sets came to.
type RuleSetUpdateSummary struct {
	// Total is the number of remote rule-sets.
	Total int
	// Loaded is how many of them got a new list.
	Loaded int
	// Failed is how many could not be downloaded or read.
	Failed int
}

func NewRuleSetUpdater(ctx context.Context, ruleSets []adapter.RuleSet) *RuleSetUpdater {
	var remoteRuleSets []*RemoteRuleSet
	for _, ruleSet := range ruleSets {
		remoteRuleSet, isRemote := ruleSet.(*RemoteRuleSet)
		if isRemote {
			remoteRuleSets = append(remoteRuleSets, remoteRuleSet)
		}
	}
	if len(remoteRuleSets) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	return &RuleSetUpdater{
		ctx:      ctx,
		cancel:   cancel,
		ruleSets: remoteRuleSets,
		requests: make(chan chan RuleSetUpdateSummary),
	}
}

func (u *RuleSetUpdater) Start() {
	go u.loopUpdate()
}

func (u *RuleSetUpdater) Close() error {
	u.cancel()
	return nil
}

// UpdateNow updates every remote rule-set at once, whatever its update_interval, and
// returns what that came to. The update runs where the scheduled ones run, one rule-set
// after another; the next scheduled update of each is counted from now.
func (u *RuleSetUpdater) UpdateNow(ctx context.Context) (RuleSetUpdateSummary, error) {
	reply := make(chan RuleSetUpdateSummary, 1)
	select {
	case u.requests <- reply:
	case <-u.ctx.Done():
		return RuleSetUpdateSummary{}, u.ctx.Err()
	case <-ctx.Done():
		return RuleSetUpdateSummary{}, ctx.Err()
	}
	select {
	case summary := <-reply:
		return summary, nil
	case <-u.ctx.Done():
		return RuleSetUpdateSummary{}, u.ctx.Err()
	case <-ctx.Done():
		return RuleSetUpdateSummary{}, ctx.Err()
	}
}

func (u *RuleSetUpdater) loopUpdate() {
	nextUpdates := make([]time.Time, len(u.ruleSets))
	retryDelays := make([]time.Duration, len(u.ruleSets))
	for i, ruleSet := range u.ruleSets {
		nextUpdates[i] = ruleSet.lastUpdated.Add(ruleSet.updateInterval)
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		// an update asked for now takes every rule-set, due or not
		var reply chan RuleSetUpdateSummary
		select {
		case <-u.ctx.Done():
			return
		case <-timer.C:
		case reply = <-u.requests:
		}
		now := time.Now()
		var loaded bool
		summary := RuleSetUpdateSummary{Total: len(u.ruleSets)}
		for i, ruleSet := range u.ruleSets {
			if reply == nil && now.Before(nextUpdates[i]) {
				continue
			}
			failures := ruleSet.failures
			if ruleSet.updateOnce() {
				loaded = true
				summary.Loaded++
			} else if ruleSet.failures > failures {
				summary.Failed++
			}
			nextUpdates[i] = now.Add(ruleSet.updateInterval)
			if ruleSet.startedEmpty {
				if ruleSet.lastUpdated.IsZero() {
					// Never downloaded (initial fetch failed): retry with backoff
					// instead of waiting a whole update_interval.
					retryDelays[i] = min(max(retryDelays[i]*2, 5*time.Second), 30*time.Second)
					nextUpdates[i] = now.Add(retryDelays[i])
				} else {
					ruleSet.startedEmpty = false
					notifyRuleSetRecovered(ruleSet.logger, ruleSet.tag)
				}
			}
		}
		if reply != nil {
			reply <- summary
		}
		// the old list is garbage once a new one is loaded; a failed or not-modified
		// update leaves nothing to collect
		if loaded {
			runtime.GC()
		}
		timer.Reset(waitUntilNext(nextUpdates))
	}
}

func waitUntilNext(nextUpdates []time.Time) time.Duration {
	next := nextUpdates[0]
	for _, nextUpdate := range nextUpdates[1:] {
		if nextUpdate.Before(next) {
			next = nextUpdate
		}
	}
	wait := time.Until(next)
	if wait < 0 {
		return 0
	}
	return wait
}
