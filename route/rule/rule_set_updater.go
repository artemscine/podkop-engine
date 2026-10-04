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
	}
}

func (u *RuleSetUpdater) Start() {
	go u.loopUpdate()
}

func (u *RuleSetUpdater) Close() error {
	u.cancel()
	return nil
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
		select {
		case <-u.ctx.Done():
			return
		case <-timer.C:
		}
		now := time.Now()
		var loaded bool
		for i, ruleSet := range u.ruleSets {
			if now.Before(nextUpdates[i]) {
				continue
			}
			if ruleSet.updateOnce() {
				loaded = true
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
