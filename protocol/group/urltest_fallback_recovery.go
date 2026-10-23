package group

// Optional fallback recovery — optional recovery confirmation. Selection and ordering
// remain the author's policy; this only delays re-admission of failed members.

import (
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"sync"
	"time"
)

type fallbackRecovery struct {
	access    sync.Mutex
	successes int
	delay     time.Duration
	members   map[string]*fallbackRecoveryMember
}

type fallbackRecoveryMember struct {
	failuresAt  time.Time
	firstAnswer time.Time
	countedAt   time.Time
	observedAt  time.Time
	successes   int
}

func validateFallbackRecovery(options option.URLTestOutboundOptions) error {
	r := options.FallbackRecovery
	if r == nil {
		return nil
	}
	if len(options.Fallbacks) == 0 {
		return E.New("urltest: fallback_recovery requires fallbacks")
	}
	if r.Successes < 0 || r.Successes > 100 {
		return E.New("urltest: fallback_recovery.successes must be 1..100 (0 uses 3)")
	}
	if r.MinHealthyTime != nil && (time.Duration(*r.MinHealthyTime) < 0 || time.Duration(*r.MinHealthyTime) > 24*time.Hour) {
		return E.New("urltest: fallback_recovery.min_healthy_time must be 0..24h")
	}
	return nil
}

func newFallbackRecovery(options *option.URLTestFallbackRecoveryOptions) *fallbackRecovery {
	if options == nil {
		return nil
	}
	successes := options.Successes
	if successes == 0 {
		successes = 3
	}
	delay := time.Minute
	if options.MinHealthyTime != nil {
		delay = time.Duration(*options.MinHealthyTime)
	}
	return &fallbackRecovery{successes: successes, delay: delay, members: make(map[string]*fallbackRecoveryMember)}
}

func (r *fallbackRecovery) failed(tag string) {
	r.access.Lock()
	defer r.access.Unlock()
	r.members[tag] = &fallbackRecoveryMember{failuresAt: time.Now()}
}

func (r *fallbackRecovery) answered(tag string, at time.Time, interval time.Duration) {
	r.access.Lock()
	defer r.access.Unlock()
	member := r.members[tag]
	if member == nil || !at.After(member.failuresAt) || !at.After(member.observedAt) {
		return
	}
	member.observedAt = at
	if member.firstAnswer.IsZero() {
		member.firstAnswer = at
	}
	// Manual tests and dashboard refreshes cannot manufacture consecutive scheduled
	// successes. Count at most one success per group interval, using proof timestamps.
	if member.countedAt.IsZero() || at.Sub(member.countedAt) >= interval {
		member.countedAt = at
		member.successes++
	}
}

func (r *fallbackRecovery) recovered(tag string, now time.Time) bool {
	r.access.Lock()
	defer r.access.Unlock()
	member := r.members[tag]
	return member == nil || (member.successes >= r.successes && !member.firstAnswer.IsZero() && now.Sub(member.firstAnswer) >= r.delay && member.observedAt.Sub(member.firstAnswer) >= r.delay)
}

func (g *URLTestGroup) fallbackRecovered(tag string) bool {
	return g.fallbackRecovery == nil || g.fallbackRecovery.recovered(tag, time.Now())
}

func (g *URLTestGroup) recoveryFailed(tag string) {
	if g.fallbackRecovery != nil {
		g.fallbackRecovery.failed(tag)
	}
}
