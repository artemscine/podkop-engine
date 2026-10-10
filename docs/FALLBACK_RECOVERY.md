# Priority recovery confirmation

The 1.13 package extends author ordered fallbacks with optional recovery proof:

```json
{"type":"urltest","outbounds":["primary"],"fallbacks":["reserve"],
 "fallback_recovery":{"successes":3,"min_healthy_time":"60s"}}
```

Without `fallback_recovery`, author selection is unchanged. After a classified
member failure, re-admission requires independent successful group probes and
proof spanning the healthy duration. Manual bursts and elapsed time alone do not
satisfy it; another failure resets proof. Download checks remain active and their
failures reset recovery too. Caller cancellation and failures across a network
reset do not count as member failures. A recovering responding member is usable
when no confirmed member is available. State is synchronized and memory-only;
a core restart clears it.

Defaults: three successes, one minute. Count accepts 1–100 (0 uses the default);
healthy duration accepts 0–24h. The configured group interval spaces successes.
Capability: `urltest.fallback_recovery` in `sing-box version`.

Validation: `TEST_RACE=1 scripts/test-recovery.sh 1.13` with the pinned Go version.
