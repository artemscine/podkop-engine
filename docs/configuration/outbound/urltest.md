### Structure

```json
{
  "type": "urltest",
  "tag": "auto",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "fallbacks": [
    "backup-a",
    "backup-b"
  ],
  "url": "",
  "download_url": "",
  "interval": "",
  "tolerance": 0,
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

### Fields

#### outbounds

List of outbound tags to test. Required unless `fallbacks` is nonempty (podkop-engine r20).

#### fallbacks

!!! info "podkop-engine r10"

List of outbound tags to use, in this order, while none of `outbounds` is available.

The fallbacks are tested with the outbounds every interval. The group selects the fastest of
`outbounds`, by the tolerance; only when none of them has passed its test, it selects the first
fallback in the list that has, regardless of the delays. As soon as one of `outbounds` passes
its test again, the group returns to it. The fallbacks follow the outbounds in the list of the
group members (Clash API `all`). With the download test (`download_url`), a fallback passes it
before the group switches to it, as an outbound does.

A fallback may not repeat a tag of `outbounds` or of `fallbacks`.

On podkop-engine r20 a `block` fallback is a terminal refusal when no preceding member
answers; it is never probed. Before the first round finishes the group uses its first
member rather than prematurely falling back to direct or block. While block is selected,
retry rounds begin after 30 seconds and back off to the configured interval.

#### fallback_selection

Optional selection policy for `fallbacks`: `ordered` (the default) or `fastest`.
With `fastest`, available fallbacks compete by delay using the group's tolerance;
the current fallback stays within that tolerance. Available `outbounds` always
take precedence. Failure, download checks and optional recovery confirmation
apply to both policies. Omitting the option preserves ordered selection.
A `block` fallback remains terminal: healthy candidates before it compete by delay,
and members after it cannot be selected.

#### fallback_recovery

Optional confirmation before a failed member is preferred again. Requires `fallbacks`.
Set `successes` (default `3`) and `min_healthy_time` (default `"1m"`); the observations
must span that healthy period, with at most one success counted per group interval.
New failures reset the proof. Without this option the author's immediate recovery applies.

If no confirmed path answers, a responding recovering member remains usable before a
terminal block fallback. An outbound swap retains independent proof snapshots only for
unchanged server objects; replacing a server under the same tag starts new proof.

#### url

The URL to test. `https://www.gstatic.com/generate_204` will be used if empty.

#### download_url

!!! info "podkop-engine r10"

The URL of the download test. Before the group switches to a member, it downloads 64 KiB from
this URL through the member; a member whose download stops on the way is passed over until its
next test result. The URL must serve at least 64 KiB, or announce a shorter length.

| Value              | Download test                                                         |
|--------------------|-----------------------------------------------------------------------|
| (absent)           | The URL in the `SING_BOX_URLTEST_DOWNLOAD_URL` environment variable, off without it |
| `""`               | Off for this group                                                    |
| `http`/`https` URL | This URL                                                              |

#### interval

The test interval. `3m` will be used if empty.

#### tolerance

The test tolerance in milliseconds. `50` will be used if empty.

#### idle_timeout

The idle timeout. `30m` will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
