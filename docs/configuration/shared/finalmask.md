# Client TCP fragmentation

The engine imports Xray's `fm` parameter from VLESS and other TLS proxy links.
Supported `tcp` masks of type `fragment` are kept under `tls.finalmask` in the
decoded outbound and applied to the TCP bytes emitted by TLS or REALITY.
This works for raw TCP and XHTTP over TCP, including shared-port REALITY
fallbacks. A separate XHTTP download stream reads its own
`downloadSettings.finalmask`; it does not inherit the upload stream's mask.

```json
{
  "tls": {
    "enabled": true,
    "server_name": "vpn.example.org",
    "finalmask": {
      "tcp": [
        {
          "type": "fragment",
          "settings": {
            "packets": "1-3",
            "length": "100-200",
            "delay": "1-3"
          }
        }
      ]
    }
  }
}
```

This is an optional client setting; it does not require a server-side mask.
Leave it unset when fragmentation is not needed. The example illustrates the
format, not a guarantee of bypassing a particular network's filtering.

- `packets: "1-3"` splits the first three writes to TCP. `"1-1"` selects
  only the first write. An empty value selects every write.
- `packets: "tlshello"` rewrites the first complete TLS record of the first
  write into smaller records. This changes TLS record boundaries and can be
  incompatible with REALITY server versions. Numeric packet ranges preserve
  the TLS byte stream.
- `length` is the fragment length in bytes; `delay` is the pause in
  milliseconds after **each** fragment, including the last one.
- `lengths` and `delays` arrays override their singular forms. Their last
  entries repeat. Earlier length entries may be zero; the final one must
  have a positive minimum.
- `maxSplit` limits the number of pieces per write or TLS record. The last
  piece carries the remainder; zero means no limit.
- Ranges use Xray's upper-exclusive random selection, or a fixed value when
  both endpoints are equal.

For example, `length: "10-20"` with `delay: "10-20"` can add seconds to a
connection: a 1500-byte write needs about 100 pieces and about 1.5 seconds
of pauses. HTTP connection probes include this setup cost.

The `interval` field belongs to Xray's Freedom fragmentation settings, not
FinalMask. In imported links it is ignored with a warning, as in Xray; it is
not silently converted into `delay`.

TCP masks are refused with QUIC/HTTP3, disabled TLS, system TLS engines,
native `fragment`/`record_fragment`, or kTLS. Unsupported masks that change
the server's expected wire format are refused. Existing client-only noise
settings remain ignored with a warning; this does not implement noise or
the other FinalMask obfuscation protocols.
