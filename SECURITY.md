# Security policy

## Supported versions

Only the latest released minor version receives security fixes.

## Threat model (summary)

`zblob` treats **everything received from the network as attacker input**:

- Tier-1 replies are BLAKE3/bao-verified against the manifest root *before*
  touching disk; pin the root (`DownloadRequest::pinned`) to also remove the
  server's choice of content. Unpinned fetches are trust-on-first-use and
  documented as such.
- Tier-2 indices are fully validated (schema version, id, CDC parameters,
  path/symlink sanitization, size↔chunk consistency, root recomputation)
  before any chunk is fetched, and materialization is defensive: relative
  `Normal`-component paths only, symlinks last with lexically confined
  targets, canonical-parent checks against writing through pre-existing
  symlinks.
- Sizes and counts from the wire are bounded (configurable blob/index caps)
  and validated, never clamped.
- Push (upload) is **off by default** and gated by a caller-supplied
  `PushPolicy` consulted on the offer and on every slice.

Known, documented limitations:

- Content addressing is a membership oracle: anyone who can query a store and
  guess content can confirm its presence. For private stores, use a private
  CDC gear seed (`CdcParams::with_seed`) and, at rest, the `encryption`
  feature.
- A `PushPolicy` token travels as a Zenoh attachment; protect the transport
  (Zenoh TLS/access control) if tokens are secret.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to **p13marc@gmail.com**
rather than opening a public issue. You should receive a response within a
week. Coordinated disclosure is appreciated; fixes are released as soon as
practical.
