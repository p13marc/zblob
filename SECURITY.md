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
  path/symlink sanitization, duplicate-path rejection, size↔chunk
  consistency, root recomputation) before any chunk is fetched. Symlink
  confinement is decided over the **whole index**, following links the index
  itself declares, because a chain of links passes any per-link lexical
  check and still escapes.
- Materialization is defensive: relative `Normal`-component paths only,
  symlinks last, canonical-parent checks against writing through pre-existing
  symlinks, and chunks re-hashed as they leave the `ContentStore` — so a store
  that corrupts them cannot produce a "verified" tree.
- Sizes and counts from the wire are bounded (configurable blob, index, tree
  and chunk-count caps) and validated, never clamped.
- Push (upload) is **off by default** and gated by a caller-supplied
  `PushPolicy` consulted on the offer and on every slice.

### What materialization does to the destination

`download_tree` writes **in place** (the rsync/casync model), so it modifies a
directory the caller may already care about. Two operations an index can ask
for are destructive enough to require an explicit opt-in through
`MaterializePolicy`, and are refused by default:

- **Replacing an existing directory** with a file, symlink or hard link, which
  deletes that directory and everything under it
  (`MaterializePolicy::replace_directories`).
- **Restoring setuid/setgid/sticky bits** from the index. Modes are otherwise
  masked to `0o0777`, since a privileged extraction of an index with
  attacker-chosen content would otherwise yield a setuid-root binary
  (`MaterializePolicy::restore_setid`). tar and rsync gate this the same way.

Materialization is also **not atomic**: an error partway through leaves a mix
of old and new entries. Re-run to converge, or materialize into a fresh
directory and swap it in.

### Read-side authorization: there is none, by design

`PushPolicy` gates **writes**. Nothing in this crate gates **reads**: any peer
that can reach the session can fetch any registered blob, any tier-2 chunk and
any tree index. That is deliberate — read authorization belongs to the
transport, where it can be enforced once for every plane rather than
re-implemented per crate. Deployments should use Zenoh access control (in the
fleet this crate was built for, the `host-serve` / `host-blob-seed` profiles of
zenkey RFC 09 §3).

Two consequences worth stating plainly:

- A router-hosted Zenoh storage answers any GET in its key range and accepts
  any PUT. Pin roots regardless (`DownloadRequest::pinned` /
  `DownloadRequest::by_root`): a pinned fetch cannot be served substituted
  content even by a hostile storage.
- Chunk *hashes* are visible to anyone who can read a key or an index.

Known, documented limitations:

- Content addressing is a membership oracle: anyone who can query a store and
  guess content can confirm its presence. For private stores, use a private
  CDC gear seed (`CdcParams::with_seed`) and, at rest, the `encryption`
  feature. This is not hypothetical — extracting a service's chunking
  parameters and exploiting them is [demonstrated in the
  literature](https://arxiv.org/abs/2504.02095).
- A `PushPolicy` token travels as a Zenoh attachment; protect the transport
  (Zenoh TLS/access control) if tokens are secret.
- The `fanout` tier (feature-gated, off by default) buffers unverified frames
  from an unauthenticated publisher before a manifest arrives. Treat it as
  experimental and do not enable it on an untrusted bus.
- The `#[cfg(windows)]` paths are **not exercised by CI** (the runner is
  Linux-only). Windows-specific path handling — reserved device names,
  alternate data streams — is not separately validated.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to **p13marc@gmail.com**
rather than opening a public issue. You should receive a response within a
week. Coordinated disclosure is appreciated; fixes are released as soon as
practical.
