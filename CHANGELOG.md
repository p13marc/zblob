# Changelog

All notable changes to `zblob` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer.

## [0.2.0] — 2026-07-28 "wire v2"

A ground-up redesign of the wire protocol and integrity model
([analysis](docs/analysis-2026-07.md), epic #37). **Breaking throughout** —
v1 and v2 peers do not interoperate (v2 renamed the reply keys so mixed
deployments fail closed instead of corrupting).

### Integrity & security

- **BLAKE3 + bao verified streaming** replaces whole-blob SHA-256 (#22): every
  Tier-1 reply is a self-verifying *bao slice* checked against the manifest's
  root *before* touching disk, out of order, at 16 KiB granularity. There is no
  end-of-transfer hash pass, and a tampered reply is dropped alone — the
  partial download is never deleted (#8).
- **Root pinning** (#14): `DownloadRequest::pinned(id, root)` (both tiers)
  rejects substituted content before anything is written. Unpinned fetches are
  explicit trust-on-first-use.
- **Path traversal is dead** (#5): the client chooses every destination
  (`download_to`); the server's filename is advisory. Tree entries are
  sanitized, symlinks materialize last with confined targets, and file parents
  are canonicalized back under the destination root.
- **Untrusted sizes are bounded** (#7, #11): manifests and indices are
  validated (never clamped) — schema version, chunk-size rules, blob/index
  size caps, id shape, file-size↔chunk consistency, root recomputation.

### Wire format

- **postcard everywhere** (#24): one wire encoding (no per-endpoint
  JSON/CBOR `Format` to mismatch), explicit schema-version-first fields,
  Zenoh `Encoding` tags on every reply (`zblob/manifest;v=2`, `zblob/bao4;v=2`,
  `zblob/index;v=2`, `zblob/chunk`).
- **Range-set resume** (#23): `?from=K` → `?v=2&ranges=0-5,9,12-20`. Clients
  re-query exactly their bitfield's holes; servers validate and cap range
  requests. Resume state is a compact bitfield sidecar written atomically and
  batched (#9).
- Tier-2 `root_hash` is a canonical versioned postcard digest with `mtime`
  excluded (#15) — byte-identical trees hash identically, and mtimes are
  restored on materialization.

### Reliability & performance

- `ConsolidationMode::None` on every streaming GET (#4) — v1's default
  buffered the entire blob in client memory before the first byte hit disk.
- Explicit query timeouts + a resume-retry loop with backoff (#6) — v1
  transfers silently truncated at Zenoh's 10 s default.
- Concurrent Tier-2 chunk fetch (#10); streaming `build_tree` (#16);
  per-query tasks + inflight semaphore on `TreeServer` (#12); `DirStore`
  fanout + atomic fsynced puts + optional verify-on-read + `scrub()` (#13);
  `publish_snapshot` read-back settle phase (#20).

### New capabilities

- **Push/upload** (#28): verified resumable uploads over the same queryable,
  spooled server-side, gated by a `PushPolicy` hook (off by default).
- **Multi-source** (#30): `…/have` availability bitfields per responder;
  replicated servers cooperate on one download; same-destination downloads
  single-flight.
- **Fanout tier** (#31, `fanout` feature): one-to-many rollout over zenoh-ext
  `AdvancedPublisher` with cached replay for late joiners.
- **Local seeding** (#29): `seed::seed_store` satisfies chunks from prior
  local copies and synthesized zero regions before touching the network.
- **Store lifecycle** (#26): `ContentStore::remove`, persistent snapshot tags,
  in-flight temp tags, `gc::sweep` mark-and-sweep.
- **Compression** (#27, `zstd` feature): self-describing per-chunk containers
  (wire + at rest), raw bail-out for incompressible data.
- **Encryption at rest** (#33, `encryption` feature):
  `DirStore::with_encryption` — per-chunk XChaCha20-Poly1305, convergent per
  store key, address-bound AAD.
- **Observability** (#32): `TransferStats` from every download, server
  `on_error` callbacks, optional `tracing` feature.

### Content-addressed snapshots

- `TreeIndex::keyed_by_root()` re-keys a snapshot by its own root hash, and
  `DownloadRequest::by_root(root)` fetches one with the root as both key and
  pin — so trust-on-first-use is not expressible for content-addressed trees
  and a router storage's last-writer-wins reconciliation cannot lose
  anything. Human snapshot names remain supported, with the documented
  consequence that such a key means whatever its last writer said; the
  recommended shape is a mutable name record pointing at an immutable root.
  (The id is not part of the root digest, so re-keying never alters
  identity.) This is the shape zenkey RFC 07 §2.3 now requires.

### Keyspace-convention alignment

Checked against zenkey RFC 07 (`@blob`), which names zblob as its reference
client — three gaps closed:

- **Bulk transfers now yield.** RFC 07 §2 makes QoS a *caller* obligation
  (Zenoh replies inherit the querier's QoS; server-side reply-QoS setters are
  no-ops), and zblob set no priority at all — every transfer competed with
  telemetry in the default `Data` lane. All queries and publications now
  default to `Priority::DataLow`, tunable via `BlobClientBuilder::priority` /
  `TreeClientBuilder::priority`; `Priority` is re-exported.
- **Wildcard-origin probes work.** `parse_id` matched the prefix by literal
  string stripping, so a server could never answer a `v1/*/@blob/...` query —
  the multi-holder probe RFC 07 §2 explicitly sanctions was unimplementable.
  It now matches positionally, accepting single-segment wildcards. `**` is
  refused for both roles, because a blob id is resolved by position and
  nothing could answer past an unbounded span.
- **Prefix validation is role-aware**: clients may query wildcard prefixes
  (probing), servers and publishers may not (they would answer for, or write
  to, keys they do not own).

### Test methodology

The scenario suite did not find the defects below — a code audit did. The
suite now has three layers (see CLAUDE.md): property tests over generated
inputs for the range grammar, chunk geometry, resume bitfield, CDC, and the
bao verification core; a hostile-peer harness that sweeps reply mutations
against the oracle "succeed with exactly the right bytes or fail cleanly";
and a `ContentStore` contract run against every configuration. Suites assert
their own discriminating power so a broken harness cannot pass vacuously.
The contract suite immediately found a residual bug in one of the audit
fixes (`DirStore::has` still claimed chunks sealed under a *wrong* key).

### Hardening from the post-implementation audit

- Push protocol: an offer can no longer hijack an already-registered id
  (different content refused, identical content acked idempotently), the
  offer key's id must match the manifest's, concurrent pushes are capped and
  idle ones evicted with their spool files, sidecar saves are batched, and the
  `pushes` lock is no longer held across I/O or replies.
- Uploaders validate the server's "wanted ranges" reply (sorted, disjoint,
  in-bounds) before any arithmetic — a hostile responder could otherwise
  drive a `u32` underflow.
- `TreeIndex::validate` bounds every `ChunkRef::len` by the declared CDC
  maximum, closing an unbounded-allocation path through `seed::seed_store`.
- Directory creation during materialization refuses to traverse pre-existing
  symlinks (prevention, not just post-hoc detection), and hardlink *targets*
  are canonicalized under the destination root.
- One bad reply no longer denies a fetch: malformed/invalid/mismatched
  manifest, index, and availability replies are skipped so an honest replica
  can still answer (root pinning is applied per reply).
- `DirStore::has` no longer claims chunks it cannot decode (a sealed store
  opened without its key previously wedged downloads permanently); presence
  checks moved off the async worker thread.
- Fanout: publisher uses `CongestionControl::Block` + `publisher_detection`,
  subscriber raises the history-replay query timeout, slices arriving before
  the manifest are buffered instead of dropped, and receives honor an
  overwrite policy and clean up their partial on failure.
- `Manifest`/tag ids reject `\\` and are length-bounded (Windows spool/tag
  traversal); resume bitfield counting masks padding bits.
- Ids may no longer begin with `@`, and key prefixes are validated (non-empty,
  no wildcards, a real Zenoh key expression). Zenoh's `**` does not match
  verbatim (`@`-leading) segments, so such an id registered successfully and
  was then unservable forever — a silent total failure with nothing in any log
  to explain it. Verbatim segments inside a *prefix* stay legal, which is what
  a keyspace convention needs.

### Filesystem fidelity

- Non-regular files and non-UTF-8 names are loud errors, hard links round-trip
  as hard links, deep trees no longer overflow the stack (#17); Windows
  symlink materialization errors instead of silently omitting entries (#18).

### API

- Builders for `BlobServer`/`BlobClient`/`TreeClient`; `BlobSpec`;
  public `fetch_manifest`; `download_to` with an `Overwrite` policy and
  `download_to_writer` for arbitrary seekable writers;
  `spawn()` → `ServerHandle::shutdown()`; `#[non_exhaustive] Progress` with
  `Started`/`Resumed`/`Cancelled` and byte counters (#19, #21).
- FastCDC v2020 Level-2 defaults (16/64/256 KiB) with a **seedable gear
  table** recorded in the index (#25); the ambiguous v1 `Chunker` trait is
  gone — Tier 1 uses `TransferChunks`, Tier 2 uses `CdcParams`.
- Removed: the nominal `Digest` trait, `Sha256Digest`, `Format`,
  `Manifest::compute`, `chunk_key`/`download_selector`/`parse_from`.
  Old `sha256/…` Tier-2 store keys coexist untouched; republish snapshots to
  migrate (chunk boundaries change with the new CDC defaults anyway).

## [0.1.0] — 2026-07

Initial release, graduated from the ZenSight monorepo (formerly the in-tree
`zenoh-blob` crate): Tier-1 single-blob transfer with SHA-256 + `?from=K`
resume, Tier-2 casync-style content-addressed directory trees, JSON/CBOR
control messages, serverless publishing into Zenoh storages.
