# Changelog

All notable changes to `zblob` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer.

## [0.3.0] — unreleased "wire v3"

**Breaking throughout.** v2 and v3 peers do not interoperate: every `ENC_*`
tag is re-spelled and `WIRE_VERSION` is 3, so a mixed deployment fails closed
rather than half-decoding. Source-breaking too — prefixes are typed now.

**Chunk addresses do not change**, so existing `DirStore`s and router-hosted
storages stay warm across the upgrade. Content is still BLAKE3 of the
uncompressed bytes under the same key, with the same container framing. This
is the opposite of the sha256→blake3 cut, which orphaned every cached chunk.
See [`docs/MIGRATION-v3.md`](docs/MIGRATION-v3.md).

### Security

Four defects reachable from a hostile tree index, all on the tier-2
materialization path, none caught by the existing adversarial suite:

- **A hostile index could delete pre-existing subtrees of the destination.**
  `remove_existing` called `remove_dir_all` for any entry landing on a
  directory, so an entry named `Documents` recursively deleted
  `<dest>/Documents`. Now refused unless `MaterializePolicy::replace_directories`.
- **Index-supplied mode bits were applied raw, including setuid/setgid.** Now
  masked to `0o0777` unless `MaterializePolicy::restore_setid`, as tar and
  rsync do.
- **Symlink confinement was defeatable by a chain within one index.** The
  per-link lexical depth check cannot see that a component is itself a link the
  same index declares. Containment is now decided over the whole index, under a
  hop budget so a cycle errors rather than loops.
- **AEAD nonce reuse** (`encryption` feature, compiled by nobody): the nonce
  derived from `(key, chunk hash)`, but what is sealed is the *compression
  container*, and `put` re-packs unconditionally — so re-putting a chunk under
  a different compression setting reused the nonce. It now covers the
  container. Reading is unaffected; `StoreKey` drops `Clone` and zeroizes.

Also: `SECURITY.md` now states that **nothing gates reads** (delegated to
Zenoh access control) and what materialization does to a destination — both
previously unstated.

### Scale

- **Batched tier-2 fetch.** A snapshot cost one Zenoh query per chunk; it now
  costs one per round of `batch_size` (default 256). Replies come back on each
  chunk's own key — individually verifiable, individually cacheable — which
  requires `accept_replies(ReplyKeyExpr::Any)` because those keys are disjoint
  from the batch key. A holder answers only what it has; whatever a round
  leaves unanswered falls back to per-chunk GETs, which is what keeps
  router-hosted storages (that never answer a batch) working.
- **A tier-2 probe**, so RFC 07 §2.5's probe-then-fetch is total across all
  three key families: `…/<algo>/have` answers one bit per address asked,
  `<tree>/<id>/have` four numbers whatever the snapshot's size.
- **Large indices are sharded** into content-addressed chunks and served as an
  `IndexDescriptor`; small ones are still served whole, since an index costs
  0.05–0.10% of its payload and adding a round trip to every tree fetch would
  fix a problem the common case does not have.
- **`download_striped`** addresses each range to one holder, so a chunk crosses
  the wire once instead of once per replica.
- **`build_tree_from`** reuses a parent snapshot's chunk references for files
  whose size and mtime are unchanged (restic's trick). Read the mtime caveat.
- **The default chunk size is 256 KiB**, down from 512 KiB — chosen by
  measurement (`tests/chunk_size.rs`), not preference.

### API

- **Typed prefixes**: `ServePrefix` (concrete) and `QueryPrefix` (wildcards
  allowed, `**` never). The role rules were always right and enforced at call
  time; now they are unrepresentable otherwise.
- **`StoreClient`** — read the content-addressed store directly, with no tree.
- **`TreeClient::fetch_index_by_root`** — inspect a snapshot with no store.
- **`BlobClient::probe`** — one entry per holder, each naming its origin.
- **`Manifest::chunk_count`** — the `div_ceil` two consumers were rewriting.
- **`download_staged`** — the staging convention both GUIs reinvented.
- **`frame_chunk` / `unframe_chunk`** — the §2.4 container, decodable from
  outside the crate at last.
- **`ext` on `Manifest`** — a trailing extension list, so an additive field
  stops costing a wire break. First use: servers advertise their
  `max_chunks_per_query` and clients clamp to it, instead of being rejected
  with `InvalidRanges` and no way to discover why.
- `TransferStats::queries`; `SettleCoverage`; `MaterializePolicy`;
  `BlobSource::fingerprint`; `gc::TagRecord`.

**Pre-release API review.** 0.3.0 was built, then reviewed before tagging;
these changes are all source-breaking and all free while the release is
unpublished.

- **Transfers are call builders.** `download_to(&req, &dest, &sink, &cancel)`
  took four positional arguments of which two were `&()` and
  `&CancelToken::new()` at nearly every call site — arguments that exist to
  say "no thanks", transposable with the ones that matter without the compiler
  noticing. What a transfer cannot do without stays positional; the rest moves
  onto a `#[must_use]` builder that runs when awaited, matching
  `zenoh::Session::get`. `Download`, `StagedDownload`, `DownloadToWriter`,
  `Upload`, `TreeDownload`. `download_striped` folds into `.striped(holders)`,
  and `Overwrite` becomes per-transfer rather than per-client.
- **`BlobId`, `HashAlgo`, `Ext`** — three fields that arrived off the network
  as `String`, `String` and `Vec<(u16, Vec<u8>)>` and were checked by a
  validator somebody had to remember to call. Validation moves into
  `Deserialize`; all three are wire-transparent, so `WIRE_VERSION` does not
  move. `Ext` gains `MAX_FIELDS`/`MAX_VALUE_LEN`, which nothing bounded.
- **`BlobError` splits its catch-all.** `Protocol(String)` carried 56 of the
  crate's error sites: `UnsafePath`, `InvalidPrefix`, `MalformedMessage`,
  `Usage`, `NotSettled` and `Task` now say which, `Zenoh`/`Encode` keep their
  cause, and `kind()`/`is_retriable()`/`is_cancelled()` classify.
- **`CancelToken` is prompt.** It wraps `tokio_util`'s token and gains
  `cancelled()`/`until_cancelled()`; every receive loop now waits through it.
- **`PushConfig`** — the three push bounds were builder methods that silently
  did nothing unless called after `accept_push`.
- **`Publisher`/`SnapshotPublisher`** replace the five `publish_*` free
  functions, the widest of which took eight arguments.
- **`zblob::keys`** — the seventeen key builders and parsers move off the
  crate root. `parse_id` borrows; `parse_tier2_tail` returns a `Tier2Tail`.
- **`WireTag`** replaces the `ENC_*` `&str` constants: the old comparison
  allocated a `String` per reply and related two values nothing typed.
- **New capabilities**: `TreeClient::fetch_file` (one path out of a snapshot
  without materializing the tree), `BlobClient::upload_source` (push any
  `BlobSource` — an in-memory buffer, a generated artifact — without staging
  it in a file; detects a source whose fingerprint changed mid-upload),
  server introspection (`registered`/`manifest`/`index`/`serves`),
  `TreeIndex` navigation (`entry`/`entries`/`files`/`file_chunks`),
  `progress_channel`, `BlobClient::priority`,
  `TransferStats: Add + AddAssign + Sum`, and `bao_tree::{ReadAt, Size}`
  re-exported so `ReadAtSize` is implementable downstream at all.
- Sessions are `&zenoh::Session`, not `Arc<zenoh::Session>` (which was an
  `Arc<Arc<..>>`); `#[must_use]` on every builder method; `Debug` on the 17
  public types that lacked it; `#[non_exhaustive]` on the output structs.

### Fixed

- **The reactor was blocked in three public async paths**: `publish_hashes`
  and `publish_store` read the store inline (a file read per chunk, and a full
  recursive `read_dir`), and `TreeServer::register` sharded an index on the
  async thread — ~64 fsynced atomic renames for a 4 MB index. All three now go
  through the blocking pool, in batches rather than per chunk.
- **`ContentStore` could not report I/O failure.** `has -> bool` and
  `get -> Option<Vec<u8>>` spelled `EIO`, `EACCES` and "absent" identically,
  and the client's response to absence is to re-fetch and `put` back into the
  same broken store. All four accessors return `io::Result`; the deliberate
  heal-on-refetch policy for a *corrupt* chunk survives as an explicit
  `Ok(None)`.
- **`cancel()` was polled, never awaited.** Every check sat after a blocking
  receive, so the observed latency was the query timeout — measured at 5.00 s
  of a 5 s budget against a peer that stops answering, versus 0.17 s now.
- **A hostile fanout publisher could hold a receiver open forever**, because
  `stall_timeout` bounded the wait for a *sample* rather than for progress.
  This is the one tier with no second responder to fall back on.
- **`TransferStats::queries` reported 0** for every ordinary single-origin
  download — it was incremented on the striped and tier-2 paths only.
- **`SettleCoverage::Sample(k)` could probe `k + 1` keys**, one over its own
  documented bound.
- An unreadable or undecodable resume sidecar restarted the whole download
  silently; it now says so (restarting remains correct).
- `publish_chunk`/`publish_index` set no congestion control, and publications
  default to `Drop` — bulk PUTs into a storage were silently sheddable while
  the sampled read-back still returned `Ok`. Both block now.
- `publish_snapshot` published the whole local store, including other
  snapshots' chunks, into what is typically a fleet-wide storage.
- Chunk replies had no size bound before unframing (`MAX_UNPACKED` guarded only
  the compressed arm), and the tree path threw away the length its own index
  had already declared.
- Tier 2 had no total-size cap: an index inside the 64 MiB limit could
  reference tens of TiB.
- Tier-2 chunks are re-hashed on the way *out* of the store, so a store that
  corrupts them cannot produce a "verified" tree.
- `build_tree` never validated its own output, so a source tree with an
  absolute symlink built a snapshot every client rejects.
- `TransferChunks::count()` truncated a `u64` to `u32` — an all-zero file could
  be renamed into place as verified.
- The push client treated any responder's `reply_err` as fatal, so a co-server
  with push disabled could abort an upload another server had accepted.
- Push handlers could return without replying, leaving the uploader to wait out
  the timeout and then misdiagnose a validation failure as a missing server.
- Re-registering an id with different content is refused, matching the push
  path's hijack defence.
- A registered source that changes on disk is diagnosed rather than making
  every slice fail the client's verification forever.
- Tier-2 keys are resolved positionally, so a wildcard-origin prefix is
  answerable instead of validating and then failing silently.
- Tier-2 replies carry the *server's* key, not the query's, so a
  wildcard-origin query can attribute and cache them.
- `Overwrite::Refuse` is checked before the transfer, not after.
- `gc::TempTags` finally has a caller: `TreeClientBuilder::temp_tags`.
- In-flight permits are taken before spawning, so queued tasks are bounded.
- `Availability::count()` masks padding bits and validates its own length.
- `fanout` samples are version-first structs with an `ENC_FANOUT` tag; its
  publisher cache, receive buffer and manifest cap are bounded and configurable.
- `fastcdc` pinned to `4.0.1`: 4.0.0 silently moved cut points for
  non-power-of-two `avg_size`, which would compute a different tree root for
  identical bytes.

### Considered and rejected

- **A negative reply for unknown ids** (#53). Premise measured and false: an
  unknown id resolves in about a millisecond, not on the query timeout,
  because a Zenoh query finalizes when its matching queryables complete.
- **Partial tier-1 holders.** A bao slice's sibling hashes require the whole
  blob, so a partial holder can serve no verified slice — advertising one would
  send clients after chunks they can never obtain. Partial possession is real
  on tier 2 and is what the new probe reports.
- **A descriptor for every index.** Measurement said no; see above.

## [0.2.0] — 2026-07-29 "wire v2"

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

### Portability

- `DirStore::put` treats losing a concurrent-put race as success: the address
  determines the bytes, so whoever landed first wrote what we would have.
  POSIX `rename` replaces silently, but Windows refuses when the destination
  exists or is open elsewhere — so concurrent puts of one chunk (the normal
  case when several downloads share a store) failed **on Windows only**.
- The non-UTF-8 filename test skips where the filesystem refuses such a name
  (APFS, Windows) instead of failing: there is no subject to test.

Both were found by a Windows+macOS CI matrix that has since been removed —
the mirror runs no CI by policy (see CLAUDE.md). These paths are consequently
not covered by CI going forward.

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
