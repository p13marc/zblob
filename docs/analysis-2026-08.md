# zblob — Pre-Release Analysis & Redesign Report (0.3 and beyond)

*2026-08-12*

---

## Context

zblob 0.2 (wire v2) shipped 2026-07-29 and is consumed by zensight, tcgui, and
zenkey (`zenkey-fleet`/`zenctl`/`zengui`), all on default features — which for
this crate is the *empty* set, so `zstd`, `tracing`, `fanout` and `encryption`
are not compiled anywhere in the fleet. The trigger
for this report is **zblob#39** — a public, verifying single-chunk fetch for
`store/<algo>/<hash>` — filed from zenkey's Explorer Suite work (**zenkey#111**),
where tier-2 keys are today *listable* and *addressable* but neither probeable
nor fetchable from outside the crate. The question asked was broader: before the
next release, what else is worth doing, with backward compatibility and even the
zenkey RFC 07 contract on the table.

Three inputs feed this report:

1. a fresh read of the 0.2 codebase (all of `src/`, 13k lines);
2. the RFC 07/08 contract and a survey of how the three consumers actually use
   (and fail to use) zblob today;
3. external research: iroh-blobs/bao, FastCDC and successors, casync/restic/borg
   tree formats, encryption-at-rest patterns, Zenoh 1.8/1.9 features, BitTorrent
   v2 / erasure-coded multi-source.

**Headline:** 0.2's core is *aligned with the state of the art* — BLAKE3 + bao
verified streaming with 16 KiB groups is exactly iroh-blobs' design; the
bitfield resume, skip-bad-replies, validate-never-clamp, and canonical
mtime-free tree root are all the right calls and should survive any redesign.
The gaps are not in the integrity core. They are (a) a **public API surface too
small for its own ecosystem** — consumers hand-decode `wire::` internals and
re-derive `chunk_count`; (b) **tier 2's per-chunk GET scaling wall** and its
missing probe; and (c) a set of **implemented-but-unconsumed subsystems**
(availability, push, fanout, seeding, publish) that should each either become
real or leave the wire.

Recommendation in one line: ship an **0.3 that is API-additive** (closes #39
and everything the explorers need, no wire change), then a **wire v3 + RFC
v1.9** release that unifies the two tiers around batched, verified range
fetches and makes probe-then-fetch total across all three tiers.

> **Superseded, 2026-08-12.** An independent review of this report and of the
> issues filed from it (#39–#55) revised three of its conclusions and found
> twenty defects it had missed. The corrections are inline below, marked
> **[rev]**; the revised plan collapses 0.3 and 0.4 into a single breaking
> **0.3.0 = wire v3**. Two of the corrections matter enough to state here:
> §4.1's batched-fetch key shape **does not work as written** (see §4.1), and
> the RFC amendments are **v1.17**, not v1.9 — the zenkey RFC set was already
> at v1.16 when this was written.

---

## 1. What consumers actually taught us (0.2 in the field)

Evidence from zensight/tcgui/zenkey source, strongest first:

1. **Tier 2 is a read-only dead end outside the crate.**
   `zenkey-fleet/src/blob/bus.rs:255-266` refuses `store/…` and `tree/…`
   targets with a typed apology; §2.4's container framing is `pub(crate)`
   (`compress.rs`), so even a hand-rolled GET on a concrete store key cannot be
   decoded correctly downstream. This is #39, and it blocks zenkey#111.
2. **The probe machinery is re-implemented outside zblob.** `bus.rs:207-238`
   hand-decodes `Availability`/`Manifest` via `zblob::wire::decode` +
   `ENC_AVAIL`/`ENC_MANIFEST` and hand-computes
   `chunk_count = total_len.div_ceil(chunk_size)`; zensight duplicates the same
   `div_ceil` at `zensight/src/app.rs:3893`. There is no `probe()` and no
   `Manifest::chunk_count()`.
3. **The server-side tier-2 store is a RAM ceiling.** zensight's
   `ArtifactChannel` uses `MemoryStore`, supports exactly one live tree, and
   must `store.clear()` before building the next
   (`zensight-sensor-core/src/artifact.rs:164,586-592`). `DirStore` exists and
   is used by nobody. A sensor restart loses every chunk.
4. **Dead subsystems.** Grepped across all three consumer repos, *nobody* calls:
   `publish_chunk`/`publish_index`/`publish_snapshot`/`publish_store`, the
   `seed` module, the `gc` module, `DirStore`, `RetryPolicy`, the entire
   `fanout` module, or the whole `push/**` upload protocol. The router-store
   storages are configured in zensight (`configs/router-blob-storage.json5`)
   and never written to. Every registry `[[blob]]` entry explicitly excludes
   `push` and `fanout`.
5. **The sha256→blake3 cut orphaned every cached chunk** — zensight's redb
   store notes pre-0.11 chunks "name a different address space and are simply
   cold" (`zensight/src/store.rs:599-604`). The RFC's `<algo>` segment exists
   *for* migration, but zenkey codegen still treats a second concurrent algo as
   a build diagnostic (`zenkey/rfcs/00-index.md:283`). Next time we change
   anything address-relevant, the migration story must be designed, not
   suffered.
6. **Consumers re-implement destination staging** ("stage under the id, keep
   `Manifest::filename` advisory until save-as") — `zensight/src/app.rs:3842`,
   `artifact_fetch.rs:493-496` — and string-check prefixes for wildcards
   (`artifact_fetch.rs:583-587`) because zblob's prefixes are plain `String`s
   while zenkey already proved the typed-prefix pattern (`BlobProbePrefix`).
7. **Undocumented behavior is being leaned on structurally.** netring runs a
   second `BlobServer` on the same `@blob/artifact` prefix and relies on "a
   blob server ignores ids it doesn't own" (`netring/src/disk.rs:16-18`) —
   true, but a coincidence of implementation, promised nowhere.

---

## 2. What the state of the art says

Full survey notes are in the appendix; the conclusions that matter:

- **iroh-blobs** ([DESIGN.md](https://github.com/n0-computer/iroh-blobs/blob/main/DESIGN.md))
  is the closest cousin and validates 0.2's core wholesale: BLAKE3 root as the
  sole trust anchor, bao outboard at 16 KiB chunk groups (~0.4% overhead),
  bitfield-of-verified-chunks resume, "request only missing ranges, from any
  provider". Two ideas we don't have: **`HashSeq`** (a blob whose content is a
  concatenation of 32-byte hashes — collections as plain blobs, one request
  streams the seq then every referenced blob) and the **ticket** (one compact
  string = root + format + provider hint). Both transfer well to a bus.
- **FastCDC** stays the right CDC — but **[rev]** the stated reason was wrong.
  SuperCDC / UltraCDC / VectorCDC are *not* single-digit-% improvements:
  [VectorCDC](https://arxiv.org/pdf/2508.05797) (arXiv 2508.05797, 2025-08)
  measures 8.35×–26.2× over existing vector-accelerated CDC and up to 207× over
  unaccelerated, using hashless SIMD boundary detection. The correct reason to
  stay is that **no maintained Rust implementation exists** (`seq_chunking`,
  `chunkrs`, `clast`, `mincatcdc` are all single-author preview crates;
  `cdchunking` and `gearhash` are stale since 2020) **and chunking is not the
  bottleneck — the bus is**. **[rev]** One live hazard: `fastcdc = "4"` admits
  **4.0.0**, whose floored-vs-rounded `log2` change silently moves cut points
  for any non-power-of-two `avg_size`, and which downgraded size-bound
  `assert!`s to `debug_assert!`. A consumer resolving 4.0.0 would compute a
  *different tree root for identical bytes*. Pin `4.0.1`.
  CDC on the one-shot artifact path remains wrong — that's a rolling-hash pass
  on an embedded sensor for a blob sent once. 0.2's split (fixed grid for tier
  1, seeded FastCDC for tier 2) is correct; keep it.
- **Tree formats**: restic's model — *the tree index is itself a
  content-addressed blob* — is the load-bearing idea we're missing. casync's
  boundary-less `catar` stream (can't fetch one file) and IPFS's free-parameter
  DAG shape (same file, different CIDs per encoder mood — see
  [IPIP-499](https://github.com/ipfs/specs/pull/499)) are the two cautionary
  tales; 0.2 already avoids both (entries are per-file; CDC params are pinned
  in the index).
- **Encryption at rest**: 0.2's sealed containers (XChaCha20-Poly1305, key- and
  nonce-derivation domain-separated, chunk hash as AAD, deterministic nonce for
  idempotent re-puts) are sound *for one holder's disk*. The state of the art
  for *shared* sealed stores is borg's **keyed chunk IDs**
  (`MAC(key, plaintext)` as the address) — noted in §5 as the design to reach
  for *if* sealed-on-bus is ever wanted; the RFC's current ban stays right
  until then.
- **Zenoh 1.8/1.9**: replies inherit the query's QoS since 1.8 (0.2 already
  exploits this — client-side `Priority::DataLow` default; note the 1.8
  corollary that `Reply::priority`/`congestion_control` setters are deprecated
  and have no effect, so the *query's* congestion control is now the only
  backpressure lever on the reply path). **[rev]** Declared **`Querier`s** are
  *not* a 1.9 feature — they landed in 1.1.0 (2024-12) and were stabilized in
  1.5.0 (2025-07), and `declare_querier` is not `unstable`-gated. What 1.9
  actually contributes to bulk transfer is **per-priority QUIC streams**, which
  removes head-of-line blocking between priorities and so strengthens the case
  for `DataLow` on bulk traffic. **[rev]** A caution before adopting a
  `Querier` here: the *drop-with-pending-query* deadlock fix
  ([PR #2635](https://github.com/eclipse-zenoh/zenoh/pull/2635)) is on `main`
  and **not in 1.9.0**. **[rev]** Also newly relevant and already available to
  this crate (it enables `zenoh/unstable`): query
  `CancellationToken` (since 1.7.0), which can abort a want-list GET the moment
  another holder empties the set, and `Querier::matching_status()`. Wire
  batches are 64 KiB
  and larger messages fragment hop-by-hop, so one lost fragment costs the whole
  message — an argument for **keeping transfer chunks in the 64–512 KiB band**,
  not multi-MB. `zenoh-plugin-storage-manager` backends (RocksDB/S3) remain the
  free store-and-forward tier the keyspace was designed for.
- **Multi-source**: the mainstream answer is *stripe chunks across known
  holders; duplicate requests only for tail latency* (BitTorrent endgame).
  Erasure coding (RaptorQ) solves feedback-free multicast, which we don't have;
  reject it.

---

## 3. Release 0.3 — API-additive, no wire change

Everything in this section is `cargo semver`-minor and unblocks the explorers.
This is the release to make *soon*.

### 3.1 Close #39, but as a family, not a method

The issue asks for `TreeClient::fetch_chunk(hash)`. Do that — and treat it as
one corner of the actual gap, which is "the content-addressed tiers have no
public read surface". Ship together:

```rust
impl TreeClient {
    /// GET one content-addressed chunk, verified against `hash`.
    /// Bad or mismatching replies are skipped; first good replier wins.
    pub async fn fetch_chunk(&self, hash: &Hash) -> Result<Vec<u8>, BlobError>;

    /// GET a tree index by its root and validate it (§2.3 self-anchoring:
    /// recomputed root must equal `root`). No ContentStore needed.
    pub async fn fetch_index(&self, root: &Hash) -> Result<TreeIndex, BlobError>;
}
```

`fetch_chunk` is `fetch_one_chunk` (`src/tree.rs:1080`) minus the store put.
`fetch_index` already exists internally as the index-fetch half of
`download_tree`; exposing it resolves zenkey#111's second question ("is `tree`
fetch worth an on-disk store?") with *no store at all* — `zenctl blob fetch
tree/<root>` can print the validated index without materializing anything.
`TAG_SEALED` errors like any store read without a key.

Also decide the standalone-caller shape now: a caller holding a bare tier-2
address and *no tree prefix* shouldn't have to construct a `TreeClient` with a
dummy tree prefix. Either a free function
`fetch_chunk(session, store_prefix, hash)` or a `StoreClient` with just the
store prefix. Recommendation: **`StoreClient`** — §3.3's probe and the v3 batch
fetch will both want a home, and a third client type keyed to the third key
family matches the tier structure.

### 3.2 A first-class `probe()` and the missing accessors

```rust
pub struct BlobProbe {
    pub manifest: Manifest,
    pub availability: Option<Availability>,
}

impl BlobClient {
    pub async fn probe(&self, id: &str) -> Result<Vec<(OwnedKeyExpr, BlobProbe)>, BlobError>;
}

impl Manifest {
    pub fn chunk_count(&self) -> u32;   // the div_ceil consumers keep rewriting
}
impl Availability {
    // is_set/count are already public; keep them, document them for consumers
}
```

This deletes `bus.rs:207-238`'s hand-decoding and the duplicated `div_ceil`s.
Returning the reply key alongside the probe lets a fan-out probe attribute
holders — which is exactly what zengui's holder-picker state machine does by
hand today.

### 3.3 Typed prefixes

Replace the four `String` prefix parameters with two newtypes:

```rust
pub struct ServePrefix(OwnedKeyExpr);   // concrete only; wildcards refused at construction
pub struct QueryPrefix(OwnedKeyExpr);   // single-segment wildcards allowed; `**` refused
```

Servers/publishers take `ServePrefix`; clients take `QueryPrefix`;
`QueryPrefix::from(ServePrefix)` is free. This moves `paths.rs`'s role-aware
validation from call-time errors to construction-time types, and lets zensight
delete its `blob_prefix.contains('*')` string check and its source-grepping
guard test (`zensight-common/src/keyexpr.rs:608-640`). zenkey's
`BlobProbePrefix` proved the pattern; align with it so the two type systems
compose.

### 3.4 Make the durable server store the default story

- Document `DirStore` as *the* server-side store for `TreeServer`; demote
  `MemoryStore` to tests/examples in the docs.
- Add multi-snapshot support to the pairing: `TreeServer` already registers by
  id; what's missing is a documented pattern (and a `gc`-integrated example)
  for "N live snapshots share one `DirStore`, sweep on unregister". The pieces
  (`SnapshotTags`, mark-and-sweep) all exist unused in `src/gc.rs` — wire them
  into the flagship example so zensight's `ArtifactChannel` can adopt them.
- While there: make `SnapshotTags` store roots, not whole `TreeIndex`es
  (`gc.rs:104-112` decodes every tagged index per sweep).

### 3.5 Small correctness fixes that don't touch the wire

Straight from the code review, all invisible to peers:

| Fix | Where | Why |
|---|---|---|
| Check `Overwrite::Refuse` *before* transferring | `client.rs:690-692` | a full download is spent before the refusal today |
| Positional tier-2 key parsing (like `parse_id`) | `tree.rs:719,737` | wildcard tier-2 prefixes currently validate client-side and then can never be answered — silent total failure, the exact bug class `parse_id` fixed for tier 1 |
| Revalidate registered sources (size+mtime at serve time) | `server.rs:330-364` | a mutated backing file today yields an eternal rejected-slice loop with no diagnosis |
| Reply to a push offer on error | `server.rs:654` | `manifest.chunks()?` propagates without any reply; uploader waits out the full timeout |
| Refuse or single-serve wildcard push prefixes | `client.rs:401,442-455` | first-reply-wins against multiple spooling servers is under-specified |
| Zeroize `StoreKey` | `crypt.rs:36-37` | key material is `Clone + pub [u8;32]` with no scrubbing; add `zeroize` |
| Error (or explicitly document) silent re-registration | `server.rs:413-421` | replacing an id's content without ceremony contradicts the push path's own hijack defenses |
| Document the two-servers-one-prefix behavior | `server.rs` docs | netring depends on it (`disk.rs:16-18`); promise it or break it, don't leave it a coincidence |
| Rename `BlobError::HashMismatch`'s one misuse | `tree.rs:1155` | it's a length mismatch |
| `SECURITY.md`: state the read-side authz story | — | "delegated to Zenoh ACL (RFC 09 §3 profiles)" is fine, but say it |

### 3.6 A staging helper

`download_to` keeps caller-chooses-destination (non-negotiable), but add the
convenience every GUI rebuilds:

```rust
impl BlobClient {
    /// Download into `dir` staged under the artifact id; returns the staged
    /// path and the advisory filename (if any) for a later rename/save-as.
    pub async fn download_staged(&self, req: DownloadRequest, dir: &Path, ...)
        -> Result<Staged, BlobError>;
}
```

---

## 4. Wire v3 — the breaking release (with RFC v1.9)

The v2→v3 rule stays the 0.2 rule: rename reply keys / bump every version-first
field so mixed deployments fail closed. Chunk *addresses* don't change (still
`blake3` of uncompressed content), so — unlike the sha256 cut — **existing
stores and router storages stay warm across v3**. That's worth stating in the
changelog before anything else.

### 4.1 Batched tier-2 fetch: the scalability fix

Today tier 2 issues **one Zenoh GET per chunk** (`tree.rs:1029-1043`,
concurrency 16). A 100k-chunk tree is 100k queries; this is the crate's biggest
wall, and the July report's unimplemented H6. v3 design:

```
batch GET  <store_prefix>/<algo>/batch?v=3      payload: postcard WantList { version, hashes: Vec<Hash> }  (cap ~256)
replies    <store_prefix>/<algo>/<hex>          one container frame per hash the holder has, existing framing
```

> **[rev] As written above, this does not work.** `…/<algo>/batch` and
> `…/<algo>/<hex>` are **disjoint** key expressions, and Zenoh enforces
> intersection on the *server*: under the default `ReplyKeyExpr::MatchingQuery`,
> `Query::reply()` fails with "does not intersect with query"
> (`zenoh-1.9.0/zenoh/src/api/queryable.rs:553`) — once per reply, loudly.
>
> The fix is `.accept_replies(ReplyKeyExpr::Any)` on the GET, **stable since
> Zenoh 1.8.0**, carried as the `_anyke` selector parameter and readable
> server-side via `Query::accepts_replies()`. The client must then check each
> reply key itself, which is nearly free since it verifies content anyway.
>
> The alternative floated below — "make the request key a wildcard" — must be
> **rejected outright, not decided later**: a GET on `<store_prefix>/<algo>/**`
> would make every router-hosted Zenoh storage **dump its entire content
> store** in one query, and `docs/router-storage.md` makes storages a
> first-class tier. Replying under `…/batch/<hex>` is also wrong: it breaks the
> single-chunk cacheability that is the whole point of the reply key.
>
> **[rev] A router storage never answers `batch` at all** — it serves by key,
> and no key `…/<algo>/batch` exists in it — so it stays silent. The client
> must therefore fall back to per-chunk GETs for every hash the batch round
> left unanswered. That fallback is not a nicety: it is what keeps the
> publish-then-exit tier working.

- The reply key is the ordinary store key, so replies remain individually
  verifiable, cacheable, and identical to single-chunk replies — a router
  storage that materialized them can still serve singles.
- A holder answers only the hashes it has; the client's want-set shrinks as
  verified chunks land (the same "resume = re-derive the query from the holes"
  shape as tier 1's `ranges`).
- **[rev]** Issue batches through a plain `session.get()` with
  `accept_replies(Any)`, **not** a declared `Querier` — see §2: the Querier
  drop-with-pending-query deadlock fix is not in 1.9.0. Revisit when it ships;
  the chunk-fetch loop is exactly the shape a Querier is for.
- `MAX_RANGE_SPANS`-style cap on `hashes.len()`, validated before I/O, exactly
  like `server.rs:560-570`.

This reuses the entire §2.4 container/verification story; only the request
shape is new. Expected effect: queries per tree drop by ~2 orders of magnitude,
and flaky-link resume gets cheaper (one query per retry round, not per hole).

*(A leading-`@` note: `batch` as a plain segment under `<algo>` collides with
nothing — hashes are hex — but the RFC should reserve it explicitly the way
tier 1 reserves `manifest`/`have`.)*

### 4.2 A tier-2 probe: make §2.5 total

Tier 2 deliberately has no probe because the key carries the object. v3 gives
it a *tiny* thing to ask for, closing the `not_probed` hole that zenkey#111
documents:

```
have GET   <store_prefix>/<algo>/have?v=3       payload: WantList        reply: bitfield over the list
tree probe <tree_prefix>/<root>/have?v=3        reply: { have_index: bool, chunks_present: u32, chunks_total: u32 }
```

Both replies are O(list), never O(object) — fan-out-safe under §2.5's cost
gate, so a `*`-origin probe becomes legitimate for tier 2 exactly as it is for
tier 1. `zenctl blob probe` stops returning "capability claim, honestly
labelled" and starts returning possession verdicts. RFC 07 §2.5 gains the
symmetric sentence it's missing; RFC 08's probe-prefix type extends to tier 2.

### 4.3 The tree index becomes a blob (restic's lesson)

Today `TreeIndex` is a monolithic reply (64 MiB cap, `tree.rs:766`), fetched
whole, undeduplicated, and unresumable. v3: **the index is itself
content-addressed data in the store** —

- Encode the canonical index; if it exceeds one chunk, CDC-chunk it and store
  the chunks like any file's; `tree/<root>` then serves a small *index
  descriptor* `{ version, root, index_chunks: Vec<ChunkRef>, stats }` (one
  reply, KBs).
- Unchanged subtrees across snapshots now dedup their *metadata*, not just
  their file bytes (borg chunks its own metadata stream for the same reason).
- Index fetch becomes resumable and batched via §4.1 with zero new machinery.
- This is also iroh's `HashSeq` insight in our clothes: a collection is just a
  blob whose content is references.

`root_hash` stays the mtime-free canonical digest — identity is unchanged;
only the *container* of the index moves.

**[rev] Cap and shard it, don't just move it.** restic caps each index *file*
at 8 MiB and keeps "an arbitrary number of index files containing information
on non-disjoint sets of packs" — a discipline designed around exactly the
failure this section is fixing. Replacing one unbounded monolithic reply with
one unbounded blob on a fragment-fragile transport keeps the ceiling;
`index_chunks` should carry several bounded shards. **[rev]** Use **fixed**
chunking for index bytes, not CDC: the CDC parameters live *inside* the index,
so CDC-chunking the index is circular, and the index is written once — CDC buys
nothing on a blob nobody edits in place.

### 4.4 One version marker, one extension point

- Drop the selector's `v=2` parameter; the version-first struct field and the
  `ENC_*` tags already carry it (`lib.rs:137,187-194` duplicate it today, and
  `parse_ranges` allocates a string per query to compare).
- Give `Manifest` and `TreeIndex` a trailing `ext: Vec<(u16, Vec<u8>)>`
  (length-prefixed, unknown ids skipped). postcard stays positional and fast;
  we stop paying a wire bump for every additive field. First users: `have`
  stats, cap advertisement (below).

### 4.5 Advertise server caps

`max_chunks_per_query` (512) and `max_blob_size` must match out of band today;
a server that lowers one strands existing clients with `InvalidRanges`. v3 puts
the caps in the manifest's `ext` (tier 1) and the index descriptor (tier 2);
clients clamp their batch/range sizes to the served value. Advertisement, not
negotiation — one extra field, no handshake.

### 4.6 Availability becomes real — or leaves

`Availability` is answered as `full` unconditionally (`server.rs:538`) and
consulted by nothing. Two honest options:

- **Make it real (recommended):** servers answer their actual bitfield (the
  push spool and a future partial-cache holder genuinely have partials);
  `download_to` grows a *striping* scheduler — when a probe found N holders,
  partition `missing_ranges()` across them round-robin, duplicate only the
  final stragglers (BitTorrent endgame). This also addresses the N-replica ×
  N-bandwidth amplification: ranges are addressed to one holder each instead
  of broadcast to all matching queryables.
- Or delete the endpoint and the wire struct.

Half-states ("wire shape exists, semantics decorative") are the thing 0.2's
changelog repeatedly paid to remove; don't carry another one into v3.

### 4.7 Decide the fates of push and fanout

Consumption is zero for both; each is ~500 lines and a wire surface.

- **push**: keep, it's the only upload story and tcgui's support-bundle plan
  will want it — but fix the wildcard-offer semantics (§3.5) and add the
  negative offer reply. It stays authorization-gated and off by default.
- **fanout**: the RFC names it in the §2.2 endpoint table, every registry entry
  excludes it, and parallax (the one obvious customer) went to `@media`
  instead. Recommendation: **demote it out of RFC §2.2 into an experimental
  appendix** and keep the feature flag. If it stays, v3 must fix its framing
  anomalies: `(u16, FanoutFrame)` tuples instead of version-first structs, and
  no `ENC_*` tag on samples (`fanout.rs:166-193,266-272`) — both violations of
  the crate's own wire rules.

### 4.8 Negative replies for unknown ids

`serve_one` returns `Ok(())` for an unknown id (`server.rs:531`), so `NotFound`
costs a 30 s timeout. v3 adds a tiny tagged reply
(`ENC_NACK`, `{ version, id }`) on the manifest and index endpoints only.
Fan-out probes ignore it (absence of a manifest already means "not here");
single-origin fetches convert it to an immediate `NotFound`.

### 4.9 Chunk-size default

Zenoh fragments >64 KiB messages hop-by-hop and one lost fragment drops the
whole message. 512 KiB transfer chunks are 8 fragments each — fine on LANs,
expensive on the flaky links this fleet actually has. v3: lower the *default*
to **256 KiB** (still 16 KiB-aligned, still caller-tunable in
`[64 KiB, 4 MiB]`), and say why in the doc. Not worth breaking anything for on
its own; free to do inside a wire bump.

### 4.10 Explicitly rejected

So the next reader doesn't re-litigate:

| Idea | Verdict |
|---|---|
| QUIC-style interleaved bao byte-stream (iroh's transport) | No — Zenoh's unit is the query reply; per-chunk bao slices give the same property without a streaming state machine |
| Generic merkle-DAG / IPLD layer | No — two node types (index, chunk) suffice; iroh's retreat from IPLD is the field evidence |
| casync-style boundary-less archive stream | No — forbids fetching one file from a tree |
| CDC on the tier-1 artifact path | No — a rolling-hash pass on a sensor for a blob sent once |
| SuperCDC/UltraCDC/VectorCDC | No — no maintained Rust crates, single-digit-% gains, chunking isn't the bottleneck |
| Convergent encryption | No — fleet shares keys anyway; its one benefit is moot, its confirmation-attack leaks remain |
| RaptorQ / erasure coding | No — we have request/response feedback; retrying a content-addressed chunk is strictly simpler |
| AdvancedPublisher retransmission for bulk | No — solves live-stream recovery, not idempotent bulk pull |
| Sealed frames on fleet-reachable `store` keys | Not now — RFC 07 §2.4's ban stays; if a shared sealed store is ever wanted, the design is borg-style keyed chunk IDs (`blake3::keyed_hash(fleet_key, plaintext)` as address) + per-blob content key wrapped in an encrypted manifest, which is a *new algo tag*, not a container tag |

---

## 5. RFC amendments (zenkey side) — **v1.17**, not v1.9 **[rev]**

*The zenkey RFC set was already at **v1.16** (2026-08-12) when this was written;
v1.9 shipped 2026-08-08. The amendments below are **v1.17**. They are already
decomposed as zenkey #142–#147 under epic #141 (whose title carries the same
stale number and needs retitling).*

The v3 wire changes touch RFC 07 §§2.2, 2.4, 2.5 and RFC 08's `[[blob]]` kind:

1. **Reconcile §2.2 with the actual query shape.** The table frames
   `<id>/slice/<i>` as a queryable you GET; wire v2 *requires* GETting
   `<prefix>/<id>/**?ranges=…` with `slice/<i>` as the **reply** key
   (`zblob/src/lib.rs:13-16,46-49` — a bare-`<id>` GET rejects every slice
   under `ReplyKeyExpr::MatchingQuery`). **[rev]** Not *silently*, and the rule
   is **intersection**, not equality: the failure surfaces as a server-side
   `reply()` error naming the non-intersecting key
   (`zenoh-1.9.0/zenoh/src/api/queryable.rs:553`), so a consumer that gets this
   wrong sees error logs on the *serving* origin, not a client timeout. The
   table should describe
   request keys and reply keys as separate columns. Latent today; mandatory
   the moment §2.2 is reopened for v3.
2. **Add the tier-2 probe (§4.2) to §2.5**, making probe-then-fetch total
   across tiers; extend RFC 08's probe-prefix type accordingly.
3. **Reserve `batch` and `have` as tier-2 endpoint tokens** (§4.1/§4.2) the
   way tier 1 reserves its six.
4. **§2.3 gains the index-as-blob layering** (§4.3): `tree/<root>` serves a
   descriptor; index bytes live in the store. Identity rules unchanged.
5. **Decide fanout's status in the §2.2 table** (§4.7).
6. **Unblock per-algo builders** in zenkey codegen (`00-index.md:283`) —
   the `<algo>` migration affordance is currently unusable by the generated
   surface, which is what made the sha256→blake3 cut all-or-nothing.
7. Registry: `[[blob]]` `endpoints` enum grows/shrinks with 1 and 3;
   the `encoding`-is-never-framing clause is untouched by all of this.

Untouched by everything above (deliberately): the three key shapes, §2.3's
objects/refs split, §3's wildcard/fan-out cost gate, §2.5's router-store PUT
exemption and read-back rule, RFC 09 ACL profiles.

---

## 6. Sequencing

| Release | Contents | Consumers unblocked |
|---|---|---|
| **0.3** (soon) | §3 in full: `fetch_chunk`/`fetch_index`/`StoreClient` (#39), `probe()` + `chunk_count()`, typed prefixes, DirStore-by-default + GC example, the §3.5 fix table, `download_staged` | zenkey#111 (zenctl `blob fetch` tier-2, zengui pane), zensight deletes hand-rolled probe math + wildcard string checks, sensors get durable multi-snapshot stores |
| **0.4 = wire v3** (with zenkey RFC v1.9) | §4: batched tier-2 fetch, tier-2 probe, index-as-blob, ext fields + cap advertisement, real availability + striping, nack, fanout decision, 256 KiB default | fleet-scale trees, flaky-link economics, total probe story |
| Later / as needed | SHM fast path when Zenoh's API destabilizes less; sealed-store algo tag if a threat model ever demands it; per-algo codegen migration rehearsal | — |

The 0.3/0.4 split matters: everything the explorers need is wire-compatible,
and shipping it first means the v3 design gets field feedback from *three*
working tier-2 consumers instead of zero.

> **[rev] The split was dropped.** All three consumers belong to the same
> author, so "field feedback from three working consumers" is really "the same
> person, twice" — what the split actually buys is two migrations instead of
> one. §3 and §4 ship together as a single breaking **0.3.0 = wire v3**, and
> §7 below joins them.

---

## 7. What this report missed **[rev]**

Twenty defects found by re-reading 0.2 against this report, each verified in
the source. They are not refinements of §3.5 — several are security defects in
a *published* crate, and the tier-2 materialization path (S1–S3) is the one
zensight uses in production today.

### Materialization is not safe against a hostile index

| | Defect | Where |
|---|---|---|
| **S1** | `remove_existing` calls `remove_dir_all` and runs for every `File`/`Hardlink`/`Symlink` entry, so an index entry named `Documents` **recursively deletes** `<dest>/Documents`. Materialization being in-place is documented (`tree.rs:928-935`); being *destructive* is not. | `tree.rs:1233-1241`, called at `:1148,1190,1207` |
| **S2** | `set_mode` applies index-supplied mode bits raw, **including setuid/setgid/sticky**. A privileged extraction of an attacker-chosen index yields a setuid-root binary. tar and rsync both gate this behind an explicit flag. | `tree.rs:1244-1249`, `:1165`, `:1225` |
| **S3** | Symlink confinement is purely lexical, and a symlink **chain** defeats it: `sanitize_symlink_target` counts every `Normal` component as +1 depth even when that component is a symlink created by the same index. `[Symlink{"sub/link" → ".."}, Symlink{"e" → "sub/link/../../etc/passwd"}]` passes the depth check and resolves above the root. | `paths.rs:40-69`; `tests/tree_security.rs` covers only the single-level case |
| **S10** | Tier 2 has **no end-to-end content verification** and `verify_on_read` defaults off: `reconstruct_tree` checks only `bytes.len() != c.len`, and `root_hash` covers the entry list, not chunk bytes. Tier 1's premise — every byte verified before it lands — has no tier-2 analogue. | `store.rs:107`, `tree.rs:1151-1158` |

### The publish tier does not keep its promise

| | Defect | Where |
|---|---|---|
| **S5** | `publish_chunk`/`publish_index` never set `CongestionControl::Block`, and **publications default to `Drop`** — which is precisely why `fanout.rs:152` sets it. Bulk chunk PUTs into a router storage are silently sheddable, and the read-back settle samples only ~7 keys, so a snapshot missing chunks in between still returns `Ok`. The whole point of this tier is "PUT, confirm, exit". | `publish.rs:48-55,86-91,113-126` |
| **S6** | `publish_store` iterates `store.hashes()`, so `publish_snapshot` pushes **every unrelated chunk in the local store** — including other snapshots' — into a shared router storage. It should iterate `index.needed_chunks()`. | `publish.rs:60-74` |

### Unbounded remote-driven resource use

| | Defect | Where |
|---|---|---|
| **S7** | No size bound before decode: `try_unpack`'s `TAG_RAW` arm does `rest.to_vec()` at any length (`MAX_UNPACKED` guards only the zstd branch), and `fetch_one_chunk` never compares a reply against the index's declared `ChunkRef::len` — which `validate()` already bounded. ×16 concurrent fetches. | `compress.rs:89,96-98`; `tree.rs:1100` |
| **S8** | Tier 2 has no `max_blob_size` analogue: a 64 MiB index can reference ~1.6 M `ChunkRef`s × 16 MiB `cdc.max`, all fetched and `put` with no ceiling. Into a `MemoryStore` that is straight RAM exhaustion. Tier 1 bounds this at `client.rs:96`. | `tree.rs:754-759,936-975` |
| **S14** | Both serve loops `tokio::spawn` per inbound query and acquire the in-flight permit *inside* the task, so queued tasks accumulate unbounded, each holding its `Query` — and a `push/slice` query holds a full chunk. The permit is also taken before `PushPolicy` is consulted. | `server.rs:466-472,497-500,648`; `tree.rs:660-679,687,714` |
| **S15** | `Availability::count()` sums the whole `Vec<u8>` without masking padding bits, and nothing validates `bits.len()` against `chunk_count` on decode. `ResumeState::received()` masks precisely because a corrupt bitfield must not over-report. | `wire.rs:73-75`, `client.rs:363-369` |
| **S18** | The fanout publisher retains every slice sample for the handle's lifetime (O(blob) resident), and the receiver buffers up to **256 MiB of unverified frames** from an unauthenticated publisher before any manifest arrives — which need never arrive. | `fanout.rs:88,157,316-322` |

### Correctness

| | Defect | Where |
|---|---|---|
| **S4** | **AEAD nonce reuse.** The nonce derives from `(key, chunk hash)` only, and `DirStore::put` unconditionally re-packs, re-seals and replaces. Re-`put` one chunk under a different `ChunkCompression` — or a different zstd *level* — and two distinct plaintexts are sealed under one (key, nonce): XChaCha20 keystream reuse plus Poly1305 one-time-key reuse. Fix by binding the nonce to a digest of the container, which keeps idempotent re-puts idempotent. | `crypt.rs:53-57`, `store.rs:256-285` |
| **S9** | The push client treats **any** `reply_err` from **any** responder as fatal, violating the crate's own fact 3. With two servers on one prefix — the arrangement zensight's netring relies on — one answering "push not enabled" aborts an upload the other already accepted. | `client.rs:449-453,539-544` |
| **S11** | `build_tree` never validates its own output and records symlink targets verbatim from `read_link`, so a source tree containing an escaping symlink yields a snapshot that builds, registers and publishes fine and that **every** client rejects. Same failure shape as the leading-`@` id lesson `manifest.rs:95-102` documents at length. | `tree.rs:336-356,418-424` |
| **S12** | `TransferChunks::count()` truncates (`div_ceil(...) as u32`) instead of erroring, so a large enough configured `max_blob_size` yields count 0 — and an all-zero file of the claimed length gets renamed into place as "verified". | `chunk.rs:85-87` |
| **S13** | `download_tree` never takes a `TempTag`, so a concurrent `gc::sweep` deletes chunks a running download already fetched. The protection mechanism exists, is documented, and has no caller. | `gc.rs:126-153`, `tree.rs:936-975` |
| **S16** | `Overwrite::Refuse` is checked *after* the transfer, and the `.part` is preallocated to the remote's `total_len` before any refusal. The fanout tier has the same bug **and then deletes the `.part`**, contradicting `Overwrite::Refuse`'s own documentation. | `client.rs:652-653,690`; `fanout.rs:418-428` |
| **S17** | `register_file` writes `<source>.obao4` into the caller's directory, unasked, and nothing ever removes it; push spool files are likewise never reclaimed. | `server.rs:342-348,425-427,966-972` |
| **S19** | `fastcdc = "4"` admits 4.0.0, which silently moved cut points for non-power-of-two `avg_size`. A consumer resolving it computes a **different tree root for identical bytes**. Pin `4.0.1`. | `Cargo.toml:29` |
| **S20** | No incremental snapshot path: `build_tree` re-walks and re-chunks every file every time. `seed.rs` solves this for the *consumer*; the producer — an embedded sensor snapshotting repeatedly — has nothing. restic's parent-snapshot trick is the biggest producer-side win available. | `tree.rs:336-356,394-471` |

Doc-truth items: `BlobError::HashMismatch`'s only producer is a *length* check
(`tree.rs:1155`) and its doc advertises two behaviours that do not exist;
`RootMismatch`'s doc claims nothing was written, contradicted by
`finalize_push`; `validate_id` permits `.` and control characters.

**The methodological point** is the same one 0.2 already paid for once
(`CLAUDE.md`'s Tests section): none of these came from the test suite. They came
from reading the code against its own documented invariants. S1, S2 and S3 sit
directly under `tests/tree_security.rs`, which passes.

---

## Appendix — external references

- iroh-blobs design: <https://github.com/n0-computer/iroh-blobs/blob/main/DESIGN.md>; protocol (ChunkRanges, HashSeq, GetMany/Push): <https://docs.rs/iroh-blobs/latest/iroh_blobs/protocol/index.html>; blob-store challenges: <https://www.iroh.computer/blog/blob-store-design-challenges>; BLAKE3 hazmat/outboard: <https://www.iroh.computer/blog/blake3-hazmat-api>; "A New Direction for Iroh" (IPLD retreat): <https://n0.computer/blog/a-new-direction-for-iroh/>
- FastCDC (USENIX ATC'16): <https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf>; Rust crate: <https://github.com/nlfiedler/fastcdc-rs>; VectorCDC: <https://arxiv.org/pdf/2508.05797>
- casync: <https://0pointer.net/blog/casync-a-tool-for-distributing-file-system-images.html>, LWN: <https://lwn.net/Articles/726625/>; desync: <https://github.com/folbricht/desync>; OSTree repo anatomy: <https://ostreedev.github.io/ostree/repo/>; restic repo format: <https://restic.readthedocs.io/en/latest/100_references.html>; borg internals/security: <https://borgbackup.readthedocs.io/en/stable/internals/security.html>
- IPFS UnixFS profiles (IPIP-499): <https://github.com/ipfs/specs/pull/499>
- Convergent encryption pitfalls: <https://en.wikipedia.org/wiki/Convergent_encryption>; age STREAM spec: <https://github.com/codesoap/age-spec>
- **[rev]** CDC-parameter extraction attacks (Alexeev, Percival, Zhang, 2025-04): <https://arxiv.org/abs/2504.02095> — extracts per-user chunking parameters from backup services and shows the resulting loss, *"including when these parameters are not set up at all"*. This is the evidence behind `CdcParams::with_seed`: an unseeded CDC over shared or sealed content is a demonstrated leak, not a theoretical one. It also strengthens §5's borg-style keyed-chunk-ID escape hatch.
- **[rev]** VectorCDC (SIMD boundary detection, 2025-08): <https://arxiv.org/pdf/2508.05797>; DedupBench harness: <https://github.com/UWASL/dedup-bench>
- Zenoh: 1.8 "Kiyohime" (reply QoS inheritance): <https://zenoh.io/blog/2026-03-18-zenoh-kiyohime/>; 1.9 "Longwang" (Querier everywhere): <https://zenoh.io/blog/2026-04-16-zenoh-longwang/>; storage manager: <https://zenoh.io/docs/manual/plugin-storage-manager/>; prior art zenoh-fs: <https://github.com/atolab/zenoh-fs>
- BitTorrent v2 (per-file merkle, 16 KiB blocks): <https://www.bittorrent.org/beps/bep_0052.html>
