# zblob — Pre-Release Analysis & Redesign Report (0.3 and beyond)

*2026-08-12*

---

## Context

zblob 0.2 (wire v2) shipped 2026-07-31 and is consumed by zensight, tcgui, and
zenkey (`zenkey-fleet`/`zenctl`/`zengui`), all on default features. The trigger
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
- **FastCDC** stays the right CDC (the `fastcdc` crate is mature; SuperCDC /
  UltraCDC / VectorCDC have no maintained Rust crates and single-digit-% gains).
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
  exploits this — client-side `Priority::DataLow` default); 1.9 adds declared
  **`Querier`s** (publisher-like optimization for repeated GETs to one
  keyexpr — exactly the shape of a chunk-fetch loop); wire batches are 64 KiB
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

- The reply key is the ordinary store key, so replies remain individually
  verifiable, cacheable, and identical to single-chunk replies — a router
  storage that materialized them can still serve singles.
- A holder answers only the hashes it has; the client's want-set shrinks as
  verified chunks land (the same "resume = re-derive the query from the holes"
  shape as tier 1's `ranges`).
- Issue batches through a declared **`Querier`** (Zenoh 1.9) per store prefix.
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

## 5. RFC v1.9 amendments (zenkey side)

The v3 wire changes touch RFC 07 §§2.2, 2.4, 2.5 and RFC 08's `[[blob]]` kind:

1. **Reconcile §2.2 with the actual query shape.** The table frames
   `<id>/slice/<i>` as a queryable you GET; wire v2 *requires* GETting
   `<prefix>/<id>/**?ranges=…` with `slice/<i>` as the **reply** key
   (`zblob/src/lib.rs:13-16,46-49` — a bare-`<id>` GET silently rejects every
   slice under `ReplyKeyExpr::MatchingQuery`). The table should describe
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

---

## Appendix — external references

- iroh-blobs design: <https://github.com/n0-computer/iroh-blobs/blob/main/DESIGN.md>; protocol (ChunkRanges, HashSeq, GetMany/Push): <https://docs.rs/iroh-blobs/latest/iroh_blobs/protocol/index.html>; blob-store challenges: <https://www.iroh.computer/blog/blob-store-design-challenges>; BLAKE3 hazmat/outboard: <https://www.iroh.computer/blog/blake3-hazmat-api>; "A New Direction for Iroh" (IPLD retreat): <https://n0.computer/blog/a-new-direction-for-iroh/>
- FastCDC (USENIX ATC'16): <https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf>; Rust crate: <https://github.com/nlfiedler/fastcdc-rs>; VectorCDC: <https://arxiv.org/pdf/2508.05797>
- casync: <https://0pointer.net/blog/casync-a-tool-for-distributing-file-system-images.html>, LWN: <https://lwn.net/Articles/726625/>; desync: <https://github.com/folbricht/desync>; OSTree repo anatomy: <https://ostreedev.github.io/ostree/repo/>; restic repo format: <https://restic.readthedocs.io/en/latest/100_references.html>; borg internals/security: <https://borgbackup.readthedocs.io/en/stable/internals/security.html>
- IPFS UnixFS profiles (IPIP-499): <https://github.com/ipfs/specs/pull/499>
- Convergent encryption pitfalls: <https://en.wikipedia.org/wiki/Convergent_encryption>; age STREAM spec: <https://github.com/codesoap/age-spec>
- Zenoh: 1.8 "Kiyohime" (reply QoS inheritance): <https://zenoh.io/blog/2026-03-18-zenoh-kiyohime/>; 1.9 "Longwang" (Querier everywhere): <https://zenoh.io/blog/2026-04-16-zenoh-longwang/>; storage manager: <https://zenoh.io/docs/manual/plugin-storage-manager/>; prior art zenoh-fs: <https://github.com/atolab/zenoh-fs>
- BitTorrent v2 (per-file merkle, 16 KiB blocks): <https://www.bittorrent.org/beps/bep_0052.html>
