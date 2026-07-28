# zblob — Deep Analysis & Redesign Report

*2026-07-28*

---

## Context

`zblob` is a single-crate library for resumable, chunked, content-addressed blob and
directory transfer over Zenoh. It graduated from the ZenSight monorepo in 2026-07 and
is ~2,300 source lines across two transfer tiers that share pluggable primitives
(`Hash`/`Digest`, `Chunker`, `Format`, `ProgressSink`, `CancelToken`):

- **Tier 1** — single blob by id, pull-only, offset-addressed wire protocol
  (`server.rs`, `client.rs`, `manifest.rs`, `resume.rs`).
- **Tier 2** — casync-style content-addressed directory trees
  (`tree.rs`, `store.rs`, `publish.rs`).

This report assesses the design against the state of the art (iroh-blobs / BLAKE3-bao,
casync/desync, restic/borg, Zenoh 1.7–1.9 features), catalogs concrete defects found by
reading the code, and proposes a prioritized improvement and redesign roadmap. The user
has authorized breaking backward compatibility.

The crate is genuinely well-built in its core ideas — manifest-first ordering, content
addressing making resume/dedup fall out for free, honest "why" module docs, and a real
malicious-server test for Tier 1. The findings below are about hardening and extending a
sound foundation, not rescuing a broken one.

---

## 1. Executive summary

Three findings are severe enough to fix regardless of any redesign:

1. **The core "O(chunk_size) bounded memory" promise is false in practice.** Neither
   `session.get` sets a consolidation mode, so Zenoh 1.9 resolves the default
   `Auto → Latest`, which buffers *every* reply inside the client session and flushes
   them only at query finalization. The entire blob is held in memory before the first
   chunk reaches disk; `Progress::Chunk` fires in one end-of-transfer burst; and
   cancellation cannot interrupt an in-flight transfer. Verified against
   `zenoh-1.9.0/src/api/session.rs:2636,3436`. One-line fix:
   `.consolidation(ConsolidationMode::None)` (stable API, *not* gated by the `internal`
   feature the docs worry about).

2. **Remote-controlled path traversal in both tiers.** `client.rs:200`
   `dest_dir.join(&manifest.filename)` and `tree.rs:475/481/498` `dest_root.join(path)`
   use unsanitized attacker-supplied paths. Absolute paths replace the base directory;
   `../` escapes it; a `Symlink` entry emitted before a `File` under it lets the
   reconstruct loop write *through* the symlink (zip-slip). The whole-blob/root hash
   does not defend against this because the attacker chooses the hash too.

3. **No query timeout + no retry loop = large transfers are structurally guaranteed to
   fail.** Zenoh's default query timeout is 10 s and nothing sets `.timeout(...)`. The
   Tier-1 streaming GET is a single query for the entire remaining blob, so any transfer
   over ~10 s is truncated into `BlobError::Incomplete`, and `download` has no internal
   retry — the caller must re-drive it, a contract that is not documented.

Beyond these, the biggest *strategic* opportunity is adopting **BLAKE3 + bao-tree
verified streaming** to replace end-of-transfer SHA-256: it turns every partial transfer
into a provably-correct prefix, verifies at 16 KiB granularity instead of trusting a
final hash, and generalizes resume from a suffix (`?from=K`) to arbitrary chunk-range
sets — at ~0.4% outboard overhead.

---

## 2. What to keep (design strengths)

- **Manifest-first / index-first ordering** — knowing `chunk_size` up front to place
  out-of-order replies by offset is the right call given Zenoh does not order replies.
- **Content addressing in Tier 2** — "progress ≡ which hashes are on disk" gives resume,
  cross-file and cross-version dedup for free; proven by
  `tests/tree.rs:193` (`reedit_transfers_only_changed_chunks`).
- **The `<prefix>/<algo>/<hex>` chunk key layout** already carries an algorithm segment,
  so migrating SHA-256 → BLAKE3 can coexist with old stores.
- **Honest module docs** explaining Zenoh backpressure (`Block`) and reply-key matching.
- **Streaming server reads** (`BlobSource` + `read_exact` into a reused buffer) — Tier 1
  never `read_to_end`s the source.
- **The `MAX_INFLIGHT` semaphore and per-query task spawn in `BlobServer`** — a coarse
  but real anti-DoS posture (that Tier 2's `TreeServer` unfortunately lacks; see §3).

---

## 3. Code-level defects (found by reading the code)

Grouped by severity. File:line references throughout.

### Critical

- **C1 — Consolidation buffering (memory / progress / cancel).** `client.rs:137`,
  `client.rs:212`, `tree.rs:365,462`: no `.consolidation(None)`. Default resolves to
  `Latest` → replies buffered until `ResponseFinal`. Breaks the O(chunk_size) invariant
  (`lib.rs:24-26`, `README.md:29-31`), makes progress bursty, and defeats
  `cancel.is_cancelled()` at `client.rs:141`. **Fix:** set `ConsolidationMode::None`
  explicitly on every chunk/stream query.
- **C2 — Path traversal, both tiers.** `client.rs:200`; `tree.rs:475,481,498`. Absolute
  paths, `..`, and symlink-then-write all escape the destination. **Fix:** reject
  absolute paths and any non-`Normal` component; materialize symlinks last (or resolve
  and re-check each write stays under a canonicalized root); open with `O_NOFOLLOW`;
  reject symlink targets that escape root; clear existing dirs before `symlink`.
- **C3 — No query timeout + no retry.** No `.timeout()` anywhere; default 10 s
  (`zenoh-config-1.9.0`). Single-query full-blob stream → `Incomplete` on any slow
  transfer with no auto-retry. **Fix:** set an explicit timeout, add a
  resume-and-retry loop with backoff, and document the "call until Ok" contract (or make
  `download` do it internally).

### High

- **H1 — Server panic via crafted `Manifest`.** `server.rs:182-190`: `buf` is sized from
  the raw (unclamped) `chunk_size`, but `chunk_len` uses the clamped chunker → slice
  panic when `chunk_size < 256 KiB`. `Manifest` fields are all public and `register`
  validates nothing. **Fix:** validate manifest on `register`; size buffer from the
  clamped value; return an error, not a panic.
- **H2 — Silent clamping livelock.** `ChunkSize::new` clamps (`chunk.rs:23-25`) instead
  of erroring; a wire manifest with `chunk_size` outside [256 KiB, 1 MiB] makes peers
  agree with each other but disagree with `chunk_count`/`total_len`, failing the final
  hash forever. **Fix:** validate wire-carried sizes; error on out-of-range.
- **H3 — No per-chunk integrity in Tier 1.** Only a whole-blob SHA-256
  (`manifest.rs:31`); one bad byte in a 50 GB download deletes the entire `.part`
  (`client.rs:195-197`) and restarts. Tier 2 does this right (`tree.rs:428-432`).
  **Fix:** per-chunk hashes in the manifest, or move to bao verified streaming (§5).
- **H4 — O(n²) sidecar I/O.** `client.rs:169` calls `state.save` on every chunk;
  `resume.rs:84-88` rewrites the whole `Vec<bool>` as JSON each time. 100 GB blob →
  ~240 GB of sidecar writes. **Fix:** bitset + batched/periodic + atomic (tmp+rename)
  persistence.
- **H5 — No crash-safe persistence.** `resume.rs:86` truncate-writes the sidecar (a
  crash mid-write → `load` returns `None` → full re-download); no `fsync`/`sync_all`
  anywhere; `file.flush()` flushes tokio's buffer, not the OS. `DirStore::put` does
  tmp+rename but doesn't fsync the file or dir. **Fix:** atomic sidecar writes,
  `sync_data` at commit points, fsync the store dir after rename.
- **H6 — Tier-2 fetch is strictly sequential.** `tree.rs:421-441` is a serial
  `for hash in needed` loop, one full GET round-trip each. A 10 GB tree at 256 KiB is
  ~40k serial RTTs (~200 s of pure latency at 5 ms RTT). **Fix:** `buffer_unordered`
  concurrency, and/or a multi-hash range selector answered with N replies.
- **H7 — Untrusted-input allocation.** `resume.rs:48` `vec![false; chunk_count]` with a
  remote `chunk_count` (u32::MAX → 4.3 GB); `client.rs:117` `set_len(total_len)` with a
  remote u64; unbounded manifest/index `decode`. **Fix:** validate
  `chunk_count == chunker.count(total_len)`; cap payload/recursion; sanity-bound sizes.
- **H8 — `TreeServer` robustness gap vs `BlobServer`.** Serves queries *inline* in the
  `select!` loop (`tree.rs:305-326`) — one slow reply stalls all clients and both
  queryables; no inflight semaphore; errors swallowed via `let _ = query.reply(...)`.
  **Fix:** spawn per query, add a semaphore, surface errors.
- **H9 — `DirStore` temp-file race + no read verification.** `store.rs:93` uses a fixed
  `{hash}.tmp` name → concurrent puts of the same chunk corrupt each other;
  `DirStore::has`/`get` (`store.rs:85-89`) trust file presence and return bytes
  unverified — a corrupted local chunk yields a corrupted output tree with a "success"
  return, because `root_hash` is over `entries`, not content. **Fix:** unique temp
  names (`tempfile`), fanout dirs, optional verify-on-read / scrub.

### Medium

- **M1 — `root_hash` is not an authenticity anchor.** `tree.rs:125-127`: recomputes and
  compares, but both sides come from the attacker-controlled payload. Detects only
  transport corruption (already covered by Zenoh). No signature, no out-of-band root
  pinning, no `expected_root` parameter on `download_tree`. **Fix:** accept a pinned
  expected root; optionally sign indices/manifests.
- **M2 — mtime recorded but never restored, yet included in `root_hash`.**
  `tree.rs:131-138` hashes entries including `mtime`, so byte-identical trees copied at
  different times get different roots (breaks the content-addressing claim), while
  `reconstruct` never applies mtime. **Fix:** exclude mtime from the identity hash;
  restore mtime on materialization if preservation is desired.
- **M3 — Tier-2 "bounded memory" is false.** `tree.rs:222` `fs::read` whole files;
  `build_tree` returns `Vec<(Hash, Vec<u8>)>` (entire deduped tree resident,
  `tree.rs:142,155`); `publish_snapshot` takes it by value. **Fix:** stream files
  through a chunker; make `build_tree`/publish operate on an iterator/callback.
- **M4 — Blocking sync I/O in async.** `ContentStore` is sync (`store.rs:14-21`) but
  `DirStore` does `std::fs` and is called from async at `tree.rs:433,489` and inside
  `TreeServer::run`'s `select!` (`tree.rs:310-312`). **Fix:** async store trait or
  `spawn_blocking`.
- **M5 — Non-regular files silently dropped; hardlinks lost; non-UTF-8 mangled.**
  `walk` (`tree.rs:211-245`) has no `else` arm (FIFOs/sockets/devices vanish);
  hardlinks become independent files; `to_string_lossy()` (`tree.rs:175,212`) corrupts
  non-UTF-8 names and can collide two files onto one path. **Fix:** error on unsupported
  types (or handle explicitly); store raw bytes for names; detect hardlinks.
- **M6 — Windows: symlink is a silent `Ok(())` no-op** (`tree.rs:523-526`); `mode`
  dropped; `rel_path` unconditionally `replace('\\','/')` corrupts unix filenames
  containing backslashes (`tree.rs:176`). A "successful" download silently omits
  symlinks. **Fix:** error or documented policy; guard the backslash replace to
  `cfg(windows)`.
- **M7 — Panics/unwraps.** `store.rs` `.lock().unwrap()` (mutex poisoning cascades);
  `tree.rs:135` `unwrap_or_default()` makes a serialization failure hash `SHA-256("")`
  as the root; `lib.rs:121` `parse().unwrap_or(0)` silently restarts a transfer from 0.
  **Fix:** propagate errors; use a poison-tolerant lock or document.
- **M8 — Unbounded recursion in `walk`** (`tree.rs:220`) — a deep tree aborts the
  process. **Fix:** explicit stack or depth limit.
- **M9 — Serverless publish race.** `publish_snapshot` awaits put-returns, which signal
  hand-off to transport, not durable storage retention; index and chunks on separate
  storages have no ordering guarantee (client can fetch index then 404 chunks). Tests
  paper over it with `sleep(200ms)`. **Fix:** read-back verification / ack / documented
  settle protocol.
- **M10 — Dead error variants & misleading progress.** `BlobError::ResumeMismatch` and
  `NoManifest` (`error.rs:34,38`) are never constructed; `download_cancellable` emits
  `Progress::Failed` for a *cancellation* (`client.rs:67-71`) though there's no
  `Cancelled` progress variant; Tier-2 `Progress::Chunk { index }` passes a counter, not
  the chunk index (`tree.rs:437` vs `progress.rs:18`). **Fix:** wire up or remove; add
  `Started`/`Resumed`/`Cancelled`; make index mean index.

### Low / inefficiency

- `server.rs:194` `buf[..len].to_vec()` copies per chunk (reuse buys nothing);
  `server.rs:167-171` re-sends the manifest on every query including resumes;
  `client.rs:193` full second pass to hash (could be incremental for in-order);
  `resume.rs` O(n) `received()`/`first_missing()` scans plus a duplicate counter in
  `client.rs:123`; no `.part` locking against concurrent downloads (`client.rs:162-169`);
  silent destination overwrite on `rename` (`client.rs:201`).

---

## 4. API & ergonomics critique

- **`Digest`/`Chunker` pluggability is nominal, not real.** `Hash` is a fixed `[u8;32]`
  (`hash.rs:15`) and every path hardcodes `Sha256Digest` (`client.rs:97,193`,
  `tree.rs:403,428,461`); neither client nor server is generic over `D`. The public
  `Digest` trait cannot actually be swapped. `FastCdcChunker` inherits nonsensical
  fixed-size `offset`/`count` defaults and ignores the size clamps
  (`chunk.rs:129`) — passing it to Tier 1 silently produces a broken manifest.
- **No builders.** `BlobServer::new`/`BlobClient::new` and `Manifest::compute` are
  fixed positional signatures with no room for timeout, consolidation, retry/overwrite
  policy, `MAX_INFLIGHT`, or digest choice without a breaking change.
  `Manifest::compute::<_, Sha256Digest>(reader, chunker, id, filename, created_ms)` has
  two adjacent swappable `impl Into<String>` args and a mandatory turbofish.
- **`download` is inflexible and unsafe by shape.** Output path is chosen from the
  *server's* filename (the C2 vulnerability); there is no `download_to(path)`,
  `download_to_writer(W)`, or streaming API. `fetch_manifest` is private, so a caller
  can't probe size/existence before committing.
- **No shutdown handle.** `run(self)` only returns when the session dies; no `stop()`,
  no `JoinHandle` wrapper, no `CancellationToken`.
- **No versioning / forward-compat.** Neither `Manifest` nor `TreeIndex` has a schema
  version field or `deny_unknown_fields`; adding a field silently breaks CBOR consumers.
  `Format` mismatch (server CBOR / client JSON) surfaces as an opaque `Encode` error;
  Zenoh's `Encoding` on the sample is never set or read.
- **`Progress` lacks `#[non_exhaustive]`** and byte-level counts / rate / ETA.
- **Misc types.** `BlobServer`/`TreeServer` are `Clone`, clients are not; nothing is
  `Debug`; `Hash` has a public tuple field (representation locked forever);
  `HashParseError` and `BuiltTree` appear in signatures but aren't re-exported (one is
  unnameable by consumers); `publish_*` demand `&Arc<Session>` where `&Session` suffices.

---

## 5. Redesign proposal (v2) — the strategic changes

Backward compat is breakable, so this is the opportunity to fix the foundations.

### 5.1 BLAKE3 + bao-tree verified streaming (highest value)

Replace whole-blob/whole-index SHA-256 with `blake3` + `bao-tree` at `BlockSize(4)`
(16 KiB chunk groups). Benefits, all concrete:

- **Incremental verification:** each Zenoh reply carries a bao-encoded slice that is
  self-verifying against the root — out-of-order replies (which zblob already tolerates)
  become individually trustworthy, and a tampered chunk is caught at 16 KiB granularity
  instead of after writing the whole `.part` (kills H3 and the Tier-1 tamper failure
  mode). Every partial transfer is a provably correct prefix.
- **Real root pinning:** the client passes the expected BLAKE3 root out of band; the
  server can no longer forge it (fixes M1 and makes the "integrity root" claim true).
- **~0.4% outboard overhead**, stored as a sibling key/file; blobs ≤ 16 KiB need no
  outboard at all.
- Keep the `<algo>` key segment so BLAKE3 stores coexist with existing SHA-256 ones.

### 5.2 Resume as a chunk-range bitfield, not `?from=K`

Generalize the suffix cursor to a `ChunkRanges` set (iroh uses `range-collections`).
This enables fetching arbitrary holes (one missing middle chunk no longer re-streams
everything after it), makes the resume sidecar a compact bitset (fixes H4), and matches
the bao slice-request model. Persist the bitfield independently of data+outboard,
batched and crash-consistent (iroh's hard-won lesson).

### 5.3 Wire format: postcard for control structs

Move `Manifest`/`TreeIndex`/range-specs to **postcard** (varint, `no_std`, ~30% smaller
than CBOR, iroh's choice). Keep **CBOR** for published/inspectable indices if desired,
and JSON only for the human-readable resume sidecar. Add an explicit schema-version
field and `Encoding` negotiation. **Important:** stop deriving the identity hash from
`serde_json` output (`tree.rs:135`) — that ties every root hash to serde_json's exact
escaping/ordering. Hash a canonical, versioned encoding instead.

### 5.4 Chunking defaults

Adopt `fastcdc::v2020` with `Normalization::Level2`, **16 KiB / 64 KiB / 256 KiB**
(min/avg/max — the casync transfer class), exposed as a preset, with a seedable gear
table (a fixed public table makes chunk boundaries a dedup side-channel; restic seeds
per-repo). Fix the `FastCdcChunker` abstraction leak (§4) or split Tier-1/Tier-2 chunker
traits so an offset-addressed chunker and a content-defined one can't be confused.

### 5.5 Zenoh 1.7–1.9 alignment

- Set `ConsolidationMode::None` and explicit `.timeout()` on every streaming query
  (fixes C1, C3) — both are stable APIs, not `internal`-gated.
- Audit the receive path to consume `ZBytes` as fragmented slices (`.slices()`/reader),
  never forcing a contiguous copy — up to 100× throughput on payloads > 32 KiB.
- Adopt Zenoh 1.7 native query cancellation (can retire the hand-rolled
  `src/cancel.rs`) and 1.8 connectivity listeners (abort/retry on link loss instead of
  timeout-only).
- Note the 1.8 change: per-reply QoS setters are deprecated (QoS now inherits from the
  query) — confirm `server.rs` doesn't rely on per-reply tuning; the crate's existing
  "set nothing" posture is now *more* correct.
- Zenoh uses SHM automatically for large messages (incl. through a router) — same-host
  chunk serving is already near-zero-copy; don't fight it.

---

## 6. Feature roadmap (prioritized)

**Quick wins (low effort, high value)**
- `ConsolidationMode::None` + explicit timeout + retry-with-backoff (C1/C3).
- Path sanitization + `O_NOFOLLOW` + symlink-last materialization (C2).
- Manifest validation on `register`; error instead of clamp (H1/H2).
- Bitset + atomic + batched sidecar (H4/H5).
- `fsync` at commit points; unique `DirStore` temp names (H5/H9).
- `Format`-mismatch and unsupported-algo produce clear diagnostics.
- Fix docs drift (§8) and dead error variants (M10).

**Medium**
- Parallelize Tier-2 fetch (`buffer_unordered`) + multi-hash range selector (H6).
- Per-chunk integrity or bao verified streaming (H3 → §5.1).
- `ContentStore` lifecycle: `remove`, size accounting, tag-based GC (iroh model:
  temp tags protect in-flight, persistent tags survive restart, mark-and-sweep).
- Stream `build_tree`/publish to fix Tier-2 memory (M3); async store trait (M4).
- Expected-root pinning on `download_tree` (M1).
- Builders for client/server/manifest; `download_to`/`download_to_writer`; public
  `fetch_manifest`; shutdown handle.
- Per-chunk **zstd** with a per-chunk algorithm tag + stored uncompressed length
  (restic v2 model: store compressed, verify uncompressed hash, bail out on
  incompressible data).

**Ambitious**
- **BLAKE3 + bao-tree** verified streaming and chunk-range resume (§5.1/5.2).
- **Tier-1 push/upload** (an `iroh` `PushRequest` analogue; mirror of `publish_snapshot`)
  with an authorization hook on `BlobSource`/server.
- **Seed / local dedup** (desync model): satisfy chunks from prior destination versions,
  other local trees, and a synthetic all-zero seed; reflink where the FS supports it.
- **Multi-source / swarming fetch**: split a range set across multiple queryables
  answering the same chunk key; single-flight de-dup of concurrent in-process requests.
- **Availability introspection** (iroh `ObserveRequest`): a queryable returning a peer's
  bitfield so a client can pick the peer that already has the most.
- **Fanout tier via `zenoh-ext` AdvancedPublisher** (`cache` + `sample_miss_detection` +
  heartbeat) for one-to-many rollout, where the queryable model degrades to N
  independent transfers.
- **Observability**: optional `tracing` feature, transfer statistics (bytes/s, retries,
  chunk latencies) returned to the caller, server error callback.
- **Encryption at rest** (borg/restic per-chunk AEAD) — with the standard caveat that
  cross-tenant content-addressed dedup leaks membership; key the gear seed per store.

---

## 7. Testing improvements

Current suite is 33 tests with good happy-path and a real Tier-1 tamper test, but with
significant gaps and flakiness:

- **Add the missing adversarial tests:** path-traversal / zip-slip indices; Tier-2
  malicious server (wrong-content chunk — the `HashMismatch` at `tree.rs:431` is
  currently dead in tests); forged `root_hash`; mismatched `size`; index for a different
  `id`; unsupported-algo rejection; `NotFound` paths; corrupted local store.
- **Cover the edges:** empty file / dir / tree; large files (nothing today exceeds
  ~787 KB, so the whole-file `fs::read` and `u32` chunk-count edges are unexercised);
  CBOR on the wire for `TreeIndex` (every Tier-2 test uses JSON, though CBOR is what the
  docs recommend); `Format` mismatch; non-UTF-8 / reserved / case-colliding names;
  hardlinks, FIFOs, read-only dirs; mode/mtime preservation (`assert_dirs_equal`
  compares only names/targets/bytes today); `build_tree` reproducibility.
- **Concurrency:** two `download_tree` against one `DirStore` (exposes the temp-file
  race); two clients against one `TreeServer` (no inflight limit).
- **De-flake:** replace the ten fixed `sleep()` calls with Zenoh matching-listener /
  liveliness synchronization; count store population without including `.tmp` leftovers;
  clean shutdown instead of `handle.abort()` mid-reply; wrap every async test in
  `tokio::time::timeout`.
- **Dead variants:** `ResumeMismatch`, `NoManifest`, and the Tier-1 resume corner cases
  (truncated `.part.meta`, sidecar hash mismatch, `.part` without sidecar) are untested.
- **De-duplicate** `pseudo_random`/`isolated_config` (copied across `tests/common`,
  `roundtrip.rs`, and `chunk.rs`).

---

## 8. CI & docs improvements

**CI** (`.forgejo/workflows/ci.yml` — already has fmt/clippy/test/msrv/docs/dry-run):
- **Cross-platform matrix** (Windows + macOS). The `#[cfg(not(unix))]` symlink/mode
  branches have literally never been compiled by CI, and they silently drop data (M6).
- **Fix the MSRV.** Declared `1.97` but the real floor is 1.88 (let-chains); 1.97 was a
  fleet-uniformization bump that needlessly excludes consumers on 1.88–1.96. Also add a
  `-Z minimal-versions` check.
- **Fuzz** `Hash::from_str`, `parse_store_key`, `parse_id`/`parse_from`, and
  `decode::<Manifest>`/`decode::<TreeIndex>` on adversarial input; **property-test** the
  chunk arithmetic and resume bitmap (`proptest`).
- **Supply chain:** `cargo audit` / `cargo deny` (Renovate updates deps but nothing
  audits them; note two parallel RustCrypto stacks from `sha2 0.11`).
- **Coverage** (`llvm-cov`) would have caught the dead variants immediately.
- **Reproducibility:** the workflow hardcodes a self-hosted sccache path
  (`/srv/cache/...`) and a `10.10.0.30:3000` release target, so a fork's PR cannot run
  CI; the GitHub mirror at `repository = github.com/p13marc/zblob` has no CI. Add
  `benches/` (criterion) for chunking/hashing/dedup regression — none exist today for a
  throughput-centric crate.

**Docs / metadata:**
- `CLAUDE.md:25` names `.github/workflows/ci.yml`; the real path is `.forgejo/...`.
- Nine broken monorepo-relative links (across `README.md`, `docs/router-storage.md`, and
  three in `src/` that ship broken to docs.rs); README uses rustdoc `[`link`]` syntax
  with no Markdown definitions; README/`router-storage.md` code snippets don't compile
  and aren't doctests.
- `src/lib.rs` crate docs describe only Tier 1 — Tier 2 (`TreeServer`/`TreeClient`/
  `publish_snapshot`, the store/tree key layout) is absent from the docs.rs front page.
- Trim deps: `tokio` `features=["full"]` in a library is an anti-pattern (use
  `fs,io-util,sync,rt`); gate `fastcdc`/`ciborium`/`serde_json` behind features; verify
  the `zenoh` `unstable` feature is actually needed (grep shows no `unstable`-gated use);
  add missing publish metadata (`documentation`, `homepage`, `[package.metadata.docs.rs]`).
- Add `CHANGELOG.md`, `SECURITY.md`, `CONTRIBUTING.md`, `rust-toolchain.toml`, and an
  `examples/` dir (currently the only usage guidance is non-compiling README snippets).
- Add a security note that materializing an untrusted tree writes wherever the index
  says (until C2 is fixed) — the docs currently contemplate an open storage without this
  warning.

---

## 9. Migration notes

Compat is breakable, so sequence the wire-affecting changes into one v2 bump:
- New `<algo>` value for BLAKE3 keeps old SHA-256 stores readable side by side.
- Add schema-version fields to `Manifest`/`TreeIndex` now, before the postcard switch, so
  future changes are detectable rather than silent.
- The path-sanitization, consolidation, timeout, sidecar, and validation fixes are
  **non-breaking** and should land first as a v0.1.x hardening release, independent of
  the v2 redesign.

---

## Verification

This report is prose; verify by review. The two headline claims were confirmed against
source: the consolidation buffering path in `zenoh-1.9.0/src/api/session.rs:2636,3436`
(Auto→Latest, replies flushed only at `ResponseFinal`) and the traversal at
`client.rs:200` / `tree.rs:475`.
