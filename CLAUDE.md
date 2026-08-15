# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`zblob` is a single-crate Cargo workspace: generic, resumable, chunked blob and
directory transfer over Zenoh — **wire v3**: BLAKE3 + bao verified streaming,
range-set resume, postcard control messages, content-addressed directory trees.
It carries no application-specific types. It graduated from the ZenSight
monorepo in 2026-07 (formerly the in-tree `zenoh-blob` crate); ZenSight
consumes it as a crates.io dependency, so local edits here are not picked up by
a zensight build until published (see `../CLAUDE.md` for the cross-repo
`[patch]` workflow and `docs/graduation.md` for history).

## Commands

```bash
cargo test --test roundtrip     # one integration test binary
cargo test key_builders         # one test by name
cargo bench                     # criterion benches (CI only compiles them)
```

**Run the gates exactly as CI does before pushing** — CI sets
`RUSTFLAGS: -D warnings` globally and builds *both* feature sets, so a plain
`cargo test` locally can pass while CI fails (feature-gated code makes a
binding unused, a `#[cfg]` arm goes dead, …). This is the full sequence:

```bash
export RUSTFLAGS="-D warnings"
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo clippy --all-targets --locked -- -D warnings   # default features too
cargo test --locked
cargo test --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo publish --dry-run
```

CI (`.forgejo/workflows/ci.yml`) runs build + test (default and all-features)
with `--locked`, fmt, clippy `-D warnings`, MSRV 1.97 check, docs, cargo-audit,
llvm-cov, bench compile, and a `cargo publish --dry-run` — keep `Cargo.lock`
committed and the crate publishable. A weekly fuzz workflow runs the `fuzz/`
targets. MSRV stays **1.97** (fleet policy).

**There is deliberately no `.github/workflows/`** — workflows live in
`.forgejo/` only and the GitHub mirror runs no CI (commit `287341a`). Do not
add one "to get cross-platform coverage": that was tried, and a permanently
red mirror CI is worse than none. The consequence is real and should be
stated rather than papered over — **the `#[cfg(windows)]` / `#[cfg(not(unix))]`
branches are not exercised by CI**, because the Forgejo runner is Linux-only
and cross-checking needs toolchains it does not have (`ring` fails for
`x86_64-pc-windows-msvc` without a C toolchain; macOS needs osxcross). If
cross-platform coverage becomes a requirement, decide it explicitly: provision
mingw on the runner for `x86_64-pc-windows-gnu` compile checks, or re-enable a
GitHub matrix on purpose.

## Architecture (wire v3)

The prose docs cover this in depth — `docs/architecture.md` (map + diagrams),
`docs/integrity-model.md`, `docs/wire-protocol.md`, `docs/design-decisions.md`.
This section is the terse in-repo version.

Both tiers share the primitives: `hash.rs` (BLAKE3-only `Hash`), `verify.rs`
(bao outboard/slice encode + verified decode — the integrity core), `wire.rs`
(postcard + `Encoding` tags + `WIRE_VERSION`), `chunk.rs` (`TransferChunks`
fixed-size arithmetic for Tier 1; `CdcParams` seedable FastCDC for Tier 2),
`paths.rs` (traversal-safe path/symlink sanitization), `compress.rs`
(self-describing chunk containers, optional zstd), `resume.rs` (crash-safe
bitfield sidecar), `progress.rs`, `cancel.rs`, `obs.rs` (TransferStats +
optional tracing).

**Tier 1 — single blob by id** (`server.rs`, `client.rs`, `manifest.rs`): one
`BlobServer` queryable on `<prefix>/**`. Registration streams the source once
to build the bao outboard (mem, or sibling `.obao4` file for huge blobs) and
derives the manifest — a served manifest can't disagree with the bytes. A
download is a manifest GET then range-set slice GETs
(`?ranges=…`, `ConsolidationMode::None`, explicit timeout, retry with
backoff); every reply is a self-verifying bao slice checked against the
(pinnable) root *before* hitting the `.part` — no final hash pass, tampered
slices are dropped alone. The server also answers `…/have` availability
bitfields and (opt-in via `accept_push` + `PushPolicy`) verified resumable
uploads spooled server-side; the uploader reads from a file (`upload_file`)
or any `BlobSource` (`upload_source`, no staging file). The caller always chooses the destination
(`download_to`); the manifest filename is advisory only.

**Tier 2 — content-addressed directory trees** (`tree.rs`, `store.rs`,
`publish.rs`, `seed.rs`, `gc.rs`): the casync model. A snapshot is a
`TreeIndex` (depth-first entries; files reference chunks by BLAKE3 hash, CDC
parameters recorded in the index) plus a `ContentStore` keyed
`<prefix>/blake3/<hex>`. `root_hash` is a canonical versioned postcard digest
with mtime excluded. The client validates the index fully (paths, sizes, root
recomputation, optional pinning) before fetching missing chunks concurrently
and materializing defensively: sanitized paths, symlinks last with confined
targets, canonical-parent checks, dir modes/mtimes restored last. `DirStore`
is fanned out (`blake3/<xx>/<hex>`), atomic + fsynced, with optional
verify-on-read, `scrub()`, zstd at rest, and (feature) XChaCha20-Poly1305
sealing. `publish_snapshot` PUTs into a router storage and **read-back
settles** before returning. `seed.rs` satisfies chunks from prior local copies
and zero regions; `gc.rs` does tag-based mark-and-sweep.

**Fanout tier** (`fanout.rs`, feature-gated): one-to-many rollout over
zenoh-ext `AdvancedPublisher` (cached bao-slice sample stream; late joiners
replay history; every receiver verifies).

All key expressions are built through the helpers in `lib.rs` (`manifest_key`,
`slice_key`, `slice_selector`, `availability_key`, `push_*_key`, `store_key`,
`store_batch_key`, `store_have_key`, `tree_key`, `tree_have_key`, `parse_id`,
`parse_tier2_tail`, `parse_ranges`) — don't format keys ad hoc. Prefixes are
typed by role (`ServePrefix` / `QueryPrefix`, `prefix.rs`); a server cannot be
built on a wildcard because there is no value to build it from.

### What v3 added, and the two traps in it

- **Batched tier-2 fetch** (`<store>/<algo>/batch`, want-list payload). Replies
  come back on each chunk's *own* key, which is **disjoint** from the batch
  key — so the query must set `accept_replies(ReplyKeyExpr::Any)`, and without
  it Zenoh refuses each reply **on the server**. Do not "simplify" this into a
  wildcard request key: `<store>/<algo>/**` would make every router-hosted
  storage in range dump its entire content store in answer to one query.
- **A batch is not answered by storages.** A storage serves by key and has
  nothing at `…/batch`, so the per-chunk fallback after each round is what
  keeps `docs/router-storage.md`'s publish-then-exit tier working. Don't drop it.
- **Tier-2 probes** (`…/<algo>/have`, `<tree>/<id>/have`) reply with a size
  that is a function of the *question*, never of the objects — that is the
  whole reason tier 2 may have a probe at all under RFC 07 §3.
- **Servers reply on their own key, not `query.key_expr()`.** Against a
  concrete GET they are the same; against a wildcard-origin one the query names
  every origin, so replying with it makes answers unattributable and
  uncacheable.
- **Large indices only** are sharded into the store and served as an
  `IndexDescriptor`; small ones go whole. An index costs ~0.05–0.10% of its
  payload, so a descriptor on every fetch would add a round trip for nothing.

### Three things that are *not* bugs

Each was proposed, investigated, and rejected on evidence. Don't re-litigate
without new measurements.

1. **An unknown id does not cost a query timeout** (~1 ms, measured; see
   `an_unknown_id_fails_fast_not_on_the_timeout`). A Zenoh query finalizes when
   its matching queryables complete, and completing without replying is
   immediate. Silence is how a server says "not mine", and it is what lets
   several servers share one prefix. No negative-reply message is needed.
2. **Tier-1 availability is all-or-nothing by construction.** A bao slice
   carries the sibling hashes proving it against the root, and those require
   the whole blob — so a partial holder can serve no verified slice at all.
   Having an in-flight push advertise its resume bitfield is the obvious
   improvement and it is a lie. Partial possession is real on *tier 2*, and
   that is what `StoreClient::probe` reports.
3. **The default chunk size is measured, not chosen.** See
   `DEFAULT_CHUNK_SIZE`'s doc table and `tests/chunk_size.rs`.

### Three facts the design relies on (from `lib.rs`)

1. **Backpressure is automatic on queries.** `Session::get` defaults to
   `CongestionControl::Block` and replies inherit it. The crate deliberately
   sets no congestion control on queries and does not enable Zenoh's
   `internal` feature — do not "fix" this by enabling it. (Reply
   *consolidation* is different: clients set `ConsolidationMode::None` so
   replies stream. **Publications default to `Drop`**, so the `fanout` tier
   sets `Block` explicitly.)
2. **Reply keys must match the query.** Clients must GET the `<prefix>/<id>/**`
   wildcard or slice replies are silently rejected
   (`ReplyKeyExpr::MatchingQuery`). `slice_selector` enforces this.
3. **Any peer can answer.** Unacceptable replies (bad decode, failed
   validation, wrong id, wrong pinned root) are skipped, never fatal — one
   hostile or stale responder must not deny a fetch an honest replica
   answers. Keep this property when touching any `fetch_*` loop.

## Tests

Three layers, because the first one alone is what let real defects through:

1. **Scenario tests** (`tests/{roundtrip,resume,cancel,tamper,tree,
   tree_security,storage,push,multisource,coverage,compression,batch,
   striping,chunk_size,read_surface,limits,fanout}.rs`) — one file per
   concern. Useful, but they only ever assert outcomes for inputs *the author
   chose*, so they confirm the implementation rather than interrogate it.
   `limits.rs` is the exception in spirit: it drives every allocation bound
   with an input just over the line *and* one just under.
2. **Property tests** (`proptest`, in `#[cfg(test)] mod properties` inside
   `src/{keys,chunk,resume,verify,wire}.rs`) — invariants over generated
   inputs: the range grammar's accept-set, chunk-grid tiling, bitfield view
   coherence, CDC losslessness, what each of the five wire validators lets
   through, and the bao core (a slice decodes to exactly its byte range; any
   mutation is caught; a slice cannot be replayed at another index).
3. **Adversarial + contract suites** (`tests/hostile_peer.rs`,
   `tests/hostile_store.rs`, `tests/hostile_server.rs`,
   `tests/hostile_fanout.rs`, `tests/store_contract.rs`,
   `tests/minifuzz.rs`) — the first two mutate every reply against a fixed
   oracle ("succeed with exactly the right bytes, or fail cleanly") pointed
   at *clients* (tier 1 and tier 2); `hostile_server.rs` and
   `hostile_fanout.rs` point the same oracle the other way, at the
   `BlobServer`'s refusal paths and the fanout receiver — the honest client
   discards error replies, so these use raw `session.get()`s to observe
   `reply_err`; and `store_contract.rs` runs one contract against *every*
   `ContentStore` configuration. These found bugs the scenario tests could
   not — including, while being written, a fanout receiver a hostile
   publisher could hold open forever, and a fanout phase-B frame filter a
   co-publisher could bypass. They are where new invariants belong.

**Coverage is ~89% of lines** (`cargo llvm-cov --all-features --summary-only`,
2026-08-15). It is a floor to hold, not a target to game: the number went from
77% to 89% during the 0.3 pre-release review, and the tests that moved it
found five real defects. The server- and receiver-facing adversarial suites
then lifted the three weakest files — `fanout.rs` 77%→92%, `server.rs`
81%→86%, `publish.rs` 77%→85% (line coverage) — and found a sixth defect on
the way (the fanout phase-B frame filter). What remains uncovered in
`server.rs`/`publish.rs` is mostly genuine-fault I/O paths (spool renames,
storage read failures) that need fault injection rather than a hostile peer.

**When adding a defence, add it at layer 2 or 3.** A scenario test for the one
input that motivated the fix is not coverage — it is a regression pin. Also
assert the test's own discriminating power (the honest control must pass and
the hostile case must fail), or a harness bug can make the suite vacuous;
`hostile_peer.rs`, `hostile_store.rs` and `tree_security.rs` all do this
explicitly — and it earns its keep: the fanout tamper test's control caught
that a single publish burst raced the subscriber declaration, which had made
both the hostile case and the control pass for the same wrong reason. Shared helpers are in `tests/common/mod.rs`:
`open_session()` opens an isolated in-process session with scouting disabled
(the loopback pattern — tests must not discover each other or the LAN),
`unique_prefix()` namespaces keys per test, `pseudo_random()` gives
deterministic data without a rand dependency (it mixes its seed — the obvious
`seed | 1` made consecutive seeds return *identical* bytes, which is invisible
until something content-addressed deduplicates them), and `common::bao` crafts real
(or deliberately tampered) bao slices for adversarial fake servers. Servers
are started with `spawn().await` (queryables are declared before it returns)
— never sleep-and-hope. Follow these patterns for new tests.

## Conventions

- Conventional commits (`feat:`, `fix:`, `chore:`, `docs:`); breaking changes
  marked `!`.
- Rust edition 2024, Zenoh 1.9 with only the `unstable` feature.
- Every public item is documented; module docs explain the *why* (several
  design invariants live only there — read them before changing behavior).
- Wire changes bump `WIRE_VERSION` (postcard is positional — schema shape
  changes are otherwise silent corruption).
- `#![warn(missing_docs)]` is on: every public item needs a doc comment.
- Test fake-servers must frame chunk payloads in a container (`0x00` + bytes)
  like a real server, or the client rejects them before the code under test
  runs and the test passes for the wrong reason.
