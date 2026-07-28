# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`zblob` is a single-crate Cargo workspace: generic, resumable, chunked blob and
directory transfer over Zenoh — **wire v2**: BLAKE3 + bao verified streaming,
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

## Architecture (wire v2)

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
(`?v=2&ranges=…`, `ConsolidationMode::None`, explicit timeout, retry with
backoff); every reply is a self-verifying bao slice checked against the
(pinnable) root *before* hitting the `.part` — no final hash pass, tampered
slices are dropped alone. The server also answers `…/have` availability
bitfields and (opt-in via `accept_push` + `PushPolicy`) verified resumable
uploads spooled server-side. The caller always chooses the destination
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
`tree_key`, `parse_id`, `parse_ranges`) — don't format keys ad hoc.

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
   tree_security,storage,push,multisource,coverage,compression,fanout}.rs`) —
   one file per concern. Useful, but they only ever assert outcomes for inputs
   *the author chose*, so they confirm the implementation rather than
   interrogate it.
2. **Property tests** (`proptest`, in `#[cfg(test)] mod properties` inside
   `src/{lib,chunk,resume,verify}.rs`) — invariants over generated inputs:
   the range grammar's accept-set, chunk-grid tiling, bitfield view coherence,
   CDC losslessness, and the bao core (a slice decodes to exactly its byte
   range; any mutation is caught; a slice cannot be replayed at another index).
3. **Adversarial + contract suites** (`tests/hostile_peer.rs`,
   `tests/store_contract.rs`, `tests/minifuzz.rs`) — a peer that mutates every
   reply against a fixed oracle ("succeed with exactly the right bytes, or
   fail cleanly"), and one contract executed against *every* `ContentStore`
   configuration. These found bugs the scenario tests could not: they are
   where new invariants belong.

**When adding a defence, add it at layer 2 or 3.** A scenario test for the one
input that motivated the fix is not coverage — it is a regression pin. Also
assert the test's own discriminating power (the honest control must pass and
the hostile case must fail), or a harness bug can make the suite vacuous;
`hostile_peer.rs` and `tree_security.rs` both do this explicitly. Shared helpers are in `tests/common/mod.rs`:
`open_session()` opens an isolated in-process session with scouting disabled
(the loopback pattern — tests must not discover each other or the LAN),
`unique_prefix()` namespaces keys per test, `pseudo_random()` gives
deterministic data without a rand dependency, and `common::bao` crafts real
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
