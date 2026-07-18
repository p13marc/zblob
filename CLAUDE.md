# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`zblob` is a single-crate Cargo workspace: generic, resumable, chunked blob and
directory transfer over Zenoh (progress, SHA-256 integrity, range resume,
bounded memory). It carries no application-specific types. It graduated from the
ZenSight monorepo in 2026-07 (formerly the in-tree `zenoh-blob` crate);
ZenSight consumes it as a git dependency, so local edits here are not picked up
by a zensight build until pushed (see `../CLAUDE.md` for the cross-repo
`[patch]` workflow and `docs/graduation.md` for history).

## Commands

```bash
cargo test                      # all tests (integration tests open in-process Zenoh sessions; no router needed)
cargo test --test roundtrip     # one integration test binary (cancel, resume, roundtrip, storage, tamper, tree)
cargo test key_builders         # one test by name
cargo clippy --all-targets -- -D warnings   # zero-warnings policy (CI gate)
cargo fmt --all --check
```

CI (`.github/workflows/ci.yml`) runs build + test with `--locked`, fmt, clippy
`-D warnings`, and a `cargo publish --dry-run` — keep `Cargo.lock` committed and
the crate publishable (no path dependencies).

## Architecture

Two independent transfer tiers share the pluggable primitives (`Digest`/`Hash` in
`hash.rs`, `Chunker` fixed-size or FastCDC in `chunk.rs`, `Format` JSON/CBOR in
`format.rs`, `ProgressSink`, `CancelToken`).

**Tier 1 — single blob by id** (`server.rs`, `client.rs`, `manifest.rs`,
`resume.rs`): one `BlobServer` queryable on `<prefix>/**` serves every registered
blob. A download is exactly two GETs: manifest first (`<prefix>/<id>/manifest`),
then chunks (`<prefix>/<id>/**?from=K`). Manifest-first is load-bearing: Zenoh
does not order query replies, and knowing `chunk_size` up front lets the client
write each out-of-order chunk at its byte offset into a `.part` file — memory
stays O(chunk_size). Resume state is a JSON sidecar (`.part.meta`, `resume.rs`)
bound to id + whole-blob hash + chunking, so a regenerated source can never
splice mismatched halves. The whole-blob SHA-256 is verified before the rename
into place. Servers stream chunks lazily from a `BlobSource` reader — never
`read_to_end`.

**Tier 2 — content-addressed directory trees** (`tree.rs`, `store.rs`,
`publish.rs`): the casync model. A snapshot is a `TreeIndex` (depth-first entry
list; files reference chunks by content hash) plus a `ContentStore` of chunks
keyed `<prefix>/<algo>/<hex>`. The client fetches only missing hashes
(`needed − have`), re-hashing each on receipt; progress *is* the set of hashes
on disk, so resume and cross-file/cross-version dedup fall out for free. Chunks
can be served live (`TreeServer`) or PUT into a router-hosted Zenoh storage via
`publish_snapshot` so the producer can exit (see `docs/router-storage.md`).
Chunk keys are immutable, so re-publishing is idempotent.

All key expressions are built through the helpers in `lib.rs` (`manifest_key`,
`chunk_key`, `download_selector`, `store_key`, `tree_key`, `parse_id`,
`parse_from`) — don't format keys ad hoc.

### Two Zenoh facts the design relies on (from `lib.rs`)

1. **Backpressure is automatic.** `Session::get` defaults to
   `CongestionControl::Block` and replies inherit it. The crate deliberately sets
   no congestion control and does not enable Zenoh's `internal` feature — do not
   "fix" this by enabling it.
2. **Reply keys must match the query.** Clients must GET the `<prefix>/<id>/**`
   wildcard or chunk replies are silently rejected
   (`ReplyKeyExpr::MatchingQuery`). `download_selector` enforces this.

## Tests

Integration tests live in `tests/`, one file per concern (roundtrip, resume,
cancel, tamper, storage, tree). Shared helpers are in `tests/common/mod.rs`:
`open_session()` opens an isolated in-process session with scouting disabled
(the loopback pattern — tests must not discover each other or the LAN),
`unique_prefix()` namespaces keys per test, `pseudo_random()` gives
deterministic data without a rand dependency, and `BytesSource` is an in-memory
`BlobSource`. Follow these patterns for new tests.

## Conventions

- Conventional commits (`feat:`, `fix:`, `chore:`, `docs:`); breaking changes
  marked `!`.
- Rust edition 2024, Zenoh 1.9 with only the `unstable` feature.
- Every public item is documented; module docs explain the *why* (several
  design invariants live only there — read them before changing behavior).
- Some links in `docs/` still point at monorepo-relative paths
  (`../../docs/design/large-data-transfer.md`) — the design doc lives in the
  zensight repo.
