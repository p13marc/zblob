# Changelog

All notable changes to `zblob` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer.

## [Unreleased] — 0.2.0 "wire v2"

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

### Filesystem fidelity

- Non-regular files and non-UTF-8 names are loud errors, hard links round-trip
  as hard links, deep trees no longer overflow the stack (#17); Windows
  symlink materialization errors instead of silently omitting entries (#18).

### API

- Builders for `BlobServer`/`BlobClient`/`TreeClient`; `BlobSpec`;
  public `fetch_manifest`; `download_to` with an `Overwrite` policy;
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
