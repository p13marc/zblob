# zblob

Generic, resumable, chunked **blob and directory transfer over [Zenoh]** — with
BLAKE3 **verified streaming** (every reply proves itself against a pinnable
root before touching disk), range-set resume that survives reconnect *and*
restart, bounded memory, and content-addressed dedup. No application-specific
types; it's the large-payload path the Zenoh ecosystem is otherwise missing.

[Zenoh]: https://zenoh.io

> Formerly incubated in the [ZenSight](https://github.com/p13marc/zensight)
> monorepo as `zenoh-blob`; renamed on graduation — this crate is a community
> project, not an Eclipse Zenoh deliverable.

## Why

Zenoh is excellent at pub/sub and query, but has no turnkey way to move a large
artifact (a debug bundle, a pcap, a dataset, a directory tree) between peers
with progress, integrity, and resume. `zblob` builds that on the primitives
Zenoh already gives you — multi-reply queryables, a reliable transport, and
`CongestionControl::Block` backpressure — so you don't fork a file-sync tool to
get it.

## The integrity model (wire v2)

A blob's identity is its **BLAKE3 bao root**. Every transfer chunk travels as a
*bao slice*: the bytes plus the parent hashes proving them against that root.
Replies are verified **as they arrive, out of order, at 16 KiB granularity** —
there is no end-of-transfer hash pass, a tampered reply is dropped alone and
re-fetched, and a partial download is always a proven-correct partial. Pin the
root (`DownloadRequest::pinned`) and a server cannot substitute content at all.

## Two tiers

**Tier 1 — single blob.** One queryable serves every blob under a key prefix.
A download is a manifest GET, then range-set slice GETs
(`?v=2&ranges=0-5,9,12-20`): the client persists a chunk bitfield next to the
`.part` file and re-queries exactly its holes, so resume, retry, and
arbitrary-hole fetch are the same code path. Memory stays `O(chunk_size)`
regardless of blob size and arrival order.

```rust,ignore
// Server
let server = zblob::BlobServer::new(session.clone(), "demo/blobs");
let manifest = server
    .register_file(zblob::BlobSpec::new("blob-1").filename("report.pcap"), &path)
    .await?;
let handle = server.spawn().await?; // distribute (id, manifest.root) out of band

// Client — the caller picks the destination; pin the root when you know it.
let client = zblob::BlobClient::new(session, "demo/blobs");
let stats = client
    .download_to(
        &zblob::DownloadRequest::pinned("blob-1", manifest.root),
        &dest_path,
        &(),
        &zblob::CancelToken::new(),
    )
    .await?;
```

Tier 1 also supports **push** (verified uploads gated by a `PushPolicy`
authorization hook), **availability introspection** (`…/have` bitfields per
responder), and replicated servers answering one download cooperatively.

**Tier 2 — content-addressed directories** (the [casync] model). A snapshot is
a [`TreeIndex`] (a depth-first entry list; files reference their chunks by
content hash) plus a content-addressed chunk store. The client fetches only the
chunks it is **missing** (`needed − have`, concurrently, re-hashing each on
receipt) and reconstructs the tree — safely: paths are sanitized, symlinks
materialize last with confined targets, hard links and modes/mtimes round-trip.
Progress *is* "which hashes are on disk", so an interrupted pull resumes for
free and identical chunks (across files or versions) transfer once. [FastCDC]
content-defined chunking (seedable gear table) localizes edits so a small
change re-transfers only its neighborhood.

[casync]: https://github.com/systemd/casync
[FastCDC]: https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia

```rust,ignore
let store = zblob::MemoryStore::new();
let index = zblob::build_tree(dir, "snap-1", &zblob::CdcParams::default(), &store)?;

// serve live...
let server = zblob::TreeServer::new(session.clone(), "demo/store", "demo/tree", Arc::new(store));
server.register(index.clone()).await;
let handle = server.spawn().await?;

// ...or publish into a router storage (with read-back settling) and exit:
zblob::publish_snapshot(&session, "demo/store", "demo/tree", &index, &store,
                        zblob::ChunkCompression::default(), settle).await?;

// client
let client = zblob::TreeClient::new(session, "demo/store", "demo/tree");
client.download_tree(
    &zblob::DownloadRequest::pinned("snap-1", index.root_hash),
    &dest, &content_store, &(), &zblob::CancelToken::new(),
).await?;
```

Tier 2 also ships **seeding** (`seed::seed_store` satisfies chunks from prior
local copies and zero regions before touching the network) and **lifecycle**
(`gc::sweep` mark-and-sweep with persistent snapshot tags and in-flight temp
tags).

## Cargo features

| Feature | What it adds |
|---|---|
| `zstd` | Per-chunk zstd compression (wire + at-rest), restic-v2 style, with raw bail-out for incompressible data. |
| `encryption` | XChaCha20-Poly1305 encryption at rest for `DirStore` (`with_encryption`), convergent per store key. |
| `fanout` | One-to-many rollout tier over `zenoh-ext` `AdvancedPublisher` (cache + miss detection + late-joiner replay). |
| `tracing` | `tracing` events at registration/serve/download/GC points. |

## Design notes

- **Backpressure is automatic.** `Session::get` defaults to
  `CongestionControl::Block` and replies inherit it, so chunk replies block
  rather than drop under load. The crate sets **no** congestion control
  explicitly (the setter is behind Zenoh's `internal` feature, deliberately not
  enabled). Reply *consolidation* is a different knob: clients set
  `ConsolidationMode::None` so replies stream instead of being buffered until
  query finalization.
- **Reply keys must match the query.** Clients GET the `<prefix>/<id>/**`
  wildcard so the `slice/<i>` replies are accepted
  (`ReplyKeyExpr::MatchingQuery`).
- **Wire format is postcard** with explicit schema-version-first fields and
  Zenoh `Encoding` tags on every reply; the Tier-2 `root_hash` is a canonical
  versioned digest (mtime excluded), so byte-identical trees hash identically.
- **Untrusted input is bounded and validated everywhere**: manifest/index
  sizes, chunk geometry (validated, never clamped), entry paths, symlink
  targets, allocation caps.

## Documentation

- [`docs/router-storage.md`](docs/router-storage.md) — run a Zenoh router as
  the fleet-wide Tier-2 chunk store: serverless transfers (the producer PUTs
  and exits), fleet-wide dedup, survival across producer restarts.
- [`docs/analysis-2026-07.md`](docs/analysis-2026-07.md) — the deep analysis
  that motivated the v2 redesign, and the design rationale behind it.
- [`CHANGELOG.md`](CHANGELOG.md) — including the full v1 → v2 migration notes.
- [`examples/`](examples/) — runnable end-to-end blob and tree transfers.

## Acknowledgements

The design borrows ideas from prior art in the space: [iroh-blobs] (BLAKE3/bao
verified streaming, range-set requests, tag-based GC), [casync] and [desync]
(content-addressed trees, seeding), [restic] (per-chunk compression container,
seeded chunking), and the [FastCDC] paper. `zblob` is an independent
implementation, not a fork of any of them.

[iroh-blobs]: https://github.com/n0-computer/iroh-blobs
[desync]: https://github.com/folbricht/desync
[restic]: https://restic.net

## License

Licensed under the [MIT license](LICENSE).

[`TreeIndex`]: https://docs.rs/zblob/latest/zblob/struct.TreeIndex.html
