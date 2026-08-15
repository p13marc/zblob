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

## The integrity model (wire v3)

A blob's identity is its **BLAKE3 bao root**. Every transfer chunk travels as a
*bao slice*: the bytes plus the parent hashes proving them against that root.
Replies are verified **as they arrive, out of order, at 16 KiB granularity** —
there is no end-of-transfer hash pass, a tampered reply is dropped alone and
re-fetched, and a partial download is always a proven-correct partial. Pin the
root (`DownloadRequest::pinned`) and a server cannot substitute content at all.

## Three tiers

**Tier 1 — single blob.** One queryable serves every blob under a key prefix.
A download is a manifest GET, then range-set slice GETs
(`?ranges=0-5,9,12-20`): the client persists a chunk bitfield next to the
`.part` file and re-queries exactly its holes, so resume, retry, and
arbitrary-hole fetch are the same code path. Memory stays `O(chunk_size)`
regardless of blob size and arrival order.

Key prefixes are typed by the role they play: a server owns a concrete
`ServePrefix`, a client asks through a `QueryPrefix` (which may name several
origins). Serving something implies being able to ask for it, so the
conversion one way is free and the other way is fallible.

```rust,ignore
use zblob::{BlobClient, BlobServer, BlobSpec, DownloadRequest, QueryPrefix, ServePrefix};

// Server
let serve = ServePrefix::new("demo/blobs")?;
let query = QueryPrefix::from(&serve);
let server = BlobServer::new(&session, serve);
let manifest = server
    .register_file(BlobSpec::new("blob-1").filename("report.pcap"), &path)
    .await?;
let handle = server.spawn().await?; // distribute (id, manifest.root) out of band

// Client — the caller picks the destination; pin the root when you know it.
// A transfer is a call builder: the two things it cannot do without are
// positional, and progress/cancellation/overwrite are set on the builder.
let client = BlobClient::new(&session, query);
let stats = client
    .download_to(&DownloadRequest::pinned("blob-1", manifest.root), &dest_path)
    .progress(&|p| println!("{p:?}"))
    .cancel(&cancel)
    .await?;
```

*(This is `examples/blob_transfer.rs`, which CI compiles and runs.)*

Tier 1 also supports **push** (verified uploads gated by a `PushPolicy`
authorization hook), **availability introspection** (`…/have` bitfields per
responder), replicated servers answering one download cooperatively, and a
filesystem-free path in both directions: `download_to_writer` fills any
seekable async writer, and `upload_source` pushes any `BlobSource` (an
in-memory buffer, a generated artifact) without staging it in a file.

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
use std::sync::Arc;
use zblob::{
    CdcParams, ContentStore, DownloadRequest, MemoryStore, Publisher, ServePrefix,
    SettleCoverage, TreeClient, TreeServer, build_tree,
};

let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
let index = build_tree(dir, "snap-1", &CdcParams::default(), &store)?;

let store_p = ServePrefix::new("demo/store")?;
let tree_p = ServePrefix::new("demo/tree")?;

// Serve it live…
let server = TreeServer::new(&session, store_p.clone(), tree_p.clone(), store.clone());
server.register(index.clone()).await?;
let handle = server.spawn().await?;

// …or publish into a router storage, wait for the read-back to settle, and
// exit. See docs/router-storage.md.
Publisher::new(&session, store_p.clone())
    .snapshots(tree_p.clone())
    .coverage(SettleCoverage::All)
    .publish(&index, &store)
    .await?;

// Consumer — either way, the same call.
let client = TreeClient::new(&session, (&store_p).into(), (&tree_p).into());
client
    .download_tree(&DownloadRequest::pinned("snap-1", index.root_hash), &dest, &cache)
    .await?;

// …or pull one file out of the snapshot without materializing the tree.
let conf = client
    .fetch_file(&DownloadRequest::pinned("snap-1", index.root_hash), "etc/app.conf", &cache)
    .await?;
```

*(Derived from `examples/tree_sync.rs` and `examples/durable_store.rs`.)*

Tier 2 also ships a **batched fetch** (`<store>/<algo>/batch` — one round for
many chunks instead of one query each), **probes** that report partial
possession (`StoreClient::probe`, `TreeClient::probe_snapshot`) so a client can
pick a holder before fetching, **index sharding** for snapshots whose index is
itself large, **seeding** (`seed::seed_store` satisfies chunks from prior local
copies and zero regions before touching the network), **lifecycle**
(`gc::sweep` mark-and-sweep with persistent snapshot tags and in-flight temp
tags), and a `DirStore` that is atomic, fsynced, optionally zstd-compressed and
optionally sealed with XChaCha20-Poly1305.

**Fanout tier — one-to-many rollout** (feature `fanout`). `fanout_file`
publishes a manifest and its bao slices through a `zenoh-ext`
`AdvancedPublisher`, so a late joiner replays the cache and every receiver
verifies each slice against the pinned root exactly as a downloader does. No
resume: an interrupted receiver starts over.


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
