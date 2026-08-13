//! `zblob` — generic resumable chunked blob and directory transfer over Zenoh.
//!
//! A small, self-contained library for moving a large artifact (a file, a
//! report bundle, a pcap, a directory tree) between Zenoh peers with
//! **progress**, **BLAKE3 verified streaming**, **range resume**, and
//! **bounded memory**. It carries no application-specific types.
//!
//! # The three tiers
//!
//! | Tier | What it moves | Entry points |
//! |---|---|---|
//! | **1 — blob by id** | one blob, named by a caller-chosen [`BlobId`] | [`BlobServer`], [`BlobClient`] |
//! | **2 — content-addressed trees** | a directory snapshot, deduplicated chunk-wise | [`TreeServer`], [`TreeClient`], [`StoreClient`], [`Publisher`] |
//! | **fanout** (feature `fanout`) | one-to-many rollout of one blob | [`fanout::fanout_file`], [`fanout::receive_fanout`] |
//!
//! Tier 1 is the whole of the model described below. **Tier 2** is the casync
//! model: a snapshot is a [`TreeIndex`] (a depth-first entry list whose files
//! reference their chunks by BLAKE3 hash) plus a [`ContentStore`] keyed
//! `<prefix>/blake3/<hex>`. A client fetches only the chunks it is *missing*,
//! in batched rounds, and materializes defensively — so an interrupted pull
//! resumes for free and identical chunks transfer once across files, versions
//! and producers. A producer can serve it live with a [`TreeServer`], or
//! [`Publisher`] it into a router-hosted Zenoh storage and exit (see
//! `docs/router-storage.md`). [`TreeClient::fetch_file`] pulls one path out of
//! a snapshot without materializing the tree, and the probes
//! ([`StoreClient::probe`], [`TreeClient::probe_snapshot`]) report *partial*
//! possession, so a client can choose a holder before fetching.
//!
//! Every key expression is built through [`keys`], and every prefix is typed
//! by the role it plays: a server owns a concrete [`ServePrefix`], a client
//! asks through a [`QueryPrefix`] that may name several origins. A server
//! cannot be built on a wildcard because there is no value to build one from.
//!
//! ```no_run
//! # use zblob::{BlobClient, BlobServer, BlobSpec, DownloadRequest, QueryPrefix, ServePrefix};
//! # async fn f(session: zenoh::Session, path: &std::path::Path, dest: &std::path::Path)
//! # -> zblob::Result<()> {
//! let serve = ServePrefix::new("demo/blobs")?;
//! let server = BlobServer::new(&session, serve.clone());
//! let manifest = server.register_file(BlobSpec::new("blob-1"), path).await?;
//! let handle = server.spawn().await?;
//!
//! // Transfers are call builders: what to fetch and where it goes are
//! // positional; progress, cancellation and overwrite policy are optional.
//! let client = BlobClient::new(&session, QueryPrefix::from(&serve));
//! let stats = client
//!     .download_to(&DownloadRequest::pinned("blob-1", manifest.root), dest)
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! # Model (wire v3)
//!
//! One queryable serves every blob under a key prefix:
//!
//! ```text
//! queryable on:   <prefix>/**
//! manifest GET:   <prefix>/<id>/manifest                -> the Manifest (one reply)
//! slice GET:      <prefix>/<id>/**?ranges=<spec>       -> bao slice replies (any order)
//! slice reply:    <prefix>/<id>/slice/<index>
//! ```
//!
//! A download is a manifest GET followed by range-set slice GETs. The
//! manifest-first step is not cosmetic — **Zenoh does not order query
//! replies** — and each slice reply is a *bao slice*: the chunk's bytes plus
//! the parent hashes proving them against the manifest's BLAKE3 `root`
//! (see [`crate::wire`] and the `verify` module). Every reply is therefore
//! independently verified before it touches disk, out of order, at 16 KiB
//! granularity; there is no end-of-transfer hash pass. Memory stays
//! O(chunk_size) regardless of blob size and arrival order.
//!
//! The `ranges` parameter is a comma-separated list of half-open chunk-index
//! spans (`"0-5,9,12-20"`; a bare `k` means `k..k+1`), which is how resume
//! works: the client persists a chunk bitfield next to the `.part` file and
//! re-queries exactly its holes. See [`keys::slice_selector`] / [`keys::parse_ranges`].
//!
//! # Three facts this design relies on
//!
//! 1. **Backpressure is automatic.** `Session::get` defaults to
//!    `CongestionControl::Block`, and replies inherit the query's congestion
//!    control, so chunk replies block (rather than drop) when the link backs up.
//!    We therefore set **no** congestion control explicitly on queries — the
//!    only setter is behind Zenoh's `internal` feature, which this crate
//!    deliberately does not enable. Do not "fix" this by enabling `internal`.
//!    (Reply *consolidation* is a different knob: clients set
//!    `ConsolidationMode::None` so replies stream instead of being buffered
//!    until query finalization. Publications — the `fanout` tier — are not
//!    queries and *do* set `Block` explicitly, since their default is `Drop`.)
//! 2. **Reply keys must *intersect* the query.** Replies use
//!    `ReplyKeyExpr::MatchingQuery` by default, so the client **must** GET the
//!    wildcard `<prefix>/<id>/**` for the `slice/<i>` replies to be accepted.
//!    [`keys::slice_selector`] enforces the wildcard. The failure is not silent and
//!    not on the client: a bare-`<id>` GET makes the *server's* `reply()` fail
//!    with a "does not intersect" error, so the diagnosis appears on the
//!    serving origin. (`accept_replies(ReplyKeyExpr::Any)` lifts the rule where
//!    a protocol genuinely needs disjoint reply keys.)
//! 3. **Silence is how a server says "not mine", and it is cheap.** A query
//!    finalizes once every matching queryable has completed, and completing
//!    without replying is immediate — an unknown id resolves in about a
//!    millisecond, not on the query timeout. That is what lets several servers
//!    share one prefix, each owning a disjoint set of ids.
//! 4. **Any peer can answer, so one bad reply must not be fatal.** A queryable
//!    key range is open: replies that fail decoding, validation, id matching,
//!    or root pinning are *skipped* rather than aborting the query, so a
//!    hostile or stale responder cannot deny a fetch that an honest replica
//!    still answers.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
// Builder methods that drop their result silently drop the setting with it.
#![warn(clippy::return_self_not_must_use)]

mod cancel;
mod chunk;
mod client;
mod compress;
#[cfg(feature = "encryption")]
mod crypt;
mod error;
#[cfg(feature = "fanout")]
pub mod fanout;
pub mod gc;
mod hash;
mod id;
pub mod keys;
mod manifest;
mod obs;
mod paths;
mod prefix;
mod progress;
mod publish;
mod resume;
pub mod seed;
mod server;
mod store;
mod store_client;
mod tree;
mod verify;
pub mod wire;

pub use cancel::CancelToken;
pub use chunk::{CdcParams, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, TransferChunks};
pub use client::{
    BlobClient, BlobClientBuilder, BlobProbe, Download, DownloadRequest, DownloadToWriter,
    Overwrite, RetryPolicy, Staged, StagedDownload, Upload,
};
pub use compress::ChunkCompression;
#[cfg(feature = "encryption")]
pub use crypt::StoreKey;
pub use error::{BlobError, ErrorKind, Result};
pub use hash::HashAlgo;
pub use hash::{Hash, HashParseError};
pub use id::BlobId;
pub use manifest::{BlobSpec, Manifest};
pub use obs::TransferStats;
pub use prefix::{QueryPrefix, ServePrefix};
pub use progress::{ChannelSink, Progress, ProgressSink, progress_channel};
pub use publish::{Publisher, SettleCoverage, SnapshotPublisher};
pub use server::{
    BlobServer, BlobServerBuilder, BlobSource, ErrorCallback, FileBlobSource, MemoryBlobSource,
    PushConfig, PushPolicy, ReadAtSize, ServerHandle, SourceFingerprint,
};
pub use store::{ContentStore, DirStore, MemoryStore};
pub use store_client::{ChunkProbe, StoreClient, StoreClientBuilder};
pub use tree::{
    ChunkRef, Entry, MaterializePolicy, TreeClient, TreeClientBuilder, TreeDownload, TreeIndex,
    TreeServer, TreeServerBuilder, build_tree, build_tree_from,
};
#[doc(no_inline)]
pub use zenoh::qos::Priority;

/// The supertraits of [`ReadAtSize`], re-exported so it can be implemented
/// downstream.
///
/// `ReadAtSize: ReadAt + Size` is public and blanket-implemented, but its
/// supertraits live in `bao_tree` — so a caller wanting a [`BlobSource`] over
/// something that is not a file or a `Vec` had to add `bao-tree` as a direct
/// dependency and match this crate's version of it exactly. That made a
/// documented extension point reachable only by accident.
#[doc(no_inline)]
pub use bao_tree::io::sync::{ReadAt, Size};

/// Decode a Tier-2 chunk container back to the chunk's raw bytes.
///
/// Chunk values on the wire and at rest are **self-describing containers** —
/// a tag byte, then the content (raw, or a compressed frame). Content
/// addressing is by the *uncompressed* bytes, so a receiver unframes first and
/// then verifies the hash against the key it asked for.
///
/// [`StoreClient`] does this for you. This is here for a caller that already
/// holds a container — one materialized from a router storage, say, or read
/// back from its own cache — and needs to get the content out of it. Without
/// it, such a caller cannot correctly decode a chunk it fetched by hand, which
/// is a strange thing for a crate that defines the framing to withhold.
///
/// **This does not verify anything.** Hash the result against the address it
/// came from; that check is what makes the tier trustworthy.
pub fn unframe_chunk(container: &[u8]) -> Result<Vec<u8>> {
    compress::unpack(container)
}

/// Frame raw chunk bytes into a Tier-2 container (the inverse of
/// [`unframe_chunk`]).
///
/// Note that a holder may re-frame a chunk however it likes without changing
/// the chunk's address — compression is not part of identity.
pub fn frame_chunk(bytes: &[u8], compression: ChunkCompression) -> Result<Vec<u8>> {
    compress::pack(bytes, compression)
}
