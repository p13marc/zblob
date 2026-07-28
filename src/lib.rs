//! `zblob` — generic resumable chunked blob transfer over Zenoh.
//!
//! A small, self-contained library for downloading a large artifact (a file, a
//! report bundle, a pcap) from one Zenoh peer to another with **progress**,
//! **BLAKE3 verified streaming**, **range resume**, and **bounded memory**. It
//! carries no application-specific types.
//!
//! # Model (wire v2)
//!
//! One queryable serves every blob under a key prefix:
//!
//! ```text
//! queryable on:   <prefix>/**
//! manifest GET:   <prefix>/<id>/manifest                -> the Manifest (one reply)
//! slice GET:      <prefix>/<id>/**?v=2&ranges=<spec>    -> bao slice replies (any order)
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
//! re-queries exactly its holes. See [`slice_selector`] / [`parse_ranges`].
//!
//! # Two Zenoh facts this design relies on
//!
//! 1. **Backpressure is automatic.** `Session::get` defaults to
//!    `CongestionControl::Block`, and replies inherit the query's congestion
//!    control, so chunk replies block (rather than drop) when the link backs up.
//!    We therefore set **no** congestion control explicitly — the only setter is
//!    behind Zenoh's `internal` feature, which this crate deliberately does not
//!    enable. Do not "fix" this by enabling `internal`. (Reply *consolidation*
//!    is a different knob: clients set `ConsolidationMode::None` so replies
//!    stream instead of being buffered until query finalization.)
//! 2. **Reply keys must match the query.** Replies use `ReplyKeyExpr::MatchingQuery`
//!    by default, so the client **must** GET the wildcard `<prefix>/<id>/**` for
//!    the `slice/<i>` replies to be accepted. A bare-`<id>` GET would silently
//!    reject every slice. [`slice_selector`] enforces the wildcard.

mod cancel;
mod chunk;
mod client;
mod error;
pub mod gc;
mod hash;
mod manifest;
mod paths;
mod progress;
mod publish;
mod resume;
mod server;
mod store;
mod tree;
mod verify;
pub mod wire;

pub use cancel::CancelToken;
pub use chunk::{CdcParams, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, TransferChunks};
pub use client::{BlobClient, BlobClientBuilder, DownloadRequest, Overwrite, RetryPolicy};
pub use error::{BlobError, Result};
pub use hash::{Hash, HashParseError};
pub use manifest::{BlobSpec, Manifest};
pub use progress::{Progress, ProgressSink};
pub use publish::{publish_chunk, publish_index, publish_snapshot, publish_store};
pub use server::{
    BlobServer, BlobServerBuilder, BlobSource, FileBlobSource, MemoryBlobSource, ReadAtSize,
    ServerHandle,
};
pub use store::{ContentStore, DirStore, MemoryStore};
pub use tree::{ChunkRef, Entry, TreeClient, TreeClientBuilder, TreeIndex, TreeServer, build_tree};

/// Key of the manifest reply for blob `id` under `prefix`.
pub fn manifest_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}/manifest")
}

/// Key of slice `index` (the bao-verified transfer chunk) for blob `id`.
pub fn slice_key(prefix: &str, id: &str, index: u32) -> String {
    format!("{prefix}/{id}/slice/{index}")
}

/// Selector a client GETs to fetch the given chunk-index ranges of blob `id`.
///
/// Always ends in the `/**` wildcard so the `slice/<i>` replies match the
/// query (see the crate docs, fact 2). `ranges` must be sorted and disjoint —
/// [`crate::resume`]'s hole computation produces exactly that.
pub fn slice_selector(prefix: &str, id: &str, ranges: &[std::ops::Range<u32>]) -> String {
    format!(
        "{prefix}/{id}/**?v={}&ranges={}",
        wire::WIRE_VERSION,
        format_ranges(ranges)
    )
}

/// Render sorted, disjoint chunk ranges as the `ranges` parameter value:
/// half-open spans `a-b`, single chunks as a bare index, comma-separated.
pub fn format_ranges(ranges: &[std::ops::Range<u32>]) -> String {
    let mut out = String::new();
    for r in ranges {
        if r.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push(',');
        }
        if r.end == r.start + 1 {
            out.push_str(&r.start.to_string());
        } else {
            out.push_str(&format!("{}-{}", r.start, r.end));
        }
    }
    out
}

/// Maximum number of spans a single `ranges` parameter may carry.
pub const MAX_RANGE_SPANS: usize = 128;

/// Parse and validate a slice-query parameter string (`v=2&ranges=<spec>`).
///
/// Enforced: the `v=2` version marker; spans well-formed (`a-b` half-open with
/// `a < b`, or a bare index), sorted, disjoint, within `chunk_count`; at most
/// [`MAX_RANGE_SPANS`] spans and `max_chunks` total chunks. Anything else is
/// an [`BlobError::InvalidRanges`] — a server must never let a remote peer
/// drive it into unbounded work from a malformed selector.
pub fn parse_ranges(
    params: &str,
    chunk_count: u32,
    max_chunks: u32,
) -> Result<Vec<std::ops::Range<u32>>> {
    let mut version: Option<&str> = None;
    let mut spec: Option<&str> = None;
    for pair in params.split('&') {
        if let Some(v) = pair.strip_prefix("v=") {
            version = Some(v);
        } else if let Some(r) = pair.strip_prefix("ranges=") {
            spec = Some(r);
        }
    }
    match version {
        Some(v) if v == wire::WIRE_VERSION.to_string() => {}
        other => {
            return Err(BlobError::InvalidRanges(format!(
                "missing or unsupported version marker: {other:?}"
            )));
        }
    }
    let Some(spec) = spec else {
        return Err(BlobError::InvalidRanges("missing ranges parameter".into()));
    };

    let mut out: Vec<std::ops::Range<u32>> = Vec::new();
    let mut total: u64 = 0;
    for span in spec.split(',') {
        if out.len() >= MAX_RANGE_SPANS {
            return Err(BlobError::InvalidRanges(format!(
                "more than {MAX_RANGE_SPANS} spans"
            )));
        }
        let (start, end) = match span.split_once('-') {
            Some((a, b)) => {
                let a: u32 = a
                    .parse()
                    .map_err(|_| BlobError::InvalidRanges(format!("bad span {span:?}")))?;
                let b: u32 = b
                    .parse()
                    .map_err(|_| BlobError::InvalidRanges(format!("bad span {span:?}")))?;
                (a, b)
            }
            None => {
                let k: u32 = span
                    .parse()
                    .map_err(|_| BlobError::InvalidRanges(format!("bad span {span:?}")))?;
                (
                    k,
                    k.checked_add(1)
                        .ok_or_else(|| BlobError::InvalidRanges("index overflow".into()))?,
                )
            }
        };
        if start >= end {
            return Err(BlobError::InvalidRanges(format!(
                "empty or inverted span {span:?}"
            )));
        }
        if end > chunk_count {
            return Err(BlobError::InvalidRanges(format!(
                "span {span:?} exceeds chunk count {chunk_count}"
            )));
        }
        if let Some(prev) = out.last()
            && start < prev.end
        {
            return Err(BlobError::InvalidRanges(
                "spans must be sorted and disjoint".into(),
            ));
        }
        total += (end - start) as u64;
        if total > max_chunks as u64 {
            return Err(BlobError::InvalidRanges(format!(
                "more than {max_chunks} chunks requested in one query"
            )));
        }
        out.push(start..end);
    }
    if out.is_empty() {
        return Err(BlobError::InvalidRanges("no spans".into()));
    }
    Ok(out)
}

/// Extract the blob `id` from a query key expression seen by a server declared on
/// `<prefix>/**`. The id is the single segment following the prefix.
pub fn parse_id(prefix: &str, key_expr: &str) -> Option<String> {
    let rest = key_expr.strip_prefix(prefix)?.strip_prefix('/')?;
    let id = rest.split('/').next()?;
    if id.is_empty() || id == "**" {
        None
    } else {
        Some(id.to_string())
    }
}

/// Key of a content-addressed chunk (Tier 2): `<prefix>/<algo>/<hex>`. Immutable,
/// so it is safe to cache fleet-wide.
pub fn store_key(prefix: &str, algo: &str, hash: &Hash) -> String {
    format!("{prefix}/{algo}/{hash}")
}

/// Key of a tree snapshot index (Tier 2): `<prefix>/<id>`.
pub fn tree_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}")
}

#[cfg(test)]
mod key_tests {
    use super::*;

    #[test]
    fn key_builders() {
        assert_eq!(
            manifest_key("v1/h-0011223344ff/@blob/artifact", "A"),
            "v1/h-0011223344ff/@blob/artifact/A/manifest"
        );
        assert_eq!(slice_key("p", "A", 7), "p/A/slice/7");
        assert_eq!(
            slice_selector("p", "A", &[0..5, 9..10, 12..20]),
            "p/A/**?v=2&ranges=0-5,9,12-20"
        );
    }

    #[test]
    fn parse_id_helper() {
        assert_eq!(parse_id("p", "p/A/**").as_deref(), Some("A"));
        assert_eq!(parse_id("p", "p/A/manifest").as_deref(), Some("A"));
        assert_eq!(parse_id("p", "p/**"), None);
        assert_eq!(parse_id("other", "p/A/**"), None);
    }

    #[test]
    fn ranges_roundtrip() {
        let ranges = vec![0..5, 9..10, 12..20];
        let params = format!("v=2&ranges={}", format_ranges(&ranges));
        assert_eq!(parse_ranges(&params, 20, 512).unwrap(), ranges);
    }

    #[test]
    fn ranges_rejects_malformed() {
        let cases: &[(&str, u32, u32)] = &[
            ("ranges=0-5", 10, 512),            // missing v=2
            ("v=1&ranges=0-5", 10, 512),        // wrong version
            ("v=2", 10, 512),                   // missing ranges
            ("v=2&ranges=", 10, 512),           // empty
            ("v=2&ranges=5-5", 10, 512),        // empty span
            ("v=2&ranges=6-2", 10, 512),        // inverted
            ("v=2&ranges=0-11", 10, 512),       // out of bounds
            ("v=2&ranges=3-6,5-8", 10, 512),    // overlap
            ("v=2&ranges=5-8,0-2", 10, 512),    // unsorted
            ("v=2&ranges=x", 10, 512),          // garbage
            ("v=2&ranges=0-9", 10, 4),          // over the chunk cap
            ("v=2&ranges=4294967295", 10, 512), // index overflow edge (oob too)
        ];
        for (params, count, cap) in cases {
            assert!(
                parse_ranges(params, *count, *cap).is_err(),
                "should reject {params:?}"
            );
        }
    }

    #[test]
    fn ranges_span_cap_enforced() {
        // 129 disjoint single-chunk spans → rejected.
        let spec = (0..258u32)
            .step_by(2)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse_ranges(&format!("v=2&ranges={spec}"), 1000, 512).is_err());
    }
}
