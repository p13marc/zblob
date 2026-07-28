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
//! 2. **Reply keys must match the query.** Replies use `ReplyKeyExpr::MatchingQuery`
//!    by default, so the client **must** GET the wildcard `<prefix>/<id>/**` for
//!    the `slice/<i>` replies to be accepted. A bare-`<id>` GET would silently
//!    reject every slice. [`slice_selector`] enforces the wildcard.
//! 3. **Any peer can answer, so one bad reply must not be fatal.** A queryable
//!    key range is open: replies that fail decoding, validation, id matching,
//!    or root pinning are *skipped* rather than aborting the query, so a
//!    hostile or stale responder cannot deny a fetch that an honest replica
//!    still answers.

#![warn(missing_docs)]

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
mod manifest;
mod obs;
mod paths;
mod progress;
mod publish;
mod resume;
pub mod seed;
mod server;
mod store;
mod tree;
mod verify;
pub mod wire;

pub use cancel::CancelToken;
pub use chunk::{CdcParams, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, TransferChunks};
pub use client::{BlobClient, BlobClientBuilder, DownloadRequest, Overwrite, RetryPolicy};
pub use compress::ChunkCompression;
#[cfg(feature = "encryption")]
pub use crypt::StoreKey;
pub use error::{BlobError, Result};
pub use hash::{Hash, HashParseError};
pub use manifest::{BlobSpec, Manifest};
pub use obs::TransferStats;
pub use progress::{Progress, ProgressSink};
pub use publish::{publish_chunk, publish_index, publish_snapshot, publish_store};
pub use server::{
    BlobServer, BlobServerBuilder, BlobSource, ErrorCallback, FileBlobSource, MemoryBlobSource,
    PushPolicy, ReadAtSize, ServerHandle,
};
pub use store::{ContentStore, DirStore, MemoryStore};
pub use tree::{
    ChunkRef, Entry, TreeClient, TreeClientBuilder, TreeIndex, TreeServer, TreeServerBuilder,
    build_tree,
};
#[doc(no_inline)]
pub use zenoh::qos::Priority;

/// Key of the manifest reply for blob `id` under `prefix`.
pub fn manifest_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}/manifest")
}

/// Key of slice `index` (the bao-verified transfer chunk) for blob `id`.
pub fn slice_key(prefix: &str, id: &str, index: u32) -> String {
    format!("{prefix}/{id}/slice/{index}")
}

/// Key a client GETs to ask responders which chunks of `id` they hold.
pub fn availability_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}/have")
}

/// Key an uploader GETs (with a manifest payload) to offer a push of `id`.
pub fn push_offer_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}/push/offer")
}

/// Key an uploader GETs (with a bao-slice payload) to push chunk `index`.
pub fn push_slice_key(prefix: &str, id: &str, index: u32) -> String {
    format!("{prefix}/{id}/push/slice/{index}")
}

/// Selector a client GETs to fetch the given chunk-index ranges of blob `id`.
///
/// Always ends in the `/**` wildcard so the `slice/<i>` replies match the
/// query (see the crate docs, fact 2). `ranges` must be sorted and disjoint —
/// the resume bitfield's hole computation produces exactly that.
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

/// Extract the blob `id` from a query key expression seen by a server
/// declared on `<prefix>/**`.
///
/// Matched **positionally**, not by literal string stripping: a client may
/// legitimately query a *wildcard* prefix (e.g. `v1/*/@blob/artifact/<id>/…`
/// to ask every origin which one holds a blob), and the server still has to
/// recognise its own id segment in that query. Single-segment wildcards (`*`,
/// `$*…`) therefore match a literal prefix segment. A `**` inside the prefix
/// region is refused instead: it can span any number of segments, so the id's
/// position is genuinely ambiguous and guessing would mis-parse.
pub fn parse_id(prefix: &str, key_expr: &str) -> Option<String> {
    let p: Vec<&str> = prefix.split('/').collect();
    let k: Vec<&str> = key_expr.split('/').collect();
    if k.len() <= p.len() {
        return None;
    }
    for (want, got) in p.iter().zip(k.iter()) {
        if got == &"**" {
            return None; // ambiguous span: the id's position is unknowable
        }
        let single_wildcard = got.contains('*');
        if want != got && !single_wildcard {
            return None;
        }
    }
    let id = k[p.len()];
    if id.is_empty() || id.contains('*') {
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
mod range_properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Whatever `parse_ranges` accepts must satisfy every documented
        /// invariant — this is the contract the server relies on to bound its
        /// work, so it is asserted over generated input rather than examples.
        #[test]
        fn accepted_ranges_always_satisfy_the_contract(
            params in ".{0,120}",
            chunk_count in 0u32..5000,
            max_chunks in 1u32..1000,
        ) {
            if let Ok(ranges) = parse_ranges(&params, chunk_count, max_chunks) {
                prop_assert!(!ranges.is_empty());
                prop_assert!(ranges.len() <= MAX_RANGE_SPANS);
                let mut prev_end = 0u32;
                let mut total = 0u64;
                for r in &ranges {
                    prop_assert!(r.start < r.end, "empty/inverted span {r:?}");
                    prop_assert!(r.start >= prev_end, "unsorted or overlapping {r:?}");
                    prop_assert!(r.end <= chunk_count, "out of bounds {r:?}");
                    prev_end = r.end;
                    total += (r.end - r.start) as u64;
                }
                prop_assert!(total <= max_chunks as u64, "over the chunk cap");
            }
        }

        /// Arbitrary bytes in the parameter position must never panic — the
        /// server parses this straight off the wire.
        #[test]
        fn parse_ranges_never_panics(params in prop::collection::vec(any::<u8>(), 0..200)) {
            let s = String::from_utf8_lossy(&params);
            let _ = parse_ranges(&s, 1000, 512);
            let _ = parse_ranges(&format!("v=2&ranges={s}"), 1000, 512);
        }

        /// Any legal hole set the client can produce must survive the round
        /// trip through the selector grammar unchanged (this is what makes
        /// resume exact rather than approximate).
        #[test]
        fn hole_sets_round_trip(seed in prop::collection::vec((0u32..200, 1u32..20), 1..40)) {
            // Build sorted, disjoint, non-empty spans from the generated gaps.
            let mut ranges: Vec<std::ops::Range<u32>> = Vec::new();
            let mut cursor = 0u32;
            for (gap, len) in seed {
                let start = cursor + gap;
                let end = start + len;
                ranges.push(start..end);
                cursor = end;
            }
            ranges.truncate(MAX_RANGE_SPANS);
            let chunk_count = cursor + 1;
            let total: u32 = ranges.iter().map(|r| r.end - r.start).sum();
            let rendered = format!("v=2&ranges={}", format_ranges(&ranges));
            let parsed = parse_ranges(&rendered, chunk_count, total)
                .map_err(|e| TestCaseError::fail(format!("rejected own output {rendered:?}: {e}")))?;
            prop_assert_eq!(parsed, ranges);
        }
    }
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
        // Multi-segment prefixes align positionally.
        assert_eq!(
            parse_id(
                "v1/host-a/@blob/artifact",
                "v1/host-a/@blob/artifact/A/manifest"
            )
            .as_deref(),
            Some("A")
        );
        // A wildcard-origin probe must still resolve to this server's id: the
        // `*` stands in for the literal origin segment.
        assert_eq!(
            parse_id("v1/host-a/@blob/artifact", "v1/*/@blob/artifact/A/manifest").as_deref(),
            Some("A")
        );
        // …but a `**` in the prefix region is ambiguous and refused.
        assert_eq!(
            parse_id("v1/host-a/@blob/artifact", "v1/**/A/manifest"),
            None
        );
        // A non-matching literal segment still fails.
        assert_eq!(
            parse_id(
                "v1/host-a/@blob/artifact",
                "v1/host-b/@blob/artifact/A/manifest"
            ),
            None
        );
        // The id itself may never be a wildcard.
        assert_eq!(parse_id("p", "p/*/manifest"), None);
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
