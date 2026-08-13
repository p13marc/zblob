//! Zenoh key expressions for every plane this crate speaks, and the parsers
//! that read them back.
//!
//! These were seventeen free functions at the crate root, where they were the
//! bulk of what a reader saw first on docs.rs and outnumbered the types. They
//! are one subject and now one module.
//!
//! Build keys through these rather than with `format!`. It is not a style
//! rule: a slice reply whose key does not match its query is *silently
//! dropped* by Zenoh (`ReplyKeyExpr::MatchingQuery`), so a hand-built key
//! fails as a timeout with nothing to see. [`slice_selector`] is what makes
//! that impossible.

use crate::error::{BlobError, Result};
use crate::hash::{Hash, HashAlgo};

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
///
/// v3 dropped the `v=` parameter this used to carry. The wire version was
/// stated twice — here, and as the version-first field of every struct the
/// query's replies contain — and the selector copy cost a `String` allocation
/// per query to compare. Every `fetch_*` loop already filters on the reply's
/// `ENC_*` tag before decoding, so a foreign or stale peer stays diagnosable
/// without it.
pub fn slice_selector(prefix: &str, id: &str, ranges: &[std::ops::Range<u32>]) -> String {
    format!("{prefix}/{id}/**?ranges={}", format_ranges(ranges))
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

/// Parse and validate a slice-query parameter string (`ranges=<spec>`).
///
/// Enforced: spans well-formed (`a-b` half-open with `a < b`, or a bare
/// index), sorted, disjoint, within `chunk_count`; at most
/// [`MAX_RANGE_SPANS`] spans and `max_chunks` total chunks. Anything else is
/// an [`BlobError::InvalidRanges`] — a server must never let a remote peer
/// drive it into unbounded work from a malformed selector.
///
/// Unknown parameters are ignored rather than refused, so a later version can
/// add one without this rejecting the whole query.
pub fn parse_ranges(
    params: &str,
    chunk_count: u32,
    max_chunks: u32,
) -> Result<Vec<std::ops::Range<u32>>> {
    let mut spec: Option<&str> = None;
    for pair in params.split('&') {
        if let Some(r) = pair.strip_prefix("ranges=") {
            spec = Some(r);
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
/// Borrows from `key_expr` rather than allocating: this runs once per served
/// query, and the caller almost always just compares it or looks it up.
pub fn parse_id<'k>(prefix: &str, key_expr: &'k str) -> Option<&'k str> {
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
        Some(id)
    }
}

/// Key of a content-addressed chunk (Tier 2): `<prefix>/<algo>/<hex>`. Immutable,
/// so it is safe to cache fleet-wide.
pub fn store_key(prefix: &str, algo: HashAlgo, hash: &Hash) -> String {
    format!("{prefix}/{algo}/{hash}")
}

/// The part of a tier-2 key expression that follows the prefix.
///
/// The protocol defines no tail longer than two segments — a store key is
/// `<algo>/<hex|batch|have>` and a tree key is `<id>` or `<id>/have` — so this
/// is the whole shape space. It was a `Vec<&str>`, which allocated on every
/// served tier-2 query and admitted lengths that cannot occur, leaving each
/// call site to re-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier2Tail<'k> {
    /// One segment: a snapshot id, under a tree prefix.
    One(&'k str),
    /// Two segments: `<algo>/<hex|batch|have>` under a store prefix, or
    /// `<id>/have` under a tree prefix.
    Two(&'k str, &'k str),
}

/// Split a query key expression into the segments that follow `prefix`,
/// matching positionally so a wildcard origin still resolves (see
/// [`parse_id`], which does the same thing for tier 1).
///
/// `None` if the key does not sit under `prefix`, if a `**` in the prefix
/// region makes the split ambiguous, or if the tail is not one or two
/// segments.
#[must_use]
pub fn parse_tier2_tail<'k>(prefix: &str, key_expr: &'k str) -> Option<Tier2Tail<'k>> {
    let p: Vec<&str> = prefix.split('/').collect();
    let k: Vec<&str> = key_expr.split('/').collect();
    if k.len() <= p.len() {
        return None;
    }
    for (want, got) in p.iter().zip(k.iter()) {
        if got == &"**" {
            return None; // ambiguous span
        }
        if want != got && !got.contains('*') {
            return None;
        }
    }
    match k[p.len()..] {
        [a] => Some(Tier2Tail::One(a)),
        [a, b] => Some(Tier2Tail::Two(a, b)),
        _ => None,
    }
}

/// Reserved tier-2 endpoint token: the batched want-list fetch.
///
/// Unambiguous against a chunk key because `<hex>` is hex and this is not.
pub const STORE_BATCH: &str = "batch";

/// Reserved tier-2 endpoint token: the chunk probe (and, under a tree key,
/// the snapshot probe).
pub const STORE_HAVE: &str = "have";

/// Key a client GETs — with a [`WantList`](crate::wire::WantList) payload — to
/// fetch many chunks in one round: `<prefix>/<algo>/batch`.
///
/// Replies come back on the ordinary [`store_key`] of each chunk the holder
/// has, which keeps them individually verifiable, individually cacheable and
/// byte-identical to a single-chunk reply. Those keys do **not** intersect
/// this one, so the query must be issued with
/// `accept_replies(ReplyKeyExpr::Any)` — Zenoh otherwise refuses the reply on
/// the *server*, once per chunk.
///
/// Note what this endpoint is not: a wildcard. A GET on
/// `<prefix>/<algo>/**` would also carry those replies, and would make every
/// router-hosted storage in range dump its entire content store in answer to
/// one query.
pub fn store_batch_key(prefix: &str, algo: HashAlgo) -> String {
    format!("{prefix}/{algo}/{STORE_BATCH}")
}

/// Key a client GETs — with a [`WantList`](crate::wire::WantList) payload — to
/// ask which of those chunks a holder has: `<prefix>/<algo>/have`.
///
/// The reply is one bit per entry, so its size is a function of the question
/// rather than of the objects asked about. That is what makes this safe to
/// fan out across origins where a tier-2 *fetch* is not.
pub fn store_have_key(prefix: &str, algo: HashAlgo) -> String {
    format!("{prefix}/{algo}/{STORE_HAVE}")
}

/// Key a client GETs to ask a holder how much of snapshot `id` it has:
/// `<prefix>/<id>/have`.
pub fn tree_have_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}/{STORE_HAVE}")
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
            "p/A/**?ranges=0-5,9,12-20"
        );
    }

    #[test]
    fn parse_id_helper() {
        assert_eq!(parse_id("p", "p/A/**"), Some("A"));
        assert_eq!(parse_id("p", "p/A/manifest"), Some("A"));
        assert_eq!(parse_id("p", "p/**"), None);
        assert_eq!(parse_id("other", "p/A/**"), None);
        // Multi-segment prefixes align positionally.
        assert_eq!(
            parse_id(
                "v1/host-a/@blob/artifact",
                "v1/host-a/@blob/artifact/A/manifest"
            ),
            Some("A")
        );
        // A wildcard-origin probe must still resolve to this server's id: the
        // `*` stands in for the literal origin segment.
        assert_eq!(
            parse_id("v1/host-a/@blob/artifact", "v1/*/@blob/artifact/A/manifest"),
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
    fn tier2_tail_matches_positionally() {
        // The ordinary case.
        assert_eq!(
            parse_tier2_tail("v1/host-a/@blob/store", "v1/host-a/@blob/store/blake3/abc"),
            Some(Tier2Tail::Two("blake3", "abc"))
        );
        // A wildcard origin stands in for the literal segment — the whole
        // point: the client cannot name the origin, the server still must
        // recognise its own key.
        assert_eq!(
            parse_tier2_tail("v1/host-a/@blob/store", "v1/*/@blob/store/blake3/abc"),
            Some(Tier2Tail::Two("blake3", "abc"))
        );
        // Tree keys have a one-segment tail.
        assert_eq!(
            parse_tier2_tail("v1/host-a/@blob/tree", "v1/*/@blob/tree/deadbeef"),
            Some(Tier2Tail::One("deadbeef"))
        );
        // `**` spans an unknown number of segments, so the tail is ambiguous.
        assert_eq!(
            parse_tier2_tail("v1/host-a/@blob/store", "v1/**/blake3/abc"),
            None
        );
        // A non-matching literal segment is still a miss.
        assert_eq!(
            parse_tier2_tail("v1/host-a/@blob/store", "v1/host-b/@blob/store/blake3/abc"),
            None
        );
        // Nothing after the prefix.
        assert_eq!(parse_tier2_tail("p/q", "p/q"), None);
        // The protocol has no three-segment tail, so neither has the type.
        assert_eq!(parse_tier2_tail("p/q", "p/q/a/b/c"), None);
    }

    #[test]
    fn ranges_roundtrip() {
        let ranges = vec![0..5, 9..10, 12..20];
        let params = format!("ranges={}", format_ranges(&ranges));
        assert_eq!(parse_ranges(&params, 20, 512).unwrap(), ranges);
    }

    #[test]
    fn ranges_rejects_malformed() {
        let cases: &[(&str, u32, u32)] = &[
            ("", 10, 512),                  // missing ranges
            ("other=1", 10, 512),           // no ranges among the parameters
            ("ranges=", 10, 512),           // empty
            ("ranges=5-5", 10, 512),        // empty span
            ("ranges=6-2", 10, 512),        // inverted
            ("ranges=0-11", 10, 512),       // out of bounds
            ("ranges=3-6,5-8", 10, 512),    // overlap
            ("ranges=5-8,0-2", 10, 512),    // unsorted
            ("ranges=x", 10, 512),          // garbage
            ("ranges=0-9", 10, 4),          // over the chunk cap
            ("ranges=4294967295", 10, 512), // index overflow edge (oob too)
        ];
        for (params, count, cap) in cases {
            assert!(
                parse_ranges(params, *count, *cap).is_err(),
                "should reject {params:?}"
            );
        }
    }

    #[test]
    fn unknown_parameters_are_tolerated() {
        // A later version must be able to add a parameter without every
        // existing server rejecting the whole query.
        assert_eq!(
            parse_ranges("future=7&ranges=0-3&other=x", 10, 512).unwrap(),
            vec![0..3]
        );
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
            let _ = parse_ranges(&format!("ranges={s}"), 1000, 512);
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
            let rendered = format!("ranges={}", format_ranges(&ranges));
            let parsed = parse_ranges(&rendered, chunk_count, total)
                .map_err(|e| TestCaseError::fail(format!("rejected own output {rendered:?}: {e}")))?;
            prop_assert_eq!(parsed, ranges);
        }
    }
}
