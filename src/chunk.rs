//! Chunking policy.
//!
//! Two unrelated kinds of "chunk" exist in this crate, and v2 keeps them apart
//! by construction (they were one trait in v1, which let a content-defined
//! chunker be handed to the offset-addressed Tier-1 protocol and silently
//! produce a broken manifest):
//!
//! - **Transfer chunks** ([`TransferChunks`]) — Tier 1's fixed-size wire unit.
//!   Pure arithmetic over `(chunk_size, total_len)`; the client addresses
//!   chunks by index without seeing the bytes, so the size *must* be constant
//!   and agreed via the manifest. Sizes are **validated, never clamped**: a
//!   peer whose manifest carries an out-of-range size gets an error, not a
//!   silently different geometry (v1's clamp made peers agree with each other
//!   but disagree with the manifest, wedging the transfer forever).
//! - **Content-defined chunks** ([`Chunker::split`]) — Tier 2's dedup unit,
//!   where boundaries are derived from the data itself (FastCDC) so edits
//!   re-chunk only their neighborhood.

use std::ops::Range;

use crate::error::{BlobError, Result};
use crate::verify::GROUP_SIZE;

/// Smallest allowed transfer chunk size (64 KiB).
pub const MIN_CHUNK_SIZE: u32 = 64 * 1024;
/// Largest allowed transfer chunk size (4 MiB) — bounds per-reply RAM.
pub const MAX_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
/// Default transfer chunk size (512 KiB = 32 bao verification groups).
pub const DEFAULT_CHUNK_SIZE: u32 = 512 * 1024;

/// Fixed-size transfer-chunk arithmetic for one blob: `(chunk_size, total_len)`
/// fully determine every chunk's index, byte range, and count.
///
/// Constructed via [`TransferChunks::new`], which **validates** the size:
/// it must be a multiple of the 16 KiB bao group (so every chunk maps to a
/// whole range of verification groups) and lie in
/// [`MIN_CHUNK_SIZE`]..=[`MAX_CHUNK_SIZE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferChunks {
    chunk_size: u32,
    total_len: u64,
}

impl TransferChunks {
    /// Validate `chunk_size` alone (range + 16 KiB alignment).
    pub fn validate_chunk_size(chunk_size: u32) -> Result<()> {
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(BlobError::InvalidManifest(format!(
                "chunk_size {chunk_size} outside [{MIN_CHUNK_SIZE}, {MAX_CHUNK_SIZE}]"
            )));
        }
        if !(chunk_size as u64).is_multiple_of(GROUP_SIZE) {
            return Err(BlobError::InvalidManifest(format!(
                "chunk_size {chunk_size} is not a multiple of the {GROUP_SIZE}-byte bao group"
            )));
        }
        Ok(())
    }

    /// Build the chunk geometry for a blob, validating `chunk_size`.
    pub fn new(chunk_size: u32, total_len: u64) -> Result<Self> {
        Self::validate_chunk_size(chunk_size)?;
        Ok(TransferChunks {
            chunk_size,
            total_len,
        })
    }

    /// The transfer chunk size in bytes.
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Total blob length in bytes.
    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// Number of chunks (`ceil(total_len / chunk_size)`; 0 for an empty blob).
    pub fn count(&self) -> u32 {
        self.total_len.div_ceil(self.chunk_size as u64) as u32
    }

    /// Byte range `[start, end)` of chunk `index` (the final chunk may be
    /// short). Empty range if `index` is past the end.
    pub fn byte_range(&self, index: u32) -> Range<u64> {
        let start = (index as u64 * self.chunk_size as u64).min(self.total_len);
        let end = (start + self.chunk_size as u64).min(self.total_len);
        start..end
    }

    /// Length in bytes of chunk `index` (0 if past the end).
    pub fn len_of(&self, index: u32) -> u32 {
        let r = self.byte_range(index);
        (r.end - r.start) as u32
    }
}

/// A chunk boundary policy. Tier 1 is fixed-size; the trait leaves room for
/// content-defined chunking later.
pub trait Chunker: Send + Sync {
    /// The (nominal) chunk size in bytes.
    fn chunk_size(&self) -> u32;

    /// Byte offset of chunk `index` (fixed-size: `index * chunk_size`).
    fn offset(&self, index: u32) -> u64 {
        index as u64 * self.chunk_size() as u64
    }

    /// Number of chunks needed for a blob of `total_len` bytes.
    fn count(&self, total_len: u64) -> u32 {
        if total_len == 0 {
            return 0;
        }
        let size = self.chunk_size() as u64;
        total_len.div_ceil(size) as u32
    }

    /// Length of chunk `index` for a blob of `total_len` bytes (the final chunk
    /// may be short).
    fn chunk_len(&self, index: u32, total_len: u64) -> u32 {
        let start = self.offset(index);
        if start >= total_len {
            return 0;
        }
        let remaining = total_len - start;
        remaining.min(self.chunk_size() as u64) as u32
    }

    /// Split `data` into chunk boundaries `(offset, len)`.
    ///
    /// This is the primitive Tier-2 ([`crate::TreeIndex`] building) uses, so a
    /// content-defined chunker can cut at data-derived boundaries. The default is
    /// fixed-size slicing derived from [`offset`](Self::offset) /
    /// [`count`](Self::count) — so a fixed-size chunker needs no override and stays
    /// usable by the offset-addressed Tier-1 protocol too.
    fn split(&self, data: &[u8]) -> Vec<(usize, usize)> {
        let total = data.len() as u64;
        (0..self.count(total))
            .map(|i| (self.offset(i) as usize, self.chunk_len(i, total) as usize))
            .collect()
    }

    /// A short, self-describing tag for this chunking policy, recorded in the tree
    /// index (e.g. `"fixed-524288"`, `"fastcdc-262144"`).
    fn policy_tag(&self) -> String {
        format!("fixed-{}", self.chunk_size())
    }
}

/// Constant-size chunker (Tier-2 use; Tier 1 uses [`TransferChunks`]).
#[derive(Debug, Clone, Copy)]
pub struct FixedSizeChunker {
    size: u32,
}

impl Default for FixedSizeChunker {
    fn default() -> Self {
        FixedSizeChunker {
            size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl FixedSizeChunker {
    /// Build a fixed-size chunker; `bytes` is clamped to the allowed range.
    pub fn new(bytes: u32) -> Self {
        FixedSizeChunker {
            size: bytes.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE),
        }
    }
}

impl Chunker for FixedSizeChunker {
    fn chunk_size(&self) -> u32 {
        self.size
    }
}

/// Content-defined chunker (FastCDC, #200). Cut points are derived from a rolling
/// gear-hash of the data, so inserting/removing bytes only re-chunks the
/// neighborhood of the edit — chunks before and after a change keep their hashes
/// and dedup across versions. **Tier-2 only**: the cut points depend on the data,
/// so the offset-addressed Tier-1 protocol (which the client must address by
/// `index * chunk_size` without seeing the bytes) cannot use it.
#[derive(Debug, Clone, Copy)]
pub struct FastCdcChunker {
    min: u32,
    avg: u32,
    max: u32,
}

impl FastCdcChunker {
    /// Build a FastCDC chunker around an average chunk size, with `min = avg/4`
    /// and `max = avg*4` (the conventional FastCDC spread). `avg` is floored at
    /// 256 bytes so `min` stays above FastCDC's hard floor of 64.
    pub fn new(avg: u32) -> Self {
        let avg = avg.max(256);
        FastCdcChunker {
            min: avg / 4,
            avg,
            max: avg.saturating_mul(4),
        }
    }
}

impl Chunker for FastCdcChunker {
    fn chunk_size(&self) -> u32 {
        self.avg
    }

    fn split(&self, data: &[u8]) -> Vec<(usize, usize)> {
        if data.is_empty() {
            return Vec::new();
        }
        fastcdc::v2020::FastCDC::new(
            data,
            self.min as usize,
            self.avg as usize,
            self.max as usize,
        )
        .map(|c| (c.offset, c.length))
        .collect()
    }

    fn policy_tag(&self) -> String {
        format!("fastcdc-{}", self.avg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_chunk_size_validated_never_clamped() {
        // In range + group-aligned.
        assert!(TransferChunks::new(DEFAULT_CHUNK_SIZE, 1).is_ok());
        assert!(TransferChunks::new(MIN_CHUNK_SIZE, 1).is_ok());
        assert!(TransferChunks::new(MAX_CHUNK_SIZE, 1).is_ok());
        // Out of range → error, not clamp (v1's clamp wedged transfers, H2).
        assert!(TransferChunks::new(MIN_CHUNK_SIZE - 1, 1).is_err());
        assert!(TransferChunks::new(MAX_CHUNK_SIZE + 16 * 1024, 1).is_err());
        assert!(TransferChunks::new(0, 1).is_err());
        // In range but not a multiple of the 16 KiB bao group → error.
        assert!(TransferChunks::new(MIN_CHUNK_SIZE + 1, 1).is_err());
    }

    #[test]
    fn transfer_chunk_geometry() {
        let size = DEFAULT_CHUNK_SIZE as u64;
        // 3.5 chunks worth → 4 chunks, last one half-size.
        let total = size * 3 + size / 2;
        let c = TransferChunks::new(DEFAULT_CHUNK_SIZE, total).unwrap();
        assert_eq!(c.count(), 4);
        assert_eq!(c.byte_range(0), 0..size);
        assert_eq!(c.byte_range(3), size * 3..total);
        assert_eq!(c.len_of(3) as u64, size / 2);
        assert_eq!(c.len_of(4), 0); // past the end
        assert_eq!(c.byte_range(9), total..total);

        // Empty blob → zero chunks.
        let empty = TransferChunks::new(DEFAULT_CHUNK_SIZE, 0).unwrap();
        assert_eq!(empty.count(), 0);

        // Exact multiple → no short tail.
        let exact = TransferChunks::new(DEFAULT_CHUNK_SIZE, size * 2).unwrap();
        assert_eq!(exact.count(), 2);
        assert_eq!(exact.len_of(1), DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn fixed_chunker_clamps() {
        assert_eq!(FixedSizeChunker::new(1).chunk_size(), MIN_CHUNK_SIZE);
        assert_eq!(FixedSizeChunker::new(u32::MAX).chunk_size(), MAX_CHUNK_SIZE);
    }

    #[test]
    fn count_and_lengths() {
        let c = FixedSizeChunker::new(MIN_CHUNK_SIZE);
        let size = MIN_CHUNK_SIZE as u64;

        assert_eq!(c.count(0), 0);
        assert_eq!(c.count(1), 1);
        assert_eq!(c.count(size), 1);
        assert_eq!(c.count(size + 1), 2);

        // 3.5 chunks worth → 4 chunks, last one half-size.
        let total = size * 3 + size / 2;
        assert_eq!(c.count(total), 4);
        assert_eq!(c.chunk_len(0, total), MIN_CHUNK_SIZE);
        assert_eq!(c.chunk_len(2, total), MIN_CHUNK_SIZE);
        assert_eq!(c.chunk_len(3, total) as u64, size / 2);
        assert_eq!(c.chunk_len(4, total), 0); // past the end
    }

    #[test]
    fn offsets() {
        let c = FixedSizeChunker::new(MAX_CHUNK_SIZE);
        assert_eq!(c.offset(0), 0);
        assert_eq!(c.offset(3), 3 * MAX_CHUNK_SIZE as u64);
    }

    #[test]
    fn fixed_split_matches_offset_arithmetic() {
        let c = FixedSizeChunker::new(MIN_CHUNK_SIZE);
        let total = MIN_CHUNK_SIZE as usize * 3 + 1000;
        let data = vec![7u8; total];
        let cuts = c.split(&data);
        assert_eq!(cuts.len(), 4);
        assert_eq!(cuts[0], (0, MIN_CHUNK_SIZE as usize));
        assert_eq!(cuts[3], (MIN_CHUNK_SIZE as usize * 3, 1000));
        // Cuts tile the whole input with no gaps or overlaps.
        let covered: usize = cuts.iter().map(|(_, l)| l).sum();
        assert_eq!(covered, total);
    }

    #[test]
    fn policy_tags() {
        assert_eq!(
            FixedSizeChunker::new(DEFAULT_CHUNK_SIZE).policy_tag(),
            format!("fixed-{DEFAULT_CHUNK_SIZE}")
        );
        assert_eq!(FastCdcChunker::new(262_144).policy_tag(), "fastcdc-262144");
    }

    /// Deterministic pseudo-random bytes (xorshift64; no `rand` dep).
    fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut x = seed | 1;
        for _ in 0..len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push((x & 0xff) as u8);
        }
        out
    }

    fn sha(data: &[u8]) -> crate::hash::Hash {
        crate::hash::Hash::of(data)
    }

    /// FastCDC chunk-hash set over `data`.
    fn cdc_hashes(
        chunker: &FastCdcChunker,
        data: &[u8],
    ) -> std::collections::HashSet<crate::hash::Hash> {
        chunker
            .split(data)
            .into_iter()
            .map(|(o, l)| sha(&data[o..o + l]))
            .collect()
    }

    /// Fixed N-byte tiling chunk-hash set (the `FixedSizeChunker` clamps to 256 KiB,
    /// too coarse for this small fixture, so tile directly for the comparison).
    fn fixed_hashes(data: &[u8], size: usize) -> std::collections::HashSet<crate::hash::Hash> {
        data.chunks(size).map(sha).collect()
    }

    /// Inserting a few bytes near the front of a file should leave most FastCDC
    /// chunks unchanged (content-defined boundaries re-sync), whereas fixed-size
    /// chunking shifts every following boundary and changes almost everything.
    #[test]
    fn fastcdc_localizes_edits_far_better_than_fixed() {
        let base = pseudo_random(200_000, 0xC0FFEE);
        // Insert 50 bytes after a 4 KiB prefix.
        let mut edited = base.clone();
        edited.splice(4096..4096, pseudo_random(50, 0xBEEF));

        let cdc = FastCdcChunker::new(8192);
        let cdc_a = cdc_hashes(&cdc, &base);
        let cdc_b = cdc_hashes(&cdc, &edited);
        let cdc_ratio = cdc_a.intersection(&cdc_b).count() as f64 / cdc_a.len() as f64;

        let fix_a = fixed_hashes(&base, 8192);
        let fix_b = fixed_hashes(&edited, 8192);
        let fix_ratio = fix_a.intersection(&fix_b).count() as f64 / fix_a.len() as f64;

        // FastCDC keeps the vast majority of chunks; fixed-size loses most.
        assert!(
            cdc_ratio > 0.8,
            "FastCDC should retain >80% of chunks, got {cdc_ratio:.2}"
        );
        assert!(
            cdc_ratio > fix_ratio + 0.3,
            "FastCDC ({cdc_ratio:.2}) should dedup far better than fixed ({fix_ratio:.2})"
        );
    }
}
