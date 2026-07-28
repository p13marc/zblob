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
//! - **Content-defined chunks** ([`CdcParams`]) — Tier 2's dedup unit, where
//!   FastCDC derives boundaries from the data itself so edits re-chunk only
//!   their neighborhood. The parameters (including the gear seed) are recorded
//!   in the [`crate::TreeIndex`] so every producer into a store cuts identical
//!   boundaries.

use std::io::Read;
use std::ops::Range;

use serde::{Deserialize, Serialize};

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

/// FastCDC (v2020) content-defined chunking parameters for Tier 2.
///
/// Recorded verbatim in the [`crate::TreeIndex`]: chunk boundaries are a
/// function of these parameters *and the data*, so all producers into one
/// store must share them for cross-producer dedup to work.
///
/// `gear_seed` perturbs the gear table ([restic] seeds per-repo for the same
/// reason): with the public table (`0`), chunk boundaries — and therefore
/// chunk sizes — are predictable from content, which makes content-addressed
/// stores a dedup side-channel across trust domains. Give each private store
/// its own random seed. The seed is visible to anyone who can read the index
/// (same trust domain as the data itself).
///
/// [restic]: https://restic.net
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcParams {
    /// Minimum chunk size in bytes (FastCDC floor: 64).
    pub min: u32,
    /// Target average chunk size in bytes.
    pub avg: u32,
    /// Maximum chunk size in bytes.
    pub max: u32,
    /// FastCDC normalization level (0–3; 2 keeps most chunks near `avg`).
    pub normalization: u8,
    /// Gear-table seed (`0` = the public table).
    pub gear_seed: u64,
}

impl Default for CdcParams {
    /// The casync-style transfer class: 16 KiB / 64 KiB / 256 KiB, level 2,
    /// public gear table.
    fn default() -> Self {
        CdcParams {
            min: 16 * 1024,
            avg: 64 * 1024,
            max: 256 * 1024,
            normalization: 2,
            gear_seed: 0,
        }
    }
}

impl CdcParams {
    /// The defaults with a per-store gear seed (see the type docs).
    pub fn with_seed(gear_seed: u64) -> Self {
        CdcParams {
            gear_seed,
            ..Default::default()
        }
    }

    /// Validate the parameter ranges (fastcdc's own bounds, plus a 16 MiB
    /// ceiling so a hostile index cannot demand giant allocations).
    pub fn validate(&self) -> Result<()> {
        let ok = self.min >= 64
            && self.min <= self.avg
            && self.avg <= self.max
            && self.avg >= 256
            && self.max <= 16 * 1024 * 1024
            && self.normalization <= 3;
        if ok {
            Ok(())
        } else {
            Err(BlobError::InvalidManifest(format!(
                "invalid CDC parameters {self:?}"
            )))
        }
    }

    fn level(&self) -> fastcdc::v2020::Normalization {
        match self.normalization {
            0 => fastcdc::v2020::Normalization::Level0,
            1 => fastcdc::v2020::Normalization::Level1,
            3 => fastcdc::v2020::Normalization::Level3,
            _ => fastcdc::v2020::Normalization::Level2,
        }
    }

    /// Stream `reader` into content-defined chunks. Memory stays
    /// O(`max` chunk size) no matter the input length.
    pub fn chunk_reader<R: Read>(
        &self,
        reader: R,
    ) -> impl Iterator<Item = std::io::Result<Vec<u8>>> {
        fastcdc::v2020::StreamCDC::with_level_and_seed(
            reader,
            self.min as usize,
            self.avg as usize,
            self.max as usize,
            self.level(),
            self.gear_seed,
        )
        .map(|item| match item {
            Ok(chunk) => Ok(chunk.data),
            Err(fastcdc::v2020::Error::IoError(e)) => Err(e),
            Err(other) => Err(std::io::Error::other(format!("fastcdc: {other:?}"))),
        })
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    /// Generate a valid transfer chunk size (16 KiB-aligned, in bounds).
    fn valid_chunk_size() -> impl Strategy<Value = u32> {
        (MIN_CHUNK_SIZE / (16 * 1024)..=MAX_CHUNK_SIZE / (16 * 1024)).prop_map(|k| k * 16 * 1024)
    }

    proptest! {
        /// The chunk grid must tile the blob exactly: contiguous, gapless,
        /// non-overlapping, summing to `total_len`. Every offset the client
        /// writes to and every range the server reads from comes from here.
        #[test]
        fn chunks_tile_the_blob_exactly(
            chunk_size in valid_chunk_size(),
            total_len in 0u64..(64 * 1024 * 1024),
        ) {
            let c = TransferChunks::new(chunk_size, total_len).unwrap();
            let count = c.count();
            let mut cursor = 0u64;
            let mut summed = 0u64;
            for i in 0..count {
                let r = c.byte_range(i);
                prop_assert_eq!(r.start, cursor, "gap or overlap at chunk {}", i);
                prop_assert!(r.end > r.start, "empty chunk {} within count", i);
                prop_assert_eq!(c.len_of(i) as u64, r.end - r.start);
                prop_assert!(c.len_of(i) <= chunk_size);
                cursor = r.end;
                summed += c.len_of(i) as u64;
            }
            prop_assert_eq!(cursor, total_len, "tiling does not reach the end");
            prop_assert_eq!(summed, total_len);
            // Past the end is empty, never a panic or a wild range.
            prop_assert_eq!(c.len_of(count), 0);
            prop_assert_eq!(c.len_of(u32::MAX), 0);
        }

        /// Chunk boundaries must be 16 KiB-group aligned, or a bao slice
        /// cannot cover a chunk exactly (the whole verification model
        /// depends on this).
        #[test]
        fn chunk_starts_are_group_aligned(
            chunk_size in valid_chunk_size(),
            total_len in 0u64..(16 * 1024 * 1024),
            index in 0u32..64,
        ) {
            let c = TransferChunks::new(chunk_size, total_len).unwrap();
            let start = c.byte_range(index).start;
            prop_assert!(
                start.is_multiple_of(crate::verify::GROUP_SIZE) || start == total_len,
                "chunk {} starts at {} which is not group-aligned", index, start
            );
        }

        /// Validation is total: no input panics, and acceptance implies the
        /// documented rules hold.
        #[test]
        fn chunk_size_validation_is_total(size in any::<u32>()) {
            match TransferChunks::validate_chunk_size(size) {
                Ok(()) => {
                    prop_assert!((MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&size));
                    prop_assert!((size as u64).is_multiple_of(crate::verify::GROUP_SIZE));
                }
                Err(_) => {
                    prop_assert!(
                        !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&size)
                            || !(size as u64).is_multiple_of(crate::verify::GROUP_SIZE)
                    );
                }
            }
        }

        /// CDC chunking must reproduce the input exactly, whatever the
        /// parameters or data — a lost or duplicated byte here silently
        /// corrupts every tree built with it.
        #[test]
        fn cdc_chunking_is_lossless(
            data in prop::collection::vec(any::<u8>(), 0..70_000),
            seed in any::<u64>(),
        ) {
            let params = CdcParams { min: 2048, avg: 8192, max: 32768, normalization: 2, gear_seed: seed };
            let mut rebuilt = Vec::new();
            for chunk in params.chunk_reader(std::io::Cursor::new(&data)) {
                let bytes = chunk.map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert!(!bytes.is_empty(), "CDC must not emit empty chunks");
                prop_assert!(bytes.len() <= params.max as usize, "chunk exceeds declared max");
                rebuilt.extend_from_slice(&bytes);
            }
            prop_assert_eq!(rebuilt, data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;

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
    fn cdc_params_validation() {
        assert!(CdcParams::default().validate().is_ok());
        assert!(CdcParams::with_seed(42).validate().is_ok());
        let bad = [
            CdcParams {
                min: 32,
                ..Default::default()
            },
            CdcParams {
                min: 1 << 20,
                avg: 1 << 18,
                ..Default::default()
            },
            CdcParams {
                max: 32 * 1024 * 1024,
                ..Default::default()
            },
            CdcParams {
                normalization: 9,
                ..Default::default()
            },
        ];
        for p in bad {
            assert!(p.validate().is_err(), "{p:?}");
        }
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

    fn small_cdc(seed: u64) -> CdcParams {
        CdcParams {
            min: 2048,
            avg: 8192,
            max: 32768,
            normalization: 2,
            gear_seed: seed,
        }
    }

    fn cdc_hashes(params: &CdcParams, data: &[u8]) -> std::collections::HashSet<Hash> {
        params
            .chunk_reader(std::io::Cursor::new(data))
            .map(|c| Hash::of(&c.unwrap()))
            .collect()
    }

    #[test]
    fn chunks_tile_the_input() {
        let data = pseudo_random(100_000, 7);
        let rebuilt: Vec<u8> = small_cdc(0)
            .chunk_reader(std::io::Cursor::new(&data))
            .flat_map(|c| c.unwrap())
            .collect();
        assert_eq!(rebuilt, data);
        // Empty input → no chunks.
        assert_eq!(
            small_cdc(0)
                .chunk_reader(std::io::Cursor::new(&[] as &[u8]))
                .count(),
            0
        );
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

        let cdc = small_cdc(0);
        let cdc_a = cdc_hashes(&cdc, &base);
        let cdc_b = cdc_hashes(&cdc, &edited);
        let cdc_ratio = cdc_a.intersection(&cdc_b).count() as f64 / cdc_a.len() as f64;

        let fix_a: std::collections::HashSet<Hash> = base.chunks(8192).map(Hash::of).collect();
        let fix_b: std::collections::HashSet<Hash> = edited.chunks(8192).map(Hash::of).collect();
        let fix_ratio = fix_a.intersection(&fix_b).count() as f64 / fix_a.len() as f64;

        assert!(
            cdc_ratio > 0.8,
            "FastCDC should retain >80% of chunks, got {cdc_ratio:.2}"
        );
        assert!(
            cdc_ratio > fix_ratio + 0.3,
            "FastCDC ({cdc_ratio:.2}) should dedup far better than fixed ({fix_ratio:.2})"
        );
    }

    /// Different gear seeds must cut different boundaries (the dedup
    /// side-channel mitigation), while each seed remains self-consistent.
    #[test]
    fn gear_seed_changes_boundaries() {
        let data = pseudo_random(200_000, 0x5EED);
        let a = cdc_hashes(&small_cdc(0), &data);
        let b = cdc_hashes(&small_cdc(0xDEADBEEF), &data);
        let a2 = cdc_hashes(&small_cdc(0), &data);
        assert_eq!(a, a2, "same seed → identical chunking");
        let overlap = a.intersection(&b).count() as f64 / a.len().max(1) as f64;
        assert!(
            overlap < 0.5,
            "different seeds should share few chunks, got {overlap:.2}"
        );
    }
}
