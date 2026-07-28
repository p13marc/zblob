//! Per-chunk compression framing (the restic v2 model).
//!
//! Every Tier-2 chunk *container* — on the wire and inside a compressing
//! [`DirStore`](crate::DirStore) — is self-describing:
//!
//! ```text
//! [0x00][raw bytes…]                                  uncompressed
//! [0x01][u32-le uncompressed len][zstd frame…]        zstd-compressed
//! ```
//!
//! Chunks stay content-addressed by their **uncompressed** BLAKE3 hash, so
//! compression never changes identity, dedup, or verification — a receiver
//! unpacks first, then re-hashes. Incompressible data (already-compressed
//! media, random bytes) is stored raw: if zstd doesn't shrink a chunk, the
//! packer bails out to `0x00`.
//!
//! Compression requires the `zstd` cargo feature; *de*framing raw chunks does
//! not, and a build without the feature rejects `0x01` frames with a clear
//! error instead of garbage.

use crate::error::{BlobError, Result};

/// Why a chunk container could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContainerError {
    /// The frame uses a format this build cannot read (missing cargo feature
    /// or, at rest, a sealed chunk without the store key). Not corruption —
    /// the bytes must be left alone.
    Unsupported,
    /// The frame is malformed or fails to decode: at-rest corruption.
    Corrupt,
}

/// Compression policy for produced chunk containers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChunkCompression {
    /// Store/send chunks raw (`0x00` frames). The default.
    #[default]
    None,
    /// zstd at the given level (1–19; 3 is a good default), bailing out to raw
    /// for incompressible chunks. Requires the `zstd` cargo feature.
    Zstd {
        /// zstd compression level.
        level: i32,
    },
}

pub(crate) const TAG_RAW: u8 = 0x00;
pub(crate) const TAG_ZSTD: u8 = 0x01;
/// XChaCha20-Poly1305 sealed container (at rest only; see `crate::crypt`).
pub(crate) const TAG_SEALED: u8 = 0x02;

/// Largest uncompressed chunk a frame may declare (matches the CDC `max` cap).
const MAX_UNPACKED: usize = 16 * 1024 * 1024;

/// Frame `bytes` according to `compression`.
pub(crate) fn pack(bytes: &[u8], compression: ChunkCompression) -> Result<Vec<u8>> {
    match compression {
        ChunkCompression::None => {
            let mut out = Vec::with_capacity(1 + bytes.len());
            out.push(TAG_RAW);
            out.extend_from_slice(bytes);
            Ok(out)
        }
        #[cfg(feature = "zstd")]
        ChunkCompression::Zstd { level } => {
            let compressed = zstd::bulk::compress(bytes, level)?;
            if compressed.len() + 5 > bytes.len() {
                // Incompressible: raw is smaller — bail out.
                return pack(bytes, ChunkCompression::None);
            }
            let mut out = Vec::with_capacity(5 + compressed.len());
            out.push(TAG_ZSTD);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
            Ok(out)
        }
        #[cfg(not(feature = "zstd"))]
        ChunkCompression::Zstd { .. } => Err(BlobError::Protocol(
            "zstd compression requested but zblob was built without the `zstd` feature".into(),
        )),
    }
}

/// Unframe a chunk container back to its raw bytes.
pub(crate) fn try_unpack(packed: &[u8]) -> std::result::Result<Vec<u8>, ContainerError> {
    match packed.split_first() {
        Some((&TAG_RAW, rest)) => Ok(rest.to_vec()),
        Some((&TAG_ZSTD, rest)) => {
            if rest.len() < 4 {
                return Err(ContainerError::Corrupt);
            }
            let (len_bytes, frame) = rest.split_at(4);
            let declared = u32::from_le_bytes(len_bytes.try_into().expect("4 bytes")) as usize;
            if declared > MAX_UNPACKED {
                return Err(ContainerError::Corrupt);
            }
            #[cfg(feature = "zstd")]
            {
                let out =
                    zstd::bulk::decompress(frame, declared).map_err(|_| ContainerError::Corrupt)?;
                if out.len() != declared {
                    return Err(ContainerError::Corrupt);
                }
                Ok(out)
            }
            #[cfg(not(feature = "zstd"))]
            {
                let _ = frame;
                Err(ContainerError::Unsupported)
            }
        }
        Some((&TAG_SEALED, _)) => Err(ContainerError::Unsupported), // wire never carries sealed
        _ => Err(ContainerError::Corrupt),
    }
}

/// Unframe for the wire path, mapping both failure kinds to a protocol error.
pub(crate) fn unpack(packed: &[u8]) -> Result<Vec<u8>> {
    try_unpack(packed).map_err(|e| match e {
        ContainerError::Unsupported => BlobError::Protocol(
            "chunk container uses a format this build does not support (missing cargo feature?)"
                .into(),
        ),
        ContainerError::Corrupt => BlobError::Protocol("malformed chunk container".into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_roundtrip() {
        let data = b"hello chunk";
        let packed = pack(data, ChunkCompression::None).unwrap();
        assert_eq!(packed[0], TAG_RAW);
        assert_eq!(unpack(&packed).unwrap(), data);
    }

    #[test]
    fn garbage_frames_rejected() {
        assert!(unpack(&[]).is_err());
        assert!(unpack(&[0x42, 1, 2, 3]).is_err());
        assert!(unpack(&[TAG_ZSTD, 1, 2]).is_err()); // truncated header
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_roundtrip_and_bailout() {
        // Highly compressible → zstd frame, smaller than raw.
        let text = vec![b'a'; 100_000];
        let packed = pack(&text, ChunkCompression::Zstd { level: 3 }).unwrap();
        assert_eq!(packed[0], TAG_ZSTD);
        assert!(packed.len() < text.len() / 10);
        assert_eq!(unpack(&packed).unwrap(), text);

        // Incompressible (pseudo-random) → bails out to raw.
        let mut random = Vec::with_capacity(50_000);
        let mut x = 0x12345u64;
        for _ in 0..50_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            random.push((x & 0xff) as u8);
        }
        let packed = pack(&random, ChunkCompression::Zstd { level: 3 }).unwrap();
        assert_eq!(packed[0], TAG_RAW, "incompressible data must stay raw");
        assert_eq!(unpack(&packed).unwrap(), random);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn oversized_declaration_rejected() {
        let mut packed = pack(&vec![b'x'; 1000], ChunkCompression::Zstd { level: 3 }).unwrap();
        assert_eq!(packed[0], TAG_ZSTD);
        // Forge a huge declared length.
        packed[1..5].copy_from_slice(&(u32::MAX).to_le_bytes());
        assert!(unpack(&packed).is_err());
    }

    #[cfg(not(feature = "zstd"))]
    #[test]
    fn zstd_unavailable_without_feature() {
        assert!(pack(b"x", ChunkCompression::Zstd { level: 3 }).is_err());
        // A zstd frame from a compressing peer is a clear error, not garbage.
        let fake = [TAG_ZSTD, 4, 0, 0, 0, 1, 2, 3];
        assert!(unpack(&fake).is_err());
    }
}
