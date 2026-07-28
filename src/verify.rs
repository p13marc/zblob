//! BLAKE3 + bao-tree verified streaming — the v2 integrity core.
//!
//! A blob's identity is its BLAKE3 root computed over a bao tree at
//! [`BAO_BLOCK`] (16 KiB verification groups). The server sends each transfer
//! chunk as a *bao slice*: the chunk's bytes plus the parent-hash pairs needed
//! to verify them against the root. This makes every Zenoh reply
//! **independently verifiable** given only `(root, total_len, chunk range)` —
//! which is exactly what out-of-order reply delivery needs — and localizes
//! tampering to a 16 KiB group instead of poisoning a whole download.
//!
//! Unit conventions (careful, three sizes are in play):
//! - a *bao chunk* ([`ChunkNum`]) is 1024 bytes — the BLAKE3 leaf;
//! - a *group* is `2^4` bao chunks = 16 KiB — the verification granularity;
//! - a *transfer chunk* (crate::chunk) is a multiple of 16 KiB — the wire unit.
use std::io::{Cursor, Read, Seek};
use std::ops::Range;
use std::path::Path;

use bao_tree::io::BaoContentItem;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::{
    CreateOutboard, DecodeResponseIter, Outboard, ReadAt, encode_ranges_validated,
};
use bao_tree::{BaoTree, BlockSize, ChunkNum, ChunkRanges};

/// Bao block size: 2^4 KiB = 16 KiB verification groups (iroh's choice; ~0.4%
/// outboard overhead, no outboard at all for blobs ≤ 16 KiB).
pub(crate) const BAO_BLOCK: BlockSize = BlockSize::from_chunk_log(4);

/// Bytes per verification group.
pub(crate) const GROUP_SIZE: u64 = 16 * 1024;

/// The in-memory outboard the server keeps per registered blob.
pub(crate) type MemOutboard = PreOrderOutboard<Vec<u8>>;

/// Compute a pre-order outboard (root + parent hashes) by streaming `data`.
/// This is the only place a blob is read end-to-end at registration time.
pub(crate) fn compute_outboard(data: impl Read + Seek) -> std::io::Result<MemOutboard> {
    PreOrderOutboard::create(data, BAO_BLOCK)
}

/// Like [`compute_outboard`], for a sequential reader whose size is already
/// known (saves the seek-to-end).
pub(crate) fn compute_outboard_sized(data: impl Read, size: u64) -> std::io::Result<MemOutboard> {
    PreOrderOutboard::create_sized(data, size, BAO_BLOCK)
}

/// The bao tree geometry for a blob of `total_len` bytes.
pub(crate) fn tree_for(total_len: u64) -> BaoTree {
    BaoTree::new(total_len, BAO_BLOCK)
}

/// Bao chunk range (1 KiB units) covering the byte range `[start, end)`.
/// `start` must be group-aligned; `end` may be the (unaligned) blob length.
pub(crate) fn chunk_range(bytes: Range<u64>) -> Range<ChunkNum> {
    ChunkNum(bytes.start >> 10)..ChunkNum::chunks(bytes.end)
}

/// Encode the bao slice for `chunks` from a blob's data + outboard. The slice
/// is self-verifying: it interleaves parent-hash pairs with the leaf bytes.
pub(crate) fn encode_slice(
    data: impl ReadAt,
    outboard: &impl Outboard,
    chunks: Range<ChunkNum>,
) -> std::io::Result<Vec<u8>> {
    let ranges = ChunkRanges::from(chunks);
    let mut out = Vec::new();
    encode_ranges_validated(data, outboard, &ranges, &mut out).map_err(std::io::Error::other)?;
    Ok(out)
}

/// Compute a pre-order outboard for a large blob into the file at `obao`,
/// streaming `data` once. The returned outboard reads its parent hashes from
/// that file, keeping server memory flat no matter the blob size.
pub(crate) fn compute_outboard_file(
    data: impl Read,
    size: u64,
    obao: &Path,
) -> std::io::Result<PreOrderOutboard<std::fs::File>> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(obao)?;
    let mut ob = PreOrderOutboard {
        root: blake3::hash(&[]),
        tree: BaoTree::new(size, BAO_BLOCK),
        data: file,
    };
    ob.init_from(data)?;
    ob.data.sync_all()?;
    Ok(ob)
}

/// Where a registered blob's outboard lives: in memory for ordinary blobs, in
/// a sibling `.obao4` file for very large ones (~0.4% of the blob size).
pub(crate) enum OutboardStore {
    /// Parent hashes held in memory.
    Mem(MemOutboard),
    /// Parent hashes in a file (large blobs).
    File(PreOrderOutboard<std::fs::File>),
}

impl OutboardStore {
    /// The blob's BLAKE3 bao root.
    pub fn root(&self) -> blake3::Hash {
        match self {
            OutboardStore::Mem(ob) => ob.root,
            OutboardStore::File(ob) => ob.root,
        }
    }

    /// Encode the bao slice for `chunks` from `data` + this outboard.
    pub fn encode_slice(
        &self,
        data: impl ReadAt,
        chunks: Range<ChunkNum>,
    ) -> std::io::Result<Vec<u8>> {
        match self {
            OutboardStore::Mem(ob) => encode_slice(data, ob, chunks),
            OutboardStore::File(ob) => encode_slice(data, ob, chunks),
        }
    }
}

/// Adapt a positional reader into a sequential [`Read`] (for outboard
/// creation, which streams the source once).
pub(crate) struct ReadAtCursor<R> {
    inner: R,
    pos: u64,
}

impl<R: ReadAt> ReadAtCursor<R> {
    pub fn new(inner: R) -> Self {
        ReadAtCursor { inner, pos: 0 }
    }
}

impl<R: ReadAt> Read for ReadAtCursor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read_at(self.pos, buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

/// Verify-decode the bao slice for `chunks` against `root`, invoking `on_leaf`
/// with each verified `(byte offset, bytes)` leaf. Any tampering — flipped
/// bytes, wrong lengths, truncation — surfaces as an `Err` *before* the
/// affected leaf is emitted; leaves already emitted are genuine.
pub(crate) fn decode_slice(
    root: &blake3::Hash,
    total_len: u64,
    chunks: Range<ChunkNum>,
    encoded: &[u8],
    mut on_leaf: impl FnMut(u64, &[u8]) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let ranges = ChunkRanges::from(chunks);
    let iter = DecodeResponseIter::new(*root, tree_for(total_len), Cursor::new(encoded), &ranges);
    for item in iter {
        match item.map_err(std::io::Error::other)? {
            BaoContentItem::Leaf(leaf) => on_leaf(leaf.offset, &leaf.data)?,
            BaoContentItem::Parent(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes (xorshift64; no rand dependency).
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

    fn decode_collect(
        root: &blake3::Hash,
        total_len: u64,
        chunks: Range<ChunkNum>,
        encoded: &[u8],
    ) -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        let mut leaves = Vec::new();
        decode_slice(root, total_len, chunks, encoded, |off, data| {
            leaves.push((off, data.to_vec()));
            Ok(())
        })?;
        Ok(leaves)
    }

    #[test]
    fn root_matches_plain_blake3() {
        let data = pseudo_random(100_000, 1);
        let ob = compute_outboard(Cursor::new(&data)).unwrap();
        assert_eq!(ob.root, blake3::hash(&data));
    }

    #[test]
    fn slice_roundtrip_interior_range() {
        // 100 000 bytes = 6 full 16 KiB groups + a short tail.
        let data = pseudo_random(100_000, 2);
        let ob = compute_outboard(Cursor::new(&data)).unwrap();

        // Byte range of "transfer chunk" [16384, 49152) = groups 1..3.
        let chunks = chunk_range(16_384..49_152);
        let slice = encode_slice(&data[..], &ob, chunks.clone()).unwrap();
        // Slice = 32 KiB of data + parent pairs; overhead stays small.
        assert!(slice.len() >= 32 * 1024);
        assert!(slice.len() < 32 * 1024 + 1024);

        let leaves = decode_collect(&ob.root, data.len() as u64, chunks, &slice).unwrap();
        let rebuilt: Vec<u8> = leaves.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(leaves.first().unwrap().0, 16_384);
        assert_eq!(rebuilt, data[16_384..49_152]);
    }

    #[test]
    fn slice_roundtrip_unaligned_tail() {
        let data = pseudo_random(100_000, 3);
        let ob = compute_outboard(Cursor::new(&data)).unwrap();
        // Final partial range: bytes 98304.. (short tail, not group-aligned end).
        let chunks = chunk_range(98_304..100_000);
        let slice = encode_slice(&data[..], &ob, chunks.clone()).unwrap();
        let leaves = decode_collect(&ob.root, data.len() as u64, chunks, &slice).unwrap();
        let rebuilt: Vec<u8> = leaves.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(rebuilt, data[98_304..]);
    }

    #[test]
    fn tampered_slice_rejected() {
        let data = pseudo_random(100_000, 4);
        let ob = compute_outboard(Cursor::new(&data)).unwrap();
        let chunks = chunk_range(0..32_768);
        let mut slice = encode_slice(&data[..], &ob, chunks.clone()).unwrap();
        // Flip one payload byte near the end (past the parent pairs).
        let n = slice.len();
        slice[n - 10] ^= 0xff;
        assert!(decode_collect(&ob.root, data.len() as u64, chunks, &slice).is_err());
    }

    #[test]
    fn wrong_root_rejected() {
        let data = pseudo_random(50_000, 5);
        let ob = compute_outboard(Cursor::new(&data)).unwrap();
        let chunks = chunk_range(0..50_000);
        let slice = encode_slice(&data[..], &ob, chunks.clone()).unwrap();
        let wrong = blake3::hash(b"not the data");
        assert!(decode_collect(&wrong, data.len() as u64, chunks, &slice).is_err());
    }

    #[test]
    fn tiny_blob_no_outboard() {
        // ≤ 16 KiB: a single group; the outboard carries no parent hashes.
        let data = pseudo_random(10_000, 6);
        let ob = compute_outboard(Cursor::new(&data)).unwrap();
        assert!(ob.data.is_empty());
        let chunks = chunk_range(0..10_000);
        let slice = encode_slice(&data[..], &ob, chunks.clone()).unwrap();
        let leaves = decode_collect(&ob.root, data.len() as u64, chunks, &slice).unwrap();
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].1, data);
    }

    #[test]
    fn empty_blob() {
        let ob = compute_outboard(Cursor::new(Vec::<u8>::new())).unwrap();
        assert_eq!(ob.root, blake3::hash(b""));
        // No chunk ranges to request; an empty range encodes to an empty slice.
        let slice = encode_slice(&[][..], &ob, ChunkNum(0)..ChunkNum(0)).unwrap();
        assert!(slice.is_empty());
    }
}
