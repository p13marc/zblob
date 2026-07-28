//! Shared test helpers: one isolated, scouting-off session per test (the repo's
//! in-process loopback pattern), deterministic pseudo-random data, and small
//! byte sources.
#![allow(dead_code)] // each test binary uses a different subset of these.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use zblob::Hash;

pub fn isolated_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .unwrap();
    config
}

pub async fn open_session() -> Arc<zenoh::Session> {
    Arc::new(zenoh::open(isolated_config()).await.expect("open zenoh"))
}

pub fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("blobtest/{nanos}")
}

/// Deterministic pseudo-random bytes (xorshift64, no rand dependency).
pub fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
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

pub fn content_hash(bytes: &[u8]) -> Hash {
    Hash::of(bytes)
}

/// Raw bao-slice construction for adversarial/fake-server tests: lets a test
/// serve protocol-correct (or deliberately tampered) slice replies without
/// going through `BlobServer`.
pub mod bao {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bao_tree::io::sync::encode_ranges_validated;
    use bao_tree::{BlockSize, ChunkNum, ChunkRanges};

    /// Must match zblob's verification block size (16 KiB groups).
    pub const BLOCK: BlockSize = BlockSize::from_chunk_log(4);

    pub fn outboard(data: &[u8]) -> PreOrderMemOutboard {
        PreOrderMemOutboard::create(data, BLOCK)
    }

    /// The bao slice for transfer chunk `index` of `data` at `chunk_size`.
    pub fn slice(data: &[u8], ob: &PreOrderMemOutboard, chunk_size: u32, index: u32) -> Vec<u8> {
        let total = data.len() as u64;
        let start = (index as u64 * chunk_size as u64).min(total);
        let end = (start + chunk_size as u64).min(total);
        let ranges = ChunkRanges::from(ChunkNum(start >> 10)..ChunkNum::chunks(end));
        let mut out = Vec::new();
        encode_ranges_validated(data, ob, &ranges, &mut out).unwrap();
        out
    }
}
