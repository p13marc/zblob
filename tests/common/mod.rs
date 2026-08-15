//! Shared test helpers: one isolated, scouting-off session per test (the repo's
//! in-process loopback pattern), deterministic pseudo-random data, and small
//! byte sources.
#![allow(dead_code)] // each test binary uses a different subset of these.

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

pub async fn open_session() -> zenoh::Session {
    zenoh::open(isolated_config()).await.expect("open zenoh")
}

pub fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("blobtest/{nanos}")
}

/// Deterministic pseudo-random bytes (xorshift64, no rand dependency).
///
/// `seed` is mixed rather than used directly: the state must be non-zero for
/// xorshift, and the obvious `seed | 1` collapses every even seed onto its odd
/// successor — so `pseudo_random(n, 900)` and `pseudo_random(n, 901)` returned
/// *byte-identical* data. A test using consecutive seeds for "distinct"
/// fixtures silently got duplicates, which is invisible until something
/// content-addressed deduplicates them.
pub fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(31)
        .wrapping_add(0xD1B5_4A32_D192_ED03)
        | 1;
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

/// A `ServePrefix` for a prefix the test knows is concrete.
#[allow(dead_code)]
pub fn serve(p: impl Into<String>) -> zblob::ServePrefix {
    let p = p.into();
    zblob::ServePrefix::new(&p).unwrap_or_else(|e| panic!("test serve prefix {p:?}: {e}"))
}

/// A `QueryPrefix` for a prefix the test knows is well-formed.
#[allow(dead_code)]
pub fn query(p: impl Into<String>) -> zblob::QueryPrefix {
    let p = p.into();
    zblob::QueryPrefix::new(&p).unwrap_or_else(|e| panic!("test query prefix {p:?}: {e}"))
}

/// Hand-rolled fanout frames and a hostile/honest co-publisher, shared by
/// `tests/fanout.rs` and `tests/hostile_fanout.rs`.
#[cfg(feature = "fanout")]
pub mod fanout {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use zblob::MIN_CHUNK_SIZE;
    use zblob::wire::{self, encode};

    /// `FanoutFrame` is private, so frames are built positionally: postcard
    /// identifies enum variants by order, so `(version, variant, ..)` is
    /// byte-identical to the struct the real publisher sends. That is the same
    /// escape hatch `BlobId`'s module doc describes, and it is what lets an
    /// adversarial test send what the types forbid.
    pub const FRAME_MANIFEST: u32 = 0;
    pub const FRAME_SLICE: u32 = 1;

    /// The manifest frame for `manifest`, as the real publisher would send it.
    pub fn manifest_frame(manifest: &zblob::Manifest) -> Vec<u8> {
        encode(&(wire::WIRE_VERSION, FRAME_MANIFEST, manifest)).unwrap()
    }

    /// The slice frame carrying `bao` at `index`.
    pub fn slice_frame(index: u32, bao: &[u8]) -> Vec<u8> {
        encode(&(wire::WIRE_VERSION, FRAME_SLICE, index, bao)).unwrap()
    }

    /// The frames a publisher would send for `data`, with `tamper` applied to
    /// every slice (`None` = honest).
    pub fn hand_rolled_frames(
        manifest: &zblob::Manifest,
        data: &[u8],
        tamper: Option<&str>,
    ) -> Vec<Vec<u8>> {
        let ob = super::bao::outboard(data);
        let mut out = vec![manifest_frame(manifest)];
        let count = manifest.chunks().unwrap().count();
        for index in 0..count {
            let mut bao = super::bao::slice(data, &ob, MIN_CHUNK_SIZE, index);
            match tamper {
                Some("flip") => {
                    let mid = bao.len() / 2;
                    bao[mid] ^= 0xFF;
                }
                Some("truncate") => bao.truncate(bao.len() / 2),
                Some(_) => bao = vec![0xABu8; bao.len()],
                None => {}
            }
            out.push(slice_frame(index, &bao));
        }
        out
    }

    /// A well-formed manifest for `data` under id `"rollout"` — the fields are
    /// public, so a hostile test clones and corrupts what it needs.
    pub fn demo_manifest(data: &[u8]) -> zblob::Manifest {
        zblob::Manifest {
            version: wire::WIRE_VERSION,
            id: zblob::BlobId::new("rollout").unwrap(),
            filename: None,
            total_len: data.len() as u64,
            chunk_size: MIN_CHUNK_SIZE,
            root: zblob::Hash::of(data),
            created_ms: 0,
            ext: wire::Ext::new(),
        }
    }

    /// Publish `frames` on the fanout key repeatedly until `done` fires.
    ///
    /// A plain publisher has no history cache, so a single burst races the
    /// receiver's subscriber declaration and can be lost entirely — which made
    /// the first version of the fanout tamper tests vacuous in both
    /// directions: the tampered case "passed" because nothing arrived at all,
    /// and the honest control failed for the same reason. Re-publishing is
    /// safe: a fanout receiver ignores a frame it already has.
    pub async fn republish_until(
        session: &zenoh::Session,
        prefix: &str,
        id: &str,
        frames: Vec<Vec<u8>>,
        done: Arc<AtomicBool>,
    ) {
        republish_until_enc(
            session,
            prefix,
            id,
            frames,
            done,
            (&wire::ENC_FANOUT).into(),
        )
        .await;
    }

    /// [`republish_until`], but stamping every frame with `encoding` — a
    /// mistagged co-publisher for tests of the tag-before-decode rule.
    pub async fn republish_until_enc(
        session: &zenoh::Session,
        prefix: &str,
        id: &str,
        frames: Vec<Vec<u8>>,
        done: Arc<AtomicBool>,
        encoding: zenoh::bytes::Encoding,
    ) {
        let publisher = session
            .declare_publisher(zblob::fanout::fanout_key(prefix, id))
            .congestion_control(zenoh::qos::CongestionControl::Block)
            .await
            .unwrap();
        while !done.load(Ordering::Relaxed) {
            for frame in &frames {
                if done.load(Ordering::Relaxed) {
                    return;
                }
                let _ = publisher
                    .put(frame.clone())
                    .encoding(encoding.clone())
                    .await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
