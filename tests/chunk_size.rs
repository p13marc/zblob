//! Deciding the default transfer chunk size, rather than guessing it.
//!
//! Two effects pull in opposite directions, and both are measurable:
//!
//! - **Fragment loss.** Zenoh batches at 64 KiB and fragments anything larger;
//!   a dropped fragment discards the *whole* message. So a chunk of `S` bytes
//!   is `F = ceil(S / 64 KiB)` fragments, and at a per-fragment loss rate `p`
//!   its chance of surviving is `(1-p)^F`. Bigger chunks lose more often, and
//!   each loss costs more.
//! - **Per-slice overhead.** A reply is a *bao slice*: the chunk's bytes plus
//!   the parent hashes proving them against the root. Smaller chunks mean more
//!   slices, and more total parent-hash bytes on the wire.
//!
//! This measures the second exactly (by encoding real slices) and models the
//! first, then reports total wire bytes per useful byte. The numbers are
//! printed so the choice is auditable; the assertions pin the *shape* of the
//! result, which is what a future change could break.

mod common;

use zblob::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, TransferChunks};

/// Zenoh's TX batch size, and so the fragmentation unit. The configured
/// maximum is 65535; a 512 KiB chunk is eight of them plus a few bytes, which
/// for this purpose is eight.
const FRAGMENT: usize = 64 * 1024;

/// Total encoded bao-slice bytes for a whole blob at `chunk_size`, measured by
/// actually encoding every slice the server would send.
///
/// Built with `bao-tree` directly, at the same 16 KiB group size the crate
/// uses, so this measures the real thing rather than a formula someone
/// believed.
fn slice_bytes(data: &[u8], chunk_size: u32) -> (usize, u32) {
    use bao_tree::io::outboard::PreOrderOutboard;
    use bao_tree::io::sync::{CreateOutboard, encode_ranges_validated};
    use bao_tree::{BlockSize, ChunkNum, ChunkRanges};

    let block = BlockSize::from_chunk_log(4); // 16 KiB verification groups
    let outboard: PreOrderOutboard<Vec<u8>> =
        PreOrderOutboard::create(std::io::Cursor::new(data), block).unwrap();
    let chunks = TransferChunks::new(chunk_size, data.len() as u64).unwrap();
    let mut total = 0usize;
    for i in 0..chunks.count() {
        let r = chunks.byte_range(i);
        let range = ChunkNum(r.start >> 10)..ChunkNum(r.end.div_ceil(1024));
        let mut buf = Vec::new();
        encode_ranges_validated(data, &outboard, &ChunkRanges::from(range), &mut buf).unwrap();
        total += buf.len();
    }
    (total, chunks.count())
}

#[test]
fn the_default_chunk_size_is_justified_by_measurement() {
    // 8 MiB of incompressible data — a plausible artifact.
    let data = common::pseudo_random(8 * 1024 * 1024, 4242);
    let payload = data.len() as f64;

    println!(
        "\n{:>9} {:>7} {:>9} {:>8}   {:>28}",
        "chunk", "chunks", "slice B", "hdr %", "wire bytes / useful byte"
    );
    println!(
        "{:>9} {:>7} {:>9} {:>8}   {:>8} {:>8} {:>8}",
        "", "", "", "", "p=0", "p=1%", "p=5%"
    );

    let mut rows = Vec::new();
    for shift in 0..=6 {
        let chunk_size = MIN_CHUNK_SIZE << shift;
        if chunk_size > MAX_CHUNK_SIZE {
            break;
        }
        let (wire, count) = slice_bytes(&data, chunk_size);
        let header_pct = 100.0 * (wire as f64 - payload) / payload;
        let fragments = (chunk_size as usize).div_ceil(FRAGMENT);
        // Expected transmissions per successful chunk at per-fragment loss p.
        let cost = |p: f64| (wire as f64 / payload) * (1.0 - p).powi(-(fragments as i32));
        let (c0, c1, c5) = (cost(0.0), cost(0.01), cost(0.05));
        println!(
            "{:>8}K {:>7} {:>9} {:>7.3}%   {:>8.4} {:>8.4} {:>8.4}",
            chunk_size / 1024,
            count,
            wire,
            header_pct,
            c0,
            c1,
            c5
        );
        rows.push((chunk_size, c0, c1, c5, fragments));
    }
    println!();

    // 1. Slice overhead falls as chunks grow — that is the pressure upward.
    let smallest = rows.first().unwrap();
    let largest = rows.last().unwrap();
    assert!(
        smallest.1 > largest.1,
        "smaller chunks must carry proportionally more parent-hash overhead"
    );

    // 2. Under fragment loss the ordering inverts — that is the pressure down.
    assert!(
        smallest.3 < largest.3,
        "under 5% fragment loss, smaller chunks must cost fewer wire bytes"
    );

    // 3. The chosen default must be at or below the point where a chunk stops
    //    being a handful of fragments. This is the substance of the decision:
    //    512 KiB is eight fragments, all of which must survive.
    let default = rows
        .iter()
        .find(|r| r.0 == zblob::DEFAULT_CHUNK_SIZE)
        .expect("the default must be one of the measured sizes");
    assert!(
        default.4 <= 4,
        "the default is {} fragments per chunk; a loss discards all of them",
        default.4
    );

    // 4. And it must not be so small that header overhead dominates: under a
    //    clean link it should stay within a few tenths of a percent of the
    //    best achievable.
    let best_clean = rows.iter().map(|r| r.1).fold(f64::MAX, f64::min);
    assert!(
        default.1 - best_clean < 0.004,
        "the default costs {:.4} on a clean link against a best of {best_clean:.4}",
        default.1
    );
}
