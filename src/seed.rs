//! Local seeding (the desync model): satisfy a snapshot's chunks from data
//! already on this machine before touching the network.
//!
//! A Tier-2 download fetches `needed − have` from its [`ContentStore`] — but
//! "have" can be grown for free first. [`seed_store`] chunks local candidate
//! trees (a *previous version of the destination* is the classic seed) with
//! the index's own CDC parameters and stores every chunk the snapshot needs;
//! it also synthesizes all-zero chunks (sparse-file regions) without reading
//! anything. The subsequent `download_tree` then transfers only what is truly
//! new to this machine.
//!
//! Seeding copies bytes through the store rather than reflinking — a chunk
//! must exist store-addressed for resume/dedup bookkeeping either way.

use std::collections::HashSet;
use std::path::Path;

use crate::error::Result;
use crate::hash::Hash;
use crate::obs::zdebug;
use crate::store::ContentStore;
use crate::tree::{Entry, TreeIndex};

/// What [`seed_store`] accomplished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SeedStats {
    /// Chunks satisfied from local seed trees.
    pub from_local: u32,
    /// Chunks synthesized as all-zeros.
    pub from_zero: u32,
    /// Chunks the snapshot needs that remain missing (to be fetched).
    pub still_missing: u32,
}

/// Populate `store` with every chunk of `index` that can be found locally:
/// all-zero chunks first, then the files under each of `seed_roots`, chunked
/// with the index's own [`CdcParams`](crate::CdcParams) (identical parameters
/// — including the gear seed — are what make boundaries line up).
///
/// Synchronous (run it in `spawn_blocking`); unreadable seed files are
/// skipped, not errors — seeding is best-effort by nature.
pub fn seed_store(
    index: &TreeIndex,
    store: &dyn ContentStore,
    seed_roots: &[&Path],
) -> Result<SeedStats> {
    let mut missing: HashSet<Hash> = index
        .needed_chunks()
        .into_iter()
        .filter(|h| !store.has(h))
        .collect();
    let mut stats = SeedStats::default();

    // 1. The synthetic all-zero seed: every distinct chunk length the index
    //    references costs one hash of a zero buffer to test — no I/O at all.
    let lens: HashSet<u32> = index
        .entries
        .iter()
        .flat_map(|e| match e {
            Entry::File { chunks, .. } => chunks.as_slice(),
            _ => &[],
        })
        .map(|c| c.len)
        .collect();
    for len in lens {
        if missing.is_empty() {
            break;
        }
        let zeros = vec![0u8; len as usize];
        let h = Hash::of(&zeros);
        if missing.remove(&h) {
            store.put(&h, &zeros)?;
            stats.from_zero += 1;
        }
    }

    // 2. Local candidate trees, chunked with the snapshot's own parameters.
    for root in seed_roots {
        if missing.is_empty() {
            break;
        }
        seed_from_tree(root, index, store, &mut missing, &mut stats)?;
    }

    stats.still_missing = missing.len() as u32;
    zdebug!(
        from_local = stats.from_local,
        from_zero = stats.from_zero,
        still_missing = stats.still_missing,
        "seeding done"
    );
    Ok(stats)
}

fn seed_from_tree(
    root: &Path,
    index: &TreeIndex,
    store: &dyn ContentStore,
    missing: &mut HashSet<Hash>,
    stats: &mut SeedStats,
) -> Result<()> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Ok(()); // unreadable seed root: skip, best-effort.
    };
    for entry in entries.flatten() {
        if missing.is_empty() {
            return Ok(());
        }
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let ftype = meta.file_type();
        if ftype.is_dir() {
            seed_from_tree(&path, index, store, missing, stats)?;
        } else if ftype.is_file() {
            let Ok(file) = std::fs::File::open(&path) else {
                continue;
            };
            for chunk in index.cdc.chunk_reader(file) {
                let Ok(bytes) = chunk else { break };
                let h = Hash::of(&bytes);
                if missing.remove(&h) {
                    store.put(&h, &bytes)?;
                    stats.from_local += 1;
                }
                if missing.is_empty() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::CdcParams;
    use crate::store::MemoryStore;
    use crate::tree::build_tree;

    fn small_cdc() -> CdcParams {
        CdcParams {
            min: 2048,
            avg: 8192,
            max: 32768,
            normalization: 2,
            gear_seed: 0,
        }
    }

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

    #[test]
    fn previous_version_seeds_most_chunks() {
        // "Remote" snapshot: v2 of a tree.
        let v2 = tempfile::tempdir().unwrap();
        let mut big = pseudo_random(120_000, 21);
        std::fs::write(v2.path().join("stable.bin"), pseudo_random(60_000, 22)).unwrap();
        big.splice(0..0, b"PREPENDED".iter().copied()); // small edit vs v1
        std::fs::write(v2.path().join("edited.bin"), &big).unwrap();
        let remote_store = MemoryStore::new();
        let index = build_tree(v2.path(), "v2", &small_cdc(), &remote_store).unwrap();

        // Local machine: has v1 (same stable file, pre-edit big file).
        let v1 = tempfile::tempdir().unwrap();
        std::fs::write(v1.path().join("stable.bin"), pseudo_random(60_000, 22)).unwrap();
        std::fs::write(v1.path().join("edited.bin"), pseudo_random(120_000, 21)).unwrap();

        let local = MemoryStore::new();
        let stats = seed_store(&index, &local, &[v1.path()]).unwrap();
        let total = index.needed_chunks().len() as u32;
        assert!(
            stats.from_local > total / 2,
            "most chunks should seed locally: {stats:?} of {total}"
        );
        assert!(
            stats.still_missing < total / 2 && stats.still_missing > 0,
            "only the edit neighborhood should be missing: {stats:?}"
        );
        assert_eq!(
            stats.from_local + stats.from_zero + stats.still_missing,
            total
        );
        // Everything seeded is really in the store and hash-correct.
        for h in local.hashes().unwrap() {
            assert_eq!(Hash::of(&local.get(&h).unwrap()), h);
        }
    }

    #[test]
    fn zero_regions_synthesized_without_io() {
        let src = tempfile::tempdir().unwrap();
        // A file that is mostly zeros (sparse-ish) plus some real data.
        let mut data = vec![0u8; 100_000];
        data[..1000].copy_from_slice(&pseudo_random(1000, 23));
        std::fs::write(src.path().join("sparse.bin"), &data).unwrap();
        let remote_store = MemoryStore::new();
        let index = build_tree(src.path(), "sparse", &small_cdc(), &remote_store).unwrap();

        let local = MemoryStore::new();
        let stats = seed_store(&index, &local, &[]).unwrap();
        assert!(
            stats.from_zero > 0,
            "zero chunks must synthesize: {stats:?}"
        );
        assert!(stats.still_missing > 0, "the data head is not zero");
    }
}
