//! Serverless Tier-2 — publish a snapshot into a **Zenoh storage**.
//!
//! Instead of running a [`crate::TreeServer`] for the lifetime of a transfer, a
//! producer can PUT its content-addressed chunks and tree index into a
//! router-hosted storage (the `zenoh-plugin-storage-manager`) and then exit. The
//! storage retains the keys, dedups them fleet-wide (a chunk PUT by any producer
//! is reused by all), and answers the GETs that [`crate::TreeClient`] already
//! issues — so the client needs no changes and the producer needn't stay alive.
//!
//! Content addressing makes this safe: a chunk key `<store>/<algo>/<hash>` only
//! ever maps to one byte string, so the storage's last-writer-wins reconciliation
//! is a no-op and re-publishing is idempotent.
//!
//! **Durability caveat**: a resolved `put` means the sample was handed to the
//! transport, *not* that a storage retained it — and index/chunk keys may land
//! on different storages with no ordering guarantee. [`publish_snapshot`]
//! therefore finishes with a **read-back settle phase**: it GETs the index and
//! a sample of chunk keys until they answer (or `settle` expires), so "publish
//! returned Ok" means "a client can fetch this now".

use std::time::Duration;

use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::ConsolidationMode;

use crate::compress::{ChunkCompression, pack};
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::store::ContentStore;
use crate::tree::TreeIndex;
use crate::wire::{ENC_CHUNK, ENC_INDEX, encode};
use crate::{store_key, tree_key};

/// PUT one content-addressed chunk into the storage under `store_prefix`.
///
/// The key is `<store_prefix>/blake3/<hash>`; the value is the chunk wrapped
/// in the self-describing container frame (`0x00` + raw bytes, or a zstd
/// frame when `compression` says so) — the same framing `TreeServer` puts on
/// the wire. Idempotent — re-PUTting an identical chunk is a no-op.
pub async fn publish_chunk(
    session: &zenoh::Session,
    store_prefix: &str,
    hash: &Hash,
    bytes: &[u8],
    compression: ChunkCompression,
) -> Result<()> {
    crate::paths::validate_serve_prefix(store_prefix)?;
    session
        .put(
            store_key(store_prefix, Hash::ALGO, hash),
            pack(bytes, compression)?,
        )
        .encoding(ENC_CHUNK)
        // Publications default to `Drop` (unlike queries, which default to
        // `Block` and lend it to their replies — see the crate docs, fact 1).
        // A dropped chunk here is invisible: the settle phase samples, so the
        // publish returns Ok and the producer exits, and the loss surfaces
        // much later on a consumer as an unresolvable NotFound. Bulk data is
        // exactly the case where blocking is right.
        .congestion_control(CongestionControl::Block)
        .priority(Priority::DataLow)
        .await
        .map_err(BlobError::zenoh)
}

/// PUT the chunks `index` references into the storage. Returns how many were
/// published. Stops at the first PUT error.
///
/// Scoped to the snapshot, not to the store: a store commonly holds chunks
/// from other snapshots — and, on a shared machine, other tenants' — while the
/// storage being published into is typically fleet-wide.
pub async fn publish_snapshot_chunks(
    session: &zenoh::Session,
    store_prefix: &str,
    index: &TreeIndex,
    store: &dyn ContentStore,
    compression: ChunkCompression,
) -> Result<u32> {
    publish_hashes(
        session,
        store_prefix,
        &index.needed_chunks(),
        store,
        compression,
    )
    .await
}

/// PUT **every** chunk in `store` into the storage. Returns how many were
/// published. Stops at the first PUT error.
///
/// This publishes whatever the store happens to hold, which is rarely what a
/// snapshot publisher wants — prefer [`publish_snapshot_chunks`]. It stays for
/// the case where a store *is* the unit being replicated (mirroring a whole
/// content store to a router).
pub async fn publish_store(
    session: &zenoh::Session,
    store_prefix: &str,
    store: &dyn ContentStore,
    compression: ChunkCompression,
) -> Result<u32> {
    publish_hashes(session, store_prefix, &store.hashes()?, store, compression).await
}

async fn publish_hashes(
    session: &zenoh::Session,
    store_prefix: &str,
    hashes: &[Hash],
    store: &dyn ContentStore,
    compression: ChunkCompression,
) -> Result<u32> {
    let mut published = 0u32;
    for hash in hashes {
        let bytes = store
            .get(hash)
            .ok_or_else(|| BlobError::NotFound(hash.to_string()))?;
        publish_chunk(session, store_prefix, hash, &bytes, compression).await?;
        published += 1;
    }
    Ok(published)
}

/// PUT a tree index into the storage at `<tree_prefix>/<id>`. A
/// [`crate::TreeClient`] with the matching `tree_prefix` then GETs it like any
/// other index.
pub async fn publish_index(
    session: &zenoh::Session,
    tree_prefix: &str,
    index: &TreeIndex,
) -> Result<()> {
    crate::paths::validate_serve_prefix(tree_prefix)?;
    let payload = encode(index)?;
    session
        .put(tree_key(tree_prefix, &index.id), payload)
        .encoding(ENC_INDEX)
        // See publish_chunk: publications default to Drop, and losing the
        // index loses the snapshot.
        .congestion_control(CongestionControl::Block)
        .priority(Priority::DataLow)
        .await
        .map_err(BlobError::zenoh)
}

/// How much of a published snapshot the read-back phase actually checks.
///
/// The distinction matters because the phase's whole job is to let a producer
/// exit safely, and "the storage answered for 7 of 10,000 keys" is a very
/// different claim from "the snapshot is retrievable". Sampling is a smoke
/// test for *did the storage receive anything at all*; it cannot detect
/// individual losses in between.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleCoverage {
    /// Probe the index plus at most `n` chunk keys, spread deterministically
    /// across the snapshot (first, last, and an even stride between).
    Sample(usize),
    /// Probe the index and **every** chunk. Proportional to the snapshot and
    /// the only setting that actually establishes retrievability.
    All,
}

impl Default for SettleCoverage {
    fn default() -> Self {
        SettleCoverage::Sample(8)
    }
}

/// Publish a whole snapshot — chunks then index — into the storage, then
/// **verify by reading back** (see the module docs) within `settle`. After
/// this resolves the producer may exit: a client can fetch the snapshot.
///
/// Only the chunks `index` references are published, not the whole `store`.
///
/// Note what the read-back can and cannot tell you. Any responder counts, so
/// a `TreeServer` running on the same prefix answers the probes and the phase
/// reports success without a storage having retained anything — which is easy
/// to arrange accidentally in development. And under
/// [`SettleCoverage::Sample`] the unprobed chunks are simply unknown. Use
/// [`SettleCoverage::All`] when the producer is about to exit and the
/// snapshot has to be there.
#[allow(clippy::too_many_arguments)]
pub async fn publish_snapshot(
    session: &zenoh::Session,
    store_prefix: &str,
    tree_prefix: &str,
    index: &TreeIndex,
    store: &dyn ContentStore,
    compression: ChunkCompression,
    coverage: SettleCoverage,
    settle: Duration,
) -> Result<()> {
    publish_snapshot_chunks(session, store_prefix, index, store, compression).await?;
    publish_index(session, tree_prefix, index).await?;

    // Read-back: the index, plus chunk keys per `coverage` (deterministic,
    // no RNG).
    let needed = index.needed_chunks();
    let mut probes: Vec<String> = vec![tree_key(tree_prefix, &index.id)];
    let n = needed.len();
    if n > 0 {
        let picked: Vec<usize> = match coverage {
            SettleCoverage::All => (0..n).collect(),
            SettleCoverage::Sample(k) => {
                let k = k.max(1);
                let step = (n / k).max(1);
                let mut picked: Vec<usize> = (0..n).step_by(step).take(k).collect();
                picked.push(n - 1);
                picked.dedup();
                picked
            }
        };
        probes.extend(
            picked
                .into_iter()
                .map(|i| store_key(store_prefix, Hash::ALGO, &needed[i])),
        );
    }

    let deadline = tokio::time::Instant::now() + settle;
    for key in probes {
        loop {
            if probe_key(session, &key).await? {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BlobError::Protocol(format!(
                    "storage did not settle within {settle:?}: {key} still unanswered"
                )));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

/// One GET probe: does anything answer for `key`?
async fn probe_key(session: &zenoh::Session, key: &str) -> Result<bool> {
    let replies = session
        .get(key)
        .consolidation(ConsolidationMode::None)
        .priority(Priority::DataLow)
        .timeout(Duration::from_secs(2))
        .await
        .map_err(BlobError::zenoh)?;
    while let Ok(reply) = replies.recv_async().await {
        if reply.result().is_ok() {
            return Ok(true);
        }
    }
    Ok(false)
}
