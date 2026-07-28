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

use zenoh::qos::Priority;
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
        .await
        .map_err(BlobError::zenoh)
}

/// PUT every chunk of `store` into the storage. Returns how many were
/// published. Stops at the first PUT error.
pub async fn publish_store(
    session: &zenoh::Session,
    store_prefix: &str,
    store: &dyn ContentStore,
    compression: ChunkCompression,
) -> Result<u32> {
    let mut published = 0u32;
    for hash in store.hashes()? {
        let bytes = store
            .get(&hash)
            .ok_or_else(|| BlobError::NotFound(hash.to_string()))?;
        publish_chunk(session, store_prefix, &hash, &bytes, compression).await?;
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
        .priority(Priority::DataLow)
        .await
        .map_err(BlobError::zenoh)
}

/// Publish a whole snapshot — chunks then index — into the storage, then
/// **verify by reading back** (see the module docs) within `settle`. After
/// this resolves the producer may exit: a client can fetch the snapshot.
pub async fn publish_snapshot(
    session: &zenoh::Session,
    store_prefix: &str,
    tree_prefix: &str,
    index: &TreeIndex,
    store: &dyn ContentStore,
    compression: ChunkCompression,
    settle: Duration,
) -> Result<()> {
    publish_store(session, store_prefix, store, compression).await?;
    publish_index(session, tree_prefix, index).await?;

    // Read-back: the index, plus a bounded sample of chunk keys (first, last,
    // and a spread between — deterministic, no RNG).
    let needed = index.needed_chunks();
    let mut probes: Vec<String> = vec![tree_key(tree_prefix, &index.id)];
    let n = needed.len();
    if n > 0 {
        let step = (n / 6).max(1);
        let mut picked: Vec<usize> = (0..n).step_by(step).collect();
        picked.push(n - 1);
        picked.dedup();
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
