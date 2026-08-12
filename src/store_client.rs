//! Reading the content-addressed chunk store directly (Tier 2's third key
//! family).
//!
//! [`TreeClient`](crate::TreeClient) reads chunks as a side effect of pulling a
//! snapshot. Plenty of callers have the other shape: a bare
//! `store/<algo>/<hash>` address and no tree at all — an explorer resolving a
//! reference, a tool checking whether an origin holds a chunk, an application
//! whose own metadata names chunks. Making such a caller construct a
//! `TreeClient` around a dummy tree prefix to reach one chunk is the kind of
//! API that gets worked around rather than used, so the store gets its own
//! client.
//!
//! Everything the tree path guarantees holds here, because it is the same
//! code: the reply is unframed from its §2.4 self-describing container and
//! re-hashed against the address before it is returned, an unusable reply is
//! skipped rather than fatal, and the first good replier wins.

use std::sync::Arc;
use std::time::Duration;

use zenoh::qos::Priority;
use zenoh::query::ConsolidationMode;

use crate::compress::{MAX_UNPACKED, unpack};
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::prefix::QueryPrefix;
use crate::store_key;
use crate::wire::ENC_CHUNK;

#[derive(Debug, Clone)]
pub(crate) struct StoreClientConfig {
    pub(crate) query_timeout: Duration,
    pub(crate) priority: Priority,
    pub(crate) max_chunk_bytes: usize,
}

impl Default for StoreClientConfig {
    fn default() -> Self {
        StoreClientConfig {
            query_timeout: Duration::from_secs(30),
            // Bulk transfer yields; see `BlobClientBuilder::priority`.
            priority: Priority::DataLow,
            max_chunk_bytes: MAX_UNPACKED,
        }
    }
}

/// Reads single chunks from a content-addressed store by their hash.
pub struct StoreClient {
    session: Arc<zenoh::Session>,
    prefix: QueryPrefix,
    cfg: StoreClientConfig,
}

/// Builder for a [`StoreClient`].
pub struct StoreClientBuilder {
    session: Arc<zenoh::Session>,
    prefix: QueryPrefix,
    cfg: StoreClientConfig,
}

impl StoreClientBuilder {
    /// Per-query timeout (default 30 s).
    pub fn query_timeout(mut self, t: Duration) -> Self {
        self.cfg.query_timeout = t;
        self
    }

    /// Zenoh priority for the queries this client issues (default
    /// [`Priority::DataLow`]).
    pub fn priority(mut self, priority: Priority) -> Self {
        self.cfg.priority = priority;
        self
    }

    /// Largest chunk this client will accept when the caller does not already
    /// know the length (default 16 MiB, the container format's own ceiling).
    ///
    /// Prefer [`StoreClient::fetch_chunk_sized`] where an index has already
    /// stated the length: a specific bound beats a generic one.
    pub fn max_chunk_bytes(mut self, n: usize) -> Self {
        self.cfg.max_chunk_bytes = n.min(MAX_UNPACKED);
        self
    }

    /// Build the client.
    pub fn build(self) -> StoreClient {
        StoreClient {
            session: self.session,
            prefix: self.prefix,
            cfg: self.cfg,
        }
    }
}

impl StoreClient {
    /// Start building a client for the store under `store_prefix`.
    pub fn builder(session: Arc<zenoh::Session>, store_prefix: QueryPrefix) -> StoreClientBuilder {
        StoreClientBuilder {
            session,
            prefix: store_prefix,
            cfg: StoreClientConfig::default(),
        }
    }

    /// Build a client with default configuration.
    pub fn new(session: Arc<zenoh::Session>, store_prefix: QueryPrefix) -> Self {
        Self::builder(session, store_prefix).build()
    }

    /// The store prefix this client reads from.
    pub fn prefix(&self) -> &QueryPrefix {
        &self.prefix
    }

    /// GET one content-addressed chunk and return its raw (unframed) bytes,
    /// verified against `hash`.
    ///
    /// A reply that is malformed, over the size bound, or does not hash to
    /// `hash` is skipped and the fetch waits for a good replier —
    /// substitution and corruption are both impossible past this point. Fails
    /// with [`BlobError::NotFound`] if nobody answers acceptably.
    pub async fn fetch_chunk(&self, hash: &Hash) -> Result<Vec<u8>> {
        let key = store_key(self.prefix.as_str(), Hash::ALGO, hash);
        let (bytes, _) = fetch_one_chunk(
            &self.session,
            &key,
            hash,
            None,
            self.cfg.max_chunk_bytes,
            self.cfg.query_timeout,
            self.cfg.priority,
        )
        .await?;
        Ok(bytes)
    }

    /// [`fetch_chunk`](Self::fetch_chunk) for a caller that already knows the
    /// chunk's length — from a [`ChunkRef`](crate::ChunkRef), typically.
    ///
    /// The length is enforced exactly, and bounds the reply *before* it is
    /// unframed, so a holder cannot pick the allocation.
    pub async fn fetch_chunk_sized(&self, hash: &Hash, len: u32) -> Result<Vec<u8>> {
        let key = store_key(self.prefix.as_str(), Hash::ALGO, hash);
        let (bytes, _) = fetch_one_chunk(
            &self.session,
            &key,
            hash,
            Some(len),
            self.cfg.max_chunk_bytes,
            self.cfg.query_timeout,
            self.cfg.priority,
        )
        .await?;
        Ok(bytes)
    }
}

/// GET one content-addressed chunk and verify it by re-hashing — the one
/// implementation behind both [`StoreClient`] and the tree download path.
///
/// `expected_len`, when known, is enforced exactly; otherwise `max_bytes`
/// applies. Either way the bound is checked *before* the container is
/// unframed, so a hostile holder cannot choose how much this process
/// allocates. Returns the bytes and how many replies were rejected on the way.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_one_chunk(
    session: &zenoh::Session,
    key: &str,
    hash: &Hash,
    expected_len: Option<u32>,
    max_bytes: usize,
    timeout: Duration,
    priority: Priority,
) -> Result<(Vec<u8>, u32)> {
    // A container is one tag byte plus the chunk (a compressed frame is
    // smaller still, plus a 4-byte length), so anything longer than this
    // cannot be the chunk that was asked for.
    let max_frame = expected_len.map_or(max_bytes, |n| n as usize) + 1 + 4;

    let mut rejected = 0u32;
    let replies = session
        .get(key)
        .consolidation(ConsolidationMode::None)
        .priority(priority)
        .timeout(timeout)
        .await
        .map_err(BlobError::zenoh)?;
    while let Ok(reply) = replies.recv_async().await {
        let Ok(sample) = reply.result() else { continue };
        if sample.encoding().to_string() != ENC_CHUNK {
            continue;
        }
        let payload = sample.payload().to_bytes();
        if payload.len() > max_frame {
            rejected += 1;
            continue; // over-long for the chunk asked for; do not unframe it.
        }
        let Ok(bytes) = unpack(&payload) else {
            rejected += 1;
            continue; // malformed frame; wait for a good replier.
        };
        if expected_len.is_some_and(|n| bytes.len() as u32 != n) || Hash::of(&bytes) != *hash {
            rejected += 1;
            continue; // hostile or corrupt replier; wait for a good one.
        }
        return Ok((bytes, rejected));
    }
    Err(BlobError::NotFound(hash.to_string()))
}
