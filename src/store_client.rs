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

use std::time::Duration;

use zenoh::qos::Priority;
use zenoh::query::ConsolidationMode;

use crate::compress::{MAX_UNPACKED, unpack};
use crate::error::{BlobError, Result};
use crate::hash::{Hash, HashAlgo};
use crate::prefix::QueryPrefix;
use crate::store_key;
use crate::tree::ChunkRef;
use crate::wire::ENC_CHUNK;
use crate::wire::{HaveBits, WantList};

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

/// What one origin holds, from [`StoreClient::probe`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkProbe {
    /// The store prefix that answered — fetch from it to get these chunks.
    pub origin: QueryPrefix,
    /// The subset of the asked-about addresses this origin holds.
    pub held: Vec<Hash>,
}

/// Reads single chunks from a content-addressed store by their hash.
#[derive(Debug)]
pub struct StoreClient {
    session: zenoh::Session,
    prefix: QueryPrefix,
    cfg: StoreClientConfig,
}

/// Builder for a [`StoreClient`].
#[derive(Debug)]
pub struct StoreClientBuilder {
    session: zenoh::Session,
    prefix: QueryPrefix,
    cfg: StoreClientConfig,
}

impl StoreClientBuilder {
    /// Per-query timeout (default 30 s).
    #[must_use]
    pub fn query_timeout(mut self, t: Duration) -> Self {
        self.cfg.query_timeout = t;
        self
    }

    /// Zenoh priority for the queries this client issues (default
    /// [`Priority::DataLow`]).
    #[must_use]
    pub fn priority(mut self, priority: Priority) -> Self {
        self.cfg.priority = priority;
        self
    }

    /// Largest chunk this client will accept when the caller does not already
    /// know the length (default 16 MiB, the container format's own ceiling).
    ///
    /// Prefer [`StoreClient::fetch_chunk_sized`] where an index has already
    /// stated the length: a specific bound beats a generic one.
    #[must_use]
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
    pub fn builder(session: &zenoh::Session, store_prefix: QueryPrefix) -> StoreClientBuilder {
        StoreClientBuilder {
            session: session.clone(),
            prefix: store_prefix,
            cfg: StoreClientConfig::default(),
        }
    }

    /// Build a client with default configuration.
    pub fn new(session: &zenoh::Session, store_prefix: QueryPrefix) -> Self {
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
        let key = store_key(self.prefix.as_str(), HashAlgo::Blake3, hash);
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

    /// Fetch many chunks in one query round.
    ///
    /// Returns the chunks that came back verified; a holder answers only what
    /// it has, so a short result is normal and not an error. Anything absent
    /// is the caller's to ask for elsewhere — [`fetch_chunk`](Self::fetch_chunk)
    /// against a router storage, typically, which serves by key and so never
    /// answers a batch at all.
    ///
    /// The list is capped at [`MAX_WANT_LIST`](crate::wire::MAX_WANT_LIST);
    /// longer input is split across rounds.
    ///
    /// **Holds every returned chunk in memory.** For a whole snapshot use
    /// [`TreeClient::download_tree`](crate::TreeClient::download_tree), which
    /// streams each round into a store instead.
    pub async fn fetch_many(&self, wanted: &[ChunkRef]) -> Result<Vec<(Hash, Vec<u8>)>> {
        let mut out = Vec::new();
        for group in wanted.chunks(crate::wire::MAX_WANT_LIST) {
            let (replies, expected) = batch_query(
                &self.session,
                self.prefix.as_str(),
                group,
                self.cfg.query_timeout,
                self.cfg.priority,
            )
            .await?;
            let mut seen = std::collections::HashSet::new();
            while let Ok(reply) = replies.recv_async().await {
                let Ok(sample) = reply.result() else { continue };
                if let Some((hash, bytes)) =
                    accept_batch_reply(self.prefix.as_str(), sample, &expected)
                    && seen.insert(hash)
                {
                    out.push((hash, bytes));
                }
            }
        }
        Ok(out)
    }

    /// Ask which of `hashes` each answering holder has.
    ///
    /// This is tier 2's probe, and the reason it can exist: the reply is one
    /// bit per address asked about, so its size is a function of the
    /// *question*, never of the objects. A tier-2 *fetch* fanned across
    /// origins would be one full copy per responder, which RFC 07 §3 forbids;
    /// a bitfield over a caller-supplied list cannot be bulk, so probing tier 2
    /// across origins is as legitimate as probing tier 1 — and
    /// probe-then-fetch becomes total across all three key families instead of
    /// being available on one of them.
    ///
    /// Each result names the origin that answered, so the follow-up fetch can
    /// go to a holder that actually has the chunks.
    pub async fn probe(&self, hashes: &[Hash]) -> Result<Vec<ChunkProbe>> {
        let mut out = Vec::new();
        for group in hashes.chunks(crate::wire::MAX_WANT_LIST) {
            for (key, bits) in probe_chunks(
                &self.session,
                self.prefix.as_str(),
                group,
                self.cfg.query_timeout,
                self.cfg.priority,
            )
            .await?
            {
                // The reply key is `<origin>/<algo>/have`; the origin is what
                // a caller needs in order to fetch from this holder.
                let Some(origin) = key
                    .strip_suffix(crate::STORE_HAVE)
                    .and_then(|k| k.strip_suffix('/'))
                    .and_then(|k| k.strip_suffix(Hash::ALGO))
                    .and_then(|k| k.strip_suffix('/'))
                else {
                    continue;
                };
                let Ok(origin) = QueryPrefix::new(origin) else {
                    continue;
                };
                let held = group
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| bits.is_set(*i as u32))
                    .map(|(_, h)| *h)
                    .collect::<Vec<_>>();
                match out
                    .iter_mut()
                    .find(|p: &&mut ChunkProbe| p.origin == origin)
                {
                    Some(existing) => existing.held.extend(held),
                    None => out.push(ChunkProbe { origin, held }),
                }
            }
        }
        Ok(out)
    }

    /// [`fetch_chunk`](Self::fetch_chunk) for a caller that already knows the
    /// chunk's length — from a [`ChunkRef`](crate::ChunkRef), typically.
    ///
    /// The length is enforced exactly, and bounds the reply *before* it is
    /// unframed, so a holder cannot pick the allocation.
    pub async fn fetch_chunk_sized(&self, hash: &Hash, len: u32) -> Result<Vec<u8>> {
        let key = store_key(self.prefix.as_str(), HashAlgo::Blake3, hash);
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

/// Issue one batched want-list query and hand back the reply stream, together
/// with the map of what was asked for.
///
/// Kept separate from the draining so both callers share the *security*
/// half — which reply counts as an answer — while each drains at the pace it
/// can afford. The tree path must stream: a round of 256 chunks at the CDC
/// maximum would be gigabytes if collected first.
pub(crate) async fn batch_query(
    session: &zenoh::Session,
    store_prefix: &str,
    wanted: &[ChunkRef],
    timeout: Duration,
    priority: Priority,
) -> Result<(
    zenoh::handlers::FifoChannelHandler<zenoh::query::Reply>,
    std::collections::HashMap<Hash, u32>,
)> {
    let expected: std::collections::HashMap<Hash, u32> =
        wanted.iter().map(|c| (c.hash, c.len)).collect();
    let want = WantList::new(wanted.iter().map(|c| c.hash).collect());
    let replies = session
        .get(crate::store_batch_key(store_prefix, HashAlgo::Blake3))
        .payload(crate::wire::encode(&want)?)
        // The replies land on each chunk's own key, which does not intersect
        // this one. Without this the *server* refuses them, once per chunk.
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .consolidation(ConsolidationMode::None)
        .priority(priority)
        .timeout(timeout)
        .await
        .map_err(BlobError::zenoh)?;
    Ok((replies, expected))
}

/// Decide whether one batch reply is an answer, and to which address.
///
/// `ReplyKeyExpr::Any` means the reply key is no longer constrained for us, so
/// everything is checked here: that it is a chunk reply, that its key is under
/// our own store prefix, that we asked for that address, that it is not
/// over-long, and finally that the bytes hash to the address. One bad chunk in
/// a batch costs only itself.
pub(crate) fn accept_batch_reply(
    store_prefix: &str,
    sample: &zenoh::sample::Sample,
    expected: &std::collections::HashMap<Hash, u32>,
) -> Option<(Hash, Vec<u8>)> {
    if !ENC_CHUNK.matches(sample.encoding()) {
        return None;
    }
    let tail = crate::parse_tier2_tail(store_prefix, sample.key_expr().as_str())?;
    let [algo, hex] = tail[..] else { return None };
    if algo != Hash::ALGO {
        return None;
    }
    let hash: Hash = hex.parse().ok()?;
    let len = *expected.get(&hash)?;
    let payload = sample.payload().to_bytes();
    if payload.len() > len as usize + 1 + 4 {
        return None; // over-long for the declared chunk; do not unframe it
    }
    let bytes = unpack(&payload).ok()?;
    if bytes.len() as u32 != len || Hash::of(&bytes) != hash {
        return None;
    }
    Some((hash, bytes))
}

/// Ask a holder which of `hashes` it has. The reply is one bit each, so its
/// size is a function of the question rather than of the chunks.
pub(crate) async fn probe_chunks(
    session: &zenoh::Session,
    store_prefix: &str,
    hashes: &[Hash],
    timeout: Duration,
    priority: Priority,
) -> Result<Vec<(String, HaveBits)>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    let want = WantList::new(hashes.to_vec());
    let replies = session
        .get(crate::store_have_key(store_prefix, HashAlgo::Blake3))
        .payload(crate::wire::encode(&want)?)
        .consolidation(ConsolidationMode::None)
        .priority(priority)
        .timeout(timeout)
        .await
        .map_err(BlobError::zenoh)?;
    let mut out = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        let Ok(sample) = reply.result() else { continue };
        if !crate::wire::ENC_HAVEBITS.matches(sample.encoding()) {
            continue;
        }
        let Ok(bits) = crate::wire::decode::<HaveBits>(&sample.payload().to_bytes()) else {
            continue;
        };
        if bits.validate(hashes.len()).is_err() {
            continue;
        }
        out.push((sample.key_expr().as_str().to_string(), bits));
    }
    Ok(out)
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
        if !ENC_CHUNK.matches(sample.encoding()) {
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
