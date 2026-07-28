//! The blob client: fetches the manifest, then streams verified bao slices
//! into a caller-chosen destination, resuming interrupted transfers.
//!
//! Every reply is verified against the manifest's BLAKE3 `root` **before** it
//! touches disk (see [`crate::verify`]), so there is no end-of-transfer hash
//! pass and a tampered reply poisons nothing — it is simply dropped and its
//! chunk re-requested. Resume state is a chunk bitfield persisted next to the
//! `.part` file (see [`crate::resume`]); every retry re-derives its query from
//! the bitfield's holes, so "resume" and "retry" are the same code path.
//!
//! The caller chooses the destination path ([`BlobClient::download_to`]) — the
//! server's advisory filename is never joined to any path (the v1 traversal
//! vector, C2).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use zenoh::query::ConsolidationMode;

use crate::cancel::CancelToken;
use crate::chunk::TransferChunks;
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::manifest::{Manifest, validate_id};
use crate::obs::{TransferStats, zdebug};
use crate::progress::{Progress, ProgressSink};
use crate::resume::ResumeState;
use crate::wire::{ENC_MANIFEST, ENC_SLICE, decode};
use crate::{MAX_RANGE_SPANS, manifest_key, slice_selector, verify};

/// What to do when the destination path already exists at completion time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overwrite {
    /// Fail with [`BlobError::DestinationExists`], keeping the finished
    /// `.part` next to the destination (nothing is lost). The default.
    #[default]
    Refuse,
    /// Atomically replace the existing file.
    Replace,
}

/// Retry/backoff policy for the download loop. An *attempt* is a slice query
/// that made no progress; queries that verify at least one new chunk reset
/// the budget.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Consecutive no-progress attempts before giving up (default 5).
    pub max_attempts: u32,
    /// Backoff before retry attempt 1 (doubles each attempt; default 250 ms).
    pub base_backoff: Duration,
    /// Backoff ceiling (default 10 s).
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 5,
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    fn backoff(&self, attempt: u32) -> Duration {
        let exp = self.base_backoff.saturating_mul(1u32 << attempt.min(16));
        exp.min(self.max_backoff)
    }
}

#[derive(Debug, Clone)]
struct ClientConfig {
    query_timeout: Duration,
    retry: RetryPolicy,
    max_chunks_per_query: u32,
    max_blob_size: u64,
    overwrite: Overwrite,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            query_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
            max_chunks_per_query: 512,
            max_blob_size: 1 << 40, // 1 TiB — a remote peer must not size our disk.
            overwrite: Overwrite::default(),
        }
    }
}

/// What to download: a blob id, optionally with a pinned BLAKE3 root.
///
/// **Pin the root whenever you know it.** The manifest travels over the same
/// channel as the data, so without a pin the first fetch is
/// trust-on-first-use: integrity holds *within* the transfer (a server cannot
/// mix content), but the server chooses *which* content. With
/// [`DownloadRequest::pinned`], a server offering different bytes is rejected
/// before anything is written.
#[derive(Debug, Clone)]
pub struct DownloadRequest {
    /// The blob id to fetch.
    pub id: String,
    /// If set, the transfer fails unless the manifest's root matches exactly.
    pub expected_root: Option<Hash>,
}

impl DownloadRequest {
    /// Fetch `id`, trusting the manifest's root (TOFU — see type docs).
    pub fn new(id: impl Into<String>) -> Self {
        DownloadRequest {
            id: id.into(),
            expected_root: None,
        }
    }

    /// Fetch `id` and require its content to match `root`.
    pub fn pinned(id: impl Into<String>, root: Hash) -> Self {
        DownloadRequest {
            id: id.into(),
            expected_root: Some(root),
        }
    }
}

/// Downloads blobs served by a [`crate::BlobServer`] under the same key prefix.
pub struct BlobClient {
    session: Arc<zenoh::Session>,
    prefix: String,
    cfg: ClientConfig,
}

/// Builder for a [`BlobClient`] (see [`BlobClient::builder`]).
pub struct BlobClientBuilder {
    session: Arc<zenoh::Session>,
    prefix: String,
    cfg: ClientConfig,
}

impl BlobClientBuilder {
    /// Per-query timeout (default 30 s). Transfers larger than one query's
    /// chunk budget span multiple queries, so this bounds *stall* time, not
    /// total transfer time.
    pub fn query_timeout(mut self, t: Duration) -> Self {
        self.cfg.query_timeout = t;
        self
    }

    /// Retry/backoff policy (default: 5 attempts, 250 ms base, 10 s cap).
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.cfg.retry = retry;
        self
    }

    /// Max chunks requested per query (default 512; must not exceed the
    /// server's own cap or queries are rejected).
    pub fn max_chunks_per_query(mut self, n: u32) -> Self {
        self.cfg.max_chunks_per_query = n.max(1);
        self
    }

    /// Upper bound on `total_len` this client will accept from a manifest
    /// (default 1 TiB) — the allocation/preallocation defense.
    pub fn max_blob_size(mut self, bytes: u64) -> Self {
        self.cfg.max_blob_size = bytes;
        self
    }

    /// Overwrite policy for the destination (default [`Overwrite::Refuse`]).
    pub fn overwrite(mut self, ow: Overwrite) -> Self {
        self.cfg.overwrite = ow;
        self
    }

    /// Build the client.
    pub fn build(self) -> BlobClient {
        BlobClient {
            session: self.session,
            prefix: self.prefix,
            cfg: self.cfg,
        }
    }
}

impl BlobClient {
    /// Start building a client for blobs under `key_prefix`.
    pub fn builder(
        session: Arc<zenoh::Session>,
        key_prefix: impl Into<String>,
    ) -> BlobClientBuilder {
        BlobClientBuilder {
            session,
            prefix: key_prefix.into(),
            cfg: ClientConfig::default(),
        }
    }

    /// Build a client with default configuration (see [`BlobClient::builder`]).
    pub fn new(session: Arc<zenoh::Session>, key_prefix: impl Into<String>) -> Self {
        Self::builder(session, key_prefix).build()
    }

    /// Fetch and validate just the manifest for blob `id` — probe existence,
    /// size, and root before committing to a download.
    pub async fn fetch_manifest(&self, id: &str) -> Result<Manifest> {
        validate_id(id)?;
        let key = manifest_key(&self.prefix, id);
        let replies = self
            .session
            .get(&key)
            .consolidation(ConsolidationMode::None)
            .timeout(self.cfg.query_timeout)
            .await
            .map_err(BlobError::zenoh)?;
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            if sample.encoding().to_string() != ENC_MANIFEST {
                continue; // stale/foreign responder; keep listening.
            }
            let manifest: Manifest = decode(&sample.payload().to_bytes())?;
            manifest.validate(self.cfg.max_blob_size)?;
            if manifest.id != id {
                return Err(BlobError::Protocol(format!(
                    "manifest id {:?} does not match requested {id:?}",
                    manifest.id
                )));
            }
            return Ok(manifest);
        }
        Err(BlobError::NotFound(id.to_string()))
    }

    /// Download a blob to the file at `dest` (written via `<dest>.part` + a
    /// resume sidecar, then atomically renamed into place). Progress events go
    /// to `sink`; a set `cancel` stops cooperatively with state persisted.
    /// Returns per-call [`TransferStats`].
    ///
    /// Call again with the same arguments to resume after `Incomplete`,
    /// `Cancelled`, or a crash.
    pub async fn download_to(
        &self,
        req: &DownloadRequest,
        dest: &Path,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
    ) -> Result<TransferStats> {
        let result = self.download_to_inner(req, dest, sink, cancel).await;
        match &result {
            Err(BlobError::Cancelled { .. }) | Ok(_) => {}
            Err(e) => sink.emit(Progress::Failed {
                error: e.to_string(),
            }),
        }
        result
    }

    /// Delete any partial download + sidecar for a previous
    /// [`download_to`](Self::download_to) call with destination `dest`.
    pub async fn delete_partial(&self, dest: &Path) {
        let part = part_path(dest);
        let _ = tokio::fs::remove_file(&part).await;
        ResumeState::remove(&part).await;
    }

    async fn download_to_inner(
        &self,
        req: &DownloadRequest,
        dest: &Path,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
    ) -> Result<TransferStats> {
        let started_at = tokio::time::Instant::now();
        let (manifest, chunks) = self.start(req).await?;
        let count = chunks.count();
        zdebug!(id = %manifest.id, total_len = manifest.total_len, chunks = count, "download start");

        if let Some(parent) = dest.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let part = part_path(dest);

        // Resume probe: reuse a matching partial, else start fresh. A sidecar
        // that doesn't match (different id/root/geometry → a regenerated
        // source) is discarded rather than spliced.
        let existing_len = tokio::fs::metadata(&part).await.map(|m| m.len()).ok();
        let mut state = match ResumeState::load(&part).await {
            Some(s) if s.matches(&manifest, count) && existing_len == Some(manifest.total_len) => {
                sink.emit(Progress::Resumed {
                    received: s.received(),
                    total: count,
                });
                s
            }
            _ => {
                let file = tokio::fs::File::create(&part).await?;
                file.set_len(manifest.total_len).await?;
                let fresh = ResumeState::fresh(&manifest, count);
                fresh.save_atomic(&part).await?;
                sink.emit(Progress::Started {
                    total_len: manifest.total_len,
                    chunk_count: count,
                });
                fresh
            }
        };

        // Open the partial for writing without truncating (we may be resuming).
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&part)
            .await?;

        let mut stats = TransferStats {
            chunks_resumed: state.received(),
            ..Default::default()
        };
        self.fill_holes(
            &manifest, &chunks, &mut file, &mut state, &part, sink, cancel, &mut stats,
        )
        .await?;

        // Every byte on disk was verified against the root as it was written —
        // no second hash pass. Make it durable, then move it into place.
        file.sync_data().await?;
        drop(file);
        if self.cfg.overwrite == Overwrite::Refuse && tokio::fs::try_exists(dest).await? {
            return Err(BlobError::DestinationExists(dest.to_path_buf()));
        }
        tokio::fs::rename(&part, dest).await?;
        ResumeState::remove(&part).await;
        sink.emit(Progress::Completed {
            path: dest.to_path_buf(),
        });
        stats.elapsed = started_at.elapsed();
        zdebug!(id = %manifest.id, fetched = stats.chunks_fetched, rejected = stats.rejected, "download complete");
        Ok(stats)
    }

    /// Fetch + validate the manifest and enforce root pinning.
    async fn start(&self, req: &DownloadRequest) -> Result<(Manifest, TransferChunks)> {
        let manifest = self.fetch_manifest(&req.id).await?;
        if let Some(expected) = req.expected_root
            && manifest.root != expected
        {
            return Err(BlobError::RootMismatch {
                expected,
                actual: manifest.root,
            });
        }
        // An empty blob carries no slices, so nothing later proves the root;
        // check it here or a server could serve "verified" emptiness.
        if manifest.total_len == 0 && manifest.root != Hash::of(b"") {
            return Err(BlobError::RootMismatch {
                expected: Hash::of(b""),
                actual: manifest.root,
            });
        }
        let chunks = manifest.chunks()?;
        Ok((manifest, chunks))
    }

    /// The retry/resume loop: query the bitfield's holes until full or out of
    /// attempts. Progress ≡ the bitfield, so resume and retry are one path.
    #[allow(clippy::too_many_arguments)]
    async fn fill_holes(
        &self,
        manifest: &Manifest,
        chunks: &TransferChunks,
        file: &mut tokio::fs::File,
        state: &mut ResumeState,
        part: &Path,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        stats: &mut TransferStats,
    ) -> Result<()> {
        let count = chunks.count();
        let root: blake3::Hash = manifest.root.into();
        let mut bytes_received: u64 = (0..count)
            .filter(|i| state.is_set(*i))
            .map(|i| chunks.len_of(i) as u64)
            .sum();
        let mut no_progress = 0u32;
        let mut marks_since_save = 0u32;
        let mut last_save = tokio::time::Instant::now();

        while !state.is_complete(count) {
            if cancel.is_cancelled() {
                return self.persist_cancel(file, state, part, sink, count).await;
            }

            // Take as many holes as one query may carry.
            let mut holes = state.missing_ranges(count);
            holes.truncate(MAX_RANGE_SPANS);
            let mut budget = self.cfg.max_chunks_per_query;
            for r in holes.iter_mut() {
                let take = (r.end - r.start).min(budget);
                r.end = r.start + take;
                budget -= take;
            }
            holes.retain(|r| !r.is_empty());

            let selector = slice_selector(&self.prefix, &manifest.id, &holes);
            let before = state.received();
            let replies = self
                .session
                .get(&selector)
                .consolidation(ConsolidationMode::None)
                .timeout(self.cfg.query_timeout)
                .await
                .map_err(BlobError::zenoh)?;

            while let Ok(reply) = replies.recv_async().await {
                if cancel.is_cancelled() {
                    drop(replies);
                    return self.persist_cancel(file, state, part, sink, count).await;
                }
                let Ok(sample) = reply.result() else { continue };
                if sample.encoding().to_string() != ENC_SLICE {
                    continue;
                }
                let Some(index) = parse_slice_index(sample.key_expr().as_str()) else {
                    continue;
                };
                if index >= count || state.is_set(index) {
                    continue; // duplicate (consolidation None) or nonsense.
                }
                // Verify-decode this slice against the pinned root; a bad
                // slice is dropped alone and its chunk stays a hole.
                let payload = sample.payload().to_bytes();
                let byte_range = chunks.byte_range(index);
                let mut leaves: Vec<(u64, Vec<u8>)> = Vec::new();
                let decoded = verify::decode_slice(
                    &root,
                    manifest.total_len,
                    verify::chunk_range(byte_range),
                    &payload,
                    |off, data| {
                        leaves.push((off, data.to_vec()));
                        Ok(())
                    },
                );
                if decoded.is_err() {
                    stats.rejected += 1;
                    zdebug!(id = %manifest.id, index, "rejected unverifiable slice");
                    continue;
                }
                for (off, data) in leaves {
                    file.seek(SeekFrom::Start(off)).await?;
                    file.write_all(&data).await?;
                }
                if state.mark(index) {
                    bytes_received += chunks.len_of(index) as u64;
                    stats.chunks_fetched += 1;
                    stats.bytes_fetched += chunks.len_of(index) as u64;
                    marks_since_save += 1;
                    sink.emit(Progress::Chunk {
                        index,
                        received: state.received(),
                        total: count,
                        bytes_received,
                    });
                    // Batched persistence: data first (sync_data), then bits —
                    // bits must never claim data the OS hasn't received.
                    if marks_since_save >= 64 || last_save.elapsed() > Duration::from_secs(2) {
                        file.sync_data().await?;
                        state.save_atomic(part).await?;
                        marks_since_save = 0;
                        last_save = tokio::time::Instant::now();
                    }
                }
            }

            if state.is_complete(count) {
                break;
            }
            if state.received() == before {
                no_progress += 1;
                stats.retries += 1;
                zdebug!(id = %manifest.id, attempt = no_progress, "no progress; backing off");
                if no_progress >= self.cfg.retry.max_attempts {
                    file.sync_data().await?;
                    state.save_atomic(part).await?;
                    return Err(BlobError::Incomplete {
                        received: state.received(),
                        total: count,
                    });
                }
                tokio::time::sleep(self.cfg.retry.backoff(no_progress - 1)).await;
            } else {
                no_progress = 0;
            }
        }

        file.sync_data().await?;
        state.save_atomic(part).await?;
        Ok(())
    }

    async fn persist_cancel(
        &self,
        file: &mut tokio::fs::File,
        state: &ResumeState,
        part: &Path,
        sink: &dyn ProgressSink,
        count: u32,
    ) -> Result<()> {
        file.sync_data().await?;
        state.save_atomic(part).await?;
        sink.emit(Progress::Cancelled {
            received: state.received(),
            total: count,
        });
        Err(BlobError::Cancelled {
            received: state.received(),
            total: count,
        })
    }
}

/// The partial-file path for a destination: `<dest>.part`.
fn part_path(dest: &Path) -> PathBuf {
    let mut p = dest.as_os_str().to_os_string();
    p.push(".part");
    PathBuf::from(p)
}

/// Parse the slice index from a `…/slice/<index>` key, if present.
fn parse_slice_index(key: &str) -> Option<u32> {
    let (head, idx) = key.rsplit_once('/')?;
    if head.ends_with("/slice") {
        idx.parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_index_parsing() {
        assert_eq!(parse_slice_index("p/A/slice/7"), Some(7));
        assert_eq!(parse_slice_index("p/A/manifest"), None);
        assert_eq!(parse_slice_index("p/A/slice/x"), None);
        assert_eq!(parse_slice_index("p/A/chunk/7"), None); // v1 keys fail closed
    }

    #[test]
    fn part_path_appends_extension() {
        assert_eq!(
            part_path(Path::new("/tmp/out/file.bin")),
            Path::new("/tmp/out/file.bin.part")
        );
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let r = RetryPolicy::default();
        assert_eq!(r.backoff(0), Duration::from_millis(250));
        assert_eq!(r.backoff(1), Duration::from_millis(500));
        assert_eq!(r.backoff(10), Duration::from_secs(10)); // capped
    }
}
