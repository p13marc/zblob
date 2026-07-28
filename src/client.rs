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
use zenoh::qos::Priority;
use zenoh::query::ConsolidationMode;

use crate::cancel::CancelToken;
use crate::chunk::TransferChunks;
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::manifest::{BlobSpec, Manifest, validate_id};
use crate::obs::{TransferStats, zdebug};
use crate::progress::{Progress, ProgressSink};
use crate::resume::ResumeState;
use crate::wire::{Availability, ENC_AVAIL, ENC_MANIFEST, ENC_PUSH, ENC_SLICE, decode};
use crate::{
    MAX_RANGE_SPANS, availability_key, manifest_key, push_offer_key, push_slice_key,
    slice_selector, verify,
};

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
    priority: Priority,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            query_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
            max_chunks_per_query: 512,
            max_blob_size: 1 << 40, // 1 TiB — a remote peer must not size our disk.
            overwrite: Overwrite::default(),
            // Bulk transfer yields. Replies inherit the *query's* QoS (a
            // server cannot set it), so the only place this can be decided is
            // here — and a multi-megabyte transfer at the default `Data`
            // priority shares a lane with telemetry and alerts, starving them
            // on a constrained link. See `BlobClientBuilder::priority`.
            priority: Priority::DataLow,
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

    /// Fetch a **content-addressed** artifact: the id *is* the root, so the
    /// request is pinned by construction and trust-on-first-use is not
    /// expressible.
    ///
    /// This is the natural request shape for a Tier-2 snapshot re-keyed with
    /// [`TreeIndex::keyed_by_root`](crate::TreeIndex::keyed_by_root), and for
    /// any deployment that names blobs by their content hash.
    pub fn by_root(root: Hash) -> Self {
        DownloadRequest {
            id: root.to_string(),
            expected_root: Some(root),
        }
    }
}

/// Downloads blobs served by a [`crate::BlobServer`] under the same key prefix.
pub struct BlobClient {
    session: Arc<zenoh::Session>,
    prefix: String,
    cfg: ClientConfig,
    // Single-flight guard: destinations with a download in progress *through
    // this client*. Two concurrent downloads to one path would corrupt each
    // other's .part/sidecar pair.
    active: Arc<std::sync::Mutex<std::collections::HashSet<PathBuf>>>,
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

    /// Zenoh priority for every query this client issues
    /// (default [`Priority::DataLow`]).
    ///
    /// **This is the only place bulk QoS can be set.** Zenoh replies inherit
    /// the querier's QoS — server-side reply-QoS setters are no-ops — so a
    /// blob transfer competes with telemetry unless the *client* yields. The
    /// default deliberately sits below `Priority::Data` so a large transfer
    /// cannot starve an alert on a constrained link. Raise it only if you
    /// know the link is not shared.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.cfg.priority = priority;
        self
    }

    /// Build the client.
    pub fn build(self) -> BlobClient {
        BlobClient {
            session: self.session,
            prefix: self.prefix,
            cfg: self.cfg,
            active: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
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
        self.fetch_manifest_matching(id, None).await
    }

    /// Like [`fetch_manifest`](Self::fetch_manifest), but with multi-responder
    /// resilience: a malformed, invalid, mismatched-id, or (when pinned)
    /// wrong-root reply is *skipped*, not fatal — one hostile or stale
    /// responder must not be able to deny a fetch that an honest replica
    /// still answers. The first rejection is kept for diagnostics if nobody
    /// acceptable replies.
    async fn fetch_manifest_matching(
        &self,
        id: &str,
        expected_root: Option<Hash>,
    ) -> Result<Manifest> {
        crate::paths::validate_query_prefix(&self.prefix)?;
        validate_id(id)?;
        let key = manifest_key(&self.prefix, id);
        let replies = self
            .session
            .get(&key)
            .consolidation(ConsolidationMode::None)
            .priority(self.cfg.priority)
            .timeout(self.cfg.query_timeout)
            .await
            .map_err(BlobError::zenoh)?;
        let mut rejected: Option<BlobError> = None;
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            if sample.encoding().to_string() != ENC_MANIFEST {
                continue; // stale/foreign responder; keep listening.
            }
            let verdict = decode::<Manifest>(&sample.payload().to_bytes())
                .and_then(|m| m.validate(self.cfg.max_blob_size).map(|()| m))
                .and_then(|m| {
                    if m.id != id {
                        return Err(BlobError::Protocol(format!(
                            "manifest id {:?} does not match requested {id:?}",
                            m.id
                        )));
                    }
                    if let Some(expected) = expected_root
                        && m.root != expected
                    {
                        return Err(BlobError::RootMismatch {
                            expected,
                            actual: m.root,
                        });
                    }
                    Ok(m)
                });
            match verdict {
                Ok(manifest) => return Ok(manifest),
                Err(e) => {
                    zdebug!(id, error = %e, "skipping unacceptable manifest reply");
                    rejected.get_or_insert(e);
                }
            }
        }
        Err(rejected.unwrap_or_else(|| BlobError::NotFound(id.to_string())))
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
        // Single-flight: a second concurrent download to the same destination
        // through this client is refused instead of silently corrupting state.
        let guard = ActiveGuard::acquire(&self.active, dest)?;
        let result = self.download_to_inner(req, dest, sink, cancel).await;
        drop(guard);
        match &result {
            Err(BlobError::Cancelled { .. }) | Ok(_) => {}
            Err(e) => sink.emit(Progress::Failed {
                error: e.to_string(),
            }),
        }
        result
    }

    /// Ask every responder which chunks of `id` it holds. Returns one
    /// [`Availability`] per reply — with replicated servers this is how a
    /// caller sees the swarm (with reply consolidation disabled, ordinary
    /// downloads already accept whichever replica answers each chunk first).
    pub async fn fetch_availability(&self, id: &str) -> Result<Vec<Availability>> {
        crate::paths::validate_query_prefix(&self.prefix)?;
        validate_id(id)?;
        let key = availability_key(&self.prefix, id);
        let replies = self
            .session
            .get(&key)
            .consolidation(ConsolidationMode::None)
            .priority(self.cfg.priority)
            .timeout(self.cfg.query_timeout)
            .await
            .map_err(BlobError::zenoh)?;
        let mut out = Vec::new();
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            if sample.encoding().to_string() != ENC_AVAIL {
                continue;
            }
            // A malformed reply from one responder must not hide the others.
            if let Ok(avail) = decode::<Availability>(&sample.payload().to_bytes())
                && avail.version == crate::wire::WIRE_VERSION
            {
                out.push(avail);
            }
        }
        Ok(out)
    }

    /// Delete any partial download + sidecar for a previous
    /// [`download_to`](Self::download_to) call with destination `dest`.
    pub async fn delete_partial(&self, dest: &Path) {
        let part = part_path(dest);
        let _ = tokio::fs::remove_file(&part).await;
        ResumeState::remove(&part).await;
    }

    /// Upload (push) the file at `path` to a server that has
    /// [`accept_push`](crate::BlobServerBuilder::accept_push) configured,
    /// under `spec`. `token` is an opaque credential the server's
    /// [`PushPolicy`](crate::PushPolicy) sees with every request.
    ///
    /// The whole file is hashed locally first; every slice the server receives
    /// is verified against that root, and a completed upload is registered and
    /// served by the receiver. Interrupted uploads resume: the server's offer
    /// reply names exactly the chunks it is still missing. Returns the
    /// manifest (distribute `(id, root)` to downloaders).
    pub async fn upload_file(
        &self,
        spec: BlobSpec,
        path: impl Into<PathBuf>,
        token: Option<Vec<u8>>,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
    ) -> Result<Manifest> {
        use crate::verify::{MemOutboard, chunk_range};
        crate::paths::validate_query_prefix(&self.prefix)?;
        let path = path.into();

        // Hash the source once: outboard + manifest, exactly like server-side
        // registration.
        let hash_path = path.clone();
        let (outboard, total_len) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(MemOutboard, u64)> {
                let file = std::fs::File::open(&hash_path)?;
                let total_len = file.metadata()?.len();
                Ok((verify::compute_outboard(file)?, total_len))
            })
            .await
            .map_err(|e| BlobError::Protocol(format!("hash task: {e}")))??;
        let outboard = Arc::new(outboard);
        let chunks = TransferChunks::new(spec.chunk_size, total_len)?;
        let manifest = Manifest {
            version: crate::wire::WIRE_VERSION,
            id: spec.id.clone(),
            filename: spec.filename,
            total_len,
            chunk_size: spec.chunk_size,
            root: crate::hash::Hash::from(outboard.root),
            created_ms: spec.created_ms,
        };
        manifest.validate(u64::MAX)?;

        // Offer: the reply lists the chunk ranges the server still wants.
        let offer_key = push_offer_key(&self.prefix, &manifest.id);
        let mut builder = self
            .session
            .get(&offer_key)
            .consolidation(ConsolidationMode::None)
            .priority(self.cfg.priority)
            .timeout(self.cfg.query_timeout)
            .payload(crate::wire::encode(&manifest)?);
        if let Some(tok) = &token {
            builder = builder.attachment(tok.clone());
        }
        let replies = builder.await.map_err(BlobError::zenoh)?;
        let mut wanted: Option<Vec<(u32, u32)>> = None;
        while let Ok(reply) = replies.recv_async().await {
            match reply.result() {
                Ok(sample) if sample.encoding().to_string() == ENC_PUSH => {
                    wanted = Some(decode(&sample.payload().to_bytes())?);
                    break;
                }
                Ok(_) => continue,
                Err(e) => {
                    return Err(BlobError::PushDenied(
                        String::from_utf8_lossy(&e.payload().to_bytes()).into_owned(),
                    ));
                }
            }
        }
        let wanted = wanted
            .ok_or_else(|| BlobError::PushDenied("no push endpoint answered the offer".into()))?;

        // The offer reply is attacker input like everything else off the wire:
        // enforce sorted, disjoint, in-bounds, non-empty spans *before* any
        // arithmetic — a hostile responder must not be able to drive an
        // underflow or an out-of-range slice encoding.
        let count = chunks.count();
        let mut prev_end = 0u32;
        for &(a, b) in &wanted {
            if a < prev_end || a >= b || b > count {
                return Err(BlobError::Protocol(format!(
                    "push offer replied malformed wanted ranges ({a}, {b}) for {count} chunks"
                )));
            }
            prev_end = b;
        }
        let to_send: u32 = wanted.iter().map(|(a, b)| b - a).sum();
        if to_send < count {
            sink.emit(Progress::Resumed {
                received: count - to_send,
                total: count,
            });
        } else {
            sink.emit(Progress::Started {
                total_len,
                chunk_count: count,
            });
        }
        zdebug!(id = %manifest.id, total = count, to_send, "push offer accepted");

        // Stream the wanted slices, one acknowledged query each.
        let mut sent = count - to_send;
        let mut bytes_sent: u64 = 0;
        let mut reader = tokio::task::spawn_blocking({
            let path = path.clone();
            move || std::fs::File::open(path)
        })
        .await
        .map_err(|e| BlobError::Protocol(format!("open task: {e}")))??;
        for (start, end) in wanted {
            for index in start..end {
                if cancel.is_cancelled() {
                    sink.emit(Progress::Cancelled {
                        received: sent,
                        total: count,
                    });
                    return Err(BlobError::Cancelled {
                        received: sent,
                        total: count,
                    });
                }
                let ob = outboard.clone();
                let byte_range = chunks.byte_range(index);
                let (r, slice) = tokio::task::spawn_blocking(
                    move || -> (std::fs::File, std::io::Result<Vec<u8>>) {
                        let slice = verify::encode_slice(&reader, &*ob, chunk_range(byte_range));
                        (reader, slice)
                    },
                )
                .await
                .map_err(|e| BlobError::Protocol(format!("encode task: {e}")))?;
                reader = r;
                let slice = slice?;

                let mut b = self
                    .session
                    .get(push_slice_key(&self.prefix, &manifest.id, index))
                    .consolidation(ConsolidationMode::None)
                    .priority(self.cfg.priority)
                    .timeout(self.cfg.query_timeout)
                    .payload(slice);
                if let Some(tok) = &token {
                    b = b.attachment(tok.clone());
                }
                let replies = b.await.map_err(BlobError::zenoh)?;
                let mut acked = false;
                while let Ok(reply) = replies.recv_async().await {
                    match reply.result() {
                        Ok(sample) if sample.encoding().to_string() == ENC_PUSH => {
                            let _remaining: u32 = decode(&sample.payload().to_bytes())?;
                            acked = true;
                            break;
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            return Err(BlobError::PushDenied(
                                String::from_utf8_lossy(&e.payload().to_bytes()).into_owned(),
                            ));
                        }
                    }
                }
                if !acked {
                    return Err(BlobError::Incomplete {
                        received: sent,
                        total: count,
                    });
                }
                sent += 1;
                bytes_sent += chunks.len_of(index) as u64;
                sink.emit(Progress::Chunk {
                    index,
                    received: sent,
                    total: count,
                    bytes_received: bytes_sent,
                });
            }
        }
        sink.emit(Progress::Completed { path });
        Ok(manifest)
    }

    /// Download a blob into any seekable async writer (a `tokio::fs::File`
    /// opened with custom options, an in-memory `Cursor`, …). Verified leaves
    /// are written at their blob offsets exactly like
    /// [`download_to`](Self::download_to), but **without resume**: no `.part`,
    /// no sidecar — an interrupted call must start over, and the writer must
    /// be sized/seekable for the whole blob. Emits `Started`/`Chunk` progress;
    /// the `Ok` return is completion.
    pub async fn download_to_writer<W>(
        &self,
        req: &DownloadRequest,
        writer: &mut W,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
    ) -> Result<TransferStats>
    where
        W: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin + Send,
    {
        let started_at = tokio::time::Instant::now();
        let (manifest, chunks) = self.start(req).await?;
        let count = chunks.count();
        zdebug!(id = %manifest.id, total_len = manifest.total_len, chunks = count, "writer download start");
        sink.emit(Progress::Started {
            total_len: manifest.total_len,
            chunk_count: count,
        });

        let mut state = ResumeState::fresh(&manifest, count);
        let mut stats = TransferStats::default();
        let mut target = WriterTarget(writer);
        let result = self
            .fill_holes(
                &manifest,
                &chunks,
                &mut target,
                &mut state,
                None,
                sink,
                cancel,
                &mut stats,
            )
            .await;
        match &result {
            Err(BlobError::Cancelled { .. }) | Ok(_) => {}
            Err(e) => sink.emit(Progress::Failed {
                error: e.to_string(),
            }),
        }
        result?;
        stats.elapsed = started_at.elapsed();
        Ok(stats)
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
            &manifest,
            &chunks,
            &mut file,
            &mut state,
            Some(&part),
            sink,
            cancel,
            &mut stats,
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

    /// Fetch + validate the manifest and enforce root pinning (pinning is
    /// applied per reply, so a wrong-root responder cannot mask a right one).
    async fn start(&self, req: &DownloadRequest) -> Result<(Manifest, TransferChunks)> {
        let manifest = self
            .fetch_manifest_matching(&req.id, req.expected_root)
            .await?;
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
    /// `part` enables sidecar persistence (file downloads); `None` keeps the
    /// bitfield purely in memory (writer downloads).
    #[allow(clippy::too_many_arguments)]
    async fn fill_holes<T: SliceTarget>(
        &self,
        manifest: &Manifest,
        chunks: &TransferChunks,
        file: &mut T,
        state: &mut ResumeState,
        part: Option<&Path>,
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
                return Self::persist_cancel(file, state, part, sink, count).await;
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
                .priority(self.cfg.priority)
                .timeout(self.cfg.query_timeout)
                .await
                .map_err(BlobError::zenoh)?;

            while let Ok(reply) = replies.recv_async().await {
                if cancel.is_cancelled() {
                    drop(replies);
                    return Self::persist_cancel(file, state, part, sink, count).await;
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
                    file.write_leaf(off, &data).await?;
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
                    // Batched persistence: data first (commit), then bits —
                    // bits must never claim data the OS hasn't received.
                    if let Some(part) = part
                        && (marks_since_save >= 64 || last_save.elapsed() > Duration::from_secs(2))
                    {
                        file.commit().await?;
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
                    file.commit().await?;
                    if let Some(part) = part {
                        state.save_atomic(part).await?;
                    }
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

        file.commit().await?;
        if let Some(part) = part {
            state.save_atomic(part).await?;
        }
        Ok(())
    }

    async fn persist_cancel<T: SliceTarget>(
        file: &mut T,
        state: &ResumeState,
        part: Option<&Path>,
        sink: &dyn ProgressSink,
        count: u32,
    ) -> Result<()> {
        file.commit().await?;
        if let Some(part) = part {
            state.save_atomic(part).await?;
        }
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

/// Where verified leaves land: a positional write plus a durability point.
trait SliceTarget: Send {
    /// Write verified bytes at their blob offset.
    async fn write_leaf(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    /// Make everything written so far durable/visible (fsync for files,
    /// flush for writers).
    async fn commit(&mut self) -> Result<()>;
}

impl SliceTarget for tokio::fs::File {
    async fn write_leaf(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        self.seek(SeekFrom::Start(offset)).await?;
        self.write_all(data).await?;
        Ok(())
    }
    async fn commit(&mut self) -> Result<()> {
        self.sync_data().await?;
        Ok(())
    }
}

/// Adapter for [`BlobClient::download_to_writer`].
struct WriterTarget<'a, W>(&'a mut W);

impl<W> SliceTarget for WriterTarget<'_, W>
where
    W: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin + Send,
{
    async fn write_leaf(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        self.0.seek(SeekFrom::Start(offset)).await?;
        self.0.write_all(data).await?;
        Ok(())
    }
    async fn commit(&mut self) -> Result<()> {
        self.0.flush().await?;
        Ok(())
    }
}

/// RAII entry in a client's active-destination set.
struct ActiveGuard {
    active: Arc<std::sync::Mutex<std::collections::HashSet<PathBuf>>>,
    dest: PathBuf,
}

impl ActiveGuard {
    fn acquire(
        active: &Arc<std::sync::Mutex<std::collections::HashSet<PathBuf>>>,
        dest: &Path,
    ) -> Result<Self> {
        let mut set = active.lock().unwrap_or_else(|e| e.into_inner());
        if !set.insert(dest.to_path_buf()) {
            return Err(BlobError::Protocol(format!(
                "a download to {dest:?} is already in progress on this client"
            )));
        }
        Ok(ActiveGuard {
            active: active.clone(),
            dest: dest.to_path_buf(),
        })
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.dest);
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
