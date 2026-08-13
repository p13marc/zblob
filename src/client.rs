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
use crate::chunk::{MIN_CHUNK_SIZE, TransferChunks};
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::id::BlobId;
use crate::manifest::{BlobSpec, Manifest, validate_id};
use crate::obs::{TransferStats, zdebug};
use crate::prefix::QueryPrefix;
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
    /// Fail with [`BlobError::DestinationExists`]. The default.
    ///
    /// Checked *before* the transfer, so an occupied destination costs
    /// nothing. It is checked again at the end — the destination can appear
    /// while a download is in flight — and in that case the finished `.part`
    /// is kept next to it, so the transfer is not thrown away either.
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
    max_probe_replies: usize,
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
            // How many responders a fan-out probe will collect. The number of
            // peers that answer is not ours to choose, so it is bounded.
            max_probe_replies: 256,
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

/// Where a staged download landed, from [`BlobClient::download_staged`].
#[derive(Debug, Clone)]
pub struct Staged {
    /// The file, named by the blob's id inside the directory you gave.
    pub path: PathBuf,
    /// The server's advisory filename, reduced to a single safe component —
    /// `None` if it offered none, or offered something unusable.
    ///
    /// A *suggestion*: nothing has been named this, and this crate will never
    /// name anything this. Renaming to it is the caller's decision (and
    /// usually the user's).
    pub suggested: Option<String>,
    /// Transfer statistics, as [`BlobClient::download_to`] returns.
    pub stats: TransferStats,
}

/// What one origin said about a blob, from [`BlobClient::probe`].
#[derive(Debug, Clone)]
pub struct BlobProbe {
    /// The prefix that answered — pass it to a [`BlobClient`] to fetch from
    /// this holder specifically.
    pub origin: QueryPrefix,
    /// The manifest this holder serves: identity, size and geometry.
    pub manifest: Manifest,
    /// Which chunks it holds, if it answered the `have` endpoint.
    pub availability: Option<Availability>,
}

/// Recover the origin prefix from a reply key of the form
/// `<origin>/<id>/<endpoint>`.
///
/// The *server* built this key from its own concrete prefix, whatever
/// wildcard the query went through — which is precisely why a probe can
/// attribute its answers at all.
fn origin_of(reply_key: &str, id: &str, endpoint: &str) -> Option<String> {
    let rest = reply_key.strip_suffix(endpoint)?.strip_suffix('/')?;
    let rest = rest.strip_suffix(id)?.strip_suffix('/')?;
    (!rest.is_empty()).then(|| rest.to_string())
}

/// Downloads blobs served by a [`crate::BlobServer`] under the same key prefix.
#[derive(Debug)]
pub struct BlobClient {
    session: zenoh::Session,
    prefix: QueryPrefix,
    cfg: ClientConfig,
    // Single-flight guard: destinations with a download in progress *through
    // this client*. Two concurrent downloads to one path would corrupt each
    // other's .part/sidecar pair.
    active: Arc<std::sync::Mutex<std::collections::HashSet<PathBuf>>>,
}

/// Builder for a [`BlobClient`] (see [`BlobClient::builder`]).
#[derive(Debug)]
pub struct BlobClientBuilder {
    session: zenoh::Session,
    prefix: QueryPrefix,
    cfg: ClientConfig,
}

impl BlobClientBuilder {
    /// Per-query timeout (default 30 s). Transfers larger than one query's
    /// chunk budget span multiple queries, so this bounds *stall* time, not
    /// total transfer time.
    #[must_use]
    pub fn query_timeout(mut self, t: Duration) -> Self {
        self.cfg.query_timeout = t;
        self
    }

    /// Retry/backoff policy (default: 5 attempts, 250 ms base, 10 s cap).
    #[must_use]
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.cfg.retry = retry;
        self
    }

    /// Max chunks requested per query (default 512; must not exceed the
    /// server's own cap or queries are rejected).
    #[must_use]
    pub fn max_chunks_per_query(mut self, n: u32) -> Self {
        self.cfg.max_chunks_per_query = n.max(1);
        self
    }

    /// Upper bound on `total_len` this client will accept from a manifest
    /// (default 1 TiB) — the allocation/preallocation defense.
    #[must_use]
    pub fn max_blob_size(mut self, bytes: u64) -> Self {
        self.cfg.max_blob_size = bytes;
        self
    }

    /// Overwrite policy for the destination (default [`Overwrite::Refuse`]).
    #[must_use]
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
    #[must_use]
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
    pub fn builder(session: &zenoh::Session, key_prefix: QueryPrefix) -> BlobClientBuilder {
        BlobClientBuilder {
            session: session.clone(),
            prefix: key_prefix,
            cfg: ClientConfig::default(),
        }
    }

    /// Build a client with default configuration (see [`BlobClient::builder`]).
    pub fn new(session: &zenoh::Session, key_prefix: QueryPrefix) -> Self {
        Self::builder(session, key_prefix).build()
    }

    /// Fetch and validate just the manifest for blob `id` — probe existence,
    /// size, and root before committing to a download.
    pub async fn fetch_manifest(&self, id: &str) -> Result<Manifest> {
        self.fetch_manifest_matching(id, None).await
    }

    /// Ask every origin that answers what it knows about `id`.
    ///
    /// This is the sequence RFC 07 §2.5 prescribes for a consumer that cannot
    /// name the origin holding a blob: ask for something *tiny* across
    /// origins, then fetch from one chosen origin's concrete key. Both replies
    /// here are small and bounded — a manifest, and a bitfield — so a
    /// wildcard-origin probe is legitimate where a wildcard-origin *fetch*
    /// would be one full copy per responder.
    ///
    /// Each result carries the `origin` that answered, as a prefix you can
    /// hand straight to another [`BlobClient`]. That attribution is the whole
    /// point of probing and was previously left for callers to reconstruct
    /// from raw reply keys.
    ///
    /// Availability is `None` for a holder that answered `manifest` but not
    /// `have`.
    pub async fn probe(&self, id: &str) -> Result<Vec<BlobProbe>> {
        validate_id(id)?;
        let mut found: Vec<BlobProbe> = Vec::new();

        let replies = self
            .session
            .get(manifest_key(self.prefix.as_str(), id))
            .consolidation(ConsolidationMode::None)
            .priority(self.cfg.priority)
            .timeout(self.cfg.query_timeout)
            .await
            .map_err(BlobError::zenoh)?;
        while let Ok(reply) = replies.recv_async().await {
            if found.len() >= self.cfg.max_probe_replies {
                break;
            }
            let Ok(sample) = reply.result() else { continue };
            if !ENC_MANIFEST.matches(sample.encoding()) {
                continue;
            }
            // The reply key names the origin: it is the prefix the *server*
            // built the key from, whatever wildcard we asked through.
            let Some(origin) = origin_of(sample.key_expr().as_str(), id, "manifest") else {
                continue;
            };
            let Ok(manifest) = decode::<Manifest>(&sample.payload().to_bytes()) else {
                continue;
            };
            if manifest.validate(self.cfg.max_blob_size).is_err() || manifest.id != id {
                continue;
            }
            let Ok(origin) = QueryPrefix::new(origin) else {
                continue;
            };
            if found.iter().any(|p| p.origin == origin) {
                continue; // one entry per origin, not per reply
            }
            found.push(BlobProbe {
                origin,
                manifest,
                availability: None,
            });
        }
        if found.is_empty() {
            return Ok(found);
        }

        // Second question, same shape: which chunks does each holder have?
        // A holder that does not answer keeps `None` rather than being
        // dropped — `have` is optional, `manifest` is what makes it a holder.
        let replies = self
            .session
            .get(availability_key(self.prefix.as_str(), id))
            .consolidation(ConsolidationMode::None)
            .priority(self.cfg.priority)
            .timeout(self.cfg.query_timeout)
            .await
            .map_err(BlobError::zenoh)?;
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            if !ENC_AVAIL.matches(sample.encoding()) {
                continue;
            }
            let Some(origin) = origin_of(sample.key_expr().as_str(), id, "have") else {
                continue;
            };
            let Ok(avail) = decode::<Availability>(&sample.payload().to_bytes()) else {
                continue;
            };
            let Some(entry) = found.iter_mut().find(|p| p.origin.as_str() == origin) else {
                continue; // answered `have` but not `manifest`; nothing to attach to
            };
            if avail
                .validate(entry.manifest.chunk_count().unwrap_or(u32::MAX))
                .is_ok()
            {
                entry.availability = Some(avail);
            }
        }
        Ok(found)
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
        validate_id(id)?;
        let key = manifest_key(self.prefix.as_str(), id);
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
            if !ENC_MANIFEST.matches(sample.encoding()) {
                continue; // stale/foreign responder; keep listening.
            }
            let verdict = decode::<Manifest>(&sample.payload().to_bytes())
                .and_then(|m| m.validate(self.cfg.max_blob_size).map(|()| m))
                .and_then(|m| {
                    if m.id != id {
                        return Err(BlobError::MalformedMessage(format!(
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

    /// Download into `dir`, staged under the blob's id.
    ///
    /// [`download_to`](Self::download_to) is deliberately caller-chooses-the
    /// -destination: the server's filename is advisory and this crate never
    /// joins it to a path, which is the structural fix for v1's traversal
    /// vector — applied at the API's shape rather than at the write site. That
    /// stays true here and is not negotiable.
    ///
    /// What it left every caller to reinvent is the *convention* around it,
    /// and both downstream GUIs reinvented the same one: stage under the id,
    /// keep the suggested name aside, offer it in a save-as dialog later.
    /// Staging under the **id** rather than the suggested name is the load
    /// bearing part — two concurrent downloads whose servers both claim
    /// `report.pcap` must not collide.
    ///
    /// Pairing the two in one call means the safe path is also the shortest
    /// one, which is the only way a security property reliably survives
    /// contact with application code.
    /// Download a blob to the file at `dest` (written via `<dest>.part` + a
    /// resume sidecar, then atomically renamed into place). Returns a
    /// [`Download`] — `.await` it to run the transfer.
    ///
    /// Call again with the same arguments to resume after `Incomplete`,
    /// `Cancelled`, or a crash. Set progress, cancellation, overwrite policy
    /// and striping on the returned builder.
    pub fn download_to<'a>(&'a self, req: &'a DownloadRequest, dest: &'a Path) -> Download<'a> {
        Download {
            client: self,
            req,
            dest,
            sink: None,
            cancel: None,
            overwrite: None,
            holders: None,
        }
    }

    /// Download into `dir`, staged under the blob's id, returning where it
    /// landed and the server's advisory filename. `.await` the returned
    /// [`StagedDownload`] to run it.
    ///
    /// [`download_to`](Self::download_to) is deliberately caller-chooses-the
    /// -destination: the server's filename is advisory and this crate never
    /// joins it to a path, which is the structural fix for v1's traversal
    /// vector — applied at the API's shape rather than at the write site. That
    /// stays true here and is not negotiable.
    ///
    /// What it left every caller to reinvent is the *convention* around it,
    /// and both downstream GUIs reinvented the same one: stage under the id,
    /// keep the suggested name aside, offer it in a save-as dialog later.
    /// Staging under the **id** rather than the suggested name is the load
    /// bearing part — two concurrent downloads whose servers both claim
    /// `report.pcap` must not collide.
    ///
    /// Pairing the two in one call means the safe path is also the shortest
    /// one, which is the only way a security property reliably survives
    /// contact with application code.
    pub fn download_staged<'a>(
        &'a self,
        req: &'a DownloadRequest,
        dir: &'a Path,
    ) -> StagedDownload<'a> {
        StagedDownload {
            inner: self.download_to(req, dir),
        }
    }

    /// Download into `writer`, **without resume**: no `.part`, no sidecar — an
    /// interrupted call must start over, and the writer must be
    /// sized/seekable for the whole blob. `.await` the returned
    /// [`DownloadToWriter`] to run it.
    pub fn download_to_writer<'a, W>(
        &'a self,
        req: &'a DownloadRequest,
        writer: &'a mut W,
    ) -> DownloadToWriter<'a, W>
    where
        W: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin + Send,
    {
        DownloadToWriter {
            client: self,
            req,
            writer,
            sink: None,
            cancel: None,
        }
    }

    /// Upload (push) the file at `path` to a server that has
    /// [`accept_push`](crate::BlobServerBuilder::accept_push) configured,
    /// under `spec`. `.await` the returned [`Upload`] to run it.
    ///
    /// The whole file is hashed locally first; every slice the server receives
    /// is verified against that root, and a completed upload is registered and
    /// served by the receiver. Interrupted uploads resume: the server's offer
    /// reply names exactly the chunks it is still missing. Resolves to the
    /// manifest (distribute `(id, root)` to downloaders).
    pub fn upload_file(&self, spec: BlobSpec, path: impl Into<PathBuf>) -> Upload<'_> {
        Upload {
            client: self,
            spec,
            path: path.into(),
            token: None,
            sink: None,
            cancel: None,
        }
    }

    async fn run_download_staged(
        &self,
        req: &DownloadRequest,
        dir: &Path,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        overwrite: Overwrite,
    ) -> Result<Staged> {
        validate_id(&req.id)?;
        tokio::fs::create_dir_all(dir).await?;
        // `validate_id` has already refused separators, `..` and wildcards, so
        // the id is a single safe component.
        let path = dir.join(&req.id);
        let stats = self
            .run_download_to(req, &path, sink, cancel, overwrite)
            .await?;
        // Re-read rather than plumbing it out of the transfer: the manifest is
        // cheap, and the alternative is threading a value through a function
        // whose whole job is bytes.
        let suggested = self
            .fetch_manifest_matching(&req.id, req.expected_root)
            .await
            .ok()
            .and_then(|m| m.suggested_filename());
        Ok(Staged {
            path,
            suggested,
            stats,
        })
    }

    /// Download a blob to the file at `dest` (written via `<dest>.part` + a
    /// resume sidecar, then atomically renamed into place). Progress events go
    /// to `sink`; a set `cancel` stops cooperatively with state persisted.
    /// Returns per-call [`TransferStats`].
    ///
    /// Call again with the same arguments to resume after `Incomplete`,
    /// `Cancelled`, or a crash.
    async fn run_download_to(
        &self,
        req: &DownloadRequest,
        dest: &Path,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        overwrite: Overwrite,
    ) -> Result<TransferStats> {
        // Single-flight: a second concurrent download to the same destination
        // through this client is refused instead of silently corrupting state.
        let guard = ActiveGuard::acquire(&self.active, dest)?;
        let result = self
            .download_to_inner(req, dest, sink, cancel, overwrite)
            .await;
        drop(guard);
        match &result {
            Err(BlobError::Cancelled { .. }) | Ok(_) => {}
            Err(e) => sink.emit(Progress::Failed {
                error: e.to_string(),
            }),
        }
        result
    }

    /// Download `req`, **striping** its chunks across the given holders.
    ///
    /// The ordinary [`download_to`](Self::download_to) asks one key expression
    /// and takes whoever answers. With several replicas that is
    /// multi-*responder* tolerance rather than multi-*source* transfer: reply
    /// consolidation is off, so every matching holder sends every requested
    /// slice and the client discards the duplicates — *after* they have
    /// crossed the wire. Zenoh cannot cancel remote replies in flight, so N
    /// replicas cost N times the bandwidth.
    ///
    /// This addresses each range to exactly one holder's concrete prefix, so
    /// each range crosses the wire once. Holders that reported an
    /// [`Availability`] only receive ranges they claim to have; stragglers at
    /// the end are duplicated to a second holder, which is BitTorrent's
    /// endgame and the standard answer to one slow peer holding up a transfer.
    ///
    /// Get `holders` from [`probe`](Self::probe). With fewer than two this
    /// degrades to `download_to`, which is the right thing rather than an
    /// error.
    async fn run_download_striped(
        &self,
        req: &DownloadRequest,
        dest: &Path,
        holders: &[BlobProbe],
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        overwrite: Overwrite,
    ) -> Result<TransferStats> {
        if holders.len() < 2 {
            return self
                .run_download_to(req, dest, sink, cancel, overwrite)
                .await;
        }
        let guard = ActiveGuard::acquire(&self.active, dest)?;
        let result = self
            .download_striped_inner(req, dest, holders, sink, cancel, overwrite)
            .await;
        drop(guard);
        if let Err(e) = &result
            && !e.is_cancelled()
        {
            sink.emit(Progress::Failed {
                error: e.to_string(),
            });
        }
        result
    }

    async fn download_striped_inner(
        &self,
        req: &DownloadRequest,
        dest: &Path,
        holders: &[BlobProbe],
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        overwrite: Overwrite,
    ) -> Result<TransferStats> {
        let started_at = tokio::time::Instant::now();
        if overwrite == Overwrite::Refuse && tokio::fs::try_exists(dest).await? {
            return Err(BlobError::DestinationExists(dest.to_path_buf()));
        }
        // Every holder must agree on the content, or "striping" would be
        // splicing two different blobs together.
        let manifest = holders[0].manifest.clone();
        if let Some(expected) = req.expected_root
            && manifest.root != expected
        {
            return Err(BlobError::RootMismatch {
                expected,
                actual: manifest.root,
            });
        }
        if holders.iter().any(|h| h.manifest.root != manifest.root) {
            return Err(BlobError::MalformedMessage(
                "holders disagree about this blob's root; probe again and pick one set".into(),
            ));
        }
        let chunks = manifest.chunks()?;
        let count = chunks.count();

        if let Some(parent) = dest.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let part = part_path(dest);
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
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&part)
            .await?;
        let mut stats = TransferStats {
            chunks_resumed: state.received(),
            ..Default::default()
        };
        self.fill_holes_striped(
            &manifest, &chunks, holders, &mut file, &mut state, &part, sink, cancel, &mut stats,
        )
        .await?;

        file.sync_data().await?;
        drop(file);
        if overwrite == Overwrite::Refuse && tokio::fs::try_exists(dest).await? {
            return Err(BlobError::DestinationExists(dest.to_path_buf()));
        }
        tokio::fs::rename(&part, dest).await?;
        ResumeState::remove(&part).await;
        sink.emit(Progress::Completed {
            path: dest.to_path_buf(),
        });
        stats.elapsed = started_at.elapsed();
        Ok(stats)
    }

    /// Ask every responder which chunks of `id` it holds. Returns one
    /// [`Availability`] per reply — with replicated servers this is how a
    /// caller sees the swarm (with reply consolidation disabled, ordinary
    /// downloads already accept whichever replica answers each chunk first).
    pub async fn fetch_availability(&self, id: &str) -> Result<Vec<Availability>> {
        validate_id(id)?;
        let key = availability_key(self.prefix.as_str(), id);
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
            // One responder per entry, and the responder count is not ours to
            // choose — cap it so a flood of repliers cannot grow this vector
            // without bound.
            if out.len() >= self.cfg.max_probe_replies {
                break;
            }
            let Ok(sample) = reply.result() else { continue };
            if !ENC_AVAIL.matches(sample.encoding()) {
                continue;
            }
            // A malformed reply from one responder must not hide the others.
            if let Ok(avail) = decode::<Availability>(&sample.payload().to_bytes())
                && avail
                    .validate(self.cfg.max_blob_size.div_ceil(MIN_CHUNK_SIZE as u64) as u32)
                    .is_ok()
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
    async fn run_upload_file(
        &self,
        spec: BlobSpec,
        path: PathBuf,
        token: Option<Vec<u8>>,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
    ) -> Result<Manifest> {
        use crate::verify::{MemOutboard, chunk_range};
        // An upload has exactly one destination: the receiver spools state
        // keyed by the id, and its acknowledgements echo the query key
        // verbatim — which for a wildcard query is a key *expression*.
        if !self.prefix.is_concrete() {
            return Err(BlobError::Usage(format!(
                "cannot upload to the wildcard prefix {} — an upload has exactly one destination",
                self.prefix
            )));
        }

        // Hash the source once: outboard + manifest, exactly like server-side
        // registration.
        let hash_path = path.clone();
        let (outboard, total_len) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(MemOutboard, u64)> {
                let file = std::fs::File::open(&hash_path)?;
                let total_len = file.metadata()?.len();
                Ok((verify::compute_outboard(file)?, total_len))
            })
            .await??;
        let outboard = Arc::new(outboard);
        let chunks = TransferChunks::new(spec.chunk_size, total_len)?;
        let manifest = Manifest {
            version: crate::wire::WIRE_VERSION,
            id: BlobId::new(spec.id.clone())?,
            filename: spec.filename,
            total_len,
            chunk_size: spec.chunk_size,
            root: crate::hash::Hash::from(outboard.root),
            created_ms: spec.created_ms,
            // The uploader has nothing to advertise; the *receiver* is the one
            // with limits, and it states them in its offer reply.
            ext: crate::wire::Ext::new(),
        };
        manifest.validate(u64::MAX)?;

        // Offer: the reply lists the chunk ranges the server still wants.
        let offer_key = push_offer_key(self.prefix.as_str(), &manifest.id);
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
        // A refusal from *a* responder is not a refusal by *the* responder.
        // More than one server can serve one prefix — zensight's netring runs
        // a second `BlobServer` on its artifact prefix deliberately — so a
        // server with push disabled answering "not enabled here" must not
        // abort an upload another server is accepting. Same rule as every
        // `fetch_*` loop (see the crate docs, fact 3): skip, keep the first
        // rejection for diagnostics, fail only if nobody accepts.
        let mut refusal: Option<String> = None;
        while let Ok(reply) = replies.recv_async().await {
            match reply.result() {
                Ok(sample) if ENC_PUSH.matches(sample.encoding()) => {
                    match decode::<Vec<(u32, u32)>>(&sample.payload().to_bytes()) {
                        Ok(ranges) => {
                            wanted = Some(ranges);
                            break;
                        }
                        Err(e) => {
                            refusal.get_or_insert_with(|| format!("undecodable offer reply: {e}"));
                        }
                    }
                }
                Ok(_) => continue,
                Err(e) => {
                    refusal.get_or_insert_with(|| {
                        String::from_utf8_lossy(&e.payload().to_bytes()).into_owned()
                    });
                }
            }
        }
        let wanted = wanted.ok_or_else(|| {
            BlobError::PushDenied(
                refusal.unwrap_or_else(|| "no push endpoint answered the offer".into()),
            )
        })?;

        // The offer reply is attacker input like everything else off the wire:
        // enforce sorted, disjoint, in-bounds, non-empty spans *before* any
        // arithmetic — a hostile responder must not be able to drive an
        // underflow or an out-of-range slice encoding.
        let count = chunks.count();
        let mut prev_end = 0u32;
        for &(a, b) in &wanted {
            if a < prev_end || a >= b || b > count {
                return Err(BlobError::MalformedMessage(format!(
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
        .await??;
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
                .await?;
                reader = r;
                let slice = slice?;

                let mut b = self
                    .session
                    .get(push_slice_key(self.prefix.as_str(), &manifest.id, index))
                    .consolidation(ConsolidationMode::None)
                    .priority(self.cfg.priority)
                    .timeout(self.cfg.query_timeout)
                    .payload(slice);
                if let Some(tok) = &token {
                    b = b.attachment(tok.clone());
                }
                let replies = b.await.map_err(BlobError::zenoh)?;
                let mut acked = false;
                // As in the offer loop: one responder's error is not the
                // answer. The server that declined the offer holds no spool
                // state for this id and will reject every slice; the one that
                // accepted acknowledges them.
                let mut slice_refusal: Option<String> = None;
                loop {
                    // Waiting for an ack is the long pole of a push round; a
                    // cancel here must not have to outlast the query timeout.
                    let Some(recv) = cancel.until_cancelled(replies.recv_async()).await else {
                        sink.emit(Progress::Cancelled {
                            received: sent,
                            total: count,
                        });
                        return Err(BlobError::Cancelled {
                            received: sent,
                            total: count,
                        });
                    };
                    let Ok(reply) = recv else { break };
                    match reply.result() {
                        Ok(sample) if ENC_PUSH.matches(sample.encoding()) => {
                            match decode::<u32>(&sample.payload().to_bytes()) {
                                Ok(_remaining) => {
                                    acked = true;
                                    break;
                                }
                                Err(e) => {
                                    slice_refusal
                                        .get_or_insert_with(|| format!("undecodable ack: {e}"));
                                }
                            }
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            slice_refusal.get_or_insert_with(|| {
                                String::from_utf8_lossy(&e.payload().to_bytes()).into_owned()
                            });
                        }
                    }
                }
                if !acked {
                    return match slice_refusal {
                        Some(reason) => Err(BlobError::PushDenied(reason)),
                        None => Err(BlobError::Incomplete {
                            received: sent,
                            total: count,
                        }),
                    };
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
    async fn run_download_to_writer<W>(
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
        overwrite: Overwrite,
    ) -> Result<TransferStats> {
        let started_at = tokio::time::Instant::now();

        // Refuse *before* transferring, not after.
        //
        // This used to be checked once the download had completed and been
        // fsynced, so a full transfer crossed the wire — and the `.part` was
        // preallocated to the remote-supplied total_len — for a request that
        // was then refused. It is re-checked after the transfer as well, since
        // the destination can appear while we are fetching; that late check is
        // the TOCTOU backstop, not the policy.
        if overwrite == Overwrite::Refuse && tokio::fs::try_exists(dest).await? {
            return Err(BlobError::DestinationExists(dest.to_path_buf()));
        }

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
        if overwrite == Overwrite::Refuse && tokio::fs::try_exists(dest).await? {
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

            // Take as many holes as one query may carry — clamped to whatever
            // the *server* said it accepts. Without this a server that lowered
            // its cap rejected every query from a client with the old default,
            // and the client had no way to find out why.
            let mut holes = state.missing_ranges(count);
            holes.truncate(MAX_RANGE_SPANS);
            let mut budget = manifest
                .max_chunks_per_query()
                .map_or(self.cfg.max_chunks_per_query, |served| {
                    self.cfg.max_chunks_per_query.min(served)
                })
                .max(1);
            for r in holes.iter_mut() {
                let take = (r.end - r.start).min(budget);
                r.end = r.start + take;
                budget -= take;
            }
            holes.retain(|r| !r.is_empty());

            let selector = slice_selector(self.prefix.as_str(), &manifest.id, &holes);
            let before = state.received();
            let replies = self
                .session
                .get(&selector)
                .consolidation(ConsolidationMode::None)
                .priority(self.cfg.priority)
                .timeout(self.cfg.query_timeout)
                .await
                .map_err(BlobError::zenoh)?;

            loop {
                // Wait *and* watch the token: a cancel must not be held up by
                // a reply that may never arrive (see `cancel.rs`). `None` here
                // is cancellation, which persists and returns; a closed channel
                // is an exhausted round, which falls through to the retry
                // logic below. Collapsing the two would turn a cancel into a
                // backoff sleep.
                let Some(recv) = cancel.until_cancelled(replies.recv_async()).await else {
                    drop(replies);
                    return Self::persist_cancel(file, state, part, sink, count).await;
                };
                let Ok(reply) = recv else { break };
                let Ok(sample) = reply.result() else { continue };
                if !ENC_SLICE.matches(sample.encoding()) {
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

    /// One striping round: partition the outstanding holes across holders,
    /// query each holder for only its share, and write whatever verifies.
    #[allow(clippy::too_many_arguments)]
    async fn fill_holes_striped(
        &self,
        manifest: &Manifest,
        chunks: &TransferChunks,
        holders: &[BlobProbe],
        file: &mut tokio::fs::File,
        state: &mut ResumeState,
        part: &Path,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        stats: &mut TransferStats,
    ) -> Result<()> {
        let count = chunks.count();
        let root: blake3::Hash = manifest.root.into();
        let total_len = manifest.total_len;
        let mut bytes_received: u64 = (0..count)
            .filter(|i| state.is_set(*i))
            .map(|i| chunks.len_of(i) as u64)
            .sum();
        let mut no_progress = 0u32;
        // Per-holder rejection tally: a peer that keeps sending unusable
        // replies is dropped from the rotation rather than re-raced forever.
        let mut rejects: Vec<u32> = vec![0; holders.len()];
        let budget = manifest
            .max_chunks_per_query()
            .map_or(self.cfg.max_chunks_per_query, |served| {
                self.cfg.max_chunks_per_query.min(served)
            })
            .max(1);

        while !state.is_complete(count) {
            if cancel.is_cancelled() {
                return Self::persist_cancel(file, state, Some(part), sink, count).await;
            }
            let before = state.received();

            // Which holders are still worth asking?
            let live: Vec<usize> = (0..holders.len())
                .filter(|i| rejects[*i] < self.cfg.retry.max_attempts.max(1) * 8)
                .collect();
            if live.is_empty() {
                return Err(BlobError::Incomplete {
                    received: state.received(),
                    total: count,
                });
            }

            // Deal the outstanding chunks out, round-robin, skipping a holder
            // that says it does not have one. The last few are duplicated to a
            // second holder — endgame mode: one slow peer must not hold up the
            // tail of an otherwise finished transfer.
            let missing: Vec<u32> = (0..count).filter(|i| !state.is_set(*i)).collect();
            let endgame = missing.len() <= live.len().max(2) * 2;
            let mut assignment: Vec<Vec<u32>> = vec![Vec::new(); holders.len()];
            let mut cursor = 0usize;
            for &idx in missing.iter().take(budget as usize * live.len()) {
                let mut dealt = 0;
                let copies = if endgame { 2.min(live.len()) } else { 1 };
                while dealt < copies {
                    let h = live[cursor % live.len()];
                    cursor += 1;
                    let claims = holders[h]
                        .availability
                        .as_ref()
                        .is_none_or(|a| a.is_set(idx));
                    if claims && assignment[h].len() < budget as usize {
                        assignment[h].push(idx);
                        dealt += 1;
                    }
                    // Give up on this chunk for this round if nobody claims it.
                    if cursor.is_multiple_of(live.len()) && dealt == 0 {
                        break;
                    }
                }
            }

            // Ask each holder for its share, concurrently; verified leaves come
            // back over a channel so the file stays under one writer.
            type Verified = (u32, Vec<(u64, Vec<u8>)>, usize, u32);
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Verified>(live.len() * 2);
            let mut tasks = tokio::task::JoinSet::new();
            for h in live.iter().copied() {
                let indices = std::mem::take(&mut assignment[h]);
                if indices.is_empty() {
                    continue;
                }
                let ranges = coalesce(&indices);
                let selector = slice_selector(holders[h].origin.as_str(), &manifest.id, &ranges);
                let session = self.session.clone();
                let timeout = self.cfg.query_timeout;
                let priority = self.cfg.priority;
                let chunks = *chunks;
                let tx = tx.clone();
                tasks.spawn(async move {
                    let Ok(replies) = session
                        .get(&selector)
                        .consolidation(ConsolidationMode::None)
                        .priority(priority)
                        .timeout(timeout)
                        .await
                    else {
                        return;
                    };
                    while let Ok(reply) = replies.recv_async().await {
                        let Ok(sample) = reply.result() else { continue };
                        if !ENC_SLICE.matches(sample.encoding()) {
                            continue;
                        }
                        let Some(index) = parse_slice_index(sample.key_expr().as_str()) else {
                            continue;
                        };
                        let mut leaves: Vec<(u64, Vec<u8>)> = Vec::new();
                        let ok = verify::decode_slice(
                            &root,
                            total_len,
                            verify::chunk_range(chunks.byte_range(index)),
                            &sample.payload().to_bytes(),
                            |off, data| {
                                leaves.push((off, data.to_vec()));
                                Ok(())
                            },
                        )
                        .is_ok();
                        let msg = if ok {
                            (index, leaves, h, 0)
                        } else {
                            (index, Vec::new(), h, 1)
                        };
                        if tx.send(msg).await.is_err() {
                            return;
                        }
                    }
                });
            }
            drop(tx);

            loop {
                // As in the single-origin loop: cancellation must be observed
                // while waiting on the holders, not only between rounds.
                // Dropping `tasks` here aborts the outstanding queries.
                let Some(next) = cancel.until_cancelled(rx.recv()).await else {
                    return Self::persist_cancel(file, state, Some(part), sink, count).await;
                };
                let Some((index, leaves, holder, rejected)) = next else {
                    break;
                };
                if rejected > 0 {
                    rejects[holder] += rejected;
                    stats.rejected += rejected;
                    continue;
                }
                if index >= count || state.is_set(index) {
                    continue; // an endgame duplicate that lost the race
                }
                for (off, data) in leaves {
                    file.write_leaf(off, &data).await?;
                }
                if state.mark(index) {
                    bytes_received += chunks.len_of(index) as u64;
                    stats.chunks_fetched += 1;
                    stats.bytes_fetched += chunks.len_of(index) as u64;
                    sink.emit(Progress::Chunk {
                        index,
                        received: state.received(),
                        total: count,
                        bytes_received,
                    });
                }
            }
            tasks.shutdown().await;
            stats.queries += live.len() as u64;
            file.commit().await?;
            state.save_atomic(part).await?;

            if state.is_complete(count) {
                break;
            }
            if state.received() == before {
                no_progress += 1;
                stats.retries += 1;
                if no_progress >= self.cfg.retry.max_attempts {
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
        state.save_atomic(part).await?;
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

/// A configured download, awaited to run it.
///
/// Returned by [`BlobClient::download_to`]. The two arguments a transfer
/// cannot do without — what to fetch and where to put it — are positional;
/// progress, cancellation, overwrite policy and multi-origin striping are
/// optional and set here.
///
/// The old shape took all five positionally, which meant `&()` and
/// `&CancelToken::new()` at nearly every call site: two arguments that
/// existed to say "no thanks", and could be swapped with the ones that
/// mattered without the compiler noticing.
///
/// ```no_run
/// # use zblob::{BlobClient, DownloadRequest, CancelToken, Overwrite};
/// # async fn f(client: BlobClient, req: DownloadRequest, cancel: CancelToken) -> zblob::Result<()> {
/// let stats = client.download_to(&req, "/tmp/out.bin".as_ref()).await?;
///
/// let stats = client
///     .download_to(&req, "/tmp/out.bin".as_ref())
///     .cancel(&cancel)
///     .overwrite(Overwrite::Replace)
///     .await?;
/// # let _ = stats; Ok(()) }
/// ```
#[must_use = "a download does nothing until it is awaited"]
pub struct Download<'a> {
    client: &'a BlobClient,
    req: &'a DownloadRequest,
    dest: &'a Path,
    sink: Option<&'a dyn ProgressSink>,
    cancel: Option<&'a CancelToken>,
    overwrite: Option<Overwrite>,
    holders: Option<&'a [BlobProbe]>,
}

impl std::fmt::Debug for Download<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Download")
            .field("req", &self.req)
            .field("dest", &self.dest)
            .field("overwrite", &self.overwrite)
            .field("holders", &self.holders.map(<[BlobProbe]>::len))
            .finish_non_exhaustive()
    }
}

impl<'a> Download<'a> {
    /// Send progress events to `sink` (default: discard them).
    pub fn progress(mut self, sink: &'a dyn ProgressSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Stop when `cancel` is cancelled, persisting resume state
    /// (default: never).
    pub fn cancel(mut self, cancel: &'a CancelToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Override the client's overwrite policy for this transfer.
    ///
    /// It was previously settable only per *client*, which is the wrong
    /// granularity: whether replacing an existing file is acceptable is a
    /// property of the transfer, not of the connection.
    pub fn overwrite(mut self, policy: Overwrite) -> Self {
        self.overwrite = Some(policy);
        self
    }

    /// Fetch from several holders at once, dealing chunks between them.
    ///
    /// Get `holders` from [`BlobClient::probe`]. With fewer than two this
    /// degrades to an ordinary download, which is the right thing rather than
    /// an error.
    pub fn striped(mut self, holders: &'a [BlobProbe]) -> Self {
        self.holders = Some(holders);
        self
    }
}

/// The no-op progress sink used when a call sets none.
pub(crate) const NO_PROGRESS: &(dyn ProgressSink + 'static) = &();

impl<'a> std::future::IntoFuture for Download<'a> {
    type Output = Result<TransferStats>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let sink = self.sink.unwrap_or(NO_PROGRESS);
            let fresh;
            let cancel = match self.cancel {
                Some(c) => c,
                None => {
                    fresh = CancelToken::new();
                    &fresh
                }
            };
            let overwrite = self.overwrite.unwrap_or(self.client.cfg.overwrite);
            match self.holders {
                Some(holders) => {
                    self.client
                        .run_download_striped(self.req, self.dest, holders, sink, cancel, overwrite)
                        .await
                }
                None => {
                    self.client
                        .run_download_to(self.req, self.dest, sink, cancel, overwrite)
                        .await
                }
            }
        })
    }
}

/// A configured staged download, awaited to run it. See
/// [`BlobClient::download_staged`].
#[must_use = "a download does nothing until it is awaited"]
#[derive(Debug)]
pub struct StagedDownload<'a> {
    inner: Download<'a>,
}

impl<'a> StagedDownload<'a> {
    /// Send progress events to `sink` (default: discard them).
    pub fn progress(mut self, sink: &'a dyn ProgressSink) -> Self {
        self.inner = self.inner.progress(sink);
        self
    }

    /// Stop when `cancel` is cancelled, persisting resume state.
    pub fn cancel(mut self, cancel: &'a CancelToken) -> Self {
        self.inner = self.inner.cancel(cancel);
        self
    }

    /// Override the client's overwrite policy for this transfer.
    pub fn overwrite(mut self, policy: Overwrite) -> Self {
        self.inner = self.inner.overwrite(policy);
        self
    }
}

impl<'a> std::future::IntoFuture for StagedDownload<'a> {
    type Output = Result<Staged>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let d = self.inner;
            let sink = d.sink.unwrap_or(NO_PROGRESS);
            let fresh;
            let cancel = match d.cancel {
                Some(c) => c,
                None => {
                    fresh = CancelToken::new();
                    &fresh
                }
            };
            let overwrite = d.overwrite.unwrap_or(d.client.cfg.overwrite);
            d.client
                .run_download_staged(d.req, d.dest, sink, cancel, overwrite)
                .await
        })
    }
}

/// A configured download into a writer, awaited to run it. See
/// [`BlobClient::download_to_writer`].
#[must_use = "a download does nothing until it is awaited"]
pub struct DownloadToWriter<'a, W> {
    client: &'a BlobClient,
    req: &'a DownloadRequest,
    writer: &'a mut W,
    sink: Option<&'a dyn ProgressSink>,
    cancel: Option<&'a CancelToken>,
}

impl<W> std::fmt::Debug for DownloadToWriter<'_, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadToWriter")
            .field("req", &self.req)
            .finish_non_exhaustive()
    }
}

impl<'a, W> DownloadToWriter<'a, W> {
    /// Send progress events to `sink` (default: discard them).
    pub fn progress(mut self, sink: &'a dyn ProgressSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Stop when `cancel` is cancelled. There is no resume state to persist
    /// here — an interrupted writer download starts over.
    pub fn cancel(mut self, cancel: &'a CancelToken) -> Self {
        self.cancel = Some(cancel);
        self
    }
}

impl<'a, W> std::future::IntoFuture for DownloadToWriter<'a, W>
where
    W: tokio::io::AsyncWrite + tokio::io::AsyncSeek + Unpin + Send,
{
    type Output = Result<TransferStats>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let sink = self.sink.unwrap_or(NO_PROGRESS);
            let fresh;
            let cancel = match self.cancel {
                Some(c) => c,
                None => {
                    fresh = CancelToken::new();
                    &fresh
                }
            };
            self.client
                .run_download_to_writer(self.req, self.writer, sink, cancel)
                .await
        })
    }
}

/// A configured upload, awaited to run it. See [`BlobClient::upload_file`].
#[must_use = "an upload does nothing until it is awaited"]
pub struct Upload<'a> {
    client: &'a BlobClient,
    spec: BlobSpec,
    path: PathBuf,
    token: Option<Vec<u8>>,
    sink: Option<&'a dyn ProgressSink>,
    cancel: Option<&'a CancelToken>,
}

impl std::fmt::Debug for Upload<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upload")
            .field("spec", &self.spec)
            .field("path", &self.path)
            .field("token", &self.token.as_ref().map(Vec::len))
            .finish_non_exhaustive()
    }
}

impl<'a> Upload<'a> {
    /// Attach an opaque credential the server's
    /// [`PushPolicy`](crate::PushPolicy) sees with every request.
    pub fn token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Send progress events to `sink` (default: discard them).
    pub fn progress(mut self, sink: &'a dyn ProgressSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Stop when `cancel` is cancelled. The server keeps its spool, so a
    /// later upload of the same id resumes from what it already holds.
    pub fn cancel(mut self, cancel: &'a CancelToken) -> Self {
        self.cancel = Some(cancel);
        self
    }
}

impl<'a> std::future::IntoFuture for Upload<'a> {
    type Output = Result<Manifest>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let sink = self.sink.unwrap_or(NO_PROGRESS);
            let fresh;
            let cancel = match self.cancel {
                Some(c) => c,
                None => {
                    fresh = CancelToken::new();
                    &fresh
                }
            };
            self.client
                .run_upload_file(self.spec, self.path, self.token, sink, cancel)
                .await
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

/// Turn a sorted list of chunk indices into the coalesced half-open spans the
/// `ranges` selector grammar wants.
fn coalesce(indices: &[u32]) -> Vec<std::ops::Range<u32>> {
    let mut out: Vec<std::ops::Range<u32>> = Vec::new();
    for &i in indices {
        match out.last_mut() {
            Some(last) if last.end == i => last.end = i + 1,
            _ => out.push(i..i + 1),
        }
    }
    out.truncate(MAX_RANGE_SPANS);
    out
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
            return Err(BlobError::Usage(format!(
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
