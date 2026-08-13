//! The blob server: a queryable that serves a manifest + verified bao slices
//! for any registered blob, lazily and with backpressure.
//!
//! Registration is where the whole blob is read (once): the server streams the
//! source through BLAKE3 to build the *outboard* (the parent-hash tree bao
//! slices are cut from) and derives the [`Manifest`] — so a served manifest
//! can never disagree with the bytes, and a crafted manifest can never panic
//! the serving path (v1's H1). Serving then reads only the requested byte
//! ranges.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bao_tree::io::sync::{ReadAt, Size};
use tokio::sync::{Notify, RwLock, Semaphore};

use crate::chunk::TransferChunks;
use crate::error::{BlobError, Result};
use crate::id::BlobId;
use crate::manifest::{BlobSpec, Manifest, validate_id};
use crate::obs::{zdebug, zwarn};
use crate::prefix::ServePrefix;
use crate::resume::ResumeState;
use crate::verify::{self, OutboardStore, ReadAtCursor};
use crate::wire::{Availability, ENC_AVAIL, ENC_MANIFEST, ENC_PUSH, ENC_SLICE, encode};
use crate::{manifest_key, parse_id, parse_ranges, slice_key};

/// A positional, sized, thread-safe byte source: what a [`BlobSource`] opens.
///
/// Blanket-implemented for anything `ReadAt + Size + Send + Sync` (notably
/// `std::fs::File`). Positional reads let bao encoding address any byte range
/// without seek state, so one reader serves a whole range query.
pub trait ReadAtSize: ReadAt + Size + Send + Sync {}
impl<T: ReadAt + Size + Send + Sync> ReadAtSize for T {}

/// Opens a fresh reader over a blob's bytes. Called once per registration (to
/// hash) and once per query (to serve), so the source can be reopened many
/// times. Opening is synchronous — implementations should be cheap (an
/// `open(2)`, not a download) and are always invoked on the blocking pool.
pub trait BlobSource: Send + Sync {
    /// Open a new positional reader over the blob.
    fn open(&self) -> std::io::Result<Box<dyn ReadAtSize>>;

    /// A cheap snapshot of the source's current identity, if it has one.
    ///
    /// Registration streams the source once to build its bao outboard, and
    /// everything served afterwards is proved against that. If the underlying
    /// bytes then change, every slice fails the *client's* verification —
    /// forever, with the client seeing only a rising rejected count and
    /// eventually [`BlobError::Incomplete`], and the server seeing nothing at
    /// all. Comparing this value at serve time turns that into one clear error
    /// on the side that can actually fix it.
    ///
    /// Returning `None` means "this source cannot change", which is why the
    /// default is `None`: a source that *can* change should say so.
    fn fingerprint(&self) -> Option<SourceFingerprint> {
        None
    }
}

/// A cheap identity snapshot of a [`BlobSource`] (see
/// [`BlobSource::fingerprint`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceFingerprint {
    /// Length in bytes.
    pub len: u64,
    /// Modification time in nanoseconds since the Unix epoch, where the source
    /// has one.
    pub mtime_ns: Option<i128>,
}

/// A [`BlobSource`] backed by a file on disk (e.g. a TTL'd report bundle).
pub struct FileBlobSource {
    path: PathBuf,
}

impl FileBlobSource {
    /// Serve the file at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        FileBlobSource { path: path.into() }
    }
}

impl BlobSource for FileBlobSource {
    fn open(&self) -> std::io::Result<Box<dyn ReadAtSize>> {
        Ok(Box::new(std::fs::File::open(&self.path)?))
    }

    fn fingerprint(&self) -> Option<SourceFingerprint> {
        let meta = std::fs::metadata(&self.path).ok()?;
        Some(SourceFingerprint {
            len: meta.len(),
            mtime_ns: meta.modified().ok().and_then(|t| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_nanos() as i128)
            }),
        })
    }
}

/// A [`BlobSource`] serving a shared in-memory buffer (generated artifacts,
/// tests). The buffer is behind an `Arc`, so opening is free and concurrent
/// transfers share one allocation.
pub struct MemoryBlobSource(Arc<Vec<u8>>);

impl MemoryBlobSource {
    /// Serve `data` from memory.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        MemoryBlobSource(Arc::new(data.into()))
    }

    /// Serve an already-shared buffer without copying.
    pub fn from_arc(data: Arc<Vec<u8>>) -> Self {
        MemoryBlobSource(data)
    }
}

struct ArcBytes(Arc<Vec<u8>>);
impl ReadAt for ArcBytes {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.as_slice().read_at(pos, buf)
    }
}
impl Size for ArcBytes {
    fn size(&self) -> std::io::Result<Option<u64>> {
        Ok(Some(self.0.len() as u64))
    }
}

impl BlobSource for MemoryBlobSource {
    fn open(&self) -> std::io::Result<Box<dyn ReadAtSize>> {
        Ok(Box::new(ArcBytes(self.0.clone())))
    }
}

/// Borrow a boxed source as a [`ReadAt`] for bao encoding.
struct DynReadAt<'a>(&'a dyn ReadAtSize);
impl ReadAt for DynReadAt<'_> {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read_at(pos, buf)
    }
}

struct Registered {
    manifest: Manifest,
    chunks: TransferChunks,
    source: Arc<dyn BlobSource>,
    outboard: Arc<OutboardStore>,
    /// The source's identity when its outboard was computed. Compared at
    /// serve time; see `BlobSource::fingerprint`.
    fingerprint: Option<SourceFingerprint>,
}

/// Callback invoked with every error raised while serving a query.
pub type ErrorCallback = Arc<dyn Fn(&BlobError) + Send + Sync>;

/// Authorization hook for the push (upload) protocol: called for the initial
/// offer *and* for every pushed slice, with the offered manifest and the
/// opaque token the uploader attached to the query (if any).
pub trait PushPolicy: Send + Sync {
    /// Whether to accept this upload.
    fn allow(&self, manifest: &Manifest, token: Option<&[u8]>) -> bool;
}

/// How a server accepts verified pushes (uploads), passed to
/// [`BlobServerBuilder::accept_push`].
///
/// The resource bounds live here rather than on the server builder because
/// they are meaningless without a push configuration. When they were builder
/// methods, `.push_max_concurrent(2).accept_push(policy, dir)` compiled and
/// silently kept the default of 8 — the setter found no config to write into
/// and did nothing. Their doc comments said "call after `accept_push`", which
/// is documentation compensating for a type error; the type now enforces it.
#[derive(Clone)]
pub struct PushConfig {
    policy: Arc<dyn PushPolicy>,
    spool_dir: std::path::PathBuf,
    max_blob_size: u64,
    max_concurrent: usize,
    idle_timeout: std::time::Duration,
}

impl std::fmt::Debug for PushConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushConfig")
            .field("spool_dir", &self.spool_dir)
            .field("max_blob_size", &self.max_blob_size)
            .field("max_concurrent", &self.max_concurrent)
            .field("idle_timeout", &self.idle_timeout)
            .finish_non_exhaustive()
    }
}

impl PushConfig {
    /// A push configuration with default bounds: `policy` authorizes each
    /// offer and slice, `spool_dir` holds in-progress `.part`s and completed
    /// blobs.
    #[must_use]
    pub fn new(policy: Arc<dyn PushPolicy>, spool_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            policy,
            spool_dir: spool_dir.into(),
            max_blob_size: 1 << 40,
            max_concurrent: 8,
            idle_timeout: std::time::Duration::from_secs(3600),
        }
    }

    /// Largest `total_len` an upload offer may declare (default 1 TiB) — the
    /// bound on spool preallocation.
    #[must_use]
    pub fn max_blob_size(mut self, bytes: u64) -> Self {
        self.max_blob_size = bytes;
        self
    }

    /// Max concurrent in-progress pushes (default 8).
    #[must_use]
    pub fn max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n.max(1);
        self
    }

    /// Idle time after which an abandoned push is evicted and its spool files
    /// removed (default 1 h).
    #[must_use]
    pub fn idle_timeout(mut self, t: std::time::Duration) -> Self {
        self.idle_timeout = t;
        self
    }

    /// The directory holding in-progress and completed pushes.
    #[must_use]
    pub fn spool_dir(&self) -> &std::path::Path {
        &self.spool_dir
    }
}

/// An in-progress push: spooled `.part` + resume bitfield, mirroring the
/// download client's state machine on the receiving side.
struct PushEntry {
    manifest: Manifest,
    chunks: TransferChunks,
    state: ResumeState,
    last_activity: tokio::time::Instant,
    /// Marks since the sidecar was last persisted (saves are batched).
    dirty_marks: u32,
}

#[derive(Clone)]
struct ServerConfig {
    max_inflight: usize,
    max_chunks_per_query: u32,
    outboard_mem_limit: u64,
    on_error: Option<ErrorCallback>,
    push: Option<PushConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            max_inflight: 8,
            max_chunks_per_query: 512,
            // ~16 MiB of outboard ≈ a 4 GiB blob; larger file-backed blobs
            // keep their outboard in a sibling file.
            outboard_mem_limit: 16 * 1024 * 1024,
            on_error: None,
            push: None,
        }
    }
}

struct Inner {
    session: Arc<zenoh::Session>,
    prefix: ServePrefix,
    registry: RwLock<HashMap<BlobId, Registered>>,
    inflight: Arc<Semaphore>,
    cfg: ServerConfig,
    pushes: tokio::sync::Mutex<HashMap<BlobId, PushEntry>>,
}

/// Serves registered blobs over a Zenoh queryable at `<prefix>/**`.
///
/// # Several servers may share one prefix
///
/// A server **ignores ids it has not registered**: an unknown id draws no
/// reply at all, not an error. So two or more `BlobServer`s can serve the same
/// prefix, each owning a disjoint set of ids, and a client's query is answered
/// by whichever one owns the id. This is supported and depended on — a sensor
/// that serves long-lived artifacts from one server and short-TTL captures
/// from another is the motivating case — so it is promised here rather than
/// left as an accident of the implementation.
///
/// Two consequences follow from it:
///
/// - **Silence is cheap, not expensive.** It is tempting to assume an unknown
///   id costs the client its query timeout, and to design a negative reply to
///   avoid that. It does not: a Zenoh query finalizes once every matching
///   queryable has completed, and completing without replying is immediate.
///   Measured at roughly a millisecond against a 30-second timeout, with a
///   server present, with none present, and across a wildcard fan-out. A
///   negative reply would buy nothing here and would have to carry an
///   awkward rule — "authoritative only when no positive reply arrives" —
///   precisely because of this arrangement.
/// - **A refusal from one server is not a refusal from all of them.** The push
///   path treats an error reply as one responder's opinion and keeps waiting
///   for an acceptance, exactly as every download loop treats an unusable
///   reply (see the crate docs, fact 3).
#[derive(Clone)]
pub struct BlobServer {
    inner: Arc<Inner>,
}

/// Builder for a [`BlobServer`] (see [`BlobServer::builder`]).
pub struct BlobServerBuilder {
    session: Arc<zenoh::Session>,
    prefix: ServePrefix,
    cfg: ServerConfig,
}

impl BlobServerBuilder {
    /// Max concurrent in-flight queries served at once (default 8). A coarse
    /// anti-DoS backstop; real authorization is the caller's job.
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.cfg.max_inflight = n.max(1);
        self
    }

    /// Max chunks one range query may request (default 512). Clients split
    /// larger hole sets across sequential queries.
    pub fn max_chunks_per_query(mut self, n: u32) -> Self {
        self.cfg.max_chunks_per_query = n.max(1);
        self
    }

    /// Outboard size above which [`BlobServer::register_file`] keeps the
    /// outboard in a sibling `<path>.obao4` file instead of memory
    /// (default 16 MiB of outboard ≈ a 4 GiB blob).
    pub fn outboard_mem_limit(mut self, bytes: u64) -> Self {
        self.cfg.outboard_mem_limit = bytes;
        self
    }

    /// Invoke `cb` with every serve error (default: a `tracing` warn event
    /// with the `tracing` feature, else stderr in debug builds only).
    pub fn on_error(mut self, cb: ErrorCallback) -> Self {
        self.cfg.on_error = Some(cb);
        self
    }

    /// Accept verified pushes (uploads) under `cfg` — see [`PushConfig`] for
    /// the policy hook, the spool directory and the resource bounds.
    ///
    /// Completed uploads are automatically registered and served. Pushes are
    /// **rejected unless this is configured**, and an offer for an id that is
    /// already registered with different content is refused (a push must
    /// never hijack a served blob). Completed `<id>.blob` files stay in the
    /// spool directory as the registered blob's backing store — remove them
    /// after [`BlobServer::unregister`] when a pushed blob is retired.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use zblob::{BlobServer, PushConfig, PushPolicy, Manifest, ServePrefix};
    /// # fn f(session: Arc<zenoh::Session>, prefix: ServePrefix, policy: Arc<dyn PushPolicy>) {
    /// let server = BlobServer::builder(session, prefix)
    ///     .accept_push(
    ///         PushConfig::new(policy, "/var/spool/zblob")
    ///             .max_concurrent(2)
    ///             .max_blob_size(64 << 20),
    ///     )
    ///     .build();
    /// # }
    /// ```
    pub fn accept_push(mut self, cfg: PushConfig) -> Self {
        self.cfg.push = Some(cfg);
        self
    }

    /// Build the server.
    pub fn build(self) -> BlobServer {
        BlobServer {
            inner: Arc::new(Inner {
                session: self.session,
                prefix: self.prefix,
                registry: RwLock::new(HashMap::new()),
                inflight: Arc::new(Semaphore::new(self.cfg.max_inflight)),
                cfg: self.cfg,
                pushes: tokio::sync::Mutex::new(HashMap::new()),
            }),
        }
    }
}

/// Handle to a spawned server task: keeps it running, stops it on demand.
pub struct ServerHandle {
    join: tokio::task::JoinHandle<Result<()>>,
    stop: Arc<Notify>,
}

impl ServerHandle {
    pub(crate) fn new(join: tokio::task::JoinHandle<Result<()>>, stop: Arc<Notify>) -> Self {
        ServerHandle { join, stop }
    }

    /// Ask the serve loop to stop and wait for it to finish. Query tasks
    /// already in flight complete on their own.
    pub async fn shutdown(self) -> Result<()> {
        self.stop.notify_one();
        self.join.await.map_err(BlobError::Task)?
    }
}

impl BlobServer {
    /// Start building a server for blobs under `key_prefix`.
    pub fn builder(session: Arc<zenoh::Session>, key_prefix: ServePrefix) -> BlobServerBuilder {
        BlobServerBuilder {
            session,
            prefix: key_prefix,
            cfg: ServerConfig::default(),
        }
    }

    /// Build a server with default configuration (see [`BlobServer::builder`]).
    pub fn new(session: Arc<zenoh::Session>, key_prefix: ServePrefix) -> Self {
        Self::builder(session, key_prefix).build()
    }

    /// Register the file at `path` under `spec`, computing its BLAKE3 outboard
    /// by streaming it once. Returns the manifest so the caller can distribute
    /// `(id, root)` out of band — the root is what downloaders should pin.
    pub async fn register_file(
        &self,
        spec: BlobSpec,
        path: impl Into<PathBuf>,
    ) -> Result<Manifest> {
        let path = path.into();
        let mem_limit = self.inner.cfg.outboard_mem_limit;
        let hash_path = path.clone();
        let (outboard, total_len) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(OutboardStore, u64)> {
                let file = std::fs::File::open(&hash_path)?;
                let total_len = file.metadata()?.len();
                let est_outboard = total_len / verify::GROUP_SIZE * 64;
                let store = if est_outboard > mem_limit {
                    let obao = {
                        let mut p = hash_path.as_os_str().to_os_string();
                        p.push(".obao4");
                        PathBuf::from(p)
                    };
                    OutboardStore::File(verify::compute_outboard_file(&file, total_len, &obao)?)
                } else {
                    OutboardStore::Mem(verify::compute_outboard(file)?)
                };
                Ok((store, total_len))
            })
            .await??;
        self.finish_register(
            spec,
            Arc::new(FileBlobSource::new(path)),
            outboard,
            total_len,
        )
        .await
    }

    /// Register an arbitrary [`BlobSource`] under `spec`, computing its
    /// outboard in memory by streaming the source once. For very large
    /// file-backed blobs prefer [`BlobServer::register_file`], which can spill
    /// the outboard to disk.
    pub async fn register_source(
        &self,
        spec: BlobSpec,
        source: Arc<dyn BlobSource>,
    ) -> Result<Manifest> {
        let src = source.clone();
        let (outboard, total_len) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(OutboardStore, u64)> {
                let reader = src.open()?;
                let total_len = reader
                    .size()?
                    .ok_or_else(|| std::io::Error::other("source has no known size"))?;
                let ob = verify::compute_outboard_sized(
                    ReadAtCursor::new(DynReadAt(&*reader)),
                    total_len,
                )?;
                Ok((OutboardStore::Mem(ob), total_len))
            })
            .await??;
        self.finish_register(spec, source, outboard, total_len)
            .await
    }

    async fn finish_register(
        &self,
        spec: BlobSpec,
        source: Arc<dyn BlobSource>,
        outboard: OutboardStore,
        total_len: u64,
    ) -> Result<Manifest> {
        validate_id(&spec.id)?;
        let chunks = TransferChunks::new(spec.chunk_size, total_len)?;
        let manifest = Manifest {
            version: crate::wire::WIRE_VERSION,
            id: BlobId::new(spec.id.clone())?,
            filename: spec.filename,
            total_len,
            chunk_size: spec.chunk_size,
            root: outboard.root().into(),
            created_ms: spec.created_ms,
            // Advertise this server's limits so a client with different
            // defaults clamps to them instead of having its queries rejected.
            ext: {
                let mut ext = crate::wire::Ext::new();
                ext.set_u32(
                    crate::wire::EXT_MAX_CHUNKS_PER_QUERY,
                    self.inner.cfg.max_chunks_per_query,
                )?;
                ext.set_u64(
                    crate::wire::EXT_MAX_BLOB_SIZE,
                    push_max_blob_size(&self.inner.cfg),
                )?;
                ext
            },
        };
        // Replacing an id's content is refused, matching the push path — which
        // goes to considerable lengths to prevent exactly this hijack
        // (`push_offer_inner`: identical content is an idempotent no-op,
        // different content is an error). A local caller is more trusted than
        // a remote one, but "the bytes behind this id changed and nobody was
        // told" is the same hazard either way: downloaders resume against a
        // root that no longer exists, and a pinned fetch starts failing with
        // no explanation. Re-registering *identical* content stays a no-op;
        // genuinely replacing content is `unregister` then register, which
        // says what it means.
        let mut registry = self.inner.registry.write().await;
        if let Some(existing) = registry.get(spec.id.as_str()) {
            if existing.manifest.root == manifest.root {
                return Ok(manifest);
            }
            return Err(BlobError::Usage(format!(
                "id {:?} is already registered with different content (root {}, offered {}); \
                 unregister it first",
                spec.id, existing.manifest.root, manifest.root
            )));
        }
        zdebug!(id = %manifest.id, total_len, root = %manifest.root, "blob registered");
        let fingerprint = source.fingerprint();
        registry.insert(
            manifest.id.clone(),
            Registered {
                manifest: manifest.clone(),
                chunks,
                source,
                outboard: Arc::new(outboard),
                fingerprint,
            },
        );
        Ok(manifest)
    }

    /// Stop serving blob `id` (e.g. after its TTL expires).
    pub async fn unregister(&self, id: &str) {
        self.inner.registry.write().await.remove(id);
    }

    /// Declare the queryable, spawn the serve loop on the current runtime, and
    /// return a stop handle. The queryable is live when this returns — a
    /// client may query immediately (no sleep-and-hope synchronization).
    pub async fn spawn(self) -> Result<ServerHandle> {
        let queryable = self.declare().await?;
        let stop = Arc::new(Notify::new());
        let stop2 = stop.clone();
        Ok(ServerHandle {
            join: tokio::spawn(self.serve_loop(queryable, stop2)),
            stop,
        })
    }

    /// Declare the queryable and serve until the session closes. Each query is
    /// served on its own task so a slow client cannot block others.
    pub async fn run(self) -> Result<()> {
        let queryable = self.declare().await?;
        self.serve_loop(queryable, Arc::new(Notify::new())).await
    }

    async fn declare(&self) -> Result<FifoQueryable> {
        let key = format!("{}/**", self.inner.prefix);
        self.inner
            .session
            .declare_queryable(&key)
            .await
            .map_err(BlobError::zenoh)
    }

    async fn serve_loop(self, queryable: FifoQueryable, stop: Arc<Notify>) -> Result<()> {
        loop {
            tokio::select! {
                _ = stop.notified() => break,
                q = queryable.recv_async() => {
                    let Ok(query) = q else { break };
                    let inner = self.inner.clone();
                    // Take the in-flight permit *here*, before spawning. Taken
                    // inside the task it would bound concurrent work but not
                    // the number of queued tasks, each of which holds its
                    // `Query` — and a push slice holds a whole chunk payload.
                    // Awaiting here also stops draining the queryable, which is
                    // the backpressure the semaphore was meant to be.
                    let Ok(permit) = inner.inflight.clone().acquire_owned().await else { break };
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Err(e) = serve_one(&inner, query).await {
                            report_error(&inner.cfg.on_error, &e);
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// What this server will accept as a whole blob, for advertisement. Only the
/// push path configures a limit; a read-only server has none of its own, so it
/// reports the widest value rather than inventing one.
fn push_max_blob_size(cfg: &ServerConfig) -> u64 {
    cfg.push.as_ref().map_or(u64::MAX, |p| p.max_blob_size)
}

/// Surface a serve error: the configured callback wins; otherwise a `tracing`
/// warn event (with the feature) or stderr in debug builds.
pub(crate) fn report_error(cb: &Option<ErrorCallback>, e: &BlobError) {
    zwarn!(error = %e, "serve error");
    if let Some(cb) = cb {
        cb(e);
    } else {
        #[cfg(all(debug_assertions, not(feature = "tracing")))]
        eprintln!("zblob: serve error: {e}");
    }
    let _ = e;
}

/// The declared-queryable type with zenoh's default FIFO handler.
pub(crate) type FifoQueryable =
    zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>;

async fn serve_one(inner: &Inner, query: zenoh::query::Query) -> Result<()> {
    let key_str = query.key_expr().as_str().to_string();
    let Some(id) = parse_id(inner.prefix.as_str(), &key_str) else {
        return Ok(()); // not a per-blob query; ignore.
    };

    // Push protocol (upload): dispatched before the registry lookup — a blob
    // being pushed is not registered yet.
    if key_str.ends_with("/push/offer") {
        return handle_push_offer(inner, query, &id).await;
    }
    if let Some(idx) = key_str
        .rsplit_once("/push/slice/")
        .and_then(|(head, i)| head.ends_with(&id).then(|| i.parse::<u32>().ok()).flatten())
    {
        return handle_push_slice(inner, query, &id, idx).await;
    }

    // Snapshot the registration (clone the cheap manifest + Arc the rest) so we
    // don't hold the registry lock across the stream.
    let (manifest, chunks, source, outboard, registered_fingerprint) = {
        let reg = inner.registry.read().await;
        match reg.get(id.as_str()) {
            Some(r) => (
                r.manifest.clone(),
                r.chunks,
                r.source.clone(),
                r.outboard.clone(),
                r.fingerprint,
            ),
            None => {
                // Unknown id: drop the query without replying.
                //
                // This does *not* cost the client its timeout — a query
                // finalizes once every matching queryable has completed, and
                // completing without replying is immediate (measured: ~1 ms
                // against a 30 s timeout; see the coverage test). Silence is
                // therefore the correct way to say "not mine", and it is what
                // lets several servers share one prefix.
                return Ok(());
            }
        }
    };

    // Has the source changed since its outboard was computed?
    //
    // Everything served is proved against that outboard, so if the backing
    // bytes moved, every slice fails verification on the *client* — forever,
    // with the client seeing only a rising rejected count and eventually
    // `Incomplete`, and this server seeing nothing at all. Neither end can
    // diagnose it. One `stat` per query converts that into a single error on
    // the side that can fix it. Sources that cannot change report `None` and
    // pay nothing.
    if let (Some(then), Some(now)) = (registered_fingerprint, source.fingerprint())
        && then != now
    {
        let e = BlobError::Protocol(format!(
            "the source behind blob {id:?} changed after registration              (was {} bytes, now {}); re-register it — every slice served from              the stale outboard would fail the client's verification",
            then.len, now.len
        ));
        let _ = query.reply_err(e.to_string()).await;
        return Err(e);
    }

    // Availability: which chunks can this server actually serve?
    //
    // All of them, and that is not a placeholder — it is the only answer tier 1
    // can honestly give. A slice is a *bao* slice: the chunk's bytes plus the
    // sibling hashes proving them against the root. Those siblings are hashes
    // of other subtrees, so producing one requires the whole blob. A holder
    // with part of a blob cannot serve any verified slice of it, which is why
    // the outboard is computed at registration and at `finalize_push` — never
    // from a partial spool.
    //
    // So partial holders are not expressible on tier 1, and an endpoint that
    // claimed otherwise would advertise chunks no client could obtain. What
    // this endpoint is genuinely for is *who has this blob at all*, which is
    // what `BlobClient::download_striped` uses to spread a transfer.
    //
    // Tier 2 is where partial possession is real — a `ContentStore` holds
    // whatever subset it holds, and each chunk is verified against its own
    // address rather than against a whole-object root. That is what
    // `StoreClient::probe` reports.
    if key_str.ends_with("/have") {
        let avail = Availability::full(chunks.count());
        query
            .reply(
                crate::availability_key(inner.prefix.as_str(), &id),
                encode(&avail)?,
            )
            .encoding(ENC_AVAIL)
            .await
            .map_err(BlobError::zenoh)?;
        return Ok(());
    }

    // Manifest-only request: exact `.../manifest` GET.
    if key_str.ends_with("/manifest") {
        let payload = encode(&manifest)?;
        query
            .reply(manifest_key(inner.prefix.as_str(), &id), payload)
            .encoding(ENC_MANIFEST)
            .await
            .map_err(BlobError::zenoh)?;
        return Ok(());
    }

    // Range-set slice request: validate before doing any work — a malformed
    // selector must not drive the server into unbounded reads.
    let ranges = match parse_ranges(
        query.parameters().as_str(),
        chunks.count(),
        inner.cfg.max_chunks_per_query,
    ) {
        Ok(r) => r,
        Err(e) => {
            let _ = query.reply_err(e.to_string()).await;
            return Err(e);
        }
    };

    // One reader per query; bao slices are encoded on the blocking pool and
    // streamed reply-by-reply.
    let mut reader = {
        let source = source.clone();
        tokio::task::spawn_blocking(move || source.open()).await??
    };
    for range in ranges {
        for index in range {
            let ob = outboard.clone();
            let byte_range = chunks.byte_range(index);
            let (r, slice) = tokio::task::spawn_blocking(
                move || -> (Box<dyn ReadAtSize>, std::io::Result<Vec<u8>>) {
                    let slice =
                        ob.encode_slice(DynReadAt(&*reader), verify::chunk_range(byte_range));
                    (reader, slice)
                },
            )
            .await?;
            reader = r;
            let slice = slice?;
            // A reply error means the client dropped the GET (query finalized):
            // stop promptly instead of streaming the rest into the void.
            if query
                .reply(slice_key(inner.prefix.as_str(), &id, index), slice)
                .encoding(ENC_SLICE)
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
    let _ = manifest; // identity travels via the manifest GET; slices are self-verifying.
    Ok(())
}

/// The spool `.part` path for an in-progress push of `id`.
fn push_part_path(push: &PushConfig, id: &str) -> std::path::PathBuf {
    push.spool_dir.join(format!("{id}.push.part"))
}

/// Handle an upload offer: authorize, refuse registry collisions, evict
/// stale pushes, create/resume spool state, and reply with the chunk ranges
/// the server still wants. Spool I/O runs with the pushes lock **released** —
/// one slow uploader must not stall the others.
/// Handle a push offer, guaranteeing the uploader hears *something*.
///
/// Every early return inside already replies. What this wrapper covers is the
/// `?` paths — encoding, geometry, spool I/O — which would otherwise propagate
/// with no reply at all, leaving the uploader to wait out the full query
/// timeout and then report "no push endpoint answered the offer": a
/// server-side validation failure diagnosed as a missing server. An extra
/// `reply_err` after a successful reply is harmless; the client takes the
/// first acknowledgement it decodes.
async fn handle_push_offer(inner: &Inner, query: zenoh::query::Query, key_id: &str) -> Result<()> {
    match push_offer_inner(inner, &query, key_id).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = query.reply_err(format!("push offer failed: {e}")).await;
            Err(e)
        }
    }
}

async fn push_offer_inner(inner: &Inner, query: &zenoh::query::Query, key_id: &str) -> Result<()> {
    let Some(push) = inner.cfg.push.clone() else {
        let _ = query.reply_err("push not enabled on this server").await;
        return Ok(());
    };
    let Some(payload) = query.payload() else {
        let _ = query.reply_err("push offer carries no manifest").await;
        return Ok(());
    };
    let manifest: Manifest = match crate::wire::decode(&payload.to_bytes()) {
        Ok(m) => m,
        Err(e) => {
            let _ = query.reply_err(format!("bad manifest: {e}")).await;
            return Ok(());
        }
    };
    if let Err(e) = manifest.validate(push.max_blob_size) {
        let _ = query.reply_err(format!("bad manifest: {e}")).await;
        return Ok(());
    }
    if manifest.id != key_id {
        let _ = query
            .reply_err(format!(
                "manifest id {:?} does not match the offer key id {key_id:?}",
                manifest.id
            ))
            .await;
        return Ok(());
    }
    let token = query.attachment().map(|a| a.to_bytes().to_vec());
    if !push.policy.allow(&manifest, token.as_deref()) {
        let _ = query.reply_err("push denied by policy").await;
        return Ok(());
    }
    let chunks = manifest.chunks()?;
    let count = chunks.count();

    // A push must never hijack a blob this server already serves. Identical
    // content is acknowledged as already complete (idempotent re-push).
    {
        let registry = inner.registry.read().await;
        if let Some(existing) = registry.get(&manifest.id) {
            if existing.manifest.root == manifest.root {
                drop(registry);
                let ack = crate::wire::encode(&Vec::<(u32, u32)>::new())?;
                query
                    .reply(query.key_expr().clone(), ack)
                    .encoding(ENC_PUSH)
                    .await
                    .map_err(BlobError::zenoh)?;
                return Ok(());
            }
            drop(registry);
            let _ = query
                .reply_err("id already registered with different content")
                .await;
            return Ok(());
        }
    }

    // Fast path under a short lock: evict stale pushes, resume an in-flight
    // one, enforce the concurrency cap.
    let evicted = {
        let mut pushes = inner.pushes.lock().await;
        let mut evicted = Vec::new();
        pushes.retain(|id, e| {
            let keep = e.last_activity.elapsed() <= push.idle_timeout;
            if !keep {
                evicted.push(id.clone());
            }
            keep
        });
        let over_cap = pushes.len() >= push.max_concurrent;
        match pushes.get_mut(&manifest.id) {
            Some(existing)
                if existing.manifest.root != manifest.root
                    || existing.manifest.total_len != manifest.total_len
                    || existing.manifest.chunk_size != manifest.chunk_size =>
            {
                drop(pushes);
                let _ = query
                    .reply_err("a conflicting push for this id is in progress")
                    .await;
                return Ok(());
            }
            Some(existing) => {
                existing.last_activity = tokio::time::Instant::now();
                let wanted: Vec<(u32, u32)> = existing
                    .state
                    .missing_ranges(count)
                    .into_iter()
                    .map(|r| (r.start, r.end))
                    .collect();
                let empty = wanted.is_empty();
                let ack = crate::wire::encode(&wanted)?;
                drop(pushes);
                cleanup_spool(&push, &evicted).await;
                if empty {
                    finalize_push(inner, &push, &manifest.id).await?;
                }
                query
                    .reply(query.key_expr().clone(), ack)
                    .encoding(ENC_PUSH)
                    .await
                    .map_err(BlobError::zenoh)?;
                return Ok(());
            }
            None if over_cap => {
                drop(pushes);
                cleanup_spool(&push, &evicted).await;
                let _ = query
                    .reply_err("too many pushes in progress; try again later")
                    .await;
                return Ok(());
            }
            None => {}
        }
        evicted
    };
    cleanup_spool(&push, &evicted).await;

    // Fresh push: spool setup happens without the lock (two racing offers for
    // one id produce identical zeroed state; the second insert defers to the
    // first).
    tokio::fs::create_dir_all(&push.spool_dir).await?;
    let part = push_part_path(&push, &manifest.id);
    // Resume a spooled push across server restarts, exactly like a download.
    let existing_len = tokio::fs::metadata(&part).await.map(|m| m.len()).ok();
    let state = match ResumeState::load(&part).await {
        Some(s) if s.matches(&manifest, count) && existing_len == Some(manifest.total_len) => s,
        _ => {
            let file = tokio::fs::File::create(&part).await?;
            file.set_len(manifest.total_len).await?;
            let fresh = ResumeState::fresh(&manifest, count);
            fresh.save_atomic(&part).await?;
            fresh
        }
    };
    zdebug!(id = %manifest.id, chunks = count, "push offer accepted");

    let wanted: Vec<(u32, u32)> = {
        let mut pushes = inner.pushes.lock().await;
        if pushes.len() >= push.max_concurrent && !pushes.contains_key(&manifest.id) {
            drop(pushes);
            let _ = query
                .reply_err("too many pushes in progress; try again later")
                .await;
            return Ok(());
        }
        let entry = pushes
            .entry(manifest.id.clone())
            .or_insert_with(|| PushEntry {
                manifest: manifest.clone(),
                chunks,
                state,
                last_activity: tokio::time::Instant::now(),
                dirty_marks: 0,
            });
        entry
            .state
            .missing_ranges(count)
            .into_iter()
            .map(|r| (r.start, r.end))
            .collect()
    };
    let empty = wanted.is_empty();
    let ack = crate::wire::encode(&wanted)?;
    // An empty or already-complete blob finalizes straight away.
    if empty {
        finalize_push(inner, &push, &manifest.id).await?;
    }
    query
        .reply(query.key_expr().clone(), ack)
        .encoding(ENC_PUSH)
        .await
        .map_err(BlobError::zenoh)?;
    Ok(())
}

/// Best-effort removal of evicted pushes' spool files.
async fn cleanup_spool(push: &PushConfig, evicted: &[BlobId]) {
    for id in evicted {
        let part = push_part_path(push, id);
        let _ = tokio::fs::remove_file(&part).await;
        ResumeState::remove(&part).await;
        zdebug!(id = %id, "evicted idle push");
    }
}

/// Handle one pushed slice: authorize, verify against the offered root, write
/// to the spool, and ack with the number of chunks still missing. The pushes
/// lock is held only for map reads/updates — verification and file I/O run
/// unlocked so concurrent pushes don't serialize.
async fn handle_push_slice(
    inner: &Inner,
    query: zenoh::query::Query,
    id: &str,
    index: u32,
) -> Result<()> {
    // Same guarantee as the offer path: no `?` may escape without the
    // uploader hearing why (see `handle_push_offer`).
    match push_slice_inner(inner, &query, id, index).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = query.reply_err(format!("push slice failed: {e}")).await;
            Err(e)
        }
    }
}

async fn push_slice_inner(
    inner: &Inner,
    query: &zenoh::query::Query,
    id: &str,
    index: u32,
) -> Result<()> {
    let Some(push) = inner.cfg.push.clone() else {
        let _ = query.reply_err("push not enabled on this server").await;
        return Ok(());
    };
    let Some(payload) = query.payload() else {
        let _ = query.reply_err("push slice carries no payload").await;
        return Ok(());
    };
    let slice = payload.to_bytes().to_vec();
    let token = query.attachment().map(|a| a.to_bytes().to_vec());

    // Snapshot what verification needs, under a short lock.
    let (manifest, chunks, already_set) = {
        let mut pushes = inner.pushes.lock().await;
        let Some(entry) = pushes.get_mut(id) else {
            drop(pushes);
            // A finalize may have raced a retried final slice: ack completion
            // if the id now serves the pushed content.
            if inner.registry.read().await.contains_key(id) {
                let ack = crate::wire::encode(&0u32)?;
                query
                    .reply(query.key_expr().clone(), ack)
                    .encoding(ENC_PUSH)
                    .await
                    .map_err(BlobError::zenoh)?;
                return Ok(());
            }
            let _ = query.reply_err("no push offer for this id").await;
            return Ok(());
        };
        entry.last_activity = tokio::time::Instant::now();
        (
            entry.manifest.clone(),
            entry.chunks,
            entry.state.is_set(index),
        )
    };
    if !push.policy.allow(&manifest, token.as_deref()) {
        let _ = query.reply_err("push denied by policy").await;
        return Ok(());
    }
    let count = chunks.count();
    if index >= count {
        let _ = query.reply_err("slice index out of range").await;
        return Ok(());
    }

    if !already_set {
        // Verify-decode against the *offered* root; a bad slice is refused.
        // Two concurrent pushes of the same slice both write the same verified
        // bytes — benign.
        let root: blake3::Hash = manifest.root.into();
        let total_len = manifest.total_len;
        let byte_range = chunks.byte_range(index);
        let mut leaves: Vec<(u64, Vec<u8>)> = Vec::new();
        let decoded = verify::decode_slice(
            &root,
            total_len,
            verify::chunk_range(byte_range),
            &slice,
            |off, data| {
                leaves.push((off, data.to_vec()));
                Ok(())
            },
        );
        if decoded.is_err() {
            let _ = query.reply_err("slice failed verification").await;
            return Ok(());
        }
        let part = push_part_path(&push, id);
        let write_part = part.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(&write_part)?;
            for (off, data) in &leaves {
                f.seek(SeekFrom::Start(*off))?;
                f.write_all(data)?;
            }
            f.sync_data()
        })
        .await??;
    }

    // Mark + batched sidecar persistence (every 16 slices or at completion;
    // the data was already fsynced above, so bits never lead the bytes).
    let (remaining, save_state) = {
        let mut pushes = inner.pushes.lock().await;
        let Some(entry) = pushes.get_mut(id) else {
            // Finalized concurrently while we were writing.
            let ack = crate::wire::encode(&0u32)?;
            query
                .reply(query.key_expr().clone(), ack)
                .encoding(ENC_PUSH)
                .await
                .map_err(BlobError::zenoh)?;
            return Ok(());
        };
        if entry.state.mark(index) {
            entry.dirty_marks += 1;
        }
        let remaining = count - entry.state.received();
        let save_state = if remaining == 0 || entry.dirty_marks >= 16 {
            entry.dirty_marks = 0;
            Some(entry.state.clone())
        } else {
            None
        };
        (remaining, save_state)
    };
    if let Some(state) = save_state {
        state.save_atomic(&push_part_path(&push, id)).await?;
    }
    let ack = crate::wire::encode(&remaining)?;
    if remaining == 0 {
        finalize_push(inner, &push, id).await?;
    }
    query
        .reply(query.key_expr().clone(), ack)
        .encoding(ENC_PUSH)
        .await
        .map_err(BlobError::zenoh)?;
    Ok(())
}

/// Complete a push: move the spool into place, compute the outboard, and
/// register the blob for serving. Never displaces an existing registration
/// (the offer refused colliding ids; this re-checks against races).
async fn finalize_push(inner: &Inner, push: &PushConfig, id: &str) -> Result<()> {
    let entry = {
        let mut pushes = inner.pushes.lock().await;
        match pushes.remove(id) {
            Some(e) => e,
            None => return Ok(()), // already finalized by a concurrent ack
        }
    };
    let part = push_part_path(push, id);
    let blob_path = push.spool_dir.join(format!("{id}.blob"));
    tokio::fs::rename(&part, &blob_path).await?;
    ResumeState::remove(&part).await;

    let mem_limit = inner.cfg.outboard_mem_limit;
    let hash_path = blob_path.clone();
    let (outboard, total_len) =
        tokio::task::spawn_blocking(move || -> std::io::Result<(OutboardStore, u64)> {
            let file = std::fs::File::open(&hash_path)?;
            let total_len = file.metadata()?.len();
            let est_outboard = total_len / verify::GROUP_SIZE * 64;
            let store = if est_outboard > mem_limit {
                let obao = {
                    let mut p = hash_path.as_os_str().to_os_string();
                    p.push(".obao4");
                    std::path::PathBuf::from(p)
                };
                OutboardStore::File(verify::compute_outboard_file(&file, total_len, &obao)?)
            } else {
                OutboardStore::Mem(verify::compute_outboard(file)?)
            };
            Ok((store, total_len))
        })
        .await??;

    // Every slice was verified against the offered root, so this cannot fail
    // unless the spool was tampered with on disk between write and finalize.
    let actual: crate::hash::Hash = outboard.root().into();
    if actual != entry.manifest.root || total_len != entry.manifest.total_len {
        let _ = tokio::fs::remove_file(&blob_path).await;
        return Err(BlobError::RootMismatch {
            expected: entry.manifest.root,
            actual,
        });
    }

    let mut registry = inner.registry.write().await;
    if let Some(existing) = registry.get(id) {
        // Raced a direct registration since the offer: the push loses.
        let same = existing.manifest.root == entry.manifest.root;
        drop(registry);
        let _ = tokio::fs::remove_file(&blob_path).await;
        if same {
            return Ok(());
        }
        return Err(BlobError::PushDenied(
            "id was registered with different content during the push".into(),
        ));
    }
    zdebug!(id = %entry.manifest.id, total_len, "push finalized and registered");
    registry.insert(
        entry.manifest.id.clone(),
        Registered {
            manifest: entry.manifest,
            chunks: entry.chunks,
            fingerprint: {
                let src = FileBlobSource::new(&blob_path);
                src.fingerprint()
            },
            source: Arc::new(FileBlobSource::new(blob_path)),
            outboard: Arc::new(outboard),
        },
    );
    Ok(())
}
