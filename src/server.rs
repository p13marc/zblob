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
use crate::manifest::{BlobSpec, Manifest, validate_id};
use crate::obs::{zdebug, zwarn};
use crate::resume::ResumeState;
use crate::verify::{self, OutboardStore, ReadAtCursor};
use crate::wire::{ENC_MANIFEST, ENC_PUSH, ENC_SLICE, encode};
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

#[derive(Clone)]
struct PushConfig {
    policy: Arc<dyn PushPolicy>,
    spool_dir: std::path::PathBuf,
    max_blob_size: u64,
}

/// An in-progress push: spooled `.part` + resume bitfield, mirroring the
/// download client's state machine on the receiving side.
struct PushEntry {
    manifest: Manifest,
    chunks: TransferChunks,
    state: ResumeState,
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
    prefix: String,
    registry: RwLock<HashMap<String, Registered>>,
    inflight: Semaphore,
    cfg: ServerConfig,
    pushes: tokio::sync::Mutex<HashMap<String, PushEntry>>,
}

/// Serves registered blobs over a Zenoh queryable at `<prefix>/**`.
#[derive(Clone)]
pub struct BlobServer {
    inner: Arc<Inner>,
}

/// Builder for a [`BlobServer`] (see [`BlobServer::builder`]).
pub struct BlobServerBuilder {
    session: Arc<zenoh::Session>,
    prefix: String,
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

    /// Accept verified pushes (uploads): `policy` authorizes each offer and
    /// slice, `spool_dir` holds in-progress `.part`s and completed blobs.
    /// Completed uploads are automatically registered and served. Pushes are
    /// **rejected unless this is configured.**
    pub fn accept_push(
        mut self,
        policy: Arc<dyn PushPolicy>,
        spool_dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        self.cfg.push = Some(PushConfig {
            policy,
            spool_dir: spool_dir.into(),
            max_blob_size: 1 << 40,
        });
        self
    }

    /// Build the server.
    pub fn build(self) -> BlobServer {
        BlobServer {
            inner: Arc::new(Inner {
                session: self.session,
                prefix: self.prefix,
                registry: RwLock::new(HashMap::new()),
                inflight: Semaphore::new(self.cfg.max_inflight),
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
        self.join
            .await
            .map_err(|e| BlobError::Protocol(format!("server task join: {e}")))?
    }
}

impl BlobServer {
    /// Start building a server for blobs under `key_prefix`.
    pub fn builder(
        session: Arc<zenoh::Session>,
        key_prefix: impl Into<String>,
    ) -> BlobServerBuilder {
        BlobServerBuilder {
            session,
            prefix: key_prefix.into(),
            cfg: ServerConfig::default(),
        }
    }

    /// Build a server with default configuration (see [`BlobServer::builder`]).
    pub fn new(session: Arc<zenoh::Session>, key_prefix: impl Into<String>) -> Self {
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
            .await
            .map_err(|e| BlobError::Protocol(format!("register task: {e}")))??;
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
            .await
            .map_err(|e| BlobError::Protocol(format!("register task: {e}")))??;
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
            id: spec.id.clone(),
            filename: spec.filename,
            total_len,
            chunk_size: spec.chunk_size,
            root: outboard.root().into(),
            created_ms: spec.created_ms,
        };
        zdebug!(id = %manifest.id, total_len, root = %manifest.root, "blob registered");
        self.inner.registry.write().await.insert(
            spec.id,
            Registered {
                manifest: manifest.clone(),
                chunks,
                source,
                outboard: Arc::new(outboard),
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
                    tokio::spawn(async move {
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
    let _permit = inner
        .inflight
        .acquire()
        .await
        .map_err(|e| BlobError::Protocol(e.to_string()))?;

    let key_str = query.key_expr().as_str().to_string();
    let Some(id) = parse_id(&inner.prefix, &key_str) else {
        return Ok(()); // not a per-blob query; ignore.
    };

    // Push protocol (upload): dispatched before the registry lookup — a blob
    // being pushed is not registered yet.
    if key_str.ends_with("/push/offer") {
        return handle_push_offer(inner, query).await;
    }
    if let Some(idx) = key_str
        .rsplit_once("/push/slice/")
        .and_then(|(head, i)| head.ends_with(&id).then(|| i.parse::<u32>().ok()).flatten())
    {
        return handle_push_slice(inner, query, &id, idx).await;
    }

    // Snapshot the registration (clone the cheap manifest + Arc the rest) so we
    // don't hold the registry lock across the stream.
    let (manifest, chunks, source, outboard) = {
        let reg = inner.registry.read().await;
        match reg.get(&id) {
            Some(r) => (
                r.manifest.clone(),
                r.chunks,
                r.source.clone(),
                r.outboard.clone(),
            ),
            None => return Ok(()), // unknown id → client times out → NotFound.
        }
    };

    // Manifest-only request: exact `.../manifest` GET.
    if key_str.ends_with("/manifest") {
        let payload = encode(&manifest)?;
        query
            .reply(manifest_key(&inner.prefix, &id), payload)
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
        tokio::task::spawn_blocking(move || source.open())
            .await
            .map_err(|e| BlobError::Protocol(format!("open task: {e}")))??
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
            .await
            .map_err(|e| BlobError::Protocol(format!("encode task: {e}")))?;
            reader = r;
            let slice = slice?;
            // A reply error means the client dropped the GET (query finalized):
            // stop promptly instead of streaming the rest into the void.
            if query
                .reply(slice_key(&inner.prefix, &id, index), slice)
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

/// Handle an upload offer: authorize, create/resume spool state, and reply
/// with the chunk ranges the server still wants.
async fn handle_push_offer(inner: &Inner, query: zenoh::query::Query) -> Result<()> {
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
    let token = query.attachment().map(|a| a.to_bytes().to_vec());
    if !push.policy.allow(&manifest, token.as_deref()) {
        let _ = query.reply_err("push denied by policy").await;
        return Ok(());
    }
    let chunks = manifest.chunks()?;
    let count = chunks.count();

    let mut pushes = inner.pushes.lock().await;
    let entry = match pushes.get(&manifest.id) {
        Some(existing) if existing.manifest.root != manifest.root => {
            let _ = query
                .reply_err("a conflicting push for this id is in progress")
                .await;
            return Ok(());
        }
        Some(existing) => existing,
        None => {
            tokio::fs::create_dir_all(&push.spool_dir).await?;
            let part = push_part_path(&push, &manifest.id);
            // Resume a spooled push across restarts, exactly like a download.
            let existing_len = tokio::fs::metadata(&part).await.map(|m| m.len()).ok();
            let state = match ResumeState::load(&part).await {
                Some(s)
                    if s.matches(&manifest, count) && existing_len == Some(manifest.total_len) =>
                {
                    s
                }
                _ => {
                    let file = tokio::fs::File::create(&part).await?;
                    file.set_len(manifest.total_len).await?;
                    let fresh = ResumeState::fresh(&manifest, count);
                    fresh.save_atomic(&part).await?;
                    fresh
                }
            };
            zdebug!(id = %manifest.id, chunks = count, "push offer accepted");
            pushes.insert(
                manifest.id.clone(),
                PushEntry {
                    manifest: manifest.clone(),
                    chunks,
                    state,
                },
            );
            pushes.get(&manifest.id).expect("just inserted")
        }
    };

    let wanted: Vec<(u32, u32)> = entry
        .state
        .missing_ranges(count)
        .into_iter()
        .map(|r| (r.start, r.end))
        .collect();
    let empty = wanted.is_empty();
    let ack = crate::wire::encode(&wanted)?;
    drop(pushes);
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

/// Handle one pushed slice: authorize, verify against the offered root, write
/// to the spool, and ack with the number of chunks still missing.
async fn handle_push_slice(
    inner: &Inner,
    query: zenoh::query::Query,
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

    let mut pushes = inner.pushes.lock().await;
    let Some(entry) = pushes.get_mut(id) else {
        let _ = query.reply_err("no push offer for this id").await;
        return Ok(());
    };
    if !push.policy.allow(&entry.manifest, token.as_deref()) {
        let _ = query.reply_err("push denied by policy").await;
        return Ok(());
    }
    let count = entry.chunks.count();
    if index >= count {
        let _ = query.reply_err("slice index out of range").await;
        return Ok(());
    }

    if !entry.state.is_set(index) {
        // Verify-decode against the *offered* root; a bad slice is refused.
        let root: blake3::Hash = entry.manifest.root.into();
        let total_len = entry.manifest.total_len;
        let byte_range = entry.chunks.byte_range(index);
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
        .await
        .map_err(|e| BlobError::Protocol(format!("push write task: {e}")))??;
        entry.state.mark(index);
        entry.state.save_atomic(&part).await?;
    }

    let remaining = count - entry.state.received();
    let ack = crate::wire::encode(&remaining)?;
    drop(pushes);
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
/// register the blob for serving.
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
        .await
        .map_err(|e| BlobError::Protocol(format!("finalize task: {e}")))??;

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

    zdebug!(id = %entry.manifest.id, total_len, "push finalized and registered");
    inner.registry.write().await.insert(
        entry.manifest.id.clone(),
        Registered {
            manifest: entry.manifest,
            chunks: entry.chunks,
            source: Arc::new(FileBlobSource::new(blob_path)),
            outboard: Arc::new(outboard),
        },
    );
    Ok(())
}
