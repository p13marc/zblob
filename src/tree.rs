//! Tier-2 — content-addressed **directory** transfer (the casync-over-Zenoh
//! model). A snapshot is a [`TreeIndex`] (a depth-first list of entries; files
//! reference their chunks by content hash) plus a content-addressed chunk store.
//!
//! The client GETs the index, computes the set of chunk hashes it is **missing**
//! (`needed − have`), fetches only those — concurrently, re-hashing each on
//! receipt — and reconstructs the tree. Because progress *is* "which hashes
//! are on disk", a dropped connection or a process restart loses nothing, and
//! identical chunks across files/versions transfer once.
//!
//! **Naming.** A snapshot's `id` is a key segment the caller chooses. Prefer
//! [`TreeIndex::keyed_by_root`] — keying a snapshot by its own root hash makes
//! the key immutable (so any holder's copy is as good as any other's, and
//! re-publishing is a no-op) and makes
//! [`DownloadRequest::by_root`](crate::DownloadRequest::by_root) pin
//! automatically. Human names ("nightly") are mutable facts; publish them
//! wherever the application keeps mutable state, pointing at the current root,
//! and resolve in two hops. A caller-chosen name is still supported for
//! deployments that want it, with the usual consequence: the key means
//! whatever its last writer said.
//!
//! Security model (v2): the index is attacker input until proven otherwise.
//! Every path is sanitized (relative, `Normal` components only), symlinks are
//! materialized **last** and their targets confined to the tree, file parents
//! are canonicalized back under the destination root before writing, and the
//! `root_hash` is a canonical postcard digest a caller can pin out of band
//! (`mtime` is excluded, so byte-identical trees hash identically).

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore};
use zenoh::qos::Priority;
use zenoh::query::ConsolidationMode;

use crate::cancel::CancelToken;
use crate::chunk::CdcParams;
use crate::client::DownloadRequest;
use crate::compress::{ChunkCompression, pack, unpack};
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::manifest::validate_id;
use crate::obs::{TransferStats, zdebug};
use crate::paths::{
    assert_parent_within, create_dir_confined, sanitize_rel_path, sanitize_symlink_target,
};
use crate::progress::{Progress, ProgressSink};
use crate::server::{ErrorCallback, FifoQueryable, ServerHandle, report_error};
use crate::store::ContentStore;
use crate::wire::{ENC_CHUNK, ENC_INDEX, WIRE_VERSION, decode, encode};
use crate::{store_key, tree_key};

/// A reference to one content-addressed chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    /// Content hash of the chunk.
    pub hash: Hash,
    /// Length of the chunk in bytes.
    pub len: u32,
}

/// One entry in a tree snapshot (paths are relative, `/`-separated, UTF-8 —
/// non-UTF-8 names are a build error, not silently mangled).
///
/// Serialized with serde's default (variant-indexed) representation; postcard
/// is not self-describing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Entry {
    /// A directory.
    Dir {
        /// Relative path.
        path: String,
        /// Unix mode bits (0 on non-unix).
        mode: u32,
        /// Modification time, Unix seconds (restored on materialization;
        /// excluded from the identity `root_hash`).
        mtime: i64,
    },
    /// A regular file, reconstructed by concatenating `chunks` in order.
    File {
        /// Relative path.
        path: String,
        /// Unix mode bits (0 on non-unix).
        mode: u32,
        /// Modification time, Unix seconds (see [`Entry::Dir::mtime`]).
        mtime: i64,
        /// Total file size in bytes (must equal the sum of chunk lengths).
        size: u64,
        /// Ordered chunk references.
        chunks: Vec<ChunkRef>,
    },
    /// A symbolic link. Materialized after all files/dirs, targets confined to
    /// the tree root.
    Symlink {
        /// Relative path.
        path: String,
        /// Link target (relative).
        target: String,
    },
    /// A hard link to an earlier [`Entry::File`] in the same snapshot.
    Hardlink {
        /// Relative path.
        path: String,
        /// Relative path of the linked-to file entry.
        target: String,
    },
}

impl Entry {
    /// The entry's relative path.
    pub fn path(&self) -> &str {
        match self {
            Entry::Dir { path, .. }
            | Entry::File { path, .. }
            | Entry::Symlink { path, .. }
            | Entry::Hardlink { path, .. } => path,
        }
    }
}

/// A directory snapshot: an ordered entry list + a digest over it (`root_hash`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeIndex {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// Snapshot id (a single key segment).
    pub id: String,
    /// Hash algorithm name (`"blake3"` in v2).
    pub algo: String,
    /// The content-defined-chunking parameters this snapshot was cut with.
    /// Producers sharing a store must share these for dedup to work.
    pub cdc: CdcParams,
    /// Depth-first entries.
    pub entries: Vec<Entry>,
    /// BLAKE3 digest over the canonical (versioned, mtime-free) postcard
    /// serialization of `entries` — the identity a caller pins out of band.
    pub root_hash: Hash,
}

/// The identity view of an [`Entry`]: what `root_hash` covers. `mtime` is
/// deliberately absent — byte-identical trees copied at different times must
/// hash identically for content addressing to mean anything.
#[derive(Serialize)]
enum IdentityEntry<'a> {
    Dir {
        path: &'a str,
        mode: u32,
    },
    File {
        path: &'a str,
        mode: u32,
        size: u64,
        chunks: &'a Vec<ChunkRef>,
    },
    Symlink {
        path: &'a str,
        target: &'a str,
    },
    Hardlink {
        path: &'a str,
        target: &'a str,
    },
}

/// Digest over the canonical serialization of an entry list (the tree root).
fn root_digest(entries: &[Entry]) -> Result<Hash> {
    let view: Vec<IdentityEntry> = entries
        .iter()
        .map(|e| match e {
            Entry::Dir { path, mode, .. } => IdentityEntry::Dir { path, mode: *mode },
            Entry::File {
                path,
                mode,
                size,
                chunks,
                ..
            } => IdentityEntry::File {
                path,
                mode: *mode,
                size: *size,
                chunks,
            },
            Entry::Symlink { path, target } => IdentityEntry::Symlink { path, target },
            Entry::Hardlink { path, target } => IdentityEntry::Hardlink { path, target },
        })
        .collect();
    // A versioned, canonical encoding — the hash preimage is defined by this
    // crate's wire format, not by an accident of a JSON library's escaping.
    let bytes = encode(&(WIRE_VERSION, Hash::ALGO, view))?;
    Ok(Hash::of(&bytes))
}

impl TreeIndex {
    /// All distinct chunk hashes referenced by file entries.
    pub fn needed_chunks(&self) -> Vec<Hash> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for e in &self.entries {
            if let Entry::File { chunks, .. } = e {
                for c in chunks {
                    if seen.insert(c.hash) {
                        out.push(c.hash);
                    }
                }
            }
        }
        out
    }

    /// Total size in bytes of all file entries (the reconstructed tree's payload).
    pub fn total_size(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| match e {
                Entry::File { size, .. } => *size,
                _ => 0,
            })
            .sum()
    }

    /// Number of file entries in the snapshot.
    pub fn file_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| matches!(e, Entry::File { .. }))
            .count()
    }

    /// Recompute the canonical root digest over `entries`.
    pub fn compute_root(&self) -> Result<Hash> {
        root_digest(&self.entries)
    }

    /// Re-key this snapshot by its own root hash, so the key it is served
    /// under *is* its identity (`<tree_prefix>/<root>`).
    ///
    /// This is the content-addressed shape a keyspace convention wants for
    /// bulk data, and it is what makes a tree key behave like a chunk key:
    /// immutable, so any holder's copy is as good as any other's, so
    /// re-publishing is a no-op and a router storage's last-writer-wins
    /// reconciliation cannot lose anything. It also removes
    /// trust-on-first-use for free — a consumer that asks for a root already
    /// stated the identity it demands, so
    /// [`DownloadRequest::by_root`](crate::DownloadRequest::by_root) pins
    /// automatically.
    ///
    /// The trade is that a *name* ("nightly") is no longer a key here. Names
    /// are mutable facts and belong wherever the application publishes
    /// mutable state — a small record pointing at the current root. The
    /// consumer then resolves in two hops: name → root, then fetch the root.
    ///
    /// The id is not part of the root digest, so re-keying never invalidates
    /// the index.
    pub fn keyed_by_root(mut self) -> Self {
        self.id = self.root_hash.to_string();
        self
    }

    /// Whether this index is keyed by its own root (see
    /// [`keyed_by_root`](Self::keyed_by_root)).
    pub fn is_content_addressed(&self) -> bool {
        self.id == self.root_hash.to_string()
    }

    /// Validate an index received from the network: schema version, algorithm,
    /// CDC parameters, id, every entry path/target (traversal-safe), file
    /// size↔chunk consistency, and that `root_hash` matches the entries.
    pub fn validate(&self) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        if self.algo != Hash::ALGO {
            return Err(BlobError::Protocol(format!(
                "unsupported algo: {}",
                self.algo
            )));
        }
        validate_id(&self.id)?;
        self.cdc.validate()?;
        for e in &self.entries {
            let rel = sanitize_rel_path(e.path())?;
            match e {
                Entry::File { size, chunks, .. } => {
                    let sum: u64 = chunks.iter().map(|c| c.len as u64).sum();
                    if sum != *size {
                        return Err(BlobError::InvalidManifest(format!(
                            "file {:?}: size {size} != chunk sum {sum}",
                            e.path()
                        )));
                    }
                    // Each declared length is an allocation the receiver (and
                    // the zero-seed synthesizer) will perform: bound it by the
                    // declared CDC maximum, which validate() capped above.
                    if let Some(bad) = chunks.iter().find(|c| c.len == 0 || c.len > self.cdc.max) {
                        return Err(BlobError::InvalidManifest(format!(
                            "file {:?}: chunk len {} outside 1..={}",
                            e.path(),
                            bad.len,
                            self.cdc.max
                        )));
                    }
                }
                Entry::Symlink { target, .. } => {
                    sanitize_symlink_target(&rel, target)?;
                }
                Entry::Hardlink { target, .. } => {
                    sanitize_rel_path(target)?;
                }
                Entry::Dir { .. } => {}
            }
        }
        let actual = self.compute_root()?;
        if actual != self.root_hash {
            return Err(BlobError::RootMismatch {
                expected: self.root_hash,
                actual,
            });
        }
        Ok(())
    }
}

/// Build a [`TreeIndex`] for the directory at `root`, streaming every file
/// through the content-defined chunker and `put`ting each new chunk into
/// `store`. Memory stays O(max chunk size) regardless of tree size.
///
/// Synchronous (run it in `spawn_blocking`). Walks depth-first in a
/// deterministic (sorted) order so the snapshot is reproducible. Policy:
/// non-UTF-8 names and non-regular files (FIFOs, sockets, devices) are
/// **errors**, not silent drops; hard links are detected (unix) and recorded
/// as [`Entry::Hardlink`].
pub fn build_tree(
    root: &Path,
    id: impl Into<String>,
    cdc: &CdcParams,
    store: &dyn ContentStore,
) -> Result<TreeIndex> {
    cdc.validate()?;
    let id = id.into();
    validate_id(&id)?;
    let mut entries = Vec::new();
    walk(root, cdc, store, &mut entries)?;
    let root_hash = root_digest(&entries)?;
    Ok(TreeIndex {
        version: WIRE_VERSION,
        id,
        algo: Hash::ALGO.to_string(),
        cdc: *cdc,
        entries,
        root_hash,
    })
}

fn rel_path_of(root: &Path, path: &Path) -> Result<String> {
    let rel = path.strip_prefix(root).unwrap_or(path);
    let mut out = String::new();
    for comp in rel.components() {
        let s = comp
            .as_os_str()
            .to_str()
            .ok_or_else(|| BlobError::Protocol(format!("non-UTF-8 file name in tree: {rel:?}")))?;
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(s);
    }
    Ok(out)
}

#[cfg(unix)]
fn mode_of(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode()
}
#[cfg(not(unix))]
fn mode_of(_meta: &std::fs::Metadata) -> u32 {
    0
}

fn mtime_of(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Iterative depth-first walk (explicit stack — a deep tree must not blow the
/// call stack), sorted per directory for reproducibility.
fn walk(
    root: &Path,
    cdc: &CdcParams,
    store: &dyn ContentStore,
    entries: &mut Vec<Entry>,
) -> Result<()> {
    fn read_dir_sorted(dir: &Path) -> std::io::Result<std::vec::IntoIter<std::fs::DirEntry>> {
        let mut items: Vec<_> = std::fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
        items.sort_by_key(|e| e.file_name());
        Ok(items.into_iter())
    }

    // (dev, ino) → first path seen, for hard-link detection.
    let mut inodes: HashMap<(u64, u64), String> = HashMap::new();
    let mut stack = vec![read_dir_sorted(root)?];
    while let Some(top) = stack.last_mut() {
        let Some(item) = top.next() else {
            stack.pop();
            continue;
        };
        let path = item.path();
        let meta = std::fs::symlink_metadata(&path)?;
        let rel = rel_path_of(root, &path)?;
        let ftype = meta.file_type();
        if ftype.is_symlink() {
            let target_os = std::fs::read_link(&path)?;
            let target = target_os
                .to_str()
                .ok_or_else(|| BlobError::Protocol(format!("non-UTF-8 symlink target at {rel:?}")))?
                .to_string();
            entries.push(Entry::Symlink { path: rel, target });
        } else if ftype.is_dir() {
            entries.push(Entry::Dir {
                path: rel,
                mode: mode_of(&meta),
                mtime: mtime_of(&meta),
            });
            stack.push(read_dir_sorted(&path)?);
        } else if ftype.is_file() {
            if let Some(first) = hardlink_target(&meta, &rel, &mut inodes) {
                entries.push(Entry::Hardlink {
                    path: rel,
                    target: first,
                });
                continue;
            }
            let file = std::fs::File::open(&path)?;
            let mut refs = Vec::new();
            let mut size: u64 = 0;
            for chunk in cdc.chunk_reader(file) {
                let bytes = chunk?;
                let hash = Hash::of(&bytes);
                size += bytes.len() as u64;
                refs.push(ChunkRef {
                    hash,
                    len: bytes.len() as u32,
                });
                if !store.has(&hash) {
                    store.put(&hash, &bytes)?;
                }
            }
            entries.push(Entry::File {
                path: rel,
                mode: mode_of(&meta),
                mtime: mtime_of(&meta),
                size,
                chunks: refs,
            });
        } else {
            // FIFO / socket / device: refusing loudly beats silently producing
            // a snapshot that claims to be the tree but isn't.
            return Err(BlobError::Protocol(format!(
                "unsupported file type at {rel:?} (fifo/socket/device)"
            )));
        }
    }
    Ok(())
}

/// If this file shares an inode with an earlier file, return that path.
#[cfg(unix)]
fn hardlink_target(
    meta: &std::fs::Metadata,
    rel: &str,
    inodes: &mut HashMap<(u64, u64), String>,
) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    if meta.nlink() > 1 {
        let key = (meta.dev(), meta.ino());
        match inodes.get(&key) {
            Some(first) => return Some(first.clone()),
            None => {
                inodes.insert(key, rel.to_string());
            }
        }
    }
    None
}
#[cfg(not(unix))]
fn hardlink_target(
    _meta: &std::fs::Metadata,
    _rel: &str,
    _inodes: &mut HashMap<(u64, u64), String>,
) -> Option<String> {
    None
}

/// Serves a tree snapshot: an index queryable + a content-addressed chunk
/// queryable, both backed by a [`ContentStore`]. Each query is served on its
/// own task under an in-flight semaphore, so one slow client cannot stall the
/// rest.
#[derive(Clone)]
pub struct TreeServer {
    inner: Arc<TreeInner>,
}

struct TreeInner {
    session: Arc<zenoh::Session>,
    store_prefix: String,
    tree_prefix: String,
    store: Arc<dyn ContentStore>,
    index: tokio::sync::RwLock<std::collections::HashMap<String, TreeIndex>>,
    inflight: Semaphore,
    compression: ChunkCompression,
    on_error: Option<ErrorCallback>,
}

/// Builder for a [`TreeServer`] (see [`TreeServer::builder`]).
pub struct TreeServerBuilder {
    session: Arc<zenoh::Session>,
    store_prefix: String,
    tree_prefix: String,
    store: Arc<dyn ContentStore>,
    max_inflight: usize,
    compression: ChunkCompression,
    on_error: Option<ErrorCallback>,
}

impl TreeServerBuilder {
    /// Max concurrent in-flight queries served at once (default 8).
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.max_inflight = n.max(1);
        self
    }

    /// Compression for chunk replies (default none; requires the `zstd`
    /// feature for [`ChunkCompression::Zstd`]).
    pub fn compression(mut self, c: ChunkCompression) -> Self {
        self.compression = c;
        self
    }

    /// Invoke `cb` with every serve error (default: a `tracing` warn event
    /// with the `tracing` feature, else stderr in debug builds only).
    pub fn on_error(mut self, cb: ErrorCallback) -> Self {
        self.on_error = Some(cb);
        self
    }

    /// Build the server.
    pub fn build(self) -> TreeServer {
        TreeServer {
            inner: Arc::new(TreeInner {
                session: self.session,
                store_prefix: self.store_prefix,
                tree_prefix: self.tree_prefix,
                store: self.store,
                index: tokio::sync::RwLock::new(std::collections::HashMap::new()),
                inflight: Semaphore::new(self.max_inflight),
                compression: self.compression,
                on_error: self.on_error,
            }),
        }
    }
}

impl TreeServer {
    /// Start building a server. `store_prefix` serves `<prefix>/<algo>/<hash>`;
    /// `tree_prefix` serves `<prefix>/<id>`.
    pub fn builder(
        session: Arc<zenoh::Session>,
        store_prefix: impl Into<String>,
        tree_prefix: impl Into<String>,
        store: Arc<dyn ContentStore>,
    ) -> TreeServerBuilder {
        TreeServerBuilder {
            session,
            store_prefix: store_prefix.into(),
            tree_prefix: tree_prefix.into(),
            store,
            max_inflight: 8,
            compression: ChunkCompression::default(),
            on_error: None,
        }
    }

    /// Build a server with default configuration.
    pub fn new(
        session: Arc<zenoh::Session>,
        store_prefix: impl Into<String>,
        tree_prefix: impl Into<String>,
        store: Arc<dyn ContentStore>,
    ) -> Self {
        Self::builder(session, store_prefix, tree_prefix, store).build()
    }

    /// Register a snapshot index (its chunks must already be in the store).
    pub async fn register(&self, index: TreeIndex) {
        self.inner
            .index
            .write()
            .await
            .insert(index.id.clone(), index);
    }

    /// Drop a previously-registered snapshot index by id (e.g. on TTL expiry). The
    /// chunks themselves are owned by the [`ContentStore`] and freed separately.
    pub async fn unregister(&self, id: &str) {
        self.inner.index.write().await.remove(id);
    }

    async fn declare(&self) -> Result<(FifoQueryable, FifoQueryable)> {
        crate::paths::validate_serve_prefix(&self.inner.store_prefix)?;
        crate::paths::validate_serve_prefix(&self.inner.tree_prefix)?;
        let store_q = self
            .inner
            .session
            .declare_queryable(format!("{}/**", self.inner.store_prefix))
            .await
            .map_err(BlobError::zenoh)?;
        let tree_q = self
            .inner
            .session
            .declare_queryable(format!("{}/**", self.inner.tree_prefix))
            .await
            .map_err(BlobError::zenoh)?;
        Ok((store_q, tree_q))
    }

    /// Declare both queryables, spawn the serve loop, and return a stop
    /// handle. Queryables are live when this returns.
    pub async fn spawn(self) -> Result<ServerHandle> {
        let (store_q, tree_q) = self.declare().await?;
        let stop = Arc::new(Notify::new());
        let stop2 = stop.clone();
        Ok(ServerHandle::new(
            tokio::spawn(self.serve_loop(store_q, tree_q, stop2)),
            stop,
        ))
    }

    /// Declare both queryables and serve until the session closes.
    pub async fn run(self) -> Result<()> {
        let (store_q, tree_q) = self.declare().await?;
        self.serve_loop(store_q, tree_q, Arc::new(Notify::new()))
            .await
    }

    async fn serve_loop(
        self,
        store_q: FifoQueryable,
        tree_q: FifoQueryable,
        stop: Arc<Notify>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = stop.notified() => break,
                q = store_q.recv_async() => {
                    let Ok(query) = q else { break };
                    let inner = self.inner.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_chunk_query(&inner, query).await {
                            report_error(&inner.on_error, &e);
                        }
                    });
                }
                q = tree_q.recv_async() => {
                    let Ok(query) = q else { break };
                    let inner = self.inner.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_index_query(&inner, query).await {
                            report_error(&inner.on_error, &e);
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

async fn serve_chunk_query(inner: &TreeInner, query: zenoh::query::Query) -> Result<()> {
    let _permit = inner
        .inflight
        .acquire()
        .await
        .map_err(|e| BlobError::Protocol(e.to_string()))?;
    let key = query.key_expr().as_str();
    let Some(hash) = parse_store_key(&inner.store_prefix, key) else {
        return Ok(()); // not a chunk key (or foreign algo); ignore.
    };
    let store = inner.store.clone();
    let compression = inner.compression;
    let packed = tokio::task::spawn_blocking(move || {
        store.get(&hash).map(|bytes| pack(&bytes, compression))
    })
    .await
    .map_err(|e| BlobError::Protocol(format!("store get task: {e}")))?;
    if let Some(packed) = packed {
        query
            .reply(query.key_expr().clone(), packed?)
            .encoding(ENC_CHUNK)
            .await
            .map_err(BlobError::zenoh)?;
    }
    Ok(())
}

async fn serve_index_query(inner: &TreeInner, query: zenoh::query::Query) -> Result<()> {
    let _permit = inner
        .inflight
        .acquire()
        .await
        .map_err(|e| BlobError::Protocol(e.to_string()))?;
    let key = query.key_expr().as_str().to_string();
    let Some(id) = key.strip_prefix(&format!("{}/", inner.tree_prefix)) else {
        return Ok(());
    };
    let Some(index) = inner.index.read().await.get(id).cloned() else {
        return Ok(()); // unknown id → client times out → NotFound.
    };
    let payload = encode(&index)?;
    query
        .reply(query.key_expr().clone(), payload)
        .encoding(ENC_INDEX)
        .await
        .map_err(BlobError::zenoh)?;
    Ok(())
}

/// Parse the chunk hash from a `<store_prefix>/<algo>/<hex>` key; only the
/// crate's algorithm is served.
fn parse_store_key(store_prefix: &str, key: &str) -> Option<Hash> {
    let rest = key.strip_prefix(store_prefix)?.strip_prefix('/')?;
    let (algo, hex) = rest.split_once('/')?;
    if algo != Hash::ALGO {
        return None;
    }
    hex.parse().ok()
}

/// Downloads a tree snapshot. Stateless server; persistent client (the store).
pub struct TreeClient {
    session: Arc<zenoh::Session>,
    store_prefix: String,
    tree_prefix: String,
    cfg: TreeClientConfig,
}

#[derive(Debug, Clone)]
struct TreeClientConfig {
    query_timeout: Duration,
    fetch_concurrency: usize,
    max_index_bytes: usize,
    priority: Priority,
}

impl Default for TreeClientConfig {
    fn default() -> Self {
        TreeClientConfig {
            query_timeout: Duration::from_secs(30),
            fetch_concurrency: 16,
            max_index_bytes: 64 * 1024 * 1024,
            // Bulk transfer yields — see `BlobClientBuilder::priority`.
            priority: Priority::DataLow,
        }
    }
}

/// Builder for a [`TreeClient`] (see [`TreeClient::builder`]).
pub struct TreeClientBuilder {
    session: Arc<zenoh::Session>,
    store_prefix: String,
    tree_prefix: String,
    cfg: TreeClientConfig,
}

impl TreeClientBuilder {
    /// Per-query timeout (default 30 s).
    pub fn query_timeout(mut self, t: Duration) -> Self {
        self.cfg.query_timeout = t;
        self
    }

    /// Concurrent chunk fetches (default 16). A serial fetch pays one full
    /// round trip per chunk — tens of thousands of RTTs on a large tree.
    pub fn fetch_concurrency(mut self, n: usize) -> Self {
        self.cfg.fetch_concurrency = n.max(1);
        self
    }

    /// Largest index payload accepted, in bytes (default 64 MiB) — the
    /// allocation bound against a hostile index reply.
    pub fn max_index_bytes(mut self, n: usize) -> Self {
        self.cfg.max_index_bytes = n;
        self
    }

    /// Zenoh priority for every query this client issues
    /// (default [`Priority::DataLow`]); see
    /// [`BlobClientBuilder::priority`](crate::BlobClientBuilder::priority) for
    /// why bulk transfers must yield.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.cfg.priority = priority;
        self
    }

    /// Build the client.
    pub fn build(self) -> TreeClient {
        TreeClient {
            session: self.session,
            store_prefix: self.store_prefix,
            tree_prefix: self.tree_prefix,
            cfg: self.cfg,
        }
    }
}

impl TreeClient {
    /// Start building a client matching a [`TreeServer`]'s prefixes.
    pub fn builder(
        session: Arc<zenoh::Session>,
        store_prefix: impl Into<String>,
        tree_prefix: impl Into<String>,
    ) -> TreeClientBuilder {
        TreeClientBuilder {
            session,
            store_prefix: store_prefix.into(),
            tree_prefix: tree_prefix.into(),
            cfg: TreeClientConfig::default(),
        }
    }

    /// Build a client with default configuration.
    pub fn new(
        session: Arc<zenoh::Session>,
        store_prefix: impl Into<String>,
        tree_prefix: impl Into<String>,
    ) -> Self {
        Self::builder(session, store_prefix, tree_prefix).build()
    }

    /// Fetch and fully validate the snapshot index `id` (schema, paths,
    /// size↔chunk consistency, root recomputation).
    pub async fn fetch_index(&self, id: &str) -> Result<TreeIndex> {
        self.fetch_index_matching(id, None).await
    }

    /// Pin-aware fetch: when `expected_root` is set, replies with a different
    /// root are skipped like any other unacceptable reply, so one wrong-root
    /// responder cannot mask an honest replica.
    async fn fetch_index_matching(
        &self,
        id: &str,
        expected_root: Option<Hash>,
    ) -> Result<TreeIndex> {
        crate::paths::validate_query_prefix(&self.store_prefix)?;
        crate::paths::validate_query_prefix(&self.tree_prefix)?;
        validate_id(id)?;
        let key = tree_key(&self.tree_prefix, id);
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
            if sample.encoding().to_string() != ENC_INDEX {
                continue;
            }
            // A bad reply from one responder is skipped, not fatal — a
            // hostile or stale replier must not deny what an honest replica
            // still answers. The first rejection is kept for diagnostics.
            let payload = sample.payload().to_bytes();
            let verdict = if payload.len() > self.cfg.max_index_bytes {
                Err(BlobError::InvalidManifest(format!(
                    "index payload {} exceeds the {} byte limit",
                    payload.len(),
                    self.cfg.max_index_bytes
                )))
            } else {
                decode::<TreeIndex>(&payload)
                    .and_then(|index| index.validate().map(|()| index))
                    .and_then(|index| {
                        if index.id != id {
                            return Err(BlobError::Protocol(format!(
                                "index id {:?} does not match requested {id:?}",
                                index.id
                            )));
                        }
                        if let Some(expected) = expected_root
                            && index.root_hash != expected
                        {
                            return Err(BlobError::RootMismatch {
                                expected,
                                actual: index.root_hash,
                            });
                        }
                        Ok(index)
                    })
            };
            match verdict {
                Ok(index) => return Ok(index),
                Err(e) => {
                    rejected.get_or_insert(e);
                }
            }
        }
        Err(rejected.unwrap_or_else(|| BlobError::NotFound(id.to_string())))
    }

    /// Download snapshot `req.id` into `dest_root`: fetch only the chunks
    /// missing from `store` (concurrently, re-hashing each on receipt),
    /// reconstruct the tree safely, and restore modes + mtimes.
    ///
    /// Pin `req.expected_root` whenever known — without it the index's
    /// `root_hash` only proves internal consistency, not *which* tree you get.
    /// Because progress *is* "which hashes are on disk", a cancelled or failed
    /// transfer leaves `store` populated with whatever it fetched, so calling
    /// again resumes — across reconnect and process restart — for free.
    ///
    /// Materialization is **in place and not atomic** (the rsync/casync
    /// model): entries are written into `dest_root` directly so unchanged
    /// files need no work, but an error mid-materialization leaves a mix of
    /// old and new entries. Re-run the same call to converge — every chunk is
    /// already in `store`, so a re-run only redoes local writes. Callers who
    /// need an atomic cut-over should materialize into a fresh directory and
    /// swap it in themselves.
    pub async fn download_tree(
        &self,
        req: &DownloadRequest,
        dest_root: &Path,
        store: &Arc<dyn ContentStore>,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
    ) -> Result<TransferStats> {
        let started_at = tokio::time::Instant::now();
        let index = self
            .fetch_index_matching(&req.id, req.expected_root)
            .await?;

        let needed = index.needed_chunks();
        let total = needed.len() as u32;
        sink.emit(Progress::Started {
            total_len: index.total_size(),
            chunk_count: total,
        });

        zdebug!(id = %index.id, chunks = total, "tree download start");
        let mut stats = self
            .fetch_missing(&needed, store, sink, cancel, total)
            .await?;

        // Materialize (all blocking fs work on the blocking pool).
        sink.emit(Progress::Verifying);
        let entries = index.entries.clone();
        let dest = dest_root.to_path_buf();
        let store2 = store.clone();
        tokio::task::spawn_blocking(move || reconstruct_tree(&dest, &entries, &*store2))
            .await
            .map_err(|e| BlobError::Protocol(format!("reconstruct task: {e}")))??;

        sink.emit(Progress::Completed {
            path: dest_root.to_path_buf(),
        });
        stats.elapsed = started_at.elapsed();
        zdebug!(id = %index.id, fetched = stats.chunks_fetched, "tree download complete");
        Ok(stats)
    }

    /// Resolve every needed hash: count present ones immediately, fetch the
    /// rest with bounded concurrency. `received` counts hashes *resolved*
    /// (present or fetched), so a resume reports its true position at once.
    async fn fetch_missing(
        &self,
        needed: &[Hash],
        store: &Arc<dyn ContentStore>,
        sink: &dyn ProgressSink,
        cancel: &CancelToken,
        total: u32,
    ) -> Result<TransferStats> {
        let mut stats = TransferStats::default();
        let mut received: u32 = 0;
        let mut bytes_received: u64 = 0;
        // One blocking pass decides present vs missing (DirStore's `has` is a
        // syscall per hash — tens of thousands of them do not belong on an
        // async worker thread one by one).
        let presence: Vec<bool> = {
            let store = store.clone();
            let needed = needed.to_vec();
            tokio::task::spawn_blocking(move || {
                needed.iter().map(|h| store.has(h)).collect::<Vec<_>>()
            })
            .await
            .map_err(|e| BlobError::Protocol(format!("presence task: {e}")))?
        };
        let mut iter = needed
            .iter()
            .copied()
            .zip(presence)
            .enumerate()
            .map(|(i, (h, present))| (i, h, present));
        let mut join: tokio::task::JoinSet<Result<(usize, u64, u32)>> = tokio::task::JoinSet::new();

        loop {
            // Keep the pipeline full.
            while join.len() < self.cfg.fetch_concurrency {
                let Some((i, hash, present)) = iter.next() else {
                    break;
                };
                if present {
                    received += 1;
                    stats.chunks_resumed += 1;
                    sink.emit(Progress::Chunk {
                        index: i as u32,
                        received,
                        total,
                        bytes_received,
                    });
                    continue;
                }
                let session = self.session.clone();
                let key = store_key(&self.store_prefix, Hash::ALGO, &hash);
                let timeout = self.cfg.query_timeout;
                let priority = self.cfg.priority;
                let store = store.clone();
                join.spawn(async move {
                    let (bytes, rejected) =
                        fetch_one_chunk(&session, &key, &hash, timeout, priority).await?;
                    let len = bytes.len() as u64;
                    let put_store = store.clone();
                    tokio::task::spawn_blocking(move || put_store.put(&hash, &bytes))
                        .await
                        .map_err(|e| BlobError::Protocol(format!("store put task: {e}")))??;
                    Ok((i, len, rejected))
                });
            }
            if join.is_empty() {
                break;
            }
            if cancel.is_cancelled() {
                join.abort_all();
                sink.emit(Progress::Cancelled { received, total });
                return Err(BlobError::Cancelled { received, total });
            }
            // Bounded wait so cancellation stays responsive.
            match tokio::time::timeout(Duration::from_millis(200), join.join_next()).await {
                Err(_) => continue, // timeout tick → re-check cancel
                Ok(None) => break,
                Ok(Some(res)) => {
                    let (i, len, rejected) =
                        res.map_err(|e| BlobError::Protocol(format!("fetch task: {e}")))??;
                    received += 1;
                    bytes_received += len;
                    stats.chunks_fetched += 1;
                    stats.bytes_fetched += len;
                    stats.rejected += rejected;
                    sink.emit(Progress::Chunk {
                        index: i as u32,
                        received,
                        total,
                        bytes_received,
                    });
                }
            }
        }
        Ok(stats)
    }
}

/// GET one content-addressed chunk and verify it by re-hashing — corruption
/// and substitution are impossible past this point.
async fn fetch_one_chunk(
    session: &zenoh::Session,
    key: &str,
    hash: &Hash,
    timeout: Duration,
    priority: Priority,
) -> Result<(Vec<u8>, u32)> {
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
        let Ok(bytes) = unpack(&sample.payload().to_bytes()) else {
            rejected += 1;
            continue; // malformed frame; wait for a good replier.
        };
        if Hash::of(&bytes) != *hash {
            rejected += 1;
            continue; // hostile or corrupt replier; wait for a good one.
        }
        return Ok((bytes, rejected));
    }
    Err(BlobError::NotFound(hash.to_string()))
}

/// Materialize `entries` under `dest_root`, defensively:
/// dirs → files/hardlinks → symlinks (last), then dir mtimes. Every path is
/// sanitized and every write's parent is canonicalized back under the root.
fn reconstruct_tree(dest_root: &Path, entries: &[Entry], store: &dyn ContentStore) -> Result<()> {
    std::fs::create_dir_all(dest_root)?;
    let root = dest_root.canonicalize()?;

    // Pass 1: directories (DFS order → parents first). Creation is
    // component-wise and refuses to traverse pre-existing symlinks — the
    // prevention counterpart of the canonicalization checks below. Modes are
    // NOT applied yet — a read-only directory would break writing its own
    // children; like tar/casync, directory permissions land last.
    for e in entries {
        if let Entry::Dir { path, .. } = e {
            create_dir_confined(&root, &sanitize_rel_path(path)?)?;
        }
    }

    // Pass 2: files, then hard links to them (entry order preserves targets).
    for e in entries {
        match e {
            Entry::File {
                path,
                mode,
                mtime,
                size,
                chunks,
            } => {
                let rel = sanitize_rel_path(path)?;
                if let Some(parent) = rel.parent() {
                    create_dir_confined(&root, parent)?;
                }
                let p = root.join(&rel);
                assert_parent_within(&root, &p)?;
                remove_existing(&p);
                let mut f = std::fs::File::create(&p)?;
                let mut written: u64 = 0;
                for c in chunks {
                    let bytes = store
                        .get(&c.hash)
                        .ok_or_else(|| BlobError::NotFound(c.hash.to_string()))?;
                    if bytes.len() as u32 != c.len {
                        return Err(BlobError::HashMismatch);
                    }
                    f.write_all(&bytes)?;
                    written += bytes.len() as u64;
                }
                if written != *size {
                    return Err(BlobError::Protocol(format!(
                        "file {path:?}: wrote {written} of declared {size} bytes"
                    )));
                }
                set_mode(&p, *mode);
                set_mtime(&f, *mtime);
            }
            Entry::Hardlink { path, target } => {
                let rel = sanitize_rel_path(path)?;
                if let Some(parent) = rel.parent() {
                    create_dir_confined(&root, parent)?;
                }
                let p = root.join(&rel);
                let t = root.join(sanitize_rel_path(target)?);
                assert_parent_within(&root, &p)?;
                // The *target* must also really live under the root:
                // `link(2)` resolves symlinks in its source path, so a
                // pre-existing outward-pointing symlink inside the dest could
                // otherwise be laundered into a hard link to outside data.
                let canon_t = t.canonicalize().map_err(|e| {
                    BlobError::Protocol(format!(
                        "hardlink target {target:?} is not materialized: {e}"
                    ))
                })?;
                if !canon_t.starts_with(&root) || !canon_t.is_file() {
                    return Err(BlobError::Protocol(format!(
                        "hardlink target {target:?} resolves outside the destination root"
                    )));
                }
                remove_existing(&p);
                std::fs::hard_link(&canon_t, &p)?;
            }
            _ => {}
        }
    }

    // Pass 3: symlinks last — nothing can be written *through* them.
    for e in entries {
        if let Entry::Symlink { path, target } = e {
            let rel = sanitize_rel_path(path)?;
            sanitize_symlink_target(&rel, target)?;
            if let Some(parent) = rel.parent() {
                create_dir_confined(&root, parent)?;
            }
            let p = root.join(&rel);
            assert_parent_within(&root, &p)?;
            remove_existing(&p);
            symlink(target, &p)?;
        }
    }

    // Pass 4: directory mtimes and modes, children-first (deepest last in
    // DFS order → reverse), so restoring a read-only or dated directory never
    // interferes with materializing its contents.
    for e in entries.iter().rev() {
        if let Entry::Dir { path, mode, mtime } = e
            && let Ok(rel) = sanitize_rel_path(path)
        {
            let p = root.join(rel);
            if *mtime > 0
                && let Ok(f) = std::fs::File::open(&p)
            {
                set_mtime(&f, *mtime);
            }
            set_mode(&p, *mode);
        }
    }
    Ok(())
}

/// Remove whatever currently sits at `p` (file, symlink, or directory) so a
/// fresh entry can take its place. Best-effort.
fn remove_existing(p: &Path) {
    if let Ok(meta) = std::fs::symlink_metadata(p) {
        if meta.is_dir() {
            let _ = std::fs::remove_dir_all(p);
        } else {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    if mode != 0 {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
    }
}
#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) {}

/// Restore a recorded mtime (best-effort; 0 means "unknown").
fn set_mtime(f: &std::fs::File, mtime: i64) {
    if mtime > 0 {
        let t = std::time::UNIX_EPOCH + Duration::from_secs(mtime as u64);
        let _ = f.set_modified(t);
    }
}

#[cfg(unix)]
fn symlink(target: &str, path: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, path)
}
#[cfg(not(unix))]
fn symlink(_target: &str, _path: &Path) -> std::io::Result<()> {
    // Symlink materialization is unsupported off unix: error loudly instead of
    // silently omitting entries from a "successful" download (v1's M6).
    Err(std::io::Error::other(
        "symlink entries are not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn build_dedups_identical_chunks() {
        let dir = tempfile::tempdir().unwrap();
        // Two files with identical content → one shared chunk.
        std::fs::write(dir.path().join("a.txt"), b"same").unwrap();
        std::fs::write(dir.path().join("b.txt"), b"same").unwrap();
        let store = MemoryStore::new();
        let index = build_tree(dir.path(), "snap", &CdcParams::default(), &store).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(index.needed_chunks().len(), 1);
        index.validate().unwrap();
        assert_eq!(index.file_count(), 2);
    }

    #[test]
    fn root_hash_ignores_mtime() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.txt"), b"content").unwrap();
        let store = MemoryStore::new();
        let a = build_tree(dir.path(), "snap", &CdcParams::default(), &store).unwrap();
        // Touch the file (bump mtime, same bytes) → identical root.
        let file = std::fs::File::options()
            .append(true)
            .open(dir.path().join("f.txt"))
            .unwrap();
        file.set_modified(std::time::SystemTime::now() + Duration::from_secs(1000))
            .unwrap();
        let b = build_tree(dir.path(), "snap", &CdcParams::default(), &store).unwrap();
        assert_eq!(a.root_hash, b.root_hash, "mtime must not change identity");
        // But the bytes do change it.
        std::fs::write(dir.path().join("f.txt"), b"different").unwrap();
        let c = build_tree(dir.path(), "snap", &CdcParams::default(), &store).unwrap();
        assert_ne!(a.root_hash, c.root_hash);
    }

    #[test]
    fn traversal_paths_rejected_by_validate() {
        let evil_paths = ["../evil", "/etc/passwd", "a/../../b"];
        for evil in evil_paths {
            let entries = vec![Entry::File {
                path: evil.into(),
                mode: 0,
                mtime: 0,
                size: 0,
                chunks: vec![],
            }];
            let root_hash = root_digest(&entries).unwrap();
            let index = TreeIndex {
                version: WIRE_VERSION,
                id: "x".into(),
                algo: Hash::ALGO.into(),
                cdc: CdcParams::default(),
                entries,
                root_hash,
            };
            assert!(index.validate().is_err(), "must reject path {evil:?}");
        }
    }

    #[test]
    fn oversized_chunk_len_rejected() {
        // A declared length is an allocation the receiver performs: lens over
        // the CDC maximum (or zero) must fail validation even when the size
        // arithmetic is consistent.
        for bad_len in [0u32, CdcParams::default().max + 1, u32::MAX] {
            let entries = vec![Entry::File {
                path: "f".into(),
                mode: 0,
                mtime: 0,
                size: bad_len as u64,
                chunks: vec![ChunkRef {
                    hash: Hash::of(b"x"),
                    len: bad_len,
                }],
            }];
            let root_hash = root_digest(&entries).unwrap();
            let index = TreeIndex {
                version: WIRE_VERSION,
                id: "x".into(),
                algo: Hash::ALGO.into(),
                cdc: CdcParams::default(),
                entries,
                root_hash,
            };
            assert!(index.validate().is_err(), "len {bad_len} must be rejected");
        }
    }

    #[test]
    fn size_chunk_mismatch_rejected() {
        let entries = vec![Entry::File {
            path: "f".into(),
            mode: 0,
            mtime: 0,
            size: 100, // but no chunks
            chunks: vec![],
        }];
        let root_hash = root_digest(&entries).unwrap();
        let index = TreeIndex {
            version: WIRE_VERSION,
            id: "x".into(),
            algo: Hash::ALGO.into(),
            cdc: CdcParams::default(),
            entries,
            root_hash,
        };
        assert!(index.validate().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn hardlinks_detected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("orig.bin"), b"shared bytes").unwrap();
        std::fs::hard_link(dir.path().join("orig.bin"), dir.path().join("link.bin")).unwrap();
        let store = MemoryStore::new();
        let index = build_tree(dir.path(), "snap", &CdcParams::default(), &store).unwrap();
        let hardlinks: Vec<_> = index
            .entries
            .iter()
            .filter(|e| matches!(e, Entry::Hardlink { .. }))
            .collect();
        assert_eq!(hardlinks.len(), 1);
        // One File + one Hardlink, single stored chunk.
        assert_eq!(index.file_count(), 1);
        assert_eq!(store.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn fifo_is_an_error_not_a_silent_drop() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        // mkfifo via libc-less route: use the `mkfifo` binary if present, else skip.
        let fifo = dir.path().join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(fifo.as_os_str())
            .status();
        if !status.map(|s| s.success()).unwrap_or(false) {
            return; // environment without mkfifo — skip silently.
        }
        let store = MemoryStore::new();
        let err = build_tree(dir.path(), "snap", &CdcParams::default(), &store)
            .expect_err("fifo must error");
        assert!(err.to_string().contains("unsupported file type"), "{err}");
        let _ = fifo.as_os_str().as_bytes();
    }
}
