//! Content-store lifecycle: tags + mark-and-sweep garbage collection.
//!
//! A [`ContentStore`] accumulates chunks forever on its
//! own — client caches and router-hosted stores both grow without bound. The
//! model here is iroh's, cut down:
//!
//! - **Persistent tags** ([`SnapshotTags`]): named references to whole
//!   snapshots, stored on disk (one postcard [`TagRecord`] per tag), so
//!   liveness survives restart. Tag what you want to keep.
//! - **Temp tags** ([`TempTags`] / [`TempTag`]): in-memory guards for chunks a
//!   running download is about to add. A sweep racing a download must not
//!   collect chunks the index references but the store only half-has.
//! - **Mark and sweep** ([`sweep`]): everything reachable from the persistent
//!   tags, live temp tags, and any extra roots survives; the rest is removed.
//!
//! Give the client the same [`TempTags`] the sweep uses and the protection is
//! automatic — `download_tree` takes a temp tag over the snapshot's chunks for
//! the duration of the transfer:
//!
//! ```ignore
//! let temps = Arc::new(gc::TempTags::new());
//! let tags = SnapshotTags::open(state_dir.join("tags"))?;
//! let client = TreeClient::builder(session, store_prefix, tree_prefix)
//!     .temp_tags(temps.clone())     // downloads now protect themselves
//!     .build();
//! client.download_tree(&req, &dest, &store, &(), &cancel).await?;
//! tags.set("current", &index)?;      // survives restart
//! let stats = gc::sweep(&*store, &tags, &temps, [])?;
//! ```
//!
//! Without a shared registry a concurrent sweep is free to delete chunks a
//! running download has already fetched, and the download then fails with a
//! `NotFound` for a chunk it just stored.
//!
//! **A sweep is still not atomic against concurrent writers.** The live set is
//! computed and then acted on, so a snapshot tagged, or registered on a
//! [`TreeServer`](crate::TreeServer), *during* a sweep is not protected by it —
//! pass such indices as `extra_roots`.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::paths::fsync_dir;
use crate::store::ContentStore;
use crate::tree::TreeIndex;

/// What a persistent tag records about a snapshot.
///
/// Only what liveness needs: the root that names the snapshot, and the chunks
/// keeping it alive. Storing the whole [`TreeIndex`] meant every sweep decoded
/// every tagged snapshot's full entry list — paths, modes, sizes, per-file
/// chunk vectors — to reach a set of hashes it could have stored directly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TagRecord {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// The tagged snapshot's identity.
    pub root: Hash,
    /// The distinct chunk hashes the snapshot references.
    pub chunks: Vec<Hash>,
}

/// Persistent, named snapshot references: one postcard-encoded [`TagRecord`]
/// per tag, in a directory. A tagged snapshot's chunks are live.
#[derive(Debug)]
pub struct SnapshotTags {
    dir: PathBuf,
}

impl SnapshotTags {
    /// Open (creating if needed) a tag directory.
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(SnapshotTags { dir })
    }

    fn path(&self, name: &str) -> Result<PathBuf> {
        // Tag names are user-chosen file names: same shape rules as blob ids.
        crate::manifest::validate_id(name)
            .map_err(|_| BlobError::Usage(format!("invalid tag name {name:?}")))?;
        Ok(self.dir.join(format!("{name}.tag")))
    }

    /// Create or replace tag `name` → `index` (atomic, fsynced).
    ///
    /// Only the snapshot's root and chunk set are written; see [`TagRecord`].
    pub fn set(&self, name: &str, index: &TreeIndex) -> Result<()> {
        use std::io::Write;
        let path = self.path(name)?;
        let record = TagRecord {
            version: crate::wire::WIRE_VERSION,
            root: index.root_hash,
            chunks: index.needed_chunks(),
        };
        let payload = crate::wire::encode(&record)?;
        let mut tmp = tempfile::NamedTempFile::new_in(&self.dir)?;
        tmp.write_all(&payload)?;
        tmp.as_file().sync_all()?;
        tmp.persist(&path).map_err(|e| e.error)?;
        fsync_dir(&self.dir)?;
        Ok(())
    }

    /// Read tag `name`, if present.
    pub fn get(&self, name: &str) -> Result<Option<TagRecord>> {
        let path = self.path(name)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(crate::wire::decode(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete tag `name`; returns whether it existed.
    pub fn remove(&self, name: &str) -> Result<bool> {
        let path = self.path(name)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// All tag names.
    pub fn list(&self) -> std::io::Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let name = entry?.file_name();
            if let Some(n) = name.to_str().and_then(|n| n.strip_suffix(".tag")) {
                out.push(n.to_string());
            }
        }
        out.sort();
        Ok(out)
    }

    /// The union of every tagged snapshot's chunk hashes.
    pub fn live_set(&self) -> Result<HashSet<Hash>> {
        let mut live = HashSet::new();
        for name in self.list()? {
            if let Some(record) = self.get(&name)? {
                live.extend(record.chunks);
            }
        }
        Ok(live)
    }
}

/// A registry of in-memory temp tags protecting in-flight chunk sets.
#[derive(Default, Debug)]
pub struct TempTags {
    sets: Mutex<Vec<Weak<HashSet<Hash>>>>,
}

/// A live guard: while it exists, its chunks survive [`sweep`]. Dropping it
/// releases the protection.
#[derive(Debug)]
pub struct TempTag {
    _set: Arc<HashSet<Hash>>,
}

impl TempTags {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Protect `hashes` until the returned tag is dropped.
    ///
    /// The protection *is* the returned value's lifetime, so `temps.protect(h);`
    /// — which compiles and reads like it did something — drops the tag on the
    /// same line and protects nothing. Hence `#[must_use]`.
    #[must_use = "protection lasts only as long as the returned TempTag is held; \
                  dropping it immediately protects nothing"]
    pub fn protect(&self, hashes: impl IntoIterator<Item = Hash>) -> TempTag {
        let set = Arc::new(hashes.into_iter().collect::<HashSet<_>>());
        let mut sets = self.sets.lock().unwrap_or_else(|e| e.into_inner());
        sets.retain(|w| w.strong_count() > 0);
        sets.push(Arc::downgrade(&set));
        TempTag { _set: set }
    }

    /// The union of all currently-alive temp tags.
    pub fn live_set(&self) -> HashSet<Hash> {
        let mut sets = self.sets.lock().unwrap_or_else(|e| e.into_inner());
        sets.retain(|w| w.strong_count() > 0);
        let mut live = HashSet::new();
        for w in sets.iter() {
            if let Some(set) = w.upgrade() {
                live.extend(set.iter().copied());
            }
        }
        live
    }
}

/// Result of a [`sweep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcStats {
    /// Chunks that survived (live).
    pub kept: u64,
    /// Chunks removed as garbage.
    pub removed: u64,
}

/// Mark and sweep `store`: everything reachable from `tags`, live `temps`, and
/// `extra_roots` (additional indices to keep, e.g. ones currently registered
/// on a [`crate::TreeServer`]) is kept; every other chunk is removed.
pub fn sweep<'a>(
    store: &dyn ContentStore,
    tags: &SnapshotTags,
    temps: &TempTags,
    extra_roots: impl IntoIterator<Item = &'a TreeIndex>,
) -> Result<GcStats> {
    let mut live = tags.live_set()?;
    live.extend(temps.live_set());
    for index in extra_roots {
        live.extend(index.needed_chunks());
    }

    let mut stats = GcStats {
        kept: 0,
        removed: 0,
    };
    for hash in store.hashes()? {
        if live.contains(&hash) {
            stats.kept += 1;
        } else if store.remove(&hash)? {
            stats.removed += 1;
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::CdcParams;
    use crate::store::{DirStore, MemoryStore};
    use crate::tree::build_tree;

    fn snapshot(dir: &std::path::Path, store: &dyn ContentStore, id: &str) -> TreeIndex {
        build_tree(dir, id, &CdcParams::default(), store).unwrap()
    }

    #[test]
    fn tags_persist_across_reopen() {
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("f.txt"), b"tagged content").unwrap();
        let store = MemoryStore::new();
        let index = snapshot(src.path(), &store, "snap");

        let tag_dir = tempfile::tempdir().unwrap();
        let tags = SnapshotTags::open(tag_dir.path()).unwrap();
        tags.set("current", &index).unwrap();
        assert_eq!(tags.list().unwrap(), vec!["current"]);

        // Reopen (as after a restart): the tag and its live set survive.
        let tags2 = SnapshotTags::open(tag_dir.path()).unwrap();
        let record = tags2.get("current").unwrap().unwrap();
        assert_eq!(record.root, index.root_hash);
        assert_eq!(record.chunks, index.needed_chunks());
        assert_eq!(
            tags2.live_set().unwrap(),
            index.needed_chunks().into_iter().collect()
        );

        assert!(tags2.remove("current").unwrap());
        assert!(!tags2.remove("current").unwrap());
        assert!(tags2.list().unwrap().is_empty());
    }

    #[test]
    fn invalid_tag_names_rejected() {
        let tag_dir = tempfile::tempdir().unwrap();
        let tags = SnapshotTags::open(tag_dir.path()).unwrap();
        for bad in ["", "../escape", "a/b", "a*"] {
            assert!(tags.list().unwrap().is_empty());
            assert!(tags.get(bad).is_err(), "tag name {bad:?} must be rejected");
        }
    }

    #[test]
    fn sweep_keeps_tagged_removes_garbage() {
        let store_dir = tempfile::tempdir().unwrap();
        let store = DirStore::open(store_dir.path()).unwrap();

        // A tagged snapshot + some orphan chunks.
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("keep.txt"), b"keep me around").unwrap();
        let index = snapshot(src.path(), &store, "keep");
        for junk in [&b"orphan 1"[..], b"orphan 2", b"orphan 3"] {
            store.put(&Hash::of(junk), junk).unwrap();
        }

        let tags = SnapshotTags::open(store_dir.path().join("tags")).unwrap();
        tags.set("keep", &index).unwrap();
        let temps = TempTags::new();

        let stats = sweep(&store, &tags, &temps, []).unwrap();
        assert_eq!(stats.kept as usize, index.needed_chunks().len());
        assert_eq!(stats.removed, 3);
        for h in index.needed_chunks() {
            assert!(store.has(&h).unwrap(), "tagged chunk must survive");
        }
        assert!(!store.has(&Hash::of(b"orphan 1")).unwrap());
    }

    #[test]
    fn temp_tag_protects_until_dropped() {
        let store = MemoryStore::new();
        let h = Hash::of(b"in flight");
        store.put(&h, b"in flight").unwrap();

        let tag_dir = tempfile::tempdir().unwrap();
        let tags = SnapshotTags::open(tag_dir.path()).unwrap();
        let temps = TempTags::new();

        let guard = temps.protect([h]);
        let stats = sweep(&store, &tags, &temps, []).unwrap();
        assert_eq!((stats.kept, stats.removed), (1, 0));
        assert!(store.has(&h).unwrap());

        drop(guard);
        let stats = sweep(&store, &tags, &temps, []).unwrap();
        assert_eq!((stats.kept, stats.removed), (0, 1));
        assert!(!store.has(&h).unwrap());
    }

    #[test]
    fn extra_roots_protect_registered_snapshots() {
        let store = MemoryStore::new();
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("live.txt"), b"registered on a server").unwrap();
        let index = snapshot(src.path(), &store, "live");

        let tag_dir = tempfile::tempdir().unwrap();
        let tags = SnapshotTags::open(tag_dir.path()).unwrap();
        let stats = sweep(&store, &tags, &TempTags::new(), [&index]).unwrap();
        assert_eq!(stats.removed, 0);
        assert_eq!(stats.kept as usize, index.needed_chunks().len());
    }
}
