//! Content-addressed chunk store (the Tier-2 substrate).
//!
//! Chunks are named by their content hash, so a store is the dedup + resume
//! substrate at once: "progress" is simply *which hashes are on disk*. The
//! trait is sync (local, fast ops) — async call sites wrap it in
//! `spawn_blocking`; a remote chunk is fetched once and `put` here.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use crate::compress::{ChunkCompression, ContainerError, TAG_RAW, TAG_SEALED, pack, try_unpack};
use crate::hash::Hash;
use crate::paths::fsync_dir;

/// A local store of content-addressed chunks.
///
/// # Absence and failure are different answers
///
/// `has` and `get` return `io::Result`, and the distinction is load-bearing.
/// "This chunk is not here" is a normal answer that makes the caller fetch it
/// over the network; "I could not tell you" is a failure that must stop the
/// transfer. Conflating them — returning `bool`/`Option` and mapping an
/// `EIO` to "absent", as this trait used to — produces a download that
/// re-fetches a chunk, stores it into the same broken store, finds it missing
/// again, and loops forever with nothing in any log to explain it.
///
/// Reporting *corruption* as absence is still correct and still expected: a
/// chunk that fails its own integrity check should be removed and reported
/// `Ok(None)`, so the caller re-fetches and the store heals. That is a policy
/// choice, and it is now spelled differently from an accident.
///
/// # Blocking is expected
///
/// Every method is synchronous, because a store is a local disk or database
/// call. The crate always invokes them from `spawn_blocking`, and prefers the
/// `*_many` methods so that one dispatch covers a whole round rather than one
/// chunk — which is also the shape a transactional store wants.
pub trait ContentStore: Send + Sync {
    /// Whether chunk `hash` is present and readable.
    fn has(&self, hash: &Hash) -> std::io::Result<bool>;
    /// Fetch chunk `hash`; `Ok(None)` if it is absent (or was removed as
    /// corrupt), `Err` if the store could not answer.
    fn get(&self, hash: &Hash) -> std::io::Result<Option<Vec<u8>>>;
    /// Store chunk `hash` → `bytes` (idempotent).
    fn put(&self, hash: &Hash, bytes: &[u8]) -> std::io::Result<()>;
    /// Remove chunk `hash`; returns whether it was present. Content-addressed
    /// removal is only safe from [`crate::gc::sweep`] or an equivalent
    /// liveness analysis — a chunk may be shared by many files and snapshots.
    fn remove(&self, hash: &Hash) -> std::io::Result<bool>;

    /// Visit every chunk hash currently in the store.
    ///
    /// The streaming form is the one to implement: [`hashes`](Self::hashes)
    /// materializes the entire keyspace, which for a fleet-scale store is
    /// 32 bytes times every chunk it holds, held in memory at once.
    fn for_each_hash(&self, f: &mut dyn FnMut(Hash) -> std::io::Result<()>) -> std::io::Result<()>;

    /// Presence of many chunks in one call.
    ///
    /// The default loops, which is right for a store where a lookup is a
    /// syscall. Override it wherever a lookup is a *transaction* — one visit
    /// for a whole round instead of one per chunk is the difference that
    /// motivated this method.
    fn has_many(&self, hashes: &[Hash]) -> std::io::Result<Vec<bool>> {
        hashes.iter().map(|h| self.has(h)).collect()
    }

    /// Fetch many chunks in one call; `None` per absent entry, positionally.
    fn get_many(&self, hashes: &[Hash]) -> std::io::Result<Vec<Option<Vec<u8>>>> {
        hashes.iter().map(|h| self.get(h)).collect()
    }

    /// Store many chunks in one call.
    ///
    /// Not atomic unless an implementation makes it so: on error, some may
    /// have landed. That is safe here because chunks are content-addressed —
    /// a partial write is a smaller store, never a wrong one.
    fn put_many(&self, chunks: &[(Hash, &[u8])]) -> std::io::Result<()> {
        for (hash, bytes) in chunks {
            self.put(hash, bytes)?;
        }
        Ok(())
    }

    /// Every chunk hash currently in the store, collected.
    ///
    /// Prefer [`for_each_hash`](Self::for_each_hash) — this exists for callers
    /// that genuinely need the whole set at once (a sweep's mark phase).
    fn hashes(&self) -> std::io::Result<Vec<Hash>> {
        let mut out = Vec::new();
        self.for_each_hash(&mut |h| {
            out.push(h);
            Ok(())
        })?;
        Ok(out)
    }
}

/// Sharing a store is transparent: `Arc<S>` is a `ContentStore` wherever `S`
/// is, so a caller holding an `Arc<dyn ContentStore>` can pass it to anything
/// taking `&dyn ContentStore` without reaching through it.
///
/// The crate's own signatures are split between the two shapes — a server
/// keeps an `Arc`, a builder borrows — and without this, callers pay for that
/// distinction with `&*` at the boundary.
impl<T: ContentStore + ?Sized> ContentStore for std::sync::Arc<T> {
    fn has(&self, hash: &Hash) -> std::io::Result<bool> {
        (**self).has(hash)
    }
    fn get(&self, hash: &Hash) -> std::io::Result<Option<Vec<u8>>> {
        (**self).get(hash)
    }
    fn put(&self, hash: &Hash, bytes: &[u8]) -> std::io::Result<()> {
        (**self).put(hash, bytes)
    }
    fn remove(&self, hash: &Hash) -> std::io::Result<bool> {
        (**self).remove(hash)
    }
    fn for_each_hash(&self, f: &mut dyn FnMut(Hash) -> std::io::Result<()>) -> std::io::Result<()> {
        (**self).for_each_hash(f)
    }
    // Forward the batch methods too, or an `Arc` would silently fall back to
    // the looping defaults and undo the batching the inner store implements.
    fn has_many(&self, hashes: &[Hash]) -> std::io::Result<Vec<bool>> {
        (**self).has_many(hashes)
    }
    fn get_many(&self, hashes: &[Hash]) -> std::io::Result<Vec<Option<Vec<u8>>>> {
        (**self).get_many(hashes)
    }
    fn put_many(&self, chunks: &[(Hash, &[u8])]) -> std::io::Result<()> {
        (**self).put_many(chunks)
    }
    fn hashes(&self) -> std::io::Result<Vec<Hash>> {
        (**self).hashes()
    }
}

/// An in-memory [`ContentStore`] (tests, ephemeral caches).
#[derive(Default)]
pub struct MemoryStore(Mutex<HashMap<Hash, Vec<u8>>>);

impl MemoryStore {
    /// A new empty store.
    pub fn new() -> Self {
        MemoryStore(Mutex::new(HashMap::new()))
    }

    /// Chunk bytes are plain data — a panic in another thread mid-`insert`
    /// cannot leave the map logically torn, so poisoning is not propagated.
    fn map(&self) -> MutexGuard<'_, HashMap<Hash, Vec<u8>>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Number of stored chunks.
    pub fn len(&self) -> usize {
        self.map().len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every stored chunk. Used by a producer that keeps only one live
    /// snapshot at a time: replacing or expiring it frees the prior bytes without
    /// swapping the `Arc<dyn ContentStore>` a [`crate::TreeServer`] holds.
    pub fn clear(&self) {
        self.map().clear();
    }
}

impl ContentStore for MemoryStore {
    fn has(&self, hash: &Hash) -> std::io::Result<bool> {
        Ok(self.map().contains_key(hash))
    }
    fn get(&self, hash: &Hash) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.map().get(hash).cloned())
    }
    fn put(&self, hash: &Hash, bytes: &[u8]) -> std::io::Result<()> {
        self.map().insert(*hash, bytes.to_vec());
        Ok(())
    }
    fn remove(&self, hash: &Hash) -> std::io::Result<bool> {
        Ok(self.map().remove(hash).is_some())
    }
    fn for_each_hash(&self, f: &mut dyn FnMut(Hash) -> std::io::Result<()>) -> std::io::Result<()> {
        // Collect under the lock, then visit: `f` is caller code and must not
        // run while the map is held, or a store operation inside it deadlocks.
        let keys: Vec<Hash> = self.map().keys().copied().collect();
        keys.into_iter().try_for_each(f)
    }
    fn put_many(&self, chunks: &[(Hash, &[u8])]) -> std::io::Result<()> {
        let mut map = self.map();
        for (hash, bytes) in chunks {
            map.insert(*hash, bytes.to_vec());
        }
        Ok(())
    }
}

/// A filesystem [`ContentStore`]: `<root>/blake3/<xx>/<hex>` (two-hex-char
/// fanout so huge stores don't melt directory listings), atomic puts (unique
/// temp file → fsync → rename → dir fsync), and optional verify-on-read.
///
/// Survives process restart, so it's the natural resume/dedup backing.
pub struct DirStore {
    root: PathBuf,
    verify_on_read: bool,
    compression: ChunkCompression,
    #[cfg(feature = "encryption")]
    encryption: Option<crate::crypt::StoreKey>,
}

impl DirStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(root.join(Hash::ALGO))?;
        Ok(DirStore {
            root,
            verify_on_read: false,
            compression: ChunkCompression::default(),
            #[cfg(feature = "encryption")]
            encryption: None,
        })
    }

    /// Seal every chunk at rest with XChaCha20-Poly1305 under `key` (see
    /// [`StoreKey`](crate::StoreKey) and the `crypt` module docs for the
    /// dedup-membership caveat). Reading understands sealed and plain chunks
    /// side by side; a sealed chunk read without the key reports missing —
    /// never deleted, never garbage.
    #[cfg(feature = "encryption")]
    pub fn with_encryption(mut self, key: crate::crypt::StoreKey) -> Self {
        self.encryption = Some(key);
        self
    }

    /// Compress chunks at rest (see [`ChunkCompression`]; requires the `zstd`
    /// feature for [`ChunkCompression::Zstd`]). Reading understands both
    /// compressed and raw frames regardless of this setting.
    pub fn with_compression(mut self, compression: ChunkCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Re-hash every chunk on [`get`](ContentStore::get); a corrupted chunk is
    /// deleted and reported missing, so the caller re-fetches instead of
    /// silently materializing garbage (local disk corruption is otherwise
    /// invisible — `root_hash` covers the entry list, not chunk contents).
    pub fn with_verify_on_read(mut self, verify: bool) -> Self {
        self.verify_on_read = verify;
        self
    }

    fn algo_dir(&self) -> PathBuf {
        self.root.join(Hash::ALGO)
    }

    fn path(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_string();
        self.algo_dir().join(&hex[..2]).join(hex)
    }

    /// Decode an at-rest chunk container: unseal (if sealed and a key is
    /// configured), then unframe compression. `strict_seal` decides what a
    /// failed unseal means: on the read path it is *not* treated as
    /// corruption (a wrong key is indistinguishable from tampering, and reads
    /// must never destroy data they cannot decrypt); a keyed [`scrub`] passes
    /// `true` to actually detect sealed-chunk tampering.
    fn decode_at_rest(
        &self,
        packed: &[u8],
        hash: &Hash,
        strict_seal: bool,
    ) -> std::result::Result<Vec<u8>, ContainerError> {
        let _ = (hash, strict_seal);
        if packed.first() == Some(&TAG_SEALED) {
            #[cfg(feature = "encryption")]
            if let Some(key) = &self.encryption {
                return match crate::crypt::open(key, hash, packed) {
                    Some(inner) => try_unpack(&inner),
                    None if strict_seal => Err(ContainerError::Corrupt),
                    None => Err(ContainerError::Unsupported),
                };
            }
            return Err(ContainerError::Unsupported);
        }
        try_unpack(packed)
    }

    /// Re-hash every chunk; delete and report the corrupted ones. The store
    /// heals on the next download (missing chunks are re-fetched). Chunks in
    /// a format this build cannot read (missing feature / no key) are
    /// skipped — but **run a keyed scrub only with the store's correct key**:
    /// with the right key a sealed chunk that fails to open is reported (and
    /// removed) as tampering.
    pub fn scrub(&self) -> std::io::Result<Vec<Hash>> {
        let mut corrupted = Vec::new();
        for hash in self.hashes()? {
            let path = self.path(&hash);
            let verdict = std::fs::read(&path)
                .map_err(|_| ContainerError::Corrupt)
                .and_then(|packed| self.decode_at_rest(&packed, &hash, true));
            match verdict {
                Ok(bytes) if Hash::of(&bytes) == hash => {}
                Err(ContainerError::Unsupported) => {}
                Ok(_) | Err(ContainerError::Corrupt) => {
                    let _ = std::fs::remove_file(&path);
                    corrupted.push(hash);
                }
            }
        }
        Ok(corrupted)
    }
}

impl ContentStore for DirStore {
    fn has(&self, hash: &Hash) -> std::io::Result<bool> {
        // Presence must mean "get() will hand these bytes back". The download
        // path calls `has` to decide *not* to fetch, so a chunk this store
        // cannot decode — missing cargo feature, missing key, *wrong* key —
        // has to report absent, or the transfer wedges with no way to heal.
        // A raw chunk costs one header byte; a compressed or sealed one costs
        // a read plus its decode, which is the only honest answer available
        // (a wrong AEAD key is indistinguishable from tampering without it).
        // Content-level rot remains verify_on_read/scrub territory; `has`
        // never deletes.
        //
        // A *missing file* is `Ok(false)`. Any other I/O failure is `Err`:
        // reporting `EIO` or `EACCES` as "absent" makes the caller re-fetch
        // into a store that will never accept the bytes, forever.
        use std::io::Read;
        let path = self.path(hash);
        let mut f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        let mut tag = [0u8; 1];
        match f.read_exact(&mut tag) {
            Ok(()) => {}
            // Zero-length file: not even a raw frame; re-fetch it.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        if tag[0] == TAG_RAW {
            return Ok(true);
        }
        drop(f);
        let packed = match std::fs::read(&path) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        Ok(self.decode_at_rest(&packed, hash, false).is_ok())
    }

    fn get(&self, hash: &Hash) -> std::io::Result<Option<Vec<u8>>> {
        let path = self.path(hash);
        let packed = match std::fs::read(&path) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let bytes = match self.decode_at_rest(&packed, hash, false) {
            Ok(bytes) => bytes,
            // A format this build can't read (missing feature / no key) must
            // be left alone; corruption is reported missing so the caller
            // re-fetches instead of materializing garbage. Both are `Ok(None)`
            // — deliberate policy, distinct from the I/O failures above.
            Err(ContainerError::Unsupported) => return Ok(None),
            Err(ContainerError::Corrupt) => {
                let _ = std::fs::remove_file(&path);
                return Ok(None);
            }
        };
        if self.verify_on_read && Hash::of(&bytes) != *hash {
            let _ = std::fs::remove_file(&path);
            return Ok(None);
        }
        Ok(Some(bytes))
    }

    fn put(&self, hash: &Hash, bytes: &[u8]) -> std::io::Result<()> {
        #[allow(unused_mut)]
        let mut packed = pack(bytes, self.compression).map_err(std::io::Error::other)?;
        #[cfg(feature = "encryption")]
        if let Some(key) = &self.encryption {
            packed = crate::crypt::seal(key, hash, &packed)?;
        }
        let dst = self.path(hash);
        let dir = dst.parent().expect("fanout dir");
        std::fs::create_dir_all(dir)?;
        // Unique temp name (concurrent puts of the same chunk must not clobber
        // each other's temp file) → fsync → atomic rename → dir fsync.
        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
        tmp.write_all(&packed)?;
        tmp.as_file().sync_all()?;
        if let Err(e) = tmp.persist(&dst) {
            // Losing the race to another writer is success, not failure: the
            // address determines the bytes, so whoever landed first wrote
            // exactly what we would have. POSIX rename silently replaces, but
            // Windows refuses when the destination exists or is open by
            // another handle — without this, concurrent puts of one chunk
            // (the normal case when several downloads share a store) fail on
            // Windows only.
            if dst.exists() {
                return fsync_dir(dir);
            }
            return Err(e.error);
        }
        fsync_dir(dir)
    }

    fn remove(&self, hash: &Hash) -> std::io::Result<bool> {
        match std::fs::remove_file(self.path(hash)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn for_each_hash(&self, f: &mut dyn FnMut(Hash) -> std::io::Result<()>) -> std::io::Result<()> {
        let algo_dir = self.algo_dir();
        for fan in std::fs::read_dir(&algo_dir)? {
            let fan = fan?;
            if !fan.file_type()?.is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(fan.path())? {
                let entry = entry?;
                if let Some(name) = entry.file_name().to_str()
                    && let Ok(hash) = name.parse::<Hash>()
                {
                    f(hash)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(data: &[u8]) -> Hash {
        Hash::of(data)
    }

    #[test]
    fn memory_store_roundtrip() {
        let s = MemoryStore::new();
        let hash = h(b"hello");
        assert!(!s.has(&hash).unwrap());
        s.put(&hash, b"hello").unwrap();
        assert!(s.has(&hash).unwrap());
        assert_eq!(s.get(&hash).unwrap().unwrap(), b"hello");
        assert_eq!(s.hashes().unwrap(), vec![hash]);
    }

    #[test]
    fn dir_store_roundtrip_with_fanout() {
        let dir = tempfile::tempdir().unwrap();
        let s = DirStore::open(dir.path()).unwrap();
        let hash = h(b"world");
        assert!(!s.has(&hash).unwrap());
        s.put(&hash, b"world").unwrap();
        assert!(s.has(&hash).unwrap());
        assert_eq!(s.get(&hash).unwrap().unwrap(), b"world");
        // Fanout layout: <root>/blake3/<xx>/<hex>.
        let hex = hash.to_string();
        assert!(
            dir.path()
                .join("blake3")
                .join(&hex[..2])
                .join(&hex)
                .exists()
        );
        // Reopening sees the persisted chunk (restart-proof).
        let s2 = DirStore::open(dir.path()).unwrap();
        assert!(s2.has(&hash).unwrap());
        assert_eq!(s2.hashes().unwrap(), vec![hash]);
    }

    #[test]
    fn verify_on_read_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let s = DirStore::open(dir.path())
            .unwrap()
            .with_verify_on_read(true);
        let hash = h(b"payload");
        s.put(&hash, b"payload").unwrap();
        // Corrupt the stored chunk behind the store's back.
        let hex = hash.to_string();
        let path = dir.path().join("blake3").join(&hex[..2]).join(&hex);
        std::fs::write(&path, b"garbage").unwrap();
        // Verified read reports it missing and removes it → refetch heals.
        assert!(s.get(&hash).unwrap().is_none());
        assert!(!path.exists());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_store_roundtrip_and_key_required() {
        use crate::crypt::StoreKey;
        let dir = tempfile::tempdir().unwrap();
        let s = DirStore::open(dir.path())
            .unwrap()
            .with_encryption(StoreKey::new([7u8; 32]));
        let plaintext = b"very secret chunk contents".to_vec();
        let hash = h(&plaintext);
        s.put(&hash, &plaintext).unwrap();
        assert_eq!(s.get(&hash).unwrap().unwrap(), plaintext);

        // The on-disk file is sealed: no plaintext, sealed tag first.
        let hex = hash.to_string();
        let path = dir.path().join("blake3").join(&hex[..2]).join(&hex);
        let on_disk = std::fs::read(&path).unwrap();
        assert_eq!(on_disk[0], 0x02);
        assert!(!on_disk.windows(plaintext.len()).any(|w| w == plaintext));

        // Opening the store without the key: chunk reads as missing but the
        // sealed file is never deleted or mistaken for corruption.
        let no_key = DirStore::open(dir.path()).unwrap();
        assert!(no_key.get(&hash).unwrap().is_none());
        assert!(path.exists(), "sealed chunk must not be deleted");
        assert!(no_key.scrub().unwrap().is_empty(), "scrub must skip sealed");

        // Wrong key: same safety.
        let wrong = DirStore::open(dir.path())
            .unwrap()
            .with_encryption(StoreKey::new([8u8; 32]));
        assert!(wrong.get(&hash).unwrap().is_none());

        // Tampered ciphertext: detected as corruption by a keyed scrub.
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(s.scrub().unwrap(), vec![hash]);
        assert!(!path.exists());
    }

    #[test]
    fn scrub_reports_and_removes_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let s = DirStore::open(dir.path()).unwrap();
        let good = h(b"good");
        let bad = h(b"will corrupt");
        s.put(&good, b"good").unwrap();
        s.put(&bad, b"will corrupt").unwrap();
        let hex = bad.to_string();
        std::fs::write(
            dir.path().join("blake3").join(&hex[..2]).join(&hex),
            b"flipped",
        )
        .unwrap();

        let corrupted = s.scrub().unwrap();
        assert_eq!(corrupted, vec![bad]);
        assert!(s.has(&good).unwrap() && !s.has(&bad).unwrap());
    }
}
