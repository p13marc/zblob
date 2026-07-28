//! On-disk resume state for an interrupted download: a chunk bitfield.
//!
//! Progress is persisted next to the `.part` file as a compact bitset sidecar
//! so a dropped connection *or* a process restart continues instead of
//! restarting. The sidecar is bound to the blob's `id` + BLAKE3 `root` +
//! geometry, so a source regenerated between attempts can never splice
//! mismatched halves — on any mismatch the partial is discarded.
//!
//! Two crash-consistency rules (v1 violated both, H4/H5):
//!
//! 1. **Writes are atomic**: unique temp file → `sync_all` → `rename` → fsync
//!    of the parent directory. A crash mid-save leaves the previous sidecar,
//!    never a torn one.
//! 2. **Bits never claim un-synced data**: the client `sync_data`s the `.part`
//!    *before* saving the sidecar (see the client's batching), so the failure
//!    direction is always "data present, bit unset" — which merely re-fetches.
//!
//! The on-disk form is an 8-byte magic (`ZBLOBRS2`) + postcard body. Saves are
//! batched by the client (every N chunks / T seconds), not per chunk — v1's
//! per-chunk full-JSON rewrite generated ~2.4× the blob size in sidecar I/O.

use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::manifest::Manifest;
use crate::wire::WIRE_VERSION;

const MAGIC: &[u8; 8] = b"ZBLOBRS2";

/// Persistent record of which chunks of a blob are already on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResumeState {
    /// Sidecar schema version (first field; postcard is positional).
    version: u16,
    /// The blob id this partial belongs to.
    id: String,
    /// The BLAKE3 bao root (resume binding — see module docs).
    root: Hash,
    /// Total blob length (must match the manifest to reuse the partial).
    total_len: u64,
    /// Transfer chunk size (ditto).
    chunk_size: u32,
    /// Presence bitfield: bit `i` (byte `i / 8`, LSB-first) = chunk `i` done.
    bits: Vec<u8>,
}

impl ResumeState {
    /// Path of the sidecar for a given `.part` file.
    pub fn sidecar_path(part: &Path) -> PathBuf {
        let mut p = part.as_os_str().to_os_string();
        p.push(".meta");
        PathBuf::from(p)
    }

    /// A fresh, all-missing state for `manifest` (validated by the caller).
    pub fn fresh(manifest: &Manifest, chunk_count: u32) -> Self {
        ResumeState {
            version: WIRE_VERSION,
            id: manifest.id.clone(),
            root: manifest.root,
            total_len: manifest.total_len,
            chunk_size: manifest.chunk_size,
            bits: vec![0u8; chunk_count.div_ceil(8) as usize],
        }
    }

    /// Whether this saved state can be reused to resume `manifest` (same id,
    /// root, geometry, and a well-formed bitfield).
    pub fn matches(&self, manifest: &Manifest, chunk_count: u32) -> bool {
        self.version == WIRE_VERSION
            && self.id == manifest.id
            && self.root == manifest.root
            && self.total_len == manifest.total_len
            && self.chunk_size == manifest.chunk_size
            && self.bits.len() == chunk_count.div_ceil(8) as usize
    }

    /// Number of chunks this state describes (derived from the recorded
    /// geometry — bounds every bit access, including the byte-padding bits).
    fn chunk_count(&self) -> u32 {
        if self.chunk_size == 0 {
            return 0;
        }
        self.total_len.div_ceil(self.chunk_size as u64) as u32
    }

    /// Whether chunk `i` is present.
    pub fn is_set(&self, i: u32) -> bool {
        i < self.chunk_count()
            && (i / 8) < self.bits.len() as u32
            && self.bits[(i / 8) as usize] & (1 << (i % 8)) != 0
    }

    /// Mark chunk `i` present; returns `true` if it was newly set.
    pub fn mark(&mut self, i: u32) -> bool {
        if i >= self.chunk_count() || (i / 8) >= self.bits.len() as u32 || self.is_set(i) {
            return false;
        }
        self.bits[(i / 8) as usize] |= 1 << (i % 8);
        true
    }

    /// Number of chunks present.
    pub fn received(&self) -> u32 {
        self.bits.iter().map(|b| b.count_ones()).sum()
    }

    /// Whether all `chunk_count` chunks are present.
    pub fn is_complete(&self, chunk_count: u32) -> bool {
        self.received() >= chunk_count
    }

    /// Coalesced ranges of missing chunks in `[0, chunk_count)` — exactly the
    /// holes a range-set query asks for.
    pub fn missing_ranges(&self, chunk_count: u32) -> Vec<Range<u32>> {
        let mut out: Vec<Range<u32>> = Vec::new();
        let mut i = 0u32;
        while i < chunk_count {
            if self.is_set(i) {
                i += 1;
                continue;
            }
            let start = i;
            while i < chunk_count && !self.is_set(i) {
                i += 1;
            }
            out.push(start..i);
        }
        out
    }

    /// Load the sidecar for `part`, if it exists, has the magic, and parses.
    pub async fn load(part: &Path) -> Option<ResumeState> {
        let bytes = tokio::fs::read(Self::sidecar_path(part)).await.ok()?;
        let body = bytes.strip_prefix(MAGIC.as_slice())?;
        crate::wire::decode(body).ok()
    }

    /// Persist the sidecar atomically: unique temp → `sync_all` → rename →
    /// fsync the parent directory (rule 1 in the module docs).
    pub async fn save_atomic(&self, part: &Path) -> Result<()> {
        let dst = Self::sidecar_path(part);
        let mut payload = Vec::with_capacity(8 + self.bits.len() + 64);
        payload.extend_from_slice(MAGIC);
        payload.extend_from_slice(&crate::wire::encode(self)?);
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            let dir = dst.parent().unwrap_or(Path::new("."));
            let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
            tmp.write_all(&payload)?;
            tmp.as_file().sync_all()?;
            tmp.persist(&dst).map_err(|e| e.error)?;
            fsync_dir(dir)
        })
        .await
        .map_err(|e| BlobError::Protocol(format!("sidecar save task: {e}")))??;
        Ok(())
    }

    /// Remove the sidecar (best-effort).
    pub async fn remove(part: &Path) {
        let _ = tokio::fs::remove_file(Self::sidecar_path(part)).await;
    }
}

/// Durably record a directory-entry change (rename/create) on platforms where
/// that requires fsyncing the directory itself.
#[cfg(unix)]
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}
#[cfg(not(unix))]
fn fsync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::DEFAULT_CHUNK_SIZE;

    fn manifest(chunks: u32) -> Manifest {
        Manifest {
            version: WIRE_VERSION,
            id: "blob".into(),
            filename: None,
            total_len: DEFAULT_CHUNK_SIZE as u64 * chunks as u64,
            chunk_size: DEFAULT_CHUNK_SIZE,
            root: Hash::of(b"data"),
            created_ms: 0,
        }
    }

    #[test]
    fn bitfield_marks_and_counts() {
        let mut s = ResumeState::fresh(&manifest(10), 10);
        assert_eq!(s.received(), 0);
        assert!(s.mark(3));
        assert!(!s.mark(3)); // duplicate → not newly set
        assert!(s.mark(9));
        assert!(!s.mark(10)); // out of capacity range
        assert_eq!(s.received(), 2);
        assert!(s.is_set(3) && s.is_set(9) && !s.is_set(0));
    }

    #[test]
    fn missing_ranges_coalesce_holes() {
        let mut s = ResumeState::fresh(&manifest(10), 10);
        for i in [0u32, 1, 4, 7, 8] {
            s.mark(i);
        }
        assert_eq!(s.missing_ranges(10), vec![2..4, 5..7, 9..10]);
        for i in 0..10 {
            s.mark(i);
        }
        assert!(s.missing_ranges(10).is_empty());
        assert!(s.is_complete(10));
    }

    #[tokio::test]
    async fn save_load_roundtrip_and_binding() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("x.part");
        let m = manifest(20);
        let mut s = ResumeState::fresh(&m, 20);
        s.mark(5);
        s.mark(6);
        s.save_atomic(&part).await.unwrap();

        let loaded = ResumeState::load(&part).await.unwrap();
        assert!(loaded.matches(&m, 20));
        assert_eq!(loaded.missing_ranges(20)[0], 0..5);

        // A different root (regenerated source) must not match.
        let other = Manifest {
            root: Hash::of(b"other"),
            ..m
        };
        assert!(!loaded.matches(&other, 20));
    }

    #[tokio::test]
    async fn torn_or_foreign_sidecar_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("y.part");
        // Garbage without magic.
        tokio::fs::write(ResumeState::sidecar_path(&part), b"not a sidecar")
            .await
            .unwrap();
        assert!(ResumeState::load(&part).await.is_none());
        // Magic but truncated body.
        tokio::fs::write(ResumeState::sidecar_path(&part), &MAGIC[..])
            .await
            .unwrap();
        assert!(ResumeState::load(&part).await.is_none());
    }
}
