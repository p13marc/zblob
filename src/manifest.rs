//! The blob manifest — the single source of truth for a transfer.
//!
//! One GET returns the manifest; everything a client needs to size, resume,
//! and verify a download lives here (not in the slice keys), so there is no
//! second metadata source that can disagree. v2 removed the fields a hostile
//! peer could use to make sources of truth *disagree with each other*:
//! `chunk_count` is derived from `total_len`/`chunk_size`, the hash algorithm
//! is implied by the wire version, and the blob's identity **is** its BLAKE3
//! bao root — the value a caller pins out of band.

use serde::{Deserialize, Serialize};

use crate::chunk::TransferChunks;
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::wire::WIRE_VERSION;

/// Describes a single blob: identity, size, chunking, and BLAKE3 root.
///
/// Fields are public so tests and advanced callers can construct manifests
/// directly, but anything received from the network must pass
/// [`Manifest::validate`] before its sizes are trusted (the server validates
/// on register; the client validates on decode).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Wire schema version — always the first field (postcard is positional).
    /// Must equal [`WIRE_VERSION`].
    pub version: u16,
    /// Opaque caller-chosen id (e.g. a ULID). Appears in the blob key, so it
    /// must be a single, non-empty key segment.
    pub id: String,
    /// **Advisory** file name. This crate never joins it to any path — the
    /// caller chooses every destination (`download_to`) — because a remote
    /// party must not pick where bytes land (the v1 path-traversal vector).
    pub filename: Option<String>,
    /// Total blob length in bytes.
    pub total_len: u64,
    /// Transfer chunk size (validated: 16 KiB-aligned, within bounds).
    pub chunk_size: u32,
    /// BLAKE3 bao root — the blob's identity and integrity anchor. Every
    /// received slice is verified against it before touching disk.
    pub root: Hash,
    /// Creation time, Unix epoch milliseconds (caller-supplied; the crate
    /// avoids reading the wall clock so it stays side-effect-free).
    pub created_ms: i64,
}

impl Manifest {
    /// Validate the manifest's self-consistency and bound its resource claims:
    /// wire version, id shape, chunk-size rules, and `total_len <= max_blob_size`
    /// (the allocation bound for `.part` preallocation and the resume bitfield —
    /// a remote peer must not pick how much we allocate).
    pub fn validate(&self, max_blob_size: u64) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        validate_id(&self.id)?;
        TransferChunks::validate_chunk_size(self.chunk_size)?;
        if self.total_len > max_blob_size {
            return Err(BlobError::InvalidManifest(format!(
                "total_len {} exceeds the configured max blob size {max_blob_size}",
                self.total_len
            )));
        }
        Ok(())
    }

    /// The transfer-chunk geometry this manifest describes.
    ///
    /// Call [`Manifest::validate`] first for untrusted manifests; this errors
    /// on an invalid `chunk_size` but applies no size cap.
    pub fn chunks(&self) -> Result<TransferChunks> {
        TransferChunks::new(self.chunk_size, self.total_len)
    }

    /// The advisory filename reduced to something safe to join: the last
    /// `Normal` path component, or `None` if there isn't one. Callers who want
    /// to honor the server's suggestion should use this, never `filename` raw.
    pub fn suggested_filename(&self) -> Option<String> {
        let raw = self.filename.as_deref()?;
        let name = std::path::Path::new(raw)
            .components()
            .filter_map(|c| match c {
                std::path::Component::Normal(s) => s.to_str(),
                _ => None,
            })
            .next_back()?;
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }
}

/// A blob id must be a single non-empty Zenoh key segment of sane length: no
/// `/` or `\` (ids are joined into spool/tag file names), no wildcard/param
/// characters, no `..`.
pub(crate) fn validate_id(id: &str) -> Result<()> {
    let ok = !id.is_empty()
        && id.len() <= 200
        && id != ".."
        && !id.contains(['/', '\\', '*', '?', '#', '$'])
        && !id.chars().any(char::is_whitespace);
    if ok {
        Ok(())
    } else {
        Err(BlobError::InvalidManifest(format!(
            "id {id:?} is not a valid single key segment"
        )))
    }
}

/// Caller-side parameters for registering a blob: everything in the
/// [`Manifest`] that isn't computed from the bytes.
///
/// ```ignore
/// let spec = BlobSpec::new("report-01")
///     .filename("report.pcap")
///     .chunk_size(256 * 1024)
///     .created_ms(now_ms);
/// let manifest = server.register_file(spec, path).await?;
/// ```
#[derive(Debug, Clone)]
pub struct BlobSpec {
    pub(crate) id: String,
    pub(crate) filename: Option<String>,
    pub(crate) chunk_size: u32,
    pub(crate) created_ms: i64,
}

impl BlobSpec {
    /// Start a spec for blob `id` with the default chunk size, no advisory
    /// filename, and `created_ms = 0`.
    pub fn new(id: impl Into<String>) -> Self {
        BlobSpec {
            id: id.into(),
            filename: None,
            chunk_size: crate::chunk::DEFAULT_CHUNK_SIZE,
            created_ms: 0,
        }
    }

    /// Set the advisory filename clients may use to name the artifact.
    pub fn filename(mut self, filename: impl Into<String>) -> Self {
        self.filename = Some(filename.into());
        self
    }

    /// Set the transfer chunk size (validated at registration).
    pub fn chunk_size(mut self, bytes: u32) -> Self {
        self.chunk_size = bytes;
        self
    }

    /// Set the manifest's creation timestamp (Unix epoch milliseconds).
    pub fn created_ms(mut self, ms: i64) -> Self {
        self.created_ms = ms;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::DEFAULT_CHUNK_SIZE;

    fn manifest() -> Manifest {
        Manifest {
            version: WIRE_VERSION,
            id: "abc".into(),
            filename: Some("f.bin".into()),
            total_len: DEFAULT_CHUNK_SIZE as u64 * 2 + 100,
            chunk_size: DEFAULT_CHUNK_SIZE,
            root: Hash::of(b"whatever"),
            created_ms: 42,
        }
    }

    #[test]
    fn valid_manifest_passes() {
        let m = manifest();
        m.validate(u64::MAX).unwrap();
        assert_eq!(m.chunks().unwrap().count(), 3);
    }

    #[test]
    fn wrong_version_rejected() {
        let m = Manifest {
            version: 3,
            ..manifest()
        };
        assert!(matches!(
            m.validate(u64::MAX),
            Err(BlobError::UnsupportedVersion(3))
        ));
    }

    #[test]
    fn bad_chunk_size_rejected_not_clamped() {
        for bad in [0u32, 1024, DEFAULT_CHUNK_SIZE + 1, u32::MAX] {
            let m = Manifest {
                chunk_size: bad,
                ..manifest()
            };
            assert!(m.validate(u64::MAX).is_err(), "chunk_size {bad}");
        }
    }

    #[test]
    fn oversized_blob_rejected() {
        let m = Manifest {
            total_len: 1024 * 1024 + 1,
            ..manifest()
        };
        assert!(m.validate(1024 * 1024).is_err());
    }

    #[test]
    fn bad_ids_rejected() {
        for bad in ["", "a/b", "a*", "a?x", "..", "a b"] {
            let m = Manifest {
                id: bad.into(),
                ..manifest()
            };
            assert!(m.validate(u64::MAX).is_err(), "id {bad:?}");
        }
    }

    #[test]
    fn suggested_filename_is_traversal_safe() {
        let cases = [
            (Some("report.pcap"), Some("report.pcap")),
            (Some("/etc/passwd"), Some("passwd")),
            (Some("../../x.bin"), Some("x.bin")),
            (Some("a/b/c.txt"), Some("c.txt")),
            (Some(".."), None),
            (Some(""), None),
            (None, None),
        ];
        for (input, expected) in cases {
            let m = Manifest {
                filename: input.map(String::from),
                ..manifest()
            };
            assert_eq!(m.suggested_filename().as_deref(), expected, "{input:?}");
        }
    }
}
