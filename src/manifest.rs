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
use crate::id::BlobId;
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
    /// must be a single, non-empty key segment — which [`BlobId`] enforces at
    /// deserialization, not at validation time.
    pub id: BlobId,
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
    /// Trailing extension list — see [`wire::Ext`](crate::wire::Ext).
    ///
    /// **Always last.** postcard is positional, so a trailing extension list is
    /// the only place a field can be added without a wire break; putting it
    /// anywhere else would defeat its own purpose.
    ///
    /// A server advertises its limits here (see
    /// [`max_chunks_per_query`](Self::max_chunks_per_query)), which is what
    /// lets a client with a larger default clamp instead of having its queries
    /// rejected with no way to discover why.
    pub ext: crate::wire::Ext,
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

    /// The server's advertised `max_chunks_per_query`, if it said.
    ///
    /// Advertisement, not negotiation: one field, no handshake, no round trip.
    /// Both sides defaulted to 512 and neither could tell the other, so a
    /// server that lowered its cap rejected every existing client's queries
    /// with `InvalidRanges` and nothing to explain it. Documented behaviour is
    /// not a protocol.
    pub fn max_chunks_per_query(&self) -> Option<u32> {
        self.ext.get_u32(crate::wire::EXT_MAX_CHUNKS_PER_QUERY)
    }

    /// The server's advertised `max_blob_size`, if it said.
    pub fn max_blob_size(&self) -> Option<u64> {
        self.ext.get_u64(crate::wire::EXT_MAX_BLOB_SIZE)
    }

    /// How many transfer chunks this manifest describes.
    ///
    /// v2 removed a stored `chunk_count` deliberately: a second source of
    /// truth is something a hostile peer can make disagree with the first, so
    /// the count is *derived* from `total_len` and `chunk_size`. Deriving it
    /// is right; making every consumer re-derive it is not — the expression
    /// was being rewritten by hand in two separate downstream crates, against
    /// geometry this crate defines.
    pub fn chunk_count(&self) -> Result<u32> {
        Ok(self.chunks()?.count())
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

/// Validate a bare `&str` id (see [`BlobId`], which is the same rules as a
/// type). Used where an id arrives as a caller argument rather than in a
/// decoded message.
pub(crate) fn validate_id(id: &str) -> Result<()> {
    BlobId::new(id).map(|_| ())
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
            id: BlobId::new("abc").unwrap(),
            filename: Some("f.bin".into()),
            total_len: DEFAULT_CHUNK_SIZE as u64 * 2 + 100,
            chunk_size: DEFAULT_CHUNK_SIZE,
            root: Hash::of(b"whatever"),
            created_ms: 42,
            ext: crate::wire::Ext::new(),
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
        // Any version but ours, in either direction: a v2 peer and a
        // hypothetical v4 one are equally unusable, and must say so rather
        // than half-decode.
        for v in [2u16, 4] {
            let m = Manifest {
                version: v,
                ..manifest()
            };
            assert!(
                matches!(m.validate(u64::MAX), Err(BlobError::UnsupportedVersion(got)) if got == v),
                "version {v} should be refused"
            );
        }
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

    /// A hostile id is refused by *decoding*, one step earlier than
    /// `validate()` — which is the point of [`BlobId`]: nothing has to
    /// remember to call anything.
    ///
    /// The message is built as a positional tuple of the manifest's field
    /// types, which postcard encodes byte-for-byte like the struct. That is
    /// also the documented way for an adversarial test to send a manifest a
    /// `BlobId` could not hold, so this exercises the escape hatch too.
    #[test]
    fn bad_ids_are_refused_at_decode_not_at_validate() {
        let m = manifest();
        let encode_with = |id: &str| {
            crate::wire::encode(&(
                m.version,
                id.to_string(),
                m.filename.clone(),
                m.total_len,
                m.chunk_size,
                m.root,
                m.created_ms,
                m.ext.clone(),
            ))
            .unwrap()
        };

        for bad in [
            "",
            "a/b",
            "a*",
            "a?x",
            "..",
            "a b",
            "@verbatim",
            "a\\b",
            "@",
        ] {
            assert!(
                crate::wire::decode::<Manifest>(&encode_with(bad)).is_err(),
                "id {bad:?} decoded"
            );
        }

        // Discriminating power: the identical construction with a legal id
        // decodes to the manifest it was built from, so the rejections above
        // are about the id and not about the hand-rolled framing.
        let decoded = crate::wire::decode::<Manifest>(&encode_with("good-id")).unwrap();
        assert_eq!(decoded.id, "good-id");
        assert_eq!(decoded.root, m.root);
    }

    /// The `@` rule exists because Zenoh's `**` does not match verbatim
    /// segments — assert the actual matching behaviour so the rule cannot be
    /// "simplified away" by someone who doesn't know why it is there.
    #[test]
    fn leading_at_ids_would_be_unservable() {
        let pattern: zenoh::key_expr::KeyExpr = "p/**".try_into().unwrap();
        let verbatim: zenoh::key_expr::KeyExpr = "p/@id/manifest".try_into().unwrap();
        let ordinary: zenoh::key_expr::KeyExpr = "p/x@y/manifest".try_into().unwrap();
        assert!(
            !pattern.intersects(&verbatim),
            "if this ever passes, the leading-@ id rule can be relaxed"
        );
        assert!(pattern.intersects(&ordinary));
        assert!(validate_id("@id").is_err());
        assert!(validate_id("x@y").is_ok());
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
