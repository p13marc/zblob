//! Error type for blob transfer.

use std::path::PathBuf;

use crate::hash::Hash;

/// Errors raised by the blob server and client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlobError {
    /// A Zenoh operation failed. `zenoh::Error` is a boxed `dyn Error`, so it is
    /// flattened to a string here.
    #[error("zenoh: {0}")]
    Zenoh(String),

    /// A local I/O operation failed (reading the source, writing the `.part`).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Serializing or deserializing a control message (e.g. the manifest) failed.
    #[error("encode: {0}")]
    Encode(String),

    /// A control message declared a schema version this crate does not speak
    /// (this crate speaks [`crate::wire::WIRE_VERSION`]).
    #[error("unsupported wire version {0}")]
    UnsupportedVersion(u16),

    /// A manifest or index failed validation (bad chunk size, oversized blob,
    /// malformed id, …). Carries a human-readable reason.
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

    /// The transfer's root hash did not match the caller-pinned expectation.
    /// Nothing was written for `expected == None` flows; for pinned flows the
    /// mismatch is detected before any byte is fetched.
    #[error("integrity: root mismatch (expected {expected}, got {actual})")]
    RootMismatch {
        /// The root the caller pinned.
        expected: Hash,
        /// The root the server offered.
        actual: Hash,
    },

    /// A received chunk's content hash did not match its content-addressed key
    /// (Tier 2), or a fully-reassembled artifact failed its digest check.
    #[error("integrity: content hash mismatch")]
    HashMismatch,

    /// A `ranges` selector parameter was malformed (unsorted, overlapping,
    /// out of bounds, over the span/chunk caps, or missing the `v=2` marker).
    #[error("invalid ranges: {0}")]
    InvalidRanges(String),

    /// The server has no blob registered under the requested id (TTL expired or
    /// never existed).
    #[error("not found: {0}")]
    NotFound(String),

    /// The download could not finish within the configured retry budget. State
    /// is persisted; call `download` again to resume from the holes.
    #[error("incomplete: {received}/{total} chunks received")]
    Incomplete {
        /// Chunks received so far.
        received: u32,
        /// Total chunks expected.
        total: u32,
    },

    /// The caller cancelled (paused) the download. State is persisted to the
    /// `.part` + sidecar, so a later `download` resumes from where it stopped.
    #[error("cancelled")]
    Cancelled {
        /// Chunks received before cancellation.
        received: u32,
        /// Total chunks expected.
        total: u32,
    },

    /// The destination path already exists and the overwrite policy is
    /// [`Refuse`](crate::Overwrite::Refuse). The finished `.part` file is kept.
    #[error("destination exists: {0}")]
    DestinationExists(PathBuf),

    /// The server refused an upload offer (no push configured, policy said
    /// no, or a conflicting push for the same id is in progress).
    #[error("push denied: {0}")]
    PushDenied(String),

    /// A generic protocol violation (malformed key, bad selector, …).
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Convenience alias for fallible blob operations.
pub type Result<T> = std::result::Result<T, BlobError>;

impl BlobError {
    /// Map a `zenoh::Error` (a boxed `dyn Error + Send + Sync`) into [`BlobError::Zenoh`].
    pub(crate) fn zenoh(e: impl std::fmt::Display) -> Self {
        BlobError::Zenoh(e.to_string())
    }

    /// Map a serde error into [`BlobError::Encode`].
    pub(crate) fn encode(e: impl std::fmt::Display) -> Self {
        BlobError::Encode(e.to_string())
    }
}
