//! Error type for blob transfer.

use std::path::PathBuf;

use crate::hash::Hash;

/// Errors raised by the blob server and client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlobError {
    /// A Zenoh operation failed.
    ///
    /// `zenoh::Error` is `Box<dyn Error + Send + Sync>`, so it is kept whole
    /// rather than flattened: the cause chain is what tells a connection
    /// refusal apart from a closed session, and stringifying discarded it.
    #[error("zenoh: {0}")]
    Zenoh(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// A local I/O operation failed (reading the source, writing the `.part`).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Serializing or deserializing a control message (e.g. the manifest)
    /// failed.
    ///
    /// On a *receive* path this is a peer's fault, not this crate's: postcard
    /// is positional, so a peer speaking a different schema shape produces
    /// exactly this. Such a reply is skipped, never fatal.
    #[error("encode: {0}")]
    Encode(#[from] postcard::Error),

    /// A blocking task panicked or was cancelled.
    ///
    /// **Not a protocol failure.** These used to be stringified into
    /// [`Protocol`](Self::Protocol) — 24 sites of it — which made a local
    /// panic indistinguishable from a peer sending something malformed. They
    /// call for opposite responses: one is a bug here, the other is a bad
    /// peer to skip.
    #[error("background task failed: {0}")]
    Task(#[from] tokio::task::JoinError),

    /// A control message declared a schema version this crate does not speak
    /// (this crate speaks [`crate::wire::WIRE_VERSION`]).
    #[error("unsupported wire version {0}")]
    UnsupportedVersion(u16),

    /// A manifest or index failed validation (bad chunk size, oversized blob,
    /// malformed id, …). Carries a human-readable reason.
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

    /// The transfer's root hash did not match the caller-pinned expectation.
    ///
    /// On the download paths this is detected before any byte is fetched. On
    /// the *upload* path it is not: `finalize_push` spools the whole blob
    /// before checking, so a rejected push has already written its spool file
    /// (which the server then discards).
    #[error("integrity: root mismatch (expected {expected}, got {actual})")]
    RootMismatch {
        /// The root the caller pinned.
        expected: Hash,
        /// The root the server offered.
        actual: Hash,
    },

    /// A [`ContentStore`](crate::ContentStore) returned bytes that do not hash
    /// to the address they were stored under.
    ///
    /// Chunks are verified when fetched, so this means the store itself is
    /// wrong: local corruption between fetch and materialization, or an
    /// implementation that broke the `has`/`get` contract. The snapshot's
    /// `root_hash` cannot catch it — that covers the entry list, not chunk
    /// contents. Re-running the download after removing the chunk heals it;
    /// [`DirStore::with_verify_on_read`](crate::DirStore::with_verify_on_read)
    /// and [`DirStore::scrub`](crate::DirStore::scrub) do the removal.
    #[error("integrity: store returned corrupt bytes for chunk {hash}")]
    CorruptStore {
        /// The address whose contents did not match.
        hash: Hash,
    },

    /// A stored chunk's length disagreed with the length its index declared.
    ///
    /// Distinct from a content mismatch, which cannot reach this point: chunks
    /// are verified against their content address the moment they are fetched
    /// (a mismatching reply is skipped and the fetch waits for an honest one).
    /// This fires when a [`ContentStore`](crate::ContentStore) hands back
    /// something other than what was put in it.
    #[error("integrity: chunk length {actual} does not match the declared {expected}")]
    ChunkLengthMismatch {
        /// The length the index declared.
        expected: u32,
        /// The length the store returned.
        actual: u32,
    },

    /// A `ranges` selector parameter was malformed (unsorted, overlapping,
    /// out of bounds, or over the span/chunk caps).
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

    /// A path in a directory index was unsafe or unrepresentable: absolute,
    /// traversing, reserved on the target platform, a duplicate, or a symlink
    /// escaping the destination.
    ///
    /// Separate from [`Protocol`](Self::Protocol) because it is the one
    /// rejection an operator usually wants to see: it means a snapshot tried
    /// to write outside the tree it was given.
    #[error("unsafe path: {0}")]
    UnsafePath(String),

    /// A key prefix could not be used as a Zenoh key expression (empty,
    /// wildcarded where it must not be, or undeclarable).
    ///
    /// Always a local configuration mistake, known at construction time —
    /// never a peer's doing and never worth retrying, which is why it does not
    /// share a variant with the two that are.
    #[error("invalid prefix: {0}")]
    InvalidPrefix(String),

    /// A control message off the wire failed validation (bad want list,
    /// oversized descriptor, malformed chunk container, …).
    ///
    /// This is a *peer's* fault. Every fetch loop treats it as one responder's
    /// problem and keeps waiting for an honest reply (see the crate docs,
    /// fact 3) — which is only possible because it is distinguishable from a
    /// local failure.
    #[error("malformed message: {0}")]
    MalformedMessage(String),

    /// A published snapshot did not become readable back from a router
    /// storage within the settle budget.
    ///
    /// Retriable, and its own variant for that reason: settling is a race
    /// against a storage's own write path, so "it was not there yet" is a
    /// timing answer, not a verdict.
    #[error("publish did not settle: {0}")]
    NotSettled(String),

    /// The caller asked for something this crate cannot do: a malformed tag
    /// name, an unusable argument. Distinct from the peer-fault variants
    /// because no amount of retrying or of finding a better peer helps.
    #[error("usage: {0}")]
    Usage(String),

    /// A protocol violation that fits none of the above (malformed key, bad
    /// selector, …).
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Coarse classification of a [`BlobError`], from [`BlobError::kind`].
///
/// The variants of `BlobError` say *what* went wrong; this says what a caller
/// can do about it. Callers were matching a dozen variants to answer "retry or
/// give up" — including this crate, which spelled
/// `matches!(e, BlobError::Cancelled { .. })` in three places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The transport or a peer let us down: nobody answered, not enough
    /// replies arrived, the session failed. Retrying can work.
    Transport,
    /// Local I/O failed — including a [`ContentStore`](crate::ContentStore)
    /// that could not be read or written.
    Io,
    /// The caller asked for something impossible: a malformed id, an unusable
    /// prefix, a destination that already exists. Retrying cannot help.
    Usage,
    /// A peer sent something unusable. The transfer can still succeed if an
    /// honest peer answers.
    Protocol,
    /// Bytes did not match the hash they were promised under. Never retried
    /// silently: something is wrong with the data or with a peer.
    Integrity,
    /// The caller cancelled. State is persisted where the operation supports
    /// resume.
    Cancelled,
    /// A bug in this crate: a background task panicked.
    Internal,
}

/// Convenience alias for fallible blob operations.
pub type Result<T> = std::result::Result<T, BlobError>;

impl BlobError {
    /// Map a `zenoh::Error` (a boxed `dyn Error + Send + Sync`) into
    /// [`BlobError::Zenoh`], keeping it whole.
    pub(crate) fn zenoh(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        BlobError::Zenoh(e.into())
    }

    /// What a caller can do about this error. See [`ErrorKind`].
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            BlobError::Zenoh(_)
            | BlobError::NotFound(_)
            | BlobError::NotSettled(_)
            | BlobError::Incomplete { .. } => ErrorKind::Transport,
            BlobError::Io(_) => ErrorKind::Io,
            BlobError::Task(_) => ErrorKind::Internal,
            BlobError::Cancelled { .. } => ErrorKind::Cancelled,
            BlobError::RootMismatch { .. }
            | BlobError::CorruptStore { .. }
            | BlobError::ChunkLengthMismatch { .. } => ErrorKind::Integrity,
            BlobError::InvalidPrefix(_)
            | BlobError::Usage(_)
            | BlobError::DestinationExists(_)
            | BlobError::InvalidRanges(_) => ErrorKind::Usage,
            BlobError::Encode(_)
            | BlobError::UnsupportedVersion(_)
            | BlobError::InvalidManifest(_)
            | BlobError::MalformedMessage(_)
            | BlobError::UnsafePath(_)
            | BlobError::PushDenied(_)
            | BlobError::Protocol(_) => ErrorKind::Protocol,
        }
    }

    /// Whether repeating the same call could plausibly succeed.
    ///
    /// True for transport failures — including
    /// [`Incomplete`](Self::Incomplete), which is the *resumable* one: its
    /// state is on disk, so a repeat continues from the holes rather than
    /// starting over. False for cancellation: a cancel is a decision, and
    /// retrying it in a loop is how a paused transfer becomes an unpausable
    /// one.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        self.kind() == ErrorKind::Transport
    }

    /// Whether this is the caller's own cancellation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(self, BlobError::Cancelled { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant must classify, and the classification must be the one a
    /// caller would act on. Written as a table so a new variant that nobody
    /// classified shows up as a compile error in `kind()` rather than as a
    /// silent `ErrorKind::Protocol`.
    #[test]
    fn every_variant_classifies_the_way_a_caller_would_act() {
        let h = Hash::of(b"x");
        let cases: Vec<(BlobError, ErrorKind)> = vec![
            (
                BlobError::Zenoh("session closed".into()),
                ErrorKind::Transport,
            ),
            (BlobError::NotFound("id".into()), ErrorKind::Transport),
            (BlobError::NotSettled("key".into()), ErrorKind::Transport),
            (
                BlobError::Incomplete {
                    received: 1,
                    total: 2,
                },
                ErrorKind::Transport,
            ),
            (BlobError::Io(std::io::Error::other("disk")), ErrorKind::Io),
            (
                BlobError::Cancelled {
                    received: 1,
                    total: 2,
                },
                ErrorKind::Cancelled,
            ),
            (
                BlobError::RootMismatch {
                    expected: h,
                    actual: h,
                },
                ErrorKind::Integrity,
            ),
            (BlobError::CorruptStore { hash: h }, ErrorKind::Integrity),
            (
                BlobError::ChunkLengthMismatch {
                    expected: 1,
                    actual: 2,
                },
                ErrorKind::Integrity,
            ),
            (BlobError::InvalidPrefix("**".into()), ErrorKind::Usage),
            (BlobError::Usage("bad tag".into()), ErrorKind::Usage),
            (
                BlobError::DestinationExists("/tmp/x".into()),
                ErrorKind::Usage,
            ),
            (BlobError::InvalidRanges("0-".into()), ErrorKind::Usage),
            (BlobError::UnsupportedVersion(9), ErrorKind::Protocol),
            (BlobError::InvalidManifest("m".into()), ErrorKind::Protocol),
            (BlobError::MalformedMessage("w".into()), ErrorKind::Protocol),
            (BlobError::UnsafePath("../x".into()), ErrorKind::Protocol),
            (BlobError::PushDenied("no".into()), ErrorKind::Protocol),
            (BlobError::Protocol("?".into()), ErrorKind::Protocol),
        ];

        for (err, want) in &cases {
            assert_eq!(err.kind(), *want, "{err}");
            assert_eq!(
                err.is_retriable(),
                *want == ErrorKind::Transport,
                "is_retriable disagrees with kind for {err}"
            );
            assert_eq!(
                err.is_cancelled(),
                *want == ErrorKind::Cancelled,
                "is_cancelled disagrees with kind for {err}"
            );
        }

        // Discriminating power: the table must not be all one kind, or every
        // assertion above holds vacuously.
        let kinds: std::collections::HashSet<ErrorKind> =
            cases.iter().map(|(e, _)| e.kind()).collect();
        assert!(kinds.len() >= 6, "the table exercises only {kinds:?}");
    }

    /// The point of keeping `zenoh::Error` boxed rather than stringified: the
    /// cause survives to `source()`, where an operator's error reporter finds
    /// it.
    #[test]
    fn a_zenoh_error_keeps_its_cause() {
        use std::error::Error as _;

        let cause = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "no route");
        let err = BlobError::zenoh(Box::new(cause) as Box<dyn std::error::Error + Send + Sync>);
        assert_eq!(err.to_string(), "zenoh: no route");
        let source = err.source().expect("the cause must survive");
        assert!(source.to_string().contains("no route"));
    }

    /// A `JoinError` is a bug here, not a peer's doing — the distinction the
    /// old `Protocol(format!("...join: {e}"))` could not make.
    #[tokio::test]
    async fn a_panicking_task_is_internal_not_protocol() {
        let join = tokio::spawn(async { panic!("boom") }).await.unwrap_err();
        let err = BlobError::from(join);
        assert_eq!(err.kind(), ErrorKind::Internal);
        assert!(!err.is_retriable());
    }
}
