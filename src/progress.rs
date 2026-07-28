//! Download progress events.

use std::path::PathBuf;

/// A progress event emitted by a download.
///
/// `#[non_exhaustive]`: match with a wildcard arm — later releases may add
/// variants (e.g. rate/ETA reporting) without a breaking change.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Progress {
    /// A fresh transfer began; the manifest arrived and sizing is known.
    Started {
        /// Total blob length in bytes.
        total_len: u64,
        /// Total number of transfer chunks.
        chunk_count: u32,
    },
    /// An interrupted transfer resumed from persisted state.
    Resumed {
        /// Chunks already present before this attempt.
        received: u32,
        /// Total chunks expected.
        total: u32,
    },
    /// A chunk was verified and written to the destination.
    Chunk {
        /// Index of the chunk just written.
        index: u32,
        /// How many distinct chunks are present so far.
        received: u32,
        /// Total chunks expected.
        total: u32,
        /// Verified payload bytes on disk so far (excludes duplicates).
        bytes_received: u64,
    },
    /// All data is present; running a final integrity/materialization step
    /// (Tier-2 tree reconstruction; Tier-1 completes without a second pass —
    /// every byte was verified against the root as it was written).
    Verifying,
    /// The download finished and verified; the artifact is at `path`.
    Completed {
        /// Final path of the assembled, verified artifact.
        path: PathBuf,
    },
    /// The caller cancelled the download; state was persisted for resume.
    Cancelled {
        /// Chunks received before cancellation.
        received: u32,
        /// Total chunks expected.
        total: u32,
    },
    /// The download failed (cancellation is *not* a failure — see
    /// [`Progress::Cancelled`]).
    Failed {
        /// Human-readable reason.
        error: String,
    },
}

/// A sink for [`Progress`] events. Implemented for any `Fn(Progress)` and for
/// `()` (a no-op), so callers can pass a closure or nothing.
pub trait ProgressSink: Send + Sync {
    /// Receive one progress event.
    fn emit(&self, progress: Progress);
}

impl<F: Fn(Progress) + Send + Sync> ProgressSink for F {
    fn emit(&self, progress: Progress) {
        self(progress)
    }
}

impl ProgressSink for () {
    fn emit(&self, _progress: Progress) {}
}
