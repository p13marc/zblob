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

/// A [`ProgressSink`] that forwards events onto a channel, from
/// [`progress_channel`].
///
/// Sending never blocks and never fails the transfer: if the receiver is gone
/// or the buffer is full, the event is dropped. Progress is advisory, and a
/// slow UI must not be able to stall a download — which is the reason this
/// exists as a type rather than as advice to write the closure yourself, since
/// the obvious closure either blocks (`send().await` from a sync `emit`) or
/// panics on a closed receiver.
#[derive(Debug, Clone)]
pub struct ChannelSink(tokio::sync::mpsc::Sender<Progress>);

impl ProgressSink for ChannelSink {
    fn emit(&self, progress: Progress) {
        let _ = self.0.try_send(progress);
    }
}

/// A [`ProgressSink`] and the receiver its events arrive on.
///
/// Both consumers of this crate are GUIs, and both wrote the same adapter:
/// progress arrives on a synchronous `emit` from inside the transfer, and has
/// to reach a widget that lives on another task.
///
/// ```
/// # use zblob::{progress_channel, Progress, ProgressSink};
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let (sink, mut events) = progress_channel(64);
/// sink.emit(Progress::Verifying);
/// drop(sink);
///
/// while let Some(event) = events.recv().await {
///     // update the UI
///     assert!(matches!(event, Progress::Verifying));
/// }
/// # }
/// ```
///
/// `buffer` bounds how far behind the reader may fall before events start
/// being dropped; 64 is plenty for a UI that repaints on each one, since every
/// event carries absolute counts rather than deltas — a dropped `Chunk` costs
/// a repaint, not a wrong total.
#[must_use]
pub fn progress_channel(buffer: usize) -> (ChannelSink, tokio::sync::mpsc::Receiver<Progress>) {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer.max(1));
    (ChannelSink(tx), rx)
}
