//! Observability: optional `tracing` instrumentation and transfer statistics.
//!
//! With the `tracing` cargo feature enabled, the crate emits `tracing` events
//! at the load-bearing points (registration, query serving, download
//! lifecycle, retries, rejected slices, GC sweeps). Without it, the macros
//! compile to nothing — no dependency, no overhead.

use std::time::Duration;

/// `tracing::debug!` when the `tracing` feature is on; nothing otherwise.
macro_rules! zdebug {
    ($($t:tt)*) => {
        #[cfg(feature = "tracing")]
        tracing::debug!($($t)*);
    };
}

/// `tracing::warn!` when the `tracing` feature is on; nothing otherwise.
macro_rules! zwarn {
    ($($t:tt)*) => {
        #[cfg(feature = "tracing")]
        tracing::warn!($($t)*);
    };
}

pub(crate) use {zdebug, zwarn};

/// Statistics for one completed (or resumed-to-completion) transfer, returned
/// by [`BlobClient::download_to`](crate::BlobClient::download_to) and
/// [`TreeClient::download_tree`](crate::TreeClient::download_tree).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransferStats {
    /// Verified payload bytes fetched over the network by this call
    /// (excludes chunks already present from an earlier attempt).
    pub bytes_fetched: u64,
    /// Chunks fetched and verified by this call.
    pub chunks_fetched: u32,
    /// Chunks already present when this call started (resume head start).
    pub chunks_resumed: u32,
    /// Replies dropped by verification (tampered/undecodable slices or
    /// wrong-content chunks). Nonzero means a bad or corrupt replier.
    pub rejected: u32,
    /// No-progress query attempts that were retried with backoff.
    pub retries: u32,
    /// Wall-clock duration of this call.
    pub elapsed: Duration,
}

impl TransferStats {
    /// Average fetch throughput in bytes/second (0 if nothing was fetched).
    pub fn throughput_bps(&self) -> u64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= f64::EPSILON {
            return 0;
        }
        (self.bytes_fetched as f64 / secs) as u64
    }
}
