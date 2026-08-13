//! Observability: optional `tracing` instrumentation and transfer statistics.
//!
//! With the `tracing` cargo feature enabled, the crate emits `tracing` events
//! at the load-bearing points (registration, query serving, download
//! lifecycle, retries, rejected slices, GC sweeps). Without it, the macros
//! compile to nothing — no dependency, no overhead.

use std::time::Duration;

/// Borrow every field value of a `zdebug!`/`zwarn!` invocation and discard it.
///
/// Without this, a binding whose only reader is a log line is *unused* in a
/// build without the `tracing` feature — so the crate compiles with
/// `--all-features` and fails with `-D warnings` on the default set. That is
/// the exact local-versus-CI divergence `CLAUDE.md` warns about, and it is
/// better fixed once here than worked around at each call site.
///
/// It accepts the subset of `tracing`'s field syntax this crate uses:
/// `name = ?expr`, `name = %expr`, `name = expr`, a bare `ident`, and a
/// trailing message literal. Values are only *borrowed*, never formatted, so
/// enabling the feature cannot change what the code does.
#[cfg(not(feature = "tracing"))]
macro_rules! zignore {
    () => {};
    ($msg:literal $(,)?) => {};
    ($name:ident = ?$val:expr $(, $($rest:tt)*)?) => {{
        let _ = &$val;
        $crate::obs::zignore!($($($rest)*)?);
    }};
    ($name:ident = %$val:expr $(, $($rest:tt)*)?) => {{
        let _ = &$val;
        $crate::obs::zignore!($($($rest)*)?);
    }};
    ($name:ident = $val:expr $(, $($rest:tt)*)?) => {{
        let _ = &$val;
        $crate::obs::zignore!($($($rest)*)?);
    }};
    ($name:ident $(, $($rest:tt)*)?) => {{
        let _ = &$name;
        $crate::obs::zignore!($($($rest)*)?);
    }};
}

/// `tracing::debug!` when the `tracing` feature is on; nothing otherwise.
macro_rules! zdebug {
    ($($t:tt)*) => {{
        #[cfg(feature = "tracing")]
        tracing::debug!($($t)*);
        #[cfg(not(feature = "tracing"))]
        $crate::obs::zignore!($($t)*);
    }};
}

/// `tracing::warn!` when the `tracing` feature is on; nothing otherwise.
macro_rules! zwarn {
    ($($t:tt)*) => {{
        #[cfg(feature = "tracing")]
        tracing::warn!($($t)*);
        #[cfg(not(feature = "tracing"))]
        $crate::obs::zignore!($($t)*);
    }};
}

#[cfg(not(feature = "tracing"))]
pub(crate) use zignore;
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
    /// Zenoh queries this call issued.
    ///
    /// The number that matters for scale: a tier-2 snapshot used to cost one
    /// query per chunk, so this is how you tell whether batching is actually
    /// working against a given fleet rather than silently falling back to
    /// single fetches.
    pub queries: u64,
    /// Wall-clock duration of this call.
    pub elapsed: Duration,
}

/// Adding stats sums every counter and the elapsed time.
///
/// A caller that fetches a snapshot in several calls — a resume, or a tree
/// download after a probe — has no other way to report the whole operation,
/// and was left summing seven fields by hand.
impl std::ops::AddAssign<&TransferStats> for TransferStats {
    fn add_assign(&mut self, rhs: &TransferStats) {
        self.bytes_fetched += rhs.bytes_fetched;
        self.chunks_fetched += rhs.chunks_fetched;
        self.chunks_resumed += rhs.chunks_resumed;
        self.rejected += rhs.rejected;
        self.retries += rhs.retries;
        self.queries += rhs.queries;
        self.elapsed += rhs.elapsed;
    }
}

impl std::ops::AddAssign for TransferStats {
    fn add_assign(&mut self, rhs: TransferStats) {
        *self += &rhs;
    }
}

impl std::ops::Add for TransferStats {
    type Output = TransferStats;
    fn add(mut self, rhs: TransferStats) -> TransferStats {
        self += &rhs;
        self
    }
}

impl std::iter::Sum for TransferStats {
    fn sum<I: Iterator<Item = TransferStats>>(iter: I) -> TransferStats {
        iter.fold(TransferStats::default(), |acc, s| acc + s)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(bytes: u64, secs: f64) -> TransferStats {
        TransferStats {
            bytes_fetched: bytes,
            chunks_fetched: 2,
            chunks_resumed: 3,
            rejected: 4,
            retries: 5,
            queries: 6,
            elapsed: Duration::from_secs_f64(secs),
        }
    }

    #[test]
    fn throughput_is_bytes_over_seconds_and_never_divides_by_zero() {
        assert_eq!(stats(1_000_000, 2.0).throughput_bps(), 500_000);
        // A transfer that took no measurable time reports 0 rather than
        // panicking or reporting infinity — the case a fully-resumed transfer
        // hits every time.
        assert_eq!(stats(1_000_000, 0.0).throughput_bps(), 0);
        assert_eq!(TransferStats::default().throughput_bps(), 0);
    }

    #[test]
    fn summing_stats_sums_every_field() {
        let a = stats(100, 1.0);
        let b = stats(200, 2.0);

        let mut acc = a.clone();
        acc += &b;
        assert_eq!(acc.bytes_fetched, 300);
        assert_eq!(acc.chunks_fetched, 4);
        assert_eq!(acc.chunks_resumed, 6);
        assert_eq!(acc.rejected, 8);
        assert_eq!(acc.retries, 10);
        assert_eq!(acc.queries, 12);
        assert_eq!(acc.elapsed, Duration::from_secs(3));

        // The three other shapes must agree with `AddAssign`, or a caller
        // picking one gets a different answer from the same numbers.
        assert_eq!(a.clone() + b.clone(), acc);
        let mut owned = a.clone();
        owned += b.clone();
        assert_eq!(owned, acc);
        assert_eq!([a, b].into_iter().sum::<TransferStats>(), acc);

        // The identity, which is what makes `sum()` over an empty run correct.
        let z = TransferStats::default();
        assert_eq!(z.clone() + z.clone(), z);
        assert_eq!(
            std::iter::empty::<TransferStats>().sum::<TransferStats>(),
            z
        );
    }
}
