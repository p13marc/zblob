//! A cheap, cloneable cancellation token for pausing/cancelling a transfer.
//!
//! Pause and cancel are the same mechanism at the transport layer: stop fetching
//! and leave the `.part` + sidecar on disk. The *caller* decides what it means —
//! a paused transfer keeps the partial and resumes later (the normal resume
//! path); a cancelled transfer additionally deletes the partial.
//!
//! # Why this is not an `AtomicBool`
//!
//! It was one until 0.3, and the flag itself worked — but a flag can only be
//! *polled*, and every poll in this crate sat after a blocking receive:
//!
//! ```ignore
//! while let Ok(reply) = replies.recv_async().await {
//!     if cancel.is_cancelled() { .. }   // never reached until a reply arrives
//! ```
//!
//! So the observed latency of `cancel()` was not "after the current chunk", as
//! documented, but *until the next reply or the query timeout* — 30 s by
//! default, and exactly 30 s in the case that matters most, a transfer stalled
//! because the peer went away. A token that can be `select!`ed on is what makes
//! the documented behaviour true; [`CancelToken::until_cancelled`] is how the
//! fetch loops use it.

/// A shared cancellation token. Clone it freely; cancelling any clone signals
/// every in-flight transfer holding one to stop as soon as it can — without
/// waiting for a reply that may never come.
///
/// Cancellation is one-way and idempotent: a cancelled token stays cancelled.
#[derive(Clone, Default, Debug)]
pub struct CancelToken(tokio_util::sync::CancellationToken);

impl CancelToken {
    /// A fresh, un-cancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal cancellation. Idempotent, and callable from any task or thread.
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// Whether cancellation was requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Resolves as soon as cancellation is requested (immediately if it
    /// already was). Cancel-safe, so it can be raced in a `select!`.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }

    /// Run `fut` to completion, giving up the moment cancellation is
    /// requested: `Some(output)` if it finished, `None` if it was cancelled.
    ///
    /// This is how every receive loop in the crate waits, so a cancel is
    /// observed while waiting rather than after.
    ///
    /// ```
    /// # use zblob::CancelToken;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let cancel = CancelToken::new();
    /// assert_eq!(cancel.until_cancelled(async { 7 }).await, Some(7));
    ///
    /// cancel.cancel();
    /// // Would otherwise sleep for an hour.
    /// let slept = cancel
    ///     .until_cancelled(tokio::time::sleep(std::time::Duration::from_secs(3600)))
    ///     .await;
    /// assert!(slept.is_none());
    /// # }
    /// ```
    pub async fn until_cancelled<F: std::future::Future>(&self, fut: F) -> Option<F::Output> {
        self.0.run_until_cancelled(fut).await
    }
}

impl From<tokio_util::sync::CancellationToken> for CancelToken {
    fn from(t: tokio_util::sync::CancellationToken) -> Self {
        Self(t)
    }
}

impl From<CancelToken> for tokio_util::sync::CancellationToken {
    fn from(t: CancelToken) -> Self {
        t.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_state_across_clones() {
        let a = CancelToken::new();
        let b = a.clone();
        assert!(!b.is_cancelled());
        a.cancel();
        assert!(b.is_cancelled());
    }

    /// The property the `AtomicBool` could not provide: a wait that is already
    /// pending when `cancel()` lands still returns promptly.
    #[tokio::test(start_paused = true)]
    async fn a_pending_wait_is_interrupted_not_polled() {
        let token = CancelToken::new();
        let t = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            t.cancel();
        });
        // The inner future never completes; only cancellation can end this.
        let out = token.until_cancelled(std::future::pending::<()>()).await;
        assert!(out.is_none());
    }

    /// Discriminating power for the test above: without a cancel, the same
    /// call does *not* return early.
    #[tokio::test(start_paused = true)]
    async fn an_uncancelled_wait_yields_the_value() {
        let token = CancelToken::new();
        let out = token
            .until_cancelled(async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                "done"
            })
            .await;
        assert_eq!(out, Some("done"));
    }
}
