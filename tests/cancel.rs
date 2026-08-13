//! Pause/cancel: a cancelled download persists its partial and resumes; a
//! deleted partial starts over from scratch.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    MemoryBlobSource, Progress, ProgressSink, RetryPolicy,
};

fn test_client(session: Arc<zenoh::Session>, prefix: &str) -> BlobClient {
    BlobClient::builder(session, common::query(prefix))
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        })
        .build()
}

/// Cancels the token after the first chunk is written.
struct CancelAfterFirst {
    token: CancelToken,
}
impl ProgressSink for CancelAfterFirst {
    fn emit(&self, p: Progress) {
        if let Progress::Chunk { received, .. } = p
            && received >= 1
        {
            self.token.cancel();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_persists_then_resumes() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("d.bin");

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 8, 0xC0FFEE);
    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));
    server
        .register_source(
            BlobSpec::new("blob-x").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = test_client(session.clone(), &prefix);

    // Cancel mid-transfer → Cancelled, partial persisted.
    let token = CancelToken::new();
    let sink = CancelAfterFirst {
        token: token.clone(),
    };
    let err = client
        .download_to(&DownloadRequest::new("blob-x"), &dest, &sink, &token)
        .await
        .expect_err("must cancel");
    assert!(matches!(err, BlobError::Cancelled { .. }), "{err}");
    assert!(dir.path().join("d.bin.part").exists());
    assert!(dir.path().join("d.bin.part.meta").exists());

    // Resume (fresh token) → completes + verifies.
    tokio::time::timeout(
        Duration::from_secs(20),
        client.download_to(
            &DownloadRequest::new("blob-x"),
            &dest,
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("resume timed out")
    .expect("resume failed");
    assert_eq!(
        content_hash(&std::fs::read(&dest).unwrap()),
        content_hash(&data)
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_partial_clears_state() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("d.bin");

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 0xBEEF);
    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));
    server
        .register_source(
            BlobSpec::new("blob-y").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data)),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = test_client(session.clone(), &prefix);
    let token = CancelToken::new();
    let sink = CancelAfterFirst {
        token: token.clone(),
    };
    let _ = client
        .download_to(&DownloadRequest::new("blob-y"), &dest, &sink, &token)
        .await;
    assert!(dir.path().join("d.bin.part").exists());

    client.delete_partial(&dest).await;
    assert!(!dir.path().join("d.bin.part").exists());
    assert!(!dir.path().join("d.bin.part.meta").exists());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A cancel must be observed *while waiting on the network*, not only between
/// replies — the failure this asserts against had `is_cancelled()` checked
/// only after `recv_async().await`, so a stalled peer made the observed
/// latency of `cancel()` the query timeout rather than "as soon as it can".
///
/// The peer here is the worst realistic case and the one the old code handled
/// worst: it answers the manifest, then accepts every slice query and never
/// replies to any of them, holding each open. Zenoh finalizes a query only
/// once every matching queryable has completed, so the client genuinely waits
/// out the full timeout.
///
/// The assertion is a ratio of the client's own configured timeout, not a wall
/// clock constant: under the old behaviour this returned in ~5 s (the timeout),
/// under the new one in milliseconds. Anything under a fifth of the timeout
/// can only be the token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_is_observed_while_stalled_on_a_silent_peer() {
    use zblob::wire::{self, ENC_MANIFEST};
    use zblob::{Manifest, manifest_key};

    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("stalled.bin");

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 8, 0xDEAD);
    let manifest = Manifest {
        version: wire::WIRE_VERSION,
        id: "stalled".into(),
        filename: None,
        total_len: data.len() as u64,
        chunk_size: MIN_CHUNK_SIZE,
        root: content_hash(&data),
        created_ms: 0,
        ext: Vec::new(),
    };

    let q = session
        .declare_queryable(format!("{prefix}/**"))
        .await
        .unwrap();
    let srv_prefix = prefix.clone();
    let peer = tokio::spawn(async move {
        // Queries are kept alive, never answered and never dropped: dropping
        // one would complete it and let the client's `get` finalize early.
        let mut held = Vec::new();
        while let Ok(query) = q.recv_async().await {
            if query.key_expr().as_str().ends_with("/manifest") {
                let _ = query
                    .reply(
                        manifest_key(&srv_prefix, "stalled"),
                        wire::encode(&manifest).unwrap(),
                    )
                    .encoding(ENC_MANIFEST)
                    .await;
            } else {
                held.push(query);
            }
        }
    });

    let timeout = Duration::from_secs(5);
    let client = BlobClient::builder(session.clone(), common::query(&prefix))
        .query_timeout(timeout)
        .build();

    let token = CancelToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        t.cancel();
    });

    let started = std::time::Instant::now();
    let err = client
        .download_to(&DownloadRequest::new("stalled"), &dest, &(), &token)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    assert!(
        matches!(err, BlobError::Cancelled { .. }),
        "expected a cancellation, got {err:?}"
    );
    assert!(
        elapsed < timeout / 5,
        "cancel took {elapsed:?} of a {timeout:?} timeout — it was polled, not awaited"
    );
    // Cancellation still means *paused*: the partial survives for a resume.
    assert!(dest.with_extension("bin.part").exists());

    peer.abort();
    session.close().await.unwrap();
}
