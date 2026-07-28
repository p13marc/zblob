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
    BlobClient::builder(session, prefix)
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
    let server = BlobServer::new(session.clone(), prefix.clone());
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
    let server = BlobServer::new(session.clone(), prefix.clone());
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
