//! Multi-source: two replica servers answering the same prefix — duplicate
//! replies are harmless (bitfield dedup), availability introspection sees
//! every responder, and concurrent same-destination downloads single-flight.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    MemoryBlobSource, RetryPolicy,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_replicas_serve_one_download_and_report_availability() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4 + 99, 31);

    // Two independent servers, same prefix, same content (replicas).
    let mut handles = Vec::new();
    for _ in 0..2 {
        let server = BlobServer::new(session.clone(), prefix.clone());
        server
            .register_source(
                BlobSpec::new("replicated").chunk_size(MIN_CHUNK_SIZE),
                Arc::new(MemoryBlobSource::new(data.clone())),
            )
            .await
            .unwrap();
        handles.push(server.spawn().await.unwrap());
    }

    let client = BlobClient::builder(session.clone(), &prefix)
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        })
        .build();

    // Availability: both replicas answer, each holding every chunk.
    let avail = client.fetch_availability("replicated").await.unwrap();
    assert_eq!(avail.len(), 2, "both replicas must report");
    for a in &avail {
        assert_eq!(a.chunk_count, 5);
        assert_eq!(a.count(), 5);
        assert!(a.is_set(0) && a.is_set(4) && !a.is_set(5));
    }

    // Download: duplicate replies from the second replica are ignored by the
    // bitfield; content verifies chunk by chunk.
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let stats = tokio::time::timeout(
        Duration::from_secs(20),
        client.download_to(
            &DownloadRequest::new("replicated"),
            &dest,
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("download");
    assert_eq!(
        content_hash(&std::fs::read(&dest).unwrap()),
        content_hash(&data)
    );
    assert_eq!(stats.chunks_fetched, 5, "each chunk verified exactly once");

    for h in handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_destination_single_flights() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 6, 32);

    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("sf").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = Arc::new(
        BlobClient::builder(session.clone(), &prefix)
            .query_timeout(Duration::from_secs(5))
            .build(),
    );
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("same.bin");

    // Deterministic overlap: the second download starts only once the first
    // has emitted Started (the guard is held from download_to entry, strictly
    // before any progress can be observed).
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let started_tx = std::sync::Mutex::new(Some(started_tx));
    let (c1, d1) = (client.clone(), dest.clone());
    let first = tokio::spawn(async move {
        let sink = move |p: zblob::Progress| {
            if matches!(p, zblob::Progress::Started { .. })
                && let Some(tx) = started_tx.lock().unwrap().take()
            {
                let _ = tx.send(());
            }
        };
        c1.download_to(&DownloadRequest::new("sf"), &d1, &sink, &CancelToken::new())
            .await
    });
    started_rx.await.expect("first download must start");
    let second = client
        .download_to(&DownloadRequest::new("sf"), &dest, &(), &CancelToken::new())
        .await;
    assert!(
        matches!(&second, Err(BlobError::Protocol(msg)) if msg.contains("already in progress")),
        "second concurrent download must be refused: {second:?}"
    );
    first.await.unwrap().expect("first download completes");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
