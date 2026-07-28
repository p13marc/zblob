//! End-to-end Tier-1 round trips: register → verified download → identical
//! bytes, across file and memory sources, sizes from empty to multi-chunk,
//! pinned and TOFU roots, and overwrite policies.

mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, Hash,
    MIN_CHUNK_SIZE, MemoryBlobSource, Overwrite, Progress, RetryPolicy,
};

/// A client tuned for tests: fail fast instead of the 30 s defaults.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_file_multichunk_pinned() {
    let session = open_session().await;
    let prefix = unique_prefix();

    // 4 full chunks + a short tail.
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4 + 12_345, 1);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("artifact.bin");
    std::fs::write(&src_path, &data).unwrap();

    let server = BlobServer::new(session.clone(), prefix.clone());
    let manifest = server
        .register_file(
            BlobSpec::new("blob-1")
                .filename("artifact.bin")
                .chunk_size(MIN_CHUNK_SIZE)
                .created_ms(42),
            &src_path,
        )
        .await
        .expect("register");
    assert_eq!(manifest.total_len, data.len() as u64);
    assert_eq!(manifest.root, content_hash(&data)); // bao root == plain blake3
    let handle = server.spawn().await.unwrap();

    let dest_dir = tempfile::tempdir().unwrap();
    let dest = dest_dir.path().join("out.bin");
    let events: Arc<Mutex<Vec<Progress>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_events = events.clone();
    let sink = move |p: Progress| sink_events.lock().unwrap().push(p);

    let client = test_client(session.clone(), &prefix);
    let stats = tokio::time::timeout(
        Duration::from_secs(20),
        client.download_to(
            &DownloadRequest::pinned("blob-1", manifest.root),
            &dest,
            &sink,
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("download");
    assert_eq!(stats.chunks_fetched, 5);
    assert_eq!(stats.chunks_resumed, 0);
    assert_eq!(stats.bytes_fetched, data.len() as u64);
    assert_eq!(stats.rejected, 0);

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    // .part and sidecar are gone after completion.
    assert!(!dest_dir.path().join("out.bin.part").exists());
    assert!(!dest_dir.path().join("out.bin.part.meta").exists());

    {
        let events = events.lock().unwrap();
        assert!(matches!(
            events.first(),
            Some(Progress::Started { chunk_count: 5, .. })
        ));
        assert!(matches!(events.last(), Some(Progress::Completed { .. })));
        let chunk_events = events
            .iter()
            .filter(|e| matches!(e, Progress::Chunk { .. }))
            .count();
        assert_eq!(chunk_events, 5);
    }

    handle.shutdown().await.expect("shutdown");
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_memory_source_tiny_and_empty() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let tiny = pseudo_random(10_000, 2); // < one 16 KiB group
    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("tiny"),
            Arc::new(MemoryBlobSource::new(tiny.clone())),
        )
        .await
        .expect("register tiny");
    server
        .register_source(
            BlobSpec::new("empty"),
            Arc::new(MemoryBlobSource::new(Vec::new())),
        )
        .await
        .expect("register empty");
    let handle = server.spawn().await.unwrap();

    let dest_dir = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &prefix);

    let tiny_dest = dest_dir.path().join("tiny.bin");
    client
        .download_to(
            &DownloadRequest::new("tiny"),
            &tiny_dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("download tiny");
    assert_eq!(std::fs::read(&tiny_dest).unwrap(), tiny);

    let empty_dest = dest_dir.path().join("empty.bin");
    client
        .download_to(
            &DownloadRequest::new("empty"),
            &empty_dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("download empty");
    assert_eq!(std::fs::read(&empty_dest).unwrap(), b"");

    handle.shutdown().await.expect("shutdown");
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_pin_rejected_before_any_write() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(50_000, 3);
    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("blob-p"),
            Arc::new(MemoryBlobSource::new(data)),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let dest_dir = tempfile::tempdir().unwrap();
    let dest = dest_dir.path().join("out.bin");
    let client = test_client(session.clone(), &prefix);
    let err = client
        .download_to(
            &DownloadRequest::pinned("blob-p", Hash::of(b"the wrong content")),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("must reject");
    assert!(matches!(err, BlobError::RootMismatch { .. }), "{err}");
    // Nothing was written — not even a .part.
    assert!(!dest.exists());
    assert!(!dest_dir.path().join("out.bin.part").exists());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_policy_refuse_and_replace() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(30_000, 4);
    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("blob-o"),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let dest_dir = tempfile::tempdir().unwrap();
    let dest = dest_dir.path().join("out.bin");
    std::fs::write(&dest, b"previous contents").unwrap();

    // Default policy refuses to clobber; the finished .part is kept.
    let refuse = test_client(session.clone(), &prefix);
    let err = refuse
        .download_to(
            &DownloadRequest::new("blob-o"),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("must refuse");
    assert!(matches!(err, BlobError::DestinationExists(_)), "{err}");
    assert_eq!(std::fs::read(&dest).unwrap(), b"previous contents");
    assert!(dest_dir.path().join("out.bin.part").exists());

    // Replace policy overwrites atomically.
    let replace = BlobClient::builder(session.clone(), &prefix)
        .overwrite(Overwrite::Replace)
        .query_timeout(Duration::from_secs(5))
        .build();
    replace
        .download_to(
            &DownloadRequest::new("blob-o"),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("replace");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_id_is_not_found() {
    let session = open_session().await;
    let prefix = unique_prefix();
    // A server exists but has nothing registered.
    let server = BlobServer::new(session.clone(), prefix.clone());
    let handle = server.spawn().await.unwrap();

    let client = BlobClient::builder(session.clone(), &prefix)
        .query_timeout(Duration::from_secs(2))
        .build();
    let err = client.fetch_manifest("nope").await.expect_err("no blob");
    assert!(matches!(err, BlobError::NotFound(_)), "{err}");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
