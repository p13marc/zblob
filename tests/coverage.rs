//! Edge and concurrency coverage: multi-query transfers, resume corner
//! cases, and concurrent downloads.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    MemoryBlobSource, Progress, RetryPolicy,
};

fn test_client(session: Arc<zenoh::Session>, prefix: &str) -> BlobClient {
    BlobClient::builder(session, prefix)
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 3,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        })
        .build()
}

/// A transfer larger than one query's chunk budget must split into multiple
/// sequential queries. The server's own cap is set to the same small value, so
/// a client that failed to split would have its query rejected and stall —
/// success *proves* the multi-query path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_blob_spans_multiple_queries() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 12, 51);
    let server = BlobServer::builder(session.clone(), prefix.clone())
        .max_chunks_per_query(4)
        .build();
    server
        .register_source(
            BlobSpec::new("big").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("big.bin");
    let client = BlobClient::builder(session.clone(), &prefix)
        .query_timeout(Duration::from_secs(5))
        .max_chunks_per_query(4)
        .build();
    let stats = tokio::time::timeout(
        Duration::from_secs(30),
        client.download_to(
            &DownloadRequest::new("big"),
            &dest,
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("download");
    assert_eq!(stats.chunks_fetched, 12);
    assert_eq!(
        content_hash(&std::fs::read(&dest).unwrap()),
        content_hash(&data)
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A stray `.part` without a sidecar (or with a wrong-length `.part`) must be
/// discarded and the download restarted cleanly — never spliced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stray_or_mismatched_partial_restarts_clean() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 52);
    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("clean").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    // Case 1: .part exists with garbage, no sidecar.
    std::fs::write(dir.path().join("out.bin.part"), b"stray garbage").unwrap();
    let client = test_client(session.clone(), &prefix);
    let saw_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = saw_started.clone();
    let sink = move |p: Progress| {
        if matches!(p, Progress::Started { .. }) {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    };
    client
        .download_to(
            &DownloadRequest::new("clean"),
            &dest,
            &sink,
            &CancelToken::new(),
        )
        .await
        .expect("download");
    assert!(
        saw_started.load(std::sync::atomic::Ordering::SeqCst),
        "a stray partial must trigger a fresh start, not a resume"
    );
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // Case 2: valid sidecar but truncated .part → fresh start, still correct.
    let dest2 = dir.path().join("out2.bin");
    // Build partial state by cancelling after one chunk.
    struct CancelFirst(CancelToken);
    impl zblob::ProgressSink for CancelFirst {
        fn emit(&self, p: Progress) {
            if matches!(p, Progress::Chunk { .. }) {
                self.0.cancel();
            }
        }
    }
    let token = CancelToken::new();
    let _ = client
        .download_to(
            &DownloadRequest::new("clean"),
            &dest2,
            &CancelFirst(token.clone()),
            &token,
        )
        .await;
    let part2 = dir.path().join("out2.bin.part");
    assert!(part2.exists());
    // Truncate the partial behind the sidecar's back.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&part2)
        .unwrap()
        .set_len(10)
        .unwrap();
    client
        .download_to(
            &DownloadRequest::new("clean"),
            &dest2,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("re-download after truncation");
    assert_eq!(std::fs::read(&dest2).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Two concurrent downloads of one blob to *different* destinations share the
/// server (inflight semaphore) without interference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_downloads_to_different_destinations() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 5, 53);
    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("shared").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = Arc::new(test_client(session.clone(), &prefix));
    let dir = tempfile::tempdir().unwrap();
    let mut joins = Vec::new();
    for i in 0..3 {
        let client = client.clone();
        let dest = dir.path().join(format!("copy{i}.bin"));
        joins.push(tokio::spawn(async move {
            client
                .download_to(
                    &DownloadRequest::new("shared"),
                    &dest,
                    &(),
                    &CancelToken::new(),
                )
                .await
                .map(|_| dest)
        }));
    }
    for join in joins {
        let dest = join.await.unwrap().expect("concurrent download");
        assert_eq!(
            content_hash(&std::fs::read(&dest).unwrap()),
            content_hash(&data)
        );
    }

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// `download_to_writer` fills an in-memory cursor with verified bytes — no
/// `.part`, no sidecar.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_to_writer_roundtrip() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2 + 500, 54);
    let server = BlobServer::new(session.clone(), prefix.clone());
    let manifest = server
        .register_source(
            BlobSpec::new("wr").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = test_client(session.clone(), &prefix);
    let mut cursor = std::io::Cursor::new(Vec::new());
    let stats = client
        .download_to_writer(
            &DownloadRequest::pinned("wr", manifest.root),
            &mut cursor,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("writer download");
    assert_eq!(cursor.into_inner(), data);
    assert_eq!(stats.chunks_fetched, 3);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A tiny `outboard_mem_limit` forces `register_file` to spill the outboard
/// to a sibling `.obao4` file; serving from it must still verify end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_outboard_spill_roundtrip() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 5 + 3, 55);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("spill.bin");
    std::fs::write(&src_path, &data).unwrap();

    let server = BlobServer::builder(session.clone(), prefix.clone())
        .outboard_mem_limit(0) // force the file-backed outboard path
        .build();
    let manifest = server
        .register_file(BlobSpec::new("spill").chunk_size(MIN_CHUNK_SIZE), &src_path)
        .await
        .expect("register");
    assert!(
        src.path().join("spill.bin.obao4").exists(),
        "outboard must be spilled to the sibling file"
    );
    let handle = server.spawn().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let client = test_client(session.clone(), &prefix);
    client
        .download_to(
            &DownloadRequest::pinned("spill", manifest.root),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("download from file outboard");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A key prefix or blob id that cannot work must fail loudly at first use,
/// not silently never serve. (`@`-leading ids are the trap: Zenoh's `**`
/// does not match verbatim segments, so a server would register happily and
/// answer nothing, forever.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unusable_prefixes_and_ids_fail_loudly() {
    let session = open_session().await;

    // Wildcard / malformed prefixes are refused when the server declares.
    for bad_prefix in ["", "wild/*/card", "trailing/", "/leading", "a//b"] {
        let server = BlobServer::new(session.clone(), bad_prefix);
        let err = server
            .spawn()
            .await
            .err()
            .unwrap_or_else(|| panic!("prefix {bad_prefix:?} should be refused"));
        assert!(
            matches!(err, zblob::BlobError::Protocol(_)),
            "{bad_prefix:?}: {err}"
        );
        // …and by clients, before any query goes out.
        let client = BlobClient::new(session.clone(), bad_prefix);
        assert!(
            client.fetch_manifest("x").await.is_err(),
            "client accepted prefix {bad_prefix:?}"
        );
    }

    // A convention-style verbatim prefix must keep working.
    let good = format!("{}/@blob/artifact", unique_prefix());
    let server = BlobServer::new(session.clone(), good.clone());
    let data = pseudo_random(MIN_CHUNK_SIZE as usize, 60);
    let manifest = server
        .register_source(
            BlobSpec::new("ok").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server
        .clone()
        .spawn()
        .await
        .expect("verbatim prefix must work");
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let client = test_client(session.clone(), &good);
    client
        .download_to(
            &DownloadRequest::pinned("ok", manifest.root),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("download under a verbatim-segment prefix");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // A leading-@ id is refused at registration rather than silently unservable.
    let err = server
        .register_source(
            BlobSpec::new("@unservable").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data)),
        )
        .await
        .expect_err("leading-@ id must be refused");
    assert!(matches!(err, zblob::BlobError::InvalidManifest(_)), "{err}");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
