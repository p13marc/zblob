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
    BlobClient::builder(session, common::query(prefix))
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
    let server = BlobServer::builder(session.clone(), common::serve(prefix.clone()))
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
    let client = BlobClient::builder(session.clone(), common::query(prefix))
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
    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));
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
    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));
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
    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));
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

    let server = BlobServer::builder(session.clone(), common::serve(prefix.clone()))
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

    // Malformed prefixes never reach a server or a client at all: they are
    // refused at construction, so there is no way to build one and discover
    // the problem at declare time. (The role rules themselves are asserted in
    // the `prefix` module's own tests; this is the end-to-end consequence.)
    for bad_prefix in ["", "trailing/", "/leading", "a//b"] {
        assert!(
            zblob::ServePrefix::new(bad_prefix).is_err(),
            "{bad_prefix:?} must not be constructible as a serve prefix"
        );
        assert!(
            zblob::QueryPrefix::new(bad_prefix).is_err(),
            "{bad_prefix:?} must not be constructible as a query prefix"
        );
    }
    // A wildcard is queryable but not servable — the asymmetry is the point.
    assert!(zblob::QueryPrefix::new("wild/*/card").is_ok());
    assert!(zblob::ServePrefix::new("wild/*/card").is_err());

    // A convention-style verbatim prefix must keep working.
    let good = format!("{}/@blob/artifact", unique_prefix());
    let server = BlobServer::new(session.clone(), common::serve(good.clone()));
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

/// A wildcard-origin prefix is a legal *probe* (ask every holder), so clients
/// must accept it — while servers and publishers must not, since they would
/// answer for or write to keys they do not own. Getting this backwards breaks
/// a sanctioned multi-holder pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_prefixes_are_queryable_but_not_servable() {
    let session = open_session().await;
    let base = unique_prefix();
    let concrete = format!("{base}/host-a/@blob/artifact");
    let wildcard = format!("{base}/*/@blob/artifact");

    // A server on the concrete prefix.
    let server = BlobServer::new(session.clone(), common::serve(concrete.clone()));
    let data = pseudo_random(MIN_CHUNK_SIZE as usize, 61);
    let manifest = server
        .register_source(
            BlobSpec::new("probed").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    // Probing across origins with a wildcard prefix finds it…
    let prober = test_client(session.clone(), &wildcard);
    let found = prober
        .fetch_manifest("probed")
        .await
        .expect("a wildcard-origin probe must be allowed");
    assert_eq!(found.root, manifest.root);

    // …and then the bulk fetch happens from the chosen concrete origin.
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    test_client(session.clone(), &concrete)
        .download_to(
            &DownloadRequest::pinned("probed", manifest.root),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("concrete fetch");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // Serving under a wildcard prefix does not typecheck: there is no
    // `ServePrefix` to build from one.
    assert!(
        zblob::ServePrefix::new(wildcard.clone()).is_err(),
        "a server must not be constructible on a wildcard prefix"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Bulk transfers must yield: replies inherit the querier's QoS, so the
/// client is the only place priority can be set — and the default must sit
/// below `Data` or a large transfer starves telemetry on a shared link.
#[test]
fn bulk_transfers_default_to_a_yielding_priority() {
    // Zenoh numbers priorities so that a *greater* discriminant is a *lower*
    // priority. If that ever flips, the crate's bulk default must be revisited.
    assert!(
        zblob::Priority::DataLow as u8 > zblob::Priority::Data as u8,
        "Priority ordering changed; revisit the bulk default"
    );
}

/// A registered file that changes on disk must be diagnosed, not served.
///
/// The bao outboard is computed once at registration and every slice is proved
/// against it. If the backing bytes move, each slice fails the *client's*
/// verification — forever — and neither end can say why: the client sees only
/// a rising rejected count and eventually `Incomplete`, the server sees
/// nothing at all. One `stat` per query turns that into a single error on the
/// side that can act on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mutated_source_is_diagnosed_not_served_forever() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mutable.bin");
    let original = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 31);
    std::fs::write(&path, &original).unwrap();

    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));
    let manifest = server
        .register_file(BlobSpec::new("mut").chunk_size(MIN_CHUNK_SIZE), &path)
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();
    let client = test_client(session.clone(), &prefix);

    // Discriminating power first: while the file is untouched, it downloads.
    let out = dir.path().join("before.bin");
    client
        .download_to(
            &DownloadRequest::pinned("mut", manifest.root),
            &out,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("an unmodified source must serve normally");
    assert_eq!(std::fs::read(&out).unwrap(), original);

    // Now rewrite the backing file behind the server's back.
    std::fs::write(&path, pseudo_random(MIN_CHUNK_SIZE as usize * 3, 32)).unwrap();

    let out2 = dir.path().join("after.bin");
    let err = client
        .download_to(
            &DownloadRequest::pinned("mut", manifest.root),
            &out2,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("a mutated source must not silently fail forever");
    // The manifest itself is refused now, so this fails fast rather than
    // grinding through the retry budget.
    assert!(
        matches!(err, zblob::BlobError::NotFound(_)),
        "unexpected error: {err}"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Re-registering an id with different content is refused, matching the push
/// path's hijack defence. Re-registering *identical* content stays a no-op, so
/// idempotent registration keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn re_registration_cannot_silently_swap_content() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let server = BlobServer::new(session.clone(), common::serve(prefix.clone()));

    let first = pseudo_random(4096, 41);
    let spec = || BlobSpec::new("stable").chunk_size(MIN_CHUNK_SIZE);
    let m1 = server
        .register_source(spec(), Arc::new(MemoryBlobSource::new(first.clone())))
        .await
        .unwrap();

    // Identical content: idempotent.
    let m2 = server
        .register_source(spec(), Arc::new(MemoryBlobSource::new(first.clone())))
        .await
        .expect("re-registering identical content must stay a no-op");
    assert_eq!(m1.root, m2.root);

    // Different content: refused, and the original keeps serving.
    let err = server
        .register_source(
            spec(),
            Arc::new(MemoryBlobSource::new(pseudo_random(4096, 42))),
        )
        .await
        .expect_err("replacing an id's content must be refused");
    assert!(matches!(err, zblob::BlobError::Protocol(_)), "{err}");

    let handle = server.spawn().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("still.bin");
    test_client(session.clone(), &prefix)
        .download_to(
            &DownloadRequest::pinned("stable", m1.root),
            &out,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("the original registration must still serve");
    assert_eq!(std::fs::read(&out).unwrap(), first);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
