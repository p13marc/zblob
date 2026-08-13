//! Edge and concurrency coverage: multi-query transfers, resume corner
//! cases, and concurrent downloads.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobId, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    MemoryBlobSource, Progress, RetryPolicy,
};

fn test_client(session: &zenoh::Session, prefix: &str) -> BlobClient {
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
    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
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
    let client = BlobClient::builder(&session, common::query(prefix))
        .query_timeout(Duration::from_secs(5))
        .max_chunks_per_query(4)
        .build();
    let stats = tokio::time::timeout(
        Duration::from_secs(30),
        client.download_to(&DownloadRequest::new("big"), &dest),
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
    let server = BlobServer::new(&session, common::serve(prefix.clone()));
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
    let client = test_client(&session, &prefix);
    let saw_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = saw_started.clone();
    let sink = move |p: Progress| {
        if matches!(p, Progress::Started { .. }) {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    };
    client
        .download_to(&DownloadRequest::new("clean"), &dest)
        .progress(&sink)
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
        .download_to(&DownloadRequest::new("clean"), &dest2)
        .progress(&CancelFirst(token.clone()))
        .cancel(&token)
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
        .download_to(&DownloadRequest::new("clean"), &dest2)
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
    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    server
        .register_source(
            BlobSpec::new("shared").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = Arc::new(test_client(&session, &prefix));
    let dir = tempfile::tempdir().unwrap();
    let mut joins = Vec::new();
    for i in 0..3 {
        let client = client.clone();
        let dest = dir.path().join(format!("copy{i}.bin"));
        joins.push(tokio::spawn(async move {
            client
                .download_to(&DownloadRequest::new("shared"), &dest)
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
    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    let manifest = server
        .register_source(
            BlobSpec::new("wr").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let client = test_client(&session, &prefix);
    let mut cursor = std::io::Cursor::new(Vec::new());
    let stats = client
        .download_to_writer(&DownloadRequest::pinned("wr", manifest.root), &mut cursor)
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

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
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
    let client = test_client(&session, &prefix);
    client
        .download_to(&DownloadRequest::pinned("spill", manifest.root), &dest)
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
    let server = BlobServer::new(&session, common::serve(good.clone()));
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
    let client = test_client(&session, &good);
    client
        .download_to(&DownloadRequest::pinned("ok", manifest.root), &dest)
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
    let server = BlobServer::new(&session, common::serve(concrete.clone()));
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
    let prober = test_client(&session, &wildcard);
    let found = prober
        .fetch_manifest("probed")
        .await
        .expect("a wildcard-origin probe must be allowed");
    assert_eq!(found.root, manifest.root);

    // …and then the bulk fetch happens from the chosen concrete origin.
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    test_client(&session, &concrete)
        .download_to(&DownloadRequest::pinned("probed", manifest.root), &dest)
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_transfers_default_to_a_yielding_priority() {
    // This asserted only that `DataLow as u8 > Data as u8` — a property of
    // *Zenoh's enum*, which would hold unchanged if this crate defaulted to
    // `RealTime`. The claim is about the default zblob picks, so ask zblob.
    let session = open_session().await;
    let prefix = unique_prefix();

    let client = BlobClient::new(&session, common::query(&prefix));
    assert!(
        client.priority() as u8 > zblob::Priority::Data as u8,
        "the bulk default ({:?}) must yield to ordinary data traffic",
        client.priority()
    );

    // Discriminating power: the knob genuinely moves it, so the assertion
    // above is reading a real value and not a constant.
    let urgent = BlobClient::builder(&session, common::query(&prefix))
        .priority(zblob::Priority::RealTime)
        .build();
    assert_eq!(urgent.priority(), zblob::Priority::RealTime);

    session.close().await.unwrap();
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

    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    let manifest = server
        .register_file(BlobSpec::new("mut").chunk_size(MIN_CHUNK_SIZE), &path)
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();
    let client = test_client(&session, &prefix);

    // Discriminating power first: while the file is untouched, it downloads.
    let out = dir.path().join("before.bin");
    client
        .download_to(&DownloadRequest::pinned("mut", manifest.root), &out)
        .await
        .expect("an unmodified source must serve normally");
    assert_eq!(std::fs::read(&out).unwrap(), original);

    // Now rewrite the backing file behind the server's back.
    std::fs::write(&path, pseudo_random(MIN_CHUNK_SIZE as usize * 3, 32)).unwrap();

    let out2 = dir.path().join("after.bin");
    let err = client
        .download_to(&DownloadRequest::pinned("mut", manifest.root), &out2)
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
    let server = BlobServer::new(&session, common::serve(prefix.clone()));

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
    assert!(matches!(err, zblob::BlobError::Usage(_)), "{err}");

    let handle = server.spawn().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("still.bin");
    test_client(&session, &prefix)
        .download_to(&DownloadRequest::pinned("stable", m1.root), &out)
        .await
        .expect("the original registration must still serve");
    assert_eq!(std::fs::read(&out).unwrap(), first);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// An id nobody serves fails **fast**, not after the query timeout.
///
/// Worth pinning, because the opposite was widely believed: the serving code
/// carried the comment "unknown id → client times out → NotFound", and a whole
/// negative-reply message was designed on that premise. It is not how Zenoh
/// behaves. A query finalizes when every matching queryable has completed, and
/// a queryable that drops the `Query` without replying completes immediately —
/// so silence resolves in about a millisecond, whatever the timeout is.
///
/// Silence is therefore the *right* way to say "not mine", which is what lets
/// several servers share one prefix without any of them having to answer for
/// ids they do not own. This test is what would catch a change that made
/// silence expensive again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_id_fails_fast_not_on_the_timeout() {
    let session = open_session().await;
    // A generous timeout: if any of these waits for it, the test fails on
    // duration rather than on outcome.
    let timeout = Duration::from_secs(30);
    let budget = Duration::from_secs(2);

    let served = unique_prefix();
    let handle = BlobServer::new(&session, common::serve(served.clone()))
        .spawn()
        .await
        .unwrap();

    let base = unique_prefix();
    let mut wide_handles = Vec::new();
    for host in ["host-a", "host-b"] {
        wide_handles.push(
            BlobServer::new(&session, common::serve(format!("{base}/{host}/x")))
                .spawn()
                .await
                .unwrap(),
        );
    }

    let cases: Vec<(&str, String)> = vec![
        // A server is listening on the prefix; it just does not own this id.
        ("a server that does not own the id", served),
        // Nothing is listening at all.
        ("no server on the prefix", unique_prefix()),
        // The fan-out shape: several origins, none of which hold it.
        ("a wildcard across two servers", format!("{base}/*/x")),
    ];
    for (what, prefix) in cases {
        let client = BlobClient::builder(&session, common::query(prefix))
            .query_timeout(timeout)
            .build();
        let started = std::time::Instant::now();
        let err = client
            .fetch_manifest("nonexistent")
            .await
            .expect_err("must not resolve");
        let elapsed = started.elapsed();
        assert!(
            matches!(err, zblob::BlobError::NotFound(_)),
            "{what}: {err}"
        );
        assert!(
            elapsed < budget,
            "{what}: took {elapsed:?}, which means silence is costing the timeout"
        );
    }

    handle.shutdown().await.unwrap();
    for h in wide_handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}

/// A server that lowers its per-query cap must still serve an unmodified
/// client, because the client clamps to what the manifest advertises.
///
/// Before this, both sides defaulted to 512 and neither could tell the other:
/// a server that lowered its cap rejected every existing client's queries with
/// `InvalidRanges`, and the client had no way to discover why. It was
/// documented, which is not the same as being a protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_clamps_to_the_server_s_advertised_cap() {
    let session = open_session().await;
    let prefix = unique_prefix();

    // Six chunks, but the server will only serve two per query.
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 6, 71);
    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .max_chunks_per_query(2)
        .build();
    let manifest = server
        .register_source(
            BlobSpec::new("capped").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    // The manifest says so, and says it in a form a consumer can read.
    assert_eq!(manifest.max_chunks_per_query(), Some(2));

    // A client with the stock 512 default downloads successfully anyway.
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let client = BlobClient::builder(&session, common::query(prefix))
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 3,
            base_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
        })
        // Deliberately far above the server's cap.
        .max_chunks_per_query(512)
        .build();
    client
        .download_to(&DownloadRequest::pinned("capped", manifest.root), &dest)
        .await
        .expect("a client must clamp to the advertised cap, not be rejected by it");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A peer sending an extension id we do not know about is handled without
/// error — which is the entire point of having an extension list.
#[test]
fn unknown_extension_ids_are_skipped() {
    let mut m = zblob::Manifest {
        version: zblob::wire::WIRE_VERSION,
        id: BlobId::new("x").unwrap(),
        filename: None,
        total_len: 1024,
        chunk_size: MIN_CHUNK_SIZE,
        root: zblob::Hash::of(b"x"),
        created_ms: 0,
        ext: zblob::wire::Ext::from_fields(vec![
            (60_000, b"from a future version".to_vec()),
            (
                zblob::wire::EXT_MAX_CHUNKS_PER_QUERY,
                7u32.to_le_bytes().to_vec(),
            ),
            (60_001, Vec::new()),
        ])
        .unwrap(),
    };
    m.validate(u64::MAX)
        .expect("unknown ids must not invalidate");
    assert_eq!(
        m.max_chunks_per_query(),
        Some(7),
        "known ids still readable"
    );
    assert_eq!(m.max_blob_size(), None, "absent ids report absent");

    // A known id carrying the wrong width is ignored rather than misread.
    m.ext =
        zblob::wire::Ext::from_fields(vec![(zblob::wire::EXT_MAX_CHUNKS_PER_QUERY, vec![1, 2])])
            .unwrap();
    assert_eq!(m.max_chunks_per_query(), None);

    // The bounds are enforced by decoding, not by remembering to check: a
    // manifest whose extension list is over either cap does not exist.
    assert!(
        zblob::wire::Ext::from_fields(
            (0..=zblob::wire::Ext::MAX_FIELDS as u16)
                .map(|i| (i, Vec::new()))
                .collect()
        )
        .is_err(),
        "too many fields must be refused"
    );
    assert!(
        zblob::wire::Ext::from_fields(vec![(1, vec![0u8; zblob::wire::Ext::MAX_VALUE_LEN + 1])])
            .is_err(),
        "an oversized value must be refused"
    );
    // Discriminating power: exactly at each cap is accepted.
    assert!(
        zblob::wire::Ext::from_fields(
            (0..zblob::wire::Ext::MAX_FIELDS as u16)
                .map(|i| (i, vec![0u8; zblob::wire::Ext::MAX_VALUE_LEN]))
                .collect()
        )
        .is_ok(),
        "the caps themselves must be legal"
    );
}
