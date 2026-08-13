//! The public read surface: what a consumer holding an address — rather than a
//! transfer — can actually do.
//!
//! Each of these existed inside the crate and was reachable only by pulling a
//! whole snapshot, or by hand-decoding wire types from outside. The tests
//! assert the *consumer's* shape: a bare `store/<algo>/<hash>` with no tree, a
//! snapshot inspected without materializing it, a probe that says who answered.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{open_session, unique_prefix};
use zblob::{
    BlobClient, BlobServer, BlobSpec, CdcParams, ContentStore, DownloadRequest, Hash,
    MIN_CHUNK_SIZE, MemoryBlobSource, MemoryStore, StoreClient, TreeServer, build_tree,
};

fn small_cdc() -> CdcParams {
    CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    }
}

/// A caller with a bare content address and no tree can fetch and verify one
/// chunk — the case that previously had no public path at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_content_address_can_be_fetched_and_verified() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    let body = common::pseudo_random(40_000, 91);
    std::fs::write(src.path().join("f.bin"), &body).unwrap();
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "chunks", &small_cdc(), &*server_store).unwrap();
    let server = TreeServer::new(
        &session,
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store.clone(),
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    // No TreeClient, no tree prefix, no ContentStore — just the store.
    let store_client = StoreClient::builder(&session, common::query(store_prefix))
        .query_timeout(Duration::from_secs(5))
        .build();

    for c in index.needed_chunk_refs() {
        let bytes = store_client
            .fetch_chunk_sized(&c.hash, c.len)
            .await
            .expect("a chunk must be fetchable by its address alone");
        assert_eq!(
            Hash::of(&bytes),
            c.hash,
            "returned bytes must match address"
        );
        assert_eq!(bytes.len() as u32, c.len);

        // The unsized form works too, for a caller with no index in hand.
        assert_eq!(store_client.fetch_chunk(&c.hash).await.unwrap(), bytes);
    }

    // An address nobody holds fails cleanly rather than hanging forever.
    let err = store_client
        .fetch_chunk(&Hash::of(b"never stored anywhere"))
        .await
        .expect_err("an absent chunk must not succeed");
    assert!(matches!(err, zblob::BlobError::NotFound(_)), "{err}");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A snapshot can be inspected — entry count, size, chunk references — without
/// a `ContentStore` and without materializing anything. Explorers want to
/// *look*, and were told they needed somewhere to write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_can_be_inspected_without_materializing_it() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(src.path().join("sub")).unwrap();
    std::fs::write(src.path().join("a.bin"), common::pseudo_random(30_000, 92)).unwrap();
    std::fs::write(src.path().join("sub/b.txt"), b"small").unwrap();
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    // Keyed by its own root: identity and address are the same thing.
    let index = build_tree(src.path(), "tmp", &small_cdc(), &*server_store)
        .unwrap()
        .keyed_by_root();
    let root = index.root_hash;
    let expected_files = index.file_count();
    let expected_size = index.total_size();

    let server = TreeServer::new(
        &session,
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let client = zblob::TreeClient::builder(
        &session,
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(5))
    .build();

    let fetched = client
        .fetch_index_by_root(&root)
        .await
        .expect("a root-addressed index must be fetchable");
    assert_eq!(fetched.root_hash, root);
    assert_eq!(fetched.file_count(), expected_files);
    assert_eq!(fetched.total_size(), expected_size);
    assert!(fetched.is_content_addressed());

    // Pinned by construction: asking for a root nobody serves cannot
    // accidentally return some other snapshot.
    let err = client
        .fetch_index_by_root(&Hash::of(b"a root that does not exist"))
        .await
        .expect_err("an unknown root must not resolve to anything");
    assert!(matches!(err, zblob::BlobError::NotFound(_)), "{err}");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A probe names who answered, so the caller can fetch from one chosen holder
/// — which is the whole shape RFC 07 §2.5 prescribes, and was previously left
/// for callers to reconstruct out of raw reply keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_attributes_each_answer_to_its_origin() {
    let session = open_session().await;
    let base = unique_prefix();
    let data = common::pseudo_random(MIN_CHUNK_SIZE as usize * 2, 93);

    // Two origins serving the same id, reachable through one wildcard.
    let mut handles = Vec::new();
    for host in ["host-a", "host-b"] {
        let server = BlobServer::new(
            &session,
            common::serve(format!("{base}/{host}/@blob/artifact")),
        );
        server
            .register_source(
                BlobSpec::new("shared").chunk_size(MIN_CHUNK_SIZE),
                Arc::new(MemoryBlobSource::new(data.clone())),
            )
            .await
            .unwrap();
        handles.push(server.spawn().await.unwrap());
    }

    let prober = BlobClient::builder(&session, common::query(format!("{base}/*/@blob/artifact")))
        .query_timeout(Duration::from_secs(5))
        .build();

    let holders = prober.probe("shared").await.expect("probe");
    assert_eq!(holders.len(), 2, "one entry per holder, not per reply");
    let mut origins: Vec<_> = holders.iter().map(|h| h.origin.to_string()).collect();
    origins.sort();
    assert_eq!(
        origins,
        vec![
            format!("{base}/host-a/@blob/artifact"),
            format!("{base}/host-b/@blob/artifact"),
        ],
        "each answer must name the origin that gave it"
    );
    for h in &holders {
        assert!(h.origin.is_concrete(), "an origin names exactly one holder");
        assert_eq!(h.manifest.chunk_count().unwrap(), 2);
        let avail = h.availability.as_ref().expect("holders answered `have`");
        assert_eq!(avail.count(), 2);
    }

    // …and the probe result feeds a fetch from one chosen holder directly.
    let chosen = holders.into_iter().next().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let staged = BlobClient::builder(&session, chosen.origin)
        .query_timeout(Duration::from_secs(5))
        .build()
        .download_staged(
            &DownloadRequest::pinned("shared", chosen.manifest.root),
            dir.path(),
        )
        .await
        .expect("fetch from the chosen origin");
    assert_eq!(std::fs::read(&staged.path).unwrap(), data);

    for h in handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}

/// Staging is by **id**, so two blobs whose servers both claim the same
/// filename cannot collide — which is the reason the convention exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_downloads_named_alike_do_not_collide() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let server = BlobServer::new(&session, common::serve(prefix.clone()));

    let first = common::pseudo_random(9_000, 94);
    let second = common::pseudo_random(9_000, 95);
    for (id, data) in [("blob-one", &first), ("blob-two", &second)] {
        server
            .register_source(
                // Both servers claim the *same* advisory filename.
                BlobSpec::new(id).filename("report.pcap"),
                Arc::new(MemoryBlobSource::new(data.clone())),
            )
            .await
            .unwrap();
    }
    let handle = server.spawn().await.unwrap();

    let client = BlobClient::builder(&session, common::query(prefix))
        .query_timeout(Duration::from_secs(5))
        .build();
    let dir = tempfile::tempdir().unwrap();

    let a = client
        .download_staged(&DownloadRequest::new("blob-one"), dir.path())
        .await
        .unwrap();
    let b = client
        .download_staged(&DownloadRequest::new("blob-two"), dir.path())
        .await
        .unwrap();

    assert_ne!(a.path, b.path, "staging under the id must keep them apart");
    assert_eq!(std::fs::read(&a.path).unwrap(), first);
    assert_eq!(std::fs::read(&b.path).unwrap(), second);
    // The suggestion is carried, and nothing was named by it.
    assert_eq!(a.suggested.as_deref(), Some("report.pcap"));
    assert_eq!(b.suggested.as_deref(), Some("report.pcap"));
    assert!(!dir.path().join("report.pcap").exists());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// The container framing is decodable from outside the crate, so a caller that
/// fetched a chunk by hand — or read one back from a storage — can get the
/// content out of it.
#[test]
fn chunk_containers_round_trip_through_the_public_api() {
    let body = common::pseudo_random(20_000, 96);
    let framed = zblob::frame_chunk(&body, zblob::ChunkCompression::None).unwrap();
    assert_ne!(framed, body, "a container is not the bare bytes");
    assert_eq!(zblob::unframe_chunk(&framed).unwrap(), body);
    // Garbage is refused rather than mistaken for content.
    assert!(zblob::unframe_chunk(&[0xEE, 1, 2, 3]).is_err());
    assert!(zblob::unframe_chunk(&[]).is_err());
}

/// One file out of a snapshot, without materializing the tree.
///
/// The capability a snapshot most obviously implies and did not have: a
/// caller wanting one config file out of a large tree had to download the
/// whole thing to a scratch directory and read one path out of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_file_pulls_one_path_and_only_its_chunks() {
    use zblob::TreeClient;

    let session = open_session().await;
    let store_prefix = unique_prefix();
    let tree_prefix = unique_prefix();

    // Three files; only the middle one is asked for.
    let src = tempfile::tempdir().unwrap();
    let wanted = common::pseudo_random(40_000, 11);
    let bulk_a = common::pseudo_random(200_000, 12);
    let bulk_b = common::pseudo_random(200_000, 13);
    std::fs::create_dir(src.path().join("etc")).unwrap();
    std::fs::write(src.path().join("etc/app.conf"), &wanted).unwrap();
    std::fs::write(src.path().join("big-a.bin"), &bulk_a).unwrap();
    std::fs::write(src.path().join("big-b.bin"), &bulk_b).unwrap();

    let cdc = small_cdc();
    let producer: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &cdc, &producer).unwrap();
    let expected_root = index.root_hash;

    // The index alone answers "what is in here" — no fetching involved.
    let paths: Vec<&str> = index.files().map(|(p, _, _)| p).collect();
    assert!(paths.contains(&"etc/app.conf"), "index lists {paths:?}");
    let (_, listed_size, listed_chunks) = index
        .files()
        .find(|(p, _, _)| *p == "etc/app.conf")
        .expect("the file must be listed");
    assert_eq!(listed_size, wanted.len() as u64);
    assert_eq!(listed_chunks, index.file_chunks("etc/app.conf").unwrap());
    assert!(
        index.file_chunks("etc").is_none(),
        "a directory is not a file"
    );
    assert!(index.file_chunks("nope").is_none());

    let server = TreeServer::new(
        &session,
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        producer.clone(),
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let consumer: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let client = TreeClient::new(
        &session,
        common::query(&store_prefix),
        common::query(&tree_prefix),
    );

    let got = client
        .fetch_file(
            &DownloadRequest::pinned("snap", expected_root),
            "etc/app.conf",
            &consumer,
        )
        .await
        .expect("fetch one file");
    assert_eq!(got, wanted, "wrong bytes");

    // Only that file's chunks were pulled: the two 200 KB files are absent
    // from the consumer's store. Without this the test would pass against an
    // implementation that fetched the whole snapshot.
    let held = consumer.hashes().unwrap();
    let wanted_chunks = index.file_chunks("etc/app.conf").unwrap().len();
    assert_eq!(
        held.len(),
        wanted_chunks,
        "expected exactly the file's {wanted_chunks} chunks, got {}",
        held.len()
    );
    let all = index.needed_chunks().len();
    assert!(
        all > wanted_chunks * 2,
        "fixture must have far more chunks than the fetched file ({all} vs {wanted_chunks})"
    );

    // A second fetch of the same file is served entirely from the store.
    let again = client
        .fetch_file(
            &DownloadRequest::pinned("snap", expected_root),
            "etc/app.conf",
            &consumer,
        )
        .await
        .unwrap();
    assert_eq!(again, wanted);

    // A path the snapshot does not have is NotFound, not a hang or a panic.
    let err = client
        .fetch_file(&DownloadRequest::new("snap"), "etc/absent.conf", &consumer)
        .await
        .unwrap_err();
    assert!(matches!(err, zblob::BlobError::NotFound(_)), "{err}");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A server can be asked what it serves. It used to be write-only, so every
/// caller needing this kept a shadow copy of the registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn servers_report_what_they_serve() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    assert!(server.registered().await.is_empty());
    assert!(!server.serves("art-1").await);
    assert!(server.manifest("art-1").await.is_none());

    let manifest = server
        .register_source(
            BlobSpec::new("art-1").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(common::pseudo_random(8192, 20))),
        )
        .await
        .unwrap();

    assert!(server.serves("art-1").await);
    assert_eq!(
        server
            .registered()
            .await
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["art-1".to_string()]
    );
    // The accessor agrees with what a client would fetch — that is the point
    // of it, so it must not be a differently-derived value.
    assert_eq!(server.manifest("art-1").await.unwrap(), manifest);

    server.unregister("art-1").await;
    assert!(!server.serves("art-1").await);
    assert!(server.registered().await.is_empty());

    session.close().await.unwrap();
}
